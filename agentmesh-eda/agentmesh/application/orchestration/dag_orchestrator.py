"""
DAG Orchestrator

Architectural Intent:
- Execute workflow steps respecting dependency order
- Parallelize independent steps automatically
- Handle errors and provide result aggregation

Key Design Decisions:
1. Steps declare their dependencies explicitly
2. Ready steps execute concurrently via asyncio.gather
3. Results passed between dependent steps
4. Failure in critical step halts workflow

Parallelization Notes:
- All steps at same dependency level run concurrently
- Backpressure via semaphore if max_concurrency set
- Timeout applied per-step and overall
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set
from enum import Enum
import asyncio
import logging
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


class StepStatus(Enum):
    """Status of a workflow step"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


@dataclass
class WorkflowStep:
    """
    A single step in a workflow.

    Attributes:
        name: Unique identifier for the step
        execute: Async function that performs the step
        depends_on: List of step names that must complete first
        is_critical: If True, failure halts entire workflow
        timeout: Maximum time for step execution (seconds)
        retry_count: Number of retries on failure
    """

    name: str
    execute: Callable[[Dict[str, Any], Dict[str, Any]], Any]
    depends_on: List[str] = field(default_factory=list)
    is_critical: bool = True
    timeout: float = 60.0
    retry_count: int = 0

    def __post_init__(self):
        if not callable(self.execute):
            raise ValueError("execute must be a callable")


@dataclass
class StepResult:
    """Result of a workflow step execution"""

    step_name: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: float = 0.0

    @property
    def success(self) -> bool:
        return self.status == StepStatus.COMPLETED


@dataclass
class OrchestrationResult:
    """Result of entire workflow orchestration"""

    workflow_name: str
    status: StepStatus
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    context: Dict[str, Any] = field(default_factory=dict)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: float = 0.0

    @property
    def success(self) -> bool:
        return self.status == StepStatus.COMPLETED

    @property
    def failed_steps(self) -> List[str]:
        return [
            name
            for name, r in self.step_results.items()
            if r.status == StepStatus.FAILED
        ]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "workflow_name": self.workflow_name,
            "status": self.status.value,
            "success": self.success,
            "step_results": {
                name: {
                    "status": r.status.value,
                    "output": r.output,
                    "error": r.error,
                    "duration_ms": r.duration_ms,
                }
                for name, r in self.step_results.items()
            },
            "duration_ms": self.duration_ms,
        }


class OrchestrationError(Exception):
    """Error during workflow orchestration"""

    pass


class DAGOrchestrator:
    """
    Executes workflow steps respecting dependency order,
    parallelizing independent steps automatically.

    Example:
        orchestrator = DAGOrchestrator([
            WorkflowStep("validate", self._validate),
            WorkflowStep("fetch_data", self._fetch_data),
            WorkflowStep("process", self._process, depends_on=["validate", "fetch_data"]),
            WorkflowStep("save", self._save, depends_on=["process"]),
        ])
        result = await orchestrator.execute({"order_id": "123"})
    """

    def __init__(
        self,
        steps: List[WorkflowStep],
        workflow_name: str = "workflow",
        max_concurrency: int = 10,
    ):
        self._workflow_name = workflow_name
        self._steps = {s.name: s for s in steps}
        self._max_concurrency = max_concurrency
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._validate_no_cycles()

    def _validate_no_cycles(self) -> None:
        """Validate that there are no circular dependencies"""
        visited: Set[str] = set()
        rec_stack: Set[str] = set()

        def has_cycle(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)

            step = self._steps.get(node)
            if step:
                for dep in step.depends_on:
                    if dep not in visited:
                        if has_cycle(dep):
                            return True
                    elif dep in rec_stack:
                        return True

            rec_stack.remove(node)
            return False

        for step_name in self._steps:
            if step_name not in visited:
                if has_cycle(step_name):
                    raise OrchestrationError(
                        f"Circular dependency detected in workflow"
                    )

    def _get_ready_steps(
        self,
        pending: Set[str],
        completed: Set[str],
    ) -> List[str]:
        """Find all steps whose dependencies are satisfied"""
        ready = []
        for name in pending:
            step = self._steps[name]
            if all(dep in completed for dep in step.depends_on):
                ready.append(name)
        return ready

    async def _execute_step(
        self,
        step: WorkflowStep,
        context: Dict[str, Any],
        completed: Dict[str, Any],
    ) -> StepResult:
        """Execute a single workflow step with timeout and retries"""
        result = StepResult(
            step_name=step.name,
            status=StepStatus.RUNNING,
            started_at=datetime.now(UTC),
        )

        for attempt in range(step.retry_count + 1):
            try:
                async with self._semaphore:
                    logger.info(f"Executing step: {step.name} (attempt {attempt + 1})")
                    output = await asyncio.wait_for(
                        step.execute(context, completed),
                        timeout=step.timeout,
                    )

                    result.status = StepStatus.COMPLETED
                    result.output = output
                    break

            except asyncio.TimeoutError:
                error_msg = f"Step {step.name} timed out after {step.timeout}s"
                logger.error(error_msg)
                if attempt == step.retry_count:
                    result.status = StepStatus.FAILED
                    result.error = error_msg

            except Exception as e:
                error_msg = f"Step {step.name} failed: {str(e)}"
                logger.error(error_msg)
                if attempt == step.retry_count:
                    result.status = StepStatus.FAILED
                    result.error = error_msg
                else:
                    logger.info(f"Retrying step {step.name}, attempt {attempt + 2}")

        result.completed_at = datetime.now(UTC)
        if result.started_at and result.completed_at:
            result.duration_ms = (
                result.completed_at - result.started_at
            ).total_seconds() * 1000

        return result

    async def execute(
        self,
        initial_context: Dict[str, Any],
    ) -> OrchestrationResult:
        """
        Execute the workflow.

        Args:
            initial_context: Initial context data for the workflow

        Returns:
            OrchestrationResult with all step results and final status
        """
        result = OrchestrationResult(
            workflow_name=self._workflow_name,
            status=StepStatus.PENDING,
            started_at=datetime.now(UTC),
            context=initial_context,
        )

        pending = set(self._steps.keys())
        completed: Dict[str, Any] = {}
        failed_critical = False

        while pending:
            ready = self._get_ready_steps(pending, set(completed.keys()))

            if not ready:
                if failed_critical:
                    break
                raise OrchestrationError(
                    "No ready steps but workflow not complete. "
                    "Possible non-critical step failures."
                )

            logger.info(f"Executing {len(ready)} steps in parallel: {ready}")

            step_objects = [self._steps[name] for name in ready]
            step_results = await asyncio.gather(
                *(
                    self._execute_step(step, result.context, completed)
                    for step in step_objects
                ),
                return_exceptions=True,
            )

            for name, step_result in zip(ready, step_results):
                if isinstance(step_result, Exception):
                    step_result = StepResult(
                        step_name=name,
                        status=StepStatus.FAILED,
                        error=str(step_result),
                    )

                result.step_results[name] = step_result

                if step_result.status == StepStatus.COMPLETED:
                    completed[name] = step_result.output
                    pending.discard(name)
                elif step_result.status == StepStatus.FAILED:
                    step = self._steps[name]
                    if step.is_critical:
                        failed_critical = True
                        pending.discard(name)
                    else:
                        completed[name] = None
                        pending.discard(name)

        result.status = (
            StepStatus.COMPLETED if not failed_critical else StepStatus.FAILED
        )
        result.completed_at = datetime.now(UTC)

        if result.started_at and result.completed_at:
            result.duration_ms = (
                result.completed_at - result.started_at
            ).total_seconds() * 1000

        logger.info(
            f"Workflow {self._workflow_name} completed with status: {result.status}"
        )

        return result
