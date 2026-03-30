"""
Parallel Pipeline

Architectural Intent:
- Process items through sequential stages
- Parallelize within each stage
- Apply backpressure via semaphore

Parallelization Notes:
- All items in a stage run concurrently
- Max concurrency limits prevent resource exhaustion
- Results flow to next stage as input
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TypeVar, Generic, List, Any, Callable, Optional
import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

T_In = TypeVar("T_In")
T_Out = TypeVar("T_Out")
T_StageIn = TypeVar("T_StageIn")
T_StageOut = TypeVar("T_StageOut")


@dataclass
class PipelineResult(Generic[T_Out]):
    """Result of pipeline execution"""

    items: List[T_Out]
    successes: int = 0
    failures: int = 0
    duration_ms: float = 0.0
    errors: List[dict] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.failures == 0


class PipelineStage(Generic[T_StageIn, T_StageOut], ABC):
    """
    Abstract base for pipeline stages.

    Each stage processes input and produces output.
    Stages are composed into pipelines.
    """

    @abstractmethod
    async def process(self, input: T_StageIn) -> T_StageOut:
        """Process a single input item"""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Stage name for logging"""
        pass


class ParallelPipeline(Generic[T_In, T_Out]):
    """
    Processes items through stages, parallelizing within each stage.

    Example:
        pipeline = ParallelPipeline([
            FetchStage(),
            TransformStage(),
            SaveStage(),
        ], max_concurrency=10)

        results = await pipeline.execute(items)
    """

    def __init__(
        self,
        stages: List[PipelineStage],
        max_concurrency: int = 10,
        fail_fast: bool = False,
    ):
        self._stages = stages
        self._max_concurrency = max_concurrency
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._fail_fast = fail_fast

    @property
    def stages(self) -> List[PipelineStage]:
        return self._stages

    async def _process_with_limit(
        self,
        stage: PipelineStage,
        item: Any,
    ) -> Any:
        """Process item with concurrency limit"""
        async with self._semaphore:
            try:
                return await stage.process(item)
            except Exception as e:
                logger.error(f"Stage {stage.name} failed for item: {e}")
                if self._fail_fast:
                    raise
                return {"error": str(e), "item": item}

    async def execute(
        self,
        items: List[T_In],
    ) -> PipelineResult[T_Out]:
        """
        Execute pipeline on all items.

        Each stage processes all items in parallel,
        then passes results to the next stage.
        """
        start_time = datetime.utcnow()
        current_items = items
        errors: List[dict] = []

        for stage in self._stages:
            logger.info(f"Executing stage {stage.name} on {len(current_items)} items")

            results = await asyncio.gather(
                *(self._process_with_limit(stage, item) for item in current_items),
                return_exceptions=True,
            )

            processed_items = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    errors.append(
                        {
                            "stage": stage.name,
                            "index": i,
                            "error": str(result),
                        }
                    )
                    if self._fail_fast:
                        raise result
                elif isinstance(result, dict) and "error" in result:
                    errors.append(result)
                else:
                    processed_items.append(result)

            current_items = processed_items

            if not current_items:
                logger.warning(f"Stage {stage.name} produced no results")
                break

        duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000

        return PipelineResult(
            items=current_items,
            successes=len(current_items),
            failures=len(errors),
            duration_ms=duration_ms,
            errors=errors,
        )


class FanOutPipeline(Generic[T_In, T_Out]):
    """
    Fan-out: Process single input through multiple parallel handlers.

    Example:
        # Enrich customer data from multiple sources concurrently
        pipeline = FanOutPipeline([
            fetch_credit_score,
            fetch_order_history,
            fetch_preferences,
        ])

        enriched = await pipeline.execute(customer_id)
    """

    def __init__(
        self,
        handlers: List[Callable[[T_In], T_Out]],
        max_concurrency: int = 10,
    ):
        self._handlers = handlers
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def execute(self, input: T_In) -> List[T_Out]:
        """Execute all handlers in parallel on the same input"""

        async def run_handler(handler):
            async with self._semaphore:
                return await handler(input)

        results = await asyncio.gather(
            *(run_handler(h) for h in self._handlers),
            return_exceptions=True,
        )

        return [r for r in results if not isinstance(r, Exception)]


class MapReducePipeline(Generic[T_In, T_Out]):
    """
    Map-Reduce: Parallel map phase, then reduce phase.

    Example:
        pipeline = MapReducePipeline(
            map_func=process_item,
            reduce_func=aggregate_results,
            max_concurrency=20,
        )

        result = await pipeline.execute(items)
    """

    def __init__(
        self,
        map_func: Callable[[T_In], T_Out],
        reduce_func: Callable[[List[T_Out]], T_Out],
        max_concurrency: int = 10,
    ):
        self._map_func = map_func
        self._reduce_func = reduce_func
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def _map_item(self, item: T_In) -> T_Out:
        """Map single item with concurrency limit"""
        async with self._semaphore:
            return await self._map_func(item)

    async def execute(self, items: List[T_In]) -> T_Out:
        """Execute map phase in parallel, then reduce"""
        map_results = await asyncio.gather(
            *(self._map_item(item) for item in items),
            return_exceptions=True,
        )

        valid_results = [r for r in map_results if not isinstance(r, Exception)]

        if not valid_results:
            raise ValueError("No successful map results")

        return await self._reduce_func(valid_results)
