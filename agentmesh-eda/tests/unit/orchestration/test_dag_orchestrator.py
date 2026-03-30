"""
Tests for DAG Orchestrator

Architectural Intent:
- Verify parallel execution of independent steps
- Verify dependency ordering
- Verify error handling and retry logic
- Verify circular dependency detection
"""

import pytest
import asyncio
from agentmesh.application.orchestration.dag_orchestrator import (
    DAGOrchestrator,
    WorkflowStep,
    OrchestrationResult,
    StepStatus,
    OrchestrationError,
)


class TestDAGOrchestrator:
    """Test cases for DAGOrchestrator"""

    @pytest.mark.asyncio
    async def test_parallel_execution_independent_steps(self):
        """Test that independent steps execute in parallel"""
        execution_order = []

        async def step_a(context, completed):
            execution_order.append("a_start")
            await asyncio.sleep(0.1)
            execution_order.append("a_end")
            return "a_result"

        async def step_b(context, completed):
            execution_order.append("b_start")
            await asyncio.sleep(0.1)
            execution_order.append("b_end")
            return "b_result"

        orchestrator = DAGOrchestrator(
            [
                WorkflowStep("a", step_a),
                WorkflowStep("b", step_b),
            ]
        )

        result = await orchestrator.execute({})

        assert result.success
        assert result.step_results["a"].status == StepStatus.COMPLETED
        assert result.step_results["b"].status == StepStatus.COMPLETED

        a_start_idx = execution_order.index("a_start")
        b_start_idx = execution_order.index("b_start")
        assert abs(a_start_idx - b_start_idx) < 2

    @pytest.mark.asyncio
    async def test_sequential_execution_dependent_steps(self):
        """Test that dependent steps execute sequentially"""
        execution_order = []

        async def step_a(context, completed):
            execution_order.append("a")
            return "a_result"

        async def step_b(context, completed):
            execution_order.append("b")
            return "b_result"

        orchestrator = DAGOrchestrator(
            [
                WorkflowStep("a", step_a),
                WorkflowStep("b", step_b, depends_on=["a"]),
            ]
        )

        result = await orchestrator.execute({})

        assert result.success
        assert execution_order == ["a", "b"]

    @pytest.mark.asyncio
    async def test_complex_dag_parallel_branches(self):
        """Test complex DAG with parallel branches"""
        execution_times = []

        async def step_validate(context, completed):
            await asyncio.sleep(0.05)
            return {"validated": True}

        async def step_check_inventory(context, completed):
            await asyncio.sleep(0.05)
            return {"inventory_checked": True}

        async def step_calculate_pricing(context, completed):
            await asyncio.sleep(0.05)
            return {"pricing_calculated": True}

        async def step_reserve_stock(context, completed):
            return {"stock_reserved": True}

        async def step_process_payment(context, completed):
            return {"payment_processed": True}

        async def step_confirm_order(context, completed):
            return {"order_confirmed": True}

        orchestrator = DAGOrchestrator(
            [
                WorkflowStep("validate", step_validate),
                WorkflowStep("check_inventory", step_check_inventory),
                WorkflowStep("calculate_pricing", step_calculate_pricing),
                WorkflowStep(
                    "reserve_stock",
                    step_reserve_stock,
                    depends_on=["validate", "check_inventory"],
                ),
                WorkflowStep(
                    "process_payment",
                    step_process_payment,
                    depends_on=["validate", "calculate_pricing"],
                ),
                WorkflowStep(
                    "confirm_order",
                    step_confirm_order,
                    depends_on=["reserve_stock", "process_payment"],
                ),
            ]
        )

        result = await orchestrator.execute({})

        assert result.success
        assert result.step_results["validate"].status == StepStatus.COMPLETED
        assert result.step_results["check_inventory"].status == StepStatus.COMPLETED
        assert result.step_results["calculate_pricing"].status == StepStatus.COMPLETED
        assert result.step_results["reserve_stock"].status == StepStatus.COMPLETED
        assert result.step_results["process_payment"].status == StepStatus.COMPLETED
        assert result.step_results["confirm_order"].status == StepStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_step_failure_non_critical(self):
        """Test that non-critical step failure doesn't halt workflow"""

        async def step_a(context, completed):
            return "a_result"

        async def step_b(context, completed):
            raise ValueError("Step B failed")

        async def step_c(context, completed):
            return "c_result"

        orchestrator = DAGOrchestrator(
            [
                WorkflowStep("a", step_a),
                WorkflowStep("b", step_b, is_critical=False),
                WorkflowStep("c", step_c, depends_on=["a"]),
            ]
        )

        result = await orchestrator.execute({})

        assert result.status == StepStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_step_failure_critical_halts_workflow(self):
        """Test that critical step failure halts workflow"""

        async def step_a(context, completed):
            return "a_result"

        async def step_b(context, completed):
            raise ValueError("Step B failed")

        async def step_c(context, completed):
            return "c_result"

        orchestrator = DAGOrchestrator(
            [
                WorkflowStep("a", step_a),
                WorkflowStep("b", step_b, is_critical=True),
                WorkflowStep("c", step_c, depends_on=["b"]),
            ]
        )

        result = await orchestrator.execute({})

        assert result.status == StepStatus.FAILED
        assert result.step_results["b"].status == StepStatus.FAILED

    def test_circular_dependency_detection(self):
        """Test that circular dependencies are detected"""

        def step_a(context, completed):
            return "a"

        def step_b(context, completed):
            return "b"

        with pytest.raises(OrchestrationError, match="Circular dependency"):
            DAGOrchestrator(
                [
                    WorkflowStep("a", step_a, depends_on=["b"]),
                    WorkflowStep("b", step_b, depends_on=["a"]),
                ]
            )

    @pytest.mark.asyncio
    async def test_step_timeout(self):
        """Test that step timeout works"""

        async def slow_step(context, completed):
            await asyncio.sleep(2.0)
            return "done"

        orchestrator = DAGOrchestrator(
            [
                WorkflowStep("slow", slow_step, timeout=0.1),
            ]
        )

        result = await orchestrator.execute({})

        assert result.step_results["slow"].status == StepStatus.FAILED
        assert "timed out" in result.step_results["slow"].error

    @pytest.mark.asyncio
    async def test_step_result_context_passing(self):
        """Test that step results are passed to dependent steps"""

        async def step_a(context, completed):
            return {"value": 42}

        async def step_b(context, completed):
            a_result = completed.get("a", {})
            return {"a_value": a_result.get("value", 0) * 2}

        orchestrator = DAGOrchestrator(
            [
                WorkflowStep("a", step_a),
                WorkflowStep("b", step_b, depends_on=["a"]),
            ]
        )

        result = await orchestrator.execute({})

        assert result.success
        assert result.step_results["b"].output["a_value"] == 84

    @pytest.mark.asyncio
    async def test_max_concurrency_limit(self):
        """Test that max concurrency is respected"""
        active_count = 0
        max_observed = 0

        async def step_task(context, completed):
            nonlocal active_count, max_observed
            active_count += 1
            max_observed = max(max_observed, active_count)
            await asyncio.sleep(0.05)
            active_count -= 1
            return "done"

        orchestrator = DAGOrchestrator(
            [WorkflowStep(f"task_{i}", step_task) for i in range(5)],
            max_concurrency=2,
        )

        result = await orchestrator.execute({})

        assert result.success
        assert max_observed <= 2
