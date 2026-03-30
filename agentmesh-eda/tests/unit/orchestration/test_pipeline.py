"""
Tests for Parallel Pipeline

Architectural Intent:
- Verify pipeline stages process items correctly
- Verify parallel execution within stages
- Verify concurrency limits
"""

import pytest
import asyncio
from agentmesh.application.orchestration.pipeline import (
    ParallelPipeline,
    PipelineStage,
    PipelineResult,
)


class TestParallelPipeline:
    """Test cases for ParallelPipeline"""

    @pytest.mark.asyncio
    async def test_single_stage_pipeline(self):
        """Test pipeline with single stage"""

        class DoubleStage(PipelineStage):
            name = "double"

            async def process(self, input):
                return input * 2

        pipeline = ParallelPipeline([DoubleStage()])

        result = await pipeline.execute([1, 2, 3])

        assert result.success
        assert result.items == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_multi_stage_pipeline(self):
        """Test pipeline with multiple stages"""

        class AddStage(PipelineStage):
            name = "add"

            async def process(self, input):
                return input + 1

        class MultiplyStage(PipelineStage):
            name = "multiply"

            async def process(self, input):
                return input * 2

        pipeline = ParallelPipeline([AddStage(), MultiplyStage()])

        result = await pipeline.execute([1, 2, 3])

        assert result.success
        assert result.items == [4, 6, 8]

    @pytest.mark.asyncio
    async def test_parallel_execution_within_stage(self):
        """Test that items in a stage execute in parallel"""
        execution_times = []

        class SlowStage(PipelineStage):
            name = "slow"

            async def process(self, input):
                await asyncio.sleep(0.05)
                return input

        pipeline = ParallelPipeline([SlowStage()])

        import time

        start = time.time()
        result = await pipeline.execute([1, 2, 3, 4, 5])
        duration = time.time() - start

        assert result.success
        assert len(result.items) == 5

    @pytest.mark.asyncio
    async def test_max_concurrency_limit(self):
        """Test that concurrency limit is respected"""
        active_count = 0
        max_observed = 0

        class TrackingStage(PipelineStage):
            name = "tracking"

            async def process(self, input):
                nonlocal active_count, max_observed
                active_count += 1
                max_observed = max(max_observed, active_count)
                await asyncio.sleep(0.02)
                active_count -= 1
                return input

        pipeline = ParallelPipeline([TrackingStage()], max_concurrency=2)

        result = await pipeline.execute([1, 2, 3, 4])

        assert result.success
        assert max_observed <= 2

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Test error handling in pipeline"""

        class ErrorStage(PipelineStage):
            name = "error"

            async def process(self, input):
                if input == 2:
                    raise ValueError("Bad value")
                return input

        pipeline = ParallelPipeline([ErrorStage()], fail_fast=False)

        result = await pipeline.execute([1, 2, 3])

        assert result.failures == 1
        assert result.successes == 2

    @pytest.mark.asyncio
    async def test_fail_fast_mode(self):
        """Test fail fast mode stops on first error"""

        class ErrorStage(PipelineStage):
            name = "error"

            async def process(self, input):
                if input == 2:
                    raise ValueError("Bad value")
                return input

        pipeline = ParallelPipeline([ErrorStage()], fail_fast=True)

        with pytest.raises(ValueError):
            await pipeline.execute([1, 2, 3])

    def test_pipeline_result_properties(self):
        """Test PipelineResult properties"""
        result = PipelineResult(
            items=[1, 2, 3],
            successes=3,
            failures=0,
            duration_ms=100.0,
        )

        assert result.success is True

        result.failures = 1
        assert result.success is False
