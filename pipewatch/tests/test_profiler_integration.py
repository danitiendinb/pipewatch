"""Integration tests for profiler: compute → save → load round-trip using real state."""

from __future__ import annotations

import pytest

from pipewatch.state import PipelineState
from pipewatch.profiler import compute_profile, save_profile, load_profile


@pytest.fixture()
def store(tmp_path):
    return tmp_path


def _record_run(state_dir, pipeline, started, finished, status="ok"):
    from pipewatch.state import start, finish
    start(str(state_dir), pipeline, run_id="r-" + started[-8:])
    # Manually inject a completed run so we control timestamps
    state = PipelineState.load(str(state_dir), pipeline)
    from pipewatch.state import PipelineRun
    state.runs.append(
        PipelineRun(
            run_id="r-" + started,
            started_at=started,
            finished_at=finished,
            status=status,
            message="",
            consecutive_failures=0,
        )
    )
    state.save(str(state_dir))
    return state


def test_profile_round_trip_preserves_mean(store):
    state = PipelineState(pipeline="etl", runs=[])
    from pipewatch.state import PipelineRun
    for i in range(5):
        state.runs.append(PipelineRun(
            run_id=f"r{i}",
            started_at=f"2024-01-01T0{i}:00:00",
            finished_at=f"2024-01-01T0{i}:02:00",  # 120s each
            status="ok", message="", consecutive_failures=0,
        ))

    profile = compute_profile("etl", state)
    assert profile is not None
    save_profile(str(store), profile)

    loaded = load_profile(str(store), "etl")
    assert loaded is not None
    assert abs(loaded.mean_seconds - 120.0) < 0.1


def test_profile_p95_above_median(store):
    from pipewatch.state import PipelineRun
    state = PipelineState(pipeline="etl", runs=[])
    durations = [60] * 18 + [600, 700]  # 20 runs; spike at end
    for i, d in enumerate(durations):
        state.runs.append(PipelineRun(
            run_id=f"r{i}",
            started_at=f"2024-01-{i+1:02d}T00:00:00",
            finished_at=f"2024-01-{i+1:02d}T00:{d//60:02d}:{d%60:02d}",
            status="ok", message="", consecutive_failures=0,
        ))
    profile = compute_profile("etl", state)
    assert profile is not None
    assert profile.p95_seconds > profile.median_seconds


def test_profile_sample_size_capped_at_window(store):
    from pipewatch.state import PipelineRun
    state = PipelineState(pipeline="etl", runs=[])
    for i in range(60):  # more than _WINDOW=50
        state.runs.append(PipelineRun(
            run_id=f"r{i}",
            started_at=f"2024-01-01T00:00:00",
            finished_at=f"2024-01-01T00:01:00",
            status="ok", message="", consecutive_failures=0,
        ))
    profile = compute_profile("etl", state)
    assert profile is not None
    assert profile.sample_size <= 50
