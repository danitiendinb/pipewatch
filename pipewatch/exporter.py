"""Export pipeline state snapshots to JSON or CSV formats."""
from __future__ import annotations

import csv
import json
import io
from typing import List, Dict, Any

from pipewatch.state import PipelineState, PipelineRun
from pipewatch.reporter import pipeline_status


def run_to_dict(pipeline: str, run: PipelineRun) -> Dict[str, Any]:
    return {
        "pipeline": pipeline,
        "run_id": run.run_id,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "success": run.success,
        "duration_seconds": run.duration_seconds,
        "message": run.message or "",
    }


def state_to_records(states: Dict[str, PipelineState]) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for name, state in states.items():
        for run in state.runs:
            records.append(run_to_dict(name, run))
    return records


def export_json(states: Dict[str, PipelineState]) -> str:
    summary = []
    for name, state in states.items():
        summary.append({
            "pipeline": name,
            "status": pipeline_status(state),
            "consecutive_failures": state.consecutive_failures,
            "last_run": state.last_run.finished_at if state.last_run else None,
            "runs": [run_to_dict(name, r) for r in state.runs],
        })
    return json.dumps(summary, indent=2)


def export_csv(states: Dict[str, PipelineState]) -> str:
    records = state_to_records(states)
    if not records:
        return ""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(records[0].keys()))
    writer.writeheader()
    writer.writerows(records)
    return output.getvalue()
