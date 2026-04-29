"""Filter pipelines by tag key/value pairs stored via the tagger module."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pipewatch.tagger import load_tags


@dataclass
class TagFilterResult:
    pipeline: str
    tags: Dict[str, str]
    matched: bool


def _tags_match(tags: Dict[str, str], criteria: Dict[str, str]) -> bool:
    """Return True when all criteria key/value pairs are present in tags."""
    return all(tags.get(k) == v for k, v in criteria.items())


def filter_by_tags(
    pipeline_names: List[str],
    state_dir: str,
    criteria: Dict[str, str],
) -> List[TagFilterResult]:
    """Return a TagFilterResult for every pipeline, marking those that match criteria."""
    results: List[TagFilterResult] = []
    for name in pipeline_names:
        tags = load_tags(state_dir, name)
        results.append(
            TagFilterResult(
                pipeline=name,
                tags=tags,
                matched=_tags_match(tags, criteria),
            )
        )
    return results


def matching_pipelines(
    pipeline_names: List[str],
    state_dir: str,
    criteria: Dict[str, str],
) -> List[str]:
    """Return only the names of pipelines whose tags satisfy all criteria."""
    return [
        r.pipeline
        for r in filter_by_tags(pipeline_names, state_dir, criteria)
        if r.matched
    ]


def format_filter_row(result: TagFilterResult) -> str:
    """Format a single TagFilterResult for CLI display."""
    tag_str = ", ".join(f"{k}={v}" for k, v in sorted(result.tags.items())) or "(none)"
    mark = "✓" if result.matched else " "
    return f"[{mark}] {result.pipeline:<30} {tag_str}"
