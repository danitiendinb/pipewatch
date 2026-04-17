"""CLI sub-commands: annotate, note-remove, notes."""
from __future__ import annotations

import argparse
import sys

from pipewatch.annotator import get_note, remove_note, set_note, annotated_runs
from pipewatch.config import load_config


def add_annotate_subparser(sub: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p_set = sub.add_parser("annotate", help="Attach a note to a pipeline run")
    p_set.add_argument("pipeline")
    p_set.add_argument("run_id")
    p_set.add_argument("text")
    p_set.set_defaults(func=cmd_annotate)

    p_rm = sub.add_parser("note-remove", help="Remove a note from a pipeline run")
    p_rm.add_argument("pipeline")
    p_rm.add_argument("run_id")
    p_rm.set_defaults(func=cmd_note_remove)

    p_ls = sub.add_parser("notes", help="List all notes for a pipeline")
    p_ls.add_argument("pipeline")
    p_ls.set_defaults(func=cmd_notes)


def cmd_annotate(args: argparse.Namespace, cfg_path: str) -> int:
    cfg = load_config(cfg_path)
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1
    set_note(cfg.state_dir, args.pipeline, args.run_id, args.text)
    print(f"Note set for {args.pipeline}/{args.run_id}")
    return 0


def cmd_note_remove(args: argparse.Namespace, cfg_path: str) -> int:
    cfg = load_config(cfg_path)
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1
    removed = remove_note(cfg.state_dir, args.pipeline, args.run_id)
    if removed:
        print(f"Note removed for {args.pipeline}/{args.run_id}")
    else:
        print(f"No note found for {args.pipeline}/{args.run_id}")
    return 0


def cmd_notes(args: argparse.Namespace, cfg_path: str) -> int:
    cfg = load_config(cfg_path)
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1
    notes = annotated_runs(cfg.state_dir, args.pipeline)
    if not notes:
        print(f"No notes for {args.pipeline}")
        return 0
    for run_id, text in notes.items():
        print(f"  {run_id}: {text}")
    return 0
