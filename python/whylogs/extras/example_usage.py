"""
example_usage.py
================
Demonstrates the whylogs × GNAP integration from whylogs#1601.

Shows two patterns:
  1. The exact metric shape proposed in the issue (manual row logging)
  2. GNAPLogger reading a real .gnap/ directory tree

Run with:
    pip install whylogs
    python example_usage.py
"""

# ── Pattern 1: Exact snippet from the issue ───────────────────────────────────
# This is what the issue proposes — log coordination events as whylogs rows.
# GNAPLogger automates exactly this from your .gnap/ directory.

import whylogs as why

def demo_manual_row():
    print("── Pattern 1: manual row (matches issue snippet) ──────────────")

    with why.log([
        {
            "gnap.task.wait_time":      45.2,           # seconds in backlog/ready
            "gnap.task.execution_time": 120.5,          # seconds to complete
            "gnap.agent.id":            "research-agent-1",
            "gnap.task.type":           "document-analysis",
            "gnap.result.success":      1,
            "gnap.result.token_count":  1847,
        },
        {
            "gnap.task.wait_time":      12.0,
            "gnap.task.execution_time": 340.1,
            "gnap.agent.id":            "research-agent-2",
            "gnap.task.type":           "document-analysis",
            "gnap.result.success":      0,              # failed run
            "gnap.result.token_count":  922,
        },
    ]) as result:
        view = result.view()

    print("Columns logged:", sorted(view.get_columns().keys()))
    return view


# ── Pattern 2: GNAPLogger reading a .gnap/ directory ─────────────────────────

import json
import tempfile
from pathlib import Path
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from gnap_logger import GNAPLogger


def make_gnap_fixture(base: Path) -> Path:
    """Create a minimal GNAP v4 fixture for the demo."""
    gnap = base / ".gnap"
    (gnap / "tasks").mkdir(parents=True)
    (gnap / "runs").mkdir()
    (gnap / "version").write_text("4")

    (gnap / "agents.json").write_text(json.dumps({"agents": [
        {"id": "research-agent-1", "name": "Research Agent 1",
         "role": "researcher", "type": "ai", "status": "active"},
        {"id": "research-agent-2", "name": "Research Agent 2",
         "role": "researcher", "type": "ai", "status": "active"},
    ]}))

    tasks = [
        {
            "id": "FA-1",
            "title": "Analyse Q1 earnings report",
            "tags": ["document-analysis"],          # → gnap.task.type
            "assigned_to": ["research-agent-1"],
            "state": "done",
            "priority": 0,
            "created_by": "ori",
            "created_at": "2026-03-12T11:40:00Z",
        },
        {
            "id": "FA-2",
            "title": "Summarise competitor landscape",
            "tags": ["document-analysis"],
            "assigned_to": ["research-agent-2"],
            "state": "in_progress",
            "priority": 1,
            "created_by": "ori",
            "created_at": "2026-03-13T09:00:00Z",
        },
        {
            "id": "FA-3",
            "title": "Draft investor update",
            "tags": ["writing"],
            "assigned_to": ["research-agent-1"],
            "state": "blocked",
            "blocked": True,
            "blocked_reason": "waiting for FA-1 and FA-2",
            "priority": 0,
            "created_by": "leo",
            "created_at": "2026-03-14T08:00:00Z",
        },
    ]
    for t in tasks:
        (gnap / "tasks" / f"{t['id']}.json").write_text(json.dumps(t))

    runs = [
        {
            "id": "FA-1-1", "task": "FA-1", "agent": "research-agent-1",
            "state": "failed", "attempt": 1,
            "started_at": "2026-03-12T11:40:45Z",   # 45s wait_time
            "finished_at": "2026-03-12T11:44:45Z",  # 240s execution
            "tokens": {"input": 900, "output": 200},
            "cost_usd": 0.02,
            "error": "context limit hit",
        },
        {
            "id": "FA-1-2", "task": "FA-1", "agent": "research-agent-1",
            "state": "completed", "attempt": 2,
            "started_at": "2026-03-12T11:44:50Z",
            "finished_at": "2026-03-12T11:46:50Z",  # 120s execution
            "tokens": {"input": 1200, "output": 647},
            "cost_usd": 0.04,
            "result": "Analysis complete. Key findings attached.",
        },
        {
            "id": "FA-2-1", "task": "FA-2", "agent": "research-agent-2",
            "state": "running", "attempt": 1,
            "started_at": "2026-03-13T09:00:12Z",
            "tokens": {"input": 500, "output": 0},
            "cost_usd": 0.01,
        },
    ]
    for r in runs:
        (gnap / "runs" / f"{r['id']}.json").write_text(json.dumps(r))

    return gnap


def demo_gnap_logger():
    print("\n── Pattern 2: GNAPLogger from .gnap/ directory ────────────────")

    with tempfile.TemporaryDirectory() as tmp:
        gnap_path = make_gnap_fixture(Path(tmp))
        logger = GNAPLogger(gnap_dir=str(gnap_path))

        view = logger.log_snapshot()

        print("\nMetrics per column:")
        for col_name in sorted(view.get_columns().keys()):
            col = view.get_column(col_name)
            dist = col.get_metric("distribution")
            counts = col.get_metric("counts")

            n = counts.n.value if counts else "?"
            if dist and hasattr(dist, "mean"):
                mean = dist.mean.value
                print(f"  {col_name:<45}  n={n}  mean={mean:.2f}")
            else:
                print(f"  {col_name:<45}  n={n}")

        # The key metrics the issue cares about
        print("\nCore coordination metrics (issue#1601 namespace):")
        core = [
            "gnap.task.wait_time",
            "gnap.task.execution_time",
            "gnap.agent.id",
            "gnap.task.type",
            "gnap.result.success",
            "gnap.result.token_count",
        ]
        for key in core:
            col = view.get_column(key)
            if col:
                print(f"  ✓ {key}")
            else:
                print(f"  ✗ {key} — MISSING")


if __name__ == "__main__":
    demo_manual_row()
    demo_gnap_logger()
    print("\nDone.")
