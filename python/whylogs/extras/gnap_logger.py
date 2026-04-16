"""
whylogs integration for GNAP (Git-Native Agent Protocol)
=========================================================

Resolves: https://github.com/whylabs/whylogs/issues/1601

Logs GNAP coordination events as whylogs statistical profiles so you can
monitor multi-agent system health alongside model telemetry.

The metric namespace matches the proposal in the issue exactly:

    gnap.task.wait_time       — seconds from task creation → first run start
    gnap.task.execution_time  — seconds from run start → run finish
    gnap.task.type            — free-form tag on the task (e.g. "document-analysis")
    gnap.agent.id             — agent who executed the run
    gnap.result.success       — 1 if the run completed, 0 otherwise
    gnap.result.token_count   — total tokens consumed (input + output)

Additional metrics derived from the GNAP v4 schema:

    gnap.task.id              — task identifier
    gnap.task.state           — backlog / ready / in_progress / review / done / blocked / cancelled
    gnap.task.attempt         — which attempt number this run is
    gnap.task.cost_usd        — cost of this run in USD
    gnap.task.blocked         — 1 if the task is currently blocked
    gnap.agent.run_id         — run identifier
    gnap.result.failed        — 1 if the run failed
    gnap.result.tokens.input  — input token count
    gnap.result.tokens.output — output token count

Usage
-----
    from gnap_logger import GNAPLogger

    logger = GNAPLogger(gnap_dir=".gnap")

    # One-shot: log current state and return a DatasetProfileView
    view = logger.log_snapshot()

    # Save profile to disk
    logger.log_snapshot_to_file("gnap_profile.bin")

    # Continuous polling — matches the default GNAP heartbeat interval
    logger.watch(interval_sec=300)

Uploading to WhyLabs
--------------------
    from whylogs.api.writer.whylabs import WhyLabsWriter
    WhyLabsWriter().write(file=logger.log_snapshot())
    # Requires env vars: WHYLABS_API_KEY, WHYLABS_DEFAULT_ORG_ID, WHYLABS_DEFAULT_DATASET_ID
"""

import json
import time
import datetime
from pathlib import Path
from typing import Optional

import whylogs as why
from whylogs.core import DatasetProfileView


class GNAPLogger:
    """
    Reads a GNAP v4 repository (.gnap/) and emits whylogs DatasetProfiles.

    One whylogs *row* is produced per (task, run) pair — mirroring the
    unit of work GNAP tracks. This means each row carries both task-level
    metadata (type, state, wait_time) and run-level results (agent,
    execution_time, token_count, success).

    Multiple runs on the same task produce multiple rows, letting whylogs
    capture retry distributions naturally.
    """

    def __init__(self, gnap_dir: str = ".gnap", dataset_name: str = "gnap"):
        self.gnap_dir = Path(gnap_dir)
        self.dataset_name = dataset_name
        self._validate_gnap_dir()

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def log_snapshot(self) -> DatasetProfileView:
        """
        Scan the GNAP repo, build whylogs rows, and return a profile view.

        Each row corresponds to a single run attempt and carries the metric
        names proposed in whylogs#1601.
        """
        rows = self._build_rows()
        if not rows:
            raise ValueError(
                f"No loggable GNAP data found in {self.gnap_dir}. "
                "Make sure tasks/ and runs/ subdirectories exist and are non-empty."
            )

        with why.log(rows) as result:
            view = result.view()

        print(f"[gnap_logger] Logged {len(rows)} row(s) from {self.gnap_dir}")
        return view

    def log_snapshot_to_file(self, output_path: str) -> DatasetProfileView:
        """Log a snapshot and write the profile binary to *output_path*."""
        view = self.log_snapshot()
        view.write(output_path)
        print(f"[gnap_logger] Profile written → {output_path}")
        return view

    def watch(self, interval_sec: int = 300):
        """
        Poll GNAP state on a fixed interval, emitting a profile each cycle.

        Runs until interrupted (Ctrl-C). The default interval (300 s) matches
        GNAP's default agent heartbeat so each poll reflects a fresh agent cycle.
        """
        print(
            f"[gnap_logger] Watching {self.gnap_dir} "
            f"every {interval_sec}s (Ctrl-C to stop) …"
        )
        while True:
            try:
                self.log_snapshot()
            except Exception as exc:  # keep the loop alive on transient errors
                print(f"[gnap_logger] WARNING: {exc}")
            time.sleep(interval_sec)

    # ------------------------------------------------------------------ #
    # Row assembly                                                         #
    # ------------------------------------------------------------------ #

    def _build_rows(self) -> list[dict]:
        """
        One row per (task, run) pair.

        For tasks that have no runs yet (e.g. state=ready/backlog), we emit
        one row with run-level fields absent so task-level metrics (wait_time,
        state, type) are still captured.
        """
        tasks = self._load_tasks()
        runs_by_task = self._load_runs_by_task()

        rows: list[dict] = []
        for task_id, task in tasks.items():
            task_runs = runs_by_task.get(task_id, [])

            if task_runs:
                for run in task_runs:
                    rows.append(self._make_row(task, run))
            else:
                # Task exists but no run yet — log task-level metrics only
                rows.append(self._make_row(task, run=None))

        return rows

    def _make_row(self, task: dict, run: Optional[dict]) -> dict:
        """
        Build a single whylogs row from a task + optional run.

        Metric names match the proposal in whylogs#1601 exactly.
        Additional metrics provide deeper observability from the v4 schema.

        gnap.task.type  — sourced from task["tags"][0] if present, else task["title"].
                          In GNAP v4 there is no dedicated "type" field; tags are the
                          idiomatic way to classify tasks (e.g. "document-analysis").
        gnap.task.wait_time — seconds from task.created_at to first run.started_at.
                              For tasks still waiting, measures elapsed wait so far.
        gnap.task.execution_time — seconds from run.started_at to run.finished_at.
                                   For runs still in progress, measures elapsed time.
        """
        # ── Task-level fields ──────────────────────────────────────────
        tags = task.get("tags", [])
        task_type = tags[0] if tags else task.get("title", "")

        row: dict = {
            # ── Core proposed metrics ──────────────────────────────────
            "gnap.task.type":    task_type,

            # ── Additional task metadata ───────────────────────────────
            "gnap.task.id":      task.get("id", ""),
            "gnap.task.state":   task.get("state", ""),
            "gnap.task.blocked": int(bool(task.get("blocked", False))),
            "gnap.task.priority": int(task.get("priority", 0)),
        }

        # wait_time: task.created_at → first run.started_at
        created_at   = _parse_iso(task.get("created_at"))
        first_started = _parse_iso(run.get("started_at")) if run else None

        if created_at and first_started:
            row["gnap.task.wait_time"] = (first_started - created_at).total_seconds()
        elif created_at:
            row["gnap.task.wait_time"] = _seconds_since(created_at)  # still waiting

        # ── Run-level fields ───────────────────────────────────────────
        if run:
            started  = _parse_iso(run.get("started_at"))
            finished = _parse_iso(run.get("finished_at"))

            tokens = run.get("tokens", {})
            token_input  = int(tokens.get("input",  0))
            token_output = int(tokens.get("output", 0))

            row.update({
                # ── Core proposed metrics ──────────────────────────────
                "gnap.agent.id":            run.get("agent", ""),
                "gnap.result.success":      int(run.get("state") == "completed"),
                "gnap.result.token_count":  token_input + token_output,

                # ── Additional run metadata ────────────────────────────
                "gnap.agent.run_id":         run.get("id", ""),
                "gnap.task.attempt":         int(run.get("attempt", 1)),
                "gnap.task.cost_usd":        float(run.get("cost_usd", 0.0)),
                "gnap.result.failed":        int(run.get("state") == "failed"),
                "gnap.result.tokens.input":  token_input,
                "gnap.result.tokens.output": token_output,
            })

            # execution_time: run.started_at → run.finished_at
            if started and finished:
                row["gnap.task.execution_time"] = (finished - started).total_seconds()
            elif started:
                row["gnap.task.execution_time"] = _seconds_since(started)  # still running

        return row

    # ------------------------------------------------------------------ #
    # GNAP file loaders                                                    #
    # ------------------------------------------------------------------ #

    def _load_tasks(self) -> dict[str, dict]:
        tasks_dir = self.gnap_dir / "tasks"
        if not tasks_dir.exists():
            return {}
        tasks: dict[str, dict] = {}
        for path in sorted(tasks_dir.glob("*.json")):
            task = _load_json(path)
            if task and "id" in task:
                tasks[task["id"]] = task
        return tasks

    def _load_runs_by_task(self) -> dict[str, list[dict]]:
        runs_dir = self.gnap_dir / "runs"
        if not runs_dir.exists():
            return {}
        runs_by_task: dict[str, list[dict]] = {}
        for path in sorted(runs_dir.glob("*.json")):
            run = _load_json(path)
            if not run:
                continue
            task_id = run.get("task", "")
            runs_by_task.setdefault(task_id, []).append(run)
        return runs_by_task

    def _validate_gnap_dir(self):
        if not self.gnap_dir.exists():
            raise FileNotFoundError(
                f"GNAP directory not found: {self.gnap_dir}\n"
                "Point gnap_dir at the .gnap/ folder inside your git repo."
            )
        version_file = self.gnap_dir / "version"
        if version_file.exists():
            version = version_file.read_text().strip()
            if int(version) < 4:
                print(
                    f"[gnap_logger] WARNING: GNAP protocol version {version} — "
                    "this integration targets v4+."
                )
            else:
                print(f"[gnap_logger] GNAP protocol version: {version} ✓")


# ------------------------------------------------------------------ #
# Module-level helpers                                                #
# ------------------------------------------------------------------ #

def _load_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        print(f"[gnap_logger] WARNING: could not read {path}: {exc}")
        return None


def _parse_iso(value: Optional[str]) -> Optional[datetime.datetime]:
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _seconds_since(dt: datetime.datetime) -> float:
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    return (now - dt).total_seconds()
