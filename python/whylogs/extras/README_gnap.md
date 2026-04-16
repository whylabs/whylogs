# whylogs × GNAP — Multi-Agent Coordination Telemetry

Resolves [whylabs/whylogs#1601](https://github.com/whylabs/whylogs/issues/1601).

Reads a [GNAP](https://github.com/farol-team/gnap) (Git-Native Agent Protocol) repository
and logs coordination events as [whylogs](https://github.com/whylabs/whylogs) statistical
profiles — so you can monitor multi-agent system health alongside model output telemetry.

---

## Installation

```bash
pip install whylogs
```

`gnap_logger.py` is a single-file module, no extra dependencies.

---

## Quick start

```python
from gnap_logger import GNAPLogger

logger = GNAPLogger(gnap_dir=".gnap")

# One-shot profile
view = logger.log_snapshot()

# Save to disk
logger.log_snapshot_to_file("gnap_profile.bin")

# Continuous polling (matches default GNAP heartbeat)
logger.watch(interval_sec=300)
```

### Or log rows manually (exact snippet from the issue)

```python
import whylogs as why

with why.log([{
    "gnap.task.wait_time":      45.2,
    "gnap.task.execution_time": 120.5,
    "gnap.agent.id":            "research-agent-1",
    "gnap.task.type":           "document-analysis",
    "gnap.result.success":      1,
    "gnap.result.token_count":  1847,
}]) as result:
    view = result.view()
```

`GNAPLogger` automates exactly this from your `.gnap/` directory.

---

## Metric schema

One whylogs **row** is produced per **(task, run) pair**. This lets whylogs capture
retry distributions naturally — a task with 3 attempts generates 3 rows.

### Core metrics (from the issue)

| Column | Type | Source |
|---|---|---|
| `gnap.task.wait_time` | float (seconds) | `task.created_at` → `run.started_at` |
| `gnap.task.execution_time` | float (seconds) | `run.started_at` → `run.finished_at` |
| `gnap.task.type` | string | `task.tags[0]` (falls back to `task.title`) |
| `gnap.agent.id` | string | `run.agent` |
| `gnap.result.success` | int 0/1 | `run.state == "completed"` |
| `gnap.result.token_count` | int | `run.tokens.input + run.tokens.output` |

### Additional metrics

| Column | Type | Source |
|---|---|---|
| `gnap.task.id` | string | `task.id` |
| `gnap.task.state` | string | `task.state` |
| `gnap.task.blocked` | int 0/1 | `task.blocked` |
| `gnap.task.priority` | int | `task.priority` |
| `gnap.task.attempt` | int | `run.attempt` |
| `gnap.task.cost_usd` | float | `run.cost_usd` |
| `gnap.agent.run_id` | string | `run.id` |
| `gnap.result.failed` | int 0/1 | `run.state == "failed"` |
| `gnap.result.tokens.input` | int | `run.tokens.input` |
| `gnap.result.tokens.output` | int | `run.tokens.output` |

---

## What whylogs gives you over time

Because whylogs profiles are mergeable and diff-able, you get:

- **Coordination drift** — `gnap.task.execution_time` mean rising week-over-week? Alert on it.
- **Bottleneck detection** — `gnap.task.wait_time` in `ready` tasks growing → agents under-provisioned.
- **Failure rate trends** — rolling mean of `gnap.result.failed` increasing for a specific agent.
- **Cost anomalies** — `gnap.task.cost_usd` suddenly spikes for one task type.
- **Workload skew** — `gnap.agent.id` distribution shows one agent doing 90% of runs.

Combine with whylogs model telemetry (hallucination scores, output distributions) for
**end-to-end observability**: coordination health + model output quality in one stack.

---

## Uploading to WhyLabs (optional)

```python
from whylogs.api.writer.whylabs import WhyLabsWriter

view = logger.log_snapshot()
WhyLabsWriter().write(file=view)
```

Set environment variables: `WHYLABS_API_KEY`, `WHYLABS_DEFAULT_ORG_ID`, `WHYLABS_DEFAULT_DATASET_ID`.

---

## Running the example

```bash
python example_usage.py
```

Demonstrates both manual row logging (matching the issue snippet exactly) and
`GNAPLogger` reading a generated `.gnap/` fixture.

---

## Notes on `gnap.task.type`

GNAP v4 has no dedicated `type` field on tasks. The idiomatic equivalent is
`task.tags` — e.g. `"tags": ["document-analysis"]`. `GNAPLogger` reads
`tags[0]` as the task type, falling back to the task `title` if no tags are set.
When creating tasks in GNAP, set a `tags` entry to get clean `gnap.task.type`
values in your profiles.
