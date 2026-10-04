"""Session summary: median over valid runs per condition, plus the derived
metrics of IMPL.md §18 (throughput per core, 2c/1c scaling) and, for room
mode, the largest user level that held the SLO."""

import json
import statistics
from pathlib import Path

from .page import write_html

# column -> path into a result record
METRICS = {
    "msg/s": ("throughput", "messages_per_second"),
    "p50": ("latency_ms", "p50"),
    "p95": ("latency_ms", "p95"),
    "p99": ("latency_ms", "p99"),
    "cores": ("cpu", "avg_cores_used"),
    "mem MB": ("memory", "average_mb"),
    "peak MB": ("memory", "peak_mb"),
    "KB/conn": ("memory", "per_connection_kb"),
}


def condition(r: dict) -> tuple:
    return (r["runtime"], r["connections"], r["payload_bytes"], r["rate"], r["cores"])


def lookup(record: dict, path: tuple[str, str]) -> float | None:
    return record.get(path[0], {}).get(path[1])


def summarize(records: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = {}
    for r in records:
        groups.setdefault(condition(r), []).append(r)

    rows = {}
    for key, runs in sorted(groups.items()):
        valid = [r for r in runs if r.get("valid")]
        medians = {}
        for name, path in METRICS.items():
            values = [v for r in valid if (v := lookup(r, path)) is not None]
            if values:
                medians[name] = statistics.median(values)
        runtime, conns, payload, rate, cores = key
        rows[key] = {
            "runtime": runtime, "cores": cores, "connections": conns, "payload_bytes": payload, "rate": rate,
            "runs": len(runs), "valid_runs": len(valid), "median": medians,
            "warnings": sorted({w for r in runs for w in r.get("warnings", [])}),
        }  # fmt: skip
        if any("slo" in r for r in runs):
            passed = sum(1 for r in runs if r.get("slo", {}).get("pass"))
            rows[key] |= {"slo_passed_runs": passed, "slo_pass": passed * 2 > len(runs)}

    for (runtime, conns, payload, rate, cores), row in rows.items():
        m = row["median"]
        if m.get("msg/s") and m.get("cores"):
            row["msg_per_sec_per_core"] = round(m["msg/s"] / m["cores"], 1)
        one_core = rows.get((runtime, conns, payload, rate, 1))
        # Room mode offers a fixed load, so throughput cannot scale with cores.
        if cores > 1 and one_core and "slo_pass" not in row and one_core["median"].get("msg/s") and m.get("msg/s"):
            row["scaling_ratio"] = round(m["msg/s"] / one_core["median"]["msg/s"], 3)
            row["scaling_efficiency"] = round(row["scaling_ratio"] / cores, 3)
    return list(rows.values())


def markdown(rows: list[dict]) -> str:
    columns = ["runtime", "cores", "conns", "payload", "rate", *METRICS, "msg/s/core", "scaling", "runs"]
    lines = ["| " + " | ".join(columns) + " |", "|---|" + "--:|" * (len(columns) - 1)]
    for row in rows:
        m = row["median"]
        metric_cells = [
            "–" if name not in m else f"{m[name]:.0f}" if name == "msg/s" else f"{m[name]:.3f}" for name in METRICS
        ]
        scaling = (
            f"{row['scaling_ratio']:.2f}x ({row['scaling_efficiency']:.0%})" if "scaling_ratio" in row else "–"
        )
        per_core = f"{row['msg_per_sec_per_core']:.0f}" if "msg_per_sec_per_core" in row else "–"
        cells = [
            row["runtime"], row["cores"], row["connections"], row["payload_bytes"], row["rate"] or "–",
            *metric_cells, per_core, scaling, f"{row['valid_runs']}/{row['runs']}",
        ]  # fmt: skip
        lines.append("| " + " | ".join(map(str, cells)) + " |")
    return "Median over valid runs.\n\n" + "\n".join(lines) + "\n"


def capacity(rows: list[dict]) -> list[dict]:
    """Room mode: per runtime and core count, the highest user level that held
    the SLO in most runs, counting up from the lowest level until one fails."""
    groups: dict[tuple, list[dict]] = {}
    for row in rows:
        if "slo_pass" in row:
            groups.setdefault((row["runtime"], row["cores"], row["payload_bytes"]), []).append(row)
    out = []
    for (runtime, cores, payload), levels in sorted(groups.items()):
        best, failed_at = None, None
        for row in sorted(levels, key=lambda r: r["connections"]):
            if not row["slo_pass"]:
                failed_at = row
                break
            best = row
        out.append({
            "runtime": runtime, "cores": cores, "payload_bytes": payload,
            "max_users": best["connections"] if best else 0,
            "failed_at": failed_at["connections"] if failed_at else None,
            # A failure while the loader was saturated may be the loader's limit, not the server's.
            "loader_limited": bool(failed_at and "loader_saturated" in failed_at["warnings"]),
        })  # fmt: skip
    return out


def capacity_markdown(cap: list[dict]) -> str:
    lines = ["| runtime | cores | payload | max users | failed at |", "|---|--:|--:|--:|--:|"]
    for c in cap:
        failed = "–" if c["failed_at"] is None else f"{c['failed_at']}" + (" (loader saturated)" if c["loader_limited"] else "")
        lines.append(f"| {c['runtime']} | {c['cores']} | {c['payload_bytes']} | {c['max_users']} | {failed} |")
    return "\nCapacity: the highest user level that held the SLO in most runs.\n\n" + "\n".join(lines) + "\n"


def write_report(session: Path) -> str:
    records = [json.loads(line) for line in (session / "results.jsonl").read_text().splitlines() if line.strip()]
    rows = summarize(records)
    md = markdown(rows)
    if cap := capacity(rows):
        md += capacity_markdown(cap)
    (session / "summary.json").write_text(json.dumps(rows, indent=2))
    (session / "summary.md").write_text(md)
    write_html(session, rows, cap)
    return md
