"""Container CPU/memory sampling and the per-round result (IMPL.md §11-14, §17)."""

import threading
import time
from dataclasses import dataclass

from docker.models.containers import Container

from .config import Host, Round, Scenario


@dataclass(frozen=True)
class Sample:
    at: float  # unix seconds
    cpu_ns: int  # cumulative CPU time of the whole container
    mem_bytes: int


def sample(container: Container) -> Sample:
    """One reading of the container's cgroup counters.

    Memory excludes page cache: that is mostly the runtime's own binary, and
    which container is charged for it depends on who read the file first. What
    remains is anonymous memory plus kernel memory (socket buffers etc).
    """
    t0 = time.time()
    s = container.stats(stream=False, one_shot=True)
    mem = s["memory_stats"]
    cache = mem.get("stats", {}).get("file", mem.get("stats", {}).get("total_cache", 0))  # cgroup v2 / v1
    return Sample(
        at=(t0 + time.time()) / 2,
        cpu_ns=s["cpu_stats"]["cpu_usage"]["total_usage"],
        mem_bytes=max(mem.get("usage", 0) - cache, 0),
    )


class Sampler:
    """Samples a container once a second in the background while in a `with` block."""

    def __init__(self, container: Container, interval: float = 1.0):
        self.container, self.interval = container, interval
        self.samples: list[Sample] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self):
        while not self._stop.is_set():
            try:
                self.samples.append(sample(self.container))
            except Exception:
                pass  # a missed sample only widens the gap between the others
            self._stop.wait(self.interval)

    def __enter__(self) -> "Sampler":
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()


def mb(n: float) -> float:
    return round(n / 2**20, 2)


def build_result(sc: Scenario, rnd: Round, host: Host, loader: dict, baseline: Sample, samples: list[Sample]) -> dict:
    """Merge the loader's client-side view with the server container's resource use."""
    window = loader["window"]
    start, end = window["start_unix_ns"] / 1e9, window["end_unix_ns"] / 1e9
    inside = [s for s in samples if start <= s.at <= end]
    conns, msgs = loader["connections"], loader["messages"]
    mps = loader["throughput"]["messages_per_second"]
    warnings = []

    # CPU: CPU time consumed between the first and last sample inside the window.
    cpu = {"allocated_cores": rnd.cores}
    if len(inside) >= 2:
        first, last = inside[0], inside[-1]
        used = (last.cpu_ns - first.cpu_ns) / 1e9 / (last.at - first.at)
        cpu |= {
            "cpu_time_seconds": round(used * window["seconds"], 3),
            "avg_cores_used": round(used, 3),
            "utilization": round(used / rnd.cores, 4),
        }
    else:
        warnings.append("too_few_resource_samples")

    memory = {"baseline_mb": mb(baseline.mem_bytes)}
    if inside:
        avg = sum(s.mem_bytes for s in inside) / len(inside)
        memory |= {"average_mb": mb(avg), "peak_mb": mb(max(s.mem_bytes for s in inside))}
        if conns["successful"] and avg > baseline.mem_bytes:
            memory["per_connection_kb"] = round((avg - baseline.mem_bytes) / 1024 / conns["successful"], 2)

    if conns["failed"]:
        warnings.append(f"connection_failures={conns['failed']}")
    if msgs["errors"]:
        warnings.append(f"message_errors={msgs['errors']}")
    if loader["loader_cpu_cores"] >= 0.9 * host.loader_core_count:
        warnings.append("loader_saturated")
    if rnd.rate and mps < 0.95 * rnd.rate:
        warnings.append("rate_not_sustained")
    room = loader.get("room")
    if room and room["sends_per_second"] < 0.95 * loader["rate_target"]:
        warnings.append("rate_not_sustained")

    # Only failures that make the numbers meaningless invalidate a run; a
    # saturated loader or an unsustained rate is a finding, not a defect.
    valid = conns["failed"] == 0 and len(inside) >= 2 and (sc.mode == "idle" or msgs["received"] > 0)

    result = {
        "duration_seconds": window["seconds"],
        "throughput": {"messages_per_second": mps},
        "cpu": cpu,
        "memory": memory,
        "errors": {"connections": conns["failed"], "messages": msgs["errors"]},
        "messages": {"sent": msgs["sent"], "received": msgs["received"]},
        "connect_p99_ms": conns["connect_p99_ms"],
        "loader": {"cpus": host.loader_cpus, "cpu_cores_used": loader["loader_cpu_cores"]},
        "valid": valid,
        "warnings": warnings,
    }
    if "latency_ms" in loader:
        result["latency_ms"] = loader["latency_ms"]
    if room:
        result["room"] = room
        result["slo"] = slo(sc, result)
    return result


def slo(sc: Scenario, result: dict) -> dict:
    """Room mode: did this user level hold up? Every user connected, every
    delivery arrived in order, p99 under the limit, the offered rate went out.
    A saturated loader does not fail the level by itself; the report flags it."""
    reasons = []
    if result["errors"]["connections"]:
        reasons.append("connection_failures")
    if result["errors"]["messages"]:
        reasons.append("delivery_errors")
    p99 = result.get("latency_ms", {}).get("p99")
    if p99 is None or p99 > sc.slo_p99_ms:
        reasons.append(f"p99>{sc.slo_p99_ms:g}ms")
    if "rate_not_sustained" in result["warnings"]:
        reasons.append("rate_not_sustained")
    if not result["valid"]:
        reasons.append("invalid")
    return {"pass": not reasons, "p99_limit_ms": sc.slo_p99_ms, "reasons": reasons}
