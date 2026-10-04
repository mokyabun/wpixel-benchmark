"""wsbench — benchmark runner.

    wsbench run [--scenario FILE] [--servers go,rust] [--cores 1,2] [--runs 3] ...
    wsbench report <session-dir>

One round (IMPL.md §15):
  start server (cpuset, CORES) -> /health -> stabilize -> baseline memory
  -> loader (warm-up + measurement) while sampling the server every second
  -> merge into one result JSON -> remove server -> cooldown
"""

import argparse
import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path

from . import containers
from .config import Host, Round, Scenario
from .metrics import Sampler, build_result, sample
from .report import write_report

log = logging.getLogger("wsbench")


def run_round(sc: Scenario, rnd: Round, host: Host) -> dict:
    with containers.server(host, rnd) as server:
        time.sleep(sc.stabilize)
        baseline = sample(server)
        with Sampler(server) as sampler:
            loader = containers.run_loader(host, sc, rnd)
        if not containers.is_running(server):
            raise RuntimeError("server died during the run")
        return build_result(sc, rnd, host, loader, baseline, sampler.samples)


def run_session(sc: Scenario, host: Host) -> Path:
    for cores in sc.cores:
        host.server_cpuset(cores)  # fail on a bad layout before anything starts
    implementations = {s: containers.implementation(s) for s in sc.servers}
    containers.remove_leftovers()

    session = host.results_dir / f"{sc.name}-{datetime.now(UTC):%Y%m%d-%H%M%S}"
    (session / "runs").mkdir(parents=True)
    meta = {
        "scenario": sc.to_dict(),
        "host": {"server_cpus": host.server_cpus, "loader_cpus": host.loader_cpus, "port": host.port},
        "implementations": implementations,
        "docker": containers.docker_info(),
        "started_at": datetime.now(UTC).isoformat(),
    }
    (session / "meta.json").write_text(json.dumps(meta, indent=2))

    rounds = sc.plan()
    log.info("session %s: %d rounds", session, len(rounds))
    failed_level: dict[tuple, int] = {}  # room mode: (run, server, cores, payload) -> users that failed the SLO
    with open(session / "results.jsonl", "a") as jsonl:
        for i, rnd in enumerate(rounds, 1):
            key = (rnd.run, rnd.server, rnd.cores, rnd.payload)
            if key in failed_level and rnd.connections > failed_level[key]:
                log.info("[%d/%d] %s  skipped: failed at %d users", i, len(rounds), rnd, failed_level[key])
                continue
            log.info("[%d/%d] %s", i, len(rounds), rnd)
            result = {
                "runtime": rnd.server,
                "implementation": implementations[rnd.server],
                "cores": rnd.cores,
                "scenario": sc.name,
                "mode": sc.mode,
                "connections": rnd.connections,
                "payload_bytes": rnd.payload,
                "rate": rnd.rate,
                "run": rnd.run,
                "timestamp": datetime.now(UTC).isoformat(),
            }
            try:
                result |= run_round(sc, rnd, host)
                log.info("  %s", one_line(result))
            except Exception as e:  # record the failure, keep the session going
                log.error("  FAILED: %s", e)
                result |= {"valid": False, "warnings": [f"round_failed: {e}"]}

            if sc.mode == "room" and not result.get("slo", {}).get("pass"):
                failed_level[key] = rnd.connections
            (session / "runs" / rnd.filename).write_text(json.dumps(result, indent=2))
            jsonl.write(json.dumps(result) + "\n")
            jsonl.flush()
            time.sleep(sc.cooldown)
    return session


def one_line(r: dict) -> str:
    lat, cpu, mem = r.get("latency_ms", {}), r["cpu"], r["memory"]
    parts = [
        f"{r['throughput']['messages_per_second']:.0f} msg/s",
        f"p99 {lat.get('p99', '-')} ms",
        f"cpu {cpu.get('avg_cores_used', '-')} cores",
        f"mem {mem.get('peak_mb', '-')} MB",
    ]
    if "slo" in r:
        parts.append("SLO pass" if r["slo"]["pass"] else f"SLO FAIL ({', '.join(r['slo']['reasons'])})")
    if r["warnings"]:
        parts.append(f"[{', '.join(r['warnings'])}]")
    return "  ".join(parts)


def int_list(s: str) -> list[int]:
    return [int(x) for x in s.split(",")]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    parser = argparse.ArgumentParser(prog="wsbench", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)  # fmt: skip
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run a scenario")
    run.add_argument("--scenario", help="JSON file with scenario values; the flags below override it")
    run.add_argument("--name")
    run.add_argument("--mode", choices=["throughput", "rate", "idle", "room"])
    run.add_argument("--servers", type=lambda s: s.split(","))
    for name in ("cores", "connections", "payload", "rates"):
        run.add_argument(f"--{name}", type=int_list)
    for name in ("pipeline", "warmup", "duration", "stabilize", "cooldown", "runs", "seed", "room_size"):
        run.add_argument(f"--{name.replace('_', '-')}", type=int)
    for name in ("user_rate", "slo_p99_ms"):
        run.add_argument(f"--{name.replace('_', '-')}", type=float)

    report = sub.add_parser("report", help="rebuild summary.md for a session")
    report.add_argument("session", type=Path)

    args = vars(parser.parse_args())
    command, scenario_file = args.pop("command"), args.pop("scenario", None)

    if command == "report":
        print(write_report(args["session"]))
        return
    sc = Scenario.load(scenario_file, args)
    session = run_session(sc, Host.from_env())
    log.info("done -> %s", session)
    print(write_report(session))
