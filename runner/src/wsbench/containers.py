"""Server and loader containers, via the Docker SDK.

Images come from `task build` (compose). Every container uses the host
network and a fixed cpuset (IMPL.md §20-21) and carries a label so leftovers
of an interrupted session can be removed.
"""

import json
import time
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from functools import cache

import docker
from docker.models.containers import Container
from docker.types import Ulimit

from .config import Host, Round, Scenario


@cache
def client() -> docker.DockerClient:
    return docker.from_env()


LABELS = {"org.wsbench.managed": "true"}
NOFILE = [Ulimit(name="nofile", soft=1 << 20, hard=1 << 20)]


def image(name: str) -> str:
    return f"wsbench/{name}:latest"


def implementation(server: str) -> str:
    """Human-readable implementation, from the image's label."""
    return client().images.get(image(server)).labels.get("org.wsbench.implementation", server)


def remove_leftovers() -> None:
    for c in client().containers.list(all=True, filters={"label": "org.wsbench.managed=true"}):
        c.remove(force=True)


def docker_info() -> dict:
    info = client().info()
    keys = ("ServerVersion", "OperatingSystem", "KernelVersion", "Architecture", "NCPU", "MemTotal", "CgroupVersion")
    return {k: info.get(k) for k in keys}


@contextmanager
def server(host: Host, rnd: Round) -> Iterator[Container]:
    """Start one server pinned to its cores; remove it on exit."""
    cpuset = host.server_cpuset(rnd.cores)
    container = client().containers.run(
        image(rnd.server),
        name="wsbench-server",
        detach=True,
        network_mode="host",
        cpuset_cpus=cpuset,
        ulimits=NOFILE,
        labels=LABELS,
        environment={"CORES": rnd.cores, "PORT": host.port},
    )
    try:
        container.reload()
        applied = container.attrs["HostConfig"]["CpusetCpus"]
        if applied != cpuset:
            raise RuntimeError(f"cpuset not applied: want {cpuset}, got {applied}")
        wait_healthy(host, container, rnd.cores)
        yield container
    finally:
        container.remove(force=True)


def wait_healthy(host: Host, container: Container, cores: int, timeout: float = 60) -> None:
    """Every port answers /health: PORT and the room shard ports PORT+1 .. PORT+cores."""
    pending = [host.port + i for i in range(cores + 1)]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        while pending:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{pending[0]}/health", timeout=1) as r:
                    if r.status != 200:
                        break
            except OSError:
                break
            pending.pop(0)
        if not pending:
            return
        container.reload()
        if container.status != "running":
            raise RuntimeError(f"server exited before becoming healthy:\n{container.logs(tail=20).decode()}")
        time.sleep(0.2)
    raise RuntimeError(f"server not healthy after {timeout}s")


def is_running(container: Container) -> bool:
    container.reload()
    return container.status == "running"


def run_loader(host: Host, sc: Scenario, rnd: Round) -> dict:
    """Run the loader to completion; return its JSON result.

    Warm-up and measurement both happen inside the loader, which reports the
    wall-clock window it measured.
    """
    args = [
        "-url", f"ws://127.0.0.1:{host.port}/ws",
        "-mode", sc.mode,
        "-connections", str(rnd.connections),
        "-payload", str(rnd.payload),
        "-pipeline", str(sc.pipeline),
        "-warmup", f"{sc.warmup}s",
        "-duration", f"{sc.duration}s",
    ]  # fmt: skip
    if rnd.rate:
        args += ["-rate", str(rnd.rate)]
    if sc.mode == "room":
        # Room r lives on port PORT+1 + r % cores (the protocol is in loader/room.go).
        args[1] = f"ws://127.0.0.1:{host.port + 1}/room"
        args += ["-shards", str(rnd.cores), "-room-size", str(sc.room_size), "-user-rate", str(sc.user_rate)]

    container = client().containers.run(
        image("loader"),
        args,
        name="wsbench-loader",
        detach=True,
        network_mode="host",
        cpuset_cpus=host.loader_cpus,
        ulimits=NOFILE,
        # Docker's default AppArmor profile costs ~8% of the loader's CPU in
        # per-syscall checks. The loader is not under test; servers keep it.
        security_opt=["apparmor=unconfined"],
        labels=LABELS,
    )
    try:
        connect_allowance = rnd.connections / 200  # seconds; tens of thousands of users take a while to join
        status = container.wait(timeout=sc.warmup + sc.duration + 120 + connect_allowance)["StatusCode"]
        if status != 0:
            raise RuntimeError(f"loader exited {status}: {container.logs(stdout=False, tail=5).decode()}")
        return json.loads(container.logs(stdout=True, stderr=False))
    finally:
        container.remove(force=True)
