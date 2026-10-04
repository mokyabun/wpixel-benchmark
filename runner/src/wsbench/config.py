"""What to run (Scenario) and where to run it (Host)."""

import itertools
import json
import os
import random
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path

MODES = ("throughput", "rate", "idle", "room")


@dataclass
class Scenario:
    name: str = "bench"
    mode: str = "throughput"
    servers: list[str] = field(default_factory=lambda: ["node", "bun", "go", "rust"])
    cores: list[int] = field(default_factory=lambda: [1, 2])
    connections: list[int] = field(default_factory=lambda: [100])
    payload: list[int] = field(default_factory=lambda: [64])
    rates: list[int] = field(default_factory=lambda: [0])
    pipeline: int = 1  # throughput mode: messages in flight per connection
    room_size: int = 20  # room mode: users per room
    user_rate: float = 0.2  # room mode: messages per second per user
    slo_p99_ms: float = 100  # room mode: delivery p99 a level must stay under
    warmup: int = 15  # seconds
    duration: int = 30
    stabilize: int = 3
    cooldown: int = 5
    runs: int = 5
    seed: int | None = None

    @classmethod
    def load(cls, path: str | None, overrides: dict) -> "Scenario":
        """Defaults, then the JSON file, then command-line overrides."""
        values = json.loads(Path(path).read_text()) if path else {}
        values.update({k: v for k, v in overrides.items() if v is not None})
        known = {f.name for f in fields(cls)}
        unknown = set(values) - known
        if unknown:
            raise ValueError(f"unknown scenario keys: {sorted(unknown)}")
        return cls(**values)

    def __post_init__(self):
        if self.mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}")
        if self.mode != "rate":
            self.rates = [0]
        elif self.rates == [0]:
            raise ValueError("rate mode needs rates")
        if self.mode == "idle":
            self.payload = [0]
        if self.seed is None:
            self.seed = random.randrange(2**31)

    def plan(self) -> list["Round"]:
        """Every condition `runs` times. The order is shuffled within each
        repetition so drift of the VM over a session does not stick to one
        runtime (IMPL.md §16). Room mode climbs the user levels in order
        within a repetition, so a level that fails can skip the ones above."""
        rng = random.Random(self.seed)
        matrix = list(itertools.product(self.servers, self.cores, self.connections, self.payload, self.rates))
        rounds = []
        for run in range(1, self.runs + 1):
            rng.shuffle(matrix)
            if self.mode == "room":
                matrix.sort(key=lambda condition: condition[2])  # stable: shuffled within a level
            rounds += [Round(run, *condition) for condition in matrix]
        return rounds

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class Round:
    run: int
    server: str
    cores: int
    connections: int
    payload: int
    rate: int

    def __str__(self) -> str:
        rate = f" rate={self.rate}/s" if self.rate else ""
        return f"run {self.run}  {self.server} {self.cores}c conns={self.connections} payload={self.payload}B{rate}"

    @property
    def filename(self) -> str:
        rate = f"-{self.rate}ps" if self.rate else ""
        return f"{self.server}-{self.cores}c-{self.connections}-{self.payload}B{rate}-r{self.run}.json"


@dataclass(frozen=True)
class Host:
    """CPU layout and paths, from the environment (see .env.example)."""

    server_cpus: list[str]
    loader_cpus: str
    port: int
    results_dir: Path

    @classmethod
    def from_env(cls) -> "Host":
        return cls(
            server_cpus=os.environ.get("SERVER_CPUS", "2,3").split(","),
            loader_cpus=os.environ.get("LOADER_CPUS", "0,1"),
            port=int(os.environ.get("PORT", "8080")),
            results_dir=Path(os.environ.get("RESULTS_DIR", "/results")),
        )

    def server_cpuset(self, cores: int) -> str:
        if not 1 <= cores <= len(self.server_cpus):
            raise ValueError(f"{cores} cores requested but SERVER_CPUS={','.join(self.server_cpus)}")
        return ",".join(self.server_cpus[:cores])

    @property
    def loader_core_count(self) -> int:
        return len(self.loader_cpus.split(","))
