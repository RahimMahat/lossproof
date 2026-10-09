"""The chaos harness. It runs on the host and drives Docker.

A scenario applies one seeded workload to the source while the consumers run, injects one kind
of fault at seeded points, waits for the stream to drain, and reconciles every consumer. All
consumers read the same stream and are killed at the same points.

Faults that depend on timing (a kill lands wherever the consumer happens to be) are not exactly
repeatable. The schedule is seeded; the damage is measured, not predicted.
"""

from __future__ import annotations

import json
import random
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

CONSUMERS = ("naive", "correct")
HOST_ENDPOINT = "http://localhost:4566"  # Floci as seen from the host
DLQ = "lossproof-dlq"
RESULTS = Path("results/chaos")


class Run:
    def __init__(self, scenario: str, n: int, seed: int) -> None:
        self.scenario, self.n, self.seed = scenario, n, seed
        self.id = f"{scenario}_{int(time.time())}"
        self.rng = random.Random(seed)
        self.msk = sh("docker", "ps", "--format", "{{.Names}}", "--filter", "name=floci-aws-msk").strip()
        self.faults = 0

    # --- building blocks -----------------------------------------------------------------

    def app(self, *args: str, check: bool = True) -> str:
        """Run one command in a fresh app container."""
        cmd = ("docker", "compose", "run", "--rm", "-T", "-e", f"LOSSPROOF_RUN={self.id}", "app", *args)
        return sh(*cmd, check=check)

    def container(self, consumer: str) -> str:
        return f"lp-{self.id}-{consumer}"

    def start_consumers(self) -> None:
        for c in CONSUMERS:
            self.app("lake-reset", c)
            sh("docker", "compose", "run", "-d", "--name", self.container(c),
               "-e", f"LOSSPROOF_RUN={self.id}", "app", "consume", c)  # fmt: skip

    def generate(self, upto: int, skip: int, evolve_at: int | None = None) -> None:
        args = ["source", "generate", "--n", str(upto), "--skip", str(skip), "--seed", str(self.seed)]
        self.app(*args, *(["--evolve-at", str(evolve_at)] if evolve_at is not None else []))

    def generate_in_background(self) -> subprocess.Popen[bytes]:
        cmd = ["docker", "compose", "run", "--rm", "-T", "-e", f"LOSSPROOF_RUN={self.id}", "app"]
        cmd += ["source", "generate", "--n", str(self.n), "--seed", str(self.seed)]
        return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def rpk(self, *args: str, stdin: str | None = None, check: bool = True) -> str:
        return sh("docker", "exec", "-i", self.msk, "rpk", *args, stdin=stdin, check=check)

    def lag(self, consumer: str) -> int | None:
        """Messages the consumer group has not committed yet, or None while it has no committed offsets."""
        out = self.rpk("group", "describe", f"lossproof-{self.id}-{consumer}", check=False)
        for line in out.splitlines():
            if line.startswith("TOTAL-LAG"):
                return int(line.split()[1])
        return None

    def end_offsets(self) -> int:
        out = self.rpk("topic", "describe", "-p", "-r", f"^lp_{self.id}\\..*", check=False)
        return sum(
            int(parts[-1]) for line in out.splitlines() if (parts := line.split()) and parts[0].isdigit()
        )

    def drain(self, timeout: float = 600) -> None:
        """Wait until Debezium has stopped producing and every consumer has committed to the end."""
        # ponytail: "quiet for 8 seconds" stands in for comparing the slot's flushed LSN with the WAL head.
        deadline, last, quiet_since = time.monotonic() + timeout, -1, time.monotonic()
        while time.monotonic() < deadline:
            for c in CONSUMERS:  # a consumer that stopped on an error will never drain
                state = sh("docker", "inspect", "-f", "{{.State.Running}}", self.container(c)).strip()
                if state != "true":
                    logs = sh("docker", "logs", "--tail", "5", self.container(c), check=False)
                    raise RuntimeError(f"{self.id}: the {c} consumer stopped\n{logs}")
            end = self.end_offsets()
            if end != last:
                last, quiet_since = end, time.monotonic()
            elif time.monotonic() - quiet_since > 8 and all(self.lag(c) == 0 for c in CONSUMERS):
                return
            time.sleep(2)
        raise TimeoutError(f"{self.id}: stream did not drain in {timeout:.0f}s")

    def stop_consumers(self) -> None:
        for c in CONSUMERS:
            sh("docker", "stop", "-t", "120", self.container(c))  # SIGTERM: finish the batch
            sh("docker", "rm", self.container(c))

    # --- faults ---------------------------------------------------------------------------

    def crash(self) -> None:
        """Kill every consumer where it stands, then start it again."""
        for c in CONSUMERS:
            sh("docker", "kill", self.container(c), check=False)  # it may already be down
        time.sleep(1)
        for c in CONSUMERS:
            sh("docker", "start", self.container(c))
        self.faults += 1

    def redeliver(self) -> None:
        """Stop the consumers cleanly, rewind their groups to the start of every topic, start them."""
        for c in CONSUMERS:
            sh("docker", "stop", "-t", "120", self.container(c))
            self.rpk("group", "seek", f"lossproof-{self.id}-{c}", "--to", "start")
            sh("docker", "start", self.container(c))
        self.faults += 1

    def poison(self, count: int) -> None:
        """Put messages on the orders topic that are not change events."""
        junk = ["not json", '{"op": "u"}', '{"after": {"id": "x"}}']
        lines = "\n".join(self.rng.choice(junk) for _ in range(count)) + "\n"
        self.rpk("topic", "produce", f"lp_{self.id}.public.orders", stdin=lines)
        self.faults += count

    def checkpoints(self, k: int) -> list[int]:
        """``k`` seeded transaction indices, in order, away from the very start and end."""
        return sorted(self.rng.sample(range(self.n // 10, self.n * 9 // 10), k))


# --- scenarios: each takes a Run with consumers started and applies the whole workload ---------


def quiet(run: Run) -> None:
    run.generate(run.n, 0)


def redelivery(run: Run) -> None:
    done = 0
    for point in run.checkpoints(2):
        run.generate(point, done)
        run.redeliver()
        done = point
    run.generate(run.n, done)


def crash(run: Run) -> None:
    """Kill the consumers every few seconds for as long as the workload runs, so kills land under load."""
    workload = run.generate_in_background()
    time.sleep(5)
    while workload.poll() is None:
        run.crash()
        time.sleep(run.rng.uniform(8, 14))
    if workload.returncode:
        raise RuntimeError("the workload failed")


def poison(run: Run) -> None:
    half = run.n // 2
    run.generate(half, 0)
    run.poison(20)
    run.generate(run.n, half)


def schema_change(run: Run) -> None:
    run.generate(run.n, 0, evolve_at=run.n // 2)
    run.faults += 2  # one added column, one widened column


def resnapshot(run: Run) -> None:
    """The replication slot is lost. Changes made before a new connector snapshots are never captured."""
    a, b = run.n // 2, run.n * 6 // 10
    run.generate(a, 0)
    run.drain()
    run.app("source", "disconnect")
    run.generate(b, a)  # the gap
    run.app("source", "connect", "--generation", "2")
    run.generate(run.n, b)
    run.faults += 1


SCENARIOS: dict[str, Callable[[Run], None]] = {
    "quiet": quiet,
    "redelivery": redelivery,
    "crash": crash,
    "poison": poison,
    "schema_change": schema_change,
    "resnapshot": resnapshot,
}


def run(scenario: str, n: int, seed: int) -> dict[str, Any]:
    from lossproof import config

    r = Run(scenario, n, seed)
    sqs = config.aws("sqs", HOST_ENDPOINT)
    queue = sqs.get_queue_url(QueueName=DLQ)["QueueUrl"]
    sqs.purge_queue(QueueUrl=queue)
    print(f"[{r.id}] reset", flush=True)
    r.app("source", "reset")
    r.start_consumers()
    try:
        print(f"[{r.id}] workload: {n:,} transactions", flush=True)
        SCENARIOS[scenario](r)
        r.drain()
    finally:
        r.stop_consumers()
    attributes = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["ApproximateNumberOfMessages"])
    result: dict[str, Any] = {
        "scenario": scenario,
        "run": r.id,
        "transactions": n,
        "seed": seed,
        "faults": r.faults,
        "dead_letters": int(attributes["Attributes"]["ApproximateNumberOfMessages"]),
        "consumers": {},
    }
    for c in CONSUMERS:
        out = f"results/chaos/{scenario}_{c}.json"
        r.app("reconcile", c, "--out", out, check=False)  # exits 1 when rows differ
        detail = json.loads(Path(out).read_text())
        result["consumers"][c] = {k: detail[k] for k in ("source_rows", "wrong_rows", "tables")}
        print(f"[{r.id}] {c}: {detail['wrong_rows']:,} wrong of {detail['source_rows']:,}", flush=True)
    (RESULTS / f"{scenario}.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def sh(*cmd: str, stdin: str | None = None, check: bool = True) -> str:
    p = subprocess.run(cmd, input=stdin, capture_output=True, text=True, encoding="utf-8")
    if check and p.returncode:
        raise RuntimeError(f"{' '.join(cmd)}\n{p.stdout}{p.stderr}")
    return p.stdout
