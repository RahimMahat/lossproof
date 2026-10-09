"""``lossproof`` command-line entry point.

Everything except ``chaos`` runs in the app container: ``docker compose run --rm app <command>``.
"""

from __future__ import annotations

import json
import time
from importlib import resources
from pathlib import Path
from typing import Any

import typer

app = typer.Typer(no_args_is_help=True, help="Lossproof: a CDC lakehouse that proves it lost nothing.")
source_app = typer.Typer(no_args_is_help=True, help="The source database and its change stream.")
app.add_typer(source_app, name="source")


def _drop_slots() -> None:
    """Remove every connector, then its replication slot once Debezium has let go of it."""
    import psycopg

    from lossproof import config
    from lossproof.source import debezium

    debezium.remove_all()
    with psycopg.connect(config.source_dsn(), autocommit=True) as conn:
        for _ in range(30):
            if not conn.execute("select 1 from pg_replication_slots where active").fetchone():
                break
            time.sleep(1)
        conn.execute("select pg_drop_replication_slot(slot_name) from pg_replication_slots where not active")


@source_app.command("reset")
def source_reset() -> None:
    """Empty the source database and start a fresh change stream for this run."""
    import psycopg

    from lossproof import config
    from lossproof.source import debezium

    _drop_slots()
    with psycopg.connect(config.source_dsn()) as conn:
        conn.execute("drop schema public cascade")
        conn.execute("create schema public")
        conn.execute(resources.files("lossproof.source").joinpath("schema.sql").read_text())
    typer.echo(f"schema created, connector: {debezium.register()['name']}")


@source_app.command("disconnect")
def source_disconnect() -> None:
    """Delete the connector and drop its replication slot. Later changes are not captured."""
    _drop_slots()


@source_app.command("connect")
def source_connect(generation: int = 2) -> None:
    """Register a new connector generation. It snapshots every table, then streams."""
    from lossproof.source import debezium

    typer.echo(f"connector: {debezium.register(generation)['name']}")


@source_app.command("generate")
def source_generate(
    n: int = 10_000,
    skip: int = typer.Option(0, help="Resume: transactions before this index are not applied."),
    evolve_at: int = typer.Option(None, help="Change the schema at this transaction index."),
    seed: int = typer.Option(None, help="Defaults to config.SEED."),
) -> None:
    """Apply transactions SKIP to N of the seeded workload to the source database."""
    from lossproof import config
    from lossproof.source import generator

    seed = config.SEED if seed is None else seed
    typer.echo(json.dumps(generator.run(config.source_dsn(), seed, n, skip, evolve_at)))


@source_app.command("status")
def source_status() -> None:
    """Show the connector state."""
    from lossproof.source import debezium

    typer.echo(json.dumps(debezium.status(), indent=2))


def _consumer(name: str) -> Any:
    from lossproof.consume import correct, naive

    consumers = {"naive": naive, "correct": correct}
    if name not in consumers:
        raise typer.BadParameter(f"unknown consumer {name!r}, expected one of {sorted(consumers)}")
    return consumers[name]


@app.command("lake-reset")
def lake_reset(consumer: str) -> None:
    """Drop and recreate one consumer's lake tables for this run."""
    from lossproof import lake

    module = _consumer(consumer)
    lake.reset(lake.catalog(), module.SUFFIX, module.schema)


@app.command("consume")
def consume(
    consumer: str,
    until_idle: float = typer.Option(None, help="Stop after this many seconds without a message."),
) -> None:
    """Apply the change stream to the lake."""
    typer.echo(f"applied {_consumer(consumer).run(until_idle):,} changes")


@app.command("reconcile")
def reconcile(consumer: str, out: Path = typer.Option(None, help="Also write the result here.")) -> None:
    """Compare the source with one consumer's lake tables. Exits 1 if any row differs."""
    from lossproof import reconcile as rec

    result = rec.run(_consumer(consumer).SUFFIX)
    if out:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(result, indent=2) + "\n")
    for table, d in result["tables"].items():
        typer.echo(f"{table:<12} " + "  ".join(f"{k}={v:,}" for k, v in d.items()))
    typer.echo(f"wrong rows: {result['wrong_rows']:,} of {result['source_rows']:,}")
    raise typer.Exit(1 if result["wrong_rows"] else 0)


@app.command("chaos")
def chaos(
    scenario: str = typer.Argument("all", help="A scenario name, or 'all'."),
    n: int = typer.Option(20_000, help="Transactions per scenario."),
    seed: int = typer.Option(None, help="Defaults to config.SEED."),
) -> None:
    """Run fault scenarios and reconcile each consumer. Runs on the host: it drives Docker."""
    from lossproof import chaos as harness
    from lossproof import config

    for name in list(harness.SCENARIOS) if scenario == "all" else [scenario]:
        harness.run(name, n, config.SEED if seed is None else seed)
