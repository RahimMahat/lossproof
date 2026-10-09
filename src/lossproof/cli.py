"""``lossproof`` command-line entry point."""

from __future__ import annotations

import json
from importlib import resources

import typer

app = typer.Typer(no_args_is_help=True, help="Lossproof: a CDC lakehouse that proves it lost nothing.")
source_app = typer.Typer(no_args_is_help=True, help="The source database and its change stream.")
app.add_typer(source_app, name="source")


@source_app.command("init")
def source_init() -> None:
    """Create the tables and register the Debezium connector."""
    import psycopg

    from lossproof import config
    from lossproof.source import debezium

    with psycopg.connect(config.source_dsn()) as conn:
        conn.execute(resources.files("lossproof.source").joinpath("schema.sql").read_text())
    typer.echo("schema created")
    typer.echo(f"connector: {debezium.register()['name']}")


@source_app.command("generate")
def source_generate(n: int = 10_000, seed: int = typer.Option(None, help="Defaults to config.SEED.")) -> None:
    """Apply N seeded transactions to the source database."""
    from lossproof import config
    from lossproof.source import generator

    typer.echo(json.dumps(generator.run(config.source_dsn(), config.SEED if seed is None else seed, n)))


@source_app.command("status")
def source_status() -> None:
    """Show the connector state."""
    from lossproof.source import debezium

    typer.echo(json.dumps(debezium.status(), indent=2))


@app.command("consume")
def consume(
    consumer: str = typer.Argument("naive", help="naive (the correct one arrives in milestone 4)."),
    reset: bool = typer.Option(False, help="Drop and recreate this consumer's lake tables first."),
    until_idle: float = typer.Option(None, help="Stop after this many seconds without a message."),
) -> None:
    """Apply the change stream to the lake."""
    from lossproof import lake
    from lossproof.consume import naive

    if consumer != "naive":
        raise typer.BadParameter(f"unknown consumer {consumer!r}")
    if reset:
        lake.reset(lake.catalog(), consumer)
    typer.echo(f"applied {naive.run(until_idle):,} changes")


@app.command("reconcile")
def reconcile(consumer: str = typer.Argument("naive")) -> None:
    """Compare the source with one consumer's lake tables. Exits 1 if any row differs."""
    from pathlib import Path

    from lossproof import reconcile as rec

    result = rec.run(consumer)
    Path("results").mkdir(exist_ok=True)
    Path(f"results/reconcile_{consumer}.json").write_text(json.dumps(result, indent=2) + "\n")
    for table, d in result["tables"].items():
        typer.echo(f"{table:<12} " + "  ".join(f"{k}={v:,}" for k, v in d.items()))
    typer.echo(f"wrong rows: {result['wrong_rows']:,} of {result['source_rows']:,}")
    raise typer.Exit(1 if result["wrong_rows"] else 0)
