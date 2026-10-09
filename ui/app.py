"""Lossproof dashboard: how far each consumer's lake drifted from the source under each fault.

    uv run streamlit run ui/app.py

Reads the JSON files in results/chaos/, so it works without Docker.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

RESULTS = Path(__file__).resolve().parents[1] / "results" / "chaos"
BLUE, ORANGE, AQUA, YELLOW = "#2a78d6", "#eb6834", "#1baf9e", "#d4a017"

SCENARIOS = {
    "quiet": "Quiet day",
    "redelivery": "Redelivery (rewind to the start, twice)",
    "poison": "Bad messages",
    "maintenance": "Compaction while writing",
    "crash": "Crashes under load",
    "resnapshot": "Lost replication slot",
    "schema_change": "Schema change",
    "everything": "All of the above in one run",
}
KINDS = {
    "missing": "Missing (in the source, not in the lake)",
    "ghost": "Ghost (deleted at the source, still in the lake)",
    "stale": "Stale (values differ)",
    "duplicate": "Duplicate",
}


def load(name: str) -> Any:
    path = RESULTS / f"{name}.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def by_kind(consumer: dict[str, Any]) -> dict[str, int]:
    return {k: sum(t[k] for t in consumer["tables"].values()) for k in KINDS}


st.set_page_config(page_title="Lossproof", layout="wide")
st.title("Lossproof")
st.caption(
    "A change-data-capture pipeline from Postgres to an Iceberg lake on emulated AWS, built carelessly "
    "and correctly. The same faults are injected into both, then every lake row is compared with the source."
)

runs = {name: r for name in SCENARIOS if (r := load(name)) is not None}
if not runs:
    st.error("No results found. Run `uv run lossproof chaos all` first (see the README).")
    st.stop()

# -- 1. the headline -------------------------------------------------------------------------
if (top := runs.get("everything")) is not None:
    naive, correct = top["consumers"]["naive"], top["consumers"]["correct"]
    st.header("Every fault in one run")
    a, b, c, d = st.columns(4)
    a.metric("Source transactions", f"{top['transactions']:,}")
    b.metric("Faults injected", f"{top['faults']:,}")
    c.metric(
        "Careless consumer: wrong rows",
        f"{naive['wrong_rows']:,}",
        f"{naive['wrong_rows'] / naive['source_rows']:.0%} of {naive['source_rows']:,}",
        delta_color="off",
    )
    d.metric(
        "Correct consumer: wrong rows",
        f"{correct['wrong_rows']:,}",
        f"of {correct['source_rows']:,}",
        delta_color="off",
    )

# -- 2. fault by fault -----------------------------------------------------------------------
st.header("Fault by fault")
st.caption(
    "Wrong rows in the careless consumer's lake, by kind. The correct consumer's count is in the table."
)
bars = pd.DataFrame(
    [
        {"Scenario": SCENARIOS[name], "Kind": KINDS[kind], "Rows": rows}
        for name, r in runs.items()
        if name != "everything"
        for kind, rows in by_kind(r["consumers"]["naive"]).items()
    ]
)
order = [SCENARIOS[n] for n in runs if n != "everything"]
st.altair_chart(
    alt.Chart(bars)
    .mark_bar(height={"band": 0.6})
    .encode(
        y=alt.Y("Scenario:N", sort=order, title=None, axis=alt.Axis(labelLimit=320)),
        x=alt.X("Rows:Q", title="Wrong rows (careless consumer)"),
        color=alt.Color(
            "Kind:N",
            sort=list(KINDS.values()),
            scale=alt.Scale(domain=list(KINDS.values()), range=[ORANGE, BLUE, AQUA, YELLOW]),
            legend=alt.Legend(orient="bottom", title=None, labelLimit=400),
        ),
        order=alt.Order("Kind:N"),
        tooltip=["Scenario", "Kind", alt.Tooltip("Rows:Q", format=",")],
    ),
    use_container_width=True,
)
st.dataframe(
    pd.DataFrame(
        [
            {
                "Scenario": SCENARIOS[name],
                "Faults": r["faults"],
                "Source rows": r["consumers"]["naive"]["source_rows"],
                "Careless: wrong rows": r["consumers"]["naive"]["wrong_rows"],
                "Correct: wrong rows": r["consumers"]["correct"]["wrong_rows"],
                "Messages quarantined": r["dead_letters"],
            }
            for name, r in runs.items()
        ]
    ),
    hide_index=True,
    use_container_width=True,
)

# -- 3. crashes are a matter of timing -------------------------------------------------------
seeds = sorted(RESULTS.glob("crash_seed*.json"), key=lambda p: int(p.stem.removeprefix("crash_seed")))
if seeds:
    st.header("Crashes are a matter of timing")
    st.caption(
        "The same scenario on different seeds. A kill only does damage if it lands between the careless "
        "consumer's offset commit and its lake write, so the count varies from run to run."
    )
    rows = []
    for path in seeds:
        r = json.loads(path.read_text(encoding="utf-8"))
        rows.append(
            {
                "Seed": r["seed"],
                "Kills": r["faults"],
                "Careless: wrong rows": r["consumers"]["naive"]["wrong_rows"],
                "Correct: wrong rows": r["consumers"]["correct"]["wrong_rows"],
            }
        )
    st.dataframe(pd.DataFrame(rows), hide_index=True)
