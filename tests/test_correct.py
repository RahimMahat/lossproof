from lossproof.consume.correct import fold_by_lsn, newer_than_lake, schema, to_rows
from lossproof.consume.events import Change


def change(pk: str, lsn: int, row: dict | None) -> Change:
    return Change(table="customers", pk=pk, lsn=lsn, row=row)


def test_fold_keeps_the_highest_log_position_whatever_the_arrival_order() -> None:
    new, old = change("1", 20, {"id": 1, "tier": "pro"}), change("1", 10, {"id": 1, "tier": "free"})
    assert fold_by_lsn([new, old])["1"] is new
    assert fold_by_lsn([old, new])["1"] is new


def test_fold_breaks_a_tie_by_arrival() -> None:
    first, second = change("1", 10, {"id": 1, "tier": "a"}), change("1", 10, {"id": 1, "tier": "b"})
    assert fold_by_lsn([first, second])["1"] is second


def test_a_change_older_than_the_lake_row_is_dropped() -> None:
    changes = {"1": change("1", 10, {"id": 1}), "2": change("2", 10, {"id": 2}), "3": change("3", 10, None)}
    kept = newer_than_lake(changes, {"1": 11, "2": 10})
    assert set(kept) == {"2", "3"}  # equal is kept: a snapshot row may restore its own tombstone


def test_a_delete_becomes_a_tombstone_row_with_its_key_columns() -> None:
    deleted = Change(table="order_items", pk="5|2", lsn=30, row=None)
    assert to_rows("order_items", {"5|2": deleted}) == [
        {"order_id": 5, "line_no": 2, "_pk": "5|2", "_lsn": 30, "_deleted": True}
    ]


def test_lake_integers_are_64_bit() -> None:
    s = schema("order_items")
    assert str(s.field("qty").type) == "int64" and str(s.field("line_no").type) == "int64"
