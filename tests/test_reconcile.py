import json

from lossproof.consume.events import fold_by_arrival, parse
from lossproof.reconcile import canonical, diff, wrong


def event(op: str, row: dict, lsn: int) -> bytes:
    before, after = (row, None) if op == "d" else (None, row)
    return json.dumps({"op": op, "before": before, "after": after, "source": {"lsn": lsn}}).encode()


def test_diff_counts_each_kind_of_difference() -> None:
    source = {"1": "a", "2": "b", "3": "c"}
    lake = [("1", "a"), ("2", "old"), ("4", "x"), ("1", "a")]
    d = diff(source, lake)
    assert (d["missing"], d["ghost"], d["stale"], d["duplicate"]) == (1, 1, 1, 1)
    assert wrong(d) == 4
    assert wrong(diff(source, list(source.items()))) == 0


def test_canonical_treats_a_missing_column_as_null() -> None:
    assert canonical({"id": 1, "v": None}, ["id", "v", "added"]) == "1|∅|∅"


def test_parse_and_fold() -> None:
    topic = "lp.public.order_items"
    row = {"order_id": 5, "line_no": 2, "sku": "s", "qty": 1, "price_cents": 100}
    changes = [parse(topic, event("c", row, 10)), parse(topic, event("u", {**row, "qty": 3}, 11))]
    assert parse(topic, None) is None
    assert changes[0] is not None and changes[0].pk == "5|2"
    folded = fold_by_arrival([c for c in changes if c])
    assert folded["order_items"]["5|2"].row == {**row, "qty": 3}
    deleted = parse(topic, event("d", {"order_id": 5, "line_no": 2}, 12))
    assert deleted is not None and deleted.row is None and deleted.lsn == 12
