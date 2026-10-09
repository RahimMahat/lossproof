from lossproof.source import generator as g
from lossproof.source.generator import Statement, Workload


def statements(seed: int, n: int = 2000) -> list[Statement]:
    return [s for tx in Workload(seed).transactions(n) for s in tx]


def test_same_seed_same_statements() -> None:
    assert statements(7) == statements(7)
    assert statements(7) != statements(8)


def test_workload_is_valid_against_a_model() -> None:
    """Replay the statements on a set model: no update or delete may miss, no insert may collide."""
    rows: dict[str, set[tuple]] = {
        "customers": set(),
        "orders": set(),
        "order_items": set(),
        "payments": set(),
    }
    payment_order: dict[tuple, int] = {}
    verbs = set()
    for sql, p in statements(7, 5000):
        words = sql.split()
        verb, table = words[0], words[1 if words[0] == "update" else 2]
        verbs.add(verb)
        if verb == "insert":
            key = p[:2] if table == "order_items" else p[:1]
            assert key not in rows[table]
            rows[table].add(key)
            if sql == g.INSERT_ORDER:
                assert (p[1],) in rows["customers"]
            if sql == g.INSERT_PAYMENT:
                assert (p[1],) in rows["orders"]
                payment_order[key] = p[1]
        elif verb == "update":
            assert (p[-1],) in rows[table]
        elif sql == g.DELETE_ITEM:
            rows["order_items"].remove(p)
        else:  # deleting an order cascades
            rows["orders"].remove(p)
            rows["order_items"] = {k for k in rows["order_items"] if k[0] != p[0]}
            rows["payments"] = {k for k in rows["payments"] if payment_order[k] != p[0]}
    assert verbs == {"insert", "update", "delete"}
