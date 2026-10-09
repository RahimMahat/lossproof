"""A seeded order-processing workload: inserts, updates and deletes against the source database.

``Workload`` is pure. It yields transactions as lists of (sql, params) and keeps just enough state
to stay valid (it never updates a deleted row). The same seed gives the same statements.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from typing import Any

Statement = tuple[str, tuple[Any, ...]]
Transaction = list[Statement]

TIERS = ("free", "plus", "pro")
OPEN, PAID, SHIPPED, CANCELLED = "open", "paid", "shipped", "cancelled"

INSERT_CUSTOMER = "insert into customers (id, email, tier) values (%s, %s, %s)"
INSERT_ORDER = "insert into orders (id, customer_id, status, total_cents) values (%s, %s, %s, %s)"
INSERT_ITEM = "insert into order_items (order_id, line_no, sku, qty, price_cents) values (%s, %s, %s, %s, %s)"
INSERT_PAYMENT = "insert into payments (id, order_id, amount_cents, status) values (%s, %s, %s, %s)"
SET_TIER = "update customers set tier = %s, updated_at = now() where id = %s"
SET_EMAIL = "update customers set email = %s, updated_at = now() where id = %s"
SET_ORDER_STATUS = "update orders set status = %s, updated_at = now() where id = %s"
SET_PAYMENT_STATUS = "update payments set status = %s where id = %s"
DELETE_ITEM = "delete from order_items where order_id = %s and line_no = %s"
DELETE_ORDER = "delete from orders where id = %s"  # items and payments cascade

# Schema evolution: one added column and one widened column, then statements that use them.
ADD_PHONE = "alter table customers add column phone text"
WIDEN_QTY = "alter table order_items alter column qty type bigint"
INSERT_CUSTOMER_PHONE = "insert into customers (id, email, tier, phone) values (%s, %s, %s, %s)"
SET_PHONE = "update customers set phone = %s, updated_at = now() where id = %s"
BULK_QTY = 3_000_000_000  # does not fit the original 32-bit column


class Workload:
    def __init__(self, seed: int, evolve_at: int | None = None) -> None:
        self.rng = random.Random(seed)
        self.evolve_at = evolve_at  # the transaction index at which the schema changes
        self.evolved = False
        self.next_id = 1
        self.customers: list[int] = []
        self.orders: dict[int, dict[str, Any]] = {}  # live orders: status, total, lines, payment id

    def _id(self) -> int:
        self.next_id += 1
        return self.next_id - 1

    def _new_customer(self) -> Transaction:
        cid = self._id()
        self.customers.append(cid)
        row = (cid, f"user{cid}@example.com", self.rng.choice(TIERS))
        if self.evolved:
            return [(INSERT_CUSTOMER_PHONE, (*row, self._phone()))]
        return [(INSERT_CUSTOMER, row)]

    def _phone(self) -> str:
        return f"+1555{self.rng.randrange(10_000_000):07d}"

    def _update_customer(self) -> Transaction:
        cid = self.rng.choice(self.customers)
        if self.evolved and self.rng.random() < 0.4:
            return [(SET_PHONE, (self._phone(), cid))]
        if self.rng.random() < 0.5:
            return [(SET_TIER, (self.rng.choice(TIERS), cid))]
        return [(SET_EMAIL, (f"user{cid}+{self.rng.randrange(1000)}@example.com", cid))]

    def _new_order(self) -> Transaction:
        oid, cid = self._id(), self.rng.choice(self.customers)
        lines = [
            (n, f"sku-{self.rng.randrange(500)}", self.rng.randint(1, 5), self.rng.randrange(100, 20_000))
            for n in range(1, self.rng.randint(1, 4) + 1)
        ]
        if self.evolved and self.rng.random() < 0.05:  # a bulk order
            lines[0] = (lines[0][0], lines[0][1], BULK_QTY, lines[0][3])
        total = sum(qty * price for _, _, qty, price in lines)
        self.orders[oid] = {"status": OPEN, "total": total, "lines": [n for n, *_ in lines], "payment": None}
        return [(INSERT_ORDER, (oid, cid, OPEN, total)), *((INSERT_ITEM, (oid, *line)) for line in lines)]

    def _advance_order(self) -> Transaction:
        """Move one order along open -> paid -> shipped, cancel it, drop a line, or archive it."""
        oid = self.rng.choice(list(self.orders))
        o = self.orders[oid]
        if o["status"] == OPEN and self.rng.random() < 0.15:
            o["status"] = CANCELLED
            return [(SET_ORDER_STATUS, (CANCELLED, oid))]
        if o["status"] == OPEN and len(o["lines"]) > 1 and self.rng.random() < 0.2:
            line = o["lines"].pop(self.rng.randrange(len(o["lines"])))
            return [(DELETE_ITEM, (oid, line))]
        if o["status"] == OPEN:
            o["status"], o["payment"] = PAID, self._id()
            return [
                (INSERT_PAYMENT, (o["payment"], oid, o["total"], "authorised")),
                (SET_ORDER_STATUS, (PAID, oid)),
            ]
        if o["status"] == PAID:
            o["status"] = SHIPPED
            return [(SET_PAYMENT_STATUS, ("captured", o["payment"])), (SET_ORDER_STATUS, (SHIPPED, oid))]
        del self.orders[oid]  # shipped or cancelled: archive it
        return [(DELETE_ORDER, (oid,))]

    def transactions(self, n: int) -> Iterator[Transaction]:
        for i in range(n):
            if i == self.evolve_at:
                self.evolved = True
                yield [(ADD_PHONE, ()), (WIDEN_QTY, ())]
                continue
            r = self.rng.random()
            if not self.customers or r < 0.10:
                yield self._new_customer()
            elif r < 0.20:
                yield self._update_customer()
            elif not self.orders or r < 0.50:
                yield self._new_order()
            else:
                yield self._advance_order()


def run(dsn: str, seed: int, n: int, skip: int = 0, evolve_at: int | None = None) -> dict[str, int]:
    """Apply transactions ``skip`` to ``n`` of the workload. Returns the number of statements by verb.

    The workload is deterministic, so a run can be split: (n=500), then (n=1000, skip=500).
    """
    import psycopg

    counts = {"transactions": 0, "insert": 0, "update": 0, "delete": 0, "alter": 0}
    with psycopg.connect(dsn) as conn:
        for i, tx in enumerate(Workload(seed, evolve_at).transactions(n)):
            if i < skip:
                continue
            for sql, params in tx:
                conn.execute(sql, params)
                counts[sql.split()[0]] += 1
            conn.commit()
            counts["transactions"] += 1
    return counts
