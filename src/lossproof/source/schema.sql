-- A small order-processing schema. Every table has a primary key, which is all Debezium needs
-- to key change events (a delete carries the key only).
create table if not exists customers (
    id         bigint primary key,
    email      text not null,
    tier       text not null,
    updated_at timestamptz not null default now()
);

create table if not exists orders (
    id          bigint primary key,
    customer_id bigint not null references customers (id),
    status      text not null,
    total_cents bigint not null,
    updated_at  timestamptz not null default now()
);

create table if not exists order_items (
    order_id    bigint not null references orders (id) on delete cascade,
    line_no     int not null,
    sku         text not null,
    qty         int not null,
    price_cents bigint not null,
    primary key (order_id, line_no)
);

create table if not exists payments (
    id           bigint primary key,
    order_id     bigint not null references orders (id) on delete cascade,
    amount_cents bigint not null,
    status       text not null
);
