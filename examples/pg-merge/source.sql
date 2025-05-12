-- Source schema and a few rows for examples/pg-merge/ferry. Load into the
-- database behind APP_DATABASE_URL.

create table customers (
    id    bigint primary key,
    name  text not null,
    email text
);

create table orders (
    id          bigint primary key,
    customer_id bigint references customers (id),
    status      text not null,
    placed_at   timestamptz not null,
    total_cents bigint not null
);

-- The primary key's leading order_id column gives the range scan that
-- co-partitioning on order_id needs (0002 step 3).
create table order_items (
    order_id   bigint not null references orders (id),
    line_no    int not null,
    sku        text not null,
    qty        int not null,
    unit_cents bigint not null,
    primary key (order_id, line_no)
);

insert into customers (id, name, email) values
    (1, 'Ana', 'ana@example.dev'),
    (2, 'Bo',  null);

insert into orders (id, customer_id, status, placed_at, total_cents) values
    (10, 1, 'paid',      '2026-03-01 10:00:00+00', 2550),
    (11, 1, 'cancelled', '2026-03-02 10:00:00+00',  900),  -- filtered
    (12, 2, 'paid',      '2026-03-03 10:00:00+00',    0),  -- no items: items = []
    (13, null, 'pending','2026-03-04 10:00:00+00',  300);  -- no customer: customer_name = null

insert into order_items (order_id, line_no, sku, qty, unit_cents) values
    (10, 1, 'SKU-2', 1, 550),
    (10, 0, 'SKU-1', 2, 1000),   -- out of insert order; nest sorts by line_no
    (11, 0, 'SKU-1', 1, 900),
    (13, 0, 'SKU-5', 0, 100),    -- qty 0: dropped from the nested array
    (13, 1, 'SKU-6', 3, 100);
