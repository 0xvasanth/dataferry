-- The golden documents (fixtures/golden/source/orders.json) as relational
-- rows, for examples/golden-pg/ferry. Load into the database behind
-- FERRY_GOLDEN_PG_SOURCE_URL. Every edge case the documents carry is kept
-- where a table can carry it: a missing field and an explicit null are the
-- same here (…0001 and …0005, …0009), and so are a missing and an empty
-- `items` (…0002, …0003).

create table orders (
    id          text primary key,      -- the document's _id, as hex
    status      text not null,
    customer_id text,                  -- null when `customer` is (…000a)
    total       numeric(12,3) not null,
    created_at  timestamptz,           -- null when missing (…0009)
    deleted_at  timestamptz            -- set: soft-deleted (…0004)
);

-- The primary key's leading order_id column gives the range scan that
-- co-partitioning on order_id needs (0002 step 3). `qty` is text, so
-- …0006's "three" stays a value int() rejects.
create table order_items (
    order_id text not null references orders (id),
    line_no  int not null,
    sku      text not null,
    qty      text not null,
    price    numeric(12,3) not null,
    primary key (order_id, line_no)
);

insert into orders (id, status, customer_id, total, created_at, deleted_at) values
    ('650000000000000000000001', 'PAID',    'c1', 25.50, '2026-03-01 10:00:00+00', null),
    ('650000000000000000000002', 'PAID',    'c1',  0.00, '2026-03-02 10:00:00+00', null),
    ('650000000000000000000003', 'PENDING', 'c2', 12.00, '2026-03-03 10:00:00+00', null),
    ('650000000000000000000004', 'PAID',    'c2',  9.00, '2026-03-04 10:00:00+00', '2026-03-05 08:00:00+00'),
    ('650000000000000000000005', 'PAID',    'c2',  7.25, '2026-03-05 10:00:00+00', null),
    ('650000000000000000000006', 'PAID',    'c3',  8.00, '2026-03-06 10:00:00+00', null),
    ('650000000000000000000007', 'PAID',    'c1', 1.005, '2026-03-07 10:00:00+00', null),
    ('650000000000000000000008', 'Shipped', 'c3',  3.00, '2026-03-08 10:00:00+00', null),
    ('650000000000000000000009', 'PAID',    'c2',  4.00, null,                     null),
    ('65000000000000000000000a', 'PENDING', null,  0.50, '2026-03-10 10:00:00+00', null);

insert into order_items (order_id, line_no, sku, qty, price) values
    ('650000000000000000000001', 0, 'SKU-1', '2',     10.00),
    ('650000000000000000000001', 1, 'SKU-2', '1',      5.50),
    ('650000000000000000000004', 0, 'SKU-1', '1',      9.00),
    ('650000000000000000000005', 0, 'SKU-3', '1',      7.25),
    ('650000000000000000000006', 0, 'SKU-1', '1',      2.00),
    ('650000000000000000000006', 1, 'SKU-2', 'three',  2.00),
    ('650000000000000000000007', 0, 'SKU-4', '1',     1.005),
    ('650000000000000000000008', 0, 'SKU-5', '3',      1.00),
    ('650000000000000000000009', 0, 'SKU-1', '4',      1.00),
    ('65000000000000000000000a', 0, 'SKU-6', '1',      0.50);
