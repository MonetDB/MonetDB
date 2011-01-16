WITH
-- Rel: ScjRel
-- Binding due to: dirty node
a0000(item_pre, iter_nat) AS
(SELECT DISTINCT c0002.pre, 1 AS iter_nat
FROM test1711251 AS c0002,
test1711251 AS c0001
WHERE (((c0001.pre + c0001.size) >= c0002.pre) AND ((c0002.pre >
c0001.pre)
AND ((c0002.kind = 1) AND ((c0002.prop = 'open_auction') AND
((c0001.prop = 'auctionG.xml') AND (c0001.kind = 6))))))),

-- Rel: count(Rel)
-- Binding due to: dirty node
a0001(item_int) AS
(SELECT COUNT (*) AS item_int
FROM a0000 AS c0003),

-- Rel: attach(Rel)
-- Binding due to: refctr > 1
a0002(iter_nat, item_int) AS
(SELECT 1 AS iter_nat, c0004.item_int
FROM a0001 AS c0004),

-- Rel: difference(Rel, Rel)
-- Binding due to: kind != sql_select
a0003(iter_nat) AS
((SELECT 1 AS iter_nat)
EXCEPT ALL
(SELECT c0008.iter_nat
FROM a0002 AS c0008)),

-- Rel: disjunion(Rel, Rel)
-- Binding due to: kind != sql_select
a0004(item_int) AS
((SELECT c0006.item_int
FROM a0002 AS c0006)
UNION ALL
(SELECT 0 AS item_int
FROM a0003 AS c0009)),

-- Rel: attach(Rel)
-- Binding due to: dirty node
a0005(pos_nat, item_int) AS
(SELECT 1 AS pos_nat, c0010.item_int
FROM a0004 AS c0010),

-- ====================
-- = RESULT RELATIONS =
-- ====================
result(pos_nat, item_int) AS
(SELECT c0011.pos_nat, c0011.item_int
FROM a0005 AS c0011)

select * from result;
