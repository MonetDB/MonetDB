select c0, cast(sum(c2) as bigint) from (select 'A' c0, 'a' c1, 1 c2 union all select 'A', 'a', 2) T;

select cast(sum(c2) as bigint), c0 from (select 'A' c0, 'a' c1, 1 c2 union all select 'A', 'a', 2) T;

select cast(sum(c2) as bigint), c0 from (select 'A' c0, 'a' c1, 1 c2 union all select 'A', 'a', 2) T GROUP BY c2;

select c0, cast(sum(c2) as bigint) from (select 'A' c0, 'a' c1, 1 c2 union all select 'A', 'a', 2) T GROUP BY c2;

select cast(sum(c2) as bigint), c0 from (select 'A' c0, 'a' c1, 1 c2 union all select 'A', 'a', 2) T GROUP BY c0;

select c0, cast(sum(c2) as bigint) from (select 'A' c0, 'a' c1, 1 c2 union all select 'A', 'a', 2) T GROUP BY c0;

select cast(sum(c2) as bigint), c0 from (select 'A' c0, 'a' c1, 1 c2 union all select 'A', 'a', 2) T GROUP BY c0, c2;

select c0, cast(sum(c2) as bigint) from (select 'A' c0, 'a' c1, 1 c2 union all select 'A', 'a', 2) T GROUP BY c0, c2;


select c0, cast(sum(c2) as bigint) from (select 'A' c0, 'a' c1, 1 c2 except all select 'A', 'a', 2) T;

select cast(sum(c2) as bigint), c0 from (select 'A' c0, 'a' c1, 1 c2 except all select 'A', 'a', 2) T;

select cast(sum(c2) as bigint), c0 from (select 'A' c0, 'a' c1, 1 c2 except all select 'A', 'a', 2) T GROUP BY c2;

select c0, cast(sum(c2) as bigint) from (select 'A' c0, 'a' c1, 1 c2 except all select 'A', 'a', 2) T GROUP BY c2;

select cast(sum(c2) as bigint), c0 from (select 'A' c0, 'a' c1, 1 c2 except all select 'A', 'a', 2) T GROUP BY c0;

select c0, cast(sum(c2) as bigint) from (select 'A' c0, 'a' c1, 1 c2 except all select 'A', 'a', 2) T GROUP BY c0;

select cast(sum(c2) as bigint), c0 from (select 'A' c0, 'a' c1, 1 c2 except all select 'A', 'a', 2) T GROUP BY c0, c2;

select c0, cast(sum(c2) as bigint) from (select 'A' c0, 'a' c1, 1 c2 except all select 'A', 'a', 2) T GROUP BY c0, c2;


select c0, cast(sum(c2) as bigint) from (select 'A' c0, 'a' c1, 1 c2 intersect all select 'A', 'a', 2) T;

select cast(sum(c2) as bigint), c0 from (select 'A' c0, 'a' c1, 1 c2 intersect all select 'A', 'a', 2) T;

select cast(sum(c2) as bigint), c0 from (select 'A' c0, 'a' c1, 1 c2 intersect all select 'A', 'a', 2) T GROUP BY c2;

select c0, cast(sum(c2) as bigint) from (select 'A' c0, 'a' c1, 1 c2 intersect all select 'A', 'a', 2) T GROUP BY c2;

select cast(sum(c2) as bigint), c0 from (select 'A' c0, 'a' c1, 1 c2 intersect all select 'A', 'a', 2) T GROUP BY c0;

select c0, cast(sum(c2) as bigint) from (select 'A' c0, 'a' c1, 1 c2 intersect all select 'A', 'a', 2) T GROUP BY c0;

select cast(sum(c2) as bigint), c0 from (select 'A' c0, 'a' c1, 1 c2 intersect all select 'A', 'a', 2) T GROUP BY c0, c2;

select c0, cast(sum(c2) as bigint) from (select 'A' c0, 'a' c1, 1 c2 intersect all select 'A', 'a', 2) T GROUP BY c0, c2;
