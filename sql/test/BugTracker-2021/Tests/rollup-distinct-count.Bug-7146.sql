select count(distinct b) from (
select 'a' b, 'A' c
union all select 'b', 'C'
) T group by rollup(c);

select c, count(distinct b) from (
select 1 a, 'a' b, 'A' c
union all select 2, 'b', 'C'
union all select 3, 'c', 'C'
union all select 4, 'a', 'A'
union all select 5, 'c', 'B'
union all select 6, 'd', 'A'
union all select 7, 'a', 'B'
union all select 8, 'b', 'D'
union all select 9, null, 'D'
) T
group by rollup(c);

select c, count(b) from (
select 1 a, 'a' b, 'A' c
union all select 2, 'b', 'C'
union all select 3, 'c', 'C'
union all select 4, 'a', 'A'
union all select 5, 'c', 'B'
union all select 6, 'd', 'A'
union all select 7, 'a', 'B'
union all select 8, 'b', 'D'
union all select 9, null, 'D'
) T
group by rollup(c);

select count(distinct b) from (
select 'a' b, 'A' c
union all select 'b', 'C'
) T group by cube(c);

select c, count(distinct b) from (
select 1 a, 'a' b, 'A' c
union all select 2, 'b', 'C'
union all select 3, 'c', 'C'
union all select 4, 'a', 'A'
union all select 5, 'c', 'B'
union all select 6, 'd', 'A'
union all select 7, 'a', 'B'
union all select 8, 'b', 'D'
union all select 9, null, 'D'
) T
group by cube(c);

select c, count(b) from (
select 1 a, 'a' b, 'A' c
union all select 2, 'b', 'C'
union all select 3, 'c', 'C'
union all select 4, 'a', 'A'
union all select 5, 'c', 'B'
union all select 6, 'd', 'A'
union all select 7, 'a', 'B'
union all select 8, 'b', 'D'
union all select 9, null, 'D'
) T
group by cube(c);
