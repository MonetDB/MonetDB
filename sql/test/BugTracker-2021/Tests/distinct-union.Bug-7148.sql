select
distinct c1, c2
from (
select 'A' c0, 'a' c1, 1 c2
union all select 'A', 'a', 2
union all select 'A', 'b', 3
union all select 'B', 'a', 4
union all select 'B', 'b', 5
union all select 'C', 'c', 6
union all select 'C', 'a', 7
union all select 'C', 'b', 8
union all select 'C', 'c', 9
union all select 'D', 'd', 0
union all select 'F', 'a', 1
union all select 'F', 'b', 2
union all select 'E', 'c', 3
union all select 'G', 'd', 4
union all select 'G', 'e', 5
union all select 'G', 'a', 6
union all select 'G', 'b', 7
union all select 'H', 'c', 8
union all select 'A', 'd', 9
union all select 'B', 'e', 0
) T;

select
c1, c2
from (
select 'A' c0, 'a' c1, 1 c2
union all select 'A', 'a', 2
union all select 'A', 'b', 3
union all select 'B', 'a', 4
union all select 'B', 'b', 5
union all select 'C', 'c', 6
union all select 'C', 'a', 7
union all select 'C', 'b', 8
union all select 'C', 'c', 9
union all select 'D', 'd', 0
union all select 'F', 'a', 1
union all select 'F', 'b', 2
union all select 'E', 'c', 3
union all select 'G', 'd', 4
union all select 'G', 'e', 5
union all select 'G', 'a', 6
union all select 'G', 'b', 7
union all select 'H', 'c', 8
union all select 'A', 'd', 9
union all select 'B', 'e', 0
) T;

select
min(c1), min(c2)
from (
select 'A' c0, 'a' c1, 1 c2
union all select 'A', 'a', 2
union all select 'A', 'b', 3
union all select 'B', 'a', 4
union all select 'B', 'b', 5
union all select 'C', 'c', 6
union all select 'C', 'a', 7
union all select 'C', 'b', 8
union all select 'C', 'c', 9
union all select 'D', 'd', 0
union all select 'F', 'a', 1
union all select 'F', 'b', 2
union all select 'E', 'c', 3
union all select 'G', 'd', 4
union all select 'G', 'e', 5
union all select 'G', 'a', 6
union all select 'G', 'b', 7
union all select 'H', 'c', 8
union all select 'A', 'd', 9
union all select 'B', 'e', 0
) T;

select
c1, min(c2)
from (
select 'A' c0, 'a' c1, 1 c2
union all select 'A', 'a', 2
union all select 'A', 'b', 3
union all select 'B', 'a', 4
union all select 'B', 'b', 5
union all select 'C', 'c', 6
union all select 'C', 'a', 7
union all select 'C', 'b', 8
union all select 'C', 'c', 9
union all select 'D', 'd', 0
union all select 'F', 'a', 1
union all select 'F', 'b', 2
union all select 'E', 'c', 3
union all select 'G', 'd', 4
union all select 'G', 'e', 5
union all select 'G', 'a', 6
union all select 'G', 'b', 7
union all select 'H', 'c', 8
union all select 'A', 'd', 9
union all select 'B', 'e', 0
) T;

select
c1, min(c2)
from (
select 'A' c0, 'a' c1, 1 c2
except all select 'A', 'a', 2
) T;

select
min(c1), c2
from (
select 'A' c0, 'a' c1, 1 c2
except all select 'A', 'a', 2
) T;

select
min(c1), min(c2)
from (
select 'A' c0, 'a' c1, 1 c2
except all select 'A', 'a', 2
) T;

select
c1, min(c2)
from (
select 'A' c0, 'a' c1, 1 c2
intersect all select 'A', 'a', 2
) T;

select
min(c1), c2
from (
select 'A' c0, 'a' c1, 1 c2
intersect all select 'A', 'a', 2
) T;

select
min(c1), min(c2)
from (
select 'A' c0, 'a' c1, 1 c2
intersect all select 'A', 'a', 2
) T;

select
distinct c1, c2
from (
select 'A' c0, 'a' c1, 1 c2
except all select 'A', 'a', 2
except all select 'A', 'b', 3
except all select 'B', 'a', 4
except all select 'B', 'b', 5
except all select 'C', 'c', 6
except all select 'C', 'a', 7
except all select 'C', 'b', 8
except all select 'C', 'c', 9
except all select 'D', 'd', 0
except all select 'F', 'a', 1
except all select 'F', 'b', 2
except all select 'E', 'c', 3
except all select 'G', 'd', 4
except all select 'G', 'e', 5
except all select 'G', 'a', 6
except all select 'G', 'b', 7
except all select 'H', 'c', 8
except all select 'A', 'd', 9
except all select 'B', 'e', 0
) T;

select
distinct c1, c2
from (
select 'A' c0, 'a' c1, 1 c2
intersect all select 'A', 'a', 2
intersect all select 'A', 'b', 3
intersect all select 'B', 'a', 4
intersect all select 'B', 'b', 5
intersect all select 'C', 'c', 6
intersect all select 'C', 'a', 7
intersect all select 'C', 'b', 8
intersect all select 'C', 'c', 9
intersect all select 'D', 'd', 0
intersect all select 'F', 'a', 1
intersect all select 'F', 'b', 2
intersect all select 'E', 'c', 3
intersect all select 'G', 'd', 4
intersect all select 'G', 'e', 5
intersect all select 'G', 'a', 6
intersect all select 'G', 'b', 7
intersect all select 'H', 'c', 8
intersect all select 'A', 'd', 9
intersect all select 'B', 'e', 0
) T;
