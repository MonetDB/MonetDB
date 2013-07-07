WITH SAWITH0 AS ( select 'a' as c1,
                          'b' as c2,
                          'c' as c3,
                           1 as c4 )
select  0 as c1,
      D1.c1 as c2,
      D1.c2 as c3,
      D1.c3 as c4,
      D1.c4 as c5
from SAWITH0 D1;

WITH SAWITH0 AS ( select 'a' as c1,
                          'b' as c2,
                          'c' as c3,
                            1 as c4 )
select  0 as c1,
      D1.c1 as c2,
      D1.c2 as c3,
      D1.c3 as c4,
      D1.c4 as c5
from SAWITH0 D1
order by 5, 4, 3, 2;
--order by c5,c4,c3,c2

WITH SAWITH0 AS ( select 'a' as c1,
                          'b' as c2,
                          'c' as c3,
                            1 as c4 )
select  0 as c1,
      D1.c1 as a2,
      D1.c2 as a3,
      D1.c3 as a4,
      D1.c4 as a5
from SAWITH0 D1
order by 5, 4, 3, 2;

WITH SAWITH0 AS ( select 2 as c2 ),
      SAWITH1 AS ( select  5 as c2, null as c4
                  UNION ALL
                   select  5 as c2, 'x' as c4  )
( select
      cast(NULL as  VARCHAR ( 1 ) ) as c2,
      D1.c2 as c7
    from  SAWITH0 D1
union all
  select
      D1.c4 as c2,
      D1.c2 as c7
    from  SAWITH1 D1 );

WITH SAWITH0 AS ( select 2 as c2 ),
     SAWITH1 AS (select  5 as c2, null as c4
               UNION ALL
               select  5 as c2, 'x' as c4  )
(select
      cast(NULL as  VARCHAR ( 1 ) ) as c200000,
      D1.c2 as c7
from  SAWITH0 D1
union all
select
      D1.c4 as c200000,
      D1.c2 as c7
from  SAWITH1 D1 );

WITH SAWITH0 AS ( select 2 as c2 ),
     SAWITH1 AS (select  5 as c2, null as c4
               UNION ALL
               select  5 as c2, 'x' as c4  )
select
      cast(NULL as  VARCHAR ( 1 ) ) as c2,
      D1.c2 as c7
from  SAWITH0 D1;

WITH SAWITH0 AS ( select 2 as c2 ),
     SAWITH1 AS (select  5 as c2, null as c4
               UNION ALL
               select  5 as c2, 'x' as c4  )
select
      cast(NULL as  VARCHAR ( 1 ) ) as c2,
      D1.c2 as c7
from  SAWITH0 D1
ORDER BY 1;
