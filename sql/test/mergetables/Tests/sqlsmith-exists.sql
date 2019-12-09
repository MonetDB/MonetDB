START TRANSACTION;
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);
CREATE TABLE LongTable (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO LongTable VALUES (1,7,2,1,1,909,1,1), (2,7,2,2,3,4,4,6), (NULL,5,4,81,NULL,5,-10,1), (-90,NULL,0,NULL,2,0,1,NULL);
CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);
CREATE TABLE tbl_ProductSales (col1 INT, col2 varchar(64), col3 varchar(64), col4 INT, col5 REAL, col6 date); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200, 1.2, date '2015-12-12'),(2,'Game','PKO Game',400, -1.0, date '2012-02-10'),(3,'Fashion','Shirt',500, NULL, date '1990-01-01'),
(4,'Fashion','Shorts',100, 102.45, date '2000-03-08'),(5,'Sport','Ball',0, 224.78, NULL);
CREATE TABLE analytics (aa INT, bb INT, cc BIGINT);
INSERT INTO analytics VALUES (15, 3, 15), (3, 1, 3), (2, 1, 2), (5, 3, 5), (NULL, 2, NULL), (3, 2, 3), (4, 1, 4), (6, 3, 6), (8, 2, 8), (NULL, 4, NULL);
CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES (97,1,99), (15,81,47),(87,21,10);
CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab1 VALUES (51,14,96), (85,5,59), (91,47,68);
CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab2 VALUES (64,77,40), (75,67,58),(46,51,23);

select
cast(coalesce(ref_0.col2, ref_0.col3) as int) as c0, ref_0.col4 as c1,
50 as c2,
ref_0.col1 as c3
from
another_T as ref_0
where (false)
or (exists (
    select
    ref_0.col4 as c0,
    ref_0.col2 as c1,
    ref_1.col6 as c2,
    ref_2.col1 as c3,
    ref_2.col5 as c4,
    ref_1.col4 as c5,
    cast(nullif(ref_1.col4, ref_1.col1) as int) as c6,
    ref_1.col6 as c7,
    73 as c8,
    ref_2.col2 as c9,
    ref_1.col2 as c10,
    ref_1.col1 as c11,
    ref_1.col5 as c12,
    ref_2.col3 as c13,
    case when exists (
        select
        ref_0.col3 as c0,
        ref_2.col2 as c1,
        ref_2.col2 as c2,
        ref_0.col5 as c3,
        ref_2.col4 as c4,
        ref_3.col8 as c5,
        ref_0.col3 as c6,
        ref_2.col6 as c7,
        ref_3.col2 as c8,
        ref_0.col1 as c9,
        ref_0.col6 as c10,
        ref_1.col1 as c11,
        ref_1.col3 as c12,
        ref_1.col4 as c13,
        ref_2.col4 as c14
        from 
        another_t as ref_3
        where ref_3.col3 is not null
    ) then ref_1.col1 
    else ref_1.col1 end as c14
    from
    LongTable as ref_1
    inner join tbl_ProductSales as ref_2
    on (ref_0.col3 is not null)
    where false)
    )
    limit 97; --empty on PostgreSQL

select 
 subq_0.c0 as c0, 
 subq_0.c2 as c1, 
 subq_0.c1 as c2, 
 subq_0.c1 as c3, 
 cast(nullif(73,
 73) as int) as c4, 
 subq_0.c2 as c5, 
 subq_0.c1 as c6, 
 subq_0.c1 as c7, 
 subq_0.c1 as c8, 
 subq_0.c0 as c9
from 
 (select 
 ref_1.col1 as c0, 
 cast(coalesce(ref_1.col2,
 case when true then ref_1.col2 else ref_1.col2 end
 ) as smallint) as c1, 
 ref_0.col2 as c2
 from 
 another_T as ref_0
 right join LongTable as ref_1
 on (exists (
 select 
 ref_0.col1 as c0, 
 ref_1.col2 as c1
 from 
 tab1 as ref_2
 where (exists (
 select 
 ref_3.col1 as c0, 
 ref_2.col2 as c1, 
 ref_3.col2 as c2, 
 ref_3.col2 as c3, 
 ref_3.col1 as c4, 
 ref_2.col0 as c5, 
 ref_0.col2 as c6, 
 ref_1.col3 as c7, 
 ref_2.col0 as c8, 
 88 as c9
 from 
 tab2 as ref_3
 where 69 is null)) 
 and ((false) 
 or ((true) 
 or (exists (
 select 
 ref_0.col2 as c0, 
 ref_0.col2 as c1, 
 ref_1.col3 as c2, 
 ref_0.col3 as c3, 
 ref_4.col0 as c4
 from 
 tab0 as ref_4
 where ref_2.col2 is not null))))))
 where (ref_0.col4 is null) 
 and (76 is null)
 limit 101) as subq_0
where true
limit 67; --empty on PostgreSQL

SELECT subq_1.c0 AS c0, 
       subq_1.c0 AS c1, 
       subq_0.c0 AS c2, 
       subq_1.c0 AS c3, 
       subq_1.c0 AS c4, 
       subq_0.c0 AS c5 
FROM   (SELECT ref_0.col0 AS c0 
        FROM   tab0 AS ref_0 
        WHERE  false 
        LIMIT  143) AS subq_0 
       LEFT JOIN (SELECT ref_1.col1 AS c0 
                  FROM   another_t AS ref_1 
                  WHERE  ( false ) 
                         AND ( EXISTS 
                         (SELECT ref_2.col0   AS c0, 
                                 ref_1.col2 AS c1, 
                                 ref_1.col3      AS c2 
                          FROM   tab1 AS 
                                 ref_2 
                               INNER JOIN tbl_ProductSales AS 
                                          ref_3 
                                       ON ( ( false ) 
                                            AND ( ( ref_3.col1 IS NULL ) 
                                                   OR ( ( true ) 
                                                        AND ( ( ( ( true ) 
                                                                   OR ( false ) 
                                                                ) 
                                                                 OR 
                                                        ( ( ref_1.col2 IS 
                                                            NULL 
                                                          ) 
                                                          AND ( ( true ) 
                                                                 OR ( true ) ) 
                                                        ) ) 
                                                              AND ( ( 
                                                            ( false ) 
                                                             OR ( ref_1.col3 IS 
                                                                  NULL 
                                                                ) 
                                                                    ) 
                                                                    AND ( false 
                                                                    ) 
                                                                  ) ) ) ) 
                                          ) 
                                       WHERE  ( ref_3.col2 IS NOT NULL ) 
                                               OR ( ( ( ( ref_3.col2 IS 
                                                          NOT NULL 
                                                        ) 
                                                        AND ( 55 IS NOT NULL ) ) 
                                                      AND ( ( ( ref_1.col3 IS 
                                                                NOT NULL 
                                                              ) 
                                                              AND ( false ) ) 
                                                            AND ( 79 IS NOT NULL 
                                                                ) ) ) 
                                                    AND ( EXISTS 
                                                    (SELECT ref_3.col1 
                                                            AS 
                                                            c0, 
       ref_4.col0 AS 
       c1, 
       ref_2.col1    AS 
       c2, 
       ref_3.col1       AS 
       c3, 
       ref_2.col2      AS 
       c4, 
       ref_1.col1      AS 
       c5 
       FROM   tab2 AS ref_4 
       WHERE  ( true ) 
       AND ( ref_2.col1 
             IS NULL ) 
       ) ) 
       )) ) 
       LIMIT  30) AS subq_1 
              ON ( subq_1.c0 IS NULL ) 
WHERE  subq_0.c0 IS NOT NULL; --empty on PostgreSQL

select 
 1
 from 
 tab1 as ref_0
 right join analytics as ref_1
 on (exists (
 select 
 1
 from 
 tbl_ProductSales as ref_3
 where ref_1.aa is null))
 right join tab2 as ref_2
 on (ref_0.col1 = ref_2.col0 );
-- 3 rows with 1

select 
 1
 from 
 tab1 as ref_0
 right join analytics as ref_1
 right join tab2 as ref_2
 on (exists (
 select 
 1
 from 
 tbl_ProductSales as ref_3
 where ref_1.aa is null))
 on (ref_0.col1 = ref_2.col0 );
-- 6 rows with 1

select 
 subq_0.c0 as c0, 
 subq_0.c0 as c1, 
 cast(coalesce(4,
 subq_0.c0) as int) as c2, 
 case when subq_0.c0 is null then subq_0.c0 else subq_0.c0 end
 as c3, 
 subq_0.c0 as c4, 
 subq_0.c0 as c5, 
 subq_0.c0 as c6, 
 subq_0.c0 as c7
from 
 (select 
 ref_4.col1 as c0
 from 
 tab1 as ref_0
 right join analytics as ref_1
 right join tab2 as ref_2
 on (exists (
 select 
 ref_2.col0 as c0, 
 ref_2.col1 as c1
 from 
 tbl_ProductSales as ref_3
 where ref_1.aa is null))
 on (ref_0.col1 = ref_2.col0 )
 inner join tab0 as ref_4
 on (ref_1.cc is not null)
 where true) as subq_0
where subq_0.c0 is not null
limit 107; --empty on PostgreSQL

SELECT subq_1.c3 AS c0
FROM
  (SELECT 1 AS c0,
          subq_0.c8 AS c1,
          subq_0.c6 AS c2,
          subq_0.c0 AS c3,
          subq_0.c4 AS c4,
          subq_0.c9 AS c5,
          subq_0.c6 AS c6,
          subq_0.c0 AS c7,
          subq_0.c3 AS c8,
          subq_0.c4 AS c9,
          CASE
              WHEN (94 IS NOT NULL)
                   OR (subq_0.c0 IS NULL) THEN subq_0.c4
              ELSE subq_0.c4
          END AS c10,
          subq_0.c5 AS c11,
          subq_0.c4 AS c12
   FROM
     (SELECT ref_0.i AS c0,
             ref_0.i AS c1,
             ref_0.i AS c2,
             ref_0.i AS c3,
             ref_0.i AS c4,
             ref_0.i AS c5,
             ref_0.i AS c6,
             ref_0.i AS c7,
             ref_0.i AS c8,
             ref_0.i AS c9,
             ref_0.i AS c10
      FROM integers AS ref_0
      WHERE (ref_0.i IS NOT NULL)
        OR (TRUE)) AS subq_0
   WHERE CASE
             WHEN (((((TRUE)
                      AND (TRUE))
                     AND (EXISTS
                            (SELECT subq_0.c3 AS c0,
                                    ref_1.col0 AS c1,
                                    subq_0.c1 AS c2,
                                    subq_0.c3 AS c3,
                                    subq_0.c10 AS c4
                             FROM tab0 AS ref_1
                             WHERE (ref_1.col0 IS NULL)
                               OR ((subq_0.c2 IS NULL)
                                   OR ((FALSE)
                                       AND ((ref_1.col0 IS NULL)
                                            OR ((TRUE)
                                                OR (ref_1.col1 IS NULL))))))))
                    OR (FALSE))
                   AND (((((TRUE)
                           AND (((EXISTS
                                    (SELECT 69 AS c0,
                                            ref_2.col1 AS c1,
                                            ref_2.col2 AS c2,
                                            ref_2.col3 AS c3,
                                            subq_0.c0 AS c4,
                                            ref_2.col4 AS c5,
                                            subq_0.c5 AS c6,
                                            subq_0.c8 AS c7,
                                            subq_0.c10 AS c8
                                     FROM tbl_productsales AS ref_2
                                     WHERE (FALSE)
                                       OR ((FALSE)
                                           AND ((TRUE)
                                                OR ((TRUE)
                                                    OR (TRUE))))))
                                 AND (TRUE))
                                OR ((FALSE)
                                    OR ((subq_0.c0 IS NOT NULL)
                                        OR (subq_0.c8 IS NULL)))))
                          AND (subq_0.c5 IS NOT NULL))
                         AND ((EXISTS
                                 (SELECT subq_0.c9 AS c0,
                                         subq_0.c6 AS c1,
                                         subq_0.c7 AS c2,
                                         subq_0.c7 AS c3,
                                         ref_3.col1 AS c4,
                                         ref_3.col2 AS c5,
                                         ref_3.col3 AS c6,
                                         subq_0.c8 AS c7,
                                         ref_3.col4 AS c8,
                                         ref_3.col5 AS c9,
                                         subq_0.c4 AS c10,
                                         ref_3.col6 AS c11,
                                         subq_0.c5 AS c12
                                  FROM another_t AS ref_3
                                  WHERE (ref_3.col5 IS NULL)
                                    OR (EXISTS
                                          (SELECT subq_0.c1 AS c0,
                                                  ref_3.col1 AS c1
                                           FROM analytics AS ref_4
                                           WHERE (TRUE)
                                             AND (FALSE)))))
                              OR ((TRUE)
                                  AND (subq_0.c9 IS NOT NULL))))
                        AND ((TRUE)
                             AND ((FALSE)
                                  AND (FALSE)))))
                  OR ((subq_0.c5 IS NOT NULL)
                      OR (FALSE)) THEN subq_0.c8
             ELSE subq_0.c8
         END IS NOT NULL) AS subq_1
WHERE TRUE
LIMIT 73;
-- On PostgreSQL it gives three rows
-- 1
-- 2
-- 3

select 
 subq_1.c1 as c0, 
 subq_1.c2 as c1, 
 subq_1.c2 as c2
from 
 (select 
 subq_0.c9 as c0, 
 subq_0.c7 as c1, 
 subq_0.c3 as c2, 
 subq_0.c6 as c3
 from 
 (select 
 ref_0.col1 as c0, 
 ref_0.col1 as c1, 
 87 as c2, 
 ref_0.col2 as c3, 
 ref_0.col3 as c4, 
 ref_0.col4 as c5, 
 ref_0.col4 as c6, 
 ref_0.col5 as c7, 
 ref_0.col6 as c8, 
 ref_0.col6 as c9
 from 
 another_T as ref_0
 where true) as subq_0
 where subq_0.c8 is not null
 limit 46) as subq_1
where false; --empty on PostgreSQL

select 
 subq_0.c4 as c0, 
 subq_0.c6 as c1, 
 subq_0.c2 as c2, 
 subq_0.c3 as c3, 
 subq_0.c5 as c4, 
 subq_0.c2 as c5, 
 subq_0.c3 as c6, 
 subq_0.c0 as c7
from 
 (select 
 ref_0.col1 as c0, 
 ref_1.col1 as c1, 
 ref_2.col1 as c2, 
 ref_1.col2 as c3, 
 ref_0.col2 as c4, 
 ref_0.col1 as c5, 
 ref_2.col7 as c6, 
 ref_1.col5 as c7
 from 
 another_t as ref_0
 inner join LongTable as ref_1
 on (true)
 right join LongTable as ref_2
 on (ref_0.col1 = ref_2.col6 )
 where (((ref_2.col5 is null) 
 and (true)) 
 and (true)) 
 and (true)
 limit 104) as subq_0
where (subq_0.c3 is not null) 
 or (subq_0.c0 is not null); --empty on PostgreSQL

select 
 cast(coalesce(subq_1.c0,
 subq_1.c1) as int) as c0
from 
 (select 
 subq_0.c6 as c0, 
 subq_0.c0 as c1
 from 
 (select 
 ref_1.col1 as c0, 
 ref_1.col1 as c1, 
 ref_0.col1 as c2, 
 ref_1.col1 as c3, 
 ref_0.col2 as c4, 
 ref_1.col1 as c5, 
 ref_0.col3 as c6, 
 ref_1.col6 as c7, 
 5 as c8, 
 ref_1.col1 as c9, 
 ref_1.col4 as c10, 
 ref_0.col1 as c11, 
 ref_0.col3 as c12
 from 
 LongTable as ref_0
 inner join another_T as ref_1
 on (ref_0.col3 = ref_1.col1 )
 where exists (
 select 
 ref_1.col4 as c0
 from 
 tab0 as ref_2
 where (((exists (
 select 
 ref_3.col0 as c0
 from 
 tab1 as ref_3
 where true)) 
 or (((true) 
 or (ref_1.col3 is not null)) 
 and (false))) 
 and ((exists (
 select 
 87 as c0, 
 ref_1.col2 as c1, 
 ref_1.col3 as c2, 
 ref_0.col1 as c3, 
 ref_4.col0 as c4, 
 ref_4.col1 as c5, 
 ref_1.col1 as c6, 
 ref_1.col1 as c7, 
 ref_2.col0 as c8, 
 ref_4.col0 as c9
 from 
 tab2 as ref_4
 where ref_1.col5 is not null)) 
 and (ref_2.col1 is null))) 
 and (ref_1.col3 is null))
 limit 26) as subq_0
 where (subq_0.c3 is null) 
 and ((false) 
 or (true))) as subq_1
where (subq_1.c1 is not null) 
 and (subq_1.c0 is not null)
limit 79; --empty on PostgreSQL

select 
 subq_2.c18 as c0, 
 subq_0.c0 as c1, 
 cast(coalesce(subq_0.c1,
 subq_2.c5) as int) as c2, 
 92 as c3, 
 subq_0.c1 as c4, 
 subq_2.c21 as c5, 
 subq_2.c24 as c6, 
 subq_2.c11 as c7, 
 subq_2.c13 as c8, 
 subq_2.c16 as c9, 
 case when subq_2.c26 is null then subq_2.c20 else subq_2.c20 end
 as c10, 
 subq_0.c0 as c11, 
 subq_0.c0 as c12, 
 subq_0.c1 as c13, 
 subq_0.c1 as c14
from 
 (select 
 ref_0.col7 as c0, 
 ref_0.col2 as c1
 from 
 another_t as ref_0
 where (true) 
 and ((ref_0.col4 is not null) 
 and (ref_0.col5 is not null))
 limit 58) as subq_0
 inner join (select 
 subq_1.c15 as c0, 
 subq_1.c1 as c1, 
 subq_1.c7 as c2, 
 subq_1.c13 as c3, 
 subq_1.c17 as c4, 
 subq_1.c21 as c5, 
 subq_1.c12 as c6, 
 subq_1.c0 as c7, 
 subq_1.c22 as c8, 
 subq_1.c20 as c9, 
 subq_1.c17 as c10, 
 subq_1.c12 as c11, 
 subq_1.c10 as c12, 
 subq_1.c5 as c13, 
 subq_1.c3 as c14, 
 subq_1.c14 as c15, 
 subq_1.c2 as c16, 
 subq_1.c20 as c17, 
 subq_1.c4 as c18, 
 subq_1.c20 as c19, 
 subq_1.c12 as c20, 
 subq_1.c11 as c21, 
 subq_1.c3 as c22, 
 subq_1.c3 as c23, 
 subq_1.c6 as c24, 
 subq_1.c4 as c25, 
 85 as c26, 
 subq_1.c0 as c27, 
 subq_1.c12 as c28, 
 subq_1.c16 as c29, 
 subq_1.c19 as c30
 from 
 (select 
 ref_1.col6 as c0, 
 ref_1.col1 as c1, 
 18 as c2, 
 ref_1.col8 as c3, 
 ref_1.col4 as c4, 
 ref_1.col1 as c5, 
 ref_1.col2 as c6, 
 ref_1.col5 as c7, 
 ref_1.col7 as c8, 
 ref_1.col3 as c9, 
 ref_1.col4 as c10, 
 ref_1.col2 as c11, 
 ref_1.col3 as c12, 
 ref_1.col4 as c13, 
 ref_1.col6 as c14, 
 ref_1.col3 as c15, 
 ref_1.col4 as c16, 
 ref_1.col5 as c17, 
 ref_1.col2 as c18, 
 ref_1.col2 as c19, 
 ref_1.col2 as c20, 
 ref_1.col8 as c21, 
 ref_1.col6 as c22
 from 
 another_t as ref_1
 where true
 limit 133) as subq_1
 where (false) 
 and ((subq_1.c2 is not null) 
 or (false))
 limit 115) as subq_2
 on (((true) 
 and (subq_2.c3 is null)) 
 or (true))
where case when subq_0.c1 is null then subq_2.c25 else subq_2.c25 end
 is null
limit 81; --empty on PostgreSQL

ROLLBACK;
