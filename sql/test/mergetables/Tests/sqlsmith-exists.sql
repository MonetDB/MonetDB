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

SELECT 1 FROM integers WHERE CASE WHEN EXISTS (SELECT 1 WHERE i IS NULL) THEN true ELSE true END;
-- 1
-- 1
-- 1
-- 1

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

SELECT
        subq_2.c3 AS c0
        ,subq_2.c6 AS c1
    FROM
        (
            SELECT
                    36 AS c0
                    ,ref_0.cc AS c1
                FROM
                    analytics AS ref_0
                WHERE
                    FALSE LIMIT 32
        ) AS subq_0 INNER JOIN (
            SELECT
                    ref_1.bb AS c0
                    ,ref_1.cc AS c1
                    ,ref_1.cc AS c2
                    ,ref_1.aa AS c3
                    ,ref_1.bb AS c4
                    ,ref_1.aa AS c5
                    ,ref_1.aa AS c6
                FROM
                    analytics AS ref_1
                WHERE
                    ref_1.bb IS NOT NULL LIMIT 83
        ) AS subq_1 RIGHT JOIN (
            SELECT
                    ref_2.col2 AS c0
                    ,ref_2.col4 AS c1
                    ,ref_3.col1 AS c2
                    ,ref_2.col8 AS c3
                    ,ref_3.col2 AS c4
                    ,ref_3.col1 AS c5
                    ,ref_2.col2 AS c6
                    ,24 AS c7
                FROM
                    longtable AS ref_2 LEFT JOIN tab1 AS ref_3
                        ON (
                        (
                            ref_3.col0 IS NULL
                        )
                        OR (TRUE)
                    )
                WHERE
                    TRUE LIMIT 138
        ) AS subq_2
            ON (
            (
                46 IS NULL
            )
            OR (
                20 IS NOT NULL
            )
        )
            ON (
            (
                EXISTS (
                    SELECT
                            subq_1.c4 AS c0
                            ,ref_4.col0 AS c1
                        FROM
                            tab1 AS ref_4
                        WHERE
                            (
                                subq_2.c5 IS NULL
                            )
                            AND (TRUE)
                )
            )
            OR (
                subq_0.c0 IS NOT NULL
            )
        )
    WHERE
        (
            (FALSE)
            AND (
                (
                    subq_2.c2 IS NULL
                )
                OR (
                    (
                        subq_2.c7 IS NULL
                    )
                    AND (TRUE)
                )
            )
        )
        OR (
            (
                (
                    subq_2.c2 IS NOT NULL
                )
                OR (
                    (
                        subq_0.c1 IS NOT NULL
                    )
                    OR (TRUE)
                )
            )
            AND (
                subq_0.c1 IS NULL
            )
        ) LIMIT 76; --empty on PostgreSQL

select 
 subq_1.c0 as c0
from 
 (select 
 subq_0.c9 as c0
 from 
 (select 
 ref_3.aa as c0, 
 ref_3.cc as c1, 
 ref_2.col2 as c2, 
 ref_2.col2 as c3, 
 ref_0.col1 as c4, 
 ref_1.col3 as c5, 
 ref_3.aa as c6, 
 ref_1.col2 as c7, 
 ref_2.col2 as c8, 
 ref_1.col8 as c9, 
 ref_2.col1 as c10, 
 ref_0.col2 as c11, 
 ref_2.col0 as c12, 
 ref_0.col1 as c13, 
 ref_2.col1 as c14
 from 
 tab1 as ref_0
 left join another_t as ref_1
 on (ref_0.col2 is null)
 inner join tab0 as ref_2
 left join analytics as ref_3
 on (ref_2.col1 is not null)
 on ((true) 
 and (true))
 where true
 limit 48) as subq_0
 where (exists (
 select 
 subq_0.c5 as c0, 
 subq_0.c14 as c1, 
 ref_4.col1 as c2
 from 
 tab1 as ref_4
 where false)) 
 or ((subq_0.c14 is null) 
 and (false))) as subq_1
where exists (
 select 
 ref_5.col6 as c0, 
 ref_7.col1 as c1, 
 cast(nullif(ref_6.col0,
 6) as int) as c2, 
 ref_7.col2 as c3, 
 ref_5.col3 as c4, 
 subq_1.c0 as c5
 from 
 longtable as ref_5
 inner join tab1 as ref_6
 on ((subq_1.c0 is null) 
 and (((false) 
 and ((ref_5.col7 is null) 
 or ((ref_5.col3 is not null) 
 and (ref_6.col2 is not null)))) 
 or (true)))
 left join tab0 as ref_7
 on (ref_6.col0 = ref_7.col0 )
 where case when ref_5.col1 is null then 44 else 44 end
 is not null)
limit 99; --empty on PostgreSQL

select 
 subq_1.c0 as c0
from 
 (select 
 case when ((false) 
 or (subq_0.c3 is not null)) 
 and (true) then subq_0.c11 else subq_0.c11 end
 as c0, 
 case when subq_0.c0 is null then subq_0.c5 else subq_0.c5 end
 as c1, 
 subq_0.c6 as c2, 
 case when true then subq_0.c3 else subq_0.c3 end
 as c3, 
 subq_0.c3 as c4, 
 subq_0.c0 as c5, 
 subq_0.c6 as c6, 
 subq_0.c3 as c7, 
 subq_0.c8 as c8, 
 42 as c9, 
 subq_0.c6 as c10
 from 
 (select 
 ref_0.col0 as c0, 
 85 as c1, 
 ref_0.col1 as c2, 
 ref_0.col2 as c3, 
 ref_0.col1 as c4, 
 ref_0.col0 as c5, 
 ref_0.col2 as c6, 
 ref_0.col0 as c7, 
 ref_0.col0 as c8, 
 ref_0.col2 as c9, 
 ref_0.col1 as c10, 
 ref_0.col2 as c11
 from 
 tab1 as ref_0
 where ref_0.col0 is not null
 limit 153) as subq_0
 where (exists (
 select 
 ref_1.col2 as c0, 
 subq_0.c3 as c1, 
 subq_0.c10 as c2, 
 ref_1.col3 as c3
 from 
 longtable as ref_1
 where false)) 
 or ((false) 
 or (subq_0.c10 is null))) as subq_1
where (subq_1.c9 is not null) 
 and (exists (
 select 
 subq_2.c4 as c0
 from 
 (select 
 subq_1.c3 as c0, 
 subq_1.c10 as c1, 
 ref_3.col0 as c2, 
 subq_1.c9 as c3, 
 ref_2.col2 as c4
 from 
 tab1 as ref_2
 right join tab1 as ref_3
 on ((ref_2.col1 is null) 
 and (true))
 where ((false) 
 and (ref_2.col0 is not null)) 
 or ((ref_3.col1 is not null) 
 and (false))) as subq_2
 right join longtable as ref_4
 inner join tbl_productsales as ref_5
 right join longtable as ref_6
 on (ref_5.col4 = ref_6.col1 )
 left join tab0 as ref_7
 on (true)
 on (ref_7.col0 is null)
 on ((subq_2.c4 is null) 
 or (ref_4.col2 is null))
 where ((true) 
 and (ref_6.col7 is not null)) 
 or ((false) 
 or ((ref_4.col5 is null) 
 or (ref_7.col1 is null)))))
limit 111; --empty on PostgreSQL

SELECT
        subq_1.c2 AS c0
        ,subq_1.c0 AS c1
    FROM
        (
            SELECT
                    subq_0.c7 AS c0
                    ,subq_0.c7 AS c1
                    ,CASE
                        WHEN TRUE
                        THEN subq_0.c7
                        ELSE subq_0.c7
                    END AS c2
                FROM
                    (
                        SELECT
                                ref_0.col1 AS c0
                                ,89 AS c1
                                ,ref_0.col2 AS c2
                                ,ref_0.col5 AS c3
                                ,93 AS c4
                                ,ref_0.col2 AS c5
                                ,ref_0.col4 AS c6
                                ,ref_0.col2 AS c7
                                ,ref_0.col5 AS c8
                                ,ref_0.col1 AS c9
                                ,ref_0.col1 AS c10
                                ,ref_0.col3 AS c11
                                ,ref_0.col3 AS c12
                            FROM
                                tbl_productsales AS ref_0
                            WHERE
                                (
                                    (FALSE)
                                    OR (
                                        EXISTS (
                                            SELECT
                                                    ref_0.col6 AS c0
                                                    ,ref_0.col3 AS c1
                                                    ,ref_1.col0 AS c2
                                                    ,ref_1.col2 AS c3
                                                    ,ref_0.col1 AS c4
                                                    ,ref_1.col0 AS c5
                                                    ,69 AS c6
                                                    ,ref_1.col2 AS c7
                                                    ,ref_0.col6 AS c8
                                                    ,ref_1.col1 AS c9
                                                FROM
                                                    tab1 AS ref_1
                                                WHERE
                                                    (FALSE)
                                                    AND (
                                                        (
                                                            (
                                                                ref_1.col2 IS NOT NULL
                                                            )
                                                            OR (
                                                                ref_0.col2 IS NOT NULL
                                                            )
                                                        )
                                                        AND (TRUE)
                                                    )
                                        )
                                    )
                                )
                                AND (
                                    (
                                        EXISTS (
                                            SELECT
                                                    ref_0.col4 AS c0
                                                FROM
                                                    analytics AS ref_2
                                                WHERE
                                                    (
                                                        (
                                                            ref_0.col4 IS NULL
                                                        )
                                                        AND (TRUE)
                                                    )
                                                    AND (TRUE)
                                        )
                                    )
                                    AND (FALSE)
                                ) LIMIT 137
                    ) AS subq_0
                WHERE
                    (
                        (
                            subq_0.c12 IS NOT NULL
                        )
                        AND (TRUE)
                    )
                    AND (
                        (
                            subq_0.c3 IS NOT NULL
                        )
                        AND (
                            (TRUE)
                            OR (
                                (
                                    subq_0.c3 IS NOT NULL
                                )
                                OR (
                                    (
                                        EXISTS (
                                            SELECT
                                                    subq_0.c11 AS c0
                                                    ,subq_0.c9 AS c1
                                                    ,subq_0.c7 AS c2
                                                    ,50 AS c3
                                                    ,ref_3.bb AS c4
                                                FROM
                                                    analytics AS ref_3
                                                WHERE
                                                    ref_3.bb IS NULL
                                        )
                                    )
                                    OR (FALSE)
                                )
                            )
                        )
                    )
        ) AS subq_1
    WHERE
        (TRUE)
        AND (
            (
                (
                    EXISTS (
                        SELECT
                                ref_5.col1 AS c0
                            FROM
                                tab1 AS ref_4 LEFT JOIN tab2 AS ref_5
                                    ON (
                                    (TRUE)
                                    OR (
                                        (
                                            EXISTS (
                                                SELECT
                                                        subq_1.c2 AS c0
                                                        ,subq_1.c2 AS c1
                                                        ,25 AS c2
                                                        ,ref_5.col2 AS c3
                                                        ,ref_6.col4 AS c4
                                                        ,subq_1.c0 AS c5
                                                        ,ref_6.col5 AS c6
                                                        ,ref_4.col0 AS c7
                                                        ,subq_1.c0 AS c8
                                                        ,ref_4.col1 AS c9
                                                        ,ref_5.col2 AS c10
                                                        ,45 AS c11
                                                        ,subq_1.c2 AS c12
                                                        ,ref_4.col1 AS c13
                                                        ,ref_5.col2 AS c14
                                                        ,ref_4.col2 AS c15
                                                        ,ref_6.col7 AS c16
                                                        ,24 AS c17
                                                        ,subq_1.c1 AS c18
                                                        ,subq_1.c0 AS c19
                                                        ,ref_6.col3 AS c20
                                                        ,ref_4.col1 AS c21
                                                        ,ref_6.col2 AS c22
                                                        ,86 AS c23
                                                    FROM
                                                        longtable AS ref_6
                                                    WHERE
                                                        TRUE
                                            )
                                        )
                                        AND (
                                            (FALSE)
                                            OR (FALSE)
                                        )
                                    )
                                )
                            WHERE
                                subq_1.c1 IS NOT NULL
                    )
                )
                OR (TRUE)
            )
            AND (
                (
                    EXISTS (
                        SELECT
                                ref_8.col6 AS c0
                                ,subq_1.c0 AS c1
                                ,subq_1.c0 AS c2
                                ,ref_7.col2 AS c3
                                ,subq_1.c1 AS c4
                                ,ref_7.col6 AS c5
                                ,ref_9.col0 AS c6
                                ,ref_7.col4 AS c7
                            FROM
                                another_t AS ref_7 LEFT JOIN tbl_productsales AS ref_8 INNER JOIN tab2 AS ref_9
                                    ON (
                                    ref_8.col1 = ref_9.col0
                                )
                                    ON (
                                    (
                                        (
                                            (FALSE)
                                            OR (FALSE)
                                        )
                                        OR (
                                            (
                                                (
                                                    (
                                                        (
                                                            subq_1.c1 IS NULL
                                                        )
                                                        OR (
                                                            (
                                                                (FALSE)
                                                                OR (
                                                                    EXISTS (
                                                                        SELECT
                                                                                ref_9.col2 AS c0
                                                                            FROM
                                                                                longtable AS ref_10
                                                                            WHERE
                                                                                TRUE
                                                                    )
                                                                )
                                                            )
                                                            OR (
                                                                (
                                                                    (
                                                                        ref_9.col2 IS NULL
                                                                    )
                                                                    AND (
                                                                        (
                                                                            EXISTS (
                                                                                SELECT
                                                                                        subq_1.c0 AS c0
                                                                                        ,ref_11.cc AS c1
                                                                                        ,ref_8.col4 AS c2
                                                                                    FROM
                                                                                        analytics AS ref_11
                                                                                    WHERE
                                                                                        subq_1.c0 IS NOT NULL
                                                                            )
                                                                        )
                                                                        AND (TRUE)
                                                                    )
                                                                )
                                                                OR (
                                                                    (FALSE)
                                                                    AND (
                                                                        (FALSE)
                                                                        AND (FALSE)
                                                                    )
                                                                )
                                                            )
                                                        )
                                                    )
                                                    AND (
                                                        subq_1.c2 IS NULL
                                                    )
                                                )
                                                AND (
                                                    ref_9.col1 IS NULL
                                                )
                                            )
                                            OR (
                                                (
                                                    18 IS NOT NULL
                                                )
                                                OR (
                                                    (FALSE)
                                                    OR (
                                                        (FALSE)
                                                        AND (FALSE)
                                                    )
                                                )
                                            )
                                        )
                                    )
                                    AND (
                                        ref_9.col1 IS NOT NULL
                                    )
                                )
                            WHERE
                                (TRUE)
                                OR (
                                    (
                                        ref_9.col1 IS NULL
                                    )
                                    AND (
                                        ref_7.col5 IS NOT NULL
                                    )
                                )
                    )
                )
                AND (
                    subq_1.c1 IS NULL
                )
            )
        )
; --empty

select
  subq_0.c1 as c0,
  subq_0.c1 as c1,
  subq_0.c1 as c2,
  cast(
    nullif(
      subq_0.c3,
      cast(
        nullif(
          subq_0.c2,
          case when (
            (subq_0.c1 is not null)
            and (subq_0.c3 is not null)
          )
          or (
            (
              (subq_0.c4 is not null)
              or (
                (
                  (
                    (true)
                    or (
                      (
                        exists (
                          select
                            ref_5.bb as c0,
                            ref_5.aa as c1
                          from
                            analytics as ref_5
                          where
                            true
                        )
                      )
                      and (
                        (false)
                        or (
                          (false)
                          and (
                            (75 is null)
                            and (
                              (
                                (true)
                                and (
                                  (true)
                                  and (subq_0.c1 is not null)
                                )
                              )
                              or (subq_0.c3 is null)
                            )
                          )
                        )
                      )
                    )
                  )
                  or (
                    (false)
                    or (
                      exists (
                        select
                          subq_0.c1 as c0,
                          32 as c1,
                          subq_0.c2 as c2,
                          ref_6.col7 as c3,
                          subq_0.c0 as c4,
                          ref_6.col7 as c5,
                          subq_0.c1 as c6
                        from
                          longtable as ref_6
                        where
                          (
                            (
                              (false)
                              and (ref_6.col8 is not null)
                            )
                            or (ref_6.col8 is null)
                          )
                          and (subq_0.c3 is null)
                      )
                    )
                  )
                )
                and (false)
              )
            )
            or (
              (subq_0.c3 is null)
              or (subq_0.c2 is null)
            )
          ) then subq_0.c1 else subq_0.c1 end
        ) as int
      )
    ) as int
  ) as c3,
  subq_0.c0 as c4
from
  (
    select
      ref_4.aa as c0,
      ref_1.col0 as c1,
      ref_2.col1 as c2,
      ref_2.col2 as c3,
      ref_2.col0 as c4
    from
      tbl_productsales as ref_0
      inner join tab2 as ref_1 on (
        (false)
        or (false)
      )
      right join tab0 as ref_2 on (
        (
          exists (
            select
              ref_2.col2 as c0,
              ref_0.col6 as c1,
              ref_3.i as c2,
              ref_1.col1 as c3,
              ref_1.col0 as c4,
              ref_2.col0 as c5,
              ref_3.i as c6,
              ref_0.col3 as c7
            from
              integers as ref_3
            where
              (true)
              or (
                (ref_3.i is not null)
                or (true)
              )
          )
        )
        and (false)
      )
      inner join analytics as ref_4 on (ref_4.bb is null)
    where
      (ref_1.col1 is not null)
      or (true)
    limit
      76
  ) as subq_0
where
  subq_0.c0 is null
limit 92; --empty

select 
 34 as c0, 
 ref_7.col1 as c1
from 
 (select 
 cast(nullif(ref_1.bb,
 ref_0.col1) as int) as c0, 
 ref_1.bb as c1, 
 ref_0.col2 as c2, 
 ref_1.bb as c3, 
 ref_0.col2 as c4, 
 cast(nullif(ref_0.col0,
 ref_1.bb) as int) as c5, 
 ref_0.col1 as c6, 
 ref_1.bb as c7, 
 ref_0.col2 as c8, 
 ref_0.col0 as c9, 
 ref_1.cc as c10, 
 ref_0.col0 as c11, 
 ref_1.aa as c12
 from 
 tab0 as ref_0
 inner join analytics as ref_1
 on (((ref_1.aa is null) 
 or ((ref_1.bb is null) 
 or ((63 is null) 
 or ((ref_1.bb is not null) 
 and (false))))) 
 or ((ref_0.col0 is not null) 
 or ((ref_1.aa is not null) 
 or ((((((false) 
 and (exists (
 select 
 ref_2.i as c0, 
 ref_2.i as c1, 
 ref_0.col2 as c2, 
 ref_2.i as c3, 
 ref_1.aa as c4, 
 ref_0.col0 as c5, 
 ref_2.i as c6
 from 
 integers as ref_2
 where ref_0.col1 is null))) 
 or (true)) 
 or (true)) 
 and (ref_1.aa is not null)) 
 or (ref_1.aa is not null)))))
 where exists (
 select 
 ref_3.col2 as c0, 
 ref_0.col2 as c1, 
 ref_0.col1 as c2, 
 ref_1.aa as c3, 
 ref_3.col0 as c4, 
 ref_3.col0 as c5
 from 
 tab2 as ref_3
 right join analytics as ref_4
 on (((false) 
 and (ref_1.aa is not null)) 
 or (ref_4.bb is not null))
 where ref_0.col2 is not null)
 limit 183) as subq_0
 right join another_t as ref_5
 right join longtable as ref_6
 inner join longtable as ref_7
 right join tbl_productsales as ref_8
 inner join integers as ref_9
 on (ref_8.col4 = ref_9.i )
 on (ref_9.i is not null)
 on ((ref_7.col5 is not null) 
 and (ref_6.col5 is not null))
 left join (select 
 ref_10.i as c0, 
 ref_10.i as c1, 
 ref_10.i as c2, 
 ref_10.i as c3, 
 ref_10.i as c4, 
 ref_10.i as c5
 from 
 integers as ref_10
 where 67 is null
 limit 151) as subq_1
 on (subq_1.c1 is null)
 on (ref_5.col5 = ref_6.col1 )
 on (ref_5.col7 is null)
where ref_7.col8 is not null
limit 151; --empty

ROLLBACK;
