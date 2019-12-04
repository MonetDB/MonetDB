START TRANSACTION;
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);

select
cast(coalesce(ref_0.id, ref_0.owner) as int) as c0, ref_0.system as c1,
50 as c2,
ref_0.name as c3
from
sys.schemas as ref_0
where (false)
or (exists (
    select
    ref_0.system as c0,
    ref_0.id as c1,
    ref_1.type_name as c2,
    ref_2.owner as c3,
    ref_2.optimize as c4,
    ref_1.function_type as c5,
    cast(nullif(ref_1.function_type, ref_1.function_id) as int) as c6,
    ref_1.type_name as c7,
    73 as c8,
    ref_2.query as c9,
    ref_1.function_name as c10,
    ref_1.function_id as c11,
    ref_1.type_id as c12,
    ref_2.pipe as c13,
    case when exists (
        select
        ref_0.owner as c0,
        ref_2.query as c1,
        ref_2.query as c2,
        ref_0.authorization as c3,
        ref_2.plan as c4,
        ref_3.col8 as c5,
        ref_0.owner as c6,
        ref_2.mal as c7,
        ref_3.col2 as c8,
        ref_0.name as c9,
        ref_2.defined as c10,
        ref_1.function_id as c11,
        ref_1.depend_type as c12,
        ref_1.function_type as c13,
        ref_2.plan as c14
        from 
        sys.another_t as ref_3
        where ref_3.col3 is not null
    ) then ref_1.function_id 
    else ref_1.function_id end as c14
    from
    sys.dependency_functions_on_types as ref_1
    inner join sys.querylog_catalog as ref_2
    on (ref_0.owner is not null)
    where false)
    )
    limit 97;

CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab1 VALUES (51,14,96), (85,5,59), (91,47,68);

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
 ref_1.schema_name as c0, 
 cast(coalesce(ref_1.depend_type,
 case when true then ref_1.depend_type else ref_1.depend_type end
 ) as smallint) as c1, 
 ref_0.action as c2
 from 
 tmp.keys as ref_0
 right join sys.dependency_schemas_on_users as ref_1
 on (exists (
 select 
 ref_0.table_id as c0, 
 ref_1.depend_type as c1
 from 
 sys.tab1 as ref_2
 where (exists (
 select 
 ref_3.owner_name as c0, 
 ref_2.col2 as c1, 
 ref_3.depend_type as c2, 
 ref_3.depend_type as c3, 
 ref_3.owner_name as c4, 
 ref_2.col0 as c5, 
 ref_0.action as c6, 
 ref_1.schema_id as c7, 
 ref_2.col0 as c8, 
 88 as c9
 from 
 sys.dependency_owners_on_schemas as ref_3
 where 69 is null)) 
 and ((false) 
 or ((true) 
 or (exists (
 select 
 ref_0.action as c0, 
 ref_0.action as c1, 
 ref_1.schema_id as c2, 
 ref_0.rkey as c3, 
 ref_4.system as c4
 from 
 sys._tables as ref_4
 where ref_2.col2 is not null))))))
 where (ref_0.name is null) 
 and (76 is null)
 limit 101) as subq_0
where true
limit 67
;

SELECT subq_1.c0 AS c0, 
       subq_1.c0 AS c1, 
       subq_0.c0 AS c2, 
       subq_1.c0 AS c3, 
       subq_1.c0 AS c4, 
       subq_0.c0 AS c5 
FROM   (SELECT ref_0.function_type_name AS c0 
        FROM   sys.function_types AS ref_0 
        WHERE  false 
        LIMIT  143) AS subq_0 
       LEFT JOIN (SELECT ref_1.table_id AS c0 
                  FROM   sys.table_partitions AS ref_1 
                  WHERE  ( false ) 
                         AND ( EXISTS 
                         (SELECT ref_2.fk_name   AS c0, 
                                 ref_1.column_id AS c1, 
                                 ref_1.type      AS c2 
                          FROM   sys.dependency_tables_on_foreignkeys AS 
                                 ref_2 
                               INNER JOIN sys.dependency_keys_on_foreignkeys AS 
                                          ref_3 
                                       ON ( ( false ) 
                                            AND ( ( ref_3.fk_name IS NULL ) 
                                                   OR ( ( true ) 
                                                        AND ( ( ( ( true ) 
                                                                   OR ( false ) 
                                                                ) 
                                                                 OR 
                                                        ( ( ref_1.column_id IS 
                                                            NULL 
                                                          ) 
                                                          AND ( ( true ) 
                                                                 OR ( true ) ) 
                                                        ) ) 
                                                              AND ( ( 
                                                            ( false ) 
                                                             OR ( ref_1.type IS 
                                                                  NULL 
                                                                ) 
                                                                    ) 
                                                                    AND ( false 
                                                                    ) 
                                                                  ) ) ) ) 
                                          ) 
                                       WHERE  ( ref_3.depend_type IS NOT NULL ) 
                                               OR ( ( ( ( ref_3.depend_type IS 
                                                          NOT NULL 
                                                        ) 
                                                        AND ( 55 IS NOT NULL ) ) 
                                                      AND ( ( ( ref_1.type IS 
                                                                NOT NULL 
                                                              ) 
                                                              AND ( false ) ) 
                                                            AND ( 79 IS NOT NULL 
                                                                ) ) ) 
                                                    AND ( EXISTS 
                                                    (SELECT ref_3.key_name 
                                                            AS 
                                                            c0, 
       ref_4.table_type_id AS 
       c1, 
       ref_2.table_name    AS 
       c2, 
       ref_3.fk_name       AS 
       c3, 
       ref_2.key_type      AS 
       c4, 
       ref_1.table_id      AS 
       c5 
       FROM   sys.table_types AS ref_4 
       WHERE  ( true ) 
       AND ( ref_2.table_name 
             IS NULL ) 
       ) ) 
       )) ) 
       LIMIT  30) AS subq_1 
              ON ( subq_1.c0 IS NULL ) 
WHERE  subq_0.c0 IS NOT NULL;

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
 ref_4.type as c0
 from 
 sys.storagemodel as ref_0
 right join sys.triggers as ref_1
 right join sys.environment as ref_2
 on (exists (
 select 
 ref_2.name as c0, 
 ref_2.value as c1
 from 
 sys.dependency_owners_on_schemas as ref_3
 where ref_1.table_id is null))
 on (ref_0.type = ref_2.name )
 inner join sys.idxs as ref_4
 on (ref_1.time is not null)
 where true) as subq_0
where subq_0.c0 is not null
limit 107
;

CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);
CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
create table analytics (aa int, bb int, cc bigint);
insert into analytics values (15, 3, 15), (3, 1, 3), (2, 1, 2), (5, 3, 5), (NULL, 2, NULL), (3, 2, 3), (4, 1, 4), (6, 3, 6), (8, 2, 8), (NULL, 4, NULL);

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
      FROM sys.integers AS ref_0
      WHERE (ref_0.i IS NOT NULL)
        OR (TRUE)) AS subq_0
   WHERE CASE
             WHEN (((((TRUE)
                      AND (TRUE))
                     AND (EXISTS
                            (SELECT subq_0.c3 AS c0,
                                    ref_1.value AS c1,
                                    subq_0.c1 AS c2,
                                    subq_0.c3 AS c3,
                                    subq_0.c10 AS c4
                             FROM sys.value_partitions AS ref_1
                             WHERE (ref_1.value IS NULL)
                               OR ((subq_0.c2 IS NULL)
                                   OR ((FALSE)
                                       AND ((ref_1.value IS NULL)
                                            OR ((TRUE)
                                                OR (ref_1.table_id IS NULL))))))))
                    OR (FALSE))
                   AND (((((TRUE)
                           AND (((EXISTS
                                    (SELECT 69 AS c0,
                                            ref_2.product_category AS c1,
                                            ref_2.product_name AS c2,
                                            ref_2.totalsales AS c3,
                                            subq_0.c0 AS c4,
                                            ref_2.colid AS c5,
                                            subq_0.c5 AS c6,
                                            subq_0.c8 AS c7,
                                            subq_0.c10 AS c8
                                     FROM sys.tbl_productsales AS ref_2
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
                                         ref_3.name AS c4,
                                         ref_3.cycle AS c5,
                                         ref_3.cacheinc AS c6,
                                         subq_0.c8 AS c7,
                                         ref_3.minvalue AS c8,
                                         ref_3.start AS c9,
                                         subq_0.c4 AS c10,
                                         ref_3.id AS c11,
                                         subq_0.c5 AS c12
                                  FROM sys.sequences AS ref_3
                                  WHERE (ref_3.start IS NULL)
                                    OR (EXISTS
                                          (SELECT subq_0.c1 AS c0,
                                                  ref_3.name AS c1
                                           FROM sys.analytics AS ref_4
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
 ref_0.depend_type as c0, 
 ref_0.depend_type as c1, 
 87 as c2, 
 ref_0.index_type as c3, 
 ref_0.table_id as c4, 
 ref_0.index_id as c5, 
 ref_0.index_id as c6, 
 ref_0.table_name as c7, 
 ref_0.table_schema_id as c8, 
 ref_0.table_schema_id as c9
 from 
 sys.dependency_tables_on_indexes as ref_0
 where true) as subq_0
 where subq_0.c8 is not null
 limit 46) as subq_1
where false;

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
 ref_0.type as c0, 
 ref_1.function_name as c1, 
 ref_2.function_type as c2, 
 ref_1.trigger_table_id as c3, 
 ref_0.storage as c4, 
 ref_0.type as c5, 
 ref_2.trigger_table_id as c6, 
 ref_1.function_schema_id as c7
 from 
 sys._columns as ref_0
 inner join sys.dependency_functions_on_triggers as ref_1
 on (true)
 right join sys.dependency_functions_on_triggers as ref_2
 on (ref_0.type = ref_2.function_name )
 where (((ref_2.function_schema_id is null) 
 and (true)) 
 and (true)) 
 and (true)
 limit 104) as subq_0
where (subq_0.c3 is not null) 
 or (subq_0.c0 is not null);

ROLLBACK;
