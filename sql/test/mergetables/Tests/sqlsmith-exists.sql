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

ROLLBACK;
