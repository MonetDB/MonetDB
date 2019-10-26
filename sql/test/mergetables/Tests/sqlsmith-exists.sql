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

ROLLBACK;
