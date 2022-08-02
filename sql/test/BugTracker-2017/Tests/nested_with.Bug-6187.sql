WITH t0(x) as (
    WITH t1(y) as (
        select 0
    )
    select * FROM t1
)
SELECT * FROM t0;
