START TRANSACTION;
CREATE TABLE t_199999 as (select cast("value" as string) as "s1", cast("value" as string) as "s2" from generate_series(0, 199999));
CREATE TABLE t_200000 as (select cast("value" as string) as "s1", cast("value" as string) as "s2" from generate_series(0, 200000));

select count(1) from t_199999 where similarity(s1, s2)> 0.5;
select count(1) from t_200000 where similarity(s1, s2)> 0.5;
ROLLBACK;
