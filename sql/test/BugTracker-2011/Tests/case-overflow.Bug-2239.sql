start transaction;

create table t2239 (
       station136 smallint
);
copy 3 records into t2239 from stdin;
1537
1228
1214

select station136,
       ((CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END) +
        (CASE WHEN station136 is NULL THEN 0 ELSE station136 END))
from t2239 order by station136;

rollback;
