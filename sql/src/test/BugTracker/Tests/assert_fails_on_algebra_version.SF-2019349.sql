create table prop1 (subj int, obj int);
create table prop2 (subj int, obj int, prop int);

WITH A(subj, obj) as (SELECT * FROM prop1 where obj = 14660332)
SELECT prop, obj, cnt from (
SELECT 'prop14657240_pso' as prop, B.obj as obj, count(*) as cnt
FROM A, prop2 as B
WHERE A.subj = B.subj
GROUP BY B.obj HAVING count(*) > 1
) as trip;

drop table prop1;
drop table prop2;
