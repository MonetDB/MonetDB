start transaction;
create table r(a1 string, a2 string);
insert into r values ('a','b'), ('a','b'), ('b','a');
SELECT DISTINCT a1,a2 FROM r;
SELECT DISTINCT
    CASE WHEN a1 > a2 THEN a2 ELSE a1 END as c1,
    CASE WHEN a1 < a2 THEN a2 ELSE a1 END as c2
FROM r;
rollback;
