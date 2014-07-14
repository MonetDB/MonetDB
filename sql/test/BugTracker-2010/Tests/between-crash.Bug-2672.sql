create table t2672a (id int);
create table t2672b (age int);
SELECT
        id
FROM
        t2672a,
        t2672b
WHERE
        id between 4800
        and age = 4863;

drop table t2672a;
drop table t2672b;
