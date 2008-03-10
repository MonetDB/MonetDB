create table t1 (z real);
create table t2 (name string, dered_g real, dered_r real);
SELECT p.name FROM t1 as s, t2 as p WHERE
(-0.399*(dered_g-4.79*LOG10(s.z/0.03)-3.65*(dered_g-dered_r)-39.55))> 10.7
LIMIT 100000;

drop table t2;
drop table t1;
