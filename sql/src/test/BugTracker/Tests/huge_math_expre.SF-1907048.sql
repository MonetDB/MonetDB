create table t1907048a (z real);
create table t1907048b (name string, dered_g real, dered_r real);
SELECT p.name FROM t1907048a as s, t1907048b as p WHERE
(-0.399*(dered_g-4.79*LOG10(s.z/0.03)-3.65*(dered_g-dered_r)-39.55))> 10.7
LIMIT 100000;

drop table t1907048b;
drop table t1907048a;
