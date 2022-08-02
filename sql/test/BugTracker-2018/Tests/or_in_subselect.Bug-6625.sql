CREATE TABLE a (a integer, b integer);
CREATE TABLE b (a integer, b integer);
SELECT (SELECT count(*) FROM b where a.a=b.a and (b.b=1 or b.b=2)) FROM a;
drop table a;
drop table b;
