create table x (id1 varchar(1000), id2 int);
create table dict (id int, x varchar(100));
UPDATE dict SET id = (SELECT id2 FROM "x" WHERE id1=dict.id);
drop table dict;
drop table x;
