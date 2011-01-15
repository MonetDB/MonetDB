create table pk2 (b1 integer primary key, b2 integer);
create table fk2 (d1 integer references pk2 (b1), d2 integer);
drop table fk2;
drop table pk2;
