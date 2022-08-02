create table a_6329 (x int, y int not null);
\d a_6329

create table b_6329 as select * from a_6329 with no data;
\d b_6329

DROP TABLE a_6329;
DROP TABLE b_6329;


create table a_pk_6329 (x int PRIMARY KEY, y int not null);
\d a_pk_6329

create table b_pk_6329 as select * from a_pk_6329 with data;
\d b_pk_6329

DROP TABLE a_pk_6329;
DROP TABLE b_pk_6329;
