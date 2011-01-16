create table a2 (x varchar(10));
insert into a2 values ('aaa');

create table b2 (x varchar(10));
insert into b2 values ('aaa');
insert into b2 values ('aAa');
insert into b2 values ('aA');

select a2.x from a2,b2 where a2.x LIKE b2.x;

drop table b2;
drop table a2;

CREATE TABLE a2 (name VARCHAR(10));
CREATE TABLE b2 (name VARCHAR(10));
INSERT INTO a2 VALUES ('a'),('b');
INSERT INTO b2 VALUES ('a'),('b');

SELECT a2.name as x, b2.name as y FROM a2,b2 WHERE a2.name LIKE b2.name;

drop table b2;
drop table a2;
