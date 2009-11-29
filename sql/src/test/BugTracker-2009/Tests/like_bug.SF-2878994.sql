create table a (x varchar(10));
insert into a values ('aaa');

create table b (x varchar(10));
insert into b values ('aaa');
insert into b values ('aAa');
insert into b values ('aA');

select a.x from a,b where a.x LIKE b.x;

drop table b;
drop table a;

CREATE TABLE a (name VARCHAR(10));
CREATE TABLE b (name VARCHAR(10));
INSERT INTO a VALUES ('a'),('b');
INSERT INTO b VALUES ('a'),('b');

SELECT a.name as x, b.name as y FROM a,b WHERE a.name LIKE b.name;

drop table b;
drop table a;
