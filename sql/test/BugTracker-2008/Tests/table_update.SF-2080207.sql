
CREATE TABLE A
(a varchar(10),
b varchar(10));

CREATE TABLE B
(a varchar(10),
b varchar(10));

insert into a values('1','2');
insert into a values('2','2');
insert into b values('1','2');
insert into b values('3','2');

select a from a where A.a not in (select B.a from B where A.b=B.b);

update A set a='a' where A.a not in (select B.a from B where A.b=B.b);

select * from a;

drop table B;
drop table A;
