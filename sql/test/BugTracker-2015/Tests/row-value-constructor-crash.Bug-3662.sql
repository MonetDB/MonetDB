CREATE TABLE een (a INTEGER, b INTEGER, C integer);
insert into een values (1,1,1);
UPDATE een SET (A,B,C) = (SELECT 2,2,2);
SELECT * FROM een;
drop table een;

