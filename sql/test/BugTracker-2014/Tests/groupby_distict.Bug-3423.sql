CREATE table a3423 (k int,b int);
INSERT into a3423 values (1,2);
INSERT into a3423 values (2,2);
INSERT into a3423 values (3,3);
INSERT into a3423 values (4,65);
INSERT into a3423 values (5,21);

SELECT k as c,count(distinct b) from a3423 group by c;
drop table a3423;
