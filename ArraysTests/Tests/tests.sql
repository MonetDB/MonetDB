--create array testa (x integer dimension[5], y integer dimension[3:3:11], v float default -1);
select * from testa;
select * from testa where x<2;
select * from testa where y>=9;
select * from testa where x>=3 and y<9;
select * from testa where x <>2 and y <>6;

select count(*) from testa;

select avg(v) from testa;

update testa set v=3 where x=3;

select * from testa;
select avg(v) from testa;


select avg(v) from testa where x=3;
select avg(v) from testa where y=12;
select avg(v) from testa where x=1 or x=3;
select avg(v) from testa where x<>1;

select x, avg(v) from testa group by x;
select y, avg(v) from testa group by y;

update testa set v=2*x where x=3;
select * from testa;
select avg(v) from testa where x<>1;
select y, avg(v) from testa group by y;
select y, avg(v) from testa where x<>1 group by y;



update testb set v=10*x+y;
update testc set v=10*x+y;

select testb.v as "b.v", testc.v as "c.v", testb.v+testc.v as "b.v+c.v" from testb, testc where testb.x=testc.x;
select testb.v as "b.v", testc.v as "c.v", testb.v+testc.v as "b.v+c.v" from testb, testc where testb.x=testc.x and testb.y<2;

--for the next running
drop array testa;
drop array testb;
drop array testc;
create array testa (x integer dimension[5], y integer dimension[3:3:11], v float default -1);
create array testb (x integer dimension[3], y integer dimension[3], v float);
create array testc (y integer dimension[3], x integer dimension[3], v float);


