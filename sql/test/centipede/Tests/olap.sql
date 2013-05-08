create table Rc(i int, j int);
create table Sc(a int, b int);

insert into Rc values (1,1), (2,3), (2,4), (2,5), (3,3);
insert into Sc values (1,1), (1,3);

set optimizer='centipede_pipe';

explain select count(*) from Rc group by i;
explain select count(*) from Rc group by j;
explain select count(*) from Sc group by a;
explain select count(*) from Sc group by b;

explain select i,sum(j) from Rc group by i;
explain select j,sum(i) from Rc group by j;
explain select b,sum(b) from Sc group by a;
explain select a,sum(a) from Sc group by b;

explain select j,avg(j) from Rc group by i;
explain select i,avg(i) from Rc group by j;
explain select b,avg(b) from Sc group by a;
explain select a,avg(a) from Sc group by b;

explain select i,min(j) from Rc group by i;
explain select j,min(i) from Rc group by j;
explain select a,min(b) from Sc group by a;
explain select b,min(a) from Sc group by b;

explain select i,max(j) from Rc group by i;
explain select j,max(i) from Rc group by j;
explain select a,max(b) from Sc group by a;
explain select b,max(a) from Sc group by b;

select i,count(*) from Rc group by i;
select j,count(*) from Rc group by j;
select a,count(*) from Sc group by a;
select b,count(*) from Sc group by b;

select i,sum(j) from Rc group by i;
select j,sum(i) from Rc group by j;
select b,sum(b) from Sc group by a;
select a,sum(a) from Sc group by b;

select i,avg(j) from Rc group by i;
select j,avg(i) from Rc group by j;
select a,avg(b) from Sc group by a;
select b,avg(a) from Sc group by b;

select i,min(j) from Rc group by i;
select j,min(i) from Rc group by j;
select a,min(b) from Sc group by a;
select b,min(a) from Sc group by b;

select i,max(j) from Rc group by i;
select j,max(i) from Rc group by j;
select a,max(b) from Sc group by a;
select b,max(a) from Sc group by b;

drop table Rc;
drop table Sc;
