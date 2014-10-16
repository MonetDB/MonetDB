create table tmptheta(i int, j int);
insert into tmptheta values(0,0),(1,1),(2,4),(3,9);
select * from tmptheta;

select * from generate_series(0,5,1);

select * from tmptheta, generate_series(0,5,1) as x
where tmptheta.j >0 and tmptheta.j <10
and tmptheta.i = x.value;

select * from generate_series(0,5,1) as x, tmptheta
where tmptheta.j >0 and tmptheta.j <10
and tmptheta.i = x.value;

drop table tmptheta;
