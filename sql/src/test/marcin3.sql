
create table nutki(id int,val int);
insert into nutki values(1,1);
insert into nutki values(1,2);
insert into nutki values(3,4);
insert into nutki values(3,3);
select id, sum(val) AS valsum from nutki group by id having val>1;
select id, sum(val) AS valsum from nutki group by id having val>2;
select id, sum(val) AS valsum from nutki group by id having valsum>3;

drop table nutki;
