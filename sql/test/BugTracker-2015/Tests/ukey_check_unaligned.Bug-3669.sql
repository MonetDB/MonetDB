create table tab (id int not null, nm varchar(30) not null, dt date, qnt dec(18,10), descr text);
-- table created.
insert into tab (id, nm) values (1, 'A');
insert into tab (id, nm, dt, qnt, descr) values (2, 'B', '2015-01-29', 3.1415629, 'iasdfhiasdhagdsnfgankkkjfgjklfgjklsklsklsdfg');
select * from tab;
-- 2 rows.
insert into tab (id, nm, dt, qnt, descr) select id, nm, dt, qnt, descr from tab;
--- 2 rows inserted
select * from tab;
-- 4 rows.
select count(*) as count_dupl_rows, id, nm, dt, qnt, descr from tab
group by id, nm, dt, qnt, descr having count(*) > 1
order by id, nm, dt, qnt, descr;
-- 2 rows (id = 1 and 2) exists twice.
alter table tab add constraint tab_uc6 unique (id, nm, dt, qnt, descr);

drop table tab;
