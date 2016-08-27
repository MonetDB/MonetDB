start transaction;

create table test1 (a int, b int, d int);
create table test_dic1 (a int, c int);
insert into test_dic1 values (1, 1), (1, 2);
insert into test1 values (1, 2, 1), (1, 3, 2), (2, 2, 1), (2, 3, 2);

select dd.*
 from test_dic1 as dd
inner join test1 as d1 on d1.a = dd.a and d1.d = 1
 left join test1 as d2 on d2.a = dd.a and d2.d = 2
where dd.c = 1;
-- Get 1 row as expected

select dd.*
 from test_dic1 as dd
inner join test1 as d1 on d1.a = dd.a and d1.d = 1
 left join test1 as d2 on d2.a = dd.a and d2.d = 2
where dd.c = 1
   or dd.c = 3;
-- should get 1 row instead of 2 rows

select *
 from test_dic1 as dd
inner join test1 as d1 on d1.a = dd.a and d1.d = 1
 left join test1 as d2 on d2.a = dd.a and d2.d = 2
where dd.c = 1
   or dd.c = 3;
-- shows 2 rows instead of 1

select *
 from test_dic1 as dd
inner join test1 as d1 on d1.a = dd.a and d1.d = 1
 left join test1 as d2 on d2.a = dd.a and d2.d = 2
where dd.c in (1, 3);
-- shows 1 row correctly

select *
 from test_dic1 as dd
inner join test1 as d1 on d1.a = dd.a
 left join test1 as d2 on d2.a = dd.a and d2.d = 2
where dd.c = 1
   or dd.c = 3
  and d1.d = 1
  and (((dd.c = 1 or dd.c = 3) and d2.d is null) or ((dd.c = 1 or dd.c = 3) and d2.d = 2));
-- shows 4 rows instead of 2

select *
 from test_dic1 as dd
inner join test1 as d1 on d1.a = dd.a and d1.d = 1
inner join test1 as d2 on d2.a = dd.a and d2.d = 2
where dd.c = 1
   or dd.c = 3;
-- shows 1 row correctly

drop table test1;
drop table test_dic1;

rollback;
