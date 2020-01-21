values (1);
values (NULL);
values (1,2,3);
values (1,2,3), (4,NULL,6), (7,8,NULL);
values (); --error
values (default); --error
values (1,2), (1), (3,3); --error
values (1), ('ok');
values (1) union values (3);
values (1,1) union values (1,1);
values (1,2,3) union all values (1,2,3);
values (3), (2) intersect values (3);
values (1,2,3), (4,5,6) except select 1,2,4;
values (1,2,3), (4,5,6) except select 1,2,3;
select 'a', 'c' union select 'b', 'c' except values ('a', 'c'), ('b', 'c');
select 'a', 'c' union select 'b', 'c' except values ('a', 'c'), ('b', 'd');
with t1(a,b) as (values (1,2), (3,5)) select t1.b from t1 where a > 1;
with t1(a,b) as (values (1,1), (2,2)),
     t2(a,b) as (values (2,4), (3,3))
     select * from t1 inner join t2 on t1.a = t2.a;
with t1(a,b) as (values (1,1), (2,2)),
     t2(a,b) as (values (2,4), (3,3,5))
     select * from t1 inner join t2 on t1.a = t2.a; --error
with t1(a,b) as (select 1) select * from t1; --error
with t1 as (select 1) values (2);
with t1 as (select 1) values (3,4,5,6,7,'ok'), (6,8,1,2,'still','ok');

create function foo() returns table (aa int, bb int) begin return table(values (1,2), (3)); end; --error
create function foo() returns table (aa int, bb int) begin return table(values (1,2)); end;
select aa, bb + 1 from foo();
select cc from foo() as bar(cc, dd);
drop function foo;
