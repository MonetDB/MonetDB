
create table test_a (a integer);
create table test_b (a smallint);

select * from test_a join test_b using (a);
select * from test_a left outer join test_b using (a);

drop table test_a;
drop table test_b;
