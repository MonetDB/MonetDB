create table test_property(subject integer, p1 integer, p2 integer, unique(subject, p1), unique(subject, p2));
select "name", "query", "type", "remark" from describe_table('sys', 'test_property');
drop table test_property;
