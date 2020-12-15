create table test (
	id1 int,
	id2 int not null,
	id3 int null
);

select c.name, c.type, c.type_digits, c.type_scale, c."null", c."default", c.number from sys._columns c, sys._tables t, sys.schemas s
where c.table_id = t.id and t.name = 'test' and t.schema_id = s.id and s.name = 'sys' order by c.number;

drop table test;
