create table foo_nil_2dec (t timestamp,v decimal(18,9));
insert into foo_nil_2dec values (timestamp '2014-10-05',42);
insert into foo_nil_2dec values (timestamp '2014-10-05',43);
select (t-(select timestamp '1970-1-1')),v from foo_nil_2dec union all select (t-(select timestamp '1970-1-1')),null from foo_nil_2dec;

--explain select (t-(select timestamp '1970-1-1')),v from foo_nil_2dec union all select (t-(select timestamp '1970-1-1')),null from foo_nil_2dec;

drop table foo_nil_2dec;
