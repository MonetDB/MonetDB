create table t (
	id serial,
	d date default cast(now() as date)
);

insert into t ;
