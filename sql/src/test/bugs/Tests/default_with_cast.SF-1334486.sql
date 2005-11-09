create table t (
	id serial,
	d date default cast(now() as date)
);

insert into t ;

-- check the date is not NULL!
select cast(now() as date) - d from t;
