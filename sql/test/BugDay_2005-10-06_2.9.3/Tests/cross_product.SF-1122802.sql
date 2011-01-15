create table t1 (id int);
create table t2 (id int);
-- note, this is actually a hack to quickly insert some tuples ;)
insert into t1 values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),
	(12),(13),(14),(15),(16),(17),(18),(19),(20);
insert into t2 values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),
	(12),(13),(14),(15),(16),(17),(18),(19),(20);

select count(*) from t1;
select count(*) from t2;
select count(*) from t1, t2;
drop table t1;
drop table t2;
