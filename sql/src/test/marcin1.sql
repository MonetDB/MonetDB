create table t1(id int, val int);
create table t2(id int, val int);

insert into t1 values(1,1);
insert into t1 values(2,2);
insert into t1 values(3,3);
insert into t1 values(4,4);
insert into t1 values(5,5);
insert into t2 values(1,3);
insert into t2 values(2,2);
insert into t2 values(3,1);
commit;

-- and now some strange queries and even stranger results

-- cartesian-product missing
select * from t1,t2;

-- These actually give good results (cartesian-product like):
select t1.id,t2.id from t1,t2 where t1.id<>t2.id;
select t1.id,t2.id from t1,t2 where t1.id>t2.id;

-- does not work (and should, assuming ids are unique)
select t1.id, (select t2.id from t2 where t1.id=t2.id) from t1;

-- As the sub-select returns a single value (3), no error occures
select t1.id, (select t2.id from t2 where t2.id>2) from t1;
rollback;

-- Return only columns from first table
select * from t2,t1 where t1.id=t2.id;
select * from t1,t2 where t1.id=t2.id;

-- I'm not sure if t2.* is allowed but anyway it's strange
select * from t2;
select t2.* from t2;

-- wrong subsets
select * from t1 where id>2;
rollback;
select * from t1 where id>2 and id<2;
rollback;

drop table t1;
drop table t2;

commit;
