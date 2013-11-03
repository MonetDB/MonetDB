start transaction;
create table t1marcin1(id int, val int);
create table t2marcin2(id int, val int);

insert into t1marcin1 values(1,1);
insert into t1marcin1 values(2,2);
insert into t1marcin1 values(3,3);
insert into t1marcin1 values(4,4);
insert into t1marcin1 values(5,5);
insert into t2marcin2 values(1,3);
insert into t2marcin2 values(2,2);
insert into t2marcin2 values(3,1);
commit;

-- and now some strange queries and even stranger results

-- cartesian-product missing
select * from t1marcin1,t2marcin2 order by t1marcin1.id, t1marcin1.val, t2marcin2.id, t2marcin2.val;

-- These actually give good results (cartesian-product like):
select t1marcin1.id,t2marcin2.id from t1marcin1,t2marcin2 where t1marcin1.id<>t2marcin2.id;
select t1marcin1.id,t2marcin2.id from t1marcin1,t2marcin2 where t1marcin1.id>t2marcin2.id;

-- does not work (and should, assuming ids are unique)
select t1marcin1.id, (select t2marcin2.id from t2marcin2 where t1marcin1.id=t2marcin2.id) from t1marcin1;

-- As the sub-select returns a single value (3), no error occures
select t1marcin1.id, (select t2marcin2.id from t2marcin2 where t2marcin2.id>2) from t1marcin1;

-- Return only columns from first table
select * from t2marcin2,t1marcin1 where t1marcin1.id=t2marcin2.id;
select * from t1marcin1,t2marcin2 where t1marcin1.id=t2marcin2.id;

-- I'm not sure if t2marcin2.* is allowed but anyway it's strange
select * from t2marcin2;
select t2marcin2.* from t2marcin2;

-- wrong subsets
select * from t1marcin1 where id>2;
select * from t1marcin1 where id>2 and id<2;

start transaction;
drop table t1marcin1;
drop table t2marcin2;
commit;
