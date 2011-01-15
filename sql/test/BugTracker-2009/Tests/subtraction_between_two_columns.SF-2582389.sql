create table t2_2582389(id int, inst float);

insert into t2_2582389 values(1, 0.2);
insert into t2_2582389 values(1, 0.4);

select l.id, r.inst, l.inst, (r.inst - l.inst) as diff from t2_2582389 l,  t2_2582389 r where r.id = l.id order by r.inst, l.inst limit 2;

drop table t2_2582389;
