create table t2(id int, inst float);

insert into t2 values(1, 0.2);
insert into t2 values(1, 0.4);

select l.id, r.inst, l.inst, (r.inst - l.inst) as diff from t2 l,  t2 r where r.id = l.id order by inst limit 2;

drop table t2;
