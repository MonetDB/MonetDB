create table t2606a (a int);
insert into t2606a values (11);
insert into t2606a values (21);
insert into t2606a values (13);
insert into t2606a values (23);
insert into t2606a values (12);
insert into t2606a values (22);
create table t2606b (a int);
insert into t2606b values (21);
insert into t2606b values (31);
insert into t2606b values (23);
insert into t2606b values (33);
insert into t2606b values (22);
insert into t2606b values (32);
select * from t2606a;
select * from t2606b;

-- ORDER BY is ignored:

plan   select * from t2606a   union   select * from t2606b   order by a;
       select * from t2606a   union   select * from t2606b   order by a;
plan ( select * from t2606a   union   select * from t2606b ) order by a;
     ( select * from t2606a   union   select * from t2606b ) order by a;
plan ( select * from t2606a ) union ( select * from t2606b ) order by a;
     ( select * from t2606a ) union ( select * from t2606b ) order by a;

plan   select * from t2606a   except   select * from t2606b   order by a;
       select * from t2606a   except   select * from t2606b   order by a;
plan ( select * from t2606a   except   select * from t2606b ) order by a;
     ( select * from t2606a   except   select * from t2606b ) order by a;
plan ( select * from t2606a ) except ( select * from t2606b ) order by a;
     ( select * from t2606a ) except ( select * from t2606b ) order by a;

plan   select * from t2606a   intersect   select * from t2606b   order by a;
       select * from t2606a   intersect   select * from t2606b   order by a;
plan ( select * from t2606a   intersect   select * from t2606b ) order by a;
     ( select * from t2606a   intersect   select * from t2606b ) order by a;
plan ( select * from t2606a ) intersect ( select * from t2606b ) order by a;
     ( select * from t2606a ) intersect ( select * from t2606b ) order by a;

-- ORDER BY is respected:

plan select * from (select * from t2606a union select * from t2606b) as t order by a;
     select * from (select * from t2606a union select * from t2606b) as t order by a;

plan select * from (select * from t2606a except select * from t2606b) as t order by a;
     select * from (select * from t2606a except select * from t2606b) as t order by a;

plan select * from (select * from t2606a intersect select * from t2606b) as t order by a;
     select * from (select * from t2606a intersect select * from t2606b) as t order by a;

-- clean-up

drop table t2606b;
drop table t2606a;
