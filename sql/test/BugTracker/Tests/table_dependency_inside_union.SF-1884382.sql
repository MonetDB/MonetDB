create table t1884382a (id int);
create table t1884382b (id int);

create view v1 as (select * from t1884382a
                        union
                   select * from t1884382b);

drop table t1884382a;
drop table t1884382b;

drop view v1;

select * from t1884382a;
drop table t1884382a;
drop table t1884382b;
