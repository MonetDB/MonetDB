create table t1959410d (id int);
create table t1959410e (age float, yea int);
select yea from t1959410d, t1959410e where age between 0.03 and id < 30 ;

drop table t1959410d;
drop table t1959410e;
