create table t4 (id int);
create table t5 (age float, yea int);
select yea from t4, t5 where age between 0.03 and id < 30 ;

drop table t4;
drop table t5;
