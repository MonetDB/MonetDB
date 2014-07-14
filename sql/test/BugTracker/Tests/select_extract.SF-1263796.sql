create table t( c time );
insert into t values( time '12:34:56');
select c from t where extract( hour from c ) = 12 and extract( minute from c ) = 34;
drop table t;
