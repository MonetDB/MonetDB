start transaction;
create table t3087 (i int);
create table t3087a as select row_number() over () as id, i from t3087 with data;
rollback;
