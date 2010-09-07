with t(id) as (select id from tables)
select id from tables 
 where id in (select id from t) 
   and id in (select id from t);

with t(id) as (select id from tables),
     x(id) as (select id from tables where id in (select id from t))
select * from t;
