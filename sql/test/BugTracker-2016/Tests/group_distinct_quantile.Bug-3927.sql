start transaction;
create table my_table (my_id clob, my_double double);
SELECT COUNT( DISTINCT my_id ) AS unique_enrollees , QUANTILE( my_double , 0.25 ) AS some_quantile FROM my_table;
