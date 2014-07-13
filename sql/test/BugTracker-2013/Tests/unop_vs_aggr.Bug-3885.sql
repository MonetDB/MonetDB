create table err_stddev2(col1 double);
insert into err_stddev2 values(2.4);
insert into err_stddev2 values(2.6);
SELECT sys.stddev_pop(col1) * sys.stddev_pop(col1) FROM err_stddev2;
drop table err_stddev2;
