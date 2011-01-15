select epoch (1087488000);
select epoch (timestamp '2004-06-17 16:00:00.000000');

select abs(epoch(timestamp '2008-02-16 14:00:00'));
CREATE TABLE "sys"."t10"("time1" int, "time2" timestamp(7));
insert into t10 values(1202900916, timestamp '2008-02-13 11:08:06.000000');
select * from t10;
select epoch(1203170400);
select epoch(time1) from t10;
select epoch(time2) from t10;

drop table t10;
