start transaction;
create table "test" (good integer, bad integer);
insert into "test" (good, bad) values (1, null);
select * from "test";
select sys.quantile(good, 0.9) from "test";
select sys.quantile(bad, 0.9) from "test" where bad is not null;
select sys.quantile(bad, 0.9) from "test";
select stddev_samp(bad), stddev_pop(bad), var_samp(bad), median(bad), quantile(1, bad), quantile(bad, 1),
       quantile(bad, bad), corr(1, bad), corr(bad, 1), corr(bad, bad) from "test" where bad is not null;
select stddev_samp(bad), stddev_pop(bad), var_samp(bad), median(bad), quantile(1, bad), quantile(bad, 1),
       quantile(bad, bad), corr(1, bad), corr(bad, 1), corr(bad, bad) from "test";
rollback;
