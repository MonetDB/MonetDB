start transaction;
create table x (y decimal(10,4));
plan select quantile(y, 0.0) from x;
plan select quantile(y, 0) from x;
rollback;
