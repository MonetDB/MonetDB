
start transaction;
create table tmp(i decimal(8));
explain select count(*) from tmp where i = 20160222;
explain select count(*) from tmp where i = '20160222';
explain select count(*) from tmp where i = 201602221;

