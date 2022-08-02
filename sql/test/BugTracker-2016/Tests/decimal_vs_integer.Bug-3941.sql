start transaction;
create table tmp(i decimal(8));
set optimizer = 'sequential_pipe';
explain select count(*) from tmp where i = 20160222;
explain select count(*) from tmp where i = '20160222';
explain select count(*) from tmp where i = 201602221;
set optimizer = 'default_pipe';
rollback;
