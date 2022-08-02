set optimizer = 'sequential_pipe';

explain select count(*) from v0;

explain select id       from v0 order by id;
