set optimizer = 'sequential_pipe';

explain select count(*) from v1;

explain select id       from v1 order by id;

explain select id , v1  from v1 order by id;
