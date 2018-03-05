set optimizer = 'sequential_pipe';

explain select count(*) from v2;

explain select id       from v2 order by id;



explain select id , v2  from v2 order by id;
