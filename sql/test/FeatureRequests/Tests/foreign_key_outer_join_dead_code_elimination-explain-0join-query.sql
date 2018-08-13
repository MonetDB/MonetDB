set optimizer = 'sequential_pipe';

explain select count(*) from fk;

explain select id       from fk order by id;
