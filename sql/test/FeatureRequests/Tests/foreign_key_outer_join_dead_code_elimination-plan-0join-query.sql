set optimizer = 'sequential_pipe';

   plan select count(*) from fk;

   plan select id       from fk order by id;
