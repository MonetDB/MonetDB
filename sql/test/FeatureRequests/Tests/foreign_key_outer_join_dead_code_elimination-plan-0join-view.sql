set optimizer = 'sequential_pipe';

   plan select count(*) from v0;

   plan select id       from v0 order by id;
