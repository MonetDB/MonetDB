set optimizer = 'sequential_pipe';

   plan select count(*) from v1;

   plan select id       from v1 order by id;

   plan select id , v1  from v1 order by id;
