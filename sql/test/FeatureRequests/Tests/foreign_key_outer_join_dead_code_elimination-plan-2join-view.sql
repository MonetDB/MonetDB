set optimizer = 'sequential_pipe';

   plan select count(*) from v2;

   plan select id       from v2 order by id;



   plan select id , v2  from v2 order by id;
