set optimizer = 'sequential_pipe';

   plan select count(*) from fk left outer join pk1 on fk.fk1 = pk1.pk1 left outer join pk2 on fk.fk2 = pk2.pk2;

   plan select id       from fk left outer join pk1 on fk.fk1 = pk1.pk1 left outer join pk2 on fk.fk2 = pk2.pk2 order by id;

   plan select id , v1  from fk left outer join pk1 on fk.fk1 = pk1.pk1 left outer join pk2 on fk.fk2 = pk2.pk2 order by id;

   plan select id , v2  from fk left outer join pk1 on fk.fk1 = pk1.pk1 left outer join pk2 on fk.fk2 = pk2.pk2 order by id;

   plan select count(*) from pk2 right outer join (pk1 right outer join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2;

   plan select id       from pk2 right outer join (pk1 right outer join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2 order by id;

   plan select id , v1  from pk2 right outer join (pk1 right outer join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2 order by id;

   plan select id , v2  from pk2 right outer join (pk1 right outer join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2 order by id;

   plan select count(*) from pk2 full outer join (pk1 full outer join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2;

   plan select id       from pk2 full outer join (pk1 full outer join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2 order by id;

   plan select id , v1  from pk2 full outer join (pk1 full outer join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2 order by id;

   plan select id , v2  from pk2 full outer join (pk1 full outer join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2 order by id;

   plan select count(*) from pk2 join (pk1 join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2;

   plan select id       from pk2 join (pk1 join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2 order by id;

   plan select id , v1  from pk2 join (pk1 join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2 order by id;

   plan select id , v2  from pk2 join (pk1 join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2 order by id;
