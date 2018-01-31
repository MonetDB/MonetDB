set optimizer = 'sequential_pipe';

   plan select count(*) from fk left outer join pk1 on fk.fk1 = pk1.pk1 left outer join pk2 on fk.fk2 = pk2.pk2;

   plan select id       from fk left outer join pk1 on fk.fk1 = pk1.pk1 left outer join pk2 on fk.fk2 = pk2.pk2 order by id;

   plan select id , v1  from fk left outer join pk1 on fk.fk1 = pk1.pk1 left outer join pk2 on fk.fk2 = pk2.pk2 order by id;

   plan select id , v2  from fk left outer join pk1 on fk.fk1 = pk1.pk1 left outer join pk2 on fk.fk2 = pk2.pk2 order by id;
