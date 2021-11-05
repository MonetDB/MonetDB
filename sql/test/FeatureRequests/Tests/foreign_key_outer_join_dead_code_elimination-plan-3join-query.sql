plan select count(*) from pk1 join fk on fk.fk1 = pk1.pk1;
plan select id from pk1 join fk on fk.fk1 = pk1.pk1 order by id;
plan select count(*) from pk2 join (pk1 join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2;
plan select id from pk2 join (pk1 join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2 order by id;
plan select id, v1 from pk2 join (pk1 join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2 order by id;
plan select id, v2 from pk2 join (pk1 join fk on fk.fk1 = pk1.pk1) on fk.fk2 = pk2.pk2 order by id;
