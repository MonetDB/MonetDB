CREATE TABLE tx ( c1 int );
CREATE INDEX "tx_index" ON "sys"."tx" ("c1");

select * from tx;

drop index tx_index;
drop table tx;
