-- platform dependent script test.
create table bug3923(i integer);

-- skip columns location, count, columnsize.
select "schema", "table", "column", "type", "mode", typewidth, hashes, phash, "imprints", sorted from storage() where "table"= '_tables';

call storagemodelinit();
update storagemodelinput set "count" =10000 where "table" ='bug3923';
update storagemodelinput set "distinct" =10 where "table" ='bug3923' and "column" ='i';

select * from storagemodel() where "table" = 'bug3923';

drop table bug3923;

crate schema bug3923schema;
create table bug3923(i integer);

-- skip columns location, count, columnsize.
select "schema", "table", "column", "type", "mode", typewidth, hashes, phash, "imprints", sorted from storage() where "table"= '_tables';

call storagemodelinit();
update storagemodelinput set "count" =10000 where "table" ='bug3923';
update storagemodelinput set "distinct" =10 where "table" ='bug3923' and "column" ='i';

select * from storagemodel() where "table" = 'bug3923';

drop table bug3923;

