-- platform dependent script test.
create table bug3923(i integer);

-- skip columns: location, count, columnsize, sorted.
select "schema", "table", "column", "type", "mode", case when typewidth < 14 then typewidth else 99 end as typewidth, hashes, phash, imprints from storage('sys','_tables');

call storagemodelinit();
update storagemodelinput set "count" =10000 where "table" ='bug3923';
update storagemodelinput set "distinct" =10 where "table" ='bug3923' and "column" ='i';

select * from storagemodel() where "table" = 'bug3923';

drop table bug3923;


create schema bug3923schema;
set schema bug3923schema;
create table bug3923(i integer);

-- skip columns: location, count, columnsize, sorted.
select "schema", "table", "column", "type", "mode", case when typewidth < 14 then typewidth else 99 end as typewidth, hashes, phash, imprints from sys.storage('sys','_tables');

call sys.storagemodelinit();
update sys.storagemodelinput set "count" =10000 where "table" ='bug3923';
update sys.storagemodelinput set "distinct" =10 where "table" ='bug3923' and "column" ='i';

select * from sys.storagemodel() where "table" = 'bug3923';

drop table bug3923;

