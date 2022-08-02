start transaction;
create table a(a int, b int, id bigserial);
create ordered index a_pk on a(id);
create imprints index a_impk on a(id);
create index a_idx1 on a(a);
select type, name from sys.idxs where table_id in (select id from sys._tables where name = 'a');

insert into a(a) values(1), (2);
update a set a = 3, b = 3 where a = 1;
select * from a;
delete from a where a = 3;
truncate table a;
rollback;
