start transaction;
create table a(a int, b int, id bigserial);
create ordered index a_pk on a(id);
create index a_idx1 on a(a);
insert into a(a) values(1);
rollback;
