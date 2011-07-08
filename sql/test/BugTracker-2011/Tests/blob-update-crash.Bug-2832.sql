start transaction;
create table blobtest_bug2832(b blob);
insert into blobtest_bug2832 values(cast('00' as blob));
update blobtest_bug2832 set b=cast(cast(b as text)||cast(b as text) as blob);
select * from blobtest_bug2832;
rollback;
