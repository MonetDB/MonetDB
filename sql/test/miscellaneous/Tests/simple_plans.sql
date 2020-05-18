start transaction;

create table myx (x uuid, y uuid);
plan select * from myx where x in ('1aea00e5db6e0810b554fde31d961965') or y = '1aea00e5db6e0810b554fde31d961965';
plan select * from myx where x in ('1aea00e5db6e0810b554fde31d961965') or y is null;

plan select * from myx where x in ('1aea00e5db6e0810b554fde31d961965', '1aea00e5db6e0810b554fde31d961966') or y = '1aea00e5db6e0810b554fde31d961967';

insert into myx values ('1aea00e5db6e0810b554fde31d961965', '1aea00e5db6e0810b554fde31d961967');
select * from myx where x in ('1aea00e5db6e0810b554fde31d961966') or y is not null;
select * from myx where x in ('1aea00e5db6e0810b554fde31d961966') or y is null;

rollback;
