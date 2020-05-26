start transaction;

create table myx (x uuid, y uuid);
plan select * from myx where x in ('1aea00e5db6e0810b554fde31d961965') or y = '1aea00e5db6e0810b554fde31d961965';
plan select * from myx where x in ('1aea00e5db6e0810b554fde31d961965') or y is null;

plan select * from myx where x in ('1aea00e5db6e0810b554fde31d961965', '1aea00e5db6e0810b554fde31d961966') or y = '1aea00e5db6e0810b554fde31d961967';

insert into myx values ('1aea00e5db6e0810b554fde31d961965', '1aea00e5db6e0810b554fde31d961967');
select * from myx where x in ('1aea00e5db6e0810b554fde31d961966') or y is not null;
select * from myx where x in ('1aea00e5db6e0810b554fde31d961966') or y is null;

CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES (97,1,99), (15,81,47),(87,21,10);

plan select 1 from tab0 where col1 = 1 or col1 = 81;

plan select 1 from tab0 where col1 = 1 or col1 = 81 or col1 = 100;

plan select 1 from tab0 where (col1 = 1 or col1 = 81) or (col2 < 0); 

plan select 1 from tab0 where ((col1 = 1 or col1 = 81) or col1 = 100) or (col2 > 10);

plan select 1 from tab0 where ((col1 = 1 or col1 = 81) or col2 = 100) or (col1 = 100); --the rightmost comparison to col1 cannot be merged to the other 2

rollback;
