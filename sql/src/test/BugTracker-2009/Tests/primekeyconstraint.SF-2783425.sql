CREATE TABLE way_nds (
way int,
idx int,
payload string
);

insert into way_nds values(1,1,'abc');
insert into way_nds values(1,1,'abc');
insert into way_nds values(2,2,'abc');
insert into way_nds values(3,3,'abc');
insert into way_nds values(4,3,'abc');
insert into way_nds values(null,3,'abc');
insert into way_nds values(null,3,'abc');
insert into way_nds values(5,null,'abc');
insert into way_nds values(5,null,'abc');

ALTER TABLE way_nds ADD CONSTRAINT pk_way_nds PRIMARY KEY (way, idx);

select way,idx from way_nds;
select count(way), way, idx from way_nds group by way, idx having count(way) > 1;
select count(idx), way, idx from way_nds group by way, idx having count(idx) > 1;
select count(*), way, idx from way_nds group by way, idx having count(*) > 1;
drop table way_nds;
