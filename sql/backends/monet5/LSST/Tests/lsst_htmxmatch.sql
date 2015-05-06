create table htm( id BIGINT);
insert into htm values (100), (101), (102), (103);
insert into htm values (110), (111), (112), (113);
insert into htm values (120), (121), (122), (123);
insert into htm values (130), (131), (132), (133);

-- select identical pairs
select  * from htm a, htm b where [a.id] xmatch [b.id,0] order by a.id, b.id;

-- select pairs at distance one
select  * from htm a, htm b where [a.id] xmatch [b.id,1] order by a.id, b.id;

drop table htm;
