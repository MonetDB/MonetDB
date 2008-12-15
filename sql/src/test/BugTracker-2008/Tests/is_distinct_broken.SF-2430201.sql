create table way_tags (way int, k int);
insert into way_tags values(23950375,1);
insert into way_tags values(23950375,1);
insert into way_tags values(23950375,1);
insert into way_tags values(23950375,1);
insert into way_tags values(23950375,1);
insert into way_tags values(24644162,1);
insert into way_tags values(24644162,1);
insert into way_tags values(24644162,1);

select distinct way from way_tags group by way, k having count(*) > 1;

drop table way_tags;

select distinct (select 10 union all select 10);

select distinct a from (select 10 as a union all select 10 as a) as b;
