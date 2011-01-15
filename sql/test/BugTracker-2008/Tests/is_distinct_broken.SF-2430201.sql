create table way_tags_2430201 (way int, k int);
insert into way_tags_2430201 values(23950375,1);
insert into way_tags_2430201 values(23950375,1);
insert into way_tags_2430201 values(23950375,1);
insert into way_tags_2430201 values(23950375,1);
insert into way_tags_2430201 values(23950375,1);
insert into way_tags_2430201 values(24644162,1);
insert into way_tags_2430201 values(24644162,1);
insert into way_tags_2430201 values(24644162,1);

select distinct way from way_tags_2430201 group by way, k having count(*) > 1;

drop table way_tags_2430201;

select distinct (select 10 union all select 10);

select distinct a from (select 10 as a union all select 10 as a) as b;
