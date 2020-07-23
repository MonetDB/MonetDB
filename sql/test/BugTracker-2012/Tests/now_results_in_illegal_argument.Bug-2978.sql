create table deterministic(a timestamp);
insert into deterministic values (now());

select (select a from deterministic) + interval '1' second - (select a from deterministic);
select (select a from deterministic) + interval '1' second - (select a from deterministic);
select (select a from deterministic) - (select a from deterministic);

drop table deterministic;
