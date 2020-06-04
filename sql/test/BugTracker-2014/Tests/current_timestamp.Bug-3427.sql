start transaction;

create table deterministic_insert (deterministic_insert timestamp);
insert into deterministic_insert values (current_timestamp);

create table x(
i integer,
t timestamp,
tn timestamp default null,
td timestamp default now(),
tc timestamp default current_timestamp);

insert into x(i,t,td,tc) values(0,(select current_timestamp from deterministic_insert),(select current_timestamp from deterministic_insert),(select current_timestamp from deterministic_insert));
select i, tn, td - t, tc - t from x;
insert into x(i,t) values(0,now());
drop table x;

drop table deterministic_insert;

create table x(
i integer,
t time,
tn time default null,
td time default now(),
tc time default current_time);

create table other_deterministic_insert (other_deterministic_insert time);
insert into other_deterministic_insert values (current_time);

insert into x(i,t,td,tc) values(0,(select other_deterministic_insert from other_deterministic_insert),(select other_deterministic_insert from other_deterministic_insert),(select other_deterministic_insert from other_deterministic_insert));
select i, tn, td - t, tc - t from x;
insert into x(i,t) values(0,now());
drop table x;

drop table other_deterministic_insert;
rollback;

create table d(t timestamp default current_time, i integer);
create table d(t time default current_timestamp, i integer);
drop table d;
