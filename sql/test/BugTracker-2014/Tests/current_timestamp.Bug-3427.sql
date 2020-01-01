start transaction;

declare deterministic_insert timestamp;
set deterministic_insert = current_timestamp;

create table x(
i integer,
t timestamp,
tn timestamp default null,
td timestamp default now(),
tc timestamp default current_timestamp);

insert into x(i,t,td,tc) values(0,deterministic_insert,deterministic_insert,deterministic_insert);
select i, tn, td - t, tc - t from x;
insert into x(i,t) values(0,now());
drop table x;

declare other_deterministic_insert time;
set other_deterministic_insert = current_time;

create table x(
i integer,
t time,
tn time default null,
td time default now(),
tc time default current_time);

insert into x(i,t,td,tc) values(0,other_deterministic_insert,other_deterministic_insert,other_deterministic_insert);
select i, tn, td - t, tc - t from x;
insert into x(i,t) values(0,now());
drop table x;

declare t timestamp;
declare tt time;
set t = now();
set t = current_timestamp;
set t = current_time;
rollback;
set tt = now(); --error, tt is no longer in the scope
set tt = current_time; --error, tt is no longer in the scope
set tt = current_timestamp; --error, tt is no longer in the scope

create table d(t timestamp default current_time, i integer);
create table d(t time default current_timestamp, i integer);
drop table d;
