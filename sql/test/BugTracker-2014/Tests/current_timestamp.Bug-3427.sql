start transaction;

create table x(
i integer,
t timestamp,
tn timestamp default null,
td timestamp default now(),
tc timestamp default current_timestamp);

insert into x(i,t) values(0,now());
select i, tn, td - t, tc - t from x;
drop table x;

create table x(
i integer,
t time,
tn time default null,
td time default now(),
tc time default current_time);

insert into x(i,t) values(0,now());
select i, tn, td - t, tc - t from x;
drop table x;

declare t timestamp;
declare tt time;
set t = now();
set t = current_timestamp;
set t = current_time;
rollback;
set tt = now();
set tt = current_time;
set tt = current_timestamp;

create table d(t timestamp default current_time, i integer);
create table d(t time default current_timestamp, i integer);
drop table d;
