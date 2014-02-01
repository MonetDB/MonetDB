create table x(
i integer,
t timestamp,
tn timestamp default null,
td timestamp default now(),
tc timestamp default current_timestamp);

insert into x(i,t) values(0,now());
select * from x;
drop table x;

create table x(
i integer,
t time,
tn time default null,
td time default now(),
tc time default current_time);

insert into x(i,t) values(0,now());
select * from x;
drop table x;

declare t timestamp;
declare tt time;
set t = now();
set t = current_time;
set tt = now();
set tt = current_time;

create table d(t timestamp default current_time, i integer);
