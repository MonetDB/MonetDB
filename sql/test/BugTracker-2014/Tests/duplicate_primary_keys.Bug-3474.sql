create table x ( a clob, b clob, c timestamp);
alter table x add primary key (a,b,c);

create table y ( a clob, b clob, c timestamp);
insert into y values ('FIAM','HHZ', '2010-04-25T14:00:00.000');

insert into x (select * from y);

-- this query should fail but doesn't:
insert into x values ('FIAM','HHZ', '2010-04-25T14:00:00.000');

-- and this one generates proper error message
insert into x (select * from y);

select * from x;
drop table x;
drop table y;
