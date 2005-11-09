create table crash_q (a integer,b char(10));
insert into crash_q values(1,'c');
update crash_q set b= (
select b from crash_me where crash_q.a = crash_me.a);
