-- Introduce firing conditions to the queries themselves.
-- The window determines the input size, it can not be overruled.
-- which is overruled here by the hearbeat (order is important)
--call cquery."window"('sys','tmp13',2);
create stream table tmp13 (t timestamp, sensor integer, val decimal(8,2)) set window 2;
create table agenda13(i integer, msg string);

create procedure cq_agenda()
begin
    declare b boolean;
    set b = (select count(*) > 0 from tmp13);
    if (b)
    then
        insert into agenda13 select count(*), 'full batch' from tmp13;
    end if;
end;

start continuous sys.cq_agenda() with heartbeat 2000;

select * from cquery.status();

stop continuous sys.cq_agenda(); --error
drop procedure cq_agenda;
drop table tmp13;
drop table agenda13;
