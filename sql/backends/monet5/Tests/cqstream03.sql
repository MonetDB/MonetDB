-- Introduce firing conditions to the queries themselves.
create stream table tmp13 (t timestamp, sensor integer, val decimal(8,2)) ;
create table agenda13(i integer, msg string);

-- The window determines the input size, it can not be overruled.
-- which is overruled here by the hearbeat (order is important)
call cquery.window('sys','tmp13',2);

create procedure cq_agenda()
begin
    declare b boolean;
    set b = (select count(*) > 0 from tmp13);
    if (b)
    then
        insert into agenda13 select count(*), 'full batch' from tmp13;
    end if;
end;
call cquery.register('sys','cq_agenda');
call cquery.heartbeat('sys','cq_agenda',1000);

select * from cquery.status();

call cquery.deregister('sys','cq_agenda');
drop procedure cq_agenda;
drop table tmp13;
drop table agenda13;

