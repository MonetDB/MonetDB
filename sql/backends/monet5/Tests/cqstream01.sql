-- Example of a window based action
create stream table stmp2 (t timestamp, sensor integer, val decimal(8,2)) set window 2 stride 2;
create table result2(like stmp2);

insert into stmp2 values('2005-09-23 12:34:26.000',1,9.0);
insert into stmp2 values('2005-09-23 12:34:27.000',1,11.0);
insert into stmp2 values('2005-09-23 12:34:28.000',1,13.0);
insert into stmp2 values('2005-09-23 12:34:28.000',1,15.0);

-- CREATE procedure cq_window
create procedure cq_window()
begin
    insert into result2 select * from stmp2 where val >12;
end;

start continuous sys.cq_window();

-- wait for a few seconds for scheduler to do its swork
call sleep(3000);

-- STOP cq_window;
pause continuous cq_window;

select 'RESULT';
select val from stmp2;
select val from result2;

--select * from cquery.log();

-- ideally auto remove upon dropping the procedure
stop continuous cq_window;

drop procedure cq_window;
drop table stmp2;
drop table result2;
