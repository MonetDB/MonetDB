set time zone interval '10:00' HOUR TO MINUTE;
--set current_timezone = 600;
create table time_example ( time_local TIMESTAMP, time_tz TIMESTAMP WITH TIME ZONE);
insert into time_example values ( TIMESTAMP '1995-07-15 07:30', TIMESTAMP '1995-07-15 07:30');
insert into time_example values ( TIMESTAMP '1995-07-15 07:30', TIMESTAMP '1995-07-15 07:30+02:30');
insert into time_example values ( TIMESTAMP '1995-07-15 07:30', TIMESTAMP '1995-07-15 07:30+04:30');
select * from time_example;
set time zone local;
select current_timezone;
