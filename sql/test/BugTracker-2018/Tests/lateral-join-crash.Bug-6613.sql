start transaction;
create table segments (meter int, distance int, speed int);
insert into segments values (1,1,1),(9,9,9);
select t.*                          from segments as s, lateral generate_series(s.meter, cast(s.meter+s.distance+1 as int)) as t;
select t.value, s.distance, s.speed from segments as s, lateral generate_series(s.meter, cast(s.meter+s.distance+1 as int)) as t;
select *                            from segments as s, lateral generate_series(s.meter, cast(s.meter+s.distance+1 as int));
select t.*, s.distance, s.speed     from segments as s, lateral generate_series(s.meter, cast(s.meter+s.distance+1 as int)) as t;
select t.meter, s.distance, s.speed from segments as s, lateral generate_series(s.meter, cast(s.meter+s.distance+1 as int)) as t(meter);
select t.*, s.distance, s.speed     from segments as s, lateral generate_series(s.meter, cast(s.meter+s.distance+1 as int)) as t(meter);
rollback;

