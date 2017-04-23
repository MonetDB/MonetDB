CREATE TABLE rooms(
     time timestamp,
     room string,
     level integer,
     temp double,
	PRIMARY KEY(time, room,level) );

insert into rooms values
(timestamp '2017/01/01 09:00:00.000', 'L302', 3, 21.3),
(timestamp '2017/01/01 09:00:15.000', 'L302', 3, 21.3),
(timestamp '2017/01/01 09:00:30.000', 'L302', 3, 21.4),
(timestamp '2017/01/01 09:00:45.000', 'L302', 3, 21.5),
(timestamp '2017/01/01 10:00:00.000', 'L302', 3, 21.4),
(timestamp '2017/01/01 10:00:15.000', 'L302', 3, 21.4),
(timestamp '2017/01/01 10:00:30.000', 'L302', 3, 21.4),
(timestamp '2017/01/01 10:00:45.000', 'L302', 3, 21.5),
(timestamp '2017/01/01 11:00:00.000', 'L302', 3, 21.4),
(timestamp '2017/01/01 11:00:15.000', 'L302', 3, 21.4),
(timestamp '2017/01/01 11:00:30.000', 'L302', 3, 21.4),
(timestamp '2017/01/01 11:00:45.000', 'L302', 3, 21.5);

--derivative
WITH bounds(first, last, period) 
AS (SELECT min(time) AS mintime, max(time) as maxtime, epoch(time)/60 AS period FROM rooms GROUP BY period) 
SELECT r2.time, r2.room, r2.level, (r2.temp - r1.temp)/ (epoch(bounds.last) - epoch(bounds.first)) FROM bounds, rooms r1, rooms r2
WHERE r1.time = bounds.first and r2.time = bounds.last and r1.room = r2.room and r1.level = r2.level;

--derivative function with hardcoded stride
--causes an infinite loop.
CREATE FUNCTION rooms_derivative( stride bigint)
RETURNS TABLE( 
    time timestamp,
    room string,
    level integer,
    temp double)
BEGIN
   RETURN
	   WITH bounds(first, last, period) 
		AS (SELECT min(time) AS mintime, max(time) as maxtime, epoch(time)/60 AS period FROM rooms GROUP BY period) 
		SELECT r2.time, r2.room, r2.level, (r2.temp - r1.temp)/ (epoch(bounds.last) - epoch(bounds.first)) FROM bounds, rooms r1, rooms r2
	   WHERE r1.time = bounds.first and r2.time = bounds.last and r1.room = r2.room and r1.level = r2.level;
END;

DROP TABLE rooms;
