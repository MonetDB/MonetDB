CREATE TABLE "datacell"."sensors" (
        "ip"        INET,
        "emit_time" TIMESTAMP,
        "recv_time" TIMESTAMP,
        "location"  VARCHAR(20),
        "type"      VARCHAR(50),
        "value"     DOUBLE
);
create table datacell.warm(like datacell.sensors);
create table datacell.hot(like datacell.sensors);

CREATE TABLE "datacell"."alarm_warm" (
        "ip"          INET,
        "emit_time"   TIMESTAMP,
        "location"    VARCHAR(20),
        "temperature" DOUBLE
);

CREATE TABLE "datacell"."alarm_hot" (
        "ip"          INET,
        "emit_time"   TIMESTAMP,
        "location"    VARCHAR(20),
        "temperature" DOUBLE
);

--CALL datacell.receptor('datacell.sensors', 'localhost', 50500);

--CALL datacell.emitter('datacell.alarm_warm', 'localhost', 50601);
--CALL datacell.emitter('datacell.alarm_hot', 'localhost', 50602);

--CALL datacell.basket('datacell.warm');
--CALL datacell.basket('datacell.hot');

CREATE PROCEDURE datacell.warm()
BEGIN
	INSERT INTO datacell.alarm_warm 
	SELECT ip, emit_time, location, value 
		FROM datacell.warm 
		WHERE "type" LIKE 'temperature' AND value BETWEEN 21 AND 24;
END;
--call datacell.warm();
CALL datacell.query('datacell.warm');

CREATE PROCEDURE datacell.hot()
BEGIN
	INSERT INTO datacell.alarm_hot 
	SELECT ip, emit_time, location, value 
	FROM datacell.hot 
	WHERE type LIKE 'temperature' AND value > 24;
END;
--call datacell.hot();
CALL datacell.query('datacell.hot');

CREATE PROCEDURE datacell.splitter()
BEGIN
	INSERT INTO datacell.warm SELECT * FROM datacell.sensors;
	INSERT INTO datacell.hot SELECT * FROM datacell.sensors;
END;
--call datacell.splitter();
CALL datacell.query('datacell.splitter');

CALL datacell.resume();
SELECT * FROM datacell.receptors(); SELECT * FROM datacell.emitters(); SELECT * FROM datacell.queries(); SELECT * FROM datacell.baskets();

