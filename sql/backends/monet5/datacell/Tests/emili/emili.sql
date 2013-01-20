-- the raw input stream received at receptor
CREATE TABLE datacell.istream(
	ip        INET,
	location  VARCHAR(5),
	kind      VARCHAR(50),
	value     DOUBLE
);
CALL datacell.receptor('datacell.istream', 'localhost', 50500);

-- collect the sensors in certain areas
CREATE TABLE datacell.area(
	ip INET,
	location varchar(5),
	primary key(ip)
); 

-- tag the events with their arrival time
CREATE TABLE datacell.sensors(
	like datacell.istream,
	time timestamp default now()
);
CALL datacell.basket('datacell.sensors');

-- administer the fire state in locations
CREATE TABLE datacell.states(
	location varchar(5),
	time timestamp default now(),
	status varchar(20) default 'normal'
);

-- warden emitter mailbox
CREATE TABLE datacell.warden(
	location varchar(5),
	message  varchar(20)
);
CALL datacell.emitter('datacell.warden','localhost',50600);

-- observations made by the warden 
CREATE TABLE datacell.observations(
	location varchar(5),
	message  varchar(20) 
);
CALL datacell.receptor('datacell.observations','localhost',50501);

-- enrich at the arrival time of each stream event
CREATE PROCEDURE datacell.enrich()
BEGIN
	INSERT INTO datacell.sensors(ip, location, kind,value) 
		SELECT ip, substring(location,0,3), kind, value FROM datacell.istream;

	INSERT INTO datacell.area 
		SELECT DISTINCT I.ip, substring(I.location,0,3) 
		FROM datacell.istream I 
		WHERE I.ip NOT IN (SELECT ip FROM datacell.area);

	INSERT INTO datacell.states 
		SELECT DISTINCT substring(location,0,3), now(), 'normal' 
		FROM datacell.istream
		WHERE substring(location,0,3) NOT IN (SELECT location FROM datacell.states);
END;
CALL datacell.query('datacell.enrich');

-- collect messages from hot sensors
CREATE TABLE datacell.hotsensors(
	ip INET,
	time timestamp,
	value double
);
CALL datacell.basket('datacell.hotsensors');

-- collect the hot sensor messages from stream
CREATE PROCEDURE datacell.hot()
BEGIN
	INSERT INTO datacell.hotsensors
	SELECT ip, time, value
	FROM datacell.sensors
	WHERE kind LIKE 'temperature' AND value > 24;
END;
CALL datacell.query('datacell.hot');

-- split the hot sensor events for different decisions
CREATE TABLE datacell.hotsensors1( LIKE hotsensors);
CALL datacell.basket('datacell.hotsensors1');
CREATE TABLE datacell.hotsensors2( LIKE hotsensors);
CALL datacell.basket('datacell.hotsensors2');

CREATE PROCEDURE datacell.splitter()
BEGIN
	INSERT INTO datacell.hotsensors1 SELECT * from datacell.hotsensors;
	INSERT INTO datacell.hotsensors2 SELECT * from datacell.hotsensors;
	INSERT INTO datacell.warden SELECT A.location, 'check it' from datacell.area A, datacell.hotsensors H
	WHERE H.ip = A.ip;
END;
call datacell.query('datacell.splitter');

-- unconfirmed fire detection based 
CREATE PROCEDURE datacell.firewarning()
BEGIN
	UPDATE datacell.states
	SET status = 'unconfirmed', time = now()
	WHERE location IN (SELECT A.location
			FROM datacell.states S, datacell.area A, datacell.hotsensors1 H
			WHERE S.status ='normal' AND A.ip = H.ip and S.location = A.location);
END;
CALL datacell.query('datacell.firewarning');

-- autoconfirm the fire warning 
CREATE PROCEDURE datacell.firespotted()
BEGIN
	UPDATE datacell.states
	SET status = 'confirmed', time = now()
	WHERE location IN (SELECT S.location
			FROM datacell.states S, datacell.area A, datacell.area B, datacell.hotsensors2 H
			WHERE S.status ='unconfirmed' 
			AND A.ip = H.ip 
			AND A.ip <> B.ip 
			AND S.location = A.location
			AND S.location = B.location);
END;
CALL datacell.query('datacell.firespotted');

-- Warden confirms location status
CREATE PROCEDURE datacell.observation()
BEGIN
	DELETE FROM datacell.states WHERE location IN (SELECT location FROM datacell.observations);
	INSERT INTO datacell.states SELECT O.location, now(), O.message FROM datacell.observations O;
END;

CALL datacell.resume();

SELECT * FROM datacell.receptors(); SELECT * FROM datacell.emitters(); SELECT * FROM datacell.queries(); SELECT * FROM datacell.baskets(); SELECT * FROM datacell.area; SELECT * FROM datacell.states;

