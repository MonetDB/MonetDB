-- the raw input stream received at receptor
CREATE TABLE istream(
	ip        INET,
	location  VARCHAR(5),
	kind      VARCHAR(50),
	value     DOUBLE
);

-- collect the sensors in certain areas
CREATE TABLE area(
	ip INET,
	location varchar(5),
	primary key(ip)
); 

-- tag the events with their arrival time
CREATE TABLE sensors(
	like istream,
	time timestamp default now()
);

-- administer the fire state in locations
CREATE TABLE states(
	location varchar(5),
	time timestamp default now(),
	status varchar(20) default 'normal'
);

-- warden emitter mailbox
CREATE TABLE warden(
	location varchar(5),
	message  varchar(20)
);

-- observations made by the warden 
CREATE TABLE observations(
	location varchar(5),
	message  varchar(20) 
);

CREATE PROCEDURE enrich_a()
BEGIN
    INSERT INTO sensors(ip, location, kind,value)
        SELECT ip, substring(location,0,3), kind, value FROM istream;
    IF TRUE
    THEN
        INSERT INTO area SELECT ip, substring(location,0,3) FROM
istream;
    END IF;
END;

CREATE PROCEDURE enrich_b()
BEGIN
    INSERT INTO sensors(ip, location, kind,value)
        SELECT ip, substring(location,0,3), kind, value FROM istream;
    IF (SELECT count(*) FROM area ) = 0
    THEN
        INSERT INTO area SELECT ip, substring(location,0,3) FROM
istream;
    END IF;
END;


CREATE PROCEDURE enrich_c()
BEGIN
    DECLARE cnt INTEGER;
    SET cnt = (SELECT count(*) FROM area ) ;
    INSERT INTO sensors(ip, location, kind,value)
        SELECT ip, substring(location,0,3), kind, value FROM istream;
    IF cnt = 0
    THEN
        INSERT INTO area SELECT ip, substring(location,0,3) FROM
istream;
    END IF;
END;

drop procedure enrich_a();
drop procedure enrich_b();
drop procedure enrich_c();

drop TABLE istream;
drop TABLE area;
drop TABLE sensors;
drop TABLE states;
drop TABLE warden;
drop TABLE observations;
