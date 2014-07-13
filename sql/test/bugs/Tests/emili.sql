start transaction;
--This one compiles

create table istream(
		ip int, 
		location varchar(16), 
		kind int,
		value int);
create table sensors(
		ip int, 
		location varchar(16), 
		kind int,
		value int);
create table area(
		ip int, 
		location varchar(16) );

CREATE PROCEDURE enrich1()
BEGIN
     INSERT INTO sensors(ip, location, kind,value)
         SELECT ip, substring(location,0,3), kind, value FROM istream;
     IF TRUE
     THEN
         INSERT INTO area SELECT ip, substring(location,0,3) FROM istream;
     END IF;
END;

--This one does not, see IF-expression, and does not produce an error message.

CREATE PROCEDURE enrich2()
BEGIN
     INSERT INTO sensors(ip, location, kind,value)
         SELECT ip, substring(location,0,3), kind, value FROM istream;
     IF (SELECT count(*) FROM area ) = 0
     THEN
         INSERT INTO area SELECT ip, substring(location,0,3) FROM istream;
     END IF;
END;

--And this one works again:
CREATE PROCEDURE enrich3()
BEGIN
     DECLARE cnt INTEGER;
     SET cnt = (SELECT count(*) FROM area ) ;
     INSERT INTO sensors(ip, location, kind,value)
         SELECT ip, substring(location,0,3), kind, value FROM istream;
     IF cnt = 0
     THEN
         INSERT INTO area SELECT ip, substring(location,0,3) FROM istream;
     END IF;
END;

rollback;
