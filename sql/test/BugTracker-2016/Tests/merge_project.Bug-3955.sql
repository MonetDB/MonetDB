-- START QUERY
start transaction;

create table input_double (a1 string, a2 double, prob double);
INSERT INTO input_double VALUES ('latitude',52.0,1.0);
INSERT INTO input_double VALUES ('longitude',5.1,1.0);


CREATE TABLE v(a1 int, a2 point, prob double);
insert into v values(0,'point(50 4)',1);
insert into v values(1,'point(51 5)',1);
insert into v values(2,'point(52 6)',1);


CREATE VIEW p AS 
SELECT ST_Point(a1,a2) AS a1, prob AS prob 
FROM (
	  SELECT tmp_2.a2 AS a1, tmp_3.a2 AS a2, tmp_2.prob * tmp_3.prob AS prob 
	  FROM 
	    (SELECT a2, prob FROM input_double WHERE a1 = 'latitude') AS tmp_2,
	    (SELECT a2, prob FROM input_double WHERE a1 = 'longitude') AS tmp_3
) AS tmp;

CREATE VIEW r AS 
SELECT a1 AS a1, ST_Distance(a2,a3) AS prob 
FROM (
	  SELECT v.a1 AS a1, v.a2 AS a2, p.a1 AS a3, v.prob * p.prob AS prob 
	  FROM v,p
) AS tmp;

plan select * from r;
-- END QUERY
