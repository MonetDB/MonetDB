CREATE TABLE tbl (col1 DATE, col2 DATE, col3 CHARACTER LARGE OBJECT, col4 CHARACTER LARGE OBJECT);
CREATE TABLE tbl1 (col7 DATE, col3 CHARACTER LARGE OBJECT, col4 CHARACTER LARGE OBJECT, col5 BOOLEAN, col6 CHARACTER LARGE OBJECT);
INSERT INTO tbl VALUES ('2015-01-01','2015-01-01','Z','0000');
INSERT INTO tbl1 VALUES ('2015-01-01','Z','0000',false,'');

CREATE FUNCTION mydateformat(d date)
RETURNS CHAR(10)
BEGIN
	DECLARE outdate varchar(10);
	DECLARE dt int;
	DECLARE dm int;
	SET dt = EXTRACT(DAY FROM d);
	IF (dt < 10 )
		THEN SET outdate = '0' || dt || '-';
	ELSE SET outdate = dt || '-';
	END IF;
	SET dm = EXTRACT(MONTH FROM d);
	IF (dm < 10 )
		THEN SET outdate = outdate || '0' || dm || '-';
	ELSE SET outdate = outdate || dm || '-';
	END IF;
	SET outdate = outdate || EXTRACT(YEAR FROM d);
	RETURN outdate;
END;

UPDATE tbl1
SET (col5,col6) = (
	SELECT true ,'overwriten by col6 with tbl.col3 (' || tbl.col3 || ') and tbl.col1 (' || mydateformat(tbl.col1) || ') and tbl.col2 (' || mydateformat(tbl.col2) || ')'
	FROM tbl
	WHERE tbl1.col7 >= tbl.col1 AND tbl1.col7 <= tbl.col2
	AND ( (tbl1.col3 = tbl.col3 AND (tbl1.col4 = '0000' OR tbl.col4 = '0000')) OR (tbl1.col4 = tbl.col4))
);

SELECT * FROM tbl1;

DROP FUNCTION mydateformat;
DROP TABLE tbl1;
DROP TABLE tbl;

