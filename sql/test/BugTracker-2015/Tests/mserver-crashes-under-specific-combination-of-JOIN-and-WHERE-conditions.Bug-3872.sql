CREATE TABLE foo (startdate DATE, enddate DATE);
CREATE TABLE bar (id INTEGER);

SELECT COUNT(*)
FROM foo AS A
INNER JOIN foo AS B
ON (
	(B.startdate = B.enddate
		AND (B.startdate = A.startdate OR B.startdate = A.enddate)
		) OR (
		A.startdate < B.enddate AND A.enddate > B.startdate
	)
)
WHERE EXISTS (
	SELECT TRUE
	FROM bar
	WHERE A.startdate > '2010-10-01'
)
AND A.startdate >= B.startdate;

DROP TABLE foo;
DROP TABLE bar;
