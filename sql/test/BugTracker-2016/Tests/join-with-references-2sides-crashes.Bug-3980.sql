CREATE TABLE foo (barid INT, name VARCHAR(25), result BOOLEAN);
CREATE TABLE bar (id INT, version VARCHAR(25));
UPDATE foo SET result =
(
	SELECT TRUE 
	FROM (
		SELECT DISTINCT B.version
		FROM foo AS F
		INNER JOIN bar AS B ON (F.barid = B.id)
	) AS X
	WHERE X.version =
	(
		SELECT
		version
		FROM bar
		WHERE barid = foo.barid
	)
	AND foo.name='0000' 
);

DROP TABLE foo;
DROP TABLE bar;

