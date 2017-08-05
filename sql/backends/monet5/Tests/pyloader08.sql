
# python loader function with multiple column inputs

START TRANSACTION;

CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (3), (4), (5);

CREATE LOADER pyloader08(i INTEGER, j INTEGER) LANGUAGE PYTHON {
    _emit.emit({'s': i, 't': j});
};

CREATE FUNCTION pyfunction(i INTEGER, j INTEGER) RETURNS TABLE(i INTEGER, j INTEGER) LANGUAGE PYTHON {
	return {'i': i, 'j': j};
};

CREATE TABLE pyloader08table FROM LOADER pyloader08(  (SELECT i, i FROM integers) );

SELECT * FROM pyloader08table;

#CREATE TABLE pyloader08table(s INTEGER, t INTEGER);
COPY LOADER INTO pyloader08table FROM pyloader08(  (SELECT i, i*2 FROM integers) );

SELECT * FROM pyloader08table;
DROP TABLE pyloader08table;
DROP LOADER pyloader08;


ROLLBACK;
