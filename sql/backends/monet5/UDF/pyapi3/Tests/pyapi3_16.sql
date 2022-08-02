# Test the special _columns and _column_types parameters that store information of the columns
START TRANSACTION;

CREATE TABLE vals(a STRING, b STRING, c STRING, d INTEGER);
INSERT INTO vals VALUES ('foo', 'bar', '123', 33), ('t', 'e', 's', 7), ('f', 'o', 'u', 4), ('i', 'k', 'r', 149);

CREATE FUNCTION pyapi16(a STRING, b string, c STRING, d INTEGER) returns table (d boolean)
language P
{
	print(_columns['a'])
	print(_columns['b'])
	print(_columns['c'])
	print(_columns['d'])
	print(_column_types)
	return True
};
SELECT * FROM pyapi16( (SELECT * FROM vals) );
DROP FUNCTION pyapi16;
DROP TABLE vals;

ROLLBACK;
