
START TRANSACTION;

CREATE TABLE booleans(a BOOLEAN);
INSERT INTO booleans VALUES (1), (0), (1);

CREATE FUNCTION pyapi34a(inp BOOLEAN) RETURNS BOOLEAN LANGUAGE PYTHON {
	results = _conn.execute(u'SELECT * FROM booleans;')
	return {'result': numpy.logical_xor(inp, results['a']) };
};

CREATE FUNCTION pyapi34b(inp BOOLEAN) RETURNS BOOLEAN LANGUAGE PYTHON {
	return {'result': inp};
};

SELECT a, pyapi34a(a), pyapi34b(a) FROM booleans;

ROLLBACK;
