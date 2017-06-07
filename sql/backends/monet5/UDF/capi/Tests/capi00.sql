

START TRANSACTION;

CREATE FUNCTION capi00(inp INTEGER) RETURNS INTEGER LANGUAGE C {
	result->initialize(result, inp.count);
	for(size_t i = 0; i < inp.count; i++) {
		result->data[i] = inp.data[i] * 2;
	}
};

CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (4), (5);

SELECT capi00(i) FROM integers;

ROLLBACK;
