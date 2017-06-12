

START TRANSACTION;

CREATE AGGREGATE capi06(inp INTEGER) RETURNS BIGINT LANGUAGE C {
	lng sum = 0;
	for(size_t i = 0; i < inp.count; i++) {
		sum += inp.data[i];
	}
	result->initialize(result, 1);
	result->data[0] = sum;
};

CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (3), (4), (1), (2), (5), (6);

SELECT capi06(i) FROM integers;

ROLLBACK;
