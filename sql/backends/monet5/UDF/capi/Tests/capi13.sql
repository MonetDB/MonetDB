
START TRANSACTION;

# grouped aggregate: sum

CREATE AGGREGATE capi13(inp INTEGER) RETURNS BIGINT LANGUAGE C {
#include <string.h>
	// initialize one aggregate per group
	result->initialize(result, aggr_group.count);
	// zero initialize the sums
	memset(result->data, 0, result->count * sizeof(result->null_value));
	// gather the sums for each of the groups
	for(size_t i = 0; i < inp.count; i++) {
		result->data[aggr_group.data[i]] += inp.data[i];
	}
};

CREATE TABLE vals(grp INTEGER, value INTEGER);
INSERT INTO vals VALUES (1, 100), (2, 200), (1, 50), (2, 300);

SELECT grp, capi13(value) FROM vals GROUP BY grp;

SELECT capi13(value) FROM vals;

DROP AGGREGATE capi13;
DROP TABLE vals;

ROLLBACK;
