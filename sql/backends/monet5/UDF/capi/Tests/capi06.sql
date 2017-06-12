
# test CPP UDFs

START TRANSACTION;

CREATE FUNCTION capi06(inp INTEGER) RETURNS INTEGER LANGUAGE CPP {
#include <vector>
	std::vector<int> elements;
	for(size_t i = 0; i < inp.count; i++) {
		elements.push_back(inp.data[i]);
	}
	std::sort(elements.begin(), elements.end());
	result->initialize(result, inp.count);
	for(size_t i = 0; i < inp.count; i++) {
		result->data[i] = elements[i];
	}
};

CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (3), (4), (1), (2), (5);

SELECT capi06(i) FROM integers;

ROLLBACK;
