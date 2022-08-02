# memory leak test
# randomly allocate some buffers


START TRANSACTION;

CREATE FUNCTION capi05(inp INTEGER) RETURNS INTEGER LANGUAGE C {
    size_t i;
    char* mydata = malloc(1000);
    result->initialize(result, inp.count);
    for(i = 0; i < inp.count; i++) {
        result->data[i] = inp.data[i] * 2;
    }
};

CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (4), (5);

SELECT capi05(i) FROM integers;

ROLLBACK;
