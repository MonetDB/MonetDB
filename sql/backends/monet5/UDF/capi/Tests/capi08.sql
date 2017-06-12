# test caching behavior

START TRANSACTION;

CREATE FUNCTION capi08(inp INTEGER) RETURNS INTEGER LANGUAGE C {
    result->initialize(result, inp.count);
    for(size_t i = 0; i < inp.count; i++) {
        result->data[i] = inp.data[i] * 2;
    }
};

CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (4), (5);

SELECT capi08(i) FROM integers;
# this function should be loaded from the cache
SELECT capi08(i) FROM integers;

DROP FUNCTION capi08;

CREATE FUNCTION capi08(inp DOUBLE) RETURNS DOUBLE LANGUAGE C {
    result->initialize(result, inp.count);
    for(size_t i = 0; i < inp.count; i++) {
        result->data[i] = inp.data[i] * 2;
    }
};

# same code and same function name, but different parameters
# this needs to be recompiled now
SELECT capi08(i) FROM integers;
SELECT capi08(i) FROM integers;


ROLLBACK;
