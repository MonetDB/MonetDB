# test caching behavior

START TRANSACTION;

CREATE FUNCTION capi08(inp INTEGER) RETURNS INTEGER LANGUAGE C {
    size_t i;
    result->initialize(result, inp.count);
    for(i = 0; i < inp.count; i++) {
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
    size_t i;
    result->initialize(result, inp.count);
    for(i = 0; i < inp.count; i++) {
        result->data[i] = inp.data[i] * 2;
    }
};

# same code and same function name, but different parameters
# this needs to be recompiled now
SELECT capi08(i) FROM integers;
SELECT capi08(i) FROM integers;


DROP FUNCTION capi08;
CREATE FUNCTION capi08(inp1 INTEGER, inp2 INTEGER) RETURNS INTEGER LANGUAGE C {
    size_t i;
    result->initialize(result, inp1.count);
    for(i = 0; i < inp1.count; i++) {
        result->data[i] = inp1.data[i] / inp2.data[i];
    }
};

SELECT capi08(i * 2, i) FROM integers;

DROP FUNCTION capi08;
CREATE FUNCTION capi08(inp2 INTEGER, inp1 INTEGER) RETURNS INTEGER LANGUAGE C {
    size_t i;
    result->initialize(result, inp1.count);
    for(i = 0; i < inp1.count; i++) {
        result->data[i] = inp1.data[i] / inp2.data[i];
    }
};

# same function body and parameter types, but switched parameter names
# should still give the same result
SELECT capi08(i, i * 2) FROM integers;



ROLLBACK;
