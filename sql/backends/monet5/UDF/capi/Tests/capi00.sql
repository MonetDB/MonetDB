
START TRANSACTION;

CREATE FUNCTION capi00(inp INTEGER) RETURNS INTEGER LANGUAGE C {
    size_t i;
    result->initialize(result, inp.count);
    for(i = 0; i < inp.count; i++) {
        result->data[i] = inp.data[i] * 2;
    }
};

CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (4), (5);

SELECT i, capi00(i) FROM integers;

DROP FUNCTION capi00;
DROP TABLE integers;

ROLLBACK;
