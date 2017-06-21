

START TRANSACTION;

CREATE AGGREGATE capi07(inp INTEGER) RETURNS BIGINT LANGUAGE C {
    size_t i;
    lng sum = 0;
    for(i = 0; i < inp.count; i++) {
        sum += inp.data[i];
    }
    result->initialize(result, 1);
    result->data[0] = sum;
};

CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (3), (4), (1), (2), (5), (6);

SELECT capi07(i) FROM integers;

ROLLBACK;
