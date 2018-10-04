# NULL values
START TRANSACTION;

CREATE FUNCTION capi14(i INTEGER, j REAL, k DOUBLE) RETURNS DOUBLE LANGUAGE C {
    size_t it;
    result->initialize(result, i.count);
    for(it = 0; it < i.count; it++) {
        result->data[it] = 0;
        if (!i.is_null(i.data[it])) {
            result->data[it] += i.data[it];
        } else {
            result->data[it] -= 1;
        }
        if (!j.is_null(j.data[it])) {
            result->data[it] += j.data[it];
        } else {
            result->data[it] -= 1;
        }
        if (!k.is_null(k.data[it])) {
            result->data[it] += k.data[it];
        } else {
            result->data[it] -= 1;
        }
    }
};

CREATE TABLE vals(i INTEGER, j REAL, k DOUBLE);
INSERT INTO vals VALUES (1, NULL, 1), (NULL, 2, 2), (3, 3, NULL), (NULL, NULL, NULL), (5, 5, 5);

SELECT i, j, k, capi14(i, j, k) FROM vals;

DROP FUNCTION capi14;
DROP TABLE vals;

ROLLBACK;
