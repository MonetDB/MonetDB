
START TRANSACTION;

CREATE FUNCTION capi16(i INTEGER, j STRING, k BLOB, l DATE, m TIMESTAMP, n TIME)
RETURNS INTEGER
LANGUAGE C {
    result->initialize(result, i.count);
    for(size_t it = 0; it < i.count; it++) {
        result->data[it] = i.data[it] * 2;
    }
};

CREATE TABLE capi16table(i INTEGER, j STRING, k BLOB, l DATE, m TIMESTAMP, n TIME);
SELECT i, capi16(i, j, k, l, m, n) FROM capi16table;

DROP TABLE capi16table;
DROP FUNCTION capi16;

ROLLBACK;
