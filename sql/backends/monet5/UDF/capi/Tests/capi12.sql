
START TRANSACTION;

# oids
CREATE FUNCTION capi12(inp OID) RETURNS BOOLEAN LANGUAGE C {
    size_t i;
    result->initialize(result, inp.count);
    for(i = 0; i < inp.count; i++) {
        if (inp.data[i] == inp.null_value) {
            result->data[i] = 0;
        } else {
            result->data[i] = 1;
        }
    }
};
CREATE TABLE oids(i OID);
INSERT INTO oids(i) VALUES (100), (NULL), (200);

SELECT * FROM oids WHERE capi12(i);

DROP FUNCTION capi12;
DROP TABLE oids;

ROLLBACK;
