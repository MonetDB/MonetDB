


START TRANSACTION;

CREATE FUNCTION capi17(inp OID) RETURNS BOOLEAN LANGUAGE C {
    size_t i;
    result->initialize(result, inp.count);
    char* value = malloc(100);
    for(i = 0; i < inp.count; i++) {
        if (inp.data[i] == inp.null_value) {
            result->data[i] = 0;
        } else {
            result->data[i] = 1;
        }
    }
    free(value);
};
CREATE TABLE oids(i OID);
INSERT INTO oids(i) VALUES (100), (NULL), (200);

SELECT * FROM oids WHERE capi17(i);

DROP FUNCTION capi17;
DROP TABLE oids;

ROLLBACK;
