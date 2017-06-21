
START TRANSACTION;

# uuids
# other (unsupported) types are simply converted to/from strings

CREATE FUNCTION capi12(inp UUID) RETURNS UUID LANGUAGE C {
    size_t i;
    result->initialize(result, inp.count);
    for(i = 0; i < inp.count; i++) {
        if (inp.data[i] == inp.null_value) {
            result->data[i] = result->null_value;
        } else {
            result->data[i] = inp.data[i];
        }
    }
};

CREATE TABLE uuids(d UUID);
INSERT INTO uuids VALUES ('ad887b3d-08f7-c308-7285-354a1857cbc8'), (NULL);

SELECT capi12(d) FROM uuids;

DROP FUNCTION capi12;
DROP TABLE uuids;

ROLLBACK;
