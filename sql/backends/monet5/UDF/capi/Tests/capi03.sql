# Failure cases


START TRANSACTION;

# Return different amount of rows in table-producing functions

CREATE FUNCTION capi03() RETURNS TABLE(i INTEGER, j INTEGER) LANGUAGE C {
    size_t index;
    i->initialize(i, 10);
    j->initialize(j, 20);
    for(index = 0; index < i->count; index++) {
        i->data[index] = 0;
    }
    for(index = 0; index < j->count; index++) {
        j->data[index] = 1;
    }
};

SELECT * FROM capi03();

ROLLBACK;

START TRANSACTION;

# No return value
CREATE FUNCTION capi03(inp INTEGER) RETURNS INTEGER LANGUAGE C {
    
};

CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (4), (5);

SELECT capi03(i) FROM integers;

ROLLBACK;


START TRANSACTION;

# Manually return an error from the function

CREATE FUNCTION capi03(inp INTEGER) RETURNS INTEGER LANGUAGE C {
    return "Something went wrong!";
};

CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (4), (5);

SELECT capi03(i) FROM integers;

ROLLBACK;
