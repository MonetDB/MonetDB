# test strings

START TRANSACTION;

CREATE FUNCTION capi04(inp STRING) RETURNS STRING LANGUAGE C {
#include <string.h>
    size_t i;

    result->initialize(result, inp.count);
    for(i = 0; i < inp.count; i++) {
        if (inp.is_null(inp.data[i])) {
            result->data[i] = result->null_value;
        } else {
            result->data[i] = malloc(strlen(inp.data[i]) + 2);
            strcpy(result->data[i] + 1, inp.data[i]);
            result->data[i][0] = 'H';
        }
    }
};

CREATE TABLE strings(i STRING);
INSERT INTO strings VALUES ('ello'), ('ow'), (NULL), ('onestly?'), ('annes');

SELECT capi04(i) FROM strings;

DROP FUNCTION capi04;

ROLLBACK;

START TRANSACTION;
# return constant strings, instead of allocated strings
CREATE FUNCTION capi04(inp STRING) RETURNS STRING LANGUAGE C {
#include <string.h>
    size_t i;

    result->initialize(result, inp.count);
    for(i = 0; i < inp.count; i++) {
        if (inp.is_null(inp.data[i])) {
            result->data[i] = result->null_value;
        } else {
            result->data[i] = "hello";
        }
    }
};

CREATE TABLE strings(i STRING);
INSERT INTO strings VALUES ('ello'), ('ow'), (NULL), ('onestly?'), ('annes');

SELECT capi04(i) FROM strings;

ROLLBACK;

