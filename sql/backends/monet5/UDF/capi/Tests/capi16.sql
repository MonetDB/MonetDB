
START TRANSACTION;
CREATE FUNCTION string_reverse(input STRING) RETURNS STRING
LANGUAGE C {
#include <string.h>
    size_t i, j;
    result->initialize(result, input.count);
    for(i = 0; i < input.count; i++) {
        char* input_string = input.data[i];
        size_t len = strlen(input_string);
        result->data[i] = malloc(len + 1);
        for(j = 0; j < len; j++) {
            result->data[i][j] = input_string[len - j - 1];
        }
        result->data[i][len] = '\0';
    }
};

SELECT 'hello', string_reverse('hello');


CREATE FUNCTION capi10(inp BLOB) RETURNS BLOB LANGUAGE C {
    size_t i;
    result->initialize(result, inp.count);
    for(i = 0; i < inp.count; i++) {
        if (inp.is_null(inp.data[i])) {
            result->data[i] = result->null_value;
        } else {
            result->data[i].data = malloc(inp.data[i].size);
            memcpy(result->data[i].data, inp.data[i].data, inp.data[i].size);
            result->data[i].size = inp.data[i].size;
        }
    }
};

SELECT BLOB '00FFFF00', capi10(BLOB '00FFFF00');


CREATE FUNCTION capi00(inp INTEGER) RETURNS INTEGER LANGUAGE C {
    size_t i;
    result->initialize(result, inp.count);
    for(i = 0; i < inp.count; i++) {
        result->data[i] = inp.data[i] * 2;
    }
};

SELECT 1, capi00(1);

CREATE FUNCTION capi02_increment_year(d DATE) RETURNS DATE
language C
{
    size_t i;
    result->initialize(result, d.count);
    for(i = 0; i < result->count; i++) {
        if (d.is_null(d.data[i])) {
            result->data[i] = result->null_value;
        } else {
            result->data[i].year = d.data[i].year + 1;
            result->data[i].month = d.data[i].month;
            result->data[i].day = d.data[i].day;
        }
    }
};


SELECT capi02_increment_year('1992-09-20');


CREATE FUNCTION capi02_randomize_time(d TIME) RETURNS TIME
language C
{
    size_t i;
    result->initialize(result, d.count);
    for(i = 0; i < result->count; i++) {
        if (d.is_null(d.data[i])) {
            result->data[i] = result->null_value;
        } else {
            result->data[i].hours = (i + 1234) % 24;
            result->data[i].minutes = (i + 1234) % 60;
            result->data[i].seconds = (i + 1234) % 60;
            result->data[i].ms = (i + 1234) % 1000;
        }
    }
};

SELECT capi02_randomize_time('03:03:02.0101');


CREATE FUNCTION capi02_increment_timestamp(d TIMESTAMP) RETURNS TIMESTAMP
language C
{
    size_t i;
    result->initialize(result, d.count);
    for(i = 0; i < result->count; i++) {
        if (d.is_null(d.data[i])) {
            result->data[i] = result->null_value;
        } else {
            result->data[i].date.year = d.data[i].date.year + 1;
            result->data[i].date.month = d.data[i].date.month;
            result->data[i].date.day = d.data[i].date.day;

            result->data[i].time.hours = (i + 1234) % 24;
            result->data[i].time.minutes = (i + 1234) % 60;
            result->data[i].time.seconds = (i + 1234) % 60;
            result->data[i].time.ms = (i + 1234) % 1000;
        }
    }
};

SELECT capi02_increment_timestamp('1992-09-20 03:03:02.0101');

ROLLBACK;
