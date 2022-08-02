# Dates, times, timestamps
START TRANSACTION;

# dates
# dates have the type
#typedef struct {
#   unsigned char day;
#   unsigned char month;
#   int year;
#} cudf_data_date;

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


CREATE TABLE dates(i DATE);
INSERT INTO dates VALUES ('1992-09-20'), ('2000-03-10'), ('1000-05-03'), (NULL);

SELECT capi02_increment_year(i) FROM dates;
DROP FUNCTION capi02_increment_year;
DROP TABLE dates;


#time
#time has the type:
#typedef struct {
#   unsigned int ms;
#   unsigned char seconds;
#   unsigned char minutes;
#   unsigned char hours;
#} cudf_data_time;

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


CREATE TABLE times(i TIME);
INSERT INTO times VALUES ('03:03:02.0101'), (NULL), ('03:03:02.0101');

SELECT capi02_randomize_time(i) FROM times;
DROP FUNCTION capi02_randomize_time;
DROP TABLE times;

#timestamps
#timestamps have the type:
#typedef struct {
#   cudf_data_date date;
#   cudf_data_time time;
#} cudf_data_timestamp;

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


CREATE TABLE times(i TIMESTAMP);
INSERT INTO times VALUES ('1992-09-20 03:03:02.0101'), (NULL), ('2000-03-10 03:03:02.0101');

SELECT capi02_increment_timestamp(i) FROM times;
DROP FUNCTION capi02_increment_timestamp;
DROP TABLE times;

ROLLBACK;
