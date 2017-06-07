# Dates, times, timestamps
START TRANSACTION;

CREATE FUNCTION capi02_increment_year(d DATE) RETURNS DATE
language C
{
	result->initialize(result, d.count);
	for(size_t i = 0; i < result->count; i++) {
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


CREATE FUNCTION capi02_randomize_time(d TIME) RETURNS TIME
language C
{
	srand(1234);
	result->initialize(result, d.count);
	for(size_t i = 0; i < result->count; i++) {
		if (d.is_null(d.data[i])) {
			result->data[i] = result->null_value;
		} else {
			result->data[i].hours = rand() % 24;
			result->data[i].minutes = rand() % 60;
			result->data[i].seconds = rand() % 60;
			result->data[i].ms = rand() % 1000;
		}
	}
};


CREATE TABLE times(i TIME);
INSERT INTO times VALUES ('03:03:02.0101'), (NULL), ('03:03:02.0101');

SELECT capi02_randomize_time(i) FROM times;
DROP FUNCTION capi02_randomize_time;
DROP TABLE times;


CREATE FUNCTION capi02_increment_timestamp(d TIMESTAMP) RETURNS TIMESTAMP
language C
{
	srand(1234);
	result->initialize(result, d.count);
	for(size_t i = 0; i < result->count; i++) {
		if (d.is_null(d.data[i])) {
			printf("Null value!\n");
			result->data[i] = result->null_value;
		} else {
			result->data[i].date.year = d.data[i].date.year + 1;
			result->data[i].date.month = d.data[i].date.month;
			result->data[i].date.day = d.data[i].date.day;

			result->data[i].time.hours = rand() % 24;
			result->data[i].time.minutes = rand() % 60;
			result->data[i].time.seconds = rand() % 60;
			result->data[i].time.ms = rand() % 1000;
		}
	}
};


CREATE TABLE times(i TIMESTAMP);
INSERT INTO times VALUES ('1992-09-20 03:03:02.0101'), (NULL), ('2000-03-10 03:03:02.0101');

SELECT capi02_increment_timestamp(i) FROM times;
DROP FUNCTION capi02_increment_timestamp;
DROP TABLE times;

ROLLBACK;
