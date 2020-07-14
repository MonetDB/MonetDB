SELECT (str_to_timestamp('2015-10-01', '%Y-%m-%d') - str_to_timestamp('2015-09-30', '%Y-%m-%d'))/86400;

SELECT (str_to_timestamp('2015-10-01', '%Y-%m-%d') - str_to_timestamp('2015-09-30', '%Y-%m-%d'))/86400;

SELECT (str_to_timestamp('2015-10-01', '%Y-%m-%d') - str_to_timestamp('2015-09-30', '%Y-%m-%d'))/86400.0;

SELECT date '2015-03-01' - (date '2015-02-01' - date '2015-01-01');
