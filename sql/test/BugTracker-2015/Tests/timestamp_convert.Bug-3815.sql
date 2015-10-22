SELECT CONVERT( CONVERT( (str_to_timestamp('2015-10-01', '%Y-%m-%d') - str_to_timestamp('2015-09-30', '%Y-%m-%d')), BIGINT) /86400, BIGINT);

SELECT CONVERT( (str_to_timestamp('2015-10-01', '%Y-%m-%d') - str_to_timestamp('2015-09-30', '%Y-%m-%d'))/86400, BIGINT);

SELECT (str_to_timestamp('2015-10-01', '%Y-%m-%d') - str_to_timestamp('2015-09-30', '%Y-%m-%d'))/86400.0;
