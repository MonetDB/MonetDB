/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* this file contains parts of queries that are used in multiple
 * places */

#define DATA_TYPE(t)					\
		"case " #t ".type "			\
		     "when 'bigint' then %d "		\
		     "when 'blob' then %d "		\
		     "when 'boolean' then %d "		\
		     "when 'char' then %d "		\
		     "when 'clob' then %d "		\
		     "when 'date' then %d "		\
		     "when 'decimal' then %d "		\
		     "when 'double' then %d "		\
		     "when 'hugeint' then %d "		\
		     "when 'int' then %d "		\
		     "when 'month_interval' then "	\
			  "case " #t ".type_digits "	\
			       "when 1 then %d "	\
			       "when 2 then %d "	\
			       "when 3 then %d "	\
			  "end "			\
		     "when 'real' then %d "		\
		     "when 'sec_interval' then "	\
			  "case " #t ".type_digits "	\
			       "when 4 then %d "	\
			       "when 5 then %d "	\
			       "when 6 then %d "	\
			       "when 7 then %d "	\
			       "when 8 then %d "	\
			       "when 9 then %d "	\
			       "when 10 then %d "	\
			       "when 11 then %d "	\
			       "when 12 then %d "	\
			       "when 13 then %d "	\
			  "end "			\
		     "when 'smallint' then %d "		\
		     "when 'time' then %d "		\
		     "when 'timestamp' then %d "	\
		     "when 'timestamptz' then %d "	\
		     "when 'timetz' then %d "		\
		     "when 'tinyint' then %d "		\
		     "when 'varchar' then %d "		\
		"end as data_type"
#define DATA_TYPE_ARGS							\
		SQL_BIGINT, SQL_LONGVARBINARY, SQL_BIT, SQL_WCHAR,	\
		SQL_WLONGVARCHAR, SQL_TYPE_DATE, SQL_DECIMAL,		\
		SQL_DOUBLE, SQL_HUGEINT, SQL_INTEGER,			\
		SQL_INTERVAL_YEAR, SQL_INTERVAL_YEAR_TO_MONTH,		\
		SQL_INTERVAL_MONTH, SQL_REAL, SQL_INTERVAL_DAY,		\
		SQL_INTERVAL_DAY_TO_HOUR, SQL_INTERVAL_DAY_TO_MINUTE,	\
		SQL_INTERVAL_DAY_TO_SECOND, SQL_INTERVAL_HOUR,		\
		SQL_INTERVAL_HOUR_TO_MINUTE,				\
		SQL_INTERVAL_HOUR_TO_SECOND, SQL_INTERVAL_MINUTE,	\
		SQL_INTERVAL_MINUTE_TO_SECOND, SQL_INTERVAL_SECOND,	\
		SQL_SMALLINT, SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP,	\
		SQL_TYPE_TIMESTAMP, SQL_TYPE_TIME, SQL_TINYINT,		\
		SQL_WVARCHAR

#define TYPE_NAME(t)							\
		"case " #t ".type "					\
		     "when 'bigint' then 'BIGINT' "			\
		     "when 'blob' then 'BINARY LARGE OBJECT' "		\
		     "when 'boolean' then 'BOOLEAN' "			\
		     "when 'char' then 'CHARACTER' "			\
		     "when 'clob' then 'CHARACTER LARGE OBJECT' "	\
		     "when 'date' then 'DATE' "				\
		     "when 'decimal' then 'DECIMAL' "			\
		     "when 'double' then 'DOUBLE' "			\
		     "when 'hugeint' then 'HUGEINT' "			\
		     "when 'int' then 'INTEGER' "			\
		     "when 'month_interval' then "			\
			  "case " #t ".type_digits "			\
			       "when 1 then 'INTERVAL YEAR' "		\
			       "when 2 then 'INTERVAL YEAR TO MONTH' "	\
			       "when 3 then 'INTERVAL MONTH' "		\
			  "end "					\
		     "when 'real' then 'REAL' "				\
		     "when 'sec_interval' then "			\
			  "case " #t ".type_digits "			\
			       "when 4 then 'INTERVAL DAY' "		\
			       "when 5 then 'INTERVAL DAY TO HOUR' "	\
			       "when 6 then 'INTERVAL DAY TO MINUTE' "	\
			       "when 7 then 'INTERVAL DAY TO SECOND' "	\
			       "when 8 then 'INTERVAL HOUR' "		\
			       "when 9 then 'INTERVAL HOUR TO MINUTE' " \
			       "when 10 then 'INTERVAL HOUR TO SECOND' " \
			       "when 11 then 'INTERVAL MINUTE' "	\
			       "when 12 then 'INTERVAL MINUTE TO SECOND' " \
			       "when 13 then 'INTERVAL SECOND' "	\
			  "end "					\
		     "when 'smallint' then 'SMALLINT' "			\
		     "when 'time' then 'TIME' "				\
		     "when 'timestamp' then 'TIMESTAMP' "		\
		     "when 'timestamptz' then 'TIMESTAMP' "		\
		     "when 'timetz' then 'TIME' "			\
		     "when 'tinyint' then 'TINYINT' "			\
		     "when 'varchar' then 'VARCHAR' "			\
		"end as type_name"

#define COLUMN_SIZE(t)					\
		"case " #t ".type "			\
		     "when 'date' then 10 "		\
		     "when 'month_interval' then "	\
			  "case " #t ".type_digits "	\
			       "when 1 then 26 "	\
			       "when 2 then 38 "	\
			       "when 3 then 27 "	\
			  "end "			\
		     "when 'sec_interval' then "	\
			  "case " #t ".type_digits "	\
			       "when 4 then 25 "	\
			       "when 5 then 36 "	\
			       "when 6 then 41 "	\
			       "when 7 then 47 "	\
			       "when 8 then 26 "	\
			       "when 9 then 39 "	\
			       "when 10 then 45 "	\
			       "when 11 then 28 "	\
			       "when 12 then 44 "	\
			       "when 13 then 30 "	\
			  "end "			\
		     "when 'time' then 12 "		\
		     "when 'timestamp' then 23 "	\
		     "when 'timestamptz' then 23 "	\
		     "when 'timetz' then 12 "		\
		     "else " #t ".type_digits "		\
		"end as column_size"

#define BUFFER_LENGTH(t) "case " #t ".type "				\
		     "when 'bigint' then 20 "				\
		     "when 'char' then 2 * " #t ".type_digits "		\
		     "when 'clob' then 2 * " #t ".type_digits "		\
		     "when 'date' then 10 "				\
		     "when 'double' then 24 "				\
		     "when 'hugeint' then 40 "				\
		     "when 'int' then 11 "				\
		     "when 'month_interval' then "			\
			  "case " #t ".type_digits "			\
			       "when 1 then 26 "			\
			       "when 2 then 38 "			\
			       "when 3 then 27 "			\
			  "end "					\
		     "when 'real' then 14 "				\
		     "when 'sec_interval' then "			\
			  "case " #t ".type_digits "			\
			       "when 4 then 25 "			\
			       "when 5 then 36 "			\
			       "when 6 then 41 "			\
			       "when 7 then 47 "			\
			       "when 8 then 26 "			\
			       "when 9 then 39 "			\
			       "when 10 then 45 "			\
			       "when 11 then 28 "			\
			       "when 12 then 44 "			\
			       "when 13 then 30 "			\
			  "end "					\
		     "when 'smallint' then 6 "				\
		     "when 'time' then 12 "				\
		     "when 'timestamp' then 23 "			\
		     "when 'timestamptz' then 23 "			\
		     "when 'timetz' then 12 "				\
		     "when 'tinyint' then 4 "				\
		     "when 'varchar' then 2 * " #t ".type_digits "	\
		     "else " #t ".type_digits "				\
		"end as buffer_length"

#define DECIMAL_DIGITS(t) "case " #t ".type "				\
		     "when 'bigint' then 0 "				\
		     "when 'decimal' then " #t ".type_scale "		\
		     "when 'double' then "				\
			  "case when " #t ".type_digits = 53 and " #t ".type_scale = 0 then 15 " \
			  "else " #t ".type_digits "			\
			  "end "					\
		     "when 'hugeint' then 0 "				\
		     "when 'int' then 0 "				\
		     "when 'month_interval' then 0 "			\
		     "when 'real' then "				\
			  "case when " #t ".type_digits = 24 and " #t ".type_scale = 0 then 7 " \
			  "else " #t ".type_digits "			\
			  "end "					\
		     "when 'sec_interval' then 0 "			\
		     "when 'smallint' then 0 "				\
		     "when 'time' then " #t ".type_digits - 1 "		\
		     "when 'timestamp' then " #t ".type_digits - 1 "	\
		     "when 'timestamptz' then " #t ".type_digits - 1 "	\
		     "when 'timetz' then " #t ".type_digits - 1 "	\
		     "when 'tinyint' then 0 "				\
		     "else cast(null as smallint) "			\
		"end as decimal_digits"

#define NUM_PREC_RADIX(t) "case " #t ".type "				\
		     "when 'bigint' then 2 "				\
		     "when 'decimal' then 10 "				\
		     "when 'double' then "				\
			  "case when " #t ".type_digits = 53 and " #t ".type_scale = 0 then 2 " \
			  "else 10 "					\
			  "end "					\
		     "when 'hugeint' then 2 "				\
		     "when 'int' then 2 "				\
		     "when 'real' then "				\
			  "case when " #t ".type_digits = 24 and " #t ". type_scale = 0 then 2 " \
			  "else 10 "					\
			  "end "					\
		     "when 'smallint' then 2 "				\
		     "when 'tinyint' then 2 "				\
		     "else cast(null as smallint) "			\
		"end as num_prec_radix"

#define SQL_DATA_TYPE(t)				\
		"case " #t ".type "			\
		     "when 'bigint' then %d "		\
		     "when 'blob' then %d "		\
		     "when 'boolean' then %d "		\
		     "when 'char' then %d "		\
		     "when 'clob' then %d "		\
		     "when 'date' then %d "		\
		     "when 'decimal' then %d "		\
		     "when 'double' then %d "		\
		     "when 'hugeint' then %d "		\
		     "when 'int' then %d "		\
		     "when 'month_interval' then %d "	\
		     "when 'real' then %d "		\
		     "when 'sec_interval' then %d "	\
		     "when 'smallint' then %d "		\
		     "when 'time' then %d "		\
		     "when 'timestamp' then %d "	\
		     "when 'timestamptz' then %d "	\
		     "when 'timetz' then %d "		\
		     "when 'tinyint' then %d "		\
		     "when 'varchar' then %d "		\
		"end as sql_data_type"
#define SQL_DATA_TYPE_ARGS						\
		SQL_BIGINT, SQL_LONGVARBINARY, SQL_BIT, SQL_WCHAR,	\
		SQL_WLONGVARCHAR, SQL_DATETIME, SQL_DECIMAL, SQL_DOUBLE, \
		SQL_HUGEINT, SQL_INTEGER, SQL_INTERVAL, SQL_REAL,	\
		SQL_INTERVAL, SQL_SMALLINT, SQL_DATETIME, SQL_DATETIME, \
		SQL_DATETIME, SQL_DATETIME, SQL_TINYINT, SQL_WVARCHAR

#define SQL_DATETIME_SUB(t)				\
		"case " #t ".type "			\
		     "when 'date' then %d "		\
		     "when 'month_interval' then "	\
			  "case " #t ".type_digits "	\
			       "when 1 then %d "	\
			       "when 2 then %d "	\
			       "when 3 then %d "	\
			  "end "			\
		     "when 'sec_interval' then "	\
			  "case " #t ".type_digits "	\
			       "when 4 then %d "	\
			       "when 5 then %d "	\
			       "when 6 then %d "	\
			       "when 7 then %d "	\
			       "when 8 then %d "	\
			       "when 9 then %d "	\
			       "when 10 then %d "	\
			       "when 11 then %d "	\
			       "when 12 then %d "	\
			       "when 13 then %d "	\
			  "end "			\
		     "when 'time' then %d "		\
		     "when 'timestamp' then %d "	\
		     "when 'timestamptz' then %d "	\
		     "when 'timetz' then %d "		\
		     "else cast(null as smallint) "	\
		"end as sql_datetime_sub"
#define SQL_DATETIME_SUB_ARGS						\
		SQL_CODE_DATE, SQL_CODE_YEAR, SQL_CODE_YEAR_TO_MONTH,	\
		SQL_CODE_MONTH, SQL_CODE_DAY, SQL_CODE_DAY_TO_HOUR,	\
		SQL_CODE_DAY_TO_MINUTE, SQL_CODE_DAY_TO_SECOND,		\
		SQL_CODE_HOUR, SQL_CODE_HOUR_TO_MINUTE,			\
		SQL_CODE_HOUR_TO_SECOND, SQL_CODE_MINUTE,		\
		SQL_CODE_MINUTE_TO_SECOND, SQL_CODE_SECOND,		\
		SQL_CODE_TIME, SQL_CODE_TIMESTAMP, SQL_CODE_TIMESTAMP,	\
		SQL_CODE_TIME

#define CHAR_OCTET_LENGTH(t)						\
		"case " #t ".type "					\
		     "when 'char' then 2 * " #t ".type_digits "		\
		     "when 'varchar' then 2 * " #t ".type_digits "	\
		     "when 'clob' then 2 * " #t ".type_digits "		\
		     "when 'blob' then " #t ".type_digits "		\
		     "else cast(null as integer) "			\
		"end as char_octet_length"
