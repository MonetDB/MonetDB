/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * This code was created by Peter Harvey (mostly during Christmas 98/99).
 * This code is LGPL. Please ensure that this message remains in future
 * distributions and uses of this code (thats about all I get out of it).
 * - Peter Harvey pharvey@codebydesign.com
 *
 * This file has been modified for the MonetDB project.  See the file
 * Copyright in this directory for more information.
 */

/**********************************************************************
 * SQLGetTypeInfo()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include <float.h>


static const struct types {
	const char *type_name;
	const int data_type;	/* concise type */
	const int column_size;
	const char *literal_prefix;
	const char *literal_suffix;
	const char *create_params;
	const int nullable;	/* NO_NULLS, NULLABLE, NULLABLE_UNKNOWN */
	const int case_sensitive;	/* SQL_FALSE, SQL_TRUE */
	const int searchable;	/* PRED_NONE, PRED_CHAR, PRED_BASIC, SEARCHABLE */
	const int unsigned_attribute;	/* SQL_FALSE, SQL_TRUE, NULL */
	const int fixed_prec_scale;	/* SQL_FALSE, SQL_TRUE */
	const int auto_unique_value;	/* SQL_FALSE, SQL_TRUE, NULL */
	const char *local_type_name;	/* localized version of type_name */
	const int minimum_scale;
	const int maximum_scale;
	const int sql_data_type;
	const int sql_datetime_sub;
	const int num_prec_radix;
	const int interval_precision;
} types[] = {
	/* This table is sorted on the value of data_type and then on
	 * how "close" the type maps to the corresponding ODBC SQL
	 * type (i.e. in the order SQLGetTypeInfo wants it).
	 * Except for the type_name value, the string values are ready
	 * to paste into an SQL query (i.e. including quotes). */
	{
		.type_name = "uuid",
		.data_type = SQL_GUID,	/* -11 */
		.column_size = 36,
		.literal_prefix = "'uuid '''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "'uuid'",
		.minimum_scale = -1,
		.maximum_scale = -1,
		.sql_data_type = SQL_GUID,
		.sql_datetime_sub = -1,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "character large object",
		.data_type = SQL_WLONGVARCHAR,	/* -10 */
		.column_size = 1000000,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_TRUE,
		.searchable = SQL_SEARCHABLE,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = -1,
		.maximum_scale = -1,
		.sql_data_type = SQL_WLONGVARCHAR,
		.sql_datetime_sub = -1,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "json",
		.data_type = SQL_WLONGVARCHAR,	/* -10 */
		.column_size = 1000000,
		.literal_prefix = "'json '''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_TRUE,
		.searchable = SQL_SEARCHABLE,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "'json'",
		.minimum_scale = -1,
		.maximum_scale = -1,
		.sql_data_type = SQL_WLONGVARCHAR,
		.sql_datetime_sub = -1,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "url",
		.data_type = SQL_WLONGVARCHAR,	/* -10 */
		.column_size = 1000000,
		.literal_prefix = "'url '''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_TRUE,
		.searchable = SQL_SEARCHABLE,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "'url'",
		.minimum_scale = -1,
		.maximum_scale = -1,
		.sql_data_type = SQL_WLONGVARCHAR,
		.sql_datetime_sub = -1,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "varchar",
		.data_type = SQL_WVARCHAR,	/* -9 */
		.column_size = 1000000,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "'length'",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_TRUE,
		.searchable = SQL_SEARCHABLE,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = -1,
		.maximum_scale = -1,
		.sql_data_type = SQL_WVARCHAR,
		.sql_datetime_sub = -1,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "character",
		.data_type = SQL_WCHAR,	/* -8 */
		.column_size = 1000000,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "'length'",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_TRUE,
		.searchable = SQL_SEARCHABLE,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = -1,
		.maximum_scale = -1,
		.sql_data_type = SQL_WCHAR,
		.sql_datetime_sub = -1,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "char",
		.data_type = SQL_WCHAR,	/* -8 */
		.column_size = 1000000,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "'length'",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_TRUE,
		.searchable = SQL_SEARCHABLE,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = -1,
		.maximum_scale = -1,
		.sql_data_type = SQL_WCHAR,
		.sql_datetime_sub = -1,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "boolean",
		.data_type = SQL_BIT,	/* -7 */
		.column_size = 1,
		.literal_prefix = "NULL",
		.literal_suffix = "NULL",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = SQL_TRUE,
		.fixed_prec_scale = SQL_TRUE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "'boolean'",
		.minimum_scale = -1,
		.maximum_scale = -1,
		.sql_data_type = SQL_BIT,
		.sql_datetime_sub = -1,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "tinyint",
		.data_type = SQL_TINYINT,	/* -6 */
		.column_size = 3,
		.literal_prefix = "NULL",
		.literal_suffix = "NULL",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = SQL_FALSE,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_TINYINT,
		.sql_datetime_sub = -1,
		.num_prec_radix = 10,
		.interval_precision = -1,
	},
	{
		.type_name = "bigint",
		.data_type = SQL_BIGINT,	/* -5 */
		.column_size = 19,
		.literal_prefix = "NULL",
		.literal_suffix = "NULL",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = SQL_FALSE,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_BIGINT,
		.sql_datetime_sub = -1,
		.num_prec_radix = 10,
		.interval_precision = -1,
	},
	{
		.type_name = "bigserial",
		.data_type = SQL_BIGINT,	/* -5 */
		.column_size = 19,
		.literal_prefix = "NULL",
		.literal_suffix = "NULL",
		.create_params = "NULL",
		.nullable = SQL_NO_NULLS,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = SQL_FALSE,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_TRUE,
		.local_type_name = "'bigserial'",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_BIGINT,
		.sql_datetime_sub = -1,
		.num_prec_radix = 10,
		.interval_precision = -1,
	},
	{
		.type_name = "binary large object",
		.data_type = SQL_LONGVARBINARY,	/* -4 */
		.column_size = 1000000,
		.literal_prefix = "'x'''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_TRUE,
		.searchable = SQL_SEARCHABLE,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = -1,
		.maximum_scale = -1,
		.sql_data_type = SQL_LONGVARBINARY,
		.sql_datetime_sub = -1,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "binary large object",
		.data_type = SQL_VARBINARY,	/* -3 */
		.column_size = 1000000,
		.literal_prefix = "'x'''",
		.literal_suffix = "''''",
		.create_params = "'length'",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_TRUE,
		.searchable = SQL_SEARCHABLE,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "'blob(max_length)'",
		.minimum_scale = -1,
		.maximum_scale = -1,
		.sql_data_type = SQL_VARBINARY,
		.sql_datetime_sub = -1,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	/* SQL_BINARY	-2 */
	{
		.type_name = "character large object",
		.data_type = SQL_LONGVARCHAR,	/* -1 */
		.column_size = 1000000,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_TRUE,
		.searchable = SQL_SEARCHABLE,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = -1,
		.maximum_scale = -1,
		.sql_data_type = SQL_LONGVARCHAR,
		.sql_datetime_sub = -1,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "char",
		.data_type = SQL_CHAR,	/* 1 */
		.column_size = 1000000,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "'length'",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_TRUE,
		.searchable = SQL_SEARCHABLE,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = -1,
		.maximum_scale = -1,
		.sql_data_type = SQL_CHAR,
		.sql_datetime_sub = -1,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "numeric",
		.data_type = SQL_NUMERIC,	/* 2 */
		.column_size = 19,
		.literal_prefix = "NULL",
		.literal_suffix = "NULL",
		.create_params = "'precision,scale'",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = SQL_FALSE,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 18,
		.sql_data_type = SQL_NUMERIC,
		.sql_datetime_sub = -1,
		.num_prec_radix = 10,
		.interval_precision = -1,
	},
	{
		.type_name = "decimal",
		.data_type = SQL_DECIMAL,	/* 3 */
		.column_size = 19,
		.literal_prefix = "NULL",
		.literal_suffix = "NULL",
		.create_params = "'precision,scale'",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = SQL_FALSE,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 18,
		.sql_data_type = SQL_DECIMAL,
		.sql_datetime_sub = -1,
		.num_prec_radix = 10,
		.interval_precision = -1,
	},
	{
		.type_name = "integer",
		.data_type = SQL_INTEGER,	/* 4 */
		.column_size = 10,
		.literal_prefix = "NULL",
		.literal_suffix = "NULL",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = SQL_FALSE,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTEGER,
		.sql_datetime_sub = -1,
		.num_prec_radix = 10,
		.interval_precision = -1,
	},
	{
		.type_name = "int",
		.data_type = SQL_INTEGER,	/* 4 */
		.column_size = 10,
		.literal_prefix = "NULL",
		.literal_suffix = "NULL",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = SQL_FALSE,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTEGER,
		.sql_datetime_sub = -1,
		.num_prec_radix = 10,
		.interval_precision = -1,
	},
	{
		.type_name = "mediumint",
		.data_type = SQL_INTEGER,	/* 4 */
		.column_size = 10,
		.literal_prefix = "NULL",
		.literal_suffix = "NULL",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = SQL_FALSE,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "'int'",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTEGER,
		.sql_datetime_sub = -1,
		.num_prec_radix = 10,
		.interval_precision = -1,
	},
	{
		.type_name = "serial",
		.data_type = SQL_INTEGER,	/* 4 */
		.column_size = 10,
		.literal_prefix = "NULL",
		.literal_suffix = "NULL",
		.create_params = "NULL",
		.nullable = SQL_NO_NULLS,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = SQL_FALSE,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_TRUE,
		.local_type_name = "'serial'",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTEGER,
		.sql_datetime_sub = -1,
		.num_prec_radix = 10,
		.interval_precision = -1,
	},
	{
		.type_name = "smallint",
		.data_type = SQL_SMALLINT,	/* 5 */
		.column_size = 5,
		.literal_prefix = "NULL",
		.literal_suffix = "NULL",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = SQL_FALSE,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_SMALLINT,
		.sql_datetime_sub = -1,
		.num_prec_radix = 10,
		.interval_precision = -1,
	},
	{
		.type_name = "float",
		.data_type = SQL_FLOAT,	/* 6 */
		.column_size = DBL_MANT_DIG,
		.literal_prefix = "NULL",
		.literal_suffix = "NULL",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = SQL_FALSE,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_FLOAT,
		.sql_datetime_sub = -1,
		.num_prec_radix = 2,
		.interval_precision = -1,
	},
	{
		.type_name = "real",
		.data_type = SQL_REAL,	/* 7 */
		.column_size = FLT_MANT_DIG,
		.literal_prefix = "NULL",
		.literal_suffix = "NULL",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = SQL_FALSE,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_REAL,
		.sql_datetime_sub = -1,
		.num_prec_radix = 2,
		.interval_precision = -1,
	},
	{
		.type_name = "double",
		.data_type = SQL_DOUBLE,	/* 8 */
		.column_size = DBL_MANT_DIG,
		.literal_prefix = "NULL",
		.literal_suffix = "NULL",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = SQL_FALSE,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_DOUBLE,
		.sql_datetime_sub = -1,
		.num_prec_radix = 2,
		.interval_precision = -1,
	},
	{
		.type_name = "varchar",
		.data_type = SQL_VARCHAR,	/* 12 */
		.column_size = 1000000,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "'length'",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_TRUE,
		.searchable = SQL_SEARCHABLE,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = -1,
		.maximum_scale = -1,
		.sql_data_type = SQL_VARCHAR,
		.sql_datetime_sub = -1,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "date",
		.data_type = SQL_TYPE_DATE,	/* 91 */
		.column_size = 10,
		.literal_prefix = "'date '''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = -1,
		.maximum_scale = -1,
		.sql_data_type = SQL_DATETIME,
		.sql_datetime_sub = SQL_CODE_DATE,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "time",
		.data_type = SQL_TYPE_TIME,	/* 92 */
		.column_size = 8,
		.literal_prefix = "'time '''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_DATETIME,
		.sql_datetime_sub = SQL_CODE_TIME,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "time(precision)",
		.data_type = SQL_TYPE_TIME,	/* 92 */
		.column_size = 9 + 6,
		.literal_prefix = "'time '''",
		.literal_suffix = "''''",
		.create_params = "'precision'",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 6,
		.sql_data_type = SQL_DATETIME,
		.sql_datetime_sub = SQL_CODE_TIME,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "timestamp",
		.data_type = SQL_TYPE_TIMESTAMP,	/* 93 */
		.column_size = 19,
		.literal_prefix = "'timestamp '''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_DATETIME,
		.sql_datetime_sub = SQL_CODE_TIMESTAMP,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "timestamp(precision)",
		.data_type = SQL_TYPE_TIMESTAMP,	/* 93 */
		.column_size = 20 + 6,
		.literal_prefix = "'timestamp '''",
		.literal_suffix = "''''",
		.create_params = "'precision'",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 6,
		.sql_data_type = SQL_DATETIME,
		.sql_datetime_sub = SQL_CODE_TIMESTAMP,
		.num_prec_radix = -1,
		.interval_precision = -1,
	},
	{
		.type_name = "interval year",
		.data_type = SQL_INTERVAL_YEAR,	/* 101 */
		.column_size = 9,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTERVAL,
		.sql_datetime_sub = SQL_CODE_YEAR,
		.num_prec_radix = -1,
		.interval_precision = 9,
	},
	{
		.type_name = "interval month",
		.data_type = SQL_INTERVAL_MONTH,	/* 102 */
		.column_size = 10,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTERVAL,
		.sql_datetime_sub = SQL_CODE_MONTH,
		.num_prec_radix = -1,
		.interval_precision = 10,
	},
	{
		.type_name = "interval day",
		.data_type = SQL_INTERVAL_DAY,	/* 103 */
		.column_size = 5,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTERVAL,
		.sql_datetime_sub = SQL_CODE_DAY,
		.num_prec_radix = -1,
		.interval_precision = 5,
	},
	{
		.type_name = "interval hour",
		.data_type = SQL_INTERVAL_HOUR,	/* 104 */
		.column_size = 6,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTERVAL,
		.sql_datetime_sub = SQL_CODE_HOUR,
		.num_prec_radix = -1,
		.interval_precision = 6,
	},
	{
		.type_name = "interval minute",
		.data_type = SQL_INTERVAL_MINUTE,	/* 105 */
		.column_size = 8,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTERVAL,
		.sql_datetime_sub = SQL_CODE_MINUTE,
		.num_prec_radix = -1,
		.interval_precision = 8,
	},
	{
		.type_name = "interval second",
		.data_type = SQL_INTERVAL_SECOND,	/* 106 */
		.column_size = 10,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "'precision'",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTERVAL,
		.sql_datetime_sub = SQL_CODE_SECOND,
		.num_prec_radix = -1,
		.interval_precision = 10,
	},
	{
		.type_name = "interval year to month",
		.data_type = SQL_INTERVAL_YEAR_TO_MONTH,	/* 107 */
		.column_size = 12,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTERVAL,
		.sql_datetime_sub = SQL_CODE_YEAR_TO_MONTH,
		.num_prec_radix = -1,
		.interval_precision = 9,
	},
	{
		.type_name = "interval day to hour",
		.data_type = SQL_INTERVAL_DAY_TO_HOUR,	/* 108 */
		.column_size = 8,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTERVAL,
		.sql_datetime_sub = SQL_CODE_DAY_TO_HOUR,
		.num_prec_radix = -1,
		.interval_precision = 5,
	},
	{
		.type_name = "interval day to minute",
		.data_type = SQL_INTERVAL_DAY_TO_MINUTE,	/* 109 */
		.column_size = 11,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTERVAL,
		.sql_datetime_sub = SQL_CODE_DAY_TO_MINUTE,
		.num_prec_radix = -1,
		.interval_precision = 5,
	},
	{
		.type_name = "interval day to second",
		.data_type = SQL_INTERVAL_DAY_TO_SECOND,	/* 110 */
		.column_size = 14,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "'precision'",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTERVAL,
		.sql_datetime_sub = SQL_CODE_DAY_TO_SECOND,
		.num_prec_radix = -1,
		.interval_precision = 5,
	},
	{
		.type_name = "interval hour to minute",
		.data_type = SQL_INTERVAL_HOUR_TO_MINUTE,	/* 111 */
		.column_size = 9,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTERVAL,
		.sql_datetime_sub = SQL_CODE_HOUR_TO_MINUTE,
		.num_prec_radix = -1,
		.interval_precision = 6,
	},
	{
		.type_name = "interval hour to second",
		.data_type = SQL_INTERVAL_HOUR_TO_SECOND,	/* 112 */
		.column_size = 12,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "'precision'",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTERVAL,
		.sql_datetime_sub = SQL_CODE_HOUR_TO_SECOND,
		.num_prec_radix = -1,
		.interval_precision = 6,
	},
	{
		.type_name = "interval minute to second",
		.data_type = SQL_INTERVAL_MINUTE_TO_SECOND,	/* 113 */
		.column_size = 13,
		.literal_prefix = "''''",
		.literal_suffix = "''''",
		.create_params = "'precision'",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = -1,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = -1,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_INTERVAL,
		.sql_datetime_sub = SQL_CODE_MINUTE_TO_SECOND,
		.num_prec_radix = -1,
		.interval_precision = 10,
	},
	{
		.type_name = "hugeint",
		.data_type = SQL_HUGEINT,	/* 0x4000 (defined in ODBCGlobal.h) */
		.column_size = 38,
		.literal_prefix = "NULL",
		.literal_suffix = "NULL",
		.create_params = "NULL",
		.nullable = SQL_NULLABLE,
		.case_sensitive = SQL_FALSE,
		.searchable = SQL_PRED_BASIC,
		.unsigned_attribute = SQL_FALSE,
		.fixed_prec_scale = SQL_FALSE,
		.auto_unique_value = SQL_FALSE,
		.local_type_name = "NULL",
		.minimum_scale = 0,
		.maximum_scale = 0,
		.sql_data_type = SQL_HUGEINT,
		.sql_datetime_sub = -1,
		.num_prec_radix = 10,
		.interval_precision = -1,
	},
};

/* find some info about a type given the concise type */
const char *
ODBCGetTypeInfo(int concise_type,
		int *data_type,
		int *sql_data_type,
		int *sql_datetime_sub)
{
	const struct types *t;

	for (t = types; t < &types[sizeof(types) / sizeof(types[0])]; t++) {
		if (t->data_type == concise_type) {
			if (data_type)
				*data_type = t->data_type;
			if (sql_data_type)
				*sql_data_type = t->sql_data_type;
			if (sql_datetime_sub)
				*sql_datetime_sub = t->sql_datetime_sub;
			return t->type_name;
		}
	}
	return NULL;
}

static SQLRETURN
MNDBGetTypeInfo(ODBCStmt *stmt,
		SQLSMALLINT DataType)
{
	const struct types *t;
	int i;
	char query[4096];

	switch (DataType) {
	case SQL_ALL_TYPES:
	case SQL_CHAR:
	case SQL_NUMERIC:
	case SQL_DECIMAL:
	case SQL_INTEGER:
	case SQL_SMALLINT:
	case SQL_FLOAT:
	case SQL_REAL:
	case SQL_DOUBLE:
	case SQL_DATE:
	case SQL_TIME:
	case SQL_TIMESTAMP:
	case SQL_VARCHAR:
	case SQL_TYPE_DATE:
	case SQL_TYPE_TIME:
	case SQL_TYPE_TIMESTAMP:
	case SQL_LONGVARCHAR:
	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
	case SQL_BIGINT:
	case SQL_TINYINT:
	case SQL_BIT:
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
	case SQL_GUID:
	case SQL_INTERVAL_YEAR:
	case SQL_INTERVAL_MONTH:
	case SQL_INTERVAL_DAY:
	case SQL_INTERVAL_HOUR:
	case SQL_INTERVAL_MINUTE:
	case SQL_INTERVAL_SECOND:
	case SQL_INTERVAL_YEAR_TO_MONTH:
	case SQL_INTERVAL_DAY_TO_HOUR:
	case SQL_INTERVAL_DAY_TO_MINUTE:
	case SQL_INTERVAL_DAY_TO_SECOND:
	case SQL_INTERVAL_HOUR_TO_MINUTE:
	case SQL_INTERVAL_HOUR_TO_SECOND:
	case SQL_INTERVAL_MINUTE_TO_SECOND:
		break;

	case SQL_HUGEINT:
		/* the application shows interest in HUGEINT, so now we
		 * enable it */
		stmt->Dbc->allow_hugeint = true;
		break;

	/* some pre ODBC 3.0 data types which can be mapped to ODBC
	 * 3.0 data types */
	case -80:		/* SQL_INTERVAL_YEAR */
		DataType = SQL_INTERVAL_YEAR;
		break;
	case -81:		/* SQL_INTERVAL_YEAR_TO_MONTH */
		DataType = SQL_INTERVAL_YEAR_TO_MONTH;
		break;
	case -82:		/* SQL_INTERVAL_MONTH */
		DataType = SQL_INTERVAL_MONTH;
		break;
	case -83:		/* SQL_INTERVAL_DAY */
		DataType = SQL_INTERVAL_DAY;
		break;
	case -84:		/* SQL_INTERVAL_HOUR */
		DataType = SQL_INTERVAL_HOUR;
		break;
	case -85:		/* SQL_INTERVAL_MINUTE */
		DataType = SQL_INTERVAL_MINUTE;
		break;
	case -86:		/* SQL_INTERVAL_SECOND */
		DataType = SQL_INTERVAL_SECOND;
		break;
	case -87:		/* SQL_INTERVAL_DAY_TO_HOUR */
		DataType = SQL_INTERVAL_DAY_TO_HOUR;
		break;
	case -88:		/* SQL_INTERVAL_DAY_TO_MINUTE */
		DataType = SQL_INTERVAL_DAY_TO_MINUTE;
		break;
	case -89:		/* SQL_INTERVAL_DAY_TO_SECOND */
		DataType = SQL_INTERVAL_DAY_TO_SECOND;
		break;
	case -90:		/* SQL_INTERVAL_HOUR_TO_MINUTE */
		DataType = SQL_INTERVAL_HOUR_TO_MINUTE;
		break;
	case -91:		/* SQL_INTERVAL_HOUR_TO_SECOND */
		DataType = SQL_INTERVAL_HOUR_TO_SECOND;
		break;
	case -92:		/* SQL_INTERVAL_MINUTE_TO_SECOND */
		DataType = SQL_INTERVAL_MINUTE_TO_SECOND;
		break;

	case -95:		/* SQL_UNICODE_CHAR and SQL_UNICODE */
		DataType = SQL_WCHAR;
		break;
	case -96:		/* SQL_UNICODE_VARCHAR */
		DataType = SQL_WVARCHAR;
		break;
	case -97:		/* SQL_UNICODE_LONGVARCHAR */
		DataType = SQL_WLONGVARCHAR;
		break;
	default:
		/* Invalid SQL data type */
		addStmtError(stmt, "HY004", NULL, 0);
		return SQL_ERROR;
	}

	i = snprintf(query, sizeof(query), "select * from (values ");

	bool first = true;
	for (t = types; t < &types[sizeof(types) / sizeof(types[0])]; t++) {
		assert(t == types || t->data_type >= (t-1)->data_type);
		if (DataType != SQL_ALL_TYPES && DataType != t->data_type)
			continue;
		if (DataType == SQL_ALL_TYPES &&
		    t->data_type == SQL_HUGEINT &&
		    !stmt->Dbc->allow_hugeint)
			continue;
		if (first) {
			/* specify column types in first set of values */
			i += snprintf(query + i, sizeof(query) - i,
				      "(cast('%s' as varchar(128))"
				      ",cast(%d as smallint)"
				      ",cast(%d as integer)"
				      ",cast(%s as varchar(128))"
				      ",cast(%s as varchar(128))"
				      ",cast(%s as varchar(128))"
				      ",cast(%d as smallint)"
				      ",cast(%d as smallint)"
				      ",cast(%d as smallint)"
				      ",cast(%d as smallint)"
				      ",cast(%d as smallint)"
				      ",cast(%d as smallint)"
				      ",cast(%s as varchar(128))"
				      ",cast(%d as smallint)"
				      ",cast(%d as smallint)"
				      ",cast(%d as smallint)"
				      ",cast(%d as smallint)"
				      ",cast(%d as integer)"
				      ",cast(%d as smallint))",
				      t->type_name,
				      t->data_type,
				      t->column_size,
				      t->literal_prefix,
				      t->literal_suffix,
				      t->create_params,
				      t->nullable,
				      t->case_sensitive,
				      t->searchable,
				      t->unsigned_attribute,
				      t->fixed_prec_scale,
				      t->auto_unique_value,
				      t->local_type_name,
				      t->minimum_scale,
				      t->maximum_scale,
				      t->sql_data_type,
				      t->sql_datetime_sub,
				      t->num_prec_radix,
				      t->interval_precision);
			first = false;
		} else {
			i += snprintf(query + i, sizeof(query) - i,
				      ",('%s',%d,%d,%s,%s,%s,%d,%d,%d"
				      ",%d,%d,%d,%s,%d,%d,%d,%d,%d,%d)",
				      t->type_name,
				      t->data_type,
				      t->column_size,
				      t->literal_prefix,
				      t->literal_suffix,
				      t->create_params,
				      t->nullable,
				      t->case_sensitive,
				      t->searchable,
				      t->unsigned_attribute,
				      t->fixed_prec_scale,
				      t->auto_unique_value,
				      t->local_type_name,
				      t->minimum_scale,
				      t->maximum_scale,
				      t->sql_data_type,
				      t->sql_datetime_sub,
				      t->num_prec_radix,
				      t->interval_precision);
		}
	}
	i += snprintf(query+ i, sizeof(query) - i, ") as monetdb_types "
		      "(type_name"
		      ",data_type"
		      ",column_size"
		      ",literal_prefix"
		      ",literal_suffix"
		      ",create_params"
		      ",nullable"
		      ",case_sensitive"
		      ",searchable"
		      ",unsigned_attribute"
		      ",fixed_prec_scale"
		      ",auto_unique_value"
		      ",local_type_name"
		      ",minimum_scale"
		      ",maximum_scale"
		      ",sql_data_type"
		      ",sql_datetime_sub"
		      ",num_prec_radix"
		      ",interval_precision)");
	assert(i < (int) sizeof(query));

	return MNDBExecDirect(stmt, (SQLCHAR *) query,
			      (SQLINTEGER) i);
}

#ifdef ODBCDEBUG
static char *
translateDataType(SQLSMALLINT DataType)
{
	switch (DataType) {
	case SQL_ALL_TYPES:
		return "SQL_ALL_TYPES";
	case SQL_CHAR:
		return "SQL_CHAR";
	case SQL_NUMERIC:
		return "SQL_NUMERIC";
	case SQL_DECIMAL:
		return "SQL_DECIMAL";
	case SQL_INTEGER:
		return "SQL_INTEGER";
	case SQL_SMALLINT:
		return "SQL_SMALLINT";
	case SQL_FLOAT:
		return "SQL_FLOAT";
	case SQL_REAL:
		return "SQL_REAL";
	case SQL_DOUBLE:
		return "SQL_DOUBLE";
	case SQL_DATE:
		return "SQL_DATE";
	case SQL_TIME:
		return "SQL_TIME";
	case SQL_TIMESTAMP:
		return "SQL_TIMESTAMP";
	case SQL_VARCHAR:
		return "SQL_VARCHAR";
	case SQL_TYPE_DATE:
		return "SQL_TYPE_DATE";
	case SQL_TYPE_TIME:
		return "SQL_TYPE_TIME";
	case SQL_TYPE_TIMESTAMP:
		return "SQL_TYPE_TIMESTAMP";
	case SQL_LONGVARCHAR:
		return "SQL_LONGVARCHAR";
	case SQL_BINARY:
		return "SQL_BINARY";
	case SQL_VARBINARY:
		return "SQL_VARBINARY";
	case SQL_LONGVARBINARY:
		return "SQL_LONGVARBINARY";
	case SQL_BIGINT:
		return "SQL_BIGINT";
	case SQL_HUGEINT:
		return "SQL_HUGEINT";
	case SQL_TINYINT:
		return "SQL_TINYINT";
	case SQL_BIT:
		return "SQL_BIT";
	case SQL_WCHAR:
		return "SQL_WCHAR";
	case SQL_WVARCHAR:
		return "SQL_WVARCHAR";
	case SQL_WLONGVARCHAR:
		return "SQL_WLONGVARCHAR";
	case SQL_GUID:
		return "SQL_GUID";
	case SQL_INTERVAL_YEAR:
		return "SQL_INTERVAL_YEAR";
	case SQL_INTERVAL_MONTH:
		return "SQL_INTERVAL_MONTH";
	case SQL_INTERVAL_DAY:
		return "SQL_INTERVAL_DAY";
	case SQL_INTERVAL_HOUR:
		return "SQL_INTERVAL_HOUR";
	case SQL_INTERVAL_MINUTE:
		return "SQL_INTERVAL_MINUTE";
	case SQL_INTERVAL_SECOND:
		return "SQL_INTERVAL_SECOND";
	case SQL_INTERVAL_YEAR_TO_MONTH:
		return "SQL_INTERVAL_YEAR_TO_MONTH";
	case SQL_INTERVAL_DAY_TO_HOUR:
		return "SQL_INTERVAL_DAY_TO_HOUR";
	case SQL_INTERVAL_DAY_TO_MINUTE:
		return "SQL_INTERVAL_DAY_TO_MINUTE";
	case SQL_INTERVAL_DAY_TO_SECOND:
		return "SQL_INTERVAL_DAY_TO_SECOND";
	case SQL_INTERVAL_HOUR_TO_MINUTE:
		return "SQL_INTERVAL_HOUR_TO_MINUTE";
	case SQL_INTERVAL_HOUR_TO_SECOND:
		return "SQL_INTERVAL_HOUR_TO_SECOND";
	case SQL_INTERVAL_MINUTE_TO_SECOND:
		return "SQL_INTERVAL_MINUTE_TO_SECOND";
	case -80:
		return "SQL_INTERVAL_YEAR(ODBC2)";
	case -81:
		return "SQL_INTERVAL_YEAR_TO_MONTH(ODBC2)";
	case -82:
		return "SQL_INTERVAL_MONTH(ODBC2)";
	case -83:
		return "SQL_INTERVAL_DAY(ODBC2)";
	case -84:
		return "SQL_INTERVAL_HOUR(ODBC2)";
	case -85:
		return "SQL_INTERVAL_MINUTE(ODBC2)";
	case -86:
		return "SQL_INTERVAL_SECOND(ODBC2)";
	case -87:
		return "SQL_INTERVAL_DAY_TO_HOUR(ODBC2)";
	case -88:
		return "SQL_INTERVAL_DAY_TO_MINUTE(ODBC2)";
	case -89:
		return "SQL_INTERVAL_DAY_TO_SECOND(ODBC2)";
	case -90:
		return "SQL_INTERVAL_HOUR_TO_MINUTE(ODBC2)";
	case -91:
		return "SQL_INTERVAL_HOUR_TO_SECOND(ODBC2)";
	case -92:
		return "SQL_INTERVAL_MINUTE_TO_SECOND(ODBC2)";
	case -95:
		return "SQL_UNICODE_CHAR and SQL_UNICODE(ODBC2)";
	case -96:
		return "SQL_UNICODE_VARCHAR(ODBC2)";
	case -97:
		return "SQL_UNICODE_LONGVARCHAR(ODBC2)";
	default:
		return "unknown";
	}
}
#endif

SQLRETURN SQL_API
SQLGetTypeInfo(SQLHSTMT StatementHandle,
	       SQLSMALLINT DataType)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetTypeInfo %p %s\n",
		StatementHandle, translateDataType(DataType));
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return MNDBGetTypeInfo(stmt, DataType);
}

SQLRETURN SQL_API
SQLGetTypeInfoA(SQLHSTMT StatementHandle,
		SQLSMALLINT DataType)
{
	return SQLGetTypeInfo(StatementHandle, DataType);
}

SQLRETURN SQL_API
SQLGetTypeInfoW(SQLHSTMT StatementHandle,
		SQLSMALLINT DataType)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetTypeInfoW %p %s\n",
		StatementHandle, translateDataType(DataType));
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return MNDBGetTypeInfo(stmt, DataType);
}
