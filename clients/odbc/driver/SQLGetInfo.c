/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
 * SQLGetInfo()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"
#include "ODBCUtil.h"


static SQLRETURN
SQLGetInfo_(ODBCDbc *dbc,
	    SQLUSMALLINT nInfoType,
	    SQLPOINTER pInfoValue,
	    SQLSMALLINT nInfoValueMax,
	    SQLSMALLINT *pnLength)
{
	int nValue = 0;
	const char *sValue = NULL;	/* iff non-NULL, return string value */
	int len = 0;

	/* For some info types an active connection is needed */
	if (!dbc->Connected &&
	    (nInfoType == SQL_DATA_SOURCE_NAME ||
	     nInfoType == SQL_SERVER_NAME ||
	     nInfoType == SQL_DATABASE_NAME ||
	     nInfoType == SQL_USER_NAME)) {
		/* Connection does not exist */
		addDbcError(dbc, "08003", NULL, 0);
		return SQL_ERROR;
	}

	switch (nInfoType) {
	case SQL_ACCESSIBLE_PROCEDURES:
		sValue = "Y";
		break;
	case SQL_ACCESSIBLE_TABLES:
		sValue = "N";
		break;
	case SQL_ACTIVE_ENVIRONMENTS:
		nValue = 0;	/* 0 = no limit */
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_AGGREGATE_FUNCTIONS:
		nValue = SQL_AF_ALL |
			SQL_AF_AVG |
			SQL_AF_COUNT |
			SQL_AF_DISTINCT |
			SQL_AF_MAX |
			SQL_AF_MIN |
			SQL_AF_SUM;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_ALTER_DOMAIN:
		nValue = 0;
		/* SQL_AD_ADD_CONSTRAINT_DEFERRABLE |
		   SQL_AD_ADD_CONSTRAINT_INITIALLY_DEFERRED |
		   SQL_AD_ADD_CONSTRAINT_INITIALLY_IMMEDIATE |
		   SQL_AD_ADD_CONSTRAINT_NON_DEFERRABLE |
		   SQL_AD_ADD_DOMAIN_CONSTRAINT |
		   SQL_AD_ADD_DOMAIN_DEFAULT |
		   SQL_AD_CONSTRAINT_NAME_DEFINITION |
		   SQL_AD_DROP_DOMAIN_CONSTRAINT |
		   SQL_AD_DROP_DOMAIN_DEFAULT */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_ALTER_TABLE:
		nValue = SQL_AT_ADD_COLUMN_DEFAULT |
			SQL_AT_ADD_COLUMN_SINGLE |
			SQL_AT_ADD_CONSTRAINT |
			SQL_AT_ADD_TABLE_CONSTRAINT |
			SQL_AT_CONSTRAINT_NAME_DEFINITION |
			SQL_AT_DROP_COLUMN_RESTRICT |
			SQL_AT_DROP_TABLE_CONSTRAINT_RESTRICT |
			SQL_AT_SET_COLUMN_DEFAULT;
		/* SQL_AT_ADD_COLUMN_SINGLE |
		   SQL_AT_ADD_COLUMN_COLLATION |
		   SQL_AT_ADD_COLUMN_DEFAULT |
		   SQL_AT_ADD_TABLE_CONSTRAINT |
		   SQL_AT_ADD_TABLE_CONSTRAINT |
		   SQL_AT_CONSTRAINT_DEFERRABLE |
		   SQL_AT_CONSTRAINT_INITIALLY_DEFERRED |
		   SQL_AT_CONSTRAINT_INITIALLY_IMMEDIATE |
		   SQL_AT_CONSTRAINT_NAME_DEFINITION |
		   SQL_AT_DROP_COLUMN_CASCADE |
		   SQL_AT_DROP_COLUMN_DEFAULT |
		   SQL_AT_DROP_COLUMN_RESTRICT |
		   SQL_AT_DROP_TABLE_CONSTRAINT_CASCADE |
		   SQL_AT_DROP_TABLE_CONSTRAINT_RESTRICT |
		   SQL_AT_SET_COLUMN_DEFAULT |
		   SQL_AT_CONSTRAINT_NON_DEFERRABLE */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_ASYNC_MODE:
		nValue = SQL_AM_NONE;
		/* SQL_AM_CONNECTION, SQL_AM_STATEMENT */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_BATCH_ROW_COUNT:
		nValue = SQL_BRC_EXPLICIT;
		/* SQL_BRC_PROCEDURES | SQL_BRC_ROLLED_UP */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_BATCH_SUPPORT:
		nValue = SQL_BS_ROW_COUNT_EXPLICIT | SQL_BS_SELECT_EXPLICIT;
		/* SQL_BS_ROW_COUNT_PROC |
		   SQL_BS_SELECT_PROC */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_BOOKMARK_PERSISTENCE:
		nValue = 0;	/* bookmarks not supported */
		/* SQL_BP_CLOSE |
		   SQL_BP_DELETE |
		   SQL_BP_DROP |
		   SQL_BP_OTHER_HSTMT |
		   SQL_BP_TRANSACTION |
		   SQL_BP_UPDATE */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_CATALOG_LOCATION:
		nValue = 0;	/* catalogs not supported */
		/* SQL_CL_END, SQL_CL_START */
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_CATALOG_NAME:
		sValue = "N";
		break;
	case SQL_CATALOG_NAME_SEPARATOR:
	case SQL_CATALOG_TERM:
		sValue = "";
		break;
	case SQL_CATALOG_USAGE:
		nValue = 0;
		/* SQL_CU_DML_STATEMENTS |
		   SQL_CU_INDEX_DEFINITION |
		   SQL_CU_PRIVILEGE_DEFINITION |
		   SQL_CU_PROCEDURE_INVOCATION |
		   SQL_CU_TABLE_DEFINITION */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_COLLATION_SEQ:
		sValue = "UTF-8";
		break;
	case SQL_COLUMN_ALIAS:
		sValue = "Y";
		break;
	case SQL_CONCAT_NULL_BEHAVIOR:
		nValue = SQL_CB_NULL;
		/* SQL_CB_NON_NULL */
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_CONVERT_BIGINT:
	case SQL_CONVERT_BINARY:
	case SQL_CONVERT_BIT:
	case SQL_CONVERT_CHAR:
	case SQL_CONVERT_DATE:
	case SQL_CONVERT_DECIMAL:
	case SQL_CONVERT_DOUBLE:
	case SQL_CONVERT_FLOAT:
	case SQL_CONVERT_INTEGER:
	case SQL_CONVERT_INTERVAL_DAY_TIME:
	case SQL_CONVERT_INTERVAL_YEAR_MONTH:
	case SQL_CONVERT_LONGVARBINARY:
	case SQL_CONVERT_LONGVARCHAR:
	case SQL_CONVERT_NUMERIC:
	case SQL_CONVERT_REAL:
	case SQL_CONVERT_SMALLINT:
	case SQL_CONVERT_TIME:
	case SQL_CONVERT_TIMESTAMP:
	case SQL_CONVERT_TINYINT:
	case SQL_CONVERT_VARBINARY:
	case SQL_CONVERT_VARCHAR:
		nValue = SQL_CVT_BIGINT |
			SQL_CVT_BINARY |
			SQL_CVT_BIT |
			SQL_CVT_CHAR |
			SQL_CVT_DATE |
			SQL_CVT_DECIMAL |
			SQL_CVT_DOUBLE |
			SQL_CVT_FLOAT |
			SQL_CVT_INTEGER |
			SQL_CVT_INTERVAL_DAY_TIME |
			SQL_CVT_INTERVAL_YEAR_MONTH |
			SQL_CVT_LONGVARBINARY |
			SQL_CVT_LONGVARCHAR |
			SQL_CVT_NUMERIC |
			SQL_CVT_REAL |
			SQL_CVT_SMALLINT |
			SQL_CVT_TIME |
			SQL_CVT_TIMESTAMP |
			SQL_CVT_TINYINT |
			SQL_CVT_VARBINARY |
			SQL_CVT_VARCHAR;
		/* SQL_CVT_GUID */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_CONVERT_FUNCTIONS:
		/* No convert function supported */
		nValue = SQL_FN_CVT_CAST | SQL_FN_CVT_CONVERT;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_CORRELATION_NAME:
		nValue = SQL_CN_ANY;
		/* SQL_CN_DIFFERENT, SQL_CN_NONE */
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_CREATE_ASSERTION:
		/* SQL_CA_CREATE_ASSERTION |
		   SQL_CA_CONSTRAINT_DEFERRABLE |
		   SQL_CA_CONSTRAINT_INITIALLY_DEFERRED |
		   SQL_CA_CONSTRAINT_INITIALLY_IMMEDIATE |
		   SQL_CA_CONSTRAINT_NON_DEFERRABLE */
	case SQL_CREATE_CHARACTER_SET:
		/* SQL_CCS_CREATE_CHARACTER_SET |
		   SQL_CCS_COLLATE_CLAUSE |
		   SQL_CCS_LIMITED_COLLATION */
	case SQL_CREATE_COLLATION:
		/* SQL_CCOL_CREATE_COLLATION */
	case SQL_CREATE_DOMAIN:
		/* SQL_CDO_CREATE_DOMAIN |
		   SQL_CDO_CONSTRAINT_NAME_DEFINITION |
		   SQL_CDO_DEFAULT |
		   SQL_CDO_CONSTRAINT |
		   SQL_CDO_COLLATION |
		   SQL_CDO_CONSTRAINT_DEFERRABLE |
		   SQL_CDO_CONSTRAINT_INITIALLY_DEFERRED |
		   SQL_CDO_CONSTRAINT_INITIALLY_IMMEDIATE |
		   SQL_CDO_CONSTRAINT_NON_DEFERRABLE */
	case SQL_CREATE_TRANSLATION:
		/* SQL_CTR_CREATE_TRANSLATION */
		nValue = 0;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_CREATE_SCHEMA:
		nValue = SQL_CS_CREATE_SCHEMA | SQL_CS_AUTHORIZATION;
		/* SQL_CS_DEFAULT_CHARACTER_SET */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_CREATE_TABLE:
		nValue = SQL_CT_COLUMN_CONSTRAINT |
			SQL_CT_COLUMN_DEFAULT |
			SQL_CT_COMMIT_PRESERVE |
			SQL_CT_CONSTRAINT_NAME_DEFINITION |
			SQL_CT_CREATE_TABLE |
			SQL_CT_GLOBAL_TEMPORARY |
			SQL_CT_LOCAL_TEMPORARY |
			SQL_CT_TABLE_CONSTRAINT;
		/* SQL_CT_COLUMN_COLLATION |
		   SQL_CT_COMMIT_DELETE |
		   SQL_CT_CONSTRAINT_DEFERRABLE |
		   SQL_CT_CONSTRAINT_INITIALLY_DEFERRED |
		   SQL_CT_CONSTRAINT_INITIALLY_IMMEDIATE |
		   SQL_CT_CONSTRAINT_NON_DEFERRABLE */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_CREATE_VIEW:
		nValue = SQL_CV_CREATE_VIEW | SQL_CV_CHECK_OPTION;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_CURSOR_COMMIT_BEHAVIOR:
	case SQL_CURSOR_ROLLBACK_BEHAVIOR:
		nValue = SQL_CB_DELETE;
		/* SQL_CB_CLOSE, SQL_CB_PRESERVE */
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_CURSOR_SENSITIVITY:
		nValue = SQL_INSENSITIVE;
		/* SQL_SENSITIVE, SQL_UNSPECIFIED */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_DATA_SOURCE_NAME:
		sValue = dbc->dsn ? dbc->dsn : "";
		break;
	case SQL_DATA_SOURCE_READ_ONLY:
		sValue = "N";
		break;
	case SQL_DATABASE_NAME:
		sValue = dbc->dbname ? dbc->dbname : "";
		break;
	case SQL_ODBC_INTERFACE_CONFORMANCE:
		nValue = SQL_OIC_CORE;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_SCROLL_OPTIONS:
		nValue = SQL_SO_STATIC;
		/* SQL_SO_DYNAMIC,
		   SQL_SO_FORWARD_ONLY,
		   SQL_SO_KEYSET_DRIVEN,
		   SQL_SO_MIXED */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_DYNAMIC_CURSOR_ATTRIBUTES1:
	case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1:
	case SQL_KEYSET_CURSOR_ATTRIBUTES1:
	case SQL_DYNAMIC_CURSOR_ATTRIBUTES2:
	case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2:
	case SQL_KEYSET_CURSOR_ATTRIBUTES2:
		nValue = 0;
		len = sizeof(SQLUINTEGER);
		break;
		/* the above values have been checked */

	case SQL_STATIC_CURSOR_ATTRIBUTES1:
		/* SQL_CA1_BOOKMARK |
		   SQL_CA1_BULK_ADD |
		   SQL_CA1_BULK_DELETE_BY_BOOKMARK |
		   SQL_CA1_BULK_FETCH_BY_BOOKMARK |
		   SQL_CA1_BULK_UPDATE_BY_BOOKMARK |
		   SQL_CA1_LOCK_EXCLUSIVE |
		   SQL_CA1_LOCK_UNLOCK |
		   SQL_CA1_POS_DELETE |
		   SQL_CA1_POSITIONED_DELETE |
		   SQL_CA1_POSITIONED_UPDATE |
		   SQL_CA1_POS_REFRESH |
		   SQL_CA1_POS_UPDATE |
		   SQL_CA1_SELECT_FOR_UPDATE */
		nValue = SQL_CA1_ABSOLUTE | SQL_CA1_LOCK_NO_CHANGE | SQL_CA1_NEXT | SQL_CA1_POS_POSITION | SQL_CA1_RELATIVE;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_STATIC_CURSOR_ATTRIBUTES2:
		/* SQL_CA2_CRC_APPROXIMATE |
		   SQL_CA2_LOCK_CONCURRENCY |
		   SQL_CA2_MAX_ROWS_AFFECTS_ALL |
		   SQL_CA2_MAX_ROWS_CATALOG |
		   SQL_CA2_MAX_ROWS_DELETE |
		   SQL_CA2_MAX_ROWS_INSERT |
		   SQL_CA2_MAX_ROWS_SELECT |
		   SQL_CA2_MAX_ROWS_UPDATE |
		   SQL_CA2_OPT_ROWVER_CONCURRENCY |
		   SQL_CA2_OPT_VALUES_CONCURRENCY |
		   SQL_CA2_READ_ONLY_CONCURRENCY |
		   SQL_CA2_SENSITIVITY_ADDITIONS |
		   SQL_CA2_SENSITIVITY_DELETIONS |
		   SQL_CA2_SENSITIVITY_UPDATES |
		   SQL_CA2_SIMULATE_NON_UNIQUE |
		   SQL_CA2_SIMULATE_TRY_UNIQUE |
		   SQL_CA2_SIMULATE_UNIQUE */
		nValue = SQL_CA2_CRC_EXACT;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_SQL_CONFORMANCE:
		nValue = SQL_SC_SQL92_FULL;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_MAX_DRIVER_CONNECTIONS:
		nValue = 0;	/* 0 = No specified limit */
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_MAX_CONCURRENT_ACTIVITIES:
		nValue = 0;	/* 0 = No specified limit */
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_DRIVER_NAME:
		sValue = MONETDB_DRIVER_NAME;
		break;
	case SQL_DRIVER_VER:
		sValue = MONETDB_DRIVER_VER;
		break;
	case SQL_FETCH_DIRECTION:
		nValue = SQL_FD_FETCH_NEXT;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_ODBC_API_CONFORMANCE:
		nValue = SQL_OIC_CORE;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_ODBC_VER:
	case SQL_DRIVER_ODBC_VER:
		sValue = MONETDB_ODBC_VER;
		break;
	case SQL_ROW_UPDATES:
		sValue = "N";
		break;
	case SQL_ODBC_SAG_CLI_CONFORMANCE:
		nValue = SQL_OSCC_COMPLIANT;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_SERVER_NAME:
		sValue = MONETDB_SERVER_NAME;
		break;
	case SQL_SEARCH_PATTERN_ESCAPE:
		sValue = "";	/* No search-char. escape char. */
		break;
	case SQL_ODBC_SQL_CONFORMANCE:
		nValue = SQL_OSC_CORE;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_DBMS_NAME:
		sValue = MONETDB_PRODUCT_NAME;
		break;
	case SQL_DBMS_VER:
		sValue = MONETDB_DRIVER_VER;
		break;
	case SQL_PROCEDURES:
		sValue = "N";
		break;
	case SQL_DEFAULT_TXN_ISOLATION:
		nValue = SQL_TXN_READ_COMMITTED;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_EXPRESSIONS_IN_ORDERBY:
		sValue = "Y";
		break;
	case SQL_IDENTIFIER_CASE:
		nValue = SQL_IC_LOWER;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_IDENTIFIER_QUOTE_CHAR:
		sValue = "\"";	/* the " (double quote) */
		break;
	case SQL_MAX_COLUMN_NAME_LEN:
	case SQL_MAX_TABLE_NAME_LEN:
	case SQL_MAX_SCHEMA_NAME_LEN:
	case SQL_MAX_CATALOG_NAME_LEN:
		/* in monet strings can be very long, but limit it
		   here to 255 which should be enough in most cases */
		nValue = 255;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_MAX_CURSOR_NAME_LEN:
		nValue = 0;	/* currently SQLSetCursorName is not supported */
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_MAX_PROCEDURE_NAME_LEN:
		/* No support for stored procedures: return 0 as value */
		nValue = 0;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_MULT_RESULT_SETS:
		sValue = "N";
		break;
	case SQL_MULTIPLE_ACTIVE_TXN:
		sValue = "Y";
		break;
	case SQL_OUTER_JOINS:
		sValue = "Y";
		break;
	case SQL_PROCEDURE_TERM:
		sValue = "procedure";
		break;
	case SQL_SCHEMA_TERM:
		sValue = "schema";
		break;
	case SQL_TABLE_TERM:
		sValue = "table";
		break;
	case SQL_SCROLL_CONCURRENCY:
		nValue = SQL_SCCO_READ_ONLY;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_TXN_CAPABLE:
		nValue = SQL_TC_ALL;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_USER_NAME:
		sValue = dbc->uid ? dbc->uid : "";
		break;
	case SQL_NUMERIC_FUNCTIONS:
		nValue = SQL_FN_NUM_ABS |
			SQL_FN_NUM_ACOS |
			SQL_FN_NUM_ASIN |
			SQL_FN_NUM_ATAN |
			SQL_FN_NUM_ATAN2 |
			SQL_FN_NUM_CEILING |
			SQL_FN_NUM_COS |
			SQL_FN_NUM_COT |
			SQL_FN_NUM_DEGREES |
			SQL_FN_NUM_EXP |
			SQL_FN_NUM_FLOOR |
			SQL_FN_NUM_LOG |
			SQL_FN_NUM_LOG10 |
			SQL_FN_NUM_MOD |
			SQL_FN_NUM_PI |
			SQL_FN_NUM_POWER |
			SQL_FN_NUM_RADIANS |
			SQL_FN_NUM_RAND |
			SQL_FN_NUM_ROUND |
			SQL_FN_NUM_SIGN |
			SQL_FN_NUM_SIN |
			SQL_FN_NUM_SQRT |
			SQL_FN_NUM_TRUNCATE |
			SQL_FN_NUM_TAN;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_STRING_FUNCTIONS:
		nValue = SQL_FN_STR_ASCII |
			SQL_FN_STR_CHAR |
			SQL_FN_STR_CONCAT |
			SQL_FN_STR_DIFFERENCE |
			SQL_FN_STR_INSERT |
			SQL_FN_STR_LCASE |
			SQL_FN_STR_LEFT |
			SQL_FN_STR_LENGTH |
			SQL_FN_STR_LOCATE |
			SQL_FN_STR_LTRIM |
			SQL_FN_STR_REPEAT |
			SQL_FN_STR_REPLACE |
			SQL_FN_STR_RIGHT |
			SQL_FN_STR_RTRIM |
			SQL_FN_STR_SOUNDEX |
			SQL_FN_STR_SPACE | 
			SQL_FN_STR_UCASE;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_SYSTEM_FUNCTIONS:
		nValue = SQL_FN_SYS_DBNAME | SQL_FN_SYS_USERNAME | SQL_FN_SYS_IFNULL;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_TIMEDATE_FUNCTIONS:
		nValue = SQL_FN_TD_CURDATE |
			SQL_FN_TD_CURTIME |
			SQL_FN_TD_DAYNAME |
			SQL_FN_TD_DAYOFMONTH |
			SQL_FN_TD_DAYOFWEEK |
			SQL_FN_TD_DAYOFYEAR |
			SQL_FN_TD_HOUR |
			SQL_FN_TD_MINUTE |
			SQL_FN_TD_MONTH |
			SQL_FN_TD_MONTHNAME |
			SQL_FN_TD_NOW |
			SQL_FN_TD_QUARTER |
			SQL_FN_TD_SECOND |
			SQL_FN_TD_TIMESTAMPADD |
			SQL_FN_TD_TIMESTAMPDIFF |
			SQL_FN_TD_WEEK |
			SQL_FN_TD_YEAR;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_TXN_ISOLATION_OPTION:
		nValue = SQL_TXN_REPEATABLE_READ;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_INTEGRITY:
		sValue = "N";
		break;
	case SQL_NON_NULLABLE_COLUMNS:
		nValue = SQL_NNC_NON_NULL;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_POSITIONED_STATEMENTS:
		nValue = SQL_PS_SELECT_FOR_UPDATE;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_GETDATA_EXTENSIONS:
		nValue = SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER | SQL_GD_BLOCK | SQL_GD_BOUND;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_FILE_USAGE:
		nValue = SQL_FILE_NOT_SUPPORTED;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_NULL_COLLATION:
		nValue = SQL_NC_LOW;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_GROUP_BY:
		nValue = SQL_GB_NO_RELATION;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_KEYWORDS:
		/* Returns the MonetDB keywords which are not listed
		 * as ODBC keyword in the #define SQL_ODBC_KEYWORDS
		 * in sql.h
		 */
		sValue = "BOOLEAN,COLUMNS,FLOOR,IMPORT,REAL";
		break;
	case SQL_ORDER_BY_COLUMNS_IN_SELECT:
		sValue = "N";
		break;
	case SQL_QUOTED_IDENTIFIER_CASE:
		nValue = SQL_IC_SENSITIVE;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_SPECIAL_CHARACTERS:
		sValue = "`!#$;:'<>";	/* allowed table name chars */
		break;
	case SQL_SUBQUERIES:
		nValue = SQL_SQ_COMPARISON | SQL_SQ_EXISTS | SQL_SQ_IN | SQL_SQ_CORRELATED_SUBQUERIES;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_UNION:
		nValue = SQL_U_UNION;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_MAX_COLUMNS_IN_GROUP_BY:
	case SQL_MAX_COLUMNS_IN_INDEX:
	case SQL_MAX_COLUMNS_IN_ORDER_BY:
	case SQL_MAX_COLUMNS_IN_SELECT:
	case SQL_MAX_COLUMNS_IN_TABLE:
	case SQL_MAX_TABLES_IN_SELECT:
		/* no specified limit for SQL_MAX_COLUMNS_IN_xxx */
		nValue = 0;	/* no limits */
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_MAX_INDEX_SIZE:
	case SQL_MAX_ROW_SIZE:
	case SQL_MAX_STATEMENT_LEN:
		nValue = 0;	/* no max.len. */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
		sValue = "N";
		break;
	case SQL_MAX_USER_NAME_LEN:
		nValue = 0;	/* no max.len. */
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_MAX_CHAR_LITERAL_LEN:
		nValue = (SQLUINTEGER) 1024 *1024;	/* 1MB */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_NEED_LONG_DATA_LEN:
		sValue = "Y";
		break;
	case SQL_MAX_BINARY_LITERAL_LEN:
		nValue = 0;	/* not supported yet */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_LIKE_ESCAPE_CLAUSE:
		sValue = "Y";
		break;
	case SQL_STATIC_SENSITIVITY:
		nValue = 0;
		/* SQL_SS_ADDITIONS | SQL_SS_DELETIONS | SQL_SS_UPDATES */
		len = sizeof(SQLINTEGER);
		break;
	case SQL_DATETIME_LITERALS:
		nValue = SQL_DL_SQL92_DATE |
			SQL_DL_SQL92_TIME |
			SQL_DL_SQL92_TIMESTAMP |
			SQL_DL_SQL92_INTERVAL_YEAR |
			SQL_DL_SQL92_INTERVAL_MONTH |
			SQL_DL_SQL92_INTERVAL_DAY |
			SQL_DL_SQL92_INTERVAL_HOUR |
			SQL_DL_SQL92_INTERVAL_MINUTE |
			SQL_DL_SQL92_INTERVAL_SECOND |
			SQL_DL_SQL92_INTERVAL_YEAR_TO_MONTH |
			SQL_DL_SQL92_INTERVAL_DAY_TO_HOUR |
			SQL_DL_SQL92_INTERVAL_DAY_TO_MINUTE |
			SQL_DL_SQL92_INTERVAL_DAY_TO_SECOND |
			SQL_DL_SQL92_INTERVAL_HOUR_TO_MINUTE |
			SQL_DL_SQL92_INTERVAL_HOUR_TO_SECOND |
			SQL_DL_SQL92_INTERVAL_MINUTE_TO_SECOND;
		len = sizeof(SQLINTEGER);
		break;

	/* return default values */
	case SQL_DDL_INDEX:
	case SQL_DROP_ASSERTION:
	case SQL_DROP_CHARACTER_SET:
	case SQL_DROP_COLLATION:
	case SQL_DROP_DOMAIN:
	case SQL_DROP_SCHEMA:
	case SQL_DROP_TABLE:
	case SQL_DROP_TRANSLATION:
	case SQL_DROP_VIEW:
	case SQL_INFO_SCHEMA_VIEWS:
	case SQL_INSERT_STATEMENT:
	case SQL_LOCK_TYPES:
	case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS:
	case SQL_OJ_CAPABILITIES:
	case SQL_PARAM_ARRAY_ROW_COUNTS:
	case SQL_PARAM_ARRAY_SELECTS:
	case SQL_POS_OPERATIONS:
	case SQL_SCHEMA_USAGE:
	case SQL_TIMEDATE_ADD_INTERVALS:
	case SQL_TIMEDATE_DIFF_INTERVALS:
		nValue = 0;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_DRIVER_HDESC:
		nValue = 0;
		len = sizeof(SQLULEN);
		break;
	case SQL_DESCRIBE_PARAMETER:
		sValue = "N";
		break;
	case SQL_DM_VER:
	case SQL_XOPEN_CLI_YEAR:
		sValue = "";
		break;
	case SQL_MAX_IDENTIFIER_LEN:
		nValue = 0;
		len = sizeof(SQLUSMALLINT);
		break;

	default:
		/* Invalid information type */
		addDbcError(dbc, "HY096", NULL, 0);
		return SQL_ERROR;
	}

	/* copy the data to the supplied output parameters */
	if (sValue) {
		copyString(sValue, strlen(sValue), pInfoValue, nInfoValueMax, pnLength, SQLSMALLINT, addDbcError, dbc, return SQL_ERROR);
	} else if (pInfoValue) {
		if (len == sizeof(SQLULEN))
			*(SQLULEN *) pInfoValue = (SQLULEN) nValue;
		else if (len == sizeof(SQLUINTEGER))
			*(SQLUINTEGER *) pInfoValue = (SQLUINTEGER) nValue;
		else if (len == sizeof(SQLUSMALLINT))
			*(SQLUSMALLINT *) pInfoValue = (SQLUSMALLINT) nValue;
		if (pnLength)
			*pnLength = len;
	}

	return dbc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLGetInfo(SQLHDBC hDbc,
	   SQLUSMALLINT nInfoType,
	   SQLPOINTER pInfoValue,
	   SQLSMALLINT nInfoValueMax,
	   SQLSMALLINT *pnLength)
{
	ODBCDbc *dbc = (ODBCDbc *) hDbc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetInfo " PTRFMT " %u\n",
		PTRFMTCAST hDbc, (unsigned int) nInfoType);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	return SQLGetInfo_(dbc, nInfoType, pInfoValue, nInfoValueMax, pnLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLGetInfoA(SQLHDBC hDbc,
	    SQLUSMALLINT nInfoType,
	    SQLPOINTER pInfoValue,
	    SQLSMALLINT nInfoValueMax,
	    SQLSMALLINT *pnLength)
{
	return SQLGetInfo(hDbc, nInfoType, pInfoValue, nInfoValueMax, pnLength);
}

SQLRETURN SQL_API
SQLGetInfoW(SQLHDBC hDbc,
	    SQLUSMALLINT nInfoType,
	    SQLPOINTER pInfoValue,
	    SQLSMALLINT nInfoValueMax,
	    SQLSMALLINT *pnLength)
{
	ODBCDbc *dbc = (ODBCDbc *) hDbc;
	SQLRETURN rc;
	SQLPOINTER ptr;
	SQLSMALLINT n;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetInfoW " PTRFMT " %u\n",
		PTRFMTCAST hDbc, (unsigned int) nInfoType);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	switch (nInfoType) {
	/* all string attributes */
	case SQL_ACCESSIBLE_PROCEDURES:
	case SQL_ACCESSIBLE_TABLES:
	case SQL_CATALOG_NAME:
	case SQL_CATALOG_NAME_SEPARATOR:
	case SQL_CATALOG_TERM:
	case SQL_COLLATION_SEQ:
	case SQL_COLUMN_ALIAS:
	case SQL_DATABASE_NAME:
	case SQL_DATA_SOURCE_NAME:
	case SQL_DATA_SOURCE_READ_ONLY:
	case SQL_DBMS_NAME:
	case SQL_DBMS_VER:
	case SQL_DESCRIBE_PARAMETER:
	case SQL_DM_VER:
	case SQL_DRIVER_NAME:
	case SQL_DRIVER_ODBC_VER:
	case SQL_DRIVER_VER:
	case SQL_EXPRESSIONS_IN_ORDERBY:
	case SQL_IDENTIFIER_QUOTE_CHAR:
	case SQL_INTEGRITY:
	case SQL_KEYWORDS:
	case SQL_LIKE_ESCAPE_CLAUSE:
	case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
	case SQL_MULTIPLE_ACTIVE_TXN:
	case SQL_MULT_RESULT_SETS:
	case SQL_NEED_LONG_DATA_LEN:
	case SQL_ODBC_VER:
	case SQL_ORDER_BY_COLUMNS_IN_SELECT:
	case SQL_OUTER_JOINS:
	case SQL_PROCEDURES:
	case SQL_PROCEDURE_TERM:
	case SQL_ROW_UPDATES:
	case SQL_SCHEMA_TERM:
	case SQL_SEARCH_PATTERN_ESCAPE:
	case SQL_SERVER_NAME:
	case SQL_SPECIAL_CHARACTERS:
	case SQL_TABLE_TERM:
	case SQL_USER_NAME:
	case SQL_XOPEN_CLI_YEAR:
		rc = SQLGetInfo_(dbc, nInfoType, NULL, 0, &n);
		if (!SQL_SUCCEEDED(rc))
			return rc;
		clearDbcErrors(dbc);
		n++;		/* account for NUL byte */
		ptr = (SQLPOINTER) malloc(n);
		break;
	default:
		n = nInfoValueMax;
		ptr = pInfoValue;
		break;
	}

	rc = SQLGetInfo_(dbc, nInfoType, ptr, n, &n);

	if (ptr != pInfoValue)
		fixWcharOut(rc, ptr, n, pInfoValue, nInfoValueMax, pnLength, 2, addDbcError, dbc);
	else if (pnLength)
		*pnLength = n;

	return rc;
}
#endif /* WITH_WCHAR */
