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
 * Author: Martin van Dinther
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"


SQLRETURN
SQLGetInfo(SQLHDBC hDbc, SQLUSMALLINT nInfoType, SQLPOINTER pInfoValue,
	   SQLSMALLINT nInfoValueMax, SQLSMALLINT *pnLength)
{
	ODBCDbc *dbc = (ODBCDbc *) hDbc;
	int nValue = 0;
	const char *sValue = NULL;	/* iff non-NULL, return string value */
	int len = 0;
	SQLRETURN returnstate = SQL_SUCCESS;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetInfo %d\n", nInfoType);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	/* For some info types an active connection is needed */
	if ((nInfoType == SQL_DATA_SOURCE_NAME ||
	     nInfoType == SQL_SERVER_NAME ||
	     nInfoType == SQL_DATABASE_NAME ||
	     nInfoType == SQL_USER_NAME) &&
	    dbc->Connected != 1) {
		/* 08003 = Connection does not exist */
		addDbcError(dbc, "08003", NULL, 0);
		return SQL_ERROR;
	}

	switch (nInfoType) {
	case SQL_MAX_DRIVER_CONNECTIONS:
		nValue = 0;			/* 0 = No specified limit */
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_MAX_CONCURRENT_ACTIVITIES:
		nValue = 0;			/* 0 = No specified limit */
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_DATA_SOURCE_NAME:
		sValue = dbc->DSN != NULL ? dbc->DSN : "";
		break;
	case SQL_DATABASE_NAME:
		sValue = dbc->DBNAME ? dbc->DBNAME : "";
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
		sValue = "";		/* No search-char. escape char. */
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
	case SQL_ACCESSIBLE_TABLES:
		sValue = "Y";
		break;
	case SQL_ACCESSIBLE_PROCEDURES:
		sValue = "Y";
		break;
	case SQL_PROCEDURES:
		sValue = "N";
		break;
	case SQL_CONCAT_NULL_BEHAVIOR:
		nValue = SQL_CB_NON_NULL;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_CURSOR_COMMIT_BEHAVIOR:
	case SQL_CURSOR_ROLLBACK_BEHAVIOR:
		nValue = SQL_CB_PRESERVE;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_DATA_SOURCE_READ_ONLY:
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
		nValue = SQL_IC_UPPER;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_IDENTIFIER_QUOTE_CHAR:
		sValue = "\"";			/* the " (double quote) */
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
	case SQL_SCHEMA_TERM:
	case SQL_PROCEDURE_TERM:
	case SQL_CATALOG_TERM:
	case SQL_TABLE_TERM:
		/* no Terms supported */
		sValue = "";
		break;
	case SQL_SCROLL_CONCURRENCY:
		nValue = SQL_SCCO_READ_ONLY;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_SCROLL_OPTIONS:
		nValue = SQL_SO_FORWARD_ONLY;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_TXN_CAPABLE:
		nValue = SQL_TC_ALL;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_USER_NAME:
		sValue = dbc->UID ? dbc->UID : "";
		break;
	case SQL_CONVERT_FUNCTIONS:
		/* No convert function supported */
		nValue = 0;	/* SQL_FN_CVT_CONVERT; */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_NUMERIC_FUNCTIONS:
		nValue = SQL_FN_NUM_ABS | SQL_FN_NUM_ACOS | SQL_FN_NUM_ASIN |
			SQL_FN_NUM_ATAN | SQL_FN_NUM_ATAN2 |
			SQL_FN_NUM_CEILING | SQL_FN_NUM_COS | SQL_FN_NUM_COT |
			SQL_FN_NUM_DEGREES | SQL_FN_NUM_EXP |
			SQL_FN_NUM_FLOOR | SQL_FN_NUM_LOG | SQL_FN_NUM_LOG10 |
			SQL_FN_NUM_MOD | SQL_FN_NUM_PI | SQL_FN_NUM_POWER |
			SQL_FN_NUM_RADIANS | SQL_FN_NUM_RAND |
			SQL_FN_NUM_ROUND | SQL_FN_NUM_SIGN | SQL_FN_NUM_SIN |
			SQL_FN_NUM_SQRT | SQL_FN_NUM_TRUNCATE | SQL_FN_NUM_TAN;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_STRING_FUNCTIONS:
		nValue = SQL_FN_STR_ASCII | SQL_FN_STR_CHAR |
			SQL_FN_STR_CONCAT | SQL_FN_STR_DIFFERENCE |
			SQL_FN_STR_INSERT | SQL_FN_STR_LCASE |
			SQL_FN_STR_LEFT | SQL_FN_STR_LENGTH |
			SQL_FN_STR_LOCATE | SQL_FN_STR_LTRIM |
			SQL_FN_STR_REPEAT | SQL_FN_STR_REPLACE |
			SQL_FN_STR_RIGHT | SQL_FN_STR_RTRIM |
			SQL_FN_STR_SOUNDEX | SQL_FN_STR_SPACE |
			SQL_FN_STR_SUBSTRING | SQL_FN_STR_UCASE;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_SYSTEM_FUNCTIONS:
		nValue = SQL_FN_SYS_DBNAME | SQL_FN_SYS_USERNAME |
			SQL_FN_SYS_IFNULL;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_TIMEDATE_FUNCTIONS:
		nValue = SQL_FN_TD_CURDATE | SQL_FN_TD_CURTIME |
			SQL_FN_TD_DAYNAME | SQL_FN_TD_DAYOFMONTH |
			SQL_FN_TD_DAYOFWEEK | SQL_FN_TD_DAYOFYEAR |
			SQL_FN_TD_HOUR | SQL_FN_TD_MINUTE | SQL_FN_TD_MONTH |
			SQL_FN_TD_MONTHNAME | SQL_FN_TD_NOW |
			SQL_FN_TD_QUARTER | SQL_FN_TD_SECOND |
			SQL_FN_TD_TIMESTAMPADD | SQL_FN_TD_TIMESTAMPDIFF |
			SQL_FN_TD_WEEK | SQL_FN_TD_YEAR;
		len = sizeof(SQLUINTEGER);
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
	case SQL_CONVERT_LONGVARCHAR:
	case SQL_CONVERT_NUMERIC:
	case SQL_CONVERT_REAL:
	case SQL_CONVERT_SMALLINT:
	case SQL_CONVERT_TIME:
	case SQL_CONVERT_TIMESTAMP:
	case SQL_CONVERT_TINYINT:
	case SQL_CONVERT_VARBINARY:
	case SQL_CONVERT_VARCHAR:
	case SQL_CONVERT_LONGVARBINARY:
		nValue = 0;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_TXN_ISOLATION_OPTION:
		nValue = SQL_TXN_REPEATABLE_READ;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_INTEGRITY:
		sValue = "N";
		break;
	case SQL_CORRELATION_NAME:
		nValue = SQL_CN_ANY;
		len = sizeof(SQLUSMALLINT);
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
		nValue = SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER | SQL_GD_BOUND;
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
	case SQL_COLUMN_ALIAS:
		sValue = "N";
		break;
	case SQL_GROUP_BY:
		nValue = SQL_GB_NO_RELATION;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_KEYWORDS: {
		/* Returns the MonetDB keywords which are not listed
		 * as ODBC keyword in the #define SQL_ODBC_KEYWORDS
		 * in sql.h
		 */
		sValue = "BOOLEAN,COLUMNS,FLOOR,IMPORT,REAL";
		break;
	}
	case SQL_ORDER_BY_COLUMNS_IN_SELECT:
		sValue = "N";
		break;
	case SQL_QUOTED_IDENTIFIER_CASE:
		nValue = SQL_IC_MIXED;
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_SPECIAL_CHARACTERS:
		sValue = "`!#$;:'<>";		/* allowed table name chars */
		break;
	case SQL_SUBQUERIES:
		nValue =
		    SQL_SQ_COMPARISON | SQL_SQ_EXISTS | SQL_SQ_IN |
		    SQL_SQ_CORRELATED_SUBQUERIES;
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
		nValue = (SQLUSMALLINT) 0;	/* no limits */
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_MAX_INDEX_SIZE:
	case SQL_MAX_ROW_SIZE:
	case SQL_MAX_STATEMENT_LEN:
		nValue = (SQLUINTEGER) 0;	/* no max.len. */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
		sValue = "N";
		break;
	case SQL_MAX_USER_NAME_LEN:
		nValue = (SQLUSMALLINT) 0;	/* no max.len. */
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
		nValue = (SQLUINTEGER) 0;	/* not supported yet */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_LIKE_ESCAPE_CLAUSE:
		sValue = "Y";
		break;

	case SQL_LOCK_TYPES:			/* SQLSetPos NOT supported */
	case SQL_POS_OPERATIONS:		/* SQLSetPos NOT supported */
	case SQL_STATIC_SENSITIVITY:		/* SQLSetPos NOT supported */
	case SQL_TIMEDATE_ADD_INTERVALS:	/* INTERVALS NOT supported */
	case SQL_TIMEDATE_DIFF_INTERVALS:	/* INTERVALS NOT supported */
		nValue = 0;
		len = sizeof(SQLUINTEGER);
		break;

	case SQL_ALTER_TABLE:
		nValue = 0;	/* XXX needs checking */
		/* SQL_AT_ADD_COLUMN_SINGLE |
		   SQL_AT_ADD_COLUMN_DEFAULT |
		   SQL_AT_ADD_COLUMN_COLLATION |
		   SQL_AT_SET_COLUMN_DEFAULT |
		   SQL_AT_DROP_COLUMN_DEFAULT |
		   SQL_AT_DROP_COLUMN_CASCADE |
		   SQL_AT_DROP_COLUMN_RESTRICT |
		   SQL_AT_ADD_TABLE_CONSTRAINT |
		   SQL_AT_DROP_TABLE_CONSTRAINT_CASCADE |
		   SQL_AT_DROP_TABLE_CONSTRAINT_RESTRICT |
		   SQL_AT_CONSTRAINT_NAME_DEFINITION |
		   SQL_AT_CONSTRAINT_INITIALLY_DEFERRED |
		   SQL_AT_CONSTRAINT_INITIALLY_IMMEDIATE |
		   SQL_AT_CONSTRAINT_DEFERRABLE |
		   SQL_AT_CONSTRAINT_NON_DEFERRABLE */
		len = sizeof(SQLUINTEGER);
		break;

	case SQL_ACTIVE_ENVIRONMENTS:
		nValue = 0;	/* 0 = no limit */
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_AGGREGATE_FUNCTIONS:
		nValue = SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT |
			SQL_AF_DISTINCT | SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM;
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_ALTER_DOMAIN:
		nValue = 0;	/* XXX needs checking */
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
	case SQL_ASYNC_MODE:
		nValue = SQL_AM_NONE;	/* XXX needs checking */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_BATCH_ROW_COUNT:
		nValue = 0;	/* XXX needs checking */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_BATCH_SUPPORT:
		nValue = 0;	/* XXX needs checking */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_BOOKMARK_PERSISTENCE:
		nValue = 0;	/* bookmarks not supported */
		len = sizeof(SQLUINTEGER);
		break;
	case SQL_CATALOG_LOCATION:
		nValue = SQL_CL_END; /* XXX needs checking */
		len = sizeof(SQLUSMALLINT);
		break;
	case SQL_CATALOG_NAME:
		sValue = "N";	/* XXX needs checking */
		break;
	case SQL_CATALOG_NAME_SEPARATOR:
		sValue = ".";	/* XXX needs checking */
		break;
	case SQL_CATALOG_USAGE:
	case SQL_COLLATION_SEQ:
	case SQL_CONVERT_INTERVAL_DAY_TIME:
	case SQL_CONVERT_INTERVAL_YEAR_MONTH:
	case SQL_CREATE_ASSERTION:
	case SQL_CREATE_CHARACTER_SET:
	case SQL_CREATE_COLLATION:
	case SQL_CREATE_DOMAIN:
	case SQL_CREATE_TABLE:
	case SQL_CREATE_TRANSLATION:
	case SQL_CURSOR_SENSITIVITY:
	case SQL_DATETIME_LITERALS:
	case SQL_DDL_INDEX:
	case SQL_DESCRIBE_PARAMETER:
	case SQL_DM_VER:
	case SQL_DRIVER_HDESC:
	case SQL_DROP_ASSERTION:
	case SQL_DROP_CHARACTER_SET:
	case SQL_DROP_COLLATION:
	case SQL_DROP_DOMAIN:
	case SQL_DROP_SCHEMA:
	case SQL_DROP_TABLE:
	case SQL_DROP_TRANSLATION:
	case SQL_DROP_VIEW:
	case SQL_DYNAMIC_CURSOR_ATTRIBUTES1:
	case SQL_DYNAMIC_CURSOR_ATTRIBUTES2:
	case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1:
	case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2:
	case SQL_INFO_SCHEMA_VIEWS:
	case SQL_INSERT_STATEMENT:
	case SQL_KEYSET_CURSOR_ATTRIBUTES1:
	case SQL_KEYSET_CURSOR_ATTRIBUTES2:
	case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS:
	case SQL_MAX_IDENTIFIER_LEN:
	case SQL_OJ_CAPABILITIES:
	case SQL_PARAM_ARRAY_ROW_COUNTS:
	case SQL_PARAM_ARRAY_SELECTS:
	case SQL_SCHEMA_USAGE:
	case SQL_STATIC_CURSOR_ATTRIBUTES1:
	case SQL_STATIC_CURSOR_ATTRIBUTES2:
	case SQL_XOPEN_CLI_YEAR:
		/* TODO: implement all the other Info Types */
		/* HYC00 = Optional feature not implemented */
		addDbcError(dbc, "HYC00", NULL, 0);
		return SQL_ERROR;

	default:
		/* HY096 = Information type out of range */
		addDbcError(dbc, "HY096", NULL, 0);
		return SQL_ERROR;
	}

	/* copy the data to the supplied output parameters */
	if (sValue) {
		len = strlen(sValue);
		if (pInfoValue && nInfoValueMax > 0) {
			strncpy((char *) pInfoValue, sValue,
				nInfoValueMax);
			if (len >= nInfoValueMax) {
				/* value got truncated */
				((char *) pInfoValue)[nInfoValueMax - 1] = 0;
				returnstate = SQL_SUCCESS_WITH_INFO;
			}
		} else {
			/* no valid return buffer pointer supplied */
			/* set warning data is truncated */
			returnstate = SQL_SUCCESS_WITH_INFO;
		}

		/* depending on the returnstate add, an error msg */
		if (returnstate == SQL_SUCCESS_WITH_INFO) {
			/* 01004 = String data, right truncation */
			addDbcError(dbc, "01004", NULL, 0);
		}

	} else if (pInfoValue) {
		if (len == sizeof(SQLUINTEGER))
			* (SQLUINTEGER *) pInfoValue = (SQLUINTEGER) nValue;
		else if (len == sizeof(SQLUSMALLINT))
			* (SQLUSMALLINT *) pInfoValue = (SQLUSMALLINT) nValue;
	}

	if (pnLength)
		*pnLength = len;

	return returnstate;
}
