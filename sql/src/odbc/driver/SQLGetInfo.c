/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 * 
 * The Original Code is the Monet Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.  
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

/**********************************************************************
 * SQLGetInfo()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"


SQLRETURN SQLGetInfo(
	SQLHDBC		hDbc,
	SQLUSMALLINT	nInfoType,
	SQLPOINTER	pInfoValue,
	SQLSMALLINT	nInfoValueMax,
	SQLSMALLINT *	pnLength)
{
	ODBCDbc * dbc = (ODBCDbc *) hDbc;
	int	nValue = 0;
	char *	sValue = NULL;
	int	isNumericValue = 1;	/* flag to determine is nValue or sValue has to be returned */
	int	len = 0;
	SQLRETURN	returnstate = SQL_SUCCESS;


	if (! isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	/* For some info types an active connection is needed */
	if (nInfoType == SQL_DATA_SOURCE_NAME	/* 02 */
	 || nInfoType == SQL_SERVER_NAME	/* 13 */
	 || nInfoType == SQL_DATABASE_NAME	/* 16 */
	 || nInfoType == SQL_USER_NAME )	/* 47 */
	{
		if (dbc->Connected != 1)
		{
			/* 08003 = Connection does not exist */
			addDbcError(dbc, "08003", NULL, 0);
			return SQL_ERROR;
		}
	}

	switch (nInfoType)
	{
		case SQL_ACTIVE_CONNECTIONS:	/* 00 */
			nValue = 1;		/* 0 = No specified limit */
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_ACTIVE_STATEMENTS:	/* 01 */
			nValue = 0;		/* 0 = No specified limit */
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_DATA_SOURCE_NAME:	/* 02 */
			isNumericValue = 0;
			sValue = strdup((dbc->DSN != NULL) ? dbc->DSN : "");
			break;
		case SQL_DATABASE_NAME:		/* 16 */
			isNumericValue = 0;
			sValue = strdup((dbc->DBNAME != NULL) ? dbc->DBNAME : "");
			break;
		case SQL_DRIVER_NAME:		/* 06 */
			isNumericValue = 0;
			sValue = strdup(MONETDB_DRIVER_NAME);
			break;
		case SQL_DRIVER_VER:		/* 07 */
			isNumericValue = 0;
			sValue = strdup(MONETDB_DRIVER_VER);
			break;
		case SQL_FETCH_DIRECTION:	/* 08 */
			nValue = SQL_FD_FETCH_NEXT;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_ODBC_API_CONFORMANCE:	/* 09 */
			nValue = SQL_OIC_CORE;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_ODBC_VER:		/* 10 */
		case SQL_DRIVER_ODBC_VER:	/* 77 */
			isNumericValue = 0;
			sValue = strdup(MONETDB_ODBC_VER);
			break;
		case SQL_ROW_UPDATES:		/* 11 */
			isNumericValue = 0;
			sValue = strdup("N");
			break;
		case SQL_ODBC_SAG_CLI_CONFORMANCE:	/* 12 */
			nValue = SQL_OSCC_COMPLIANT;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_SERVER_NAME:		/* 13 */
			isNumericValue = 0;
			sValue = strdup(MONETDB_SERVER_NAME);
			break;
		case SQL_SEARCH_PATTERN_ESCAPE:	/* 14 */
			isNumericValue = 0;
			sValue = strdup("");	/* No search-char. escape char.*/
			break;
		case SQL_ODBC_SQL_CONFORMANCE:	/* 15 */
			nValue = SQL_OSC_CORE;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_DBMS_NAME:		/* 17 */
			isNumericValue = 0;
			sValue = strdup(MONETDB_PRODUCT_NAME);
			break;
		case SQL_DBMS_VER:		/* 18 */
			isNumericValue = 0;
			sValue = strdup(MONETDB_DRIVER_VER);
			break;
		case SQL_ACCESSIBLE_TABLES:	/* 19 */
			isNumericValue = 0;
			sValue = strdup("Y");
			break;
		case SQL_ACCESSIBLE_PROCEDURES:	/* 20 */
			isNumericValue = 0;
			sValue = strdup("N");
			break;
		case SQL_PROCEDURES:		/* 21 */
			isNumericValue = 0;
			sValue = strdup("N");
			break;
		case SQL_CONCAT_NULL_BEHAVIOR:	/* 22 */
			nValue = SQL_CB_NON_NULL;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_CURSOR_COMMIT_BEHAVIOR:	/* 23 */
		case SQL_CURSOR_ROLLBACK_BEHAVIOR:	/* 24 */
			nValue = SQL_CB_PRESERVE;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_DATA_SOURCE_READ_ONLY:	/* 25 */
			isNumericValue = 0;
			sValue = strdup("N");
			break;
		case SQL_DEFAULT_TXN_ISOLATION:	/* 26 */
			nValue = SQL_TXN_READ_COMMITTED;
			len = sizeof(SQLUINTEGER);
			break;
		case SQL_EXPRESSIONS_IN_ORDERBY:	/* 27 */
			isNumericValue = 0;
			sValue = strdup("Y");
			break;
		case SQL_IDENTIFIER_CASE:	/* 28 */
			nValue = SQL_IC_UPPER;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_IDENTIFIER_QUOTE_CHAR:	/* 29 */
			isNumericValue = 0;
			sValue = strdup("\""); /* the " (double quote) */
			break;
		case SQL_MAX_COLUMN_NAME_LEN:	/* 30 */
		case SQL_MAX_TABLE_NAME_LEN:	/* 35 */
		case SQL_MAX_OWNER_NAME_LEN:	/* 32 */
		case SQL_MAX_QUALIFIER_NAME_LEN:/* 34 */
			nValue = 255;	/* in monet string can be very long, but limit it here to 255 which should be enough in most cases */
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_MAX_CURSOR_NAME_LEN:	/* 31 */
			nValue = 0;	/* currently SQLSetCursorName is not supported */
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_MAX_PROCEDURE_NAME_LEN:/* 33 */
			/* No support for stored procedures: return 0 as value */
			nValue = 0;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_MULT_RESULT_SETS:	/* 36 */
			isNumericValue = 0;
			sValue = strdup("N");
			break;
		case SQL_MULTIPLE_ACTIVE_TXN:	/* 37 */
			isNumericValue = 0;
			sValue = strdup("Y");
			break;
		case SQL_OUTER_JOINS:	/* 38 */
			isNumericValue = 0;
			sValue = strdup("Y");
			break;
		case SQL_OWNER_TERM:		/* 39 */
		case SQL_PROCEDURE_TERM:	/* 40 */
		case SQL_QUALIFIER_TERM:	/* 42 */
		case SQL_TABLE_TERM:		/* 45 */
			/* no Terms suoported */
			isNumericValue = 0;
			sValue = strdup("");
			break;
		case SQL_QUALIFIER_NAME_SEPARATOR:	/* 41 */
			isNumericValue = 0;
			sValue = strdup(".");
			break;
		case SQL_SCROLL_CONCURRENCY:	/* 43 */
			nValue = SQL_SCCO_READ_ONLY;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_SCROLL_OPTIONS:	/* 44 */
			nValue = SQL_SO_FORWARD_ONLY;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_TXN_CAPABLE:		/* 46 */
			nValue = SQL_TC_ALL;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_USER_NAME:		/* 47 */
			isNumericValue = 0;
			sValue = strdup((dbc->UID != NULL) ? dbc->UID : "");
			break;
		case SQL_CONVERT_FUNCTIONS:		/* 48 */
			/* No convert function supported */
			nValue = 0;		/* SQL_FN_CVT_CONVERT; */
			len = sizeof(SQLUINTEGER);
			break;
		case SQL_NUMERIC_FUNCTIONS:		/* 49 */
			if (nInfoValueMax >= sizeof(SQLUINTEGER)) /* 32 bits */
			{
				nValue =
					SQL_FN_NUM_ABS |
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
					SQL_FN_NUM_TAN ;
				len = sizeof(SQLUINTEGER);
			}
			else
				returnstate = SQL_ERROR;
			break;
		case SQL_STRING_FUNCTIONS:			/* 50 */
			if (nInfoValueMax >= sizeof(SQLUINTEGER)) /* 32 bits */
			{
				nValue =
					SQL_FN_STR_ASCII |
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
					SQL_FN_STR_SUBSTRING |
					SQL_FN_STR_UCASE ;
				len = sizeof(SQLUINTEGER);
			}
			else
				returnstate = SQL_ERROR;
			break;
		case SQL_SYSTEM_FUNCTIONS:			/* 51 */
			if (nInfoValueMax >= sizeof(SQLUINTEGER)) /* 32 bits */
			{
				nValue =
					SQL_FN_SYS_DBNAME |
					SQL_FN_SYS_USERNAME |
					SQL_FN_SYS_IFNULL ;
				len = sizeof(SQLUINTEGER);
			}
			else
				returnstate = SQL_ERROR;
			break;
		case SQL_TIMEDATE_FUNCTIONS:		/* 52 */
			if (nInfoValueMax >= sizeof(SQLUINTEGER)) /* 32 bits */
			{
				nValue =
					SQL_FN_TD_CURDATE |
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
					SQL_FN_TD_YEAR ;
				len = sizeof(SQLUINTEGER);
			}
			else
				returnstate = SQL_ERROR;
			break;
		case SQL_CONVERT_BIGINT:	/* 53 */
		case SQL_CONVERT_BINARY:	/* 54 */
		case SQL_CONVERT_BIT:		/* 55 */
		case SQL_CONVERT_CHAR:		/* 56 */
		case SQL_CONVERT_DATE:		/* 57 */
		case SQL_CONVERT_DECIMAL:	/* 58 */
		case SQL_CONVERT_DOUBLE:	/* 59 */
		case SQL_CONVERT_FLOAT:		/* 60 */
		case SQL_CONVERT_INTEGER:	/* 61 */
		case SQL_CONVERT_LONGVARCHAR:	/* 62 */
		case SQL_CONVERT_NUMERIC:	/* 63 */
		case SQL_CONVERT_REAL:		/* 64 */
		case SQL_CONVERT_SMALLINT:	/* 65 */
		case SQL_CONVERT_TIME:		/* 66 */
		case SQL_CONVERT_TIMESTAMP:	/* 67 */
		case SQL_CONVERT_TINYINT:	/* 68 */
		case SQL_CONVERT_VARBINARY:	/* 69 */
		case SQL_CONVERT_VARCHAR:	/* 70 */
		case SQL_CONVERT_LONGVARBINARY:	/* 71 */
			if (nInfoValueMax >= sizeof(SQLUINTEGER)) /* 32 bits */
			{
				nValue = 0;
				len = sizeof(SQLUINTEGER);
			}
			else
				returnstate = SQL_ERROR;
			break;
		case SQL_TXN_ISOLATION_OPTION:	/* 72 */
			if (nInfoValueMax >= sizeof(SQLUINTEGER)) /* 32 bits */
			{
				nValue = SQL_TXN_REPEATABLE_READ;
				len = sizeof(SQLUINTEGER);
			}
			else
				returnstate = SQL_ERROR;
			break;
		case SQL_ODBC_SQL_OPT_IEF:	/* 73 */
			isNumericValue = 0;
			sValue = strdup("N");
			break;
		case SQL_CORRELATION_NAME:	/* 74 */
			nValue = SQL_CN_ANY;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_NON_NULLABLE_COLUMNS:	/* 75 */
			nValue = SQL_NNC_NON_NULL;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_POSITIONED_STATEMENTS:	/* 80 */
			if (nInfoValueMax >= sizeof(SQLUINTEGER)) /* 32 bits */
			{
				nValue = SQL_PS_SELECT_FOR_UPDATE;
				len = sizeof(SQLUINTEGER);
			}
			else
				returnstate = SQL_ERROR;
			break;
		case SQL_GETDATA_EXTENSIONS:	/* 81 */
			if (nInfoValueMax >= sizeof(SQLUINTEGER)) /* 32 bits */
			{
				nValue =
					SQL_GD_ANY_COLUMN |
					SQL_GD_ANY_ORDER |
					SQL_GD_BOUND ;
				len = sizeof(SQLUINTEGER);
			}
			else
				returnstate = SQL_ERROR;
			break;
		case SQL_FILE_USAGE:		/* 84 */
			nValue = SQL_FILE_NOT_SUPPORTED;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_NULL_COLLATION:	/* 85 */
			nValue = SQL_NC_LOW;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_COLUMN_ALIAS:		/* 87 */
			isNumericValue = 0;
			sValue = strdup("N");
			break;
		case SQL_GROUP_BY:		/* 88 */
			nValue = SQL_GB_NO_RELATION;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_KEYWORDS:		/* 89 */
		{	/* Returns the MonetDB keywords which are not listed
			 * as ODBC keyword in the #define SQL_ODBC_KEYWORDS
			 * in sql.h */
			char MonetSQL_keywords[] = "BOOLEAN,COLUMNS,FLOOR,IMPORT,REAL";
			isNumericValue = 0;
			sValue = strdup(MonetSQL_keywords);
			break;
		}
		case SQL_ORDER_BY_COLUMNS_IN_SELECT:	/* 90 */
			isNumericValue = 0;
			sValue = strdup("N");
			break;
		case SQL_QUOTED_IDENTIFIER_CASE:	/* 93 */
			nValue = SQL_IC_MIXED;
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_SPECIAL_CHARACTERS:	/* 94 */
			isNumericValue = 0;
			sValue = strdup("`!#$;:'<>"); /* allowed table name chars */
			break;
		case SQL_SUBQUERIES:		/* 95 */
			if (nInfoValueMax >= sizeof(SQLUINTEGER)) /* 32 bits */
			{
				nValue =
					SQL_SQ_COMPARISON |
					SQL_SQ_EXISTS |
					SQL_SQ_IN |
					SQL_SQ_CORRELATED_SUBQUERIES;
				len = sizeof(SQLUINTEGER);
			}
			else
				returnstate = SQL_ERROR;
			break;
		case SQL_UNION:			/* 96 */
			if (nInfoValueMax >= sizeof(SQLUINTEGER)) /* 32 bits */
			{
				nValue = SQL_U_UNION;
				len = sizeof(SQLUINTEGER);
			}
			else
				returnstate = SQL_ERROR;
			break;

		case SQL_MAX_COLUMNS_IN_GROUP_BY:	/* 97 */
		case SQL_MAX_COLUMNS_IN_INDEX:		/* 98 */
		case SQL_MAX_COLUMNS_IN_ORDER_BY:	/* 99 */
		case SQL_MAX_COLUMNS_IN_SELECT:		/* 100 */
		case SQL_MAX_COLUMNS_IN_TABLE:		/* 101 */
		case SQL_MAX_TABLES_IN_SELECT:		/* 106 */
			/* no specified limit for SQL_MAX_COLUMNS_IN_xxx */
			nValue = (SQLUSMALLINT) 0;	/* no limits */
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_MAX_INDEX_SIZE:	/* 102 */
		case SQL_MAX_ROW_SIZE:		/* 104 */
		case SQL_MAX_STATEMENT_LEN:	/* 105 */
			if (nInfoValueMax >= sizeof(SQLUINTEGER)) /* 32 bits */
			{
				nValue = (SQLUINTEGER) 0; /* no max.len.*/
				len = sizeof(SQLUINTEGER);
			}
			else
				returnstate = SQL_ERROR;
			break;
		case SQL_MAX_ROW_SIZE_INCLUDES_LONG:	/* 103 */
			isNumericValue = 0;
			sValue = strdup("N");
			break;
		case SQL_MAX_USER_NAME_LEN:	/* 107 */
			nValue = (SQLUSMALLINT) 0; /* no max.len. */
			len = sizeof(SQLUSMALLINT);
			break;
		case SQL_MAX_CHAR_LITERAL_LEN:	/* 108 */
			if (nInfoValueMax >= sizeof(SQLUINTEGER)) /* 32 bits */
			{
				nValue = (SQLUINTEGER) 1024 * 1024;	/* 1MB */
				len = sizeof(SQLUINTEGER);
			}
			else
				returnstate = SQL_ERROR;
			break;
		case SQL_NEED_LONG_DATA_LEN:	/* 111 */
			isNumericValue = 0;
			sValue = strdup("Y");
			break;
		case SQL_MAX_BINARY_LITERAL_LEN:	/* 112 */
			if (nInfoValueMax >= sizeof(SQLUINTEGER)) /* 32 bits */
			{
				nValue = (SQLUINTEGER) 0;	/* not supported yet */
				len = sizeof(SQLUINTEGER);
			}
			else
				returnstate = SQL_ERROR;
			break;
		case SQL_LIKE_ESCAPE_CLAUSE:		/* 113 */
			isNumericValue = 0;
			sValue = strdup("Y");
			break;
		case SQL_QUALIFIER_LOCATION:		/* 114 */
			nValue = (SQLUSMALLINT) SQL_QL_END;
			len = sizeof(SQLUSMALLINT);
			break;


/**** The next Infotypes all return 0x00000000L ****/
		case SQL_LOCK_TYPES:		/* 78 */ /* SQLSetPos NOT supported */
		case SQL_POS_OPERATIONS:	/* 79 */ /* SQLSetPos NOT supported */
		case SQL_BOOKMARK_PERSISTENCE:	/* 82 */ /* Bookmarks NOT supported */
		case SQL_STATIC_SENSITIVITY:	/* 83 */ /* SQLSetPos NOT supported */
		case SQL_TIMEDATE_ADD_INTERVALS:/* 109 */ /* INTERVALS NOT supported */
		case SQL_TIMEDATE_DIFF_INTERVALS:/* 110 */ /* INTERVALS NOT supported */
			if (nInfoValueMax >= sizeof(SQLUINTEGER)) /* 32 bits */
			{
				nValue = 0x00000000L;
				len = sizeof(SQLUINTEGER);
			}
			else
				returnstate = SQL_ERROR;
			break;

		case SQL_ALTER_TABLE:		/* 86 */
		case SQL_OWNER_USAGE:		/* 91 */
		case SQL_QUALIFIER_USAGE:	/* 92 */
		case SQL_OJ_CAPABILITIES:	/* 65003 */
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
	if (isNumericValue)
	{
		if (pInfoValue)
		{
			if (len == sizeof(SQLUINTEGER)) {
				*((SQLUINTEGER*)pInfoValue) = (SQLUINTEGER)nValue;
			} else if (len == sizeof(SQLUSMALLINT)) {
				*((SQLUSMALLINT*)pInfoValue) = (SQLUSMALLINT)nValue;
			}
		}
	} else {
		if (sValue)
		{
			len = strlen(sValue);
			if (pInfoValue)
			{
				if (len < nInfoValueMax) {
					strcpy((char *)pInfoValue, sValue);
				} else {
					/* truncate the data before copying */
					sValue[nInfoValueMax] = '\0';
					strcpy((char *)pInfoValue, sValue);

					/* data is truncated, set warning */
					if (returnstate == SQL_SUCCESS)
						returnstate = SQL_SUCCESS_WITH_INFO;
				}
			} else {
				/* no valid return buffer pointer supplied */
				/* set warning data is truncated */
				if (returnstate == SQL_SUCCESS)
					returnstate = SQL_SUCCESS_WITH_INFO;
			}
			/* clean up the allocated string */
			free(sValue);
		} else {
			len = 0;
			if (pInfoValue)
				strcpy((char *)pInfoValue, "");
		}
	}

	if (pnLength) {
		*pnLength = len;
	}


	/* depending on the returnstate add, an error msg */
	if (returnstate == SQL_SUCCESS_WITH_INFO)
	{
		/* 01004 = String data, right truncation */
		addDbcError(dbc, "01004", NULL, 0);
	}
	else if (returnstate == SQL_ERROR) {
		/* HY090 = Invalid string or buffer length */
		addDbcError(dbc, "HY090", NULL, 0);
	}

	return returnstate;
}
