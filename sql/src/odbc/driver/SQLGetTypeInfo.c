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
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLGetTypeInfo(SQLHSTMT hStmt, SQLSMALLINT nSqlDataType)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetTypeInfo %d\n", nSqlDataType);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, no query should be prepared or executed */
	if (stmt->State != INITED) {
		/* 24000 = Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);

		return SQL_ERROR;
	}

	switch (nSqlDataType) {
	case SQL_ALL_TYPES:	/* 0 */
	case SQL_CHAR:		/* 1 */
	case SQL_NUMERIC:	/* 2 */
	case SQL_DECIMAL:	/* 3 */
	case SQL_INTEGER:	/* 4 */
	case SQL_SMALLINT:	/* 5 */
	case SQL_FLOAT:		/* 6 */
	case SQL_REAL:		/* 7 */
	case SQL_DOUBLE:	/* 8 */
	case SQL_DATE:		/* 9 aka SQL_DATE_TIME in ODBC 3.0+ */
	case SQL_TIME:		/* 10 aka SQL_INTERVAL in ODBC 3.0+ */
	case SQL_TIMESTAMP:	/* 11 */
	case SQL_VARCHAR:	/* 12 */
	case SQL_TYPE_DATE:	/* 91 */
	case SQL_TYPE_TIME:	/* 92 */
	case SQL_TYPE_TIMESTAMP:	/* 93 */
	case SQL_LONGVARCHAR:	/* (-1) */
	case SQL_BINARY:	/* (-2) */
	case SQL_VARBINARY:	/* (-3) */
	case SQL_LONGVARBINARY:	/* (-4) */
	case SQL_BIGINT:	/* (-5) */
	case SQL_TINYINT:	/* (-6) */
	case SQL_BIT:		/* (-7) */
	case SQL_WCHAR:	/* (-8) */
	case SQL_WVARCHAR:	/* (-9) */
	case SQL_WLONGVARCHAR:	/* (-10) */
	case SQL_GUID:		/* (-11) */
	case SQL_INTERVAL_YEAR:	/* (100 + 1) */
	case SQL_INTERVAL_MONTH:	/* (100 + 2) */
	case SQL_INTERVAL_DAY:	/* (100 + 3) */
	case SQL_INTERVAL_HOUR:	/* (100 + 4) */
	case SQL_INTERVAL_MINUTE:	/* (100 + 5) */
	case SQL_INTERVAL_SECOND:	/* (100 + 6) */
	case SQL_INTERVAL_YEAR_TO_MONTH:	/* (100 + 7) */
	case SQL_INTERVAL_DAY_TO_HOUR:	/* (100 + 8) */
	case SQL_INTERVAL_DAY_TO_MINUTE:	/* (100 + 9) */
	case SQL_INTERVAL_DAY_TO_SECOND:	/* (100 + 10) */
	case SQL_INTERVAL_HOUR_TO_MINUTE:	/* (100 + 11) */
	case SQL_INTERVAL_HOUR_TO_SECOND:	/* (100 + 12) */
	case SQL_INTERVAL_MINUTE_TO_SECOND:	/* (100 + 13) */
		break;

		/* some pre ODBC 3.0 data types which can be mapped to ODBC 3.0 data types */
	case -80:		/* SQL_INTERVAL_YEAR */
		nSqlDataType = SQL_INTERVAL_YEAR;
		break;
	case -81:		/* SQL_INTERVAL_YEAR_TO_MONTH */
		nSqlDataType = SQL_INTERVAL_YEAR_TO_MONTH;
		break;
	case -82:		/* SQL_INTERVAL_MONTH */
		nSqlDataType = SQL_INTERVAL_MONTH;
		break;
	case -83:		/* SQL_INTERVAL_DAY */
		nSqlDataType = SQL_INTERVAL_DAY;
		break;
	case -84:		/* SQL_INTERVAL_HOUR */
		nSqlDataType = SQL_INTERVAL_HOUR;
		break;
	case -85:		/* SQL_INTERVAL_MINUTE */
		nSqlDataType = SQL_INTERVAL_MINUTE;
		break;
	case -86:		/* SQL_INTERVAL_SECOND */
		nSqlDataType = SQL_INTERVAL_SECOND;
		break;
	case -87:		/* SQL_INTERVAL_DAY_TO_HOUR */
		nSqlDataType = SQL_INTERVAL_DAY_TO_HOUR;
		break;
	case -88:		/* SQL_INTERVAL_DAY_TO_MINUTE */
		nSqlDataType = SQL_INTERVAL_DAY_TO_MINUTE;
		break;
	case -89:		/* SQL_INTERVAL_DAY_TO_SECOND */
		nSqlDataType = SQL_INTERVAL_DAY_TO_SECOND;
		break;
	case -90:		/* SQL_INTERVAL_HOUR_TO_MINUTE */
		nSqlDataType = SQL_INTERVAL_HOUR_TO_MINUTE;
		break;
	case -91:		/* SQL_INTERVAL_HOUR_TO_SECOND */
		nSqlDataType = SQL_INTERVAL_HOUR_TO_SECOND;
		break;
	case -92:		/* SQL_INTERVAL_MINUTE_TO_SECOND */
		nSqlDataType = SQL_INTERVAL_MINUTE_TO_SECOND;
		break;

	case -95:		/* SQL_UNICODE_CHAR and SQL_UNICODE */
		nSqlDataType = SQL_WCHAR;
		break;
	case -96:		/* SQL_UNICODE_VARCHAR */
		nSqlDataType = SQL_WVARCHAR;
		break;
	case -97:		/* SQL_UNICODE_LONGVARCHAR */
		nSqlDataType = SQL_WLONGVARCHAR;
		break;
	default:
		/* HY004 = Invalid SQL data type */
		addStmtError(stmt, "HY004", NULL, 0);

		return SQL_ERROR;
	}

	/* construct the query now */
	query = malloc(1000);
	assert(query);

	/* result (see http://odbcrouter.com/api/SQLGetTypeInfo.shtml (some
	 * names changed when going from odbc 2.0 to 3.0)
	 * list below is odbc 3.0
	 *      VARCHAR(128)   TYPE_NAME
	 *      SMALLINT       DATA_TYPE NOT NULL
	 *      INTEGER        COLUMN_SIZE
	 *      VARCHAR(128)   LITERAL_PREFIX   (example 0x (binary data), 
	 *      VARCHAR(128)   LITERAL_SUFFIX   "'" (strings))
	 *      VARCHAR(128)   CREATE_PARAMS    (example precision,scale or max length)
	 *      SMALLINT       NULLABLE NOT NULL 
	 *      SMALLINT       CASE_SENSITIVE NOT NULL
	 *      SMALLINT       SEARCHABLE NOT NULL 
	 *      SMALLINT       UNSIGNED_ATTRIBUTE
	 *      SMALLINT       FIXED_PREC_SCALE NOT NULL
	 *      SMALLINT       AUTO_UNIQUE_VALUE
	 *      VARCHAR(128)   LOCAL_TYPE_NAME
	 *      SMALLINT       MINIMUM_SCALE
	 *      SMALLINT       MAXIMUM_SCALE
	 *      SMALLINT       SQL_DATA_TYPE NOT NULL (== DATA_TYPE except for date interval)
	 *      SMALLINT       SQL_DATETIME_SUB
	 *      SMALLINT       NUM_PREC_RADIX (2 for approximate 10 for exact numeric values)
	 *      SMALLINT       INTERVAL_PRECISION 
	 */
	strcpy(query,
	       "SELECT SQLNAME AS TYPE_NAME, "
	       "SYSTEMNAME AS DATA_TYPE, "
	       "DIGITS AS COLUMN_SIZE, "
	       "'' /*LITERAL_PREFIX*/ AS LITERAL_PREFIX, "
	       "'' /*LITERAL_SUFFIX*/ AS LITERAL_SUFFIX, "
	       "'' /*CREATE_PARAMS*/ AS CREATE_PARAMS, "
	       "1 /*SQL_NULLABLE*/ AS NULLABLE, "
	       "0 /*CASE_SENSITIVE*/ AS CASE_SENSITIVE, "
	       "0 /*SEARCHABLE*/ AS SEARCHABLE, "
	       "0 /*UNSIGNED*/ AS UNSIGNED_ATTRIBUTE, "
	       "SCALE /*FIXED_PREC_SCALE*/ AS FIXED_PREC_SCALE, "
	       "0 /*AUTO_UNIQUE_VALE*/ AS AUTO_UNIQUE_VALE, "
	       "SYSTEMNAME /*MONET_TYPE_NAME*/ AS LOCAL_TYPE_NAME, "
	       "0 /*MIN_SCALE*/ AS MINIMUM_SCALE, "
	       "SCALE /*MAX_SCALE*/ AS MAXIMUM_SCALE, "
	       "0 /*SQL_TYPE*/ AS SQL_DATA_TYPE, "
	       "NULL AS SQL_DATETIME_SUB, " /* parser complains about NULL */
	       "10 AS NUM_PREC_RADIX, "
	       "0 /*INTERVAL_PREC*/ AS INTERVAL_PRECISION "
	       "FROM TYPES");

/* TODO: SEARCHABLE should return an int iso str. Add a CASE
   SEARCHABLE WHEN ... to convert str to correct int values */

/* SQL_TYPE does not exist in table */
	if (nSqlDataType != SQL_ALL_TYPES) {
		/* add a selection when a specific SQL data type is
		   requested */
		char *tmp = query + strlen(query);

		snprintf(tmp, 30, " WHERE SQL_TYPE = %d", nSqlDataType);
	} else {
		/* add the ordering (only needed when all types are
		   selected) */
		strcat(query, " ORDER BY SQL_TYPE");
	}

	/* query the MonetDb data dictionary tables */
	assert(query);
	rc = SQLExecDirect_(hStmt, (SQLCHAR *) query, SQL_NTS);

	free(query);

	return rc;
}
