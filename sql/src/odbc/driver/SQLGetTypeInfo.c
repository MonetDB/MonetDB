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
 * SQLGetTypeInfo()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN SQLGetTypeInfo(
	SQLHSTMT	hStmt,
	SQLSMALLINT	nSqlDataType )
{
	ODBCStmt * stmt = (ODBCStmt *) hStmt;
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char * query = NULL;


	if (! isValidStmt(stmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, no query should be prepared or executed */
	if (stmt->State != INITED) {
		/* 24000 = Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	assert(stmt->Query == NULL);


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
		case SQL_TYPE_TIMESTAMP:/* 93 */
		case SQL_LONGVARCHAR:	/* (-1) */
		case SQL_BINARY:	/* (-2) */
		case SQL_VARBINARY:	/* (-3) */
		case SQL_LONGVARBINARY:	/* (-4) */
		case SQL_BIGINT:	/* (-5) */
		case SQL_TINYINT:	/* (-6) */
		case SQL_BIT:		/* (-7) */
		case SQL_WCHAR:		/* (-8) */
		case SQL_WVARCHAR:	/* (-9) */
		case SQL_WLONGVARCHAR:	/* (-10) */
		case SQL_GUID:		/* (-11) */
		case SQL_INTERVAL_YEAR:		/* (100 + 1) */
		case SQL_INTERVAL_MONTH:	/* (100 + 2) */
		case SQL_INTERVAL_DAY:		/* (100 + 3) */
		case SQL_INTERVAL_HOUR:		/* (100 + 4) */
		case SQL_INTERVAL_MINUTE:	/* (100 + 5) */
		case SQL_INTERVAL_SECOND:	/* (100 + 6) */
		case SQL_INTERVAL_YEAR_TO_MONTH:	/* (100 + 7) */
		case SQL_INTERVAL_DAY_TO_HOUR:		/* (100 + 8) */
		case SQL_INTERVAL_DAY_TO_MINUTE:	/* (100 + 9) */
		case SQL_INTERVAL_DAY_TO_SECOND:	/* (100 + 10) */
		case SQL_INTERVAL_HOUR_TO_MINUTE:	/* (100 + 11) */
		case SQL_INTERVAL_HOUR_TO_SECOND:	/* (100 + 12) */
		case SQL_INTERVAL_MINUTE_TO_SECOND:	/* (100 + 13) */
			break;

		/* some pre ODBC 3.0 data types which can be mapped to ODBC 3.0 data types */
		case (-80):	/* SQL_INTERVAL_YEAR */
			nSqlDataType = SQL_INTERVAL_YEAR;
			break;
		case (-81):	/* SQL_INTERVAL_YEAR_TO_MONTH */
			nSqlDataType = SQL_INTERVAL_YEAR_TO_MONTH;
			break;
		case (-82):	/* SQL_INTERVAL_MONTH */
			nSqlDataType = SQL_INTERVAL_MONTH;
			break;
		case (-83):	/* SQL_INTERVAL_DAY */
			nSqlDataType = SQL_INTERVAL_DAY;
			break;
		case (-84):	/* SQL_INTERVAL_HOUR */
			nSqlDataType = SQL_INTERVAL_HOUR;
			break;
		case (-85):	/* SQL_INTERVAL_MINUTE */
			nSqlDataType = SQL_INTERVAL_MINUTE;
			break;
		case (-86):	/* SQL_INTERVAL_SECOND */
			nSqlDataType = SQL_INTERVAL_SECOND;
			break;
		case (-87):	/* SQL_INTERVAL_DAY_TO_HOUR */
			nSqlDataType = SQL_INTERVAL_DAY_TO_HOUR;
			break;
		case (-88):	/* SQL_INTERVAL_DAY_TO_MINUTE */
			nSqlDataType = SQL_INTERVAL_DAY_TO_MINUTE;
			break;
		case (-89):	/* SQL_INTERVAL_DAY_TO_SECOND */
			nSqlDataType = SQL_INTERVAL_DAY_TO_SECOND;
			break;
		case (-90):	/* SQL_INTERVAL_HOUR_TO_MINUTE */
			nSqlDataType = SQL_INTERVAL_HOUR_TO_MINUTE;
			break;
		case (-91):	/* SQL_INTERVAL_HOUR_TO_SECOND */
			nSqlDataType = SQL_INTERVAL_HOUR_TO_SECOND;
			break;
		case (-92):	/* SQL_INTERVAL_MINUTE_TO_SECOND */
			nSqlDataType = SQL_INTERVAL_MINUTE_TO_SECOND;
			break;

		case (-95):	/* SQL_UNICODE_CHAR and SQL_UNICODE */
			nSqlDataType = SQL_WCHAR;
			break;
		case (-96):	/* SQL_UNICODE_VARCHAR */
			nSqlDataType = SQL_WVARCHAR;
			break;
		case (-97):	/* SQL_UNICODE_LONGVARCHAR */
			nSqlDataType = SQL_WLONGVARCHAR;
			break;
		default:
			/* HY004 = Invalid SQL data type */
			addStmtError(stmt, "HY004", NULL, 0);
			return SQL_ERROR;
	}

	/* construct the query now */
	query = GDKmalloc(1000);
	assert(query);

	strcpy(query, "SELECT SQL_TYPE_NAME AS TYPE_NAME, SQL_TYPE AS DATA_TYPE, MAX_COL_SIZE AS COLUMN_SIZE, LITERAL_PREFIX AS LITERAL_PREFIX, LITERAL_SUFFIX AS LITERAL_SUFFIX, CREATE_PARAMS AS CREATE_PARAMS, 1 /* SQL_NULLABLE */ AS NULLABLE, CASE_SENSITIVE AS CASE_SENSITIVE, SEARCHABLE AS SEARCHABLE, UNSIGNED AS UNSIGNED_ATTRIBUTE, FIXED_PREC_SCALE AS FIXED_PREC_SCALE, AUTO_UNIQUE_VALE AS AUTO_UNIQUE_VALE, MONET_TYPE_NAME AS LOCAL_TYPE_NAME, MIN_SCALE AS MINIMUM_SCALE, MAX_SCALE AS MAXIMUM_SCALE, SQL_TYPE AS SQL_DATA_TYPE, NULL AS SQL_DATETIME_SUB, 10 AS NUM_PREC_RADIX, INTERVAL_PREC AS INTERVAL_PRECISION FROM SQL_DATATYPE");

/* TODO: SEARCHABLE should return an int iso str. Add a CASE SEARCHABLE WHEN ... to convert str to correct int values */

	/* add a selection when a specific SQL data type is requested */
	if (nSqlDataType != SQL_ALL_TYPES) {
		/* add the selection condition */
		char tmp[30];
		snprintf(tmp, 30, "%d", nSqlDataType);
		strcat(query, " WHERE SQL_TYPE = ");
		strcat(query, tmp);
	}

	/* add the ordering (Note: only needed when all types are selected) */
	if (nSqlDataType == SQL_ALL_TYPES) {
		strcat(query, " ORDER BY SQL_TYPE");
	}

	/* query the MonetDb data dictionary tables */
	assert(query);
	rc = ExecDirect(hStmt, query, SQL_NTS);

	GDKfree(query);

	return rc;
}
