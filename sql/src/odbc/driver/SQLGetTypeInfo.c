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


static SQLRETURN
SQLGetTypeInfo_(ODBCStmt *stmt, SQLSMALLINT nSqlDataType)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;

	/* check statement cursor state, no query should be prepared or executed */
	if (stmt->State == EXECUTED) {
		/* 24000 = Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);

		return SQL_ERROR;
	}

	switch (nSqlDataType) {
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

	/* some pre ODBC 3.0 data types which can be mapped to ODBC
	   3.0 data types */
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
	query = (char *) malloc(1200);
	assert(query);

	/* SQLGetTypeInfo returns a table with the following columns:
	   VARCHAR	type_name NOT NULL
	   SMALLINT	data_type NOT NULL
	   INTEGER	column_size
	   VARCHAR	literal_prefix
	   VARCHAR	literal_suffix
	   VARCHAR	create_params
	   SMALLINT	nullable NOT NULL
	   SMALLINT	case_sensitive NOT NULL
	   SMALLINT	searchable NOT NULL
	   SMALLINT	unsigned_attribute
	   SMALLINT	fixed_prec_scale NOT NULL
	   SMALLINT	auto_unique_value
	   VARCHAR	local_type_name
	   SMALLINT	minimum_scale
	   SMALLINT	maximum_scale
	   SMALLINT	sql_data_type NOT NULL
	   SMALLINT	sql_datetime_sub
	   SMALLINT	num_prec_radix
	   SMALLINT	interval_precision
	*/
	strcpy(query,
	       "select "
	       "cast(sqlname as varchar) as type_name, "
	       "cast(systemname as smallint) as data_type, "
	       "cast(digits as integer) as column_size, "
	       "cast('' as varchar) /*literal_prefix*/ as literal_prefix, "
	       "cast('' as varchar) /*literal_suffix*/ as literal_suffix, "
	       "cast('' as varchar) /*create_params*/ as create_params, "
	       "cast(1 as smallint) /*sql_nullable*/ as nullable, "
	       "cast(0 as smallint) /*case_sensitive*/ as case_sensitive, "
	       "cast(0 as smallint) /*searchable*/ as searchable, "
	       "cast(0 as smallint) /*unsigned*/ as unsigned_attribute, "
	       "cast(scale as smallint) /*fixed_prec_scale*/ as fixed_prec_scale, "
	       "cast(0 as smallint) /*auto_unique_vale*/ as auto_unique_vale, "
	       "cast(systemname as varchar) /*monet_type_name*/ as local_type_name, "
	       "cast(0 as smallint) /*min_scale*/ as minimum_scale, "
	       "cast(scale as smallint) /*max_scale*/ as maximum_scale, "
	       "cast(0 as smallint) /*sql_type*/ as sql_data_type, "
	       "cast(null as smallint) as sql_datetime_sub, "
	       "cast(10 as smallint) as num_prec_radix, "
	       "cast(0 as smallint) /*interval_prec*/ as interval_precision "
	       "from types");

/* TODO: SEARCHABLE should return an int iso str. Add a CASE
   SEARCHABLE WHEN ... to convert str to correct int values */

/* SQL_TYPE does not exist in table */
	if (nSqlDataType != SQL_ALL_TYPES) {
		/* add a selection when a specific SQL data type is
		   requested */
		char *tmp = query + strlen(query);

		snprintf(tmp, 30, " where sql_type = %d", nSqlDataType);
	} else {
		/* add the ordering (only needed when all types are
		   selected) */
		strcat(query, " order by sql_data_type");
	}

	/* query the MonetDb data dictionary tables */
	assert(query);
	rc = SQLExecDirect_(stmt, (SQLCHAR *) query, SQL_NTS);

	free(query);

	return rc;
}

SQLRETURN SQL_API
SQLGetTypeInfo(SQLHSTMT hStmt, SQLSMALLINT nSqlDataType)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetTypeInfo " PTRFMT " %d\n",
		PTRFMTCAST hStmt, nSqlDataType);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLGetTypeInfo_(stmt, nSqlDataType);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLGetTypeInfoW(SQLHSTMT hStmt, SQLSMALLINT nSqlDataType)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetTypeInfoW " PTRFMT " %d\n",
		PTRFMTCAST hStmt, nSqlDataType);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLGetTypeInfo_(stmt, nSqlDataType);
}
#endif	/* WITH_WCHAR */
