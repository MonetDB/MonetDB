/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "rel_proto_loader.h"
#include "rel_exp.h"
#include "gdk.h"	// COLnew(), BUNappend()
#include "gdk_time.h"	// date_create(), daytime_create(), timestamp_create()
#include "mal_exception.h"
#include "mal_builder.h"
#include "mal_client.h"

//#ifdef _MSC_VER
//#include <WTypes.h>
//#endif
//#include <stdint.h>
//#include <ctype.h>
//#include <wchar.h>

/**** Define the ODBC Version our ODBC application complies with ****/
#define ODBCVER 0x0352		/* Important: this must be defined before include of sql.h and sqlext.h */
#include <sql.h>
#include <sqlext.h>


#define ODBC_RELATION 1
#define ODBC_LOADER   2

#define QUERY_MAX_COLUMNS 4096
#define MAX_COL_NAME_LEN  1024
#define MAX_TBL_NAME_LEN  1024


/* map ODBC SQL datatype to MonetDB SQL datatype */
static sql_subtype *
map_rescol_type(SQLSMALLINT dataType, SQLULEN columnSize, SQLSMALLINT decimalDigits, mvc * sql)
{
	char * typenm;
	int interval_type = 0;

	switch (dataType) {
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
	default:	/* all other ODBC types are also mapped to varchar for now */
		/* all ODBC char datatypes are mapped to varchar. char and clob are internally not used anymore */
		return sql_bind_subtype(sql->sa, "varchar", (int) columnSize, 0);

	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
		return sql_bind_subtype(sql->sa, "blob", (int) columnSize, 0);

	case SQL_DECIMAL:
	case SQL_NUMERIC:
		/* columnSize contains the defined number of digits, so precision. */
		/* decimalDigits contains the scale (which can be negative). */
		return sql_bind_subtype(sql->sa, "decimal", (int) columnSize, (int) decimalDigits);

	case SQL_BIT:
		typenm = "boolean";
		break;

	case SQL_TINYINT:
		typenm = "tinyint";
		break;
	case SQL_SMALLINT:
		typenm = "smallint";
		break;
	case SQL_INTEGER:
		typenm = "int";
		break;
	case SQL_BIGINT:
		typenm = "bigint";
		break;

	case SQL_REAL:
		typenm = "real";
		break;
	case SQL_DOUBLE:
		typenm = "double";
		break;
	case SQL_FLOAT:
		/* the precision of SQL_FLOAT can be either 24 or 53:
		   if it is 24, the SQL_FLOAT data type is the same as SQL_REAL;
		   if it is 53, the SQL_FLOAT data type is the same as SQL_DOUBLE. */
		typenm = (columnSize == 7) ? "real" : "double";
		break;

	case SQL_TYPE_DATE:
		typenm = "date";
		break;
	case SQL_TYPE_TIME:
		/* decimalDigits contains the precision of fractions of a second */
		typenm = "time";
		break;
	case SQL_DATETIME:
	case SQL_TYPE_TIMESTAMP:
		/* decimalDigits contains the precision of fractions of a second */
		typenm = "timestamp";
		break;

	case SQL_INTERVAL_MONTH:
		typenm = "month_interval";
		interval_type = 3;
		break;
	case SQL_INTERVAL_YEAR:
		typenm = "month_interval";
		interval_type = 1;
		break;
	case SQL_INTERVAL_YEAR_TO_MONTH:
		typenm = "month_interval";
		interval_type = 2;
		break;
	case SQL_INTERVAL_DAY:
		typenm = "day_interval";
		interval_type = 4;
		break;
	case SQL_INTERVAL_HOUR:
		typenm = "sec_interval";
		interval_type = 8;
		break;
	case SQL_INTERVAL_MINUTE:
		typenm = "sec_interval";
		interval_type = 11;
		break;
	case SQL_INTERVAL_SECOND:
		typenm = "sec_interval";
		interval_type = 13;
		break;
	case SQL_INTERVAL_DAY_TO_HOUR:
		typenm = "sec_interval";
		interval_type = 5;
		break;
	case SQL_INTERVAL_DAY_TO_MINUTE:
		typenm = "sec_interval";
		interval_type = 6;
		break;
	case SQL_INTERVAL_DAY_TO_SECOND:
		typenm = "sec_interval";
		interval_type = 7;
		break;
	case SQL_INTERVAL_HOUR_TO_MINUTE:
		typenm = "sec_interval";
		interval_type = 9;
		break;
	case SQL_INTERVAL_HOUR_TO_SECOND:
		typenm = "sec_interval";
		interval_type = 10;
		break;
	case SQL_INTERVAL_MINUTE_TO_SECOND:
		typenm = "sec_interval";
		interval_type = 12;
		break;

	case SQL_GUID:
		/* represents a uuid of length 36, such as: dbe7343c-1f11-4fa9-a9c8-a31cd26f92fe */
		typenm = "uuid";
		break;
	}
	return sql_bind_subtype(sql->sa, typenm, interval_type, 0);
}

/* return atom type for ODBC SQL datatype. */
/* atom types are defined in gdk/gdh.h enum */
static int
map_rescol_mtype(SQLSMALLINT dataType, SQLULEN columnSize)
{
	switch (dataType) {
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
		return TYPE_str;
	case SQL_BIT:
		return TYPE_bit;
	case SQL_TINYINT:
		return TYPE_bte;
	case SQL_SMALLINT:
		return TYPE_sht;
	case SQL_INTEGER:
		return TYPE_int;
	case SQL_BIGINT:
		return TYPE_lng;
	case SQL_REAL:
		return TYPE_flt;
	case SQL_FLOAT:
		return (columnSize == 7) ? TYPE_flt : TYPE_dbl;
	case SQL_DOUBLE:
		return TYPE_dbl;
	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
		return TYPE_blob;
	case SQL_TYPE_DATE:
		return TYPE_date;
	case SQL_TYPE_TIME:
		return TYPE_daytime;
	case SQL_DATETIME:
	case SQL_TYPE_TIMESTAMP:
		return TYPE_timestamp;
	case SQL_GUID:
		return TYPE_uuid;
	case SQL_INTERVAL_MONTH:
	case SQL_INTERVAL_YEAR:
	case SQL_INTERVAL_YEAR_TO_MONTH:
		return TYPE_int;
	case SQL_INTERVAL_DAY:
	case SQL_INTERVAL_HOUR:
	case SQL_INTERVAL_MINUTE:
	case SQL_INTERVAL_SECOND:
	case SQL_INTERVAL_DAY_TO_HOUR:
	case SQL_INTERVAL_DAY_TO_MINUTE:
	case SQL_INTERVAL_DAY_TO_SECOND:
	case SQL_INTERVAL_HOUR_TO_MINUTE:
	case SQL_INTERVAL_HOUR_TO_SECOND:
	case SQL_INTERVAL_MINUTE_TO_SECOND:
		return TYPE_lng;

	case SQL_DECIMAL:
	case SQL_NUMERIC:
		// we will fetch decimals as string data
	default:
		return TYPE_str;
	}
}

/* return name for ODBC SQL datatype */
static char *
nameofSQLtype(SQLSMALLINT dataType)
{
	/* https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/sql-data-types */
	switch (dataType) {
	case SQL_CHAR:		return "CHAR";
	case SQL_VARCHAR:	return "VARCHAR";
	case SQL_LONGVARCHAR:	return "LONG VARCHAR";
	case SQL_WCHAR:		return "WCHAR";
	case SQL_WVARCHAR:	return "WVARCHAR";
	case SQL_WLONGVARCHAR:	return "WLONGVARCHAR";
	case SQL_DECIMAL:	return "DECIMAL";
	case SQL_NUMERIC:	return "NUMERIC";
	case SQL_SMALLINT:	return "SMALLINT";
	case SQL_INTEGER:	return "INTEGER";
	case SQL_REAL:		return "REAL";
	case SQL_FLOAT:		return "FLOAT";
	case SQL_DOUBLE:	return "DOUBLE";
	case SQL_BIT:		return "BIT";
	case SQL_TINYINT:	return "TINYINT";
	case SQL_BIGINT:	return "BIGINT";
	case SQL_BINARY:	return "BINARY";
	case SQL_VARBINARY:	return "VARBINARY";
	case SQL_LONGVARBINARY:	return "LONG VARBINARY";
	case SQL_DATETIME:	return "DATETIME";
	case SQL_TYPE_DATE:	return "DATE";
	case SQL_TYPE_TIME:	return "TIME";
	case SQL_TYPE_TIMESTAMP:	return "TIMESTAMP";
	case SQL_INTERVAL_MONTH:	return "INTERVAL MONTH";
	case SQL_INTERVAL_YEAR:		return "INTERVAL YEAR";
	case SQL_INTERVAL_YEAR_TO_MONTH: return "INTERVAL YEAR TO MONTH";
	case SQL_INTERVAL_DAY:		return "INTERVAL DAY";
	case SQL_INTERVAL_HOUR:		return "INTERVAL HOUR";
	case SQL_INTERVAL_MINUTE:	return "INTERVAL MINUTE";
	case SQL_INTERVAL_SECOND:	return "INTERVAL SECOND";
	case SQL_INTERVAL_DAY_TO_HOUR:	return "INTERVAL DAY TO HOUR";
	case SQL_INTERVAL_DAY_TO_MINUTE:	return "INTERVAL DAY TO MINUTE";
	case SQL_INTERVAL_DAY_TO_SECOND:	return "INTERVAL DAY TO SECOND";
	case SQL_INTERVAL_HOUR_TO_MINUTE:	return "INTERVAL HOUR TO MINUTE";
	case SQL_INTERVAL_HOUR_TO_SECOND:	return "INTERVAL HOUR TO SECOND";
	case SQL_INTERVAL_MINUTE_TO_SECOND:	return "INTERVAL MINUTE TO SECOND";
	case SQL_GUID:		return "GUID";
/*	case SQL_HUGEINT:	return "HUGEINT";	0x4000 (defined in ODBCGlobal.h) */
	default:		return "Driver specific type";
	}
}

/* utility function to safely close all opened ODBC resources */
static void
odbc_cleanup(SQLHANDLE env, SQLHANDLE dbc, SQLHANDLE stmt) {
	SQLRETURN ret = SQL_SUCCESS;

	if (stmt != SQL_NULL_HSTMT) {
		ret = SQLFreeStmt(stmt, SQL_CLOSE);
		if (ret != SQL_INVALID_HANDLE)
			SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	}
	if (dbc != SQL_NULL_HDBC) {
		ret = SQLDisconnect(dbc);
		if (ret != SQL_INVALID_HANDLE)
			SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	}
	if (env != SQL_NULL_HENV) {
		SQLFreeHandle(SQL_HANDLE_ENV, env);
	}
}

/* copied from monetdb5/modules/mal/tablet.c */
static BAT *
bat_create(int adt, BUN nr)
{
	BAT *b = COLnew(0, adt, nr, TRANSIENT);

	/* check for correct structures */
	if (b == NULL)
		return NULL;
	if ((b = BATsetaccess(b, BAT_APPEND)) == NULL) {
		return NULL;
	}

	/* disable all properties here */
	b->tsorted = false;
	b->trevsorted = false;
	b->tnosorted = 0;
	b->tnorevsorted = 0;
	b->tseqbase = oid_nil;
	b->tkey = false;
	b->tnokey[0] = 0;
	b->tnokey[1] = 0;
	return b;
}

/*
 * odbc_query() contains the logic for both odbc_relation() and ODBCloader()
 * the caller arg is 1 when called from odbc_relation and 2 when called from ODBCloader
 */
static str
odbc_query(mvc *sql, sql_subfunc *f, char *url, list *res_exps, MalStkPtr stk, InstrPtr pci, int caller)
{
	bool trace_enabled = false;	/* used for development only */

	if (sql == NULL)
		return "Missing mvc value.";
	if (f == NULL)
		return "Missing sql_subfunc value.";

	/* check received url and extract the ODBC connection string and the SQL query */
	if (!url || (url && strncasecmp("odbc:", url, 5) != 0))
		return "Invalid URI. Must start with 'odbc:'.";

	// skip 'odbc:' prefix from url so we get a connection string including the query
	char * con_str = &url[5];
	/* the connection string must start with 'DSN=' or 'DRIVER=' or 'FILEDSN='
	   else the ODBC driver manager can't load the ODBC driver */
	if (con_str
	  && (strncmp("DSN=", con_str, 4) != 0)
	  && (strncmp("DRIVER=", con_str, 7) != 0)
	  && (strncmp("FILEDSN=", con_str, 8) != 0))
		return "Invalid ODBC connection string. Should start with 'DSN=' or 'DRIVER=' or 'FILEDSN='.";

	// locate the 'QUERY=' part to extract the SQL query string to execute
	char * qry_str = strstr(con_str, "QUERY=");
	if (qry_str == NULL)
		return "Incomplete ODBC connection string. Missing 'QUERY=' part (to specify the SQL SELECT query to execute).";

	char * query = GDKstrdup(&qry_str[6]);	// we expect that QUERY= is at the end of the connection string

	// create a new ODBC connection string without the QUERY= part
	char * odbc_con_str = GDKstrndup(con_str, qry_str - con_str);

	// TODO convert con_str and qry_str from UTF-8 to UCS16, so we can use ODBC W functions

	// trace_enabled = true;
	if (trace_enabled)
		printf("\nExtracted ODBC connection string: %s\n  and SQL query: %s\n", odbc_con_str, query);

	/* now we can (try to) connect to the ODBC driver and execute the SQL query */
	SQLRETURN ret = SQL_INVALID_HANDLE;
	SQLHANDLE env = SQL_NULL_HENV;
	SQLHANDLE dbc = SQL_NULL_HDBC;
	SQLHANDLE stmt = SQL_NULL_HSTMT;
	char * errmsg = NULL;

	ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "Allocate ODBC ENV handle failed.";
		goto finish;
	}
	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) (uintptr_t) SQL_OV_ODBC3, 0);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3) failed.";
		goto finish;
	}

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "Allocate ODBC DBC handle failed.";
		goto finish;
	}
	/* to avoid an endless blocking SQLDriverConnect() set a login timeout of 8s */
	ret = SQLSetConnectAttr(dbc, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER) (uintptr_t) 8UL, 0);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "SQLSetConnectAttr (SQL_ATTR_LOGIN_TIMEOUT 8s) failed.";
		goto finish;
	}

	// TODO convert con_str from UTF-8 to UCS16, so we can use ODBC W functions
	SQLSMALLINT len = 0;
	ret = SQLDriverConnect(dbc, NULL, (SQLCHAR *) odbc_con_str, SQL_NTS, NULL, 0, &len, SQL_DRIVER_NOPROMPT);
	if (trace_enabled)
		printf("After SQLDriverConnect(%s) returned %d\n", odbc_con_str, ret);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "Could not connect. SQLDriverConnect failed.";
		goto finish;
	}

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "Allocate ODBC STMT handle failed.";
		goto finish;
	}

	// TODO convert qry_str from UTF-8 to UCS16, so we can use ODBC W functions
	ret = SQLExecDirect(stmt, (SQLCHAR *) query, SQL_NTS);
	if (trace_enabled)
		printf("After SQLExecDirect(%s) returned %d\n", query, ret);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "SQLExecDirect query failed.";
		goto finish;
	}

	SQLSMALLINT nr_cols = 0;
	ret = SQLNumResultCols(stmt, &nr_cols);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "SQLNumResultCols failed.";
		goto finish;
	}
	if (nr_cols <= 0) {
		errmsg = "ODBC query did not return a resultset.";
		goto finish;
	}
	if (trace_enabled)
		printf("Query has %d result columns\n", nr_cols);
	if (nr_cols > QUERY_MAX_COLUMNS) {
		/* limit the number of data columns, as we do not want to block or blow up the mserver */
		nr_cols = QUERY_MAX_COLUMNS;
		printf("\nODBC_loader limited Query result to first %d columns.\n", nr_cols);
	}

	/* when called from odbc_relation() */
	if (caller == ODBC_RELATION) {
		char tname[MAX_TBL_NAME_LEN];
		char cname[MAX_COL_NAME_LEN];
		sql_alias * tblname;
		char * colname;
		SQLSMALLINT dataType = 0;
		SQLULEN columnSize = 0;
		SQLSMALLINT decimalDigits = 0;
		sql_subtype * sql_mtype;
		list * typelist = sa_list(sql->sa);
		list * nameslist = sa_list(sql->sa);
		for (SQLUSMALLINT col = 1; col <= (SQLUSMALLINT) nr_cols; col++) {
			/* for each result column get name, datatype, size and decdigits */
			// TODO use ODBC W function
			ret = SQLDescribeCol(stmt, col, (SQLCHAR *) cname, (SQLSMALLINT) sizeof(cname) -1,
					NULL, &dataType, &columnSize, &decimalDigits, NULL);
			if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
				errmsg = "SQLDescribeCol failed.";
				goto finish;
			}
			if (trace_enabled)
				printf("ResCol %u, name: %s, type %d (%s), size %u, decdigits %d\n",
					col, cname, dataType, nameofSQLtype(dataType), (unsigned int)columnSize, decimalDigits);
			colname = sa_strdup(sql->sa, cname);
			list_append(nameslist, colname);
			sql_mtype = map_rescol_type(dataType, columnSize, decimalDigits, sql);
			list_append(typelist, sql_mtype);

			if (res_exps) {
				/* also get the table name for this result column */
				// TODO use ODBC W function
				ret = SQLColAttribute(stmt, col, SQL_DESC_TABLE_NAME, (SQLPOINTER) tname, (SQLSMALLINT) sizeof(tname) -1, NULL, NULL);
				if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
					strcpy(tname, "");
				}
				tblname = a_create(sql->sa, tname);
				sql_exp *ne = exp_column(sql->sa, tblname, colname, sql_mtype, CARD_MULTI, 1, 0, 0);
				set_basecol(ne);
				ne->alias.label = -(sql->nid++);
				list_append(res_exps, ne);
			}
		}

		f->tname = sa_strdup(sql->sa, tname);
		f->colnames = nameslist;
		f->coltypes = typelist;
		f->res = typelist;
		goto finish;
	}

	/* when called from ODBCloader() */
	if (caller == ODBC_LOADER) {
		BAT ** bats = (BAT **) GDKzalloc(nr_cols * sizeof(BAT *));
		if (bats == NULL) {
			errmsg = "GDKmalloc bats failed.";
			goto finish;
		}
		int * mtypes = (int *) GDKzalloc(nr_cols * sizeof(int));
		if (mtypes == NULL) {
			errmsg = "GDKmalloc mtypes failed.";
			GDKfree(bats);
			bats = NULL;
			goto finish;
		}
		SQLULEN largestStringSize = 0;
		bool hasBlobCols = false;
		SQLULEN largestBlobSize = 0;
		/* make bats with right atom type */
		for (SQLUSMALLINT col = 0; col < (SQLUSMALLINT) nr_cols; col++) {
			char cname[MAX_COL_NAME_LEN];
			SQLSMALLINT dataType = 0;
			SQLULEN columnSize = 0;
			SQLSMALLINT decimalDigits = 0;
			int mtype = TYPE_str;
			BAT * b = NULL;

			/* for each result column get SQL datatype, size and decdigits */
			// TODO use ODBC W function
			ret = SQLDescribeCol(stmt, col+1, (SQLCHAR *) cname, (SQLSMALLINT) sizeof(cname) -1,
					NULL, &dataType, &columnSize, &decimalDigits, NULL);
			if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
				errmsg = "SQLDescribeCol failed.";
				/* cleanup already created bats */
				while (col > 0) {
					col--;
					BBPreclaim(bats[col]);
					bats[col] = NULL;
				}
				GDKfree(bats);
				GDKfree(mtypes);
				goto finish;
			}
			mtype = map_rescol_mtype(dataType, columnSize);
			mtypes[col] = mtype;
			if (mtype == TYPE_str) {
				if (columnSize > largestStringSize) {
					largestStringSize = columnSize;
				}
			}
			if (mtype == TYPE_blob) {
				hasBlobCols = true;
				if (columnSize > largestBlobSize) {
					largestBlobSize = columnSize;
				}
			}
			if (trace_enabled)
				printf("ResCol %u, name: %s, type %d (%s), size %u, decdigits %d, atomtype %d\n",
					col+1, cname, dataType, nameofSQLtype(dataType), (unsigned int)columnSize, decimalDigits, mtype);

			if (trace_enabled)
				printf("Before create BAT %d\n", col+1);
			b = bat_create(mtype, 0);
			if (b) {
				bats[col] = b;
				if (trace_enabled)
					printf("After create BAT %d\n", col+1);
			} else {
				errmsg = "Failed to create bat.";
				/* cleanup already created bats */
				while (col > 0) {
					col--;
					BBPreclaim(bats[col]);
					bats[col] = NULL;
				}
				GDKfree(bats);
				GDKfree(mtypes);
				goto finish;
			}
		}

		/* allocate storage for all the fixed size atom types. TODO num_val for decimals and numeric data */
		bit bit_val;
		bte bte_val;
		sht sht_val;
		int int_val;
		lng lng_val;
		flt flt_val;
		dbl dbl_val;
		DATE_STRUCT date_val;
		TIME_STRUCT time_val;
		TIMESTAMP_STRUCT ts_val;
//		SQL_NUMERIC_STRUCT num_val;
		SQLGUID guid_val;

		/* allocate storage for all the var sized atom types. */
		char * str_val = NULL;		// TODO: change to wchar
		if (largestStringSize == 0)	// no valid string length, use 65535 (64kB) as default
			largestStringSize = 65535;
		if (largestStringSize > 16777215) // string length too large, limit to 16MB
			largestStringSize = 16777215;
		str_val = (char *)GDKzalloc((largestStringSize +1) * sizeof(char));	// +1 for the eos char
		if (!str_val) {
			errmsg = "Failed to alloc memory for largest rescol string.";
			goto finish_fetch;
		}
		if (trace_enabled)
			printf("Allocated str_val buffer of size %zu\n", (largestStringSize +1) * sizeof(char));

		bte * blob_val = NULL;
		if (hasBlobCols) {
			if (largestBlobSize == 0)	// no valid blob length, assume 65536 (64kB) as default
				largestBlobSize = 65532;
			if (largestBlobSize > 16777212) // blob length too large, limit to 16MB
				largestBlobSize = 16777212;
			blob_val = (bte *)GDKzalloc(sizeof(unsigned int) + (largestBlobSize * sizeof(bte)));
			if (!blob_val) {
				errmsg = "Failed to alloc memory for largest rescol blob.";
				goto finish_fetch;
			}
			if (trace_enabled)
				printf("Allocated blob_val buffer of size %zu\n", largestBlobSize * sizeof(bte));
		}

		unsigned long row = 0;
		ret = SQLFetch(stmt);	// TODO optimisation: use SQLExtendedFetch() to pull data array wise and use BUNappendmulti()
		while (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
			row++;
			if (trace_enabled)
				printf("Fetched row %lu\n", row);

			for (SQLUSMALLINT col = 0; col < (SQLUSMALLINT) nr_cols; col++) {
				int mtype = mtypes[col];
				BAT * b = bats[col];
				if (!b)
					continue;

				SQLSMALLINT targetType;
				SQLPOINTER * targetValuePtr;
				SQLLEN bufferLength = 0;
				SQLLEN strLen = 0;
				/* mapping based on https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/c-data-types */
				switch(mtype) {
					case TYPE_str:
					default:
						targetType = SQL_C_CHAR;	// TODO later: SQL_C_WCHAR
						targetValuePtr = (SQLPOINTER *) str_val;
						bufferLength = largestStringSize;
						break;
					case TYPE_bit:
						targetType = SQL_C_BIT;
						targetValuePtr = (SQLPOINTER *) &bit_val;
						break;
					case TYPE_bte:
						targetType = SQL_C_STINYINT;
						targetValuePtr = (SQLPOINTER *) &bte_val;
						break;
					case TYPE_sht:
						targetType = SQL_C_SSHORT;
						targetValuePtr = (SQLPOINTER *) &sht_val;
						break;
					case TYPE_int:
						targetType = SQL_C_SLONG;
						targetValuePtr = (SQLPOINTER *) &int_val;
						break;
					case TYPE_lng:
						targetType = SQL_C_SBIGINT;
						targetValuePtr = (SQLPOINTER *) &lng_val;
						break;
					case TYPE_flt:
						targetType = SQL_C_FLOAT;
						targetValuePtr = (SQLPOINTER *) &flt_val;
						break;
					case TYPE_dbl:
						targetType = SQL_C_DOUBLE;
						targetValuePtr = (SQLPOINTER *) &dbl_val;
						break;
					case TYPE_date:
						targetType = SQL_C_TYPE_DATE;
						targetValuePtr = (SQLPOINTER *) &date_val;
						break;
					case TYPE_daytime:
						targetType = SQL_C_TYPE_TIME;
						targetValuePtr = (SQLPOINTER *) &time_val;
						break;
					case TYPE_timestamp:
						targetType = SQL_C_TYPE_TIMESTAMP;
						targetValuePtr = (SQLPOINTER *) &ts_val;
						break;
					case TYPE_uuid:
						targetType = SQL_C_GUID;
						targetValuePtr = (SQLPOINTER *) &guid_val;
						break;
					case TYPE_blob:
						targetType = SQL_C_BINARY;
						targetValuePtr = (SQLPOINTER *) &blob_val;
						bufferLength = largestBlobSize;
						break;
				}
				ret = SQLGetData(stmt, col+1, targetType, targetValuePtr, bufferLength, &strLen);
				if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
					if (trace_enabled)
						printf("Failed to get data for col %u of row %lu\n", col+1, row);
					if (BUNappend(b, (void *) NULL, false) != GDK_SUCCEED)
						if (trace_enabled)
							printf("BUNappend(b, NULL, false) failed\n");
				} else {
					if (strLen == SQL_NULL_DATA) {
						if (trace_enabled)
							printf("Data row %lu col %u: NULL\n", row, col+1);
						if (BUNappend(b, ATOMnilptr(b->ttype), false) != GDK_SUCCEED)
							if (trace_enabled)
								printf("BUNappend(b, ATOMnilptr(b->ttype), false) failed\n");
					} else {
						gdk_return gdkret = GDK_SUCCEED;
						switch(mtype) {
							case TYPE_str:
							default:
								if (strLen != SQL_NTS && strLen >= 0) {
									/* make sure it is a Nul Terminated String */
									if ((SQLULEN) strLen < largestStringSize) {
										if (str_val[strLen] != '\0')
											str_val[strLen] = '\0';
									} else {
										if (str_val[largestStringSize] != '\0')
											str_val[largestStringSize] = '\0';
									}
								}
								if (trace_enabled)
									printf("Data row %lu col %u: %s\n", row, col+1, str_val);
								gdkret = BUNappend(b, (void *) str_val, false);
								break;
							case TYPE_bit:
								if (trace_enabled)
									printf("Data row %lu col %u: %c\n", row, col+1, bit_val);
								gdkret = BUNappend(b, (void *) &bit_val, false);
								break;
							case TYPE_bte:
								if (trace_enabled)
									printf("Data row %lu col %u: %c\n", row, col+1, bte_val);
								gdkret = BUNappend(b, (void *) &bte_val, false);
								break;
							case TYPE_sht:
								if (trace_enabled)
									printf("Data row %lu col %u: %hd\n", row, col+1, sht_val);
								gdkret = BUNappend(b, (void *) &sht_val, false);
								break;
							case TYPE_int:
								if (trace_enabled)
									printf("Data row %lu col %u: %d\n", row, col+1, int_val);
								gdkret = BUNappend(b, (void *) &int_val, false);
								break;
							case TYPE_lng:
								if (trace_enabled)
									printf("Data row %lu col %u: %" PRId64 "\n", row, col+1, lng_val);
								gdkret = BUNappend(b, (void *) &lng_val, false);
								break;
							case TYPE_flt:
								if (trace_enabled)
									printf("Data row %lu col %u: %f\n", row, col+1, flt_val);
								gdkret = BUNappend(b, (void *) &flt_val, false);
								break;
							case TYPE_dbl:
								if (trace_enabled)
									printf("Data row %lu col %u: %f\n", row, col+1, dbl_val);
								gdkret = BUNappend(b, (void *) &dbl_val, false);
								break;
							case TYPE_date:
							{
								date mdate_val = date_create(date_val.year, date_val.month, date_val.day);
								if (trace_enabled)
									printf("Data row %lu col %u: date(%04d-%02u-%02u)\n", row, col+1, date_val.year, date_val.month, date_val.day);
								gdkret = BUNappend(b, (void *) &mdate_val, false);
								break;
							}
							case TYPE_daytime:
							{
								daytime daytime_val = daytime_create(time_val.hour, time_val.minute, time_val.second, 0);
								if (trace_enabled)
									printf("Data row %lu col %u: daytime(%02u:%02u:%02u)\n", row, col+1, time_val.hour, time_val.minute, time_val.second);
								gdkret = BUNappend(b, (void *) &daytime_val, false);
								break;
							}
							case TYPE_timestamp:
							{
								date mdate_val = date_create(ts_val.year, ts_val.month, ts_val.day);
								daytime daytime_val = daytime_create(ts_val.hour, ts_val.minute, ts_val.second, ts_val.fraction);
								timestamp timestamp_val = timestamp_create(mdate_val, daytime_val);
								if (trace_enabled)
									printf("Data row %lu col %u: timestamp(%04d-%02u-%02u %02u:%02u:%02u.%06u)\n", row, col+1,
										ts_val.year, ts_val.month, ts_val.day, ts_val.hour, ts_val.minute, ts_val.second, ts_val.fraction);
								gdkret = BUNappend(b, (void *) &timestamp_val, false);
								break;
							}
							case TYPE_uuid:
							{
								if (trace_enabled)
									printf("Data row %lu col %u: guid_val\n", row, col+1);
								// uuid is 16 bytes, same as SQLGUID guid_val
								gdkret = BUNappend(b, (void *) &guid_val, false);
								break;
							}
							case TYPE_blob:
							{
								//TODO convert blob_val to blob struct which starts with length (4 bytes) and next bytes.
								if (trace_enabled)
									printf("Data row %lu col %u: blob_val\n", row, col+1);
								// gdkret = BUNappend(b, (void *) blob_val, false);
								break;
							}
						}
						if (gdkret != GDK_SUCCEED)
							if (trace_enabled)
								printf("BUNappend(b, val, false) failed\n");
					}
				}
			}
			ret = SQLFetch(stmt);	// get data of next row
		}
		/* the last SQLFetch() will return SQL_NO_DATA at end, treat it as success */
		if (ret == SQL_NO_DATA)
			ret = SQL_SUCCESS;	// we retrieved all rows

  finish_fetch:
		/* pass bats to caller */
		if (bats) {
			for (int col = 0; col < (int) nr_cols; col++) {
				bat * rescol = getArgReference_bat(stk, pci, col);
				BAT * b = bats[col];
				if (rescol && b) {
					*rescol = b->batCacheid;
					BBPkeepref(b);
				}
			}
			/* free locally allocated memory */
			GDKfree(bats);
		}
		if (mtypes)
			GDKfree(mtypes);
		if (str_val)
			GDKfree(str_val);
		if (blob_val)
			GDKfree(blob_val);
	}

  finish:
	if (query)
		GDKfree(query);
	if (odbc_con_str)
		GDKfree(odbc_con_str);

	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		/* an ODBC function call returned an error, get the error msg from the ODBC driver */
		SQLSMALLINT handleType;
		SQLHANDLE handle;
		SQLCHAR state[SQL_SQLSTATE_SIZE +1];
		SQLINTEGER errnr;
		SQLCHAR msg[2048];
		SQLSMALLINT msglen;

		/* get err message(s) from the right handle */
		if (stmt != SQL_NULL_HSTMT) {
			handleType = SQL_HANDLE_STMT;
			handle = stmt;
		} else
		if (dbc != SQL_NULL_HDBC) {
			handleType = SQL_HANDLE_DBC;
			handle = dbc;
		} else {
			handleType = SQL_HANDLE_ENV;
			handle = env;
		}
		// TODO use ODBC W function
		ret = SQLGetDiagRec(handleType, handle, 1, state, &errnr, msg, (sizeof(msg) -1), &msglen);
		if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
			str retmsg;
			if (state[SQL_SQLSTATE_SIZE] != '\0')
				state[SQL_SQLSTATE_SIZE] = '\0';
			if (errmsg != NULL) {
				retmsg = sa_message(sql->sa, "odbc_loader" " %s SQLstate %s, Errnr %d, Message %s", errmsg, (char*)state, errnr, (char*)msg);
			} else {
				retmsg = sa_message(sql->sa, "odbc_loader" " SQLstate %s, Errnr %d, Message %s", (char*)state, errnr, (char*)msg);
			}
			odbc_cleanup(env, dbc, stmt);
			return retmsg;
		}
	}
	odbc_cleanup(env, dbc, stmt);
	return (errmsg != NULL) ? (str)errmsg : MAL_SUCCEED;
}

/*
 * returns an error string (static or via tmp sa_allocator allocated), NULL on success
 *
 * Extend the subfunc f with result columns, ie.
	f->res = typelist;
	f->coltypes = typelist;
	f->colnames = nameslist; use tname if passed, for the relation name
 * Fill the list res_exps, with one result expressions per resulting column.
 */
static str
odbc_relation(mvc *sql, sql_subfunc *f, char *url, list *res_exps, char *aname)
{
	(void) aname;
	return odbc_query(sql, f, url, res_exps, NULL, NULL, ODBC_RELATION);
}

static void *
odbc_load(void *BE, sql_subfunc *f, char *url, sql_exp *topn)
{
	backend *be = (backend*)BE;
	if (!f)
		return NULL;

	(void)topn;

	InstrPtr q = newStmtArgs(be->mb, "odbc", "loader", list_length(f->coltypes) + 2);
	int col = 0;
	list *l = sa_list(be->mvc->sa);
	sql_alias *ta = a_create(be->mvc->sa, f->tname);
	for (node *n = f->coltypes->h, *nn = f->colnames->h; n && nn; col++, n = n->next, nn = nn->next) {
		const char *name = nn->data;
		sql_subtype *tp = n->data;
		int type = newBatType(tp->type->localtype);
		if (col)
			q = pushReturn(be->mb, q, newTmpVariable(be->mb, type));
		else
			getArg(q, 0) = newTmpVariable(be->mb, type);
		stmt *s = stmt_blackbox_result(be, q, col, tp);
		s = stmt_alias(be, s, col+1, ta, name);
		list_append(l, s);
	}
	q = pushStr(be->mb, q, url);
	q = pushPtr(be->mb, q, f);
	pushInstruction(be->mb, q);
	return stmt_list(be, l);
}

static str
ODBCloader(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *be = NULL;
	str msg;
	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	(void)mb;
	str uri = *getArgReference_str(stk, pci, pci->retc);
	sql_subfunc *f = *(sql_subfunc**)getArgReference_ptr(stk, pci, pci->retc+1);

	return odbc_query(be->mvc, f, uri, NULL, stk, pci, ODBC_LOADER);
	//return MAL_SUCCEED;
}

static str
ODBCprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt; (void)mb; (void)stk; (void)pci;
	pl_register("odbc", &odbc_relation, &odbc_load);
	return MAL_SUCCEED;
}

static str
ODBCepilogue(void *ret)
{
	(void)ret;
	pl_unregister("odbc");
	return MAL_SUCCEED;
}

#include "sql_scenario.h"
#include "mel.h"

static mel_func odbc_init_funcs[] = {
	pattern("odbc", "prelude", ODBCprelude, false, "", noargs),
	command("odbc", "epilogue", ODBCepilogue, false, "", noargs),
	pattern("odbc", "loader", ODBCloader, true, "Import a query result via the odbc uri", args(1,3, batvarargany("",0),arg("uri",str),arg("func",ptr))),
{ .imp=NULL }
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_odbc_mal)
{ mal_module("odbc", NULL, odbc_init_funcs); }

