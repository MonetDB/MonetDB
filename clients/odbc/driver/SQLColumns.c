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
 * SQLColumns()
 * CLI Compliance: X/Open
 *
 * Note: catalogs are not supported, we ignore any value set for szCatalogName
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

#define NCOLUMNS	18

static SQLRETURN
SQLColumns_(ODBCStmt *stmt,
	    SQLCHAR *CatalogName,
	    SQLSMALLINT NameLength1,
	    SQLCHAR *SchemaName,
	    SQLSMALLINT NameLength2,
	    SQLCHAR *TableName,
	    SQLSMALLINT NameLength3,
	    SQLCHAR *ColumnName,
	    SQLSMALLINT NameLength4)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end = NULL;

	/* null pointers not allowed if arguments are identifiers */
	if (stmt->Dbc->sql_attr_metadata_id == SQL_TRUE &&
	    (SchemaName == NULL || TableName == NULL || ColumnName == NULL)) {
		addStmtError(stmt, "HY090", NULL, 0);
		return SQL_ERROR;
	}

	fixODBCstring(CatalogName, NameLength1, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(SchemaName, NameLength2, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(TableName, NameLength3, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(ColumnName, NameLength4, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG(" \"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) NameLength1, (char *) CatalogName,
		(int) NameLength2, (char *) SchemaName,
		(int) NameLength3, (char *) TableName,
		(int) NameLength4, (char *) ColumnName);
#endif

	/* construct the query now */
	query = (char *) malloc(12000 + NameLength2 + NameLength3 + NameLength4);
	assert(query);
	query_end = query;

	/* SQLColumns returns a table with the following columns:
	   VARCHAR      table_cat
	   VARCHAR      table_schem
	   VARCHAR      table_name NOT NULL
	   VARCHAR      column_name NOT NULL
	   SMALLINT     data_type NOT NULL
	   VARCHAR      type_name NOT NULL
	   INTEGER      column_size
	   INTEGER      buffer_length
	   SMALLINT     decimal_digits
	   SMALLINT     num_prec_radix
	   SMALLINT     nullable NOT NULL
	   VARCHAR      remarks
	   VARCHAR      column_def
	   SMALLINT     sql_data_type NOT NULL
	   SMALLINT     sql_datetime_sub
	   INTEGER      char_octet_length
	   INTEGER      ordinal_position NOT NULL
	   VARCHAR      is_nullable
	 */

	sprintf(query_end,
		"select cast(NULL as varchar(1)) as table_cat,"
		"       s.\"name\" as table_schem,"
		"       t.\"name\" as table_name,"
		"       c.\"name\" as column_name,"
		"       case c.\"type\""
		"            when 'bigint' then %d"
		"            when 'blob' then %d"
		"            when 'boolean' then %d"
		"            when 'char' then %d"
		"            when 'clob' then %d"
		"            when 'date' then %d"
		"            when 'decimal' then %d"
		"            when 'double' then %d"
		"            when 'int' then %d"
		"            when 'month_interval' then"
		"                 case c.type_digits"
		"                      when 1 then %d"
		"                      when 2 then %d"
		"                      when 3 then %d"
		"                 end"
		"            when 'real' then %d"
		"            when 'sec_interval' then"
		"                 case c.type_digits"
		"                      when 4 then %d"
		"                      when 5 then %d"
		"                      when 6 then %d"
		"                      when 7 then %d"
		"                      when 8 then %d"
		"                      when 9 then %d"
		"                      when 10 then %d"
		"                      when 11 then %d"
		"                      when 12 then %d"
		"                      when 13 then %d"
		"                 end"
		"            when 'smallint' then %d"
		"            when 'timestamp' then %d"
		"            when 'timestamptz' then %d"
		"            when 'time' then %d"
		"            when 'timetz' then %d"
		"            when 'tinyint' then %d"
		"            when 'varchar' then %d"
		"       end as data_type,"
		"       case c.\"type\""
		"            when 'bigint' then 'BIGINT'"
		"            when 'blob' then 'BINARY LARGE OBJECT'"
		"            when 'boolean' then 'BOOLEAN'"
		"            when 'char' then 'CHARACTER'"
		"            when 'clob' then 'CHARACTER LARGE OBJECT'"
		"            when 'date' then 'DATE'"
		"            when 'decimal' then 'DECIMAL'"
		"            when 'double' then 'DOUBLE'"
		"            when 'int' then 'INTEGER'"
		"            when 'month_interval' then"
		"                 case c.type_digits"
		"                      when 1 then 'INTERVAL YEAR'"
		"                      when 2 then 'INTERVAL YEAR TO MONTH'"
		"                      when 3 then 'INTERVAL MONTH'"
		"                 end"
		"            when 'real' then 'REAL'"
		"            when 'sec_interval' then"
		"                 case c.type_digits"
		"                      when 4 then 'INTERVAL DAY'"
		"                      when 5 then 'INTERVAL DAY TO HOUR'"
		"                      when 6 then 'INTERVAL DAY TO MINUTE'"
		"                      when 7 then 'INTERVAL DAY TO SECOND'"
		"                      when 8 then 'INTERVAL HOUR'"
		"                      when 9 then 'INTERVAL HOUR TO MINUTE'"
		"                      when 10 then 'INTERVAL HOUR TO SECOND'"
		"                      when 11 then 'INTERVAL MINUTE'"
		"                      when 12 then 'INTERVAL MINUTE TO SECOND'"
		"                      when 13 then 'INTERVAL SECOND'"
		"                 end"
		"            when 'smallint' then 'SMALLINT'"
		"            when 'timestamp' then 'TIMESTAMP'"
		"            when 'timestamptz' then 'TIMESTAMP'"
		"            when 'time' then 'TIME'"
		"            when 'timetz' then 'TIME'"
		"            when 'tinyint' then 'TINYINT'"
		"            when 'varchar' then 'VARCHAR'"
		"       end as type_name,"
		"       case c.\"type\""
		"            when 'month_interval' then"
		"                 case c.type_digits"
		"                      when 1 then 26"
		"                      when 2 then 38"
		"                      when 3 then 27"
		"                 end"
		"            when 'sec_interval' then"
		"                 case c.type_digits"
		"                      when 4 then 25"
		"                      when 5 then 36"
		"                      when 6 then 41"
		"                      when 7 then 47"
		"                      when 8 then 26"
		"                      when 9 then 39"
		"                      when 10 then 45"
		"                      when 11 then 28"
		"                      when 12 then 44"
		"                      when 13 then 30"
		"                 end"
		"            when 'date' then 10"
		"            when 'time' then 12"
		"            when 'timetz' then 12"
		"            when 'timestamp' then 23"
		"            when 'timestamptz' then 23"
		"            else c.type_digits"
		"       end as column_size,"
		"       case c.\"type\""
		"            when 'month_interval' then"
		"                 case c.type_digits"
		"                      when 1 then 26"
		"                      when 2 then 38"
		"                      when 3 then 27"
		"                 end"
		"            when 'sec_interval' then"
		"                 case c.type_digits"
		"                      when 4 then 25"
		"                      when 5 then 36"
		"                      when 6 then 41"
		"                      when 7 then 47"
		"                      when 8 then 26"
		"                      when 9 then 39"
		"                      when 10 then 45"
		"                      when 11 then 28"
		"                      when 12 then 44"
		"                      when 13 then 30"
		"                 end"
		"            when 'date' then 10"
		"            when 'time' then 12"
		"            when 'timetz' then 12"
		"            when 'timestamp' then 23"
		"            when 'timestamptz' then 23"
		"            when 'bigint' then 20"
		"            when 'int' then 11"
		"            when 'smallint' then 6"
		"            when 'tinyint' then 4"
		"            when 'char' then 6 * c.type_digits"
		"            when 'varchar' then 6 * c.type_digits"
		"            when 'double' then 24"
		"            when 'real' then 14"
		"            else c.type_digits"
		"       end as buffer_length,"
		"       case c.\"type\""
		"            when 'time' then c.type_digits - 1"
		"            when 'timetz' then c.type_digits - 1"
		"            when 'timestamp' then c.type_digits - 1"
		"            when 'timestamptz' then c.type_digits - 1"
		"            when 'sec_interval' then 0"
		"            when 'month_interval' then 0"
		"            when 'real' then"
		"                 case when c.type_digits = 24 and c.type_scale = 0 then 7"
		"                 else c.type_digits"
		"                 end"
		"            when 'double' then"
		"                 case when c.type_digits = 53 and c.type_scale = 0 then 15"
		"                 else c.type_digits"
		"                 end"
		"            when 'decimal' then c.type_digits"
		"            when 'bigint' then 19"
		"            when 'int' then 10"
		"            when 'smallint' then 5"
		"            when 'tinyint' then 3"
		"       end as decimal_digits,"
		"       case c.\"type\""
		"            when 'double' then"
		"                 case when c.type_digits = 53 and c.type_scale = 0 then 2"
		"                 else 10"
		"                 end"
		"            when 'real' then"
		"                 case when c.type_digits = 24 and c.type_scale = 0 then 2"
		"                 else 10"
		"                 end"
		"            when 'bigint' then 2"
		"            when 'int' then 2"
		"            when 'smallint' then 2"
		"            when 'tinyint' then 2"
		"            when 'decimal' then 10"
		"            else cast(null as smallint)"
		"       end as num_prec_radix,"
		"       case c.\"null\""
		"            when true then cast(%d as smallint)"
		"            when false then cast(%d as smallint)"
		"       end as nullable,"
		"       cast('' as varchar(1)) as remarks,"
		"       c.\"default\" as column_def,"
		"       case c.\"type\""
		"            when 'bigint' then %d"
		"            when 'blob' then %d"
		"            when 'boolean' then %d"
		"            when 'char' then %d"
		"            when 'clob' then %d"
		"            when 'date' then %d"
		"            when 'decimal' then %d"
		"            when 'double' then %d"
		"            when 'int' then %d"
		"            when 'month_interval' then %d"
		"            when 'real' then %d"
		"            when 'sec_interval' then %d"
		"            when 'smallint' then %d"
		"            when 'timestamp' then %d"
		"            when 'timestamptz' then %d"
		"            when 'time' then %d"
		"            when 'timetz' then %d"
		"            when 'tinyint' then %d"
		"            when 'varchar' then %d"
		"       end as sql_data_type,"
		"       case c.\"type\""
		"            when 'date' then %d"
		"            when 'month_interval' then"
		"                 case c.type_digits"
		"                      when 1 then %d"
		"                      when 2 then %d"
		"                      when 3 then %d"
		"                 end"
		"            when 'sec_interval' then"
		"                 case c.type_digits"
		"                      when 4 then %d"
		"                      when 5 then %d"
		"                      when 6 then %d"
		"                      when 7 then %d"
		"                      when 8 then %d"
		"                      when 9 then %d"
		"                      when 10 then %d"
		"                      when 11 then %d"
		"                      when 12 then %d"
		"                      when 13 then %d"
		"                 end"
		"            when 'timestamp' then %d"
		"            when 'timestamptz' then %d"
		"            when 'time' then %d"
		"            when 'timetz' then %d"
		"            else cast(null as smallint)"
		"       end as sql_datetime_sub,"
		"       case c.\"type\""
		"            when 'char' then 6 * c.type_digits"
		"            when 'varchar' then 6 * c.type_digits"
		"            when 'clob' then 6 * c.type_digits"
		"            when 'blob' then c.type_digits"
		"            else cast(null as integer)"
		"       end as char_octet_length,"
		"       cast(c.\"number\" + 1 as integer) as ordinal_position,"
		"       case c.\"null\""
		"            when true then cast('yes' as varchar(3))"
		"            when false then cast('no' as varchar(3))"
		"       end as is_nullable"
		" from sys.\"schemas\" s,"
		"      sys.\"tables\" t,"
		"      sys.\"columns\" c"
		" where s.\"id\" = t.\"schema_id\" and"
		"       t.\"id\" = c.\"table_id\"",
		SQL_BIGINT, SQL_LONGVARBINARY, SQL_BIT, SQL_CHAR,
		SQL_LONGVARCHAR, SQL_TYPE_DATE, SQL_DECIMAL, SQL_DOUBLE,
		SQL_INTEGER, SQL_INTERVAL_YEAR, SQL_INTERVAL_YEAR_TO_MONTH,
		SQL_INTERVAL_MONTH, SQL_REAL, SQL_INTERVAL_DAY,
		SQL_INTERVAL_DAY_TO_HOUR, SQL_INTERVAL_DAY_TO_MINUTE,
		SQL_INTERVAL_DAY_TO_SECOND, SQL_INTERVAL_HOUR,
		SQL_INTERVAL_HOUR_TO_MINUTE, SQL_INTERVAL_HOUR_TO_SECOND,
		SQL_INTERVAL_MINUTE, SQL_INTERVAL_MINUTE_TO_SECOND,
		SQL_INTERVAL_SECOND, SQL_SMALLINT, SQL_TYPE_TIMESTAMP,
		SQL_TYPE_TIMESTAMP, SQL_TYPE_TIME, SQL_TYPE_TIME,
		SQL_TINYINT, SQL_VARCHAR, SQL_NULLABLE, SQL_NO_NULLS,
		SQL_BIGINT, SQL_LONGVARBINARY, SQL_BIT, SQL_CHAR,
		SQL_LONGVARCHAR, SQL_DATETIME, SQL_DECIMAL, SQL_DOUBLE,
		SQL_INTEGER, SQL_INTERVAL, SQL_REAL, SQL_INTERVAL,
		SQL_SMALLINT, SQL_DATETIME, SQL_DATETIME, SQL_DATETIME,
		SQL_DATETIME, SQL_TINYINT, SQL_VARCHAR, SQL_CODE_DATE,
		SQL_CODE_YEAR, SQL_CODE_YEAR_TO_MONTH, SQL_CODE_MONTH,
		SQL_CODE_DAY, SQL_CODE_DAY_TO_HOUR, SQL_CODE_DAY_TO_MINUTE,
		SQL_CODE_DAY_TO_SECOND, SQL_CODE_HOUR, SQL_CODE_HOUR_TO_MINUTE,
		SQL_CODE_HOUR_TO_SECOND, SQL_CODE_MINUTE,
		SQL_CODE_MINUTE_TO_SECOND, SQL_CODE_SECOND,
		SQL_CODE_TIMESTAMP, SQL_CODE_TIMESTAMP, SQL_CODE_TIME,
		SQL_CODE_TIME);
	query_end += strlen(query_end);

	/* depending on the input parameter values we must add a
	   variable selection condition dynamically */

	/* Construct the selection condition query part */
	if (stmt->Dbc->sql_attr_metadata_id == SQL_TRUE) {
		/* treat arguments as identifiers */
		/* remove trailing blanks */
		while (NameLength2 > 0 &&
		       isspace((int) SchemaName[NameLength2 - 1]))
			NameLength2--;
		while (NameLength3 > 0 &&
		       isspace((int) TableName[NameLength3 - 1]))
			NameLength3--;
		while (NameLength4 > 0 &&
		       isspace((int) ColumnName[NameLength4 - 1]))
			NameLength4--;
		if (NameLength2 > 0) {
			sprintf(query_end, " and s.\"name\" = '");
			query_end += strlen(query_end);
			while (NameLength2-- > 0)
				*query_end++ = tolower(*SchemaName++);
			*query_end++ = '\'';
		}
		if (NameLength3 > 0) {
			sprintf(query_end, " and t.\"name\" = '");
			query_end += strlen(query_end);
			while (NameLength3-- > 0)
				*query_end++ = tolower(*TableName++);
			*query_end++ = '\'';
		}
		if (NameLength4 > 0) {
			sprintf(query_end, " and c.\"name\" = '");
			query_end += strlen(query_end);
			while (NameLength4-- > 0)
				*query_end++ = tolower(*ColumnName++);
			*query_end++ = '\'';
		}
	} else {
		int escape;
		if (NameLength2 > 0) {
			escape = 0;
			sprintf(query_end, " and s.\"name\" like '");
			query_end += strlen(query_end);
			while (NameLength2-- > 0) {
				if (*SchemaName == '\\') {
					escape = 1;
					*query_end++ = '\\';
				}
				*query_end++ = *SchemaName++;
			}
			*query_end++ = '\'';
			if (escape) {
				sprintf(query_end, " escape '\\\\'");
				query_end += strlen(query_end);
			}
		}
		if (NameLength3 > 0) {
			escape = 0;
			sprintf(query_end, " and t.\"name\" like '");
			query_end += strlen(query_end);
			while (NameLength3-- > 0) {
				if (*TableName == '\\') {
					escape = 1;
					*query_end++ = '\\';
				}
				*query_end++ = *TableName++;
			}
			*query_end++ = '\'';
			if (escape) {
				sprintf(query_end, " escape '\\\\'");
				query_end += strlen(query_end);
			}
		}
		if (NameLength4 > 0) {
			escape = 0;
			sprintf(query_end, " and c.\"name\" like '");
			query_end += strlen(query_end);
			while (NameLength4-- > 0) {
				if (*ColumnName == '\\') {
					escape = 1;
					*query_end++ = '\\';
				}
				*query_end++ = *ColumnName++;
			}
			*query_end++ = '\'';
			if (escape) {
				sprintf(query_end, " escape '\\\\'");
				query_end += strlen(query_end);
			}
		}
	}

	/* add the ordering */
	strcpy(query_end,
	       " order by table_cat, table_schem, "
	       "table_name, ordinal_position");
	query_end += strlen(query_end);
	assert(query_end - query < 12000 + NameLength2 + NameLength3 + NameLength4);

	/* query the MonetDB data dictionary tables */
	rc = SQLExecDirect_(stmt, (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
}

SQLRETURN SQL_API
SQLColumns(SQLHSTMT StatementHandle,
	   SQLCHAR *CatalogName,
	   SQLSMALLINT NameLength1,
	   SQLCHAR *SchemaName,
	   SQLSMALLINT NameLength2,
	   SQLCHAR *TableName,
	   SQLSMALLINT NameLength3,
	   SQLCHAR *ColumnName,
	   SQLSMALLINT NameLength4)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLColumns " PTRFMT, PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLColumns_(stmt,
			   CatalogName, NameLength1,
			   SchemaName, NameLength2,
			   TableName, NameLength3,
			   ColumnName, NameLength4);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLColumnsA(SQLHSTMT StatementHandle,
	    SQLCHAR *CatalogName,
	    SQLSMALLINT NameLength1,
	    SQLCHAR *SchemaName,
	    SQLSMALLINT NameLength2,
	    SQLCHAR *TableName,
	    SQLSMALLINT NameLength3,
	    SQLCHAR *ColumnName,
	    SQLSMALLINT NameLength4)
{
	return SQLColumns(StatementHandle,
			  CatalogName, NameLength1,
			  SchemaName, NameLength2,
			  TableName, NameLength3,
			  ColumnName, NameLength4);
}

SQLRETURN SQL_API
SQLColumnsW(SQLHSTMT StatementHandle,
	    SQLWCHAR *CatalogName,
	    SQLSMALLINT NameLength1,
	    SQLWCHAR *SchemaName,
	    SQLSMALLINT NameLength2,
	    SQLWCHAR *TableName,
	    SQLSMALLINT NameLength3,
	    SQLWCHAR *ColumnName,
	    SQLSMALLINT NameLength4)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLCHAR *catalog = NULL, *schema = NULL, *table = NULL, *column = NULL;
	SQLRETURN rc = SQL_ERROR;

#ifdef ODBCDEBUG
	ODBCLOG("SQLColumnsW " PTRFMT, PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(CatalogName, NameLength1, SQLCHAR, catalog,
		   addStmtError, stmt, goto exit);
	fixWcharIn(SchemaName, NameLength2, SQLCHAR, schema,
		   addStmtError, stmt, goto exit);
	fixWcharIn(TableName, NameLength3, SQLCHAR, table,
		   addStmtError, stmt, goto exit);
	fixWcharIn(ColumnName, NameLength4, SQLCHAR, column,
		   addStmtError, stmt, goto exit);

	rc = SQLColumns_(stmt,
			 catalog, SQL_NTS,
			 schema, SQL_NTS,
			 table, SQL_NTS,
			 column, SQL_NTS);

      exit:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (table)
		free(table);
	if (column)
		free(column);
	return rc;
}
#endif /* WITH_WCHAR */
