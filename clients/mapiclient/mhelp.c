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

/**
 * 2016 Martin Kersten
 *
 * The SQL syntax help synopsis.
 */

/* produce a synopsis of the SQL syntax, inspired by a competing product.
 * Use the conventional grammar constructs:
 * [ A | B ]    token A or B or none
 * { A | B }    exactly one of the options A or B should be chosen
 * A [',' ...]       a comma separated list of A elements
 * { A | B } ...     a series of A and B's
 * { A B } [',' ...] a series of A B,A B,A B,A B
 *
 * Ideally each major command line should point into the website for
 * more details and variations not covered here.
 * */

#include "monetdb_config.h"
#include <ctype.h>
#include <string.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>		/* for strncasecmp */
#endif
#include "stream.h"
#include "mhelp.h"

typedef struct {
	const char *command;
	const char *synopsis;
	const char *syntax;
	const char *rules;
	const char *comments;
} SQLhelp;

SQLhelp sqlhelp1[] = {
	// major commands
	{"ALTER MERGE TABLE",
	 "",
	 "ALTER TABLE [ IF EXISTS ] qname ADD TABLE qname [ AS PARTITION partition_spec ]\n"
	 "ALTER TABLE [ IF EXISTS ] qname DROP TABLE qname [ RESTRICT | CASCADE ]\n"
	 "ALTER TABLE [ IF EXISTS ] qname SET TABLE qname AS PARTITION partition_spec",
	 "qname,partition_spec",
	 "See also https://www.monetdb.org/documentation/admin-guide/distributed-query-processing/"},
	{"ALTER SCHEMA",
	 "",
	 "ALTER SCHEMA [ IF EXISTS ] ident RENAME TO ident",
	 "ident",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/schema-definitions/"},
	{"ALTER SEQUENCE",
	 "",
	 "ALTER SEQUENCE [ IF EXISTS ] qname [AS seq_int_datatype] [RESTART [WITH {bigint|subquery}] ] [INCREMENT BY bigint]\n"
	 "  [MINVALUE bigint | NO MINVALUE] [MAXVALUE bigint | NO MAXVALUE] [CACHE bigint] [[NO] CYCLE]",
	 "seq_int_datatype",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-types/serial-types/"},
	{"ALTER TABLE",
	 "",
	 "ALTER TABLE [ IF EXISTS ] qname ADD [ COLUMN ] column_def\n"
	 "ALTER TABLE [ IF EXISTS ] qname ADD [ CONSTRAINT ident ] table_constraint\n"
	 "ALTER TABLE [ IF EXISTS ] qname ALTER [ COLUMN ] ident SET DEFAULT value\n"
	 "ALTER TABLE [ IF EXISTS ] qname ALTER [ COLUMN ] ident SET [NOT] NULL\n"
	 "ALTER TABLE [ IF EXISTS ] qname ALTER [ COLUMN ] ident DROP DEFAULT\n"
	 "ALTER TABLE [ IF EXISTS ] qname ALTER [ COLUMN ] ident SET STORAGE {string | NULL}\n"
	 "ALTER TABLE [ IF EXISTS ] qname DROP [ COLUMN ] ident [ RESTRICT | CASCADE ]\n"
	 "ALTER TABLE [ IF EXISTS ] qname DROP CONSTRAINT ident [ RESTRICT | CASCADE ]\n"
	 "ALTER TABLE [ IF EXISTS ] qname RENAME [ COLUMN ] ident TO ident\n"
	 "ALTER TABLE [ IF EXISTS ] qname RENAME TO ident\n"
	 "ALTER TABLE [ IF EXISTS ] qname SET { INSERT ONLY | READ ONLY | READ WRITE }\n"
	 "ALTER TABLE [ IF EXISTS ] qname SET SCHEMA ident",
	 "qname,column_def,table_constraint,ident",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/alter-statement/"},
	{"ALTER USER",
	 "Change a user's login name or password or default schema",
	 "ALTER USER [ IF EXISTS ] ident RENAME TO ident\n"
	 "ALTER USER SET [ENCRYPTED | UNENCRYPTED] PASSWORD string USING OLD PASSWORD string\n"
	 "ALTER USER [ IF EXISTS ] ident\n"
	 "    [WITH [ENCRYPTED | UNENCRYPTED] PASSWORD string]\n"
	 "    [SET SCHEMA ident] [SCHEMA PATH string] [DEFAULT ROLE ident]\n"
	 "    [MAX_MEMORY posbytes | MAX_MEMORY sizestr | NO MAX_MEMORY] [MAX_WORKERS poscount | NO MAX_WORKERS]",
	 "ident",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/privileges/"},
	{"ANALYZE",
	 "Analyze and update column data statistics of column(s) of one or all tables in a schema",
	 "ANALYZE ident [ . ident [ column_list ] ]",
	 "ident,column_list",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-manipulation/analyze-statement/"},
	{"CALL",
	 "Call a stored procedure",
	 "CALL qname '(' [ scalar_expression [',' ...] ] ')' | CALL ident '.' ident",
	 NULL,
	 "See also https://www.monetdb.org/documentation/user-guide/sql-programming/flow-of-control/"},
	{"COMMENT",
	 "Add, update or remove a comment or description for a database object",
	 "COMMENT ON { SCHEMA | TABLE | VIEW | INDEX | SEQUENCE | function_type }\n"
	 "     qname IS { 'my description text' | NULL | '' }\n"
	 "COMMENT ON COLUMN q3name IS { 'my description text' | NULL | '' }",
	 "function_type,qname,q3name",
	 NULL},
	{"COMMIT",
	 "Commit the current transaction",
	 "COMMIT [ WORK ] [ AND CHAIN | AND NO CHAIN ]",
	 NULL,
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/transactions/"},
	{"COPY BINARY",
	 "Append binary representations into a table",
	 "COPY [{BIG | LITTLE | NATIVE} ENDIAN] BINARY INTO qname [column_list] FROM string [',' ...] [ON { CLIENT | SERVER }]",
	 "qname,column_list",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-loading/binary-loading/"},
	{"COPY INTO",
	 "Write query result data to a csv file or standard output stream",
	 "COPY query_expression INTO [STDOUT | string [ON { CLIENT | SERVER }]] [separators] [NULL [AS] string]",
	 "query_expression,separators",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-loading/export_data/"},
	{"COPY INTO BINARY",
	 "Write query result data to binary files",
	 "COPY query_expression INTO [{BIG | LITTLE | NATIVE} ENDIAN] BINARY string_commalist [ON { CLIENT | SERVER }]",
	 "query_expression",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-loading/export_data/"},
	{"COPY INTO FROM",
	 "Read csv file(s) or standard input stream data and insert into a table",
	 "COPY [nrofrecords] INTO qname [column_list] FROM string [',' ...] [headerlist] [ON { CLIENT | SERVER }] [ separators]\n"
	 " [NULL [AS] string] [BEST EFFORT] [FWF '(' integer [',' ...] ')'\n"
	 "COPY [nrofrecords] INTO qname [column_list] FROM STDIN [headerlist] [ separators]\n"
	 " [NULL [AS] string] [BEST EFFORT]\n",
	 "nrofrecords,qname,column_list,headerlist,separators",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-loading/copy-from/"},
	{"COPY LOADER",
	 "Copy into using a user supplied parsing function",
	 "COPY LOADER INTO qname FROM qname '(' [ scalar_expression ... ] ')'",
	 "qname,scalar_expression",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-loading/loader-functions/"},
	{"CREATE AGGREGATE",
	 "Create a user-defined aggregate function. The body of the aggregate function\n"
	 "can also be defined in other programming languages such as Python, R, C or CPP.",
	 "CREATE [ OR REPLACE ] AGGREGATE [ FUNCTION ] qname '(' { '*' | [ param [',' ...]] } ')'\n"
	 "    RETURNS function_return_data_type\n"
	 "    EXTERNAL NAME ident ',' ident\n"
	 "CREATE [ OR REPLACE ] AGGREGATE [ FUNCTION ] qname '(' { '*' | [ param [',' ...]] } ')'\n"
	 "    RETURNS function_return_data_type\n"
	 "    LANGUAGE language_keyword external_code",
	 "qname,param,function_return_data_type,ident,language_keyword,external_code",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-programming/function-definitions/"},
	{"CREATE FILTER FUNCTION",
	 "Create a user-defined filter function. Currently only MAL definitions\n"
	 "CREATE [ OR REPLACE ] FILTER [ FUNCTION ] qname '(' { '*' | [ param [',' ...]] } ')'\n"
	 "    RETURNS function_return_data_type\n"
	 "    EXTERNAL NAME ident ',' ident",
	 "qname,param,function_return_data_type,ident",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-programming/function-definitions/"},
	{"CREATE FUNCTION",
	 "Create a user-defined function (UDF). The body of the function can be defined in\n"
	 " PL/SQL or programming languages such as Python, R, C or CPP when embedded on the server.",
	 "CREATE [ OR REPLACE ] FUNCTION qname '(' { '*' | [ param [',' ...]] } ')'\n"
	 "    RETURNS function_return_data_type\n"
	 "    BEGIN [ ATOMIC ] statement [ ';' ...] END\n"
	 "CREATE [ OR REPLACE ] FUNCTION qname '(' { '*' | [ param [',' ...]] } ')'\n"
	 "    RETURNS function_return_data_type\n"
	 "    EXTERNAL NAME ident ',' ident\n"
	 "CREATE [ OR REPLACE ] FUNCTION qname '(' { '*' | [ param [',' ...]] } ')'\n"
	 "    RETURNS function_return_data_type\n"
	 "    LANGUAGE language_keyword external_code",
	 "qname,param,function_return_data_type,statement,ident,language_keyword,external_code",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-programming/function-definitions/"},
	{"CREATE INDEX",
	 "Create a hint for a secondary index on a column or set of columns of a table",
	 "CREATE [ UNIQUE | ORDERED | IMPRINTS ] INDEX [ IF NOT EXISTS ] ident ON qname '(' ident_list ')'",
	 NULL,
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/index-definitions/"},
	{"CREATE LOADER",
	 "Create a custom (external) data loader function. The body is defined in Python language",
	 "CREATE [ OR REPLACE ] LOADER [ FUNCTION ] qname '(' [ param [',' ...]] ')'\n"
	 "    LANGUAGE PYTHON external_code",
	 "qname,param,external_code",
	 "See also https://www.monetdb.org/documentation/user-guide/blog-archive/python-loader/"},
	{"CREATE MERGE TABLE",
	 "",
	 "CREATE MERGE TABLE [ IF NOT EXISTS ] qname table_source [ partition_by ]",
	 "table_source,partition_by",
	 "See also https://www.monetdb.org/documentation/admin-guide/distributed-query-processing/"},
	{"CREATE PROCEDURE",
	 "Create a user-defined procedure",
	 "CREATE [ OR REPLACE ] PROCEDURE qname '(' { '*' | [ param [',' ...]] } ')'\n"
	 "    BEGIN [ ATOMIC ] procedure_statement [ ';' ...] END\n"
	 "CREATE [ OR REPLACE ] PROCEDURE qname '(' { '*' | [ param [',' ...]] } ')'\n"
	 "    EXTERNAL NAME ident ',' ident",
	 "qname,param,procedure_statement,ident",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-programming/procedure-definitions/"},
	{"CREATE REMOTE TABLE",
	 "",
	 "CREATE REMOTE TABLE [ IF NOT EXISTS ] qname ON string [WITH [USER 'username'] [[ENCRYPTED] PASSWORD 'password']]",
	 NULL,
	 "remote name should match mapi:monetdb://host:port/database[/schema[/table]]"},
	{"CREATE UNLOGGED TABLE",
	 "Create a new unlogged table",
	 "CREATE UNLOGGED TABLE [ IF NOT EXISTS ] qname table_source [STORAGE ident string]\n"
	 "CREATE UNLOGGED TABLE [ IF NOT EXISTS ] qname FROM LOADER function_ref\n"
	 "CREATE UNLOGGED TABLE [ IF NOT EXISTS ] qname table_source [on_commit]",
	 "table_source,on_commit,function_ref",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/table-definition/"},
	{"CREATE REPLICA TABLE",
	 "",
	 "CREATE REPLICA TABLE [ IF NOT EXISTS ] qname table_source",
	 NULL,
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/table-definition/"},
	{"CREATE ROLE",
	 "Create a new role. You can grant privileges to a role and next\n"
	 "grant a role (or multiple roles) to specific users",
	 "CREATE ROLE [ IF NOT EXISTS ] ident [ WITH ADMIN { CURRENT_USER | CURRENT_ROLE } ]",
	 "ident",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/privileges/#roles"},
	{"CREATE SCHEMA",
	 "Create a new schema",
	 "CREATE SCHEMA [ IF NOT EXISTS ] schema_name [default_char_set] [path_spec] [schema_element]",
	 "schema_name,default_char_set,path_spec,schema_element",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/schema-definitions/"},
	{"CREATE SEQUENCE",
	 "Define a new integer number sequence generator",
	 "CREATE SEQUENCE [ IF NOT EXISTS ] qname [AS seq_int_datatype] [START WITH bigint] [INCREMENT BY bigint]\n"
	 "  [MINVALUE bigint | NO MINVALUE] [MAXVALUE bigint | NO MAXVALUE] [CACHE bigint] [[NO] CYCLE]",
	 "seq_int_datatype",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-types/serial-types/"},
	{"CREATE TABLE",
	 "Create a new table",
	 "CREATE TABLE [ IF NOT EXISTS ] qname table_source [STORAGE ident string]\n"
	 "CREATE TABLE [ IF NOT EXISTS ] qname FROM LOADER function_ref\n"
	 "CREATE [ LOCAL | GLOBAL ] { TEMPORARY | TEMP } TABLE [ IF NOT EXISTS ] qname table_source [on_commit]",
	 "table_source,on_commit,function_ref",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/table-definition/"},
	{"CREATE TRIGGER",
	 "Define a triggered action for a table data update event",
	 "CREATE [ OR REPLACE ] TRIGGER ident { BEFORE | AFTER }\n"
	 " { INSERT | DELETE | TRUNCATE | UPDATE [ OF ident_list ] | LOGIN }\n"
	 " [ ON qname ] [ REFERENCING trigger_reference [...] ] triggered_action",
	 "qname,ident_list,trigger_reference,triggered_action",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-programming/trigger-definition/"},
	{"CREATE TYPE",
	 "Add user defined type to the type system ",
	 "CREATE TYPE [ IF NOT EXISTS ] qname EXTERNAL NAME ident",
	 NULL,
	 NULL},
	{"CREATE USER",
	 "Create a new database user",
	 "CREATE USER [ IF NOT EXISTS ] ident WITH [ENCRYPTED | UNENCRYPTED] PASSWORD string NAME string\n"
	 "    [SCHEMA ident] [SCHEMA PATH string] [MAX_MEMORY posbytes | MAX_MEMORY sizestr | NO MAX_MEMORY]\n"
	 "    [MAX_WORKERS poscount | NO MAX_WORKERS] [OPTIMIZER string] [DEFAULT ROLE ident]",
	 "ident",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/privileges/"},
	{"CREATE VIEW",
	 "Create a new view",
	 "CREATE [ OR REPLACE ] VIEW qname [ column_list ]\n"
	 "    AS { query_expression | '(' query_expression ')' } [ WITH CHECK OPTION ]\n"
	 "CREATE [ OR REPLACE ] [ LOCAL ] { TEMPORARY | TEMP } VIEW qname [ column_list ]\n"
	 "    AS { query_expression | '(' query_expression ')' }",
	 "qname,column_list,query_expression",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/view-definition/"},
	{"CREATE WINDOW",
	 "Create a user-defined window function. Currently only MAL definitions\n"
	 "are supported.",
	 "CREATE [ OR REPLACE ] WINDOW [ FUNCTION ] qname '(' { '*' | [ param [',' ...]] } ')'\n"
	 "    RETURNS function_return_data_type\n"
	 "    EXTERNAL NAME ident ',' ident",
	 "qname,param,function_return_data_type,ident",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-manipulation/window-expressions/"},
	{"CURRENT_DATE",
	 "Pseudo column or function to get the current date",
	 "CURRENT_DATE [ '(' ')' ]",
	 NULL,
	 NULL},
	{"CURRENT_ROLE",
	 "Pseudo column to get the current role name",
	 "CURRENT_ROLE",
	 NULL,
	 NULL},
	{"CURRENT_SCHEMA",
	 "Pseudo column to get the current schema name",
	 "CURRENT_SCHEMA",
	 NULL,
	 NULL},
	{"CURRENT_TIME",
	 "Pseudo column or function to get the current time including timezone",
	 "CURRENT_TIME [ '(' ')' ]",
	 NULL,
	 NULL},
	{"CURRENT_TIMESTAMP",
	 "Pseudo column or function to get the current timestamp including timezone",
	 "CURRENT_TIMESTAMP [ '(' ')' ] | NOW [ '(' ')' ]",
	 NULL,
	 NULL},
	{"CURRENT_TIMEZONE",
	 "Pseudo column to get the current timezone offset as a second interval",
	 "CURRENT_TIMEZONE",
	 NULL,
	 NULL},
	{"CURRENT_USER",
	 "Pseudo column to get the current user name",
	 "CURRENT_USER | USER",
	 NULL,
	 NULL},
	{"DEALLOCATE",
	 "Deallocates a prepared statement or all from the client's session cache",
	 "DEALLOCATE [ PREPARE ] { intnr | ** | ALL }",
	 NULL,
	 NULL},
	{"DECLARE",
	 "Define a local variable",
	 "DECLARE ident_list data_type",
	 "ident_list,data_type",
	 NULL},
	{"DELETE",
	 "Remove data rows from a table",
	 "[ WITH cte_list ] DELETE FROM qname [ [AS] ident ] [ WHERE search_condition ]",
	 "cte_list,search_condition",
	 NULL},
	{"DROP AGGREGATE",
	 "",
	 "DROP ALL AGGREGATE [ FUNCTION ] qname [ RESTRICT | CASCADE ]\n"
	 "DROP AGGREGATE [ FUNCTION ] [ IF EXISTS ] qname [ '(' [ data_type [',' ...]] ')' ] [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"DROP FILTER FUNCTION",
	 "",
	 "DROP ALL FILTER [ FUNCTION ] qname [ RESTRICT | CASCADE ]\n"
	 "DROP FILTER [ FUNCTION ] [ IF EXISTS ] qname [ '(' [ data_type [',' ...]] ')' ] [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"DROP FUNCTION",
	 "",
	 "DROP ALL FUNCTION qname [ RESTRICT | CASCADE ]\n"
	 "DROP FUNCTION [ IF EXISTS ] qname [ '(' [ data_type [',' ...]] ')' ] [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"DROP INDEX",
	 "",
	 "DROP INDEX [ IF EXISTS ] qname",
	 NULL,
	 NULL},
	{"DROP LOADER",
	 "",
	 "DROP ALL LOADER [ FUNCTION ] qname [ RESTRICT | CASCADE ]\n"
	 "DROP LOADER [ FUNCTION ] [ IF EXISTS ] qname [ '(' [ data_type [',' ...]] ')' ] [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"DROP PROCEDURE",
	 "",
	 "DROP ALL PROCEDURE qname [ RESTRICT | CASCADE ]\n"
	 "DROP PROCEDURE [ IF EXISTS ] qname [ '(' [ data_type [',' ...]] ')' ] [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"DROP ROLE",
	 "",
	 "DROP ROLE [ IF EXISTS ] ident",
	 NULL,
	 NULL},
	{"DROP SCHEMA",
	 "",
	 "DROP SCHEMA [ IF EXISTS ] ident [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"DROP SEQUENCE",
	 "",
	 "DROP SEQUENCE [ IF EXISTS ] qname",
	 NULL,
	 NULL},
	{"DROP TABLE",
	 "",
	 "DROP TABLE [ IF EXISTS ] qname [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"DROP TRIGGER",
	 "",
	 "DROP TRIGGER [ IF EXISTS ] qname",
	 NULL,
	 NULL},
	{"DROP TYPE",
	 "",
	 "DROP TYPE [ IF EXISTS ] qname [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"DROP USER",
	 "",
	 "DROP USER [ IF EXISTS ] ident",
	 NULL,
	 NULL},
	{"DROP VIEW",
	 "",
	 "DROP VIEW [ IF EXISTS ] qname [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"DROP WINDOW",
	 "",
	 "DROP ALL WINDOW [ FUNCTION ] qname [ RESTRICT | CASCADE ]\n"
	 "DROP WINDOW [ FUNCTION ] [ IF EXISTS ] qname [ '(' [ data_type [',' ...]] ')' ] [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"EXECUTE",
	 "Execute a prepared SQL statement with supplied parameter values",
	 "EXECUTE { intnr | ** } '(' [ value [, ...] ] ')'",
	 NULL,
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-manipulation/prepare-statement/"},
	{"EXPLAIN",
	 "Give MAL execution plan for the SQL statement",
	 "EXPLAIN statement",
	 NULL,
	 "See also https://www.monetdb.org/documentation/admin-guide/debugging-features/explain-sql-stmt/"},
	{"EXTRACT",
	 "Built-in function",
	 "EXTRACT '(' { YEAR | MONTH | DAY | HOUR | MINUTE | SECOND | CENTURY | DECADE | QUARTER | WEEK | DOW | DOY | EPOCH } FROM scalar_expression ')'",
	 NULL,
	 NULL},
	{"INSERT",
	 "Add data rows to a table",
	 "[ WITH cte_list ] INSERT INTO qname [ column_list ]\n"
	 " [ { DEFAULT VALUES | VALUES row_values | query_expression } ]",
	 "cte_list,column_list,row_values,query_expression",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-manipulation/table-updates/"},
	{"GRANT",
	 "Define access privileges",
	 "GRANT privileges TO grantee [',' ...] [ WITH GRANT OPTION ]\n"
	 "GRANT role [',' ...] TO grantee [',' ...] [ WITH ADMIN OPTION]",
	 "privileges,table_privileges,global_privileges,role,grantee",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/privileges/#grant-and-revoke"},
	{"LOCALTIME",
	 "Pseudo column or function to get the current client time excluding timezone",
	 "LOCALTIME [ '(' ')' ]",
	 NULL,
	 NULL},
	{"LOCALTIMESTAMP",
	 "Pseudo column or function to get the current client timestamp excluding timezone",
	 "LOCALTIMESTAMP [ '(' ')' ]",
	 NULL,
	 NULL},
	{"MERGE",
	 "",
	 "[ WITH cte_list ] MERGE INTO qname [ [AS] ident ] USING table_ref [ [AS] ident ] ON search_condition merge_list",
	 "cte_list,table_ref,search_condition,merge_list",
	 "See also: https://www.monetdb.org/documentation/user-guide/blog-archive/merge-statements/"},
	{"PLAN",
	 "Give relational execution plan for the SQL statement",
	 "PLAN statement",
	 NULL,
	 "See also https://www.monetdb.org/documentation/admin-guide/debugging-features/plan-sql-stmt/"},
	{"PREPARE",
	 "Prepare a SQL DML statement with optional question-mark parameter markers",
	 "PREPARE statement",
	 NULL,
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-manipulation/prepare-statement/"},
	{"RELEASE SAVEPOINT",
	 "",
	 "RELEASE SAVEPOINT ident",
	 NULL,
	 NULL},
	{"REVOKE",
	 "Remove some privileges",
	 "REVOKE [GRANT OPTION FOR] privileges FROM { grantee [',' ...] | CURRENT_USER | CURRENT_ROLE }\n"
	 "REVOKE [ADMIN OPTION FOR] role [',' ...] FROM { grantee [',' ...] | CURRENT_USER | CURRENT_ROLE }",
	 "privileges,table_privileges,global_privileges,grantee,role",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/privileges/#grant-and-revoke"},
	{"ROLLBACK",
	 "Rollback the current transaction",
	 "ROLLBACK [WORK] [ AND CHAIN | AND NO CHAIN ] [TO SAVEPOINT ident]",
	 NULL,
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/transactions/"},
	{"SAVEPOINT",
	 NULL,
	 "SAVEPOINT ident",
	 NULL,
	 NULL},
	{"SELECT",
	 "",
	 "[ WITH cte_list ]\n"
	 "SELECT [ ALL | DISTINCT [ ON { expression [',' ...] } ] ]\n"
	 "[ '*' | expression [ [ AS ] output_name ] [',' ...] ]\n"
	 "[ FROM from_item [',' ...] ]\n"
	 "[ WINDOW window_definition [',' ...] ]\n"
	 "[ WHERE condition ]\n"
	 "[ GROUP BY group_by_element [',' ...] ]\n"
	 "[ HAVING condition [',' ...] ]\n"
	 "[ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] [ CORRESPONDING ] select ]\n"
	 "[ ORDER BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [',' ...] ]\n"
	 "[ limit_offset_clause | offset_fetchfirst_clause ]\n"
	 "[ SAMPLE size [ SEED size ] ]",
	 "cte_list,expression,group_by_element,window_definition",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-manipulation/table-expressions/"},
	{"SET",
	 "Assign a value to a variable or column",
	 "SET ident '=' simple_atom",
	 "simple_atom",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-programming/variable-definitions/"},
	{"SET LOCAL TRANSACTION",
	 "",
	 "SET LOCAL TRANSACTION [ transactionmode ]",
	 "transactionmode",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-summary/#set-local-transaction"},
	{"SET ROLE",
	 "Change current role",
	 "SET ROLE ident",
	 NULL,
	 NULL},
	{"SET SCHEMA",
	 "Change current schema",
	 "SET SCHEMA ident",
	 NULL,
	 NULL},
	{"SET SESSION AUTHORIZATION",
	 "",
	 "SET SESSION AUTHORIZATION ident",
	 NULL,
	 NULL},
	{"SET TIME ZONE",
	 NULL,
	 "SET TIME ZONE interval",
	 "interval",
	 NULL},
	{"SET TIME ZONE LOCAL",
	 NULL,
	 "SET TIME ZONE LOCAL",
	 NULL,
	 NULL},
	{"SET TRANSACTION",
	 "",
	 "SET TRANSACTION [ transactionmode ]",
	 "transactionmode",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-summary/#set-transaction"},
	{"SET USER",
	 "Change current user",
	 "SET USER '=' ident",
	 NULL,
	 NULL},
	{"START TRANSACTION",
	 "Change transaction mode from auto-commit to user controlled commit/rollback",
	 "{ START | BEGIN } TRANSACTION [ transactionmode ]",
	 "transactionmode",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/transactions/"},
	{"TABLE JOINS",
	 "",
	 "'(' joined_table ') |\n"
	 "table_ref CROSS JOIN table_ref ')' |\n"
	 "table_ref NATURAL [ INNER | LEFT | RIGHT | FULL ] JOIN table_ref |\n"
	 "table_ref UNION JOIN table_ref { ON search_condition | USING column_list } |\n"
	 "table_ref [ INNER | LEFT | RIGHT | FULL ] JOIN table_ref { ON search_condition | USING column_list }",
	 "table_ref,search_condition,column_list",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-manipulation/table-expressions/"},
	{"TRACE",
	 "Give execution trace for the SQL statement",
	 "TRACE statement",
	 NULL,
	 "See also https://www.monetdb.org/documentation/admin-guide/debugging-features/trace-sql-stmt/"},
	{"TRUNCATE",
	 "Remove all rows from a table",
	 "TRUNCATE [ TABLE ] qname [ CONTINUE IDENTITY | RESTART IDENTITY ] [ CASCADE | RESTRICT ]",
	 "",
	 NULL},
	{"UPDATE",
	 "Change data in a table",
	 "[ WITH cte_list ] UPDATE qname [ [AS] ident ] SET assignment_list\n"
	 " [ FROM from_item ] [ WHERE search_condition ]",
	 "cte_list,assignment_list,search_condition",
	 NULL},
	{"VALUES",
	 "Specify a list of row values",
	 "VALUES row_values",
	 "row_values",
	 NULL},
	{"WINDOW FUNCTIONS",
	 "",
	 "{ window_aggregate_function | window_rank_function } OVER { ident | '(' window_specification ')' }",
	 "window_aggregate_function,window_rank_function,window_specification",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-manipulation/window-expressions/"},
	{NULL, NULL, NULL, NULL, NULL}	/* End of list marker */
};

SQLhelp sqlhelp2[] = {
// The subgrammar rules
	{"and_exp",
	 NULL,
	 "{ and_exp AND pred_exp | pred_exp }",
	 "pred_exp",
	 NULL},
	{"assignment_list",
	 NULL,
	 "column '=' DEFAULT | column '=' search_condition | '(' column [',' ...] ')' '=' subquery",
	 "search_condition,column,subquery",
	 NULL},
	{"authid",
	 NULL,
	 "restricted ident",
	 NULL,
	 NULL},
	{"case_statement",
	 "Case statement for procedures/functions",
	 "CASE scalar_expression [ when_statement ...]  [ELSE procedure_statement ... ] END CASE",
	 NULL,
	 "See also https://www.monetdb.org/documentation/user-guide/sql-programming/flow-of-control/"},
	{"column_def",
	 NULL,
	 "ident { data_type [ column_option ... ] | SERIAL | BIGSERIAL }",
	 "ident,data_type,column_option",
	 NULL},
	{"column_list",
	 NULL,
	 "'(' ident [',' ...] ')'",
	 NULL,
	 NULL},
	{"column_option",
	 NULL,
	 "DEFAULT value | column_constraint | generated_column",
	 "column_constraint,generated_column",
	 NULL},
	{"column_option_list",
	 NULL,
	 "ident WITH OPTIONS '(' column_constraint ')' [',' ...]",
	 "column_constraint",
	 NULL},
	{"column_constraint",
	 NULL,
	 "[ CONSTRAINT ident ] { NOT NULL | NULL | CHECK '(' search_condition ')' |\n"
	 "    PRIMARY KEY | UNIQUE | UNIQUE NULLS [ NOT ] DISTINCT |\n"
	 "    REFERENCES qname [ column_list ] [ match_option ] [ reference_actions ] }",
	 "column_list,search_condition,reference_actions,match_option",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/table-elements/"},
	{"control_statement",
	 NULL,
	 "call_procedure | while_statement | if_statement | case_statement | return_statement",
	 "call_procedure,while_statement,if_statement,case_statement,return_statement",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-programming/flow-of-control/"},
	{"datetime_type",
	 NULL,
	 "DATE | TIME [ time_precision ] [ WITH TIME ZONE ] |\n"
	 " TIMESTAMP [ timestamp_precision ] [ WITH TIME ZONE ]",
	 "time_precision,timestamp_precision",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-types/temporal-types/"},
	{"data_type",
	 NULL,
	 "BOOLEAN | BOOL | TINYINT | SMALLINT | INT | INTEGER | BIGINT | HUGEINT |\n"
	 " { DECIMAL | DEC | NUMERIC | FLOAT } [ '(' nonzero [',' nonzero ] ')' ] |\n"
	 " REAL | DOUBLE [ PRECISION ] |\n"
	 " { VARCHAR | CHARACTER VARYING } '(' nonzero ')' |\n"
	 " { CHAR | CHARACTER [ LARGE OBJECT ] | CLOB | TEXT | STRING | JSON | URL } [ '(' nonzero ')' ] |\n"
	 " { BINARY LARGE OBJECT | BLOB } [ '(' nonzero ')' ] |\n"
	 " UUID | INET | datetime_type | interval_type | geometry_type",
	 "datetime_type,interval_type,geometry_type",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-types/"},
	{"default_char_set",
	 NULL,
	 "DEFAULT CHARACTER SET ident",
	 NULL,
	 NULL},
	{"drop_table_element",
	 NULL,
	 "{ CONSTRAINT | TABLE | COLUMN } ident [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"end_time",
	 NULL,
	 "SECOND timestamp_precision\n,timestamp_precision",
	 NULL,
	 NULL},
	{"function_return_data_type",
	 NULL,
	 "data_type | TABLE '(' ident data_type [',' ...] ')'",
	 NULL,
	 NULL},
	{"function_type",
	 NULL,
	 "{ FUNCTION | PROCEDURE | { { AGGREGATE | FILTER | LOADER | WINDOW } [ FUNCTION ] } }",
	 NULL,
	 NULL},
	{"generated_column",
	 NULL,
	 "AUTO_INCREMENT | GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ '('\n"
	 "  [AS seq_int_datatype] [START WITH bigint] [INCREMENT BY bigint]\n"
	 "  [MINVALUE bigint | NO MINVALUE] [MAXVALUE bigint | NO MAXVALUE] [CACHE bigint] [[NO] CYCLE] ')' ]",
	 "seq_int_datatype",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-types/serial-types/"},
	{"global_privileges",
	 NULL,
	 "{ COPY FROM | COPY INTO } [',' ...]",
	 NULL,
	 NULL},
	{"grantee",
	 NULL,
	 "{ PUBLIC | authid } ",
	 "authid",
	 NULL},
	{"group_by_element",
	 NULL,
	 "{ expression | '(' ')' | ROLLUP '(' ident [',' ... ] ')' | CUBE '(' ident [',' ... ] ')'\n"
	 "| GROUPING SETS '(' group_by_element [',' ... ] ')' }",
	 "expression",
	 NULL},
	{"headerlist",
	 NULL,
	 "'(' { ident [string] } [',' ...] ')'",
	 NULL,
	 NULL},
	{"ident",
	 "An identifier name. Use double quote's around the identifier name to include\n"
	 "        mixed/upper case letters and/or special characters",
	 NULL,
	 NULL,
	 NULL},
	{"ident_list",
	 NULL,
	 "ident [',' ...]",
	 "ident",
	 NULL},
	{"if_statement",
	 NULL,
	 "IF search_condition THEN procedure_statement ...\n"
	 "[ELSE IF search_condition THEN procedure_statement ... ]...\n"
	 "[ ELSE procedure_statement ... ] END IF",
	 "search_condition,procedure_statement",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-programming/flow-of-control/"},
	{"seq_int_datatype",
	 NULL,
	 "BIGINT | INTEGER | INT | SMALLINT | TINYINT",
	 NULL,
	 NULL},
	{"interval",
	 NULL,
	 "INTERVAL [ '+' | '-' ] string start_field TO end_field",
	 "start_field,end_field",
	 NULL},
	{"interval_type",
	 NULL,
	 "INTERVAL { YEAR | MONTH | DAY | HOUR | MINUTE | SECOND [time_precision] | start_field TO end_field }",
	 "time_precision,start_field,end_field",
	 NULL},
	{"isolevel",
	 NULL,
	 "READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE",
	 NULL,
	 NULL},
	{"language_keyword",
	 NULL,
	 "C | CPP | R | PYTHON | PYTHON3",
	 NULL,
	 NULL},
	{"match_option",
	 NULL,
	 "MATCH [ FULL | PARTIAL | SIMPLE ]",
	 NULL,
	 NULL},
	{"merge_list",
	 NULL,
	 "merge_clause [ merge_clause ]",
	 "merge_clause",
	 NULL},
	{"merge_clause",
	 NULL,
	 "{ WHEN NOT MATCHED THEN INSERT [ column_list ] [ { VALUES row_values | DEFAULT VALUES } ]\n"
	 "| WHEN MATCHED THEN { UPDATE SET assignment_list | DELETE } }",
	 "column_list,row_values,assignment_list",
	 NULL},
	{"nrofrecords",
	 NULL,
	 "OFFSET integer | integer RECORDS | integer OFFSET integer RECORDS | integer RECORDS OFFSET integer",
	 NULL,
	 NULL},
	{"on_commit",
	 NULL,
	 "ON COMMIT { DELETE ROWS | PRESERVE ROWS | DROP }",
	 NULL,
	 NULL},
	{"partition_by",
	 NULL,
	 "PARTITION BY { RANGE | VALUES } { ON '(' ident ')' | USING '(' query_expression ')' }",
	 "query_expression",
	 "See also: https://www.monetdb.org/documentation/user-guide/blog-archive/update-mergetables/"},
	{"partition_spec",
	 NULL,
	 "{ IN '(' partition_list ')' [ WITH NULL VALUES ]\n"
	 "| FROM partition_range_from TO partition_range_to [ WITH NULL VALUES ]\n"
	 "| FOR NULL VALUES }",
	 "partition_list,partition_range_from,partition_range_to",
	 "See also: https://www.monetdb.org/documentation/user-guide/blog-archive/update-mergetables/"},
	{"param",
	 NULL,
	 "ident data_type",
	 NULL,
	 NULL},
	{"partition_list",
	 NULL,
	 "query_expression [',' ...]",
	 "query_expression",
	 NULL},
	{"partition_range_from",
	 NULL,
	 "{ RANGE MINVALUE | query_expression }",
	 "query_expression",
	 NULL},
	{"partition_range_to",
	 NULL,
	 "{ RANGE MAXVALUE | query_expression }",
	 "query_expression",
	 NULL},
	{"pred_exp",
	 NULL,
	 "{ NOT pred_exp | predicate }",
	 "predicate",
	 NULL},
	{"predicate",
	 NULL,
	 "comparison_predicate | between_predicate | like_predicate | test_for_null | in_predicate | all_or_any_predicate | existence_test | filter_exp | scalar_exp",
	 NULL,
	 NULL},
	{"privileges",
	 NULL,
	 "table_privileges | EXECUTE ON function_type qname | global_privileges",
	 "function_type,table_privileges,global_privileges",
	 NULL},
	{"procedure_statement",
	 NULL,
	 "{ update_statement | declare_statement | set_statement | control_statement | select_single_row } ';'",
	 "update_statement,declare_statement,set_statement,control_statement,select_single_row",
	 NULL},
	{"select_single_row",
	 NULL,
	 "SELECT [ ALL | DISTINCT ] column_exp_commalist INTO select_target_list [ from_clause ] [ window_clause ] [ where_clause ] [ group_by_clause ] [ having_clause ]",
	 "column_exp_commalist,select_target_list,from_clause,window_clause,where_clause,group_by_clause,having_clause",
	 NULL},
	{"query_expression",
	 NULL,
	 "select_no_parens [ order_by_clause ] [ limit_offset_clause | offset_fetchfirst_clause ] [ sample_clause ]",
	 "select_no_parens",
	 NULL},
	{"select_no_parens",
	 NULL,
	 "{ SELECT [ ALL | DISTINCT ] column_exp_commalist [ from_clause ] [ window_clause ] [ where_clause ] [ group_by_clause ] [ having_clause ]\n"
	 "| select_no_parens { UNION | EXCEPT | INTERSECT } [ ALL | DISTINCT ] [ corresponding ] select_no_parens\n"
	 "| '(' select_no_parens ')' }",
	 "column_exp_commalist,from_clause,window_clause,where_clause,group_by_clause,having_clause,corresponding",
	 NULL},
	{"order_by_clause",
	 NULL,
	 "ORDER BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [',' ...]",
	 "",
	 NULL},
	{"limit_offset_clause",
	 NULL,
	 "[ LIMIT { count | param } ]  [ OFFSET { count | param } ]",
	 "",
	 NULL},
	{"offset_fetchfirst_clause",
	 NULL,
	 "[ OFFSET { count | param } [ {ROW|ROWS} ] ]  [ FETCH {FIRST|NEXT} [ count | param ] {ROW|ROWS} ONLY ]",
	 "",
	 NULL},
	{"corresponding",
	 NULL,
	 "{ CORRESPONDING | CORRESPONDING BY '(' column_ref_commalist ')' }",
	 "column_ref_commalist",
	 NULL},
	{"qname",
	 NULL,
	 "ident [ '.' ident ]",
	 NULL,
	 NULL},
	{"q3name",
	 NULL,
	 "ident [ '.' ident ['.' ident] ]",
	 NULL,
	 NULL},
	{"ref_action",
	 NULL,
	 "RESTRICT | CASCADE | NO ACTION | SET NULL | SET DEFAULT",
	 NULL,
	 NULL},
	{"ref_on_delete",
	 NULL,
	 "ON DELETE ref_action",
	 NULL,
	 NULL},
	{"ref_on_update",
	 NULL,
	 "ON UPDATE ref_action",
	 NULL,
	 NULL},
	{"reference_actions",
	 NULL,
	 "ref_on_delete | ref_on_update | ref_on_delete ref_on_update | ref_on_update ref_on_delete",
	 NULL,
	 NULL},
	{"return_statement",
	 "",
	 "RETURN { query_expression | search_condition | TABLE '(' query_expression ')' | NULL }",
	 "query_expression,search_condition",
	 NULL},
	{"row_values",
	 NULL,
	 "'(' atom [ ',' atom ]... ')' [ ',' row_values ] ...",
	 "atom",
	 NULL},
	{"schema_name",
	 NULL,
	 "ident | [ident] AUTHORIZATION authorization_ident",
	 NULL,
	 NULL},
	{"schema_element",
	 NULL,
	 "grant | revoke | create_statement | drop_statement | alter_statement",
	 NULL,
	 NULL},
	{"search_condition",
	 NULL,
	 "{ search_condition OR and_exp | and_exp }",
	 "and_exp",
	 NULL},
	{"separators",
	 NULL,
	 "[USING] DELIMITERS field_sep_string [',' record_sep_string [',' quote_string]]",
	 NULL,
	 NULL},
	{"split_part",
	 NULL,
	 "SPLIT_PART '(' string ',' delimiter_string ',' field_index ')'",
	 NULL,
	 NULL,},
	{"table_constraint",
	 NULL,
	 "[ CONSTRAINT ident ] { CHECK '(' search_condition ')' |\n"
	 "    PRIMARY KEY column_list | UNIQUE column_list | UNIQUE NULLS [ NOT ] DISTINCT column_list |\n"
	 "    FOREIGN KEY column_list REFERENCES qname [ column_list ] [ match_option ] [ reference_actions ] }",
	 "column_list,search_condition,reference_actions,match_option",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/table-elements/"},
	{"table_element",
	 NULL,
	 "column_def | table_constraint | column_option_list | LIKE qname",
	 "column_def,table_constraint,column_option_list",
	 NULL},
	{"table_name",
	 NULL,
	 "[AS] ident ['(' name [',' ...] ')' ]",
	 NULL,
	 NULL},
	{"table_privileges",
	 NULL,
	 "{ ALL [ PRIVILEGES ] | INSERT | DELETE | TRUNCATE\n"
	 "| { SELECT | UPDATE | REFERENCES } [ column_list ] } [',' ...] ON [ TABLE ] qname",
	 "column_list",
	 NULL},
	{"table_ref",
	 NULL,
	 "[LATERAL] func_ref [table_name] | [LATERAL] subquery | joined_table",
	 "table_name,subquery",
	 NULL},
	{"table_source",
	 NULL,
	 "'(' table_element [',' ...] ')' | column_list AS query_expression [ WITH [NO] DATA ] ",
	 "table_element,column_list,query_expression",
	 NULL},
	{"transaction_statement",
	 NULL,
	 "commit | savepoint | release | rollback | start transaction | set local transaction",
	 "commit,savepoint,release,rollback,start transaction,set local transaction",
	 NULL},
	{"time_precision",
	 NULL,
	 "'(' integer ')'",
	 NULL,
	 NULL},
	{"timestamp_precision",
	 NULL,
	 "'(' integer ')'",
	 NULL,
	 NULL},
	{"transactionmode",
	 NULL,
	 "{ READ ONLY | READ WRITE | ISOLATION LEVEL isolevel | DIAGNOSTICS integer } [ , ... ]",
	 "isolevel",
	 "Note: DIAGNOSTICS is not yet implemented"},
	{"trigger_reference",
	 NULL,
	 "{ OLD | NEW } { [ROW] | TABLE } [AS] ident",
	 NULL,
	 NULL},
	{"update_statement",
	 NULL,
	 "delete_stmt | truncate_stmt | insert_stmt | update_stmt | merge_stmt | copyfrom_stmt",
	 "delete_stmt,truncate_stmt,insert_stmt,update_stmt,merge_stmt,copyfrom_stmt",
	 NULL},
	{"triggered_action",
	 NULL,
	 "[ FOR EACH { ROW | STATEMENT } ]\n"
	 "[ WHEN '(' search_condition ')' ]\n"
	 "{ trigger_statement | BEGIN ATOMIC trigger_statement [ ; ... ] END }",
	 "trigger_statement,search_condition",
	 NULL},
	{"trigger_statement",
	 NULL,
	 "update_statement | declare_statement | set_statement | control_statement | select_single_row",
	 "update_statement,declare_statement,set_statement,control_statement,select_single_row",
	 NULL},
	{"when_statement",
	 NULL,
	 "WHEN scalar_expression THEN procedure_statement ...",
	 "procedure_statement",
	 NULL},
	{"while_statement",
	 NULL,
	 "[ident ':'] WHILE search_condition DO procedure_statement ... END WHILE [ident]",
	 "search_condition,procedure_statement",
	 "See also https://www.monetdb.org/documentation/user-guide/sql-programming/flow-of-control/"},
	{"window_aggregate_function",
	 NULL,
	 "{ AVG '(' query_expression ')' | COUNT '(' { '*' | query_expression } ')' | MAX '(' query_expression ')'\n"
	 "| MIN '(' query_expression ')' | PROD '(' query_expression ')' | SUM '(' query_expression ')' }",
	 "query_expression",
	 NULL},
	{"window_bound",
	 NULL,
	 "{ UNBOUNDED FOLLOWING | query_expression FOLLOWING | UNBOUNDED PRECEDING | query_expression PRECEDING | CURRENT ROW }",
	 "query_expression",
	 NULL},
	{"window_definition",
	 NULL,
	 "ident AS '(' window_specification ')'",
	 "window_specification",
	 NULL},
	{"window_frame_start",
	 NULL,
	 "{ UNBOUNDED PRECEDING | query_expression PRECEDING | CURRENT ROW }",
	 "query_expression",
	 NULL},
	{"window_rank_function",
	 NULL,
	 "{ CUME_DIST '(' ')' | DENSE_RANK '(' ')' | FIRST_VALUE '(' query_expression ')'\n"
	 "| LAG '(' query_expression [ ',' query_expression [ ',' query_expression ] ] ')' | LAST_VALUE '(' query_expression ')'\n"
	 "| LEAD '(' query_expression [ ',' query_expression [ ',' query_expression ] ] ')'\n"
	 "| NTH_VALUE '(' query_expression ',' query_expression ')' | NTILE '(' query_expression ')'\n"
	 "| PERCENT_RANK '(' ')' | RANK '(' ')' | ROW_NUMBER '(' ')' }",
	 "query_expression",
	 NULL},
	{"window_specification",
	 NULL,
	 "[ ident ]\n"
	 "[ PARTITION BY expression [',' ...] ]\n"
	 "[ ORDER BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [',' ...] ]\n"
	 "[ { ROWS | RANGE | GROUPS } { window_frame_start | BETWEEN window_bound AND window_bound }\n"
	 "  [ EXCLUDING { CURRENT ROW | GROUP | TIES | NO OTHERS } ] ]",
	 "window_bound,window_frame_start",
	 NULL},
	{"cte_list",
	 NULL,
	 "ident [ column_list ] AS query_expression [ ',' cte_list ] ...",
	 "column_list,query_expression",
	 NULL},
	{NULL, NULL, NULL, NULL, NULL}	/* End of list marker */
};

static const char *
sql_grammar_rule(const char *word, stream *toConsole)
{
	char buf[65], *s = buf;
	int i;
	while (s < buf + 64 && *word != ',' && *word && !isspace((unsigned char) *word))
		*s++ = *word++;
	*s = 0;

	for (i = 0; sqlhelp2[i].command; i++) {
		if (strcasecmp(sqlhelp2[i].command, buf) == 0) {
			if (sqlhelp2[i].syntax) {
				mnstr_printf(toConsole, "%s : %s\n", buf, sqlhelp2[i].syntax);
				if (sqlhelp2[i].synopsis)
					mnstr_printf(toConsole, "%.*s   %s\n", (int) (s - buf), "", sqlhelp2[i].synopsis);
			} else if (sqlhelp2[i].synopsis)
				mnstr_printf(toConsole, "%s : %s\n", buf, sqlhelp2[i].synopsis);
		}
	}
	while (*word && (isalnum((unsigned char) *word || *word == '_')))
		word++;
	while (*word && isspace((unsigned char) *word))
		word++;
	return *word == ',' ? word + 1 : NULL;
}

static void
sql_grammar(SQLhelp *sqlhelp, stream *toConsole)
{
	const char *t1;
	if (sqlhelp->synopsis == NULL) {
		mnstr_printf(toConsole, "%s : %s\n", sqlhelp->command, sqlhelp->syntax);
		if (sqlhelp->comments)
			mnstr_printf(toConsole, "%s\n", sqlhelp->comments);
		t1 = sqlhelp->rules;
		if (t1 && *t1)
			do
				t1 = sql_grammar_rule(t1, toConsole);
			while (t1);
		return;
	}
	if (sqlhelp->command)
		mnstr_printf(toConsole, "command  : %s\n", sqlhelp->command);
	if (sqlhelp->synopsis && *sqlhelp->synopsis)
		mnstr_printf(toConsole, "synopsis : %s\n", sqlhelp->synopsis);
	if (sqlhelp->syntax && *sqlhelp->syntax) {
		mnstr_printf(toConsole, "syntax   : ");
		for (t1 = sqlhelp->syntax; *t1; t1++) {
			if (*t1 == '\n')
				mnstr_printf(toConsole, "\n           ");
			else
				mnstr_printf(toConsole, "%c", *t1);
		}
		mnstr_printf(toConsole, "\n");
		t1 = sqlhelp->rules;
		if (t1 && *t1)
			do
				t1 = sql_grammar_rule(t1, toConsole);
			while (t1);
	}
	if (sqlhelp->comments)
		mnstr_printf(toConsole, "%s\n", sqlhelp->comments);
}

static void
sql_word(const char *word, size_t maxlen, stream *toConsole)
{
	size_t i;

	mnstr_printf(toConsole, "%s", word);
	for (i = strlen(word); i <= maxlen; i++)
		mnstr_printf(toConsole, " ");
}

void
sql_help(const char *pattern, stream *toConsole, int pagewidth)
{
	size_t maxlen = 1, len;
	int i, step, ncolumns, total = 0;

	if (*pattern == '\\')
		pattern++;
	while (*pattern && !isspace((unsigned char) *pattern)) {
		pattern++;
	}
	while (*pattern && isspace((unsigned char) *pattern)) {
		pattern++;
	}

	if (*pattern && *pattern != '*') {
		bool first = true;
		size_t patlen = strlen(pattern);
		/* ignore possible final newline in pattern */
		if (pattern[patlen - 1] == '\n')
			patlen--;
		for (i = 0; sqlhelp1[i].command; i++)
			if (strncasecmp(sqlhelp1[i].command, pattern, patlen) == 0) {
				if (!first)
					mnstr_printf(toConsole, "\n");
				sql_grammar(&sqlhelp1[i], toConsole);
				first = false;
			}
		for (i = 0; sqlhelp2[i].command; i++)
			if (strncasecmp(sqlhelp2[i].command, pattern, patlen) == 0) {
				if (!first)
					mnstr_printf(toConsole, "\n");
				sql_grammar(&sqlhelp2[i], toConsole);
				first = false;
			}
		return;
	}

	// collect the major topics
	for (i = 0; sqlhelp1[i].command; i++) {
		total++;
		if ((len = strlen(sqlhelp1[i].command)) > maxlen)
			maxlen = len;
	}
	// provide summary of all major topics  (=search terms)
	ncolumns = (int) maxlen > pagewidth ? 1 : (int) (pagewidth / maxlen);
	if (ncolumns > 1 && ncolumns * (int) maxlen + ncolumns - 1 > pagewidth)
		ncolumns--;
	step = total / ncolumns;
	if(total % ncolumns) {
		step++;
	}
	for (i = 0; i < step; i++) {
		for (int j = 0; j < ncolumns; j++) {
			size_t nextNum = i + j * step;
			if(nextNum < sizeof(sqlhelp1)/sizeof(sqlhelp1[0]) - 1) {
				sql_word(sqlhelp1[nextNum].command, j < ncolumns - 1 ? maxlen : 0, toConsole);
			}
		}
		mnstr_printf(toConsole, "\n");
	}
	mnstr_printf(toConsole,
		"Using the conventional grammar constructs:\n"
		"[ A | B ]    token A or B or none\n"
		"{ A | B }    exactly one of the options A or B should be chosen\n"
		"A [',' ...]       a comma separated list of A elements\n"
		"{ A | B } ...     a series of A and B's\n"
		"{ A B } [',' ...] a series of A B,A B,A B,A B\n"
		"For more search terms type: \\help *\n"
		"See also https://www.monetdb.org/documentation/user-guide/sql-manual/\n");
}
