/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/**
 * 2016 Martin Kersten
 *
 * The SQL syntax help synopsis.
 */

/* produce a synposis of the SQL syntax, inspired by a competing product.
 * Use the conventional grammar constructs:
 * [ A | B ]   optionally token A or B or none
 * { A | B }   exactly one of the options should be chosen
 * A [ ',' ...]  a comma separate lists of A elements
 * { A | B } ... a series of A and Bs
 * ( A B ) [','...] a series of AB,AB,AB,AB
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

#define NUMBER_MAJOR_COMMANDS 73 // The number of major commands to show in case of no query

SQLhelp sqlhelp[] = {
	// major commands
	{"ALTER TABLE",
	 "",
	 "ALTER TABLE qname ADD [COLUMN] { column_def | table_constraint }\n"
	 "ALTER TABLE qname ALTER [COLUMN] ident SET DEFAULT value\n"
	 "ALTER TABLE qname ALTER [COLUMN] ident SET [NOT] NULL\n"
	 "ALTER TABLE qname ALTER [COLUMN] ident DROP DEFAULT\n"
	 "ALTER TABLE qname ALTER [COLUMN] ident SET STORAGE {string | NULL}\n"
	 "ALTER TABLE qname DROP [COLUMN] ident [RESTRICT | CASCADE]\n"
	 "ALTER TABLE qname DROP CONSTRAINT ident [RESTRICT | CASCADE]\n"
	 "ALTER TABLE qname SET { { READ | INSERT } ONLY | READ WRITE }",
	 "column_def,table_constraint",
	 "See also https://www.monetdb.org/Documentation/SQLreference/Alter"},
	{"ALTER MERGE TABLE",
	 "",
	 "ALTER TABLE qname ADD TABLE qname\n"
	 "ALTER TABLE qname DROP TABLE qname [RESTRICT | CASCADE]\n",
	 "",
	 "See also https://www.monetdb.org/Documentation/Cookbooks/SQLrecipes/DataPartitioning"},
	{"ALTER SEQUENCE",
	 "",
	 "ALTER SEQUENCE ident [ AS datatype] [ RESTART [WITH start]] [INCREMENT BY increment]\n"
	 "[MINVALUE minvalue | NO MINVALUE]  [MAXVALUE maxvalue | NOMAXVALUE] | [ [ NO] CYCLE]",
	 NULL,
	 "See also https://www.monetdb.org/Documentation/Manuals/SQLreference/SerialTypes"},
	{"ALTER USER",
	 "",
	 "ALTER USER ident  WITH [ ENCRYPTED | UNENCRYPTED] PASSWORD string\n"
	 "ALTER USER ident  SET SCHEMA ident\n"
	 "ALTER USER ident  WITH [ENCRYPTED | UNENCRYPTED] PASSWORD SET SCHEMA ident\n"
	 "ALTER USER RENAME TO ident\n"
	 "ALTER USER SET [ ENCRYPTED | UNENCRYPTED] PASSWORD string USING OLD PASSWORD string",
	 NULL,
	 "See also https://www.monetdb.org/Documentation/SQLreference/Users"},
	{"ANALYZE",
	 "Collect column data statistics for optimizations",
	 "ANALYZE schemaname [ . tablename [ column_list ] ] [SAMPLE size] [MINMAX]",
	 "column_list",
	 "See also https://www.monetdb.org/Documentation/Cookbooks/SQLrecipes/statistics"},
	{"CALL",
	 "",
	 "CALL qname '(' [ [scalar_expression ] [ ',' ...]  ]')' | CALL ident '.' ident",
	 NULL,
	 NULL},
	{"CASE",
	 "Case statement for procedures/functions",
	 "CASE scalar_expression [ when_statement ...]  [ELSE procedure_statement ... ] END CASE",
	 NULL,
	 "See also https://www.monetdb.org/Documentation/SQLreference/Flowofcontrol"},
	{"COMMENT",
	 "Add, update or remove a comment or description for a database object",
	 "COMMENT ON [ SCHEMA | TABLE | VIEW | COLUMN | INDEX | SEQUENCE |\n"
	 "           FUNCTION | PROCEDURE | AGGREGATE | FILTER FUNCTION | LOADER ]\n"
	 "     qname IS [ 'my description text' | NULL | '' ]",
	 NULL,
	 NULL},
	{"COMMIT",
	 "Commit the current transaction",
	 "COMMIT [ WORK ] [ AND CHAIN | AND NO CHAIN ]",
	 NULL,
	 NULL},
	{"COPY BINARY",
	 "Append binary representations into a table",
	 "COPY [nrofrecords] BINARY INTO qname [column_list] FROM string [','...] [NO CONSTRAINT]",
	 "nrofrecords",
	 "See also https://www.monetdb.org/Documentation/Cookbooks/SQLrecipes/BinaryBulkLoad"},
	{"COPY INTO",
	 "Parse a csv-file into a table",
	 "COPY [nrofrecords] INTO qname [column_list] FROM string [','...] [headerlist] [ separators]\n"
	 " [NULL [AS] string] [LOCKED] [BEST EFFORT] [NO CONSTRAINT] [FWF '(' integer [','...]')'\n"
	 "COPY [nrofrecords] INTO qname [column_list] FROM STDIN [headerlist] [ separators]\n"
	 " [NULL [AS] string] [LOCKED] [BEST EFFORT] [NO CONSTRAINT]\n"
	 "COPY query_expression INTO [STDOUT | string] [seps] [NULL [AS] string]",
	 "nrofrecords,headerlist,separators",
	 "See also https://www.monetdb.org/Documentation/Cookbooks/SQLrecipes/LoadingBulkData"},
	{"COPY LOADER",
	 "Copy into using a user supplied parsing function",
	 "COPY LOADER INTO qname FROM qname '(' [ scalar_expression ... ] ')'",
	 NULL,
	 NULL},
	{"CREATE AGGREGATE FUNCTION",
	 "",
	 "CREATE [ OR REPLACE ] AGGREGATE FUNCTION qname '(' { '*' | [ param [',' ...]] } ')'\n"
	 "    RETURNS { data_type | TABLE '(' function_return [ ',' ... ] ')' }\n"
	 "    EXTERNAL NAME ident ',' ident\n"
	 "CREATE [ OR REPLACE ] AGGREGATE FUNCTION qname '(' { '*' | [ param [',' ...]] }')'\n"
	 "    RETURNS { data_type | TABLE '(' function_return [ ',' ... ] ')' }\n"
	 "    LANGUAGE ident external_code",
	 "param,data_type,function_return",
	 NULL},
	{"CREATE FILTER FUNCTION",
	 "",
	 "CREATE [ OR REPLACE ] FILTER FUNCTION qname '(' { '*' | [ param [',' ...]] } ')'\n"
	 "    RETURNS { data_type | TABLE '(' function_return [ ',' ... ] ')' }\n"
	 "    EXTERNAL NAME ident ',' ident",
	 "param,data_type,function_return",
	 NULL},
	{"CREATE FUNCTION",
	 "",
	 "CREATE [ OR REPLACE ] FUNCTION qname '(' { '*' | [ param [',' ...]] } ')'\n"
	 "    RETURNS { data_type | TABLE '(' function_return [ ',' ... ] ')' }\n"
	 "    EXTERNAL NAME ident ',' ident\n"
	 "CREATE [ OR REPLACE ] FUNCTION qname '(' { '*' | [ param [',' ...]] } ')'\n"
	 "    RETURNS { data_type | TABLE '(' function_return [ ',' ... ] ')' }\n"
	 "    BEGIN [ ATOMIC ] statement [ ';' ...] END\n"
	 "CREATE [ OR REPLACE ] FUNCTION qname '(' { '*' | [ param [',' ...]] }')'\n"
	 "    RETURNS { data_type | TABLE '(' function_return [ ',' ... ] ')' }\n"
	 "    LANGUAGE ident external_code",
	 "param,data_type,function_return,external_code",
	 NULL},
	{"CREATE INDEX",
	 "",
	 "CREATE [ UNIQUE | ORDERED | IMPRINTS ] INDEX ident ON qname '(' ident_list ')'",
	 NULL,
	 NULL},
	{"CREATE PROCEDURE",
	 "",
	 "CREATE [ OR REPLACE ] PROCEDURE qname '(' { '*' | [ param [',' ...]] }')'\n"
	 "    EXTERNAL NAME ident ',' ident\n"
	 "CREATE [ OR REPLACE ] PROCEDURE qname '(' { '*' | [ param [',' ...]] } ')'\n"
	 "    BEGIN [ ATOMIC ] procedure_statement [ ';' ...] END\n"
	 "CREATE [ OR REPLACE ] PROCEDURE qname '(' { '*' | [ param [',' ...]] } ')'\n"
	 "    LANGUAGE ident external_code",
	 "param,data_type,external_code",
	 NULL},
	{"CREATE LOADER",
	 "",
	 "CREATE [ OR REPLACE ] LOADER qname '(' [ param [',' ...]] ')'\n"
	 "    LANGUAGE ident external_code",
	 "param,data_type,function_return,external_code",
	 NULL},
	{"CREATE MERGE TABLE",
	 "",
	 "CREATE MERGE TABLE [ IF NOT EXISTS ] qname table_source;",
	 NULL,
	 "See also https://www.monetdb.org/Documentation/Cookbooks/SQLrecipes/DataPartitioning"},
	{"CREATE REMOTE TABLE",
	 "",
	 "CREATE REMOTE TABLE [ IF NOT EXISTS ] qname ON string",
	 NULL,
	 "remote name should match mapi:monetdb://host:port/database[/schema[/table]]"},
	{"CREATE REPLICA TABLE",
	 "",
	 "CREATE REPLICA TABLE [ IF NOT EXISTS ] qname table_source;",
	 NULL,
	 "See also https://www.monetdb.org/Documentation/Cookbooks/SQLrecipes/TransactionReplication"},
	{"CREATE SCHEMA",
	 "",
	 "CREATE SCHEMA [ IF NOT EXISTS ] schema_name [default_char_set] [path_spec] [schema_element]",
	 "schema_name,default_char_set,path_spec,schema_element",
	 NULL},
	{"CREATE SEQUENCE",
	 "Define a new sequence generator",
	 "CREATE SEQUENCE ident [ AS datatype] [ START [WITH start]] [INCREMENT BY increment]\n"
	 "[MINVALUE minvalue | NO MINVALUE] [MAXVALUE maxvalue | NOMAXVALUE] | [ [ NO] CYCLE]",
	 NULL,
	 "See also https://www.monetdb.org/Documentation/Manuals/SQLreference/SerialTypes"},
	{"CREATE STREAM TABLE",
	 "Temporary table, locked during updates/ continues query processing",
	 "CREATE STREAM TABLE [ IF NOT EXISTS ] qname table_source\n",
	 NULL,
	 NULL},
	{"CREATE TABLE",
	 "",
	 "CREATE TABLE [ IF NOT EXISTS ] qname table_source [STORAGE ident string]\n"
	 "CREATE TABLE [ IF NOT EXISTS ] qname FROM LOADER function_ref\n"
	 "CREATE [ LOCAL | GLOBAL ] TEMP[ORARY] TABLE [ IF NOT EXISTS ] qname table_source [on_commit]",
	 "table_source,on_commit,function_ref",
	 NULL},
	{"CREATE TRIGGER",
	 "",
	 "CREATE [ OR REPLACE ] TRIGGER wname { BEFORE | AFTER } { INSERT | DELETE | TRUNCATE ...\n"
	 " | UPDATE [ OF ident [',' ident]] } ON qname REFERENCING trigger_reference... triggered_action",
	 "trigger_reference",
	 NULL},
	{"CREATE TYPE",
	 "Add user defined type to the type system ",
	 "CREATE TYPE qname EXTERNAL NAME ident",
	 NULL,
	 NULL},
	{"CREATE VIEW",
	 "",
	 "CREATE [ OR REPLACE ] VIEW qname [ column_list ] AS { query_expression | '(' query_expression ')' }\n"
	 "[ WITH CHECK OPTION ]",
	 "column_list,query_expression",
	 NULL},
	{"CURRENT_DATE",
	 "Built-in function",
	 "CURRENT_DATE [ '(' ')']",
	 NULL,
	 NULL},
	{"CURRENT_TIME",
	 "Built-in function",
	 "CURRENT_TIME [ '(' ')']",
	 NULL,
	 NULL},
	{"CURRENT_TIMESTAMP",
	 "Built-in function",
	 "CURRENT_TIMESTAMP [ '(' ')']",
	 NULL,
	 NULL},
	{"EXPLAIN",
	 "Give execution plan details",
	 "EXPLAIN statement",
	 NULL,
	 "See alsp https://www.monetdb.org/Documentation/Manuals/SQLreference/Explain"},
	{"LOCAL_TIMESTAMP",
	 "Built-in function",
	 "LOCAL_TIMESTAMP [ '(' ')']",
	 NULL,
	 NULL},
	{"EXTRACT",
	 "Built-in function",
	 "EXTRACT '(' { YEAR | MONTH | DAY | HOUR | MINUTE | SECOND } FROM scalar_expression ')'",
	 NULL,
	 NULL},
	{"DECLARE",
	 "Define a local variable",
	 "DECLARE ident_list data_type",
	 "ident_list,data_type",
	 NULL},
	{"DELETE",
	 "",
	 "[ WITH with_list] DELETE FROM qname [WHERE search_condition]",
	 "with_list,search_condition",
	 NULL},
	{"DROP AGGREGATE",
	 "",
	 "DROP ALL AGGREGATE qname [ RESTRICT | CASCADE ]\n"
	 "DROP AGGREGATE [ IF EXISTS ] qname [ '(' [ param [',' ...]] ')' ] [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"DROP FUNCTION",
	 "",
	 "DROP ALL [FILTER] FUNCTION qname [ RESTRICT | CASCADE ]\n"
	 "DROP [FILTER] FUNCTION [ IF EXISTS ] qname [ '(' [ param [',' ...]] ')' ] [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"DROP INDEX",
	 "",
	 "DROP INDEX qname",
	 NULL,
	 NULL},
	{"DROP LOADER",
	 "",
	 "DROP ALL LOADER qname [ RESTRICT | CASCADE ]\n"
	 "DROP LOADER [ IF EXISTS ] qname [ '(' [ param [',' ...]] ')' ] [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"DROP PROCEDURE",
	 "",
	 "DROP ALL PROCEDURE qname [ RESTRICT | CASCADE ]\n"
	 "DROP PROCEDURE [ IF EXISTS ] qname [ '(' [ param [',' ...]] ')' ] [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"DROP ROLE",
	 "",
	 "DROP ROLE ident",
	 NULL,
	 NULL},
	{"DROP SCHEMA",
	 "",
	 "DROP SCHEMA [ IF EXISTS ] qname [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"DROP SEQUENCE",
	 "",
	 "DROP SEQUENCE qname",
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
	 "DROP TYPE qname [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"DROP USER",
	 "",
	 "DROP USER ident",
	 NULL,
	 NULL},
	{"DROP VIEW",
	 "",
	 "DROP VIEW [ IF EXISTS ] qname [ RESTRICT | CASCADE ]",
	 NULL,
	 NULL},
	{"IF",
	 "",
	 "IF search_condition THEN procedure_statement ...\n"
	 "[ELSE IF search_condition THEN procedure_statement ... ]...\n"
	 "[ ELSE procedure_statement ... ] END IF",
	 "search_condition,procedure_statement",
	 "See also https://www.monetdb.org/Documentation/SQLreference/Flowofcontrol"},
	{"INSERT",
	 "",
	 "[WITH with_list ] INSERT INTO qname [ column_list ] [ DEFAULT VALUES | VALUES row_values | query_expression]",
	 "with_list,column_list,row_values,query_expression",
	 "See also https://www.monetdb.org/Documentation/SQLreference/Updates"},
	{"GRANT",
	 "Define access privileges",
	 "GRANT privileges TO grantee_list [ WITH GRANT OPTION ]"
	 "GRANT authid [',' ... ] TO grantee_list [ WITH ADMIN OPTION]",
	 "grantee_list,authid",
	 NULL},
	{"RELEASE SAVEPOINT",
	 "",
	 "RELEASE SAVEPOINT ident",
	 NULL,
	 NULL},
	{"RETURN",
	 "",
	 "RETURN { query_expression | search_condition | TABLE '(' query_expression ')'",
	 NULL,
	 NULL},
	{"REVOKE",
	 "Remove some privileges",
	 "REVOKE [GRANT OPTION FOR] privileges FROM grantee [ ',' ... ] [ FROM [CURRENT_USER | CURRENT_ROLE]]\n"
	 "REVOKE [ADMIN OPTION FOR] authid [ ',' ... ] FROM grantee [ ',' ... ] [ FROM [CURRENT_USER | CURRENT_ROLE]]",
	 "privileges,authid,grantee",
	 NULL},
	{"ROLLBACK",
	 "Rollback the current transaction",
	 "ROLLBACK [WORK] [ AND CHAIN | AND NO CHAIN ] [TO SAVEPOINT ident]",
	 NULL,
	 NULL},
	{"SAVEPOINT",
	 NULL,
	 "SAVEPOINT ident",
	 NULL,
	 NULL},
	{"SELECT",
	 "",
	 "[ WITH with_list ]\n"
	 "SELECT [ ALL | DISTINCT [ ON ( expression [',' ...] ) ] ]\n"
	 "[ '*' | expression [ [ AS ] output_name ] [',' ...] ]\n"
	 "[ FROM from_item [',' ...] ]\n"
	 "[ WHERE condition ]\n"
	 "[ GROUP BY grouping_element ',', ...] ]\n"
	 "[ HAVING condition [',' ...] ]\n"
	 "[ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] select ]\n"
	 "[ ORDER BY expression [ ASC | DESC ] [',' ...] ]\n"
	 "[ LIMIT { count | param } ]\n"
	 "[ OFFSET { count | param} ]\n"
	 "[ SAMPLE size ]\n"
	 " select_expression {UNION | INTERSECT | EXCEPT} [ALL | DISTINCT] [CORRESPONDING] select_expression",
	 "",
	 "See also https://www.monetdb.org/Documentation/SQLreference/TableExpressions"},
	{"SET",
	 "Assign a value to a variable or column",
	 "SET '=' simple_atom",
	 "simple_atom",
	 NULL},
	{"SET LOCAL TRANSACTION",
	 "",
	 "START LOCAL TRANSACTION transactionmode",
	 "transactionmode,isolevel",
	 "DIAGNOSTICS is not yet supported"},
	{"SET ROLE",
	 "",
	 "SET ROLE ident",
	 NULL,
	 NULL},
	{"SET SCHEMA",
	 "",
	 "SET SCHEMA ident",
	 NULL,
	 NULL},
	{"SET SESSION AUTHORIZATION",
	 "",
	 "SESSION AUTHORIZATIO ident",
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
	 "SET TRANSACTION transactionmode",
	 "transactionmode,isolevel",
	 "DIAGNOSTICS is not yet supported"},
	{"START TRANSACTION",
	 "",
	 "START TRANSACTION transactionmode",
	 "transactionmode,isolevel",
	 "DIAGNOSTICS is not yet supported"},
	{"SET USER",
	 "",
	 "SET USER '=' ident",
	 NULL,
	 NULL},
	{"TABLE JOINS",
	 "",
	 "'(' joined_table ') |\n"
	 "table_ref CROSS JOIN table_ref ')' |\n"
	 "table_ref NATURAL [ INNER | LEFT | RIGHT | FULL] JOIN table_ref |\n"
	 "table_ref UNION JOIN table_ref { ON search_condition | USING column_list } |\n"
	 "table_ref [ INNER | LEFT | RIGHT | FULL] JOIN table_ref { ON search_condition | USING column_list } |\n",
	 NULL,
	 "See also https://www.monetdb.org/Documentation/SQLreference/TableExpressions"},
	{"TRACE",
	 "Give execution trace",
	 "TRACE statement",
	 NULL,
	 NULL},
	{"TRUNCATE",
	 "",
	 "TRUNCATE [ TABLE ] qname [ CONTINUE IDENTITY | RESTART IDENTITY ] [ CASCADE | RESTRICT ]",
	 "",
	 NULL},
	{"UPDATE",
	 "",
	 "[WITH with_list] UPDATE qname SET assignment_list [FROM from_clause] [WHERE search_condition]",
	 "with_list,assignment_list,from_clause,search_condition",
	 NULL},
	{"WHILE",
	 "",
	 "[ident ':'] WHILE search_condition DO procedure_statement ... END WHILE [ident]",
	 NULL,
	 "See also https://www.monetdb.org/Documentation/SQLreference/Flowofcontrol"},
	{"WINDOW",
	 "",
	 "{RANK | DENSE_RANK | PERCENT_RANK | CUME_DIST} OVER window_name |\n"
	 "{RANK | DENSE_RANK | PERCENT_RANK | CUME_DIST} OVER '('\n"
	 "[window_name] [PARTITION BY column_ref ... ]\n"
	 "[ORDER BY sort_spec]\n"
	 "{ROWS | RANGE} {UNBOUNDED PRECEDING | value PRECEDING | CURRENT ROW}\n"
	 "[BETWEEN {UNBOUNDED FOLLOWING | value FOLLOWING | UNBOUNDED PRECEDING | value PRECEDING | CURRENT ROW}\n"
	 "AND {UNBOUNDED FOLLOWING | value FOLLOWING | UNBOUNDED PRECEDING | value PRECEDING | CURRENT ROW} ]\n"
	 "[EXCLUDING {CURRENT ROW | GROUP | TIES | NO OTHERS}",
	 NULL,
	 "See also https://www.monetdb.org/Documentation/Manuals/SQLreference/WindowFunctions"},

// The subgrammar rules
	{"assignment_list",
	 NULL,
	 "column '=' DEFAULT | column '=' search_condition | '(' column [','...] ')' '=' subquery",
	 "search_condition,column,subquery",
	 NULL},
	{"authid",
	 NULL,
	 "restricted ident",
	 NULL,
	 NULL},
	{"column_def",
	 NULL,
	 "COLUMN [ SERIAL | BIGSERIAL ] | COLUMN data_type [ column_option ...]",
	 "column_option",
	 NULL},
	{"column_list",
	 NULL,
	 "'(' ident [ ',' ... ] ')'",
	 NULL,
	 NULL},
	{"column_option",
	 NULL,
	 "DEFAULT value | column_constraint |generated_column",
	 "column_constraint,generated_column",
	 NULL},
	{"column_option_list",
	 NULL,
	 "ident WITH OPTIONS '(' column_constraint ')' [ ',' ... ]",
	 "column_constraint",
	 NULL},
	{"column_constraint",
	 NULL,
	 "[ [ NOT] NULL | UNIQUE | PRIMARY KEY | CHECK '(' search_condition ')'\n"
	 "| REFERENCES qname [ column_list ][ MATCH [ FULL | PARTIAL | SIMPLE]] reference_action ...\n",
	 "reference_action",
	 NULL},
	{"control_statement",
	 NULL,
	 "call_procedure | while_statement | if_statement | case_statement | return_statement",
	 "call_procedure | while_statement | if_statement | case_statement | return_statement",
	 NULL},
	{"datetime_type",
	 NULL,
	 "DATE | TIME [ time_precision ] tz | TIMESTAMP [ timestamp_precision ] tz",
	 "time_precision,timestamp_precision,tz",
	 NULL},
	{"data_type",
	 NULL,
	 "[ [ CHAR[ACTER] | VARCHAR | CLOB | TEXT | BLOB] [ '(' nonzero ')' ] |\n"
	 "TINYINT | SMALLINT | INT[EGER] | BIGINT | HUGEINT | [ DECIMAL | FLOAT] [ '(' nonzero [',' nonzero ] ')'] |\n"
	 " DOUBLE [ PRECISION ] | REAL | datetime_type | interval_type | geometry_type",
	 "datetime_type,interval_type,geometry_type",
	 NULL},
	{"default_char_set",
	 NULL,
	 "DEFAULT CHARACTER SET ident",
	 NULL,
	 NULL},
	{"drop_table_element",
	 NULL,
	 "{ CONSTRAINT | TABLE | COLUMN | } ident [ { RESTRICT | CASCADE } ]",
	 NULL,
	 NULL},
	{"end_time",
	 NULL,
	 "SECOND timestamp_precision\n,timestamp_precision",
	 NULL,
	 NULL},
	{"function_return",
	 NULL,
	 "ident data_type",
	 NULL,
	 NULL},
	{"generated_column",
	 NULL,
	 "AUTO_INCREMENT | GENERATED ALWAYS AS IDENTITY [ '(' [ AS datatype] [ START [WITH start]] [INCREMENT BY increment]\n"
	 "[MINVALUE minvalue | NO MINVALUE] [MAXVALUE maxvalue | NOMAXVALUE] | [ [ NO] CYCLE] ')' ] ",
	 NULL,
	 "See also https://www.monetdb.org/Documentation/Manuals/SQLreference/SerialTypes"},
	{"global_privileges",
	 NULL,
	 "{ COPY FROM | COPY INTO } [ ',' ... ]",
	 NULL,
	 NULL},
	{"grantee",
	 NULL,
	 "{ PUBLIC | authid } ",
	 "authid",
	 NULL},
	{"headerlist",
	 NULL,
	 "'(' ( ident [string] ) [',' ...]",
	 NULL,
	 NULL},
	{"ident",
	 "An identifier",
	 NULL,
	 NULL,
	 NULL},
	{"ident_list",
	 NULL,
	 "ident [ ',' ... ]",
	 NULL,
	 NULL},
	{"interval",
	 NULL,
	 "INTERVAL [ '+' | '-' ] string start_field TO end_field",
	 "start_field,end_time",
	 NULL},
	{"intval",
	 "Integer value",
	 NULL,
	 NULL,
	 NULL},
	{"isolevel",
	 NULL,
	 "READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE ",
	 NULL,
	 NULL},
	{"nrofrecords",
	 "",
	 "OFFSET integer | integer RECORDS | integer OFFSET integer RECORDS | integer RECORDS OFFSET integer",
	 NULL,
	 NULL},
	{"on_commit",
	 NULL,
	 "ON COMMIT { DELETE ROWS | PRESERVE ROWS | DROP }",
	 NULL,
	 NULL},
	{"param",
	 NULL,
	 "ident data_type",
	 NULL,
	 NULL},
	{"privileges",
	 NULL,
	 "{ ALL [PRIVILEGES ] | INSERT | DELETE | EXECUTE | REFERENCES | SELECT | TRUNCATE | UPDATE } column_list ON "
	 " { [TABLE] qname | routine_designator } | global_privileges",
	 "global_privileges,routine_designator",
	 NULL},
	{"procedure_statement",
	 NULL,
	 "{transaction_statement | update_statement | grant | revoke | declare | set_statement | control_statement | select_single_row} ';'",
	 "transaction_statement | update_statement | grant | revoke | declare | set_statement | control_statement | select_single_row",
	 NULL},
	{"query_expression",
	 NULL,
	 "with_query | select_no_parens_orderby",
	 NULL,
	 NULL},
	{"qname",
	 NULL,
	 "ident [ '.' ident ['.' ident]]",
	 NULL,
	 NULL},
	{"reference_action",
	 NULL,
	 "ON { UPDATE | DELETE } {NO ACTION | CASCADE | RESTRICT | SET NULL | SET DEFAULT}",
	 NULL,
	 NULL},
	{"row_values",
	 NULL,
	 "'(' atom [ ',' atom]... ')' [ ',' row_values] ...",
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
	 "CONSTRAINT [ ident ] { UNIQUE | PRIMARY KEY } column_list | FOREIGN KEY } column_list REFERENCES qname [ column_list ][ MATCH [ FULL | PARTIAL | SIMPLE]]",
	 NULL,
	 NULL},
	{"table_element",
	 NULL,
	 "column_def | table_constraint | column_option_list | LIKE qname",
	 "column_def,table_constraint,column_option_list",
	 NULL},
	{"table_name",
	 NULL,
	 "[AS] ident ['(' name [','...] ')' ]",
	 NULL,
	 NULL},
	{"table_ref",
	 NULL,
	 "[LATERAL] func_ref [table_name] | [LATERAL] subquery | joined_table",
	 NULL,
	 NULL},
	{"table_source",
	 NULL,
	 "'(' table_element [ ',' ... ] ')' | column_list AS query_expression [ WITH [NO] DATA ] ",
	 "table_element",
	 NULL},
	{"transaction_statement",
	 NULL,
	 "commit | savepoint | release | rollback | start transaction | set local transaction",
	 "commit,savepoint,release,rollback,start transaction,set local transaction",
	 NULL},
	{"time_precision",
	 NULL,
	 "'(' YEAR | MONTH | DAY | HOUR | MINUTE ')'",
	 NULL,
	 NULL},
	{"timestamp_precision",
	 NULL,
	 "'(' integer ')'",
	 NULL,
	 NULL},
	{"transactionmode",
	 NULL,
	 "{ READ ONLY | READ WRITE | ISOLATION LEVEL isolevel | DIAGNOSTICS intval} [ , ... ]",
	 NULL,
	 NULL},
	{"trigger_reference",
	 NULL,
	 "OLD [ROW] [AS] ident | NEW [ROW] [AS] ident",
	 NULL,
	 NULL},
	{"update_statement",
	 NULL,
	 "delete_stmt | truncate_stmt | insert_stmt | update_stmt | copyfrom_stmt",
	 "delete_stmt | truncate_stmt | insert_stmt | update_stmt | copyfrom_stmt",
	 NULL},
	{"triggered_action",
	 NULL,
	 "[ FOR EACH { ROW | STATEMENT } ] [ WHEN '(' search_condition ')'\n"
	 "BEGIN ATOMIC trigger_statement ... END ",
	 NULL,
	 NULL},
	{"trigger_statement",
	 NULL,
	 "transaction_statement | update_statement | grant | revoke | declare_statement | set_statement | control_statement | select_single_row",
	 "transaction_statement,update_statement,grant,revoke,declare_statement,set_statement,control_statement,select_single_row",
	 NULL},
	{"when_statement",
	 NULL,
	 "WHEN scalar_expression THEN procedure_statement ...",
	 NULL,
	 NULL},
	{"with_list",
	 NULL,
	 "ident [ column_list] AS  [',' with_list]...",
	 NULL,
	 NULL},
	{NULL, NULL, NULL, NULL, NULL}	/* End of list marker */
};

#ifndef HAVE_STRNCASECMP
static int
strncasecmp(const char *s1, const char *s2, size_t n)
{
	int c1, c2;

	while (n > 0) {
		c1 = (unsigned char) *s1++;
		c2 = (unsigned char) *s2++;
		if (c1 == 0)
			return -c2;
		if (c2 == 0)
			return c1;
		if (c1 != c2 && tolower(c1) != tolower(c2))
			return tolower(c1) - tolower(c2);
		n--;
	}
	return 0;
}
#endif

static const char *
sql_grammar_rule(const char *word, stream *toConsole)
{
	char buf[65], *s = buf;
	size_t buflen;
	int i;
	while (s < buf + 64 && *word != ',' && *word && !isspace((unsigned char) *word))
		*s++ = *word++;
	*s = 0;
	buflen = (size_t) (s - buf);

	for (i = 0; sqlhelp[i].command; i++) {
		if (strncasecmp(sqlhelp[i].command, buf, buflen) == 0 && sqlhelp[i].synopsis == NULL) {
			mnstr_printf(toConsole, "%s : %s\n", buf, sqlhelp[i].syntax);
		}
	}
	while (*word && (isalnum((unsigned char) *word || *word == '_')))
		word++;
	while (*word && isspace((unsigned char) *word))
		word++;
	return *word == ',' ? word + 1 : NULL;
}

static void
sql_grammar(int idx, stream *toConsole)
{
	const char *t1;
	if (sqlhelp[idx].synopsis == NULL) {
		mnstr_printf(toConsole, "%s : %s\n", sqlhelp[idx].command, sqlhelp[idx].syntax);
		if (sqlhelp[idx].comments)
			mnstr_printf(toConsole, "%s\n", sqlhelp[idx].comments);
		return;
	}
	if (sqlhelp[idx].command)
		mnstr_printf(toConsole, "command  : %s\n", sqlhelp[idx].command);
	if (sqlhelp[idx].synopsis && *sqlhelp[idx].synopsis)
		mnstr_printf(toConsole, "synopsis : %s\n", sqlhelp[idx].synopsis);
	if (sqlhelp[idx].syntax && *sqlhelp[idx].syntax) {
		mnstr_printf(toConsole, "syntax   : ");
		for (t1 = sqlhelp[idx].syntax; *t1; t1++) {
			if (*t1 == '\n')
				mnstr_printf(toConsole, "\n           ");
			else
				mnstr_printf(toConsole, "%c", *t1);
		}
		mnstr_printf(toConsole, "\n");
		t1 = sqlhelp[idx].rules;
		if (t1 && *t1)
			do
				t1 = sql_grammar_rule(t1, toConsole);
			while (t1);
	}
	if (sqlhelp[idx].comments)
		mnstr_printf(toConsole, "%s\n", sqlhelp[idx].comments);
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
		int first = 1;
		size_t patlen = strlen(pattern);
		/* ignore possible final newline in pattern */
		if (pattern[patlen - 1] == '\n')
			patlen--;
		for (i = 0; *pattern && sqlhelp[i].command; i++)
			if (strncasecmp(sqlhelp[i].command, pattern, patlen) == 0) {
				if (!first)
					mnstr_printf(toConsole, "\n");
				sql_grammar(i, toConsole);
				first = 0;
			}
		return;
	}
	// collect the major topics
	for (i = 0; sqlhelp[i].command; i++) {
		if (islower((unsigned char) sqlhelp[i].command[0]) && *pattern != '*')
			break;
		total++;
		if ((len = strlen(sqlhelp[i].command)) > maxlen)
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
		int j;
		for (j = 0; j < ncolumns; j++) {
			int nextNum = i + j * step;
			if(nextNum < NUMBER_MAJOR_COMMANDS) {
				sql_word(sqlhelp[nextNum].command, j < ncolumns - 1 ? maxlen : 0, toConsole);
			}
		}
		mnstr_printf(toConsole, "\n");
	}
	mnstr_printf(toConsole, "Using the conventional grammar constructs:\n");
	mnstr_printf(toConsole, "[ A | B ]   optionally token A or B or none\n");
	mnstr_printf(toConsole, "{ A | B }   exactly one of the options should be chosen\n");
	mnstr_printf(toConsole, "A [ ',' ...]  a comma separate lists of A elements\n");
	mnstr_printf(toConsole, "{ A | B } ... a series of A and Bs\n");
	mnstr_printf(toConsole, "( A B ) [','...] a series of AB,AB,AB,AB\n");
	mnstr_printf(toConsole, "For more search terms type: \\help *\n");
	mnstr_printf(toConsole, "See also https://www.monetdb.org/Documentation/SQLreference\n");
}
