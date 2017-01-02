/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
#include "stream.h"
#include "mhelp.h"

typedef struct{
	char *command;
	char *synopsis;
	char *syntax;
	char *rules;
	char *comments;
} SQLhelp;

SQLhelp sqlhelp[]={
	// major commands
	{ "ALTER TABLE",
	  "",
	  "ALTER TABLE qname ADD [COLUMN] { column_def | table_constraint }\n"
	  "ALTER TABLE qname ALTER [COLUMN] ident SET DEFAULT value\n"
	  "ALTER TABLE qname ALTER [COLUMN] ident SET [NOT] NULL\n"
	  "ALTER TABLE qname ALTER [COLUMN] ident DROP DEFAULT\n"
	  "ALTER TABLE qname ALTER [COLUMN] ident SET STORAGE {string | NULL} \n"
	  "ALTER TABLE qname DROP [COLUMN] ident [RESTRICT | CASCADE]\n"
	  "ALTER TABLE qname DROP CONSTRAINT ident [RESTRICT | CASCADE]\n"
	  "ALTER TABLE qname SET { { READ | INSERT } ONLY | READ WRITE }",
	  "column_def,table_constraint",
	  "See also https://www.monetdb.org/Documentation/SQLreference/Alter"
	},
	{ "ALTER MERGE TABLE",
	  "",
	  "ALTER TABLE qname ADD TABLE qname\n"
	  "ALTER TABLE qname DROP TABLE qname [RESTRICT | CASCADE]\n",
	  "",
	  "See also https://www.monetdb.org/Documentation/Cookbooks/SQLrecipes/DataPartitioning"
	},
	{ "ALTER SEQUENCE",
	  "",
	  "ALTER SEQUENCE ident [ AS datatype] [ RESTART [WITH start]] [INCREMENT BY increment]\n"
	  "[MINVALUE minvalue | NO MINVALUE]  [MAXVALUE maxvalue | NOMAXVALUE] | [ [ NO] CYCLE]",
	  0,
	  "See also https://www.monetdb.org/Documentation/Manuals/SQLreference/SerialTypes"
	},
	{ "ALTER USER",
	  "",
     "ALTER USER ident  WITH [ ENCRYPTED | UNENCRYPTED] PASSWORD string\n"
     "ALTER USER ident  SET SCHEMA ident\n"
     "ALTER USER ident  WITH [ENCRYPTED | UNENCRYPTED] PASSWORD SET SCHEMA ident\n"
     "ALTER USER RENAME TO ident  \n"
     "ALTER USER SET [ ENCRYPTED | UNENCRYPTED] PASSWORD string USING OLD PASSWORD string",
	  0, "See also https://www.monetdb.org/Documentation/SQLreference/Users"
	},
    { "ANALYZE",
	  "Collect statistics for optimizations",
	  "ANALYZE qname [column_list] [SAMPLE size] [MINMAX]",
	  "column_list",
	  "See also https://www.monetdb.org/Documentation/Cookbooks/SQLrecipes/statistics"
	},
	{ "CALL",
	  "",
	  "CALL qname '(' [ [scalar_expression ] [ ',' ...]  ]')' | CALL ident '.' ident",
	  0,0
	},
	{ "CASE",
	  "Case statement for procedures/functions",
	  "CASE  scalar_expression [ when_statement ...]  [ELSE procedure_statement ... ] END CASE",
	  0,"See also https://www.monetdb.org/Documentation/SQLreference/Flowofcontrol"
	},
    { "COMMIT",
	  "Commit the current transaction",
	  "COMMIT [ WORK ] [ AND CHAIN | AND NO CHAIN ]",
	  0,0
	},
	{ "COPY BINARY",
	  "Append binary representations into a table",
	  "COPY [nrofrecords] BINARY INTO qname [column_list] FROM string [','...] [NO CONSTRAINT]",
	  "nrofrecords",
	  "see https://www.monetdb.org/Documentation/Cookbooks/SQLrecipes/BinaryBulkLoad"
	},
	{ "COPY INTO",
	  "Parse a csv-file into a table",
	  "COPY [nrofrecords] INTO qname [column_list] FROM string [','...] [headerlist] [ separators]\n"
	  " [NULL [AS] string] [LOCKED] [BEST EFFORT] [NO CONSTRAINT] [FWF '(' integer [','...]')'\n"
	  "COPY [nrofrecords] INTO qname [column_list] FROM STDIN [headerlist] [ separators]\n"
	  " [NULL [AS] string] [LOCKED] [BEST EFFORT] [NO CONSTRAINT]\n"
	  "COPY query_expression INTO [STDOUT | string] [seps] [NULL [AS] string]",
	  "nrofrecords,headerlist,separators",
	  "See also https://www.monetdb.org/Documentation/Cookbooks/SQLrecipes/LoadingBulkData"
	},
	{ "COPY LOADER",
	  "Copy into using a user supplied parsing function",
	  "COPY LOADER INTO qname FROM qname '(' [ scalar_expression ... ] ')'",
	 0,0
	},
	{ "CREATE AGGREGATE FUNCTION",
	  "",
	  "CREATE AGGREGATE  FUNCTION qname '(' { '*' | [ param [',' ...]] } ')'\n"
	  "    RETURNS { data_type | TABLE '(' function_return [ ',' ... ] ')' }\n"
	  "    EXTERNAL NAME ident ',' ident\n"
	  "CREATE AGGREGATE FUNCTION qname '(' { '*' | [ param [',' ...]] }')'\n"
	  "    RETURNS { data_type | TABLE '(' function_return [ ',' ... ] ')' }\n"
	  "    LANGUAGE ident external_code",
	  "param,data_type,function_return",
	  0
	},
	{ "CREATE FILTER FUNCTION",
	  "",
	  "CREATE FILTER  FUNCTION qname '(' { '*' | [ param [',' ...]] } ')'\n"
	  "    RETURNS { data_type | TABLE '(' function_return [ ',' ... ] ')' }\n"
	  "    EXTERNAL NAME ident ',' ident",
	  "param,data_type,function_return",
	  0
	},
	{ "CREATE FUNCTION",
	  "",
	  "CREATE FUNCTION qname '(' { '*' | [ param [',' ...]] } ')'\n"
	  "    RETURNS { data_type | TABLE '(' function_return [ ',' ... ] ')' }\n"
	  "    EXTERNAL NAME ident ',' ident\n"
	  "CREATE FUNCTION qname '(' { '*' | [ param [',' ...]] } ')'\n"
	  "    RETURNS { data_type | TABLE '(' function_return [ ',' ... ] ')' }\n"
	  "    BEGIN [ ATOMIC ] statement [ ';' ...] END\n"
	  "CREATE FUNCTION qname '(' { '*' | [ param [',' ...]] }')'\n"
	  "    RETURNS { data_type | TABLE '(' function_return [ ',' ... ] ')' }\n"
	  "    LANGUAGE ident external_code",
	  "param,data_type,function_return,external_code",
	  0
	},
	{ "CREATE INDEX",
	  "",
	  "CREATE [ UNIQUE | ORDERED | IMPRINTS ] INDEX ident ON qname '(' ident_list ')'",
	  0,0
	},
	{ "CREATE PROCEDURE",
	  "",
	  "CREATE PROCEDURE qname '(' { '*' | [ param [',' ...]] }')'\n"
	  "    EXTERNAL NAME ident ',' ident\n"
	  "CREATE PROCEDURE qname '(' { '*' | [ param [',' ...]] } ')'\n"
	  "    BEGIN [ ATOMIC ] procedure_statement [ ';' ...] END\n"
	  "CREATE PROCEDURE qname '(' { '*' | [ param [',' ...]] } ')'\n"
	  "    LANGUAGE ident external_code",
	  "param,data_type,external_code",
	  0
	},
	{ "CREATE LOADER",
	  "",
	  "CREATE LOADER qname '(' [ param [',' ...]] ')'\n"
	  "    LANGUAGE ident external_code",
	  "param,data_type,function_return,external_code",
	  0
	},
	{ "CREATE MERGE TABLE",
	  "",
	  "CREATE MERGE TABLE qname table_source;",
	  0, "See also https://www.monetdb.org/Documentation/Cookbooks/SQLrecipes/DataPartitioning"
     },
	{ "CREATE REMOTE TABLE",
	  "",
	  "CREATE REMOTE TABLE qname ON string",
	 0,"remote name should match mapi:monetdb://host:port/database[/schema[/table]]"
	},
	{ "CREATE REPLICA TABLE",
	  "",
	  "CREATE REPLICA TABLE qname table_source;",
	  0, "https://www.monetdb.org/Documentation/Cookbooks/SQLrecipes/TransactionReplication"
     },
    { "CREATE SCHEMA",
	  "",
	  "CREATE SCHEMA schema_name [default_char_set] [path_spec] [schema_element]",
	  "schema_name,default_char_set,path_spec,schema_element",
	  0
	},
	{ "CREATE SEQUENCE",
	  "Define a new sequence generator",
	  "CREATE SEQUENCE ident   [ AS datatype] [ START [WITH start]] [INCREMENT BY increment]\n"
	  "[MINVALUE minvalue | NO MINVALUE] [MAXVALUE maxvalue | NOMAXVALUE] | [ [ NO] CYCLE]",
	  0, "See also https://www.monetdb.org/Documentation/Manuals/SQLreference/SerialTypes"
	},
	{ "CREATE STREAM TABLE",
	 "Temporary table, locked during updates/ continues query processing",
	  "CREATE STREAM TABLE qname table_source \n",
	 0,0
	},
	{ "CREATE TABLE",
	  "",
	  "CREATE TABLE qname table_source [STORAGE ident string]\n"
	  "CREATE TABLE qname FROM LOADER function_ref\n"
	  "CREATE [ LOCAL | GLOBAL ] TEMP[ORARY] TABLE qname table_source [on_commit]",
	  "table_source,on_commit,function_ref",0
	},
	{ "CREATE TRIGGER",
	  "",
	  "CREATE TRIGGER wname { BEFORE | AFTER } {INSERT | DELETE | UPDATE [ OF ident [',' ident]...\n"
	  "ON qname [REFERENCING trigger_reference... triggered_action",
	  "trigger_reference",
	  0
	},
	{ "CREATE TYPE",
	  "Add user defined type to the type system ",
	  "CREATE TYPE qname EXTERNAL NAME ident",
	  0,0
	},
	{ "CREATE VIEW",
	  "",
	  "CREATE VIEW qname [ column_list ] AS { query_expression | '(' query_expression ') } [ WITH CHECK OPTION]",
	  "column_list,query_expression", 0
	},
	{ "CURRENT_DATE",
	  "Built-in function",
	  "CURRENT_DATE [ '(' ')']",
	 0,0
	},
	{ "CURRENT_TIME",
	  "Built-in function",
	  "CURRENT_TIME [ '(' ')']",
	 0,0
	},
	{ "CURRENT_TIMESTAMP",
	  "Built-in function",
	  "CURRENT_TIMESTAMP [ '(' ')']",
	 0,0
	},
	{ "EXPLAIN",
	  "Give execution plan details",
	  "EXPLAIN statement",
	  0,"See alsp https://www.monetdb.org/Documentation/Manuals/SQLreference/Explain"
	},
	{ "LOCAL_TIMESTAMP",
	  "Built-in function",
	  "LOCAL_TIMESTAMP [ '(' ')']",
	 0,0
	},
	{ "EXTRACT",
	   "Built-in function",
	  "EXTRACT '(' { YEAR | MONTH | DAY | HOUR | MINUTE | SECOND } FROM scalar_expression ')'",
	  0,0
	},
    { "DECLARE",
	  "Define a local variable",
	  "DECLARE ident_list data_type",
	  "ident_list,data_type",
	  0
	},
	{ "DELETE",
	  "",
	  "[ WITH with_list] DELETE FROM qname [WHERE search_condition]",
	  "with_list,search_condition",
	  0
	},
	{ "DROP AGGREGATE",
	  "",
	  "DROP AGGREGATE qname [ RESTRICT | CASCADE ]",
	  0,0
	},
	{ "DROP FUNCTION",
	  "",
	  "DROP ALL [FILTER] FUNCTION qname [ RESTRICT | CASCADE ]\n"
	  "DROP routine_designator [ RESTRICT | CASCADE ]",
	  0,0
	},
	{ "DROP INDEX",
	  "",
	  "DROP INDEX qname",
	  0,0
	},
	{ "DROP LOADER",
	  "",
	  "DROP ALL LOADED qname [ RESTRICT | CASCADE ]",
	  0,0
	},
	{ "DROP PROCEDURE",
	  "",
	  "DROP PROCEDURE qname [ RESTRICT | CASCADE ]",
	  0,0
	},
	{ "DROP ROLE",
	  "",
	  "DROP ROLE ident",
	  0,0
	},
    { "DROP SCHEMA",
	  "",
	  "DROP SCHEMA qname [ RESTRICT | CASCADE ]",
	  0,0
	},
	{ "DROP SEQUENCE",
	  "",
	  "DROP SEQUENCE qname",
	  0,0
	},
	{ "DROP TABLE",
	  "",
	  "DROP TABLE qname [ RESTRICT | CASCADE ]",
	  0,0
	},
	{ "DROP TRIGGER",
	  "",
	  "DROP TRIGGER qname",
	  0,0
	},
	{ "DROP TYPE",
	  "",
	  "DROP TYPE qname [ RESTRICT | CASCADE ]",
	  0,0
	},
	{ "DROP USER",
	  "",
	  "DROP USER ident",
	  0,0
	},
	{ "DROP VIEW",
	  "",
	  "DROP VIEW qname [ RESTRICT | CASCADE ]",
	  0,0
	},
	{ "IF",
	  "",
	  "IF  search_condition THEN procedure_statement ...\n"
	  "[ELSE IF search_condition THEN procedure_statement ... ]...\n"
	  "[ ELSE procedure_statement ... ] END IF",
	  "search_condition,procedure_statement",
	  "See also https://www.monetdb.org/Documentation/SQLreference/Flowofcontrol"
	},
	{ "INSERT",
	  "",
	  "[WITH with_list ] INSERT INTO qname [ column_list ] [ DEFAULT VALUES | VALUES row_values | query_expression]",
	  "with_list,column_list,row_values,query_expression",
	  "See also https://www.monetdb.org/Documentation/SQLreference/Updates"
	},
	{ "GRANT",
	  "Define access privileges",
	  "GRANT privileges TO grantee_list [ WITH GRANT OPTION ]"
	  "GRANT authid [',' ... ] TO grantee_list [ WITH ADMIN OPTION]",
	  "grantee_list,authid",
	  0
	},
    { "RELEASE SAVEPOINT",
	  "",
	  "RELEASE SAVEPOINT ident",
	  0,0
	},
	{ "RETURN",
	  "",
	  "RETURN { query_expression | search_condition | TABLE '(' query_expression ')'",
	  0,0
	},
	{ "REVOKE",
	  "Remove some privileges",
	  "REVOKE [GRANT OPTION FOR] privileges FROM grantee [ ',' ... ] [ FROM [CURRENT_USER | CURRENT_ROLE]]\n"
	  "REVOKE [ADMIN OPTION FOR] authid [ ',' ... ] FROM grantee [ ',' ... ] [ FROM [CURRENT_USER | CURRENT_ROLE]]",
	  "privileges,authid,grantee",
	  0
	},
    { "ROLLBACK",
	  "Rollback the current transaction",
	  "ROLLBACK [WORK] [ AND CHAIN | AND NO CHAIN ] [TO SAVEPOINT ident]",
	  0,0
	},
    { "SAVEPOINT",
	  0,
	  "SAVEPOINT ident",
	  0,0
	},
	{ "SELECT",
	  "",
	  "[ WITH with_list  ]\n"
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
	  "See also https://www.monetdb.org/Documentation/SQLreference/TableExpressions"
	},
	{ "SET",
	  "Assign a value to a variable or column",
	  "SET '=' simple_atom",
	  "simple_atom",
	  0
	},
    { "SET LOCALSTART TRANSACTION",
	  "",
	  "START LOCAL TRANSACTION transactionmode",
	  "transactionmode,isolevel",
	  "DIAGNOSTICS is not yet supported"
	},
    { "SET ROLE",
	  "",
	  "SET ROLE ident",
	  0,0
	},
    { "SET SCHEMA",
	  "",
	  "SET SCHEMA ident",
	  0,0
	},
    { "SET SESSION AUTHORIZATION",
	  "",
	  "SESSION AUTHORIZATIO ident",
	  0,0
	},
    { "SET TIME ZONE",
	  0,
	  "SET TIME ZONE interval",
	  "interval",
	  0
	},
    { "SET TIME ZONE LOCAL",
	  0,
	  "SET TIME ZONE LOCAL",
	  0,0
	},
    { "SET TRANSACTION",
	  "",
	  "SET TRANSACTION transactionmode",
	  "transactionmode,isolevel",
	  "DIAGNOSTICS is not yet supported"
	},
    { "START TRANSACTION",
	  "",
	  "START TRANSACTION transactionmode",
	  "transactionmode,isolevel",
	  "DIAGNOSTICS is not yet supported"
	},
	{ "SET USER",
	  "",
	  "SET USER '=' ident",
	  0,0
	},
	{ "TABLE JOINS",
	  "",
	  "'(' joined_table ') |\n"
	  "table_ref CROSS JOIN table_ref ')' |\n"
	  "table_ref NATURAL [ INNER | LEFT | RIGHT | FULL] JOIN table_ref |\n"
	  "table_ref UNION JOIN table_ref { ON search_condition | USING column_list } |\n"
	  "table_ref [ INNER | LEFT | RIGHT | FULL] JOIN table_ref { ON search_condition | USING column_list } |\n",
	   0,"See also https://www.monetdb.org/Documentation/SQLreference/TableExpressions"
	},

	{ "TRACE",
	  "Give execution trace",
	  "TRACE statement",
	  0,0
	},
	{ "UPDATE",
	  "",
	  "[WITH with_list] UPDATE qname SET assignment_list [FROM from_clause] [WHERE search_condition]",
	  "with_list,assignment_list,from_clause",
	  0
	},
	{ "WHILE",
	  "",
	  "[ident ':'] WHILE search_condition DO procedure_statement ... END WHILE [ident]",
	  0,"See also https://www.monetdb.org/Documentation/SQLreference/Flowofcontrol"
	},
	{ "WINDOW",
	  "",
	  "{RANK | DENSE_RANK | PERCENT_RANK | CUME_DIST} OVER window_name |\n"
	  "{RANK | DENSE_RANK | PERCENT_RANK | CUME_DIST} OVER '(' \n"
	  "[window_name] [PARTITION BY column_ref ... ]\n"
	  "[ORDER BY sort_spec] \n"
	  "{ROWS | RANGE} {UNBOUNDED PRECEDING | value PRECEDING | CURRENT ROW} \n"
	  "[BETWEEN {UNBOUNDED FOLLOWING | value FOLLOWING | UNBOUNDED PRECEDING | value PRECEDING | CURRENT ROW} \n"
	  "AND {UNBOUNDED FOLLOWING | value FOLLOWING | UNBOUNDED PRECEDING | value PRECEDING | CURRENT ROW} ]\n"
	  "[EXCLUDING {CURRENT ROW | GROUP | TIES | NO OTHERS}",
	  0,"See also https://www.monetdb.org/Documentation/Manuals/SQLreference/WindowFunctions"
	},

// The subgrammar rules
	{ "assignment_list",0,"colum '=' search_condition | '(' column [','...] ')' '=' subquery","search_condition,column,subquery",0},
	{ "authid",0,"restricted ident",0,0},
	{ "column_def", 0, "COLUMN [ SERIAL | BIGSERIAL] | COLUMN data_type [ column_option ...]","column_option",0},
	{ "column_list", 0 ,"'(' ident [ ',' ... ] ')'",0,0 },
	{ "column_option",0,"DEFAULT value | column_constraint |generated_column","column_constraint,generated_column",0},
	{ "column_option_list",0, " ident WITH OPTIONS '(' column_constraint ')' [ ',' ... ]","column_constraint",0},
	{ "column_constraint",0," [ [ NOT] NULL | UNIQUE | PRIMARY KEY | CHECK '(' search_condition ')'\n"
	  "| REFERENCES qname [ column_list ][ MATCH [ FULL | PARTIAL | SIMPLE]] reference_action ... \n","reference_action",0},
	{ "control_statement", 0, "call_procedure | while_statement | if_statement | case_statement | return_statement",
	"call_procedure | while_statement | if_statement | case_statement | return_statement", 0 },
	{ "datetime_type",0," DATE  | TIME [ time_precision ] tz | TIMESTAMP [ timestamp_precision ] tz","time_precision,timestamp_precision,tz",0},
	{
	  "data_type", 0 ,"[ [ CHARACTER | VARCHAR | CLOB | BLOB] [ '(' nonzero ')' ] |\n"
	"TINYINT | SMALLINT | BIGINT | HUGEINT | [ DECIMAL | FLOAT] [ '(' nonzero [',' nonzero ] ')'] | \n"
	" DOUBLE [ PRECISION ] | REAL | datetime_type | interval_type | geometry_type",
	"datetime_type,interval_type,geometry_type",0},
	{ "default_char_set",0,"DEFAULT CHARACTER SET ident",0,0},
	{ "drop_table_element",0," { CONSTRAINT | TABLE | COLUMN | } ident [ { RESTRICT | CASCADE } ]",0,0},
	{ "end_time", 0 ,"SECOND  timestamp_precision\n,timestamp_precision",0,0},
	{ "function_return",0,"ident data_type",0,0},
	{ "generated_column",0," AUTO_INCREMENT | GENERATED ALWAYS AS IDENTITY [ '(' [ AS datatype] [ START [WITH start]] [INCREMENT BY increment]\n"
      "[MINVALUE minvalue | NO MINVALUE] [MAXVALUE maxvalue | NOMAXVALUE] | [ [ NO] CYCLE] ')' ] ",0,"see https://www.monetdb.org/Documentation/Manuals/SQLreference/SerialTypes"},
	{ "global_privileges",0," { COPY FROM | COPY INTO } [ ',' ... ]",0,0},
	{ "grantee",0," { PUBLIC | authid } ","authid",0},
	{ "headerlist",0,"'(' ( ident [string] ) [',' ...]",0,0},
    { "ident", "An identifier", NULL, NULL, NULL },
    { "ident_list", 0 , "ident [ ',' ... ]", NULL, NULL },
	{ "interval", 0 ,"INTERVAL [ '+' | '-' ] string  start_field TO end_field","start_field,end_time",0 },
    { "intval", "Integer value", NULL, NULL, NULL },
	{ "isolevel", 0 ,"READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE ",0,0},
	{ "nrofrecords", "", "OFFSET integer | integer RECORDS | integer OFFSET integer RECORDS | integer RECORDS OFFSET integer", 0,0 },
	{ "on_commit",0,"ON COMMIT { DELETE ROWS | PRESERVE ROWS | DROP }",0,0},
	{ "param",0,"ident data_type",0,0},
	{ "privileges",0," { ALL [PRIVILEGES ] | { INSERT | DELETE | EXECUTE | [ REFERENCES | SELECT | UPDATE } column_list ON "
	  " { [TABLE] qname | routine_designator }  | global_privileges","global_privileges,routine_designator",0},
	{ "procedure_statement",0," {transaction_statement | update_statement | grant |revoke | declare |set_statement | control_statement |select_single_row } ';'",
		"transaction_statement | update_statement | grant |revoke | declare |set_statement | control_statement |select_single_row",
		0
	},
	{ "query_expression",0,"with_query | select_no_parens_orderby",0,0},
	{ "qname",0,"ident [ '.' ident ['.' ident]]",0,0},
	{ "reference_action",0, " ON { UPDATE | DELETE } {NO ACTION | CASCADE | RESTRICT | SET NULL | SET DEFAULT}",0,0},
	{ "row_values",0, " '(' atom [ ',' atom]... ')' [ ',' row_values] ...", "atom", 0},
	{ "schema_name",0," ident | [ident] AUTHORIZATION authorization_ident",0,0},
	{ "schema_element",0,"grant | revoke | create_statement | drop_statement | alter_statement",0,0},
	{ "separators",0,"[USING] DELIMITERS field_sep_string [',' record_sep_string [',' quote_string]]",0,0},
	{ "split_part",0,"SPLIT_PART '(' string ',' delimiter_string ',' field_index ')'",0,0,},
	{ "table_constraint",0," CONSTRAINT [ ident ] { UNIQUE | PRIMARY KEY } column_list | FOREIGN KEY } column_list REFERENCES qname [ column_list ][ MATCH [ FULL | PARTIAL | SIMPLE]]",0,0},
	{ "table_element",0, "column_def | table_constraint | column_option_list | LIKE qname","column_def,table_constraint,column_option_list",0},
	{ "table_name",0," [AS] ident ['(' name [','...] ')' ]",0,0},
	{ "table_ref",0," [LATERAL] func_ref [table_name] | [LATERAL] subquery | joined_table",0,0},
	{ "table_source", 0,"'(' table_element [ ',' ... ] ')' | column_list AS query_expression [ WITH [NO] DATA ] ","table_element",0},
	{ "transaction_statement",0,"commit | savepoint | release | rollback | start transaction | set local transaction" ,
	 "commit,savepoint,release,rollback,start transaction,set local transaction" ,
	0},
	{ "time_precision", 0, " '(' YEAR | MONTH | DAY | HOUR | MINUTE ')'",0,0},
	{ "timestamp_precision", 0, " '(' integer ')'",0,0},
	{ "transactionmode",0,"{ READ ONLY | READ WRITE | ISOLATION LEVEL isolevel | DIAGNOSTICS intval} [ , ... ]",0,0},
	{ "trigger_reference",0,"OLD [ROW] [AS] ident | NEW [ROW] [AS] ident",0,0},
	{ "update_statement",0,"delete_stmt | insert_stmt | update_stmt | copyfrom_stmt","delete_stmt | insert_stmt | update_stmt | copyfrom_stmt",0},
	{ "triggered_action",0,"[FOR EACH { ROW | STATEMENT } ] [ WHEN '(' search_condition ')'\n"
	  "BEGIN ATOMIC trigger_statement ... END ",0,0},
	{ "trigger_statement",0,"transaction_statement | update_statement | grant | revoke | declare_statement | set_statement | control_statement | select_single_row",
	"transaction_statement,update_statement,grant,revoke,declare_statement,set_statement,control_statement,select_single_row", 0 },
	{ "when_statement",0,"WHEN salar_expression THEN procedure_statement ...",0,0},
	{ "with_list",0," ident [ column_list] AS  [',' with_list]...",0,0},
    { NULL, NULL, NULL, NULL, NULL }    /* End of list marker */
};

// matching is against a substring of the command string
static int strmatch(char *heap, char *needle)
{
	char heapbuf[2048], *s = heapbuf;
	char needlebuf[2048], *t = needlebuf;

	for( ; *heap ; heap++)
		*s++ = (char) tolower((int) *heap);
	*s = 0;
	for( ; *needle ; needle++)
		*t++ = (char) tolower((int) *needle);
	*t = 0;
	return strncmp(heapbuf,needlebuf, strlen(needlebuf))== 0;
}

static char * sql_grammar_rule(char *word, stream *toConsole)
{
	char buf[65], *s= buf;
	int i;
	while ( s < buf+64 && *word != ',' && *word && !isspace((int) *word)) *s++ = *word++;
	*s = 0;

	for(i=0; sqlhelp[i].command; i++){
		if( strmatch(sqlhelp[i].command, buf) && sqlhelp[i].synopsis == 0){
			mnstr_printf(toConsole,"%s : %s\n",buf, sqlhelp[i].syntax);
		}
	}
	while ( *word && (isalnum((int) *word || *word == '_')) ) word++;
	while ( *word && isspace((int) *word) ) word++;
	return *word==',' ? word +1 :0;
}

static void sql_grammar(int idx, stream *toConsole)
{
	char *t1;
	if( sqlhelp[idx].synopsis == 0){
		mnstr_printf(toConsole,"%s  :%s\n", sqlhelp[idx].command, sqlhelp[idx].syntax);
		if( sqlhelp[idx].comments)
			mnstr_printf(toConsole,"%s\n", sqlhelp[idx].comments);
		return;
	}
	if( sqlhelp[idx].command)
		mnstr_printf(toConsole,"command  : %s\n", sqlhelp[idx].command);
	if( sqlhelp[idx].synopsis && *sqlhelp[idx].synopsis )
		mnstr_printf(toConsole,"synopsis : %s\n", sqlhelp[idx].synopsis);
	if( sqlhelp[idx].syntax && *sqlhelp[idx].syntax){
		mnstr_printf(toConsole,"syntax   : ");
		for( t1 = sqlhelp[idx].syntax; *t1; t1++){
			if( *t1 == '\n')
				mnstr_printf(toConsole,"\n           ");
			else
				mnstr_printf(toConsole,"%c", *t1);
		}
		mnstr_printf(toConsole,"\n");
		t1 = sqlhelp[idx].rules;
		if( t1 && *t1)
		do
			t1 = sql_grammar_rule(t1, toConsole);
		while( t1 );
	}
	if( sqlhelp[idx].comments)
		mnstr_printf(toConsole,"%s\n", sqlhelp[idx].comments);
}

static void sql_word(char *word, size_t maxlen, stream *toConsole)
{
	size_t i;

	mnstr_printf(toConsole,"%s", word);
	for( i = strlen(word); i<= maxlen; i++)
		mnstr_printf(toConsole," ");
}

void sql_help( char *pattern, stream *toConsole)
{
	size_t maxlen= 0, len;
	int i, step, total=0;
	
	if( *pattern == '\\')
		pattern ++;
	while( *pattern &&  !isspace((int) *pattern) ) { pattern++;}
	while( *pattern && isspace((int) *pattern) ) { pattern++;}

	if( *pattern &&  pattern[strlen(pattern)-1] == '\n')
		pattern[strlen(pattern)-1] =0;

	if( *pattern && *pattern != '*')
		for( i=0; *pattern && sqlhelp[i].command; i++)
		if( strmatch(sqlhelp[i].command, pattern) ){
			sql_grammar(i,toConsole);
			return;
		}
		
	// collect the major topics
	for( i=0; sqlhelp[i].command; i++){
		if ( islower((int) sqlhelp[i].command[0])  &&  *pattern != '*')
			break;
		total++;
		if ( (len = strlen(sqlhelp[i].command)) > maxlen)
			maxlen = len;
	}

	// provide summary of all major topics  (=search terms)
	step = total / 4;
	for( i=0;  i < step; i++){
		sql_word(sqlhelp[i].command, maxlen, toConsole);
		if( i + step < total)
			sql_word(sqlhelp[i + step].command, maxlen, toConsole);
		if( i + 2 * step < total)
			sql_word(sqlhelp[i + 2 * step].command, maxlen, toConsole);
		if( i + 3 * step < total)
			sql_word(sqlhelp[i + 3 * step].command, maxlen, toConsole);
		if( i + 4 * step < total)
			sql_word(sqlhelp[i + 4 * step].command, maxlen, toConsole);
		mnstr_printf(toConsole,"\n");
	}
	mnstr_printf(toConsole,"Using the conventional grammar constructs:\n");
	mnstr_printf(toConsole,"[ A | B ]   optionally token A or B or none\n");
	mnstr_printf(toConsole,"{ A | B }   exactly one of the options should be chosen\n");
	mnstr_printf(toConsole,"A [ ',' ...]  a comma separate lists of A elements\n");
	mnstr_printf(toConsole,"{ A | B } ... a series of A and Bs\n");
	mnstr_printf(toConsole,"( A B ) [','...] a series of AB,AB,AB,AB\n");
	mnstr_printf(toConsole,"For more search terms type: \\help *\n");
	mnstr_printf(toConsole,"See also https://www.monetdb.org/Documentation/SQLreference\n");
}
