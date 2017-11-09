/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

%{
#include "monetdb_config.h"
#include <sql_mem.h>
#include "sql_parser.h"
#include "sql_symbol.h"
#include "sql_datetime.h"
#include "sql_decimal.h"	/* for decimal_from_str() */
#include "sql_semantic.h"	/* for sql_add_param() & sql_add_arg() */
#include "sql_env.h"
#include "rel_sequence.h"	/* for sql_next_seq_name() */
#ifdef HAVE_HGE
#include "mal.h"		/* for have_hge */
#endif

#include <unistd.h>
#include <string.h>
#include <stdlib.h>

#define SA 	m->sa
#define _symbol_create(t,d)         symbol_create( SA, t, d)
#define _symbol_create_list(t,d)    symbol_create_list( SA, t, d)
#define _symbol_create_int(t,d)     symbol_create_int( SA, t, d)
#define _symbol_create_lng(t,d)     symbol_create_lng( SA, t, d)
#define _symbol_create_symbol(t,d)  symbol_create_symbol( SA, t, d)
#define _symbol_create_hexa(t,d)    symbol_create_hexa( SA, t, d)
#define _newAtomNode(d)		    newAtomNode( SA, d)

#define L()                  dlist_create( SA )

#define append_list(l,d)     dlist_append_list( SA, l, d)
#define append_int(l,d)      dlist_append_int( SA, l, d)
#define append_lng(l,d)      dlist_append_lng( SA, l, d)
#define append_symbol(l,d)   dlist_append_symbol( SA, l, d)
#define append_string(l,d)   dlist_append_string( SA, l, d)
#define append_type(l,d)     dlist_append_type( SA, l, d)

#define _atom_string(t, v)   atom_string(SA, t, v)

#define YYMALLOC GDKmalloc
#define YYFREE GDKfree

#define YY_parse_LSP_NEEDED	/* needed for bison++ 1.21.11-3 */

#define SET_Z(info)(info = info | 0x02)
#define SET_M(info)(info = info | 0x01)

#ifdef HAVE_HGE
#define MAX_DEC_DIGITS (have_hge ? 38 : 18)
#define MAX_HEX_DIGITS (have_hge ? 32 : 16)
#else
#define MAX_DEC_DIGITS 18
#define MAX_HEX_DIGITS 16
#endif

static inline int
UTF8_strlen(const char *val)
{
	const unsigned char *s = (const unsigned char *) val;
	int pos = 0;

	while (*s) {
		int c = *s++;

		pos++;
		if (c < 0xC0)
			continue;
		if (*s++ < 0x80)
			return int_nil;
		if (c < 0xE0)
			continue;
		if (*s++ < 0x80)
			return int_nil;
		if (c < 0xF0)
			continue;
		if (*s++ < 0x80)
			return int_nil;
		if (c < 0xF8)
			continue;
		if (*s++ < 0x80)
			return int_nil;
		if (c < 0xFC)
			continue;
		if (*s++ < 0x80)
			return int_nil;
	}
	return pos;
}

%}
/* KNOWN NOT DONE OF sql'99
 *
 * COLLATION
 * TRANSLATION
 * REF/SCOPE
 * UDT
 */

%parse-param { mvc *m }
%lex-param { void *m }

/* reentrant parser */
%pure-parser
%union {
	int		i_val,bval;
	lng		l_val,operation;
	double		fval;
	char *		sval;
	symbol*		sym;
	dlist*		l;
	sql_subtype	type;
}
%{
extern int sqllex( YYSTYPE *yylval, void *m );
/* enable to activate debugging support
int yydebug=1;
*/
%}

	/* symbolic tokens */
%type <sym>
	alter_statement
	assignment
	create_statement
	drop_statement
	declare_statement
	set_statement
	sql
	sqlstmt
	with_query
	schema
	opt_schema_default_char_set
	opt_path_specification
	path_specification
	schema_element
	delete_stmt
	copyfrom_stmt
	table_def
	view_def
	query_expression_def
	query_expression
	with_query_expression
	role_def
	type_def
	func_def
	index_def
	seq_def
	opt_seq_param
	opt_alt_seq_param
	opt_seq_common_param
	all_or_any_predicate
	like_exp
	between_predicate
	comparison_predicate
	opt_from_clause
	existence_test
	in_predicate
	insert_stmt
	transaction_statement
	_transaction_stmt
	like_predicate
	opt_where_clause
	opt_having_clause
	opt_group_by_clause
	predicate
	filter_exp
	joined_table
	join_spec
	search_condition
	and_exp
	update_statement
	update_stmt
	control_statement
	select_statement_single_row
	call_statement
	call_procedure_statement
	routine_invocation
	return_statement
	return_value
	case_statement
	when_statement
	when_search_statement
	if_statement
	while_statement
	simple_select
	select_no_parens
	select_no_parens_orderby
	subquery
	subquery_with_orderby
	test_for_null
	values_or_query_spec
	grant
	revoke
	operation
	table_content_source
	table_element
	add_table_element
	alter_table_element
	drop_table_element
	table_constraint
	table_constraint_type
	column_def
	column_options
	column_option
	column_constraint
	column_constraint_type
	generated_column
	like_table
	domain_constraint_type 
	opt_order_by_clause
	default
	default_value
	assign_default
	cast_value
	aggr_ref
	var_ref
	func_ref
	datetime_funcs
	string_funcs
	scalar_exp
	pred_exp
	simple_scalar_exp
	value_exp
	column_exp
	atom
	insert_atom
	simple_atom
	value
	literal
	null
	interval_expression
	ordering_spec
	table_ref
	opt_limit
	opt_offset
	opt_sample
	param
	case_exp
	case_scalar_exp
	cast_exp
	when_value
	when_search
	case_opt_else
	table_name
	opt_table_name
	object_name
	exec
	exec_ref
	trigger_def
	trigger_event
	opt_when
	procedure_statement
	trigger_procedure_statement
	if_opt_else
	func_data_type
	with_list_element
	window_function
	window_function_type
	window_partition_clause
	window_order_clause
	window_frame_clause
	window_frame_start
	window_frame_end
	window_frame_preceding
	window_frame_following
	XML_value_function
	XML_comment
  	XML_concatenation
  	XML_document
  	XML_element
  	XML_forest
  	XML_parse
  	XML_PI
  	XML_query
  	XML_text
  	XML_validate
	XML_namespace_declaration
	opt_XML_namespace_declaration_and_comma
	XML_namespace_declaration_item_list
	XML_namespace_declaration_item
	XML_regular_namespace_declaration_item
	XML_default_namespace_declaration_item
	XML_namespace_URI
	XML_attributes
	XML_attribute_list
	XML_attribute
	XML_attribute_value
	XML_element_content
	forest_element_value
	XML_aggregate
	XML_value_expression
	XML_primary
	opt_comma_string_value_expression

%type <type>
	data_type
	datetime_type
	interval_type

%type <sval>
	opt_constraint_name
	non_reserved_word
	ident
	authorization_identifier
	func_ident
	restricted_ident
	column
	authid
	grantee
	opt_alias_name
	opt_to_savepoint
	opt_using
	opt_null_string
	string
	type_alias
	varchar
	clob
	blob
	opt_begin_label
	opt_end_label
	target_specification
	XML_element_name
	opt_XML_attribute_name
	XML_attribute_name
	opt_forest_element_name
	forest_element_name
	XML_namespace_prefix
	XML_PI_target
	function_body

%type <l>
	passwd_schema
	object_privileges
	global_privileges
	privileges
	schema_name_clause
	assignment_commalist
	opt_column_list
	column_commalist_parens
	opt_fwf_widths
	fwf_widthlist
	opt_header_list
	header_list
	header
	ident_commalist
	opt_corresponding
	column_ref_commalist
	name_commalist
	schema_name_list
	column_ref
	atom_commalist
	value_commalist
	pred_exp_list
	row_commalist
	filter_arg_list
	filter_args
	qname
	qfunc
	qrank
	qaggr
	qaggr2
	routine_name
	sort_specification_list
	opt_schema_element_list
	schema_element_list
	operation_commalist
	authid_list
	grantee_commalist
	column_def_opt_list
	opt_column_def_opt_list
	table_exp
	table_ref_commalist
	table_element_list
	table_opt_storage
	as_subquery_clause
	column_exp_commalist
	column_option_list
	selection
	start_field
	end_field
	single_datetime_field
	interval_qualifier
	scalar_exp_list
	case_scalar_exp_list
	when_value_list
	when_search_list
	opt_seps
	opt_nr
	string_commalist
	string_commalist_contents
	paramlist
	opt_paramlist
	opt_typelist
	typelist
	opt_seq_params
	opt_alt_seq_params
	serial_opt_params
	triggered_action
	opt_referencing_list
	old_or_new_values_alias_list
	old_or_new_values_alias
	triggered_statement
	procedure_statement_list
	trigger_procedure_statement_list
	argument_list
	when_statements
	when_search_statements
	case_opt_else_statement
	variable_list
	routine_body
	table_function_column_list
	select_target_list
	external_function_name
	with_list
	window_specification
	opt_comma_XML_namespace_declaration_attributes_element_content
	XML_element_content_and_option
	XML_element_content_list
	forest_element_list
	forest_element
	XML_value_expression_list
	window_frame_extent
	window_frame_between
	routine_designator

%type <i_val>
	any_all_some
	datetime_field
	document_or_content
	document_or_content_or_sequence
	drop_action
	extract_datetime_field
	grantor
	intval
	join_type
	opt_outer
	non_second_datetime_field
	nonzero
	opt_bounds
	opt_column
	opt_encrypted
	opt_for_each
	opt_from_grantor
	opt_grantor	
	global_privilege
	opt_index_type
	opt_match
	opt_match_type
	opt_on_commit
	opt_ref_action
	opt_sign
	opt_temp
	opt_minmax
	opt_XML_content_option
	opt_XML_returning_clause
	outer_join_type
	posint
	ref_action
	ref_on_delete
	ref_on_update
	row_or_statement
	serial_or_bigserial
	time_precision
	timestamp_precision
	transaction_mode
	transaction_mode_list
	_transaction_mode_list
	trigger_action_time
	with_or_without_data
	XML_content_option
	XML_whitespace_option
	window_frame_units
	window_frame_exclusion
	subgeometry_type

%type <l_val>
	lngval
	poslng
	nonzerolng

%type <bval>
	opt_brackets

	opt_work
	opt_chain
	opt_distinct
	opt_locked
	opt_best_effort
	opt_constraint
	set_distinct
	opt_with_check_option
	create
	create_or_replace
	if_exists
	if_not_exists

	opt_with_grant
	opt_with_admin
	opt_admin_for
	opt_grant_for

	opt_asc_desc
	tz

%right <sval> STRING
%right <sval> X_BODY

/* sql prefixes to avoid name clashes on various architectures */
%token <sval>
	IDENT aTYPE ALIAS AGGR AGGR2 RANK sqlINT OIDNUM HEXADECIMAL INTNUM APPROXNUM 
	USING 
	GLOBAL CAST CONVERT
	CHARACTER VARYING LARGE OBJECT VARCHAR CLOB sqlTEXT BINARY sqlBLOB
	sqlDECIMAL sqlFLOAT
	TINYINT SMALLINT BIGINT HUGEINT sqlINTEGER
	sqlDOUBLE sqlREAL PRECISION PARTIAL SIMPLE ACTION CASCADE RESTRICT
	BOOL_FALSE BOOL_TRUE
	CURRENT_DATE CURRENT_TIMESTAMP CURRENT_TIME LOCALTIMESTAMP LOCALTIME
	LEX_ERROR 
	
/* the tokens used in geom */
%token <sval> GEOMETRY GEOMETRYSUBTYPE GEOMETRYA 

%token	USER CURRENT_USER SESSION_USER LOCAL LOCKED BEST EFFORT
%token  CURRENT_ROLE sqlSESSION
%token <sval> sqlDELETE UPDATE SELECT INSERT 
%token <sval> LATERAL LEFT RIGHT FULL OUTER NATURAL CROSS JOIN INNER
%token <sval> COMMIT ROLLBACK SAVEPOINT RELEASE WORK CHAIN NO PRESERVE ROWS
%token  START TRANSACTION READ WRITE ONLY ISOLATION LEVEL
%token  UNCOMMITTED COMMITTED sqlREPEATABLE SERIALIZABLE DIAGNOSTICS sqlSIZE STORAGE

%token <sval> ASYMMETRIC SYMMETRIC ORDER ORDERED BY IMPRINTS
%token <operation> EXISTS ESCAPE HAVING sqlGROUP sqlNULL
%token <operation> FROM FOR MATCH

%token <operation> EXTRACT

/* sequence operations */
%token SEQUENCE INCREMENT RESTART
%token MAXVALUE MINVALUE CYCLE
%token NOMAXVALUE NOMINVALUE NOCYCLE
%token NEXT VALUE CACHE
%token GENERATED ALWAYS IDENTITY
%token SERIAL BIGSERIAL AUTO_INCREMENT /* PostgreSQL and MySQL immitators */

/* SQL's terminator, the semi-colon */
%token SCOLON AT

/* SQL/XML tokens */
%token XMLCOMMENT XMLCONCAT XMLDOCUMENT XMLELEMENT XMLATTRIBUTES XMLFOREST 
%token XMLPARSE STRIP WHITESPACE XMLPI XMLQUERY PASSING XMLTEXT
%token NIL REF ABSENT EMPTY DOCUMENT ELEMENT CONTENT XMLNAMESPACES NAMESPACE
%token XMLVALIDATE RETURNING LOCATION ID ACCORDING XMLSCHEMA URI XMLAGG
%token FILTER


/* operators */
%left UNION EXCEPT INTERSECT CORRESPONDING UNIONJOIN
%left JOIN CROSS LEFT FULL RIGHT INNER NATURAL
%left WITH DATA
%left <operation> '(' ')'
%left <sval> FILTER_FUNC 

%left <operation> NOT
%left <operation> '='
%left <operation> ALL ANY NOT_BETWEEN BETWEEN NOT_IN sqlIN NOT_LIKE LIKE NOT_ILIKE ILIKE OR SOME
%left <operation> AND
%left <sval> COMPARISON /* <> < > <= >= */
%left <operation> '+' '-' '&' '|' '^' LEFT_SHIFT RIGHT_SHIFT LEFT_SHIFT_ASSIGN RIGHT_SHIFT_ASSIGN CONCATSTRING SUBSTRING POSITION SPLIT_PART
%right UMINUS
%left <operation> '*' '/' '%'
%left <operation> '~'

%left <operation> GEOM_OVERLAP GEOM_OVERLAP_OR_ABOVE GEOM_OVERLAP_OR_BELOW GEOM_OVERLAP_OR_LEFT
%left <operation> GEOM_OVERLAP_OR_RIGHT GEOM_BELOW GEOM_ABOVE GEOM_DIST GEOM_MBR_EQUAL

/* literal keyword tokens */
/*
CONTINUE CURRENT CURSOR FOUND GOTO GO LANGUAGE
SQLCODE SQLERROR UNDER WHENEVER
*/

%token TEMP TEMPORARY STREAM MERGE REMOTE REPLICA
%token<sval> ASC DESC AUTHORIZATION
%token CHECK CONSTRAINT CREATE
%token TYPE PROCEDURE FUNCTION sqlLOADER AGGREGATE RETURNS EXTERNAL sqlNAME DECLARE
%token CALL LANGUAGE 
%token ANALYZE MINMAX SQL_EXPLAIN SQL_PLAN SQL_DEBUG SQL_TRACE PREP PREPARE EXEC EXECUTE
%token DEFAULT DISTINCT DROP
%token FOREIGN
%token RENAME ENCRYPTED UNENCRYPTED PASSWORD GRANT REVOKE ROLE ADMIN INTO
%token IS KEY ON OPTION OPTIONS
%token PATH PRIMARY PRIVILEGES
%token<sval> PUBLIC REFERENCES SCHEMA SET AUTO_COMMIT
%token RETURN 

%token ALTER ADD TABLE COLUMN TO UNIQUE VALUES VIEW WHERE WITH
%token<sval> sqlDATE TIME TIMESTAMP INTERVAL
%token YEAR QUARTER MONTH WEEK DAY HOUR MINUTE SECOND ZONE
%token LIMIT OFFSET SAMPLE

%token CASE WHEN THEN ELSE NULLIF COALESCE IF ELSEIF WHILE DO
%token ATOMIC BEGIN END
%token COPY RECORDS DELIMITERS STDIN STDOUT FWF
%token INDEX REPLACE

%token AS TRIGGER OF BEFORE AFTER ROW STATEMENT sqlNEW OLD EACH REFERENCING
%token OVER PARTITION CURRENT EXCLUDE FOLLOWING PRECEDING OTHERS TIES RANGE UNBOUNDED

%token X_BODY 
%%

sqlstmt:
   sql SCOLON
	{
		if (m->sym) {
			append_symbol(m->sym->data.lval, $$);
			$$ = m->sym;
		} else {
			m->sym = $$ = $1;
		}
		YYACCEPT;
	}

 | prepare 		{
		  	  m->emode = m_prepare; 
			  m->scanner.as = m->scanner.yycur; 
			  m->scanner.key = 0;
			}
	sql SCOLON 	{
			  if (m->sym) {
				append_symbol(m->sym->data.lval, $3);
				$$ = m->sym;
			  } else {
				m->sym = $$ = $3;
			  }
			  YYACCEPT;
			}
 | SQL_PLAN 		{
		  	  m->emode = m_plan;
			  m->scanner.as = m->scanner.yycur; 
			  m->scanner.key = 0;
			}
	sql SCOLON 	{
			  if (m->sym) {
				append_symbol(m->sym->data.lval, $3);
				$$ = m->sym;
			  } else {
				m->sym = $$ = $3;
			  }
			  YYACCEPT;
			}

 | SQL_EXPLAIN 		{
		  	  m->emod |= mod_explain;
			  m->scanner.as = m->scanner.yycur; 
			  m->scanner.key = 0;
			}
   sql SCOLON 		{
			  if (m->sym) {
				append_symbol(m->sym->data.lval, $3);
				$$ = m->sym;
			  } else {
				m->sym = $$ = $3;
			  }
			  YYACCEPT;
			}

 | SQL_DEBUG 		{
			  if (m->scanner.mode == LINE_1) {
				yyerror(m, "SQL debugging only supported in interactive mode");
				YYABORT;
			  }
		  	  m->emod |= mod_debug;
			  m->scanner.as = m->scanner.yycur; 
			  m->scanner.key = 0;
			}
   sqlstmt		{ $$ = $3; YYACCEPT; }
 | SQL_TRACE 		{
		  	  m->emod |= mod_trace;
			  m->scanner.as = m->scanner.yycur; 
			  m->scanner.key = 0;
			}
   sqlstmt		{ $$ = $3; YYACCEPT; }
 | exec SCOLON		{ m->sym = $$ = $1; YYACCEPT; }
 | /*empty*/		{ m->sym = $$ = NULL; YYACCEPT; }
 | SCOLON		{ m->sym = $$ = NULL; YYACCEPT; }
 | error SCOLON		{ m->sym = $$ = NULL; YYACCEPT; }
 | LEX_ERROR		{ m->sym = $$ = NULL; YYABORT; }
 ;


prepare:
       PREPARE
 |     PREP
 ; 

execute:
       EXECUTE
 |     EXEC
 ; 


create:
    CREATE  { $$ = FALSE; }

create_or_replace:
	create
|	CREATE OR REPLACE { $$ = TRUE; }


if_exists:
	/* empty */   { $$ = FALSE; }
|	IF EXISTS     { $$ = TRUE; }

if_not_exists:
	/* empty */   { $$ = FALSE; }
|	IF NOT EXISTS { $$ = TRUE; }


drop:
    DROP 		

set:
    SET 		

declare:
    DECLARE 		

	/* schema definition language */
sql:
    schema
 |  grant
 |  revoke
 |  create_statement
 |  drop_statement
 |  alter_statement
 |  declare_statement
 |  set_statement
 |  ANALYZE qname opt_column_list opt_sample opt_minmax
		{ dlist *l = L();
		append_list(l, $2);
		append_list(l, $3);
		append_symbol(l, $4);
		append_int(l, $5);
		$$ = _symbol_create_list( SQL_ANALYZE, l); }
 |  call_procedure_statement
 ;

opt_minmax:
   /* empty */  	{ $$ = 0; }
 | MINMAX		{ $$ = 1; }
 ;

declare_statement:
	declare variable_list
		{ $$ = _symbol_create_list( SQL_DECLARE, $2); }
    |   declare table_def { $$ = $2; }
    ;

variable_list:
	ident_commalist data_type
		{ dlist *l = L();
		append_list(l, $1 );
		append_type(l, &$2 );
		$$ = append_symbol(L(), _symbol_create_list( SQL_DECLARE, l)); }
    |	variable_list ',' ident_commalist data_type
		{ dlist *l = L();
		append_list(l, $3 );
		append_type(l, &$4 );
		$$ = append_symbol($1, _symbol_create_list( SQL_DECLARE, l)); }
    ;

set_statement:
	/*set ident '=' simple_atom*/
        set ident '=' search_condition
		{ dlist *l = L();
		append_string(l, $2 );
		append_symbol(l, $4 );
		$$ = _symbol_create_list( SQL_SET, l); }
  |     set column_commalist_parens '=' subquery
		{ dlist *l = L();
	  	append_list(l, $2);
	  	append_symbol(l, $4);
	  	$$ = _symbol_create_list( SQL_SET, l ); }
  |	set sqlSESSION AUTHORIZATION ident
		{ dlist *l = L();
		  sql_subtype t;
	        sql_find_subtype(&t, "char", UTF8_strlen($4), 0 );
		append_string(l, sa_strdup(SA, "current_user"));
		append_symbol(l,
			_newAtomNode( _atom_string(&t, sql2str($4))) );
		$$ = _symbol_create_list( SQL_SET, l); }
  |	set SCHEMA ident
		{ dlist *l = L();
		  sql_subtype t;
		sql_find_subtype(&t, "char", UTF8_strlen($3), 0 );
		append_string(l, sa_strdup(SA, "current_schema"));
		append_symbol(l,
			_newAtomNode( _atom_string(&t, sql2str($3))) );
		$$ = _symbol_create_list( SQL_SET, l); }
  |	set user '=' ident
		{ dlist *l = L();
		  sql_subtype t;
		sql_find_subtype(&t, "char", UTF8_strlen($4), 0 );
		append_string(l, sa_strdup(SA, "current_user"));
		append_symbol(l,
			_newAtomNode( _atom_string(&t, sql2str($4))) );
		$$ = _symbol_create_list( SQL_SET, l); }
  |	set ROLE ident
		{ dlist *l = L();
		  sql_subtype t;
		sql_find_subtype(&t, "char", UTF8_strlen($3), 0);
		append_string(l, sa_strdup(SA, "current_role"));
		append_symbol(l,
			_newAtomNode( _atom_string(&t, sql2str($3))) );
		$$ = _symbol_create_list( SQL_SET, l); }
  |	set TIME ZONE LOCAL
		{ dlist *l = L();
		append_string(l, sa_strdup(SA, "current_timezone"));
		append_symbol(l, _symbol_create_list( SQL_OP, append_list(L(),
			append_string( L(), sa_strdup(SA, "local_timezone")))));
		$$ = _symbol_create_list( SQL_SET, l); }
  |	set TIME ZONE interval_expression
		{ dlist *l = L();
		append_string(l, sa_strdup(SA, "current_timezone"));
		append_symbol(l, $4 );
		$$ = _symbol_create_list( SQL_SET, l); }
  ;

schema:
	create SCHEMA if_not_exists schema_name_clause opt_schema_default_char_set
			opt_path_specification	opt_schema_element_list
		{ dlist *l = L();
		append_list(l, $4);
		append_symbol(l, $5);
		append_symbol(l, $6);
		append_list(l, $7);
		append_int(l, $3);
		$$ = _symbol_create_list( SQL_CREATE_SCHEMA, l); }
  |	drop SCHEMA if_exists qname drop_action
		{ dlist *l = L();
		append_list(l, $4);
		append_int(l, 1 /*$5 use CASCADE in the release */);
		append_int(l, $3);
		$$ = _symbol_create_list( SQL_DROP_SCHEMA, l); }
 ;

schema_name_clause:
    ident
	{ $$ = L();
	  append_string($$, $1 );
	  append_string($$, NULL ); }
 |  AUTHORIZATION authorization_identifier
	{ $$ = L();
	  append_string($$, NULL );
	  append_string($$, $2 ); }
 |  ident AUTHORIZATION authorization_identifier
	{ $$ = L();
	  append_string($$, $1 );
	  append_string($$, $3 ); }
 ;

authorization_identifier:
	ident	/* role name | user identifier */ ;

opt_schema_default_char_set:
    /* empty */			{ $$ = NULL; }
 |  DEFAULT CHARACTER SET ident { $$ = _symbol_create( SQL_CHARSET, $4 ); }
 ;

opt_schema_element_list:
    /* empty */			{ $$ = L(); }
 |  schema_element_list
 ;

schema_element_list:
    schema_element	{ $$ = append_symbol(L(), $1); }
 |  schema_element_list schema_element
			{ $$ = append_symbol( $1, $2 ); }
 ;

schema_element: grant | revoke | create_statement | drop_statement | alter_statement ;

opt_grantor:
     /* empty */	 { $$ = cur_user; }
 |   WITH ADMIN grantor  { $$ = $3; }
 ;

grantor:
    CURRENT_USER	{ $$ = cur_user; }
 |  CURRENT_ROLE	{ $$ = cur_role; }
 ;

grant:
    GRANT privileges TO grantee_commalist opt_with_grant opt_from_grantor
	{ dlist *l = L();
	  append_list(l, $2);
	  append_list(l, $4);
	  append_int(l, $5);
	  append_int(l, $6);
	$$ = _symbol_create_list( SQL_GRANT, l);
	}

 |  GRANT authid_list TO grantee_commalist opt_with_admin
		opt_from_grantor
	{ dlist *l = L();
	  append_list(l, $2);
	  append_list(l, $4);
	  append_int(l, $5);
	  append_int(l, $6);
	$$ = _symbol_create_list( SQL_GRANT_ROLES, l); }
 ;

authid_list:
	authid		{ $$ = append_string(L(), $1); }
 |	authid_list ',' authid
			{ $$ = append_string($1, $3); }
 ;

opt_with_grant:
    /* empty */				{ $$ = 0; }
 |	WITH GRANT OPTION		{ $$ = 1; }
 ;

opt_with_admin:
	/* emtpy */		{ $$ = 0; }
 |	WITH ADMIN OPTION	{ $$ = 1; }
 ;


opt_from_grantor:
	/* empty */	{ $$ = cur_user; }
 |	FROM grantor	{ $$ = $2; }
 ;

revoke:
     REVOKE opt_grant_for privileges FROM grantee_commalist opt_from_grantor
	{ dlist *l = L();
	  append_list(l, $3);
	  append_list(l, $5);
	  append_int(l, $2); /* GRANT OPTION FOR */
	  append_int(l, 0);
	  append_int(l, $6);
	$$ = _symbol_create_list( SQL_REVOKE, l); }
 |   REVOKE opt_admin_for authid_list FROM grantee_commalist opt_from_grantor
	{ dlist *l = L();
	  append_list(l, $3);
	  append_list(l, $5);
	  append_int(l, $2);
	  append_int(l, $6);
	$$ = _symbol_create_list( SQL_REVOKE_ROLES, l); }
 ;

opt_grant_for:
	/* empty */			{ $$ = 0; }
 |	GRANT OPTION FOR		{ $$ = 1; }
 ;

opt_admin_for:
	/* empty */			{ $$ = 0; }
 |	ADMIN OPTION FOR		{ $$ = 1; }
 ;

privileges:
 	global_privileges 
	{ $$ = L();
	  append_list($$, $1);
	  append_symbol($$, _symbol_create(SQL_GRANT, NULL)); }
 |	object_privileges ON object_name
	{ $$ = L();
	  append_list($$, $1);
	  append_symbol($$, $3); }
 ;

global_privileges:
    global_privilege	{ $$ = append_int(L(), $1); }
 |  global_privilege ',' global_privilege
			{ $$ = append_int(append_int(L(), $1), $3); }
 ;

global_privilege:
	COPY FROM 	{ $$ = PRIV_COPYFROMFILE; }
 |	COPY INTO 	{ $$ = PRIV_COPYINTOFILE; }
 ;

object_name:
     TABLE qname		{ $$ = _symbol_create_list(SQL_TABLE, $2); }
 |   qname			{ $$ = _symbol_create_list(SQL_NAME, $1); }
 |   routine_designator 	{ $$ = _symbol_create_list(SQL_FUNC, $1); }
/* | DOMAIN domain_name
   | CHARACTER SET char_set_name
   | COLLATION collation_name
   | TRANSLATION trans_name
   | TYPE udt_name
   | TYPE typed_table_name
*/
 ;

object_privileges:
    ALL PRIVILEGES			{ $$ = NULL; }
 |  ALL					{ $$ = NULL; }
 |  operation_commalist
 ;

operation_commalist:
    operation		{ $$ = append_symbol(L(), $1); }
 |  operation_commalist ',' operation
			{ $$ = append_symbol($1, $3); }
 ;

operation:
    INSERT			    { $$ = _symbol_create(SQL_INSERT,NULL); }
 |  sqlDELETE			    { $$ = _symbol_create(SQL_DELETE,NULL); }
 |  UPDATE opt_column_list          { $$ = _symbol_create_list(SQL_UPDATE,$2); }
 |  SELECT opt_column_list	    { $$ = _symbol_create_list(SQL_SELECT,$2); }
 |  REFERENCES opt_column_list 	    { $$ = _symbol_create_list(SQL_SELECT,$2); }
 |  execute			    { $$ = _symbol_create(SQL_EXECUTE,NULL); }
 ;

grantee_commalist:
    grantee			{ $$ = append_string(L(), $1); }
 |  grantee_commalist ',' grantee
				{ $$ = append_string($1, $3); }
 ;

grantee:
    PUBLIC			{ $$ = NULL; }
 |  authid			{ $$ = $1; }
 ;

/* DOMAIN, ASSERTION, CHARACTER SET, TRANSLATION, TRIGGER */

alter_statement:
   ALTER TABLE qname ADD opt_column add_table_element

	{ dlist *l = L();
	  append_list(l, $3);
	  append_symbol(l, $6);
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
 | ALTER TABLE qname ADD TABLE qname
	{ dlist *l = L();
	  append_list(l, $3);
	  append_symbol(l, _symbol_create_list( SQL_TABLE, $6));
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
 | ALTER TABLE qname ALTER alter_table_element
	{ dlist *l = L();
	  append_list(l, $3);
	  append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
 | ALTER TABLE qname DROP drop_table_element
	{ dlist *l = L();
	  append_list(l, $3);
	  append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
 | ALTER TABLE qname SET READ ONLY
	{ dlist *l = L();
	  append_list(l, $3);
	  append_symbol(l, _symbol_create_int(SQL_ALTER_TABLE, tr_readonly));
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
 | ALTER TABLE qname SET INSERT ONLY
	{ dlist *l = L();
	  append_list(l, $3);
	  append_symbol(l, _symbol_create_int(SQL_ALTER_TABLE, tr_append));
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
 | ALTER TABLE qname SET READ WRITE
	{ dlist *l = L();
	  append_list(l, $3);
	  append_symbol(l, _symbol_create_int(SQL_ALTER_TABLE, tr_writable));
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
 | ALTER USER ident passwd_schema
	{ dlist *l = L();
	  append_string(l, $3);
	  append_list(l, $4);
	  $$ = _symbol_create_list( SQL_ALTER_USER, l ); }
 | ALTER USER ident RENAME TO ident
	{ dlist *l = L();
	  append_string(l, $3);
	  append_string(l, $6);
	  $$ = _symbol_create_list( SQL_RENAME_USER, l ); }
 | ALTER USER SET opt_encrypted PASSWORD string USING OLD PASSWORD string
	{ dlist *l = L();
	  dlist *p = L();
	  append_string(l, NULL);
	  append_string(p, $6);
	  append_string(p, NULL);
	  append_int(p, $4);
	  append_string(p, $10);
	  append_list(l, p);
	  $$ = _symbol_create_list( SQL_ALTER_USER, l ); }
  ;

passwd_schema:
  	WITH opt_encrypted PASSWORD string	{ dlist * l = L();
				  append_string(l, $4);
				  append_string(l, NULL);
				  append_int(l, $2);
				  append_string(l, NULL);
				  $$ = l; }
  |	SET SCHEMA ident	{ dlist * l = L();
				  append_string(l, NULL);
				  append_string(l, $3);
				  append_int(l, 0);
				  append_string(l, NULL);
				  $$ = l; }
  |	WITH opt_encrypted PASSWORD string SET SCHEMA ident	
				{ dlist * l = L();
				  append_string(l, $4);
				  append_string(l, $7);
				  append_int(l, $2);
				  append_string(l, NULL);
				  $$ = l; }
  ;

alter_table_element:
	opt_column ident SET DEFAULT default_value
	{ dlist *l = L();
	  append_string(l, $2);
	  append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_DEFAULT, l); }
 |	opt_column ident SET sqlNULL
	{ dlist *l = L();
	  append_string(l, $2);
	  $$ = _symbol_create_list( SQL_NULL, l); }
 |	opt_column ident SET NOT sqlNULL
	{ dlist *l = L();
	  append_string(l, $2);
	  $$ = _symbol_create_list( SQL_NOT_NULL, l); }
 |	opt_column ident DROP DEFAULT
	{ $$ = _symbol_create( SQL_DROP_DEFAULT, $2); }
 |	opt_column ident SET STORAGE STRING
	{ dlist *l = L();
	  append_string(l, $2);
	  if (!strlen($5))
	  	append_string(l, NULL);
	  else
	  	append_string(l, $5);
	  $$ = _symbol_create_list( SQL_STORAGE, l); }
 |	opt_column ident SET STORAGE sqlNULL
	{ dlist *l = L();
	  append_string(l, $2);
	  append_string(l, NULL);
	  $$ = _symbol_create_list( SQL_STORAGE, l); }
 ;

drop_table_element:
     opt_column ident drop_action
	{ dlist *l = L();
	  append_string(l, $2 );
	  append_int(l, $3 );
	  $$ = _symbol_create_list( SQL_DROP_COLUMN, l ); }
  |  CONSTRAINT ident drop_action
	{ dlist *l = L();
	  append_string(l, $2 );
	  append_int(l, $3 );
	  $$ = _symbol_create_list( SQL_DROP_CONSTRAINT, l ); }
  |  TABLE ident drop_action
	{ dlist *l = L();
	  append_string(l, $2 );
	  append_int(l, $3 );
	  append_int(l, 0);
	  $$ = _symbol_create_list( SQL_DROP_TABLE, l ); }
  ;

opt_column:
     COLUMN	 { $$ = 0; }
 |   /* empty */ { $$ = 0; }
 ;

create_statement:	
   create role_def 	{ $$ = $2; }
 | create table_def 	{ $$ = $2; }
 | create view_def 	{ $$ = $2; }
 | type_def
 | func_def
 | index_def
 | seq_def
 | trigger_def
 ;

/*=== BEGIN SEQUENCES ===*/
seq_def:
/*
 * CREATE SEQUENCE name 
 *      [ AS datatype ]
 * 	[ START WITH start ] 
 * 	[ INCREMENT BY increment ]
 * 	[ MINVALUE minvalue | NO MINVALUE ]
 * 	[ MAXVALUE maxvalue | NO MAXVALUE ]
 * 	[ CACHE cache ] 		* not part of standard -- will be dropped *
 * 	[ [ NO ] CYCLE ]
 * start may be a value or subquery
 */
    create SEQUENCE qname opt_seq_params
	{
		dlist *l = L();
		append_list(l, $3);
		append_list(l, $4);
		append_int(l, 0); /* to be dropped */
		$$ = _symbol_create_list(SQL_CREATE_SEQ, l);
	}
/*
 * DROP SEQUENCE name
 */
  | drop SEQUENCE qname
	{
		dlist *l = L();
		append_list(l, $3);
		$$ = _symbol_create_list(SQL_DROP_SEQ, l);
	}
/*
 * ALTER SEQUENCE name
 *      [ AS datatype ]
 * 	[ RESTART [ WITH start ] ] 
 * 	[ INCREMENT BY increment ]
 * 	[ MINVALUE minvalue | NO MINVALUE ]
 * 	[ MAXVALUE maxvalue | NO MAXVALUE ]
 * 	[ CACHE cache ] 		* not part of standard -- will be dropped *
 * 	[ [ NO ] CYCLE ]
 * start may be a value or subquery
 */
  | ALTER SEQUENCE qname opt_alt_seq_params 	
	{
		dlist *l = L();
		append_list(l, $3);
		append_list(l, $4); 
		$$ = _symbol_create_list(SQL_ALTER_SEQ, l);
	}
  ;

opt_seq_params:
	opt_seq_param				{ $$ = append_symbol(L(), $1); }
  |	opt_seq_params opt_seq_param		{ $$ = append_symbol($1, $2); }
  ;

opt_alt_seq_params:
	opt_alt_seq_param			{ $$ = append_symbol(L(), $1); }
  |	opt_alt_seq_params opt_alt_seq_param	{ $$ = append_symbol($1, $2); }
  ;

opt_seq_param:
    	AS data_type 			{ $$ = _symbol_create_list(SQL_TYPE, append_type(L(),&$2)); }
  |	START WITH poslng 		{ $$ = _symbol_create_lng(SQL_START, $3); }
  |	opt_seq_common_param		{ $$ = $1; }
  ;

opt_alt_seq_param:
    	AS data_type 			{ $$ = _symbol_create_list(SQL_TYPE, append_type(L(),&$2)); }
  |	RESTART 			{ $$ = _symbol_create_list(SQL_START, append_int(L(),0)); /* plain restart now */ }
  |	RESTART WITH poslng 		{ $$ = _symbol_create_list(SQL_START, append_lng(append_int(L(),2), $3));  }
  |	RESTART WITH subquery 		{ $$ = _symbol_create_list(SQL_START, append_symbol(append_int(L(),1), $3));  }
  |	opt_seq_common_param		{ $$ = $1; }
  ;

opt_seq_common_param:
  	INCREMENT BY nonzerolng		{ $$ = _symbol_create_lng(SQL_INC, $3); }
  |	MINVALUE nonzerolng		{ $$ = _symbol_create_lng(SQL_MINVALUE, $2); }
  |	NOMINVALUE			{ $$ = _symbol_create_lng(SQL_MINVALUE, 0); }
  |	MAXVALUE nonzerolng		{ $$ = _symbol_create_lng(SQL_MAXVALUE, $2); }
  |	NOMAXVALUE			{ $$ = _symbol_create_lng(SQL_MAXVALUE, 0); }
  |	CACHE nonzerolng		{ $$ = _symbol_create_lng(SQL_CACHE, $2); }
  |	CYCLE				{ $$ = _symbol_create_int(SQL_CYCLE, 1); }
  |	NOCYCLE				{ $$ = _symbol_create_int(SQL_CYCLE, 0); }
  ;

/*=== END SEQUENCES ===*/


index_def:
    create opt_index_type INDEX ident ON qname '(' ident_commalist ')'
	{ dlist *l = L();
	  append_string(l, $4);
	  append_int(l, $2);
	  append_list(l, $6);
	  append_list(l, $8);
	  $$ = _symbol_create_list( SQL_CREATE_INDEX, l); }
  ;

opt_index_type:
     UNIQUE		{ $$ = hash_idx; }
 |   ORDERED		{ $$ = ordered_idx; }
 |   IMPRINTS		{ $$ = imprints_idx; }
 |   /* empty */	{ $$ = hash_idx; }
 ;

/* sql-server def
CREATE [ UNIQUE ] INDEX index_name
    ON { table | view } ( column [ ASC | DESC ] [ ,...n ] )
[ WITH < index_option > [ ,...n] ]
[ ON filegroup ]

< index_option > :: =
    { PAD_INDEX |
        FILLFACTOR = fillfactor |
        IGNORE_DUP_KEY |
        DROP_EXISTING |
    STATISTICS_NORECOMPUTE |
    SORT_IN_TEMPDB
}
*/

role_def:
    ROLE ident opt_grantor
	{ dlist *l = L();
	  append_string(l, $2);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_CREATE_ROLE, l ); }
 |  USER ident WITH opt_encrypted PASSWORD string sqlNAME string SCHEMA ident
	{ dlist *l = L();
	  append_string(l, $2);
	  append_string(l, $6);
	  append_string(l, $8);
	  append_string(l, $10);
	  append_int(l, $4);
	  $$ = _symbol_create_list( SQL_CREATE_USER, l ); }
 ;

opt_encrypted:
    /* empty */		{ $$ = SQL_PW_UNENCRYPTED; }
 |  UNENCRYPTED		{ $$ = SQL_PW_UNENCRYPTED; }
 |  ENCRYPTED		{ $$ = SQL_PW_ENCRYPTED; }
 ;

table_opt_storage:
    /* empty */		 { $$ = NULL; }
 |  STORAGE ident STRING { $$ = append_string(append_string(L(), $2), $3); } 
 ;

table_def:
    TABLE if_not_exists qname table_content_source  table_opt_storage
	{ int commit_action = CA_COMMIT;
	  dlist *l = L();

	  append_int(l, SQL_PERSIST);
	  append_list(l, $3);
	  append_symbol(l, $4);
	  append_int(l, commit_action);
	  append_string(l, NULL);
	  append_int(l, $2);
	  append_list(l, $5);
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
 |  TABLE if_not_exists qname FROM sqlLOADER func_ref
    {
      dlist *l = L();
      append_list(l, $3);
      append_symbol(l, $6);
      $$ = _symbol_create_list( SQL_CREATE_TABLE_LOADER, l);
    }
 |  STREAM TABLE if_not_exists qname table_content_source 
	{ int commit_action = CA_COMMIT, tpe = SQL_STREAM;
	  dlist *l = L();

	  append_int(l, tpe);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  append_int(l, commit_action);
	  append_string(l, NULL);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
 |  MERGE TABLE if_not_exists qname table_content_source 
	{ int commit_action = CA_COMMIT, tpe = SQL_MERGE_TABLE;
	  dlist *l = L();

	  append_int(l, tpe);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  append_int(l, commit_action);
	  append_string(l, NULL);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
 |  REPLICA TABLE if_not_exists qname table_content_source 
	{ int commit_action = CA_COMMIT, tpe = SQL_REPLICA_TABLE;
	  dlist *l = L();

	  append_int(l, tpe);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  append_int(l, commit_action);
	  append_string(l, NULL);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
 /* mapi:monetdb://host:port/database[/schema[/table]] 
    This also allows access via monetdbd. 
    We assume the monetdb user with default password */
 |  REMOTE TABLE if_not_exists qname table_content_source ON STRING
	{ int commit_action = CA_COMMIT, tpe = SQL_REMOTE;
	  dlist *l = L();

	  append_int(l, tpe);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  append_int(l, commit_action);
	  append_string(l, $7);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
  | opt_temp TABLE if_not_exists qname table_content_source opt_on_commit 
	{ int commit_action = CA_COMMIT;
	  dlist *l = L();

	  append_int(l, $1);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  if ($1 != SQL_PERSIST)
		commit_action = $6;
	  append_int(l, commit_action);
	  append_string(l, NULL);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
 ;

opt_temp:
    TEMPORARY		{ $$ = SQL_LOCAL_TEMP; }
 |  TEMP		{ $$ = SQL_LOCAL_TEMP; }
 |  LOCAL TEMPORARY	{ $$ = SQL_LOCAL_TEMP; }
 |  LOCAL TEMP		{ $$ = SQL_LOCAL_TEMP; }
 |  GLOBAL TEMPORARY	{ $$ = SQL_GLOBAL_TEMP; }
 |  GLOBAL TEMP		{ $$ = SQL_GLOBAL_TEMP; }
 ;

opt_on_commit: /* only for temporary tables */
    /* empty */			 { $$ = CA_COMMIT; } 
 |  ON COMMIT sqlDELETE ROWS	 { $$ = CA_DELETE; }
 |  ON COMMIT PRESERVE ROWS	 { $$ = CA_PRESERVE; }
 |  ON COMMIT DROP	 	 { $$ = CA_DROP; }
 ;

table_content_source:
    '(' table_element_list ')'	{ $$ = _symbol_create_list( SQL_CREATE_TABLE, $2); }
 |  as_subquery_clause		{ $$ = _symbol_create_list( SQL_SELECT, $1); }		
 ;

as_subquery_clause:
	opt_column_list
	AS
	query_expression_def
	with_or_without_data
			{ $$ = append_list(L(), $1);
			  append_symbol($$, $3); 
			  append_int($$, $4); }
 ;

with_or_without_data:
	 /* empty */	{ $$ = 1; }
 |   WITH NO DATA  	{ $$ = 0; }
 |   WITH DATA 		{ $$ = 1; }
 ;

table_element_list:
    table_element
			{ $$ = append_symbol(L(), $1); }
 |  table_element_list ',' table_element
			{ $$ = append_symbol( $1, $3 ); }
 ;

add_table_element: column_def | table_constraint ;
table_element: add_table_element | column_options | like_table ;

serial_or_bigserial:
	SERIAL       { $$ = 0; }
 |	BIGSERIAL    { $$ = 1; }
 ;

column_def:
	column data_type opt_column_def_opt_list
		{
			dlist *l = L();
			append_string(l, $1);
			append_type(l, &$2);
			append_list(l, $3);
			$$ = _symbol_create_list(SQL_COLUMN, l);
		}
 |  column serial_or_bigserial
		{ /* SERIAL = INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY */
			/* handle multi-statements by wrapping them in a list */
			sql_subtype it;
			dlist* stmts;
			/* note: sql_next_seq_name uses sa_alloc */
			str sn = sql_next_seq_name(m);
			dlist *p; /* primary key */
			/* sequence generation code */
			dlist *l = L();
			/* finally all the options */
			dlist *o = L();

			/* the name of the sequence */
			dlist *seqn1 = L(), *seqn2 = L();

			if (m->scanner.schema)
				append_string(seqn1, m->scanner.schema);
			append_list(l, append_string(seqn1, sn));
			if ($2 == 1)
				sql_find_subtype(&it, "bigint", 64, 0);
			else
				sql_find_subtype(&it, "int", 32, 0);
    			append_symbol(o, _symbol_create_list(SQL_TYPE, append_type(L(),&it)));
			append_list(l, o);
			append_int(l, 1); /* to be dropped */

			if (m->sym) {
				stmts = m->sym->data.lval;
			} else {
				stmts = L();
				m->sym = _symbol_create_list(SQL_MULSTMT, stmts);
			}	
			append_symbol(stmts, _symbol_create_list(SQL_CREATE_SEQ, l));

			l = L();
			append_string(l, $1);
			append_type(l, &it);
			o = L();
			if (m->scanner.schema)
				append_string(seqn2, m->scanner.schema);
			append_string(seqn2, sn);
			append_symbol(o, _symbol_create_symbol(SQL_DEFAULT, _symbol_create_list(SQL_NEXT, seqn2)));
			p = L();
			append_string(p, NULL);
			append_symbol(p, _symbol_create(SQL_PRIMARY_KEY, NULL));
			append_symbol(o, _symbol_create_list(SQL_CONSTRAINT, p));
			append_list(l, o);
			$$ = _symbol_create_list(SQL_COLUMN, l);
		}
 ;

opt_column_def_opt_list:
    /* empty */			{ $$ = NULL; }
 | column_def_opt_list
 ;

column_def_opt_list:
    column_option
			{ $$ = append_symbol(L(), $1 ); }
 |  column_def_opt_list column_option
			{ $$ = append_symbol( $1, $2 ); }
 ;

column_options:
    ident WITH OPTIONS '(' column_option_list ')'

	{ dlist *l = L();
	  append_string(l, $1 );
	  append_list(l, $5 );
	  $$ = _symbol_create_list( SQL_COLUMN_OPTIONS, l ); }
 ;

column_option_list:
    column_option
			{ $$ = append_symbol(L(), $1 ); }
 |  column_option_list ',' column_option
			{ $$ = append_symbol($1, $3 ); }
 ;

column_option: default | column_constraint | generated_column;

default:
    DEFAULT default_value { $$ = _symbol_create_symbol(SQL_DEFAULT, $2); }
 ;

default_value:
    simple_scalar_exp 	{ $$ = $1; }
 ;

column_constraint:
    opt_constraint_name column_constraint_type  /*opt_constraint_attributes*/

	{ dlist *l = L();
	  append_string(l, $1 );
	  append_symbol(l, $2 );
	  $$ = _symbol_create_list( SQL_CONSTRAINT, l ); }
 ;

generated_column:
	GENERATED ALWAYS AS IDENTITY serial_opt_params
	{
		/* handle multi-statements by wrapping them in a list */
		sql_subtype it;
		dlist* stmts;
		/* note: sql_next_seq_name uses sa_alloc */
		str sn = sql_next_seq_name(m);
		/* sequence generation code */
		dlist *l = L();
		/* the name of the sequence */
		append_list(l, append_string(L(), sn));
		if (!$5)
			$5 = L();
		sql_find_subtype(&it, "int", 32, 0);
    		append_symbol($5, _symbol_create_list(SQL_TYPE, append_type(L(),&it)));

		/* finally all the options */
		append_list(l, $5);
		append_int(l, 0); /* to be dropped */
		$$ = _symbol_create_symbol(SQL_DEFAULT, _symbol_create_list(SQL_NEXT, append_string(L(), sn)));

		if (m->sym) {
			stmts = m->sym->data.lval;
		} else {
			stmts = L();
			m->sym = _symbol_create_list(SQL_MULSTMT, stmts);
		}	
		append_symbol(stmts, _symbol_create_list(SQL_CREATE_SEQ, l));
	}
 |	AUTO_INCREMENT
	{
		/* handle multi-statements by wrapping them in a list */
		sql_subtype it;
		dlist* stmts;
		/* note: sql_next_seq_name uses sa_alloc */
		str sn = sql_next_seq_name(m);
		/* sequence generation code */
		dlist *l = L();
		/* finally all the options */
		dlist *o = L();

		/* the name of the sequence */
		dlist *seqn1 = L(), *seqn2 = L();

		if (m->scanner.schema)
			append_string(seqn1, m->scanner.schema);
		append_list(l, append_string(seqn1, sn));
		sql_find_subtype(&it, "int", 32, 0);
    		append_symbol(o, _symbol_create_list(SQL_TYPE, append_type(L(),&it)));
		append_list(l, o);
		append_int(l, 0); /* to be dropped */
		if (m->scanner.schema)
			append_string(seqn2, m->scanner.schema);
		append_string(seqn2, sn);
		$$ = _symbol_create_symbol(SQL_DEFAULT, _symbol_create_list(SQL_NEXT, seqn2));

		if (m->sym) {
			stmts = m->sym->data.lval;
		} else {
			stmts = L();
			m->sym = _symbol_create_list(SQL_MULSTMT, stmts);
		}	
		append_symbol(stmts, _symbol_create_list(SQL_CREATE_SEQ, l));
	}
 ;

serial_opt_params:
	/* empty: return the defaults */ 	{ $$ = NULL; }
  |	'(' opt_seq_params ')'			{ $$ = $2; }
 ;


table_constraint:
    opt_constraint_name table_constraint_type  /*opt_constraint_attributes*/

	{ dlist *l = L();
	  append_string(l, $1 );
	  append_symbol(l, $2 );
	  $$ = _symbol_create_list( SQL_CONSTRAINT, l ); }
 ;

/* opt_constraint_attributes: ; */

opt_constraint_name:
    /* empty */			{ $$ = NULL; }
 |  CONSTRAINT ident		{ $$ = $2; }
 ;

ref_action:
	NO ACTION		{ $$ = 0; }
 |	CASCADE			{ $$ = 1; }
 |	RESTRICT		{ $$ = 2; }
 |	SET sqlNULL		{ $$ = 3; }
 |	SET DEFAULT		{ $$ = 4; }
 ;

ref_on_update:
   ON UPDATE ref_action         { $$ = ($3 << 8); }
;

ref_on_delete:
   ON sqlDELETE ref_action      { $$ = $3; }
 ;

opt_ref_action:
   /* empty */                          { $$ = (2 << 8) + 2; /* defaults are RESTRICT */ }
 | ref_on_update                        { $$ = $1; }
 | ref_on_delete                        { $$ = $1; }
 | ref_on_delete ref_on_update          { $$ = $1 + $2; }
 | ref_on_update ref_on_delete          { $$ = $1 + $2; }
 ;

opt_match_type:
    /* empty */			{ $$ = 0; }
 | FULL				{ $$ = 1; }
 | PARTIAL			{ $$ = 2; }
 | SIMPLE			{ $$ = 0; }
 ;

opt_match:
    /* empty */			{ $$ = 0; }
 | MATCH opt_match_type		{ $$ = $2; }
 ;

column_constraint_type:
    NOT sqlNULL	{ $$ = _symbol_create( SQL_NOT_NULL, NULL); }
 |  sqlNULL	{ $$ = _symbol_create( SQL_NULL, NULL); }
 |  UNIQUE	{ $$ = _symbol_create( SQL_UNIQUE, NULL ); }
 |  PRIMARY KEY	{ $$ = _symbol_create( SQL_PRIMARY_KEY, NULL ); }
 |  REFERENCES qname opt_column_list opt_match opt_ref_action

			{ dlist *l = L();
			  append_list(l, $2 );
			  append_list(l, $3 );
			  append_int(l, $4 );
			  append_int(l, $5 );
			  $$ = _symbol_create_list( SQL_FOREIGN_KEY, l); }
 /*TODO: Implemente domain_constraint_type*/

 |  domain_constraint_type
 ;

table_constraint_type:
    UNIQUE column_commalist_parens
			{ $$ = _symbol_create_list( SQL_UNIQUE, $2); }
 |  PRIMARY KEY column_commalist_parens
			{ $$ = _symbol_create_list( SQL_PRIMARY_KEY, $3); }
 |  FOREIGN KEY column_commalist_parens
    REFERENCES qname opt_column_list opt_match opt_ref_action

			{ dlist *l = L();
			  append_list(l, $5 );
			  append_list(l, $3 );
			  append_list(l, $6 );
			  append_int(l, $7 );
			  append_int(l, $8 );
			  $$ = _symbol_create_list( SQL_FOREIGN_KEY, l); }
 /*TODO: Implemente domain_constraint_type*/
 ;

domain_constraint_type:
/*    CHECK '(' search_condition ')' { $$ = _symbol_create_symbol(SQL_CHECK, $3); }*/
    CHECK '(' search_condition ')' { $$ = NULL; }
 ;

ident_commalist:
    ident
			{ $$ = append_string(L(), $1); }
 |  ident_commalist ',' ident
			{ $$ = append_string( $1, $3 ); }
 ;

like_table:
	LIKE qname	{ $$ = _symbol_create_list( SQL_LIKE, $2 ); }
 ;

view_def:
    VIEW qname opt_column_list AS query_expression_def opt_with_check_option
	{  dlist *l = L();
	  append_list(l, $2);
	  append_list(l, $3);
	  append_symbol(l, $5);
	  append_int(l, $6);
	  append_int(l, TRUE);	/* persistent view */
	  $$ = _symbol_create_list( SQL_CREATE_VIEW, l ); 
	}
  ;

query_expression_def:
	query_expression
  |	'(' query_expression_def ')'	{ $$ = $2; }
  ;

query_expression:
	select_no_parens_orderby
  |	with_query
  ;

opt_with_check_option:
    /* empty */			{ $$ = FALSE; }
 |  WITH CHECK OPTION		{ $$ = TRUE; }
 ;

opt_column_list:
    /* empty */			{ $$ = NULL; }
 | column_commalist_parens
 ;

column_commalist_parens:
   '(' ident_commalist ')'	{ $$ = $2; }
 ;

type_def:
    create TYPE qname EXTERNAL sqlNAME ident
			{ dlist *l = L();
				append_list(l, $3);
				append_string(l, $6);
			  $$ = _symbol_create_list( SQL_CREATE_TYPE, l ); }
 ;

external_function_name:
	ident '.' ident { $$ = append_string(append_string(L(), $1), $3); }
 ;


function_body:
	X_BODY
|	string
;


func_def:
    create_or_replace FUNCTION qname
	'(' opt_paramlist ')'
    RETURNS func_data_type
    EXTERNAL sqlNAME external_function_name 	
			{ dlist *f = L();
				append_list(f, $3);
				append_list(f, $5);
				append_symbol(f, $8);
				append_list(f, $11);
				append_list(f, NULL);
				append_int(f, F_FUNC);
				append_int(f, FUNC_LANG_MAL);
				append_int(f, $1);
			  $$ = _symbol_create_list( SQL_CREATE_FUNC, f ); }
 |  create_or_replace FUNCTION qname
	'(' opt_paramlist ')'
    RETURNS func_data_type
    routine_body
			{ dlist *f = L();
				append_list(f, $3);
				append_list(f, $5);
				append_symbol(f, $8);
				append_list(f, NULL);
				append_list(f, $9);
				append_int(f, F_FUNC);
				append_int(f, FUNC_LANG_SQL);
				append_int(f, $1);
			  $$ = _symbol_create_list( SQL_CREATE_FUNC, f ); }
  | create_or_replace FUNCTION qname
	'(' opt_paramlist ')'
    RETURNS func_data_type
    LANGUAGE IDENT function_body { 
			int lang = 0;
			dlist *f = L();
			char l = *$10;

			if (l == 'R' || l == 'r')
				lang = FUNC_LANG_R;
			else if (l == 'P' || l == 'p')
            {
            	// code does not get cleaner than this people
                if (strcasecmp($10, "PYTHON_MAP") == 0) {
					lang = FUNC_LANG_MAP_PY;
                } else if (strcasecmp($10, "PYTHON3_MAP") == 0) {
                	lang = FUNC_LANG_MAP_PY3;
                } else if (strcasecmp($10, "PYTHON3") == 0) {
                	lang = FUNC_LANG_PY3;
                } else if (strcasecmp($10, "PYTHON2_MAP") == 0) {
                	lang = FUNC_LANG_MAP_PY2;
                } else if (strcasecmp($10, "PYTHON2") == 0) {
                	lang = FUNC_LANG_PY2;
                } else {
                	lang = FUNC_LANG_PY;
                }
            }
			else if (l == 'C' || l == 'c')
				lang = FUNC_LANG_C;
			else if (l == 'J' || l == 'j')
				lang = FUNC_LANG_J;
			else {
				char *msg = sql_message("Language name R, C, PYTHON[3], PYTHON[3]_MAP or J(avascript):expected, received '%c'", l);
				yyerror(m, msg);
				_DELETE(msg);
			}

			append_list(f, $3);
			append_list(f, $5);
			append_symbol(f, $8);
			append_list(f, NULL); 
			append_list(f, append_string(L(), $11));
			append_int(f, F_FUNC);
			append_int(f, lang);
			append_int(f, $1);
			$$ = _symbol_create_list( SQL_CREATE_FUNC, f ); }
  | create_or_replace FILTER FUNCTION qname
	'(' opt_paramlist ')'
    EXTERNAL sqlNAME external_function_name 	
			{ dlist *f = L();
				append_list(f, $4);
				append_list(f, $6); 
				/* no returns - use OID */
				append_symbol(f, NULL); 
				append_list(f, $10);
				append_list(f, NULL);
				append_int(f, F_FILT);
				append_int(f, FUNC_LANG_MAL);
				append_int(f, $1);
			  $$ = _symbol_create_list( SQL_CREATE_FUNC, f ); }
  | create_or_replace AGGREGATE qname
	'(' opt_paramlist ')'
    RETURNS func_data_type
    EXTERNAL sqlNAME external_function_name 	
			{ dlist *f = L();
				append_list(f, $3);
				append_list(f, $5);
				append_symbol(f, $8);
				append_list(f, $11);
				append_list(f, NULL);
				append_int(f, F_AGGR);
				append_int(f, FUNC_LANG_MAL);
				append_int(f, $1);
			  $$ = _symbol_create_list( SQL_CREATE_FUNC, f ); }
  | create_or_replace AGGREGATE qname
	'(' opt_paramlist ')'
    RETURNS func_data_type
    LANGUAGE IDENT function_body { 
			int lang = 0;
			dlist *f = L();
			char l = *$10;

			if (l == 'R' || l == 'r')
				lang = FUNC_LANG_R;
			else if (l == 'P' || l == 'p')
            {
                if (strcasecmp($10, "PYTHON_MAP") == 0) {
					lang = FUNC_LANG_MAP_PY;
                } else if (strcasecmp($10, "PYTHON3_MAP") == 0) {
                	lang = FUNC_LANG_MAP_PY3;
                } else if (strcasecmp($10, "PYTHON3") == 0) {
                	lang = FUNC_LANG_PY3;
                } else if (strcasecmp($10, "PYTHON2_MAP") == 0) {
                	lang = FUNC_LANG_MAP_PY2;
                } else if (strcasecmp($10, "PYTHON2") == 0) {
                	lang = FUNC_LANG_PY2;
                } else {
                	lang = FUNC_LANG_PY;
                }
            }
			else if (l == 'C' || l == 'c')
				lang = FUNC_LANG_C;
			else if (l == 'J' || l == 'j')
				lang = FUNC_LANG_J;
			else {
				char *msg = sql_message("Language name R, C, PYTHON[3], PYTHON[3]_MAP or J(avascript):expected, received '%c'", l);
				yyerror(m, msg);
				_DELETE(msg);
			}

			append_list(f, $3);
			append_list(f, $5);
			append_symbol(f, $8);
			append_list(f, NULL);
			append_list(f, append_string(L(), $11));
			append_int(f, F_AGGR);
			append_int(f, lang);
			append_int(f, $1);
			$$ = _symbol_create_list( SQL_CREATE_FUNC, f ); }
 | /* proc ie no result */
    create_or_replace PROCEDURE qname
	'(' opt_paramlist ')'
    EXTERNAL sqlNAME external_function_name 	
			{ dlist *f = L();
				append_list(f, $3);
				append_list(f, $5);
				append_symbol(f, NULL); /* no result */
				append_list(f, $9);
				append_list(f, NULL);
				append_int(f, F_PROC);
				append_int(f, FUNC_LANG_MAL);
				append_int(f, $1);
			  $$ = _symbol_create_list( SQL_CREATE_FUNC, f ); }
  | create_or_replace PROCEDURE qname
	'(' opt_paramlist ')'
    routine_body
			{ dlist *f = L();
				append_list(f, $3);
				append_list(f, $5);
				append_symbol(f, NULL); /* no result */
				append_list(f, NULL); 
				append_list(f, $7);
				append_int(f, F_PROC);
				append_int(f, FUNC_LANG_SQL);
				append_int(f, $1);
			  $$ = _symbol_create_list( SQL_CREATE_FUNC, f ); }
  |	create_or_replace sqlLOADER qname
	'(' opt_paramlist ')'
    LANGUAGE IDENT function_body { 
			int lang = 0;
			dlist *f = L();
			char l = *$8;
			/* other languages here if we ever get to it */
			if (l == 'P' || l == 'p')
            {
                lang = FUNC_LANG_PY;
            }
			else
				yyerror(m, sql_message("Language name P(ython) expected, received '%c'", l));

			append_list(f, $3);
			append_list(f, $5);
			append_symbol(f, NULL);
			append_list(f, NULL); 
			append_list(f, append_string(L(), $9));
			append_int(f, F_LOADER);
			append_int(f, lang);
			append_int(f, $1);
			$$ = _symbol_create_list( SQL_CREATE_FUNC, f ); }
;

routine_body:
	procedure_statement 
		{ $$ = L(); append_symbol( $$, $1); }
 |  BEGIN
	procedure_statement_list procedure_statement SCOLON 
    END
		{ $$ = append_symbol($2,$3); }
 |  BEGIN ATOMIC
	procedure_statement_list procedure_statement SCOLON 
    END
		{ $$ = append_symbol($3,$4); }
 ;

/* change into compound statement 
<compound statement> ::=
                [ <beginning label> <colon> ] BEGIN [ [ NOT ] ATOMIC ]
                [ <local declaration list> ] [ <local cursor declaration list> ] [ <local handler declaration list> ]
                [ <SQL statement list> ] END [ <ending label> ]

<beginning label> ::= <statement label>

<statement label> ::= <identifier>
*/

procedure_statement_list:
    /* empty*/ 	 { $$ = L(); }
 |  procedure_statement_list 
    procedure_statement SCOLON 	{ $$ = append_symbol($1,$2);}
 ;

trigger_procedure_statement_list:
    /* empty*/ 	 { $$ = L(); }
 |  trigger_procedure_statement_list 
    trigger_procedure_statement SCOLON 	{ $$ = append_symbol($1,$2);}
 ;

procedure_statement:
	transaction_statement
    |	update_statement
    |	schema
    | 	grant
    | 	revoke
    | 	create_statement
    |	drop_statement
    |	alter_statement
    |   declare_statement
    |   set_statement
    |	control_statement
    |   select_statement_single_row
    ;

trigger_procedure_statement:
	transaction_statement
    |	update_statement
    | 	grant
    | 	revoke
    |   declare_statement
    |   set_statement
    |	control_statement
    |   select_statement_single_row
    ;

control_statement:
	call_procedure_statement
    |	call_statement
    |   while_statement
    |   if_statement
    |   case_statement
    |	return_statement
/*
    |   for_statement		fetch tuples, not supported because of cursors 

    |   loop_statement		while (true) 
    |   repeat_statement	do while 

    |   leave_statement 	multilevel break 
    |   iterate_statement	multilevel continue 
*/
    ;

call_statement:
	CALL routine_invocation 	{ $$ = $2; }
    ;

call_procedure_statement:
	CALL func_ref		 	{$$ = _symbol_create_symbol(SQL_CALL, $2);}
    ;

routine_invocation: 
	routine_name '(' argument_list ')'
		{ dlist *l = L(); 
		  append_list( l, $1);
		  append_list( l, $3);
		  assert(0);
		  $$ = _symbol_create_list( SQL_FUNC, l);
		}
    ;

routine_name: qname ;

argument_list:
 /*empty*/		{$$ = L();}
 |  scalar_exp 		{ $$ = append_symbol( L(), $1); }
 |  argument_list ',' scalar_exp
			{ $$ = append_symbol( $1, $3); }
 ;


return_statement:
        RETURN return_value { $$ = _symbol_create_symbol(SQL_RETURN, $2); }
   ;

return_value:
      query_expression
   |  search_condition
   |  TABLE '(' query_expression ')'	
		{ $$ = _symbol_create_symbol(SQL_TABLE, $3); }
   ;

case_statement:
     CASE scalar_exp when_statements case_opt_else_statement END CASE
		{ $$ = _symbol_create_list(SQL_CASE,
		   append_list(
		    append_list(
		     append_symbol(
		      L(),$2),$3),$4)); }
 |   CASE when_search_statements case_opt_else_statement END CASE
		 { $$ = _symbol_create_list(SQL_CASE,
		   append_list(
		    append_list(
		     L(),$2),$3)); }
 ;

when_statement:
    WHEN scalar_exp THEN procedure_statement_list
			{ $$ = _symbol_create_list( SQL_WHEN,
			   append_list(
			    append_symbol(
			     L(), $2),$4)); }
 ;

when_statements:
    when_statement
			{ $$ = append_symbol( L(), $1);}
 |  when_statements when_statement
			{ $$ = append_symbol( $1, $2); }
 ;

when_search_statement:
    WHEN search_condition THEN procedure_statement_list
			{ $$ = _symbol_create_list( SQL_WHEN,
			   append_list(
			    append_symbol(
			     L(), $2),$4)); }
 ;

when_search_statements:
    when_search_statement
			{ $$ = append_symbol( L(), $1); }
 |  when_search_statements when_search_statement
			{ $$ = append_symbol( $1, $2); }
 ;

case_opt_else_statement:
    /* empty */	        		{ $$ = NULL; }
 |  ELSE procedure_statement_list	{ $$ = $2; }
 ;

		/* data types, more types to come */


if_statement:
	IF search_condition THEN procedure_statement_list 
	if_opt_else
	END IF
		{ dlist *l = L();
		  append_symbol(l, $2);
		  append_list(l, $4);
		  append_symbol(l, $5);
		  $$ = _symbol_create_list(SQL_IF, l);
		}
		  
	;

if_opt_else:
	/* empty */ 	
		{ $$ = NULL; }
   |	ELSE procedure_statement_list 
	  	{ $$ = _symbol_create_list(SQL_ELSE, $2); }
   |	ELSEIF search_condition THEN procedure_statement_list 
	if_opt_else
		{ dlist *l = L();
		  append_symbol(l, $2);
		  append_list(l, $4);
		  append_symbol(l, $5);
		  { $$ = _symbol_create_list(SQL_IF, l); }
		}
	;

while_statement:
	opt_begin_label
	WHILE search_condition DO
	procedure_statement_list
	END WHILE opt_end_label

		{ dlist *l;
		  char *label = $1?$1:$8;
		  if ($1 && $8 && strcmp($1, $8) != 0) {
			$$ = NULL;
			yyerror(m, "WHILE: labels should match");
			YYABORT;
		  }
 		  l = L();
		  append_symbol(l, $3); /* condition */
		  append_list(l, $5);	/* statements */
		  append_string(l, label);
		  $$ = _symbol_create_list(SQL_WHILE, l);
		}
 ;

opt_begin_label:
	/* empty */ 	{ $$ = NULL; }
 |	ident ':'
 ;

opt_end_label:
	/* empty */ 	{ $$ = NULL; }
 |	ident 
 ;
	
	
table_function_column_list:
	column data_type	{ $$ = L();
				  append_string($$, $1);
			  	  append_type($$, &$2);
				}
  |     table_function_column_list ',' column data_type
				{ 
				  append_string($$, $3);
			  	  append_type($$, &$4);
				}
  ;

func_data_type:
    TABLE '(' table_function_column_list ')'	
		{ $$ = _symbol_create_list(SQL_TABLE, $3); }
 |  data_type
		{ $$ = _symbol_create_list(SQL_TYPE, append_type(L(),&$1)); }
 ;

opt_paramlist:
    paramlist
 |  '*'			{ dlist *vararg = L();
			  append_string(vararg, "*");
			  append_type(vararg, NULL);
			  $$ = append_list(L(), vararg); }
 |			{ $$ = NULL; }
 ;

paramlist:
    paramlist ',' ident data_type
			{ dlist *p = L();
			  append_string(p, $3);
			  append_type(p, &$4);
			  $$ = append_list($1, p); }
 |  ident data_type
			{ dlist *l = L();
			  dlist *p = L();
			  append_string(p, $1);
			  append_type(p, &$2);
			  $$ = append_list(l, p); }
 ;

/*
Define triggered SQL-statements.

  <trigger definition>    ::=
         CREATE TRIGGER <trigger name> <trigger action time> <trigger event>
         ON <table name> [ REFERENCING <old or new values alias list> ]
         <triggered action>

  <trigger action time>    ::=   BEFORE | AFTER | INSTEAD OF

  <trigger event>    ::=   INSERT | DELETE | UPDATE [ OF <trigger column list> ]

  <trigger column list>    ::=   <column name list>

  <triggered action>    ::=
         [ FOR EACH { ROW | STATEMENT } ]
         [ WHEN <left paren> <search condition> <right paren> ]
         <triggered SQL statement>

  <triggered SQL statement>    ::=
         <SQL procedure statement>
     |     BEGIN ATOMIC { <SQL procedure statement> <semicolon> }... END

  <old or new values alias list>    ::=   <old or new values alias> ...

  <old or new values alias>    ::=
         OLD [ ROW ] [ AS ] <old values correlation name>
     |   NEW [ ROW ] [ AS ] <new values correlation name>
     |   OLD TABLE [ AS ] <old values table alias>
     |   NEW TABLE [ AS ] <new values table alias>

  <old values table alias>    ::=   <identifier>

  <new values table alias>    ::=   <identifier>

  <old values correlation name>    ::=   <correlation name>

  <new values correlation name>    ::=   <correlation name>
*/

trigger_def:
    create TRIGGER qname trigger_action_time trigger_event
    ON qname opt_referencing_list triggered_action
	{ dlist *l = L();
	  append_list(l, $3);
	  append_int(l, $4);
	  append_symbol(l, $5);
	  append_list(l, $7);
	  append_list(l, $8);
	  append_list(l, $9);
	  $$ = _symbol_create_list(SQL_CREATE_TRIGGER, l); 
	}
 ;

trigger_action_time:
    BEFORE	{ $$ = 0; }
 |  AFTER	{ $$ = 1; }
/* | INSTEAD OF { $$ = 2; } */
 ;

trigger_event:
    INSERT 			{ $$ = _symbol_create_list(SQL_INSERT, NULL); }
 |  sqlDELETE 			{ $$ = _symbol_create_list(SQL_DELETE, NULL); }
 |  UPDATE 			{ $$ = _symbol_create_list(SQL_UPDATE, NULL); }
 |  UPDATE OF ident_commalist 	{ $$ = _symbol_create_list(SQL_UPDATE, $3); }
 ;

opt_referencing_list:
    /* empty */ 				{ $$ = NULL; }
 |  REFERENCING old_or_new_values_alias_list 	{ $$ = $2; }
 ;

old_or_new_values_alias_list:
    old_or_new_values_alias	{ $$ = append_list(L(), $1); }
 |  old_or_new_values_alias_list 
    old_or_new_values_alias	{ $$ = append_list($1, $2); } 
 ;
       
old_or_new_values_alias:
	/* is really a correlation name */
       OLD opt_row opt_as ident	{ $$ = append_string(append_int(L(), 0), $4); }
 |  sqlNEW opt_row opt_as ident	{ $$ = append_string(append_int(L(), 1), $4); }
 |     OLD TABLE opt_as ident 	{ $$ = append_string(append_int(L(), 0), $4); }
 |  sqlNEW TABLE opt_as ident 	{ $$ = append_string(append_int(L(), 1), $4); }
 ;

opt_as:
    /* empty */
 |  AS
 ;

opt_row:
    /* empty */
 |  ROW	
 ;

triggered_action:
    opt_for_each opt_when triggered_statement
	{ $$ = L();
	  append_int($$, $1);
	  append_symbol($$, $2);
	  append_list($$, $3);
	}

opt_for_each:
    /* default for each statement */ 	{ $$ = 1; }
 |  FOR EACH row_or_statement 		{ $$ = $3; }
 ;

row_or_statement:
    ROW 	{ $$ = 0; }
 |  STATEMENT 	{ $$ = 1; }
 ;

opt_when:
    /* empty */ 			{ $$ = NULL; }
 |  WHEN  '(' search_condition ')'  	{ $$ = $3; }
 ;

triggered_statement:
    trigger_procedure_statement		
				{ $$ = append_symbol(L(), $1); }
 |  BEGIN ATOMIC 
    trigger_procedure_statement_list 
    END 			{ $$ = $3; }
 ;

routine_designator:
	FUNCTION qname opt_typelist
	{ dlist *l = L();
	  append_list(l, $2 );	
	  append_list(l, $3 );
	  append_int(l, F_FUNC );
	  $$ = l; }
 |	FILTER FUNCTION qname opt_typelist
	{ dlist *l = L();
	  append_list(l, $3 );	
	  append_list(l, $4 );
	  append_int(l, F_FILT );
	  $$ = l; }
 |	AGGREGATE qname opt_typelist
	{ dlist *l = L();
	  append_list(l, $2 );	
	  append_list(l, $3 );
	  append_int(l, F_AGGR );
	  $$ = l; }
 |	PROCEDURE qname opt_typelist
	{ dlist *l = L();
	  append_list(l, $2 );	
	  append_list(l, $3 );
	  append_int(l, F_PROC );
	  $$ = l; }
 |	sqlLOADER qname opt_typelist
	{ dlist *l = L();
	  append_list(l, $2 );	
	  append_list(l, $3 );
	  append_int(l, F_LOADER );
	  $$ = l; }
 ;

drop_statement:
   drop TABLE if_exists qname drop_action
	{ dlist *l = L();
	  append_list(l, $4 );
	  append_int(l, $5 );
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_DROP_TABLE, l ); }
 | drop routine_designator drop_action
	{ dlist *l = $2;
	  append_int(l, 0 ); /* not all */
	  append_int(l, $3 );
	  $$ = _symbol_create_list( SQL_DROP_FUNC, l ); }
 | drop ALL FUNCTION qname drop_action
	{ dlist *l = L();
	  append_list(l, $4 );
	  append_list(l, NULL );
	  append_int(l, F_FUNC );
	  append_int(l, 1 );
	  append_int(l, $5 );
	  $$ = _symbol_create_list( SQL_DROP_FUNC, l ); }
 | drop ALL FILTER FUNCTION qname drop_action
	{ dlist *l = L();
	  append_list(l, $5 );
	  append_list(l, NULL );
	  append_int(l, F_FILT );
	  append_int(l, 1 );
	  append_int(l, $6 );
	  $$ = _symbol_create_list( SQL_DROP_FUNC, l ); }
 | drop ALL AGGREGATE qname drop_action
	{ dlist *l = L();
	  append_list(l, $4 );
	  append_list(l, NULL );
	  append_int(l, F_AGGR );
	  append_int(l, 1 );
	  append_int(l, $5 );
	  $$ = _symbol_create_list( SQL_DROP_FUNC, l ); }
 | drop ALL PROCEDURE qname drop_action
	{ dlist *l = L();
	  append_list(l, $4 );
	  append_list(l, NULL );
	  append_int(l, F_PROC );
	  append_int(l, 1 );
	  append_int(l, $5 );
	  $$ = _symbol_create_list( SQL_DROP_FUNC, l ); }
 | drop ALL sqlLOADER qname drop_action
	{ dlist *l = L();
	  append_list(l, $4 );
	  append_list(l, NULL );
	  append_int(l, F_LOADER );
	  append_int(l, 1 );
	  append_int(l, $5 );
	  $$ = _symbol_create_list( SQL_DROP_FUNC, l ); }
 |  drop VIEW if_exists qname drop_action
	{ dlist *l = L();
	  append_list(l, $4 );
	  append_int(l, $5 );
	  append_int(l, $3 );
	  $$ = _symbol_create_list( SQL_DROP_VIEW, l ); }
 |  drop TYPE qname drop_action	 
	{ dlist *l = L();
	  append_list(l, $3 );
	  append_int(l, $4 );
	  $$ = _symbol_create_list( SQL_DROP_TYPE, l ); }
 |  drop ROLE ident	  { $$ = _symbol_create( SQL_DROP_ROLE, $3 ); }
 |  drop USER ident	  { $$ = _symbol_create( SQL_DROP_USER, $3 ); }
 |  drop INDEX qname	  { $$ = _symbol_create_list( SQL_DROP_INDEX, $3 ); }
 |  drop TRIGGER qname	  { $$ = _symbol_create_list( SQL_DROP_TRIGGER, $3 ); }
 ;

opt_typelist:
    /*empty*/		{$$ = NULL;}
 |  '(' typelist ')'	{$$ = $2;}
 |  '(' ')'		{$$ = L(); }
 ;

typelist:
    data_type			{ dlist *l = L();
				  append_type(l, &$1 );
				  $$= l; }
 |  data_type ',' typelist	{ append_type($3, &$1);
				  $$ = $3; }
 ;

drop_action:
    /* empty */		{ $$ = 0; }
 |  RESTRICT		{ $$ = 0; }
 |  CASCADE		{ $$ = 1; }
 ;
	/* data manipulative stmts */

sql:
   transaction_statement
 | update_statement
 ;

update_statement: 
/* todo merge statement */
   delete_stmt
 | insert_stmt
 | update_stmt
 | copyfrom_stmt
 ;

transaction_statement:
   _transaction_stmt
	{
	  $$ = $1;
	  m->type = Q_TRANS;					}
 ;

_transaction_stmt:
    COMMIT opt_work opt_chain
		{ $$ = _symbol_create_int( TR_COMMIT, $3);  }
 |  SAVEPOINT ident
		{ $$ = _symbol_create( TR_SAVEPOINT, $2); }
 |  RELEASE SAVEPOINT ident
		{ $$ = _symbol_create( TR_RELEASE, $3); }
 |  ROLLBACK opt_work opt_chain opt_to_savepoint
		{ $$ = _symbol_create_list( TR_ROLLBACK,
		   append_string(
			append_int(L(), $3), $4 )); }
 |  START TRANSACTION transaction_mode_list
		{ $$ = _symbol_create_int( TR_START, $3); }
 |  SET LOCAL TRANSACTION transaction_mode_list
		{ $$ = _symbol_create_int( TR_MODE, $4); }
 |  SET TRANSACTION transaction_mode_list
		{ $$ = _symbol_create_int( TR_MODE, $3); }
 ;

transaction_mode_list:
	/* empty */		{ $$ = tr_none; }
 |	_transaction_mode_list
 ;

_transaction_mode_list:
	transaction_mode
		{ $$ = $1; }
 |	_transaction_mode_list ',' transaction_mode
		{ $$ = ($1 | $3); }
 ;


transaction_mode:
	READ ONLY			{ $$ = tr_readonly; }
 |	READ WRITE			{ $$ = tr_writable; }
 |	ISOLATION LEVEL iso_level	{ $$ = tr_serializable; }
 |	DIAGNOSTICS sqlSIZE intval	{ $$ = tr_none; /* not supported */ }
 ;

iso_level:
	READ UNCOMMITTED
 |	READ COMMITTED
 |	sqlREPEATABLE READ
 |	SERIALIZABLE
 ;

opt_work: /* pure syntax sugar */
    WORK		{ $$ = 0; }
 |  /* empty */		{ $$ = 0; }
 ;

opt_chain:
    AND CHAIN		{ $$ = 1; }
 |  AND NO CHAIN	{ $$ = 0; }
 |  /* empty */		{ $$ = 0; }
 ;

opt_to_savepoint:
    /* empty */		{ $$ = NULL; }
 |  TO SAVEPOINT ident  { $$ = $3; }
 ;

copyfrom_stmt:
    COPY opt_nr INTO qname opt_column_list FROM string_commalist opt_header_list opt_seps opt_null_string opt_locked opt_best_effort opt_constraint opt_fwf_widths
	{ dlist *l = L();
	  append_list(l, $4);
	  append_list(l, $5);
	  append_list(l, $7);
	  append_list(l, $8);
	  append_list(l, $9);
	  append_list(l, $2);
	  append_string(l, $10);
	  append_int(l, $11);
	  append_int(l, $12);
	  append_int(l, $13);
	  append_list(l, $14);
	  $$ = _symbol_create_list( SQL_COPYFROM, l ); }
  | COPY opt_nr INTO qname opt_column_list FROM STDIN  opt_header_list opt_seps opt_null_string opt_locked opt_best_effort opt_constraint 
	{ dlist *l = L();
	  append_list(l, $4);
	  append_list(l, $5);
	  append_list(l, NULL);
	  append_list(l, $8);
	  append_list(l, $9);
	  append_list(l, $2);
	  append_string(l, $10);
	  append_int(l, $11);
	  append_int(l, $12);
	  append_int(l, $13);
	  append_list(l, NULL);
	  $$ = _symbol_create_list( SQL_COPYFROM, l ); }
  | COPY sqlLOADER INTO qname FROM func_ref
	{ dlist *l = L();
	  append_list(l, $4);
	  append_symbol(l, $6);
	  $$ = _symbol_create_list( SQL_COPYLOADER, l ); }
   | COPY opt_nr BINARY INTO qname opt_column_list FROM string_commalist /* binary copy from */ opt_constraint
	{ dlist *l = L();
	  if ($2 != NULL) {
	  	yyerror(m, "COPY INTO: cannot pass number of records when using binary COPY INTO");
		YYABORT;
	  }
	  append_list(l, $5);
	  append_list(l, $6);
	  append_list(l, $8);
	  append_int(l, $9);
	  $$ = _symbol_create_list( SQL_BINCOPYFROM, l ); }
  | COPY query_expression_def INTO string opt_seps opt_null_string 
	{ dlist *l = L();
	  append_symbol(l, $2);
	  append_string(l, $4);
	  append_list(l, $5);
	  append_string(l, $6);
	  $$ = _symbol_create_list( SQL_COPYTO, l ); }
  | COPY query_expression_def INTO STDOUT opt_seps opt_null_string
	{ dlist *l = L();
	  append_symbol(l, $2);
	  append_string(l, NULL);
	  append_list(l, $5);
	  append_string(l, $6);
	  $$ = _symbol_create_list( SQL_COPYTO, l ); }
  ;
  


opt_fwf_widths:
       /* empty */		{ $$ = NULL; }
| FWF '(' fwf_widthlist ')' { $$ = $3; }
 
 ;
 
 fwf_widthlist:
    poslng		{ $$ = append_lng(L(), $1); }
 |  fwf_widthlist ',' poslng
			{ $$ = append_lng($1, $3); }
 ;

 
opt_header_list:
       /* empty */		{ $$ = NULL; }
 | '(' header_list ')'		{ $$ = $2; }
 ;

header_list:
   header 			{ $$ = append_list(L(), $1); }
 | header_list ',' header 	{ $$ = append_list($1, $3); }
 ;

header:
	ident		
			{ dlist *l = L();
			  append_string(l, $1 );
			  $$ = l; }
 |	ident STRING
			{ dlist *l = L();
			  append_string(l, $1 );
			  append_string(l, $2 );
			  $$ = l; }
 ;

opt_seps:
    /* empty */
				{ dlist *l = L();
				  append_string(l, sa_strdup(SA, "|"));
				  append_string(l, sa_strdup(SA, "\\n"));
				  $$ = l; }
 |  opt_using DELIMITERS string
				{ dlist *l = L();
				  append_string(l, $3);
				  append_string(l, sa_strdup(SA, "\\n"));
				  $$ = l; }
 |  opt_using DELIMITERS string ',' string
				{ dlist *l = L();
				  append_string(l, $3);
				  append_string(l, $5);
				  $$ = l; }
 |  opt_using DELIMITERS string ',' string ',' string
				{ dlist *l = L();
				  append_string(l, $3);
				  append_string(l, $5);
				  append_string(l, sql2str($7));
				  $$ = l; }
 ;

opt_using:
    /* empty */			{ $$ = NULL; }
 |  USING			{ $$ = NULL; }
 ;

opt_nr:
    /* empty */			{ $$ = NULL; }
 |  poslng RECORDS		{ $$ = append_lng(append_lng(L(), $1), 0); }
 |  OFFSET poslng 		{ $$ = append_lng(append_lng(L(), -1), $2); }
 |  poslng OFFSET poslng RECORDS	
				{ $$ = append_lng(append_lng(L(), $1), $3); }
 |  poslng RECORDS OFFSET poslng	
				{ $$ = append_lng(append_lng(L(), $1), $4); }
 ;

opt_null_string:
	/* empty */		{ $$ = NULL; }
 |  	sqlNULL opt_as string	{ $$ = $3; }
 ;

opt_locked:
	/* empty */	{ $$ = FALSE; }
 |  	LOCKED		{ $$ = TRUE; }
 ;

opt_best_effort:
	/* empty */	{ $$ = FALSE; }
 |  	BEST EFFORT	{ $$ = TRUE; }
 ;

opt_constraint:
	/* empty */	{ $$ = TRUE; }
 |  	NO CONSTRAINT	{ $$ = FALSE; }
 ;

string_commalist:
	string_commalist_contents          { $$ = $1; }
 |	'(' string_commalist_contents ')'  { $$ = $2; }
 ;

string_commalist_contents:
    string		{ $$ = append_string(L(), $1); }
 |  string_commalist_contents ',' string
			{ $$ = append_string($1, $3); }
 ;

delete_stmt:
    sqlDELETE FROM qname opt_where_clause

	{ dlist *l = L();
	  append_list(l, $3);
	  append_symbol(l, $4);
	  $$ = _symbol_create_list( SQL_DELETE, l ); }
 ;

update_stmt:
    UPDATE qname SET assignment_commalist opt_from_clause opt_where_clause

	{ dlist *l = L();
	  append_list(l, $2);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  append_symbol(l, $6);
	  $$ = _symbol_create_list( SQL_UPDATE, l ); }
 ;

/* todo merge statment 

Conditionally update rows of a table, or insert new rows into a table, or both.


<merge statement> ::=
		MERGE INTO <target table> [ [ AS ] <merge correlation name> ]
		USING <table reference> ON <search condition> <merge operation specification>

<merge correlation name> ::= <correlation name>

<merge operation specification> ::= <merge when clause>...

<merge when clause> ::= <merge when matched clause> | <merge when not matched clause>

<merge when matched clause> ::= WHEN MATCHED THEN <merge update specification>

<merge when not matched clause> ::= WHEN NOT MATCHED THEN <merge insert specification>

<merge update specification> ::= UPDATE SET <set clause list>

<merge insert specification> ::=
		INSERT [ <left paren> <insert column list> <right paren> ]
		[ <override clause> ] VALUES <merge insert value list>

<merge insert value list> ::=
		<left paren> <merge insert value element> [ { <comma> <merge insert value element> }... ] <right paren>

<merge insert value element> ::= <value expression> | <contextually typed value specification>
*/

insert_stmt:
    INSERT INTO qname values_or_query_spec

	{ dlist *l = L();
	  append_list(l, $3);
	  append_list(l, NULL);
	  append_symbol(l, $4);
	  $$ = _symbol_create_list( SQL_INSERT, l ); }

 |  INSERT INTO qname column_commalist_parens values_or_query_spec

	{ dlist *l = L();
	  append_list(l, $3);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_INSERT, l ); }
 ;

values_or_query_spec:
/* empty values list */
		{ $$ = _symbol_create_list( SQL_VALUES, L()); }
 |   DEFAULT VALUES
		{ $$ = _symbol_create_list( SQL_VALUES, L()); }
 |   VALUES row_commalist
		{ $$ = _symbol_create_list( SQL_VALUES, $2); }
 |  query_expression
 ;


row_commalist:
    '(' atom_commalist ')'	{ $$ = append_list(L(), $2); }
 |  row_commalist ',' '(' atom_commalist ')'
				{ $$ = append_list($1, $4); }
 ;

atom_commalist:
    insert_atom		{ $$ = append_symbol(L(), $1); }
 |  atom_commalist ',' insert_atom
			{ $$ = append_symbol($1, $3); }
 ;

value_commalist:
    value		{ $$ = append_symbol(L(), $1); }
 |  value_commalist ',' value
			{ $$ = append_symbol($1, $3); }
 ;

null:
   sqlNULL
	 { 
	  if (m->emode == m_normal && m->caching) {
		/* replace by argument */
		atom *a = atom_general(SA, sql_bind_localtype("void"), NULL);

		sql_add_arg( m, a);
		$$ = _symbol_create_list( SQL_COLUMN,
			append_int(L(), m->argc-1));
	   } else {
		$$ = _symbol_create(SQL_NULL, NULL );
	   }
	}
 ;


simple_atom:
    scalar_exp
 ;

insert_atom:
    simple_atom
 |  DEFAULT		{ $$ = _symbol_create(SQL_DEFAULT, NULL ); }
 ;

value:
    simple_atom
 |  select_no_parens
 ;

opt_distinct:
    /* empty */		{ $$ = FALSE; }
 |  ALL			{ $$ = FALSE; }
 |  DISTINCT		{ $$ = TRUE; }
 ;

assignment_commalist:
    assignment		{ $$ = append_symbol(L(), $1 ); }
 |  assignment_commalist ',' assignment
			{ $$ = append_symbol($1, $3 ); }
 ;

assign_default:
    DEFAULT		{ $$ = _symbol_create(SQL_DEFAULT, NULL ); }
 ;

assignment:
   column '=' assign_default
	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_string(l, $1);
	  $$ = _symbol_create_list( SQL_ASSIGN, l); }
 |  column '=' search_condition
	{ dlist *l = L();
	  append_symbol(l, $3 );
	  append_string(l, $1);
	  $$ = _symbol_create_list( SQL_ASSIGN, l); }
 |  column_commalist_parens '=' subquery
	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_list(l, $1);
	  $$ = _symbol_create_list( SQL_ASSIGN, l ); }
 ;

opt_where_clause:
    /* empty */			{ $$ = NULL; }
 |  WHERE search_condition	{ $$ = $2; }
 ;

	/* query expressions */

joined_table:
   '(' joined_table ')'
	{ $$ = $2; }
 |  table_ref CROSS JOIN table_ref
	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_symbol(l, $4);
	  $$ = _symbol_create_list( SQL_CROSS, l); }
 |  table_ref UNIONJOIN table_ref join_spec
	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, 0);
	  append_int(l, 4);
	  append_symbol(l, $3);
	  append_symbol(l, $4);
	  $$ = _symbol_create_list( SQL_UNIONJOIN, l); }
 |  table_ref join_type JOIN table_ref join_spec
	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, 0);
	  append_int(l, $2);
	  append_symbol(l, $4);
	  append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_JOIN, l); }
 |  table_ref NATURAL join_type JOIN table_ref
	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, 1);
	  append_int(l, $3);
	  append_symbol(l, $5);
	  append_symbol(l, NULL);
	  $$ = _symbol_create_list( SQL_JOIN, l); }
  ;

join_type:
    /* empty */			{ $$ = 0; }
  | INNER			{ $$ = 0; }
  | outer_join_type opt_outer	{ $$ = 1 + $1; }
  ;

opt_outer:
    /* empty */ 		{ $$ = 0; }
  | OUTER			{ $$ = 0; }
  ;

outer_join_type:
    LEFT		{ $$ = 0; }
  | RIGHT		{ $$ = 1; }
  | FULL		{ $$ = 2; }
  ;

join_spec:
    ON search_condition			{ $$ = $2; }
  | USING column_commalist_parens
		{ $$ = _symbol_create_list( SQL_USING, $2); }
  ;

/*
<query expression> ::= [ <with clause> ] <query expression body>

<with clause> ::= WITH [ RECURSIVE ] <with list>

<with list> ::= <with list element> [ { <comma> <with list element> }... ]

<with list element> ::=
                <query name> [ <left paren> <with column list> <right paren> ]
                AS <left paren> <query expression> <right paren> [ <search or cycle clause> ]

<with column list> ::= <column name list>

RECURSIVE and <search or cycle clause> are currently not supported
*/

sql: with_query
	;

with_query:
	WITH with_list with_query_expression
	{
		dlist *l = L();
	  	append_list(l, $2);
	  	append_symbol(l, $3);
	  	$$ = _symbol_create_list( SQL_WITH, l ); 
	}
  ;

with_list:
	with_list ',' with_list_element	 { $$ = append_symbol($1, $3); }
 |	with_list_element	 	 { $$ = append_symbol(L(), $1); }
 ;

with_list_element: 
    ident opt_column_list AS subquery_with_orderby 
	{  dlist *l = L();
	  append_list(l, append_string(L(), $1));
	  append_list(l, $2);
	  append_symbol(l, $4);
	  append_int(l, FALSE);	/* no with check */
	  append_int(l, FALSE);	/* inlined view  (ie not persistent) */
	  $$ = _symbol_create_list( SQL_CREATE_VIEW, l ); 
	}
 ;

with_query_expression:
	select_no_parens_orderby
  ;


sql:
    select_statement_single_row
|
    select_no_parens_orderby
 ;

simple_select:
    SELECT opt_distinct selection table_exp
	{ $$ = newSelectNode( SA, $2, $3, NULL,
		$4->h->data.sym,
		$4->h->next->data.sym,
		$4->h->next->next->data.sym,
		$4->h->next->next->next->data.sym,
		NULL, NULL, NULL, NULL, NULL);
	}
    ;

select_statement_single_row:
    SELECT opt_distinct selection INTO select_target_list table_exp
	{ $$ = newSelectNode( SA, $2, $3, $5,
		$6->h->data.sym,
		$6->h->next->data.sym,
		$6->h->next->next->data.sym,
		$6->h->next->next->next->data.sym,
		NULL, NULL, NULL, NULL, NULL);
	}
    ;

select_no_parens_orderby:
     select_no_parens opt_order_by_clause opt_limit opt_offset opt_sample
	 { 
	  $$ = $1;
	  if ($2 || $3 || $4 || $5) {
	  	if ($1 != NULL &&
		    ($1->token == SQL_SELECT ||
		     $1->token == SQL_UNION  ||
		     $1->token == SQL_EXCEPT ||
		     $1->token == SQL_INTERSECT)) {
			if ($1->token == SQL_SELECT) {
	 			SelectNode *s = (SelectNode*)$1;
	
	  			s -> orderby = $2;
	  			s -> limit = $3;
	  			s -> offset = $4;
	  			s -> sample = $5;
			} else { /* Add extra select * from .. in case of UNION, EXCEPT, INTERSECT */
				$$ = newSelectNode( 
					SA, 0, 
					append_symbol(L(), _symbol_create_list(SQL_TABLE, append_string(append_string(L(),NULL),NULL))), NULL,
					_symbol_create_list( SQL_FROM, append_symbol(L(), $1)), NULL, NULL, NULL, $2, _symbol_create_list(SQL_NAME, append_list(append_string(L(),"inner"),NULL)), $3, $4, $5);
			}
	  	} else {
			yyerror(m, "missing SELECT operator");
			YYABORT;
	  	}
	 } 
	}
    ;

select_target_list:
	target_specification 	{ $$ = append_string(L(), $1); }
 |  	select_target_list ',' target_specification
				{ $$ = append_string($1, $3); }
 ;

target_specification:
	ident
 ;

select_no_parens:
    select_no_parens UNION set_distinct opt_corresponding select_no_parens

	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, $3);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_UNION, l); }

 |  select_no_parens EXCEPT set_distinct opt_corresponding select_no_parens

	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, $3);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_EXCEPT, l); }

 |  select_no_parens INTERSECT set_distinct opt_corresponding select_no_parens

	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, $3);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_INTERSECT, l); }
 |  '(' select_no_parens ')' { $$ = $2; }
 |   simple_select
 ;

set_distinct:
    /* empty */		{ $$ = TRUE; }
 |  ALL			{ $$ = FALSE; }
 |  DISTINCT		{ $$ = TRUE; }
 ;


opt_corresponding:
	/* empty */	{ $$ = NULL; }
 |  CORRESPONDING
			{ $$ = L(); }
 |  CORRESPONDING BY '(' column_ref_commalist ')'
			{ $$ = $4; }
 ;

selection:
    column_exp_commalist
 ;

table_exp:
    opt_from_clause opt_where_clause opt_group_by_clause opt_having_clause

	{ $$ = L();
	  append_symbol($$, $1);
	  append_symbol($$, $2);
	  append_symbol($$, $3);
	  append_symbol($$, $4); }
 ;

opt_from_clause:
    /* empty */			 { $$ = NULL; }
 |  FROM table_ref_commalist 	 { $$ = _symbol_create_list( SQL_FROM, $2); }
 ;

table_ref_commalist:
    table_ref		{ $$ = append_symbol(L(), $1); }
 |  table_ref_commalist ',' table_ref
			{ $$ = append_symbol($1, $3); }
 ;

table_ref:
    qname opt_table_name 	{ dlist *l = L();
		  		  append_list(l, $1);
		  	  	  append_symbol(l, $2);
		  		  $$ = _symbol_create_list(SQL_NAME, l); }
 |  func_ref opt_table_name
	 		        { dlist *l = L();
		  		  append_symbol(l, $1);
		  	  	  append_symbol(l, $2);
		  	  	  append_int(l, 0);
		  		  $$ = _symbol_create_list(SQL_TABLE, l); }
 |  LATERAL func_ref opt_table_name
	 		        { dlist *l = L();
		  		  append_symbol(l, $2);
		  	  	  append_symbol(l, $3);
		  	  	  append_int(l, 1);
		  		  $$ = _symbol_create_list(SQL_TABLE, l); }
 |  subquery_with_orderby table_name		
				{
				  $$ = $1;
				  if ($$->token == SQL_SELECT) {
				  	SelectNode *sn = (SelectNode*)$1;
				  	sn->name = $2;
				  } else {
				  	append_symbol($1->data.lval, $2);
				  }
				}
 |  LATERAL subquery table_name		
				{
				  $$ = $2;
				  if ($$->token == SQL_SELECT) {
				  	SelectNode *sn = (SelectNode*)$2;
				  	sn->name = $3;
					sn->lateral = 1;
				  } else {
				  	append_symbol($2->data.lval, $3);
	  				append_int($2->data.lval, 1);
				  }
				}
 |  subquery_with_orderby
				{ $$ = NULL;
				  yyerror(m, "subquery table reference needs alias, use AS xxx");
				  YYABORT;
				}
 |  joined_table 		{ $$ = $1;
				  append_symbol($1->data.lval, NULL); }
/* Basket expression, TODO window */
 |  '[' 
	{ m->caching = 0; }
	select_no_parens ']' table_name 	
	{
		dlist *op = L();

 	  	append_symbol(op, $3);
		append_symbol(op, $5);
		$$ = _symbol_create_list(SQL_TABLE_OPERATOR, op); 
	}
 ;

table_name:
    AS ident '(' name_commalist ')'
				{ dlist *l = L();
		  		  append_string(l, $2);
		  	  	  append_list(l, $4);
		  		  $$ = _symbol_create_list(SQL_NAME, l); }
 |  AS ident
				{ dlist *l = L();
		  		  append_string(l, $2);
		  	  	  append_list(l, NULL);
		  		  $$ = _symbol_create_list(SQL_NAME, l); }
 |  ident '(' name_commalist ')'
				{ dlist *l = L();
		  		  append_string(l, $1);
		  	  	  append_list(l, $3);
		  		  $$ = _symbol_create_list(SQL_NAME, l); }
 |  ident
				{ dlist *l = L();
		  		  append_string(l, $1);
		  	  	  append_list(l, NULL);
		  		  $$ = _symbol_create_list(SQL_NAME, l); }
 ;

opt_table_name:
	      /* empty */ 	{ $$ = NULL; }
 | table_name			{ $$ = $1; }
 ;

opt_group_by_clause:
    /* empty */ 		  { $$ = NULL; }
 |  sqlGROUP BY column_ref_commalist { $$ = _symbol_create_list( SQL_GROUPBY, $3 );}
 ;

column_ref_commalist:
    column_ref		{ $$ = append_symbol(L(),
			       _symbol_create_list(SQL_COLUMN,$1)); }
 |  column_ref_commalist ',' column_ref
			{ $$ = append_symbol( $1,
			       _symbol_create_list(SQL_COLUMN,$3)); }
 ;

opt_having_clause:
    /* empty */ 		 { $$ = NULL; }
 |  HAVING search_condition	 { $$ = $2; }
 ;


search_condition:
    search_condition OR and_exp
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_symbol(l, $3);
		  $$ = _symbol_create_list(SQL_OR, l ); }
 |  and_exp	{ $$ = $1; }
 ;
   
and_exp:
    and_exp AND pred_exp
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_symbol(l, $3);
		  $$ = _symbol_create_list(SQL_AND, l ); }
 |  pred_exp	{ $$ = $1; }
 ;

opt_order_by_clause:
    /* empty */ 			  { $$ = NULL; }
 |  ORDER BY sort_specification_list
		{ $$ = _symbol_create_list( SQL_ORDERBY, $3); }
 ;

opt_limit:
    /* empty */ 	{ $$ = NULL; }
 |  LIMIT nonzerolng	{ 
		  	  sql_subtype *t = sql_bind_localtype("lng");
			  $$ = _newAtomNode( atom_int(SA, t, $2)); 
			}
 |  LIMIT param		{ $$ = $2; }
 ;

opt_offset:
	/* empty */	{ $$ = NULL; }
 |  OFFSET poslng	{ 
		  	  sql_subtype *t = sql_bind_localtype("lng");
			  $$ = _newAtomNode( atom_int(SA, t, $2)); 
			}
 |  OFFSET param	{ $$ = $2; }
 ;

opt_sample:
	/* empty */	{ $$ = NULL; }
 |  SAMPLE poslng	{
		  	  sql_subtype *t = sql_bind_localtype("lng");
			  $$ = _newAtomNode( atom_int(SA, t, $2));
			}
 |  SAMPLE INTNUM	{
		  	  sql_subtype *t = sql_bind_localtype("dbl");
			  $$ = _newAtomNode( atom_float(SA, t, strtod($2,NULL)));
			}
 |  SAMPLE param	{ $$ = $2; }
 ;

sort_specification_list:
    ordering_spec	 { $$ = append_symbol(L(), $1); }
 |  sort_specification_list ',' ordering_spec
			 { $$ = append_symbol( $1, $3 ); }
 ;

ordering_spec:
    scalar_exp opt_asc_desc
	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, $2);
	  $$ = _symbol_create_list(SQL_COLUMN, l ); }

 ;

opt_asc_desc:
    /* empty */ 	{ $$ = TRUE; }
 |  ASC			{ $$ = TRUE; }
 |  DESC		{ $$ = FALSE; }
 ;

predicate:
    comparison_predicate
 |  between_predicate
 |  like_predicate
 |  test_for_null
 |  in_predicate
 |  all_or_any_predicate
 |  existence_test
 |  filter_exp
 |  scalar_exp
 ;

pred_exp:
    NOT pred_exp 
		{ $$ = $2;

		  if ($$->token == SQL_EXISTS)
			$$->token = SQL_NOT_EXISTS;
		  else if ($$->token == SQL_NOT_EXISTS)
			$$->token = SQL_EXISTS;
		  else if ($$->token == SQL_NOT_BETWEEN)
			$$->token = SQL_BETWEEN;
		  else if ($$->token == SQL_BETWEEN)
			$$->token = SQL_NOT_BETWEEN;
		  else if ($$->token == SQL_NOT_LIKE)
			$$->token = SQL_LIKE;
		  else if ($$->token == SQL_LIKE)
			$$->token = SQL_NOT_LIKE;
		  else
			$$ = _symbol_create_symbol(SQL_NOT, $2); }
 |   predicate	{ $$ = $1; }
 ;

comparison_predicate:
    pred_exp COMPARISON pred_exp
		{ dlist *l = L();

		  append_symbol(l, $1);
		  append_string(l, $2);
		  append_symbol(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 |  pred_exp '=' pred_exp
		{ dlist *l = L();

		  append_symbol(l, $1);
		  append_string(l, sa_strdup(SA, "="));
		  append_symbol(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 ;

between_predicate:
    pred_exp NOT_BETWEEN opt_bounds pred_exp AND pred_exp
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_int(l, $3);
		  append_symbol(l, $4);
		  append_symbol(l, $6);
		  $$ = _symbol_create_list(SQL_NOT_BETWEEN, l ); }
 |  pred_exp BETWEEN opt_bounds pred_exp AND pred_exp
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_int(l, $3);
		  append_symbol(l, $4);
		  append_symbol(l, $6);
		  $$ = _symbol_create_list(SQL_BETWEEN, l ); }
 ;

opt_bounds:
   /* empty */ 	{ $$ = 0; }
 | ASYMMETRIC 	{ $$ = 0; }
 | SYMMETRIC 	{ $$ = 1; }
 ;

like_predicate:
    pred_exp NOT_LIKE like_exp
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_symbol(l, $3);
		  append_int(l, FALSE);  /* case sensitive */
		  append_int(l, TRUE);  /* anti */
		  $$ = _symbol_create_list( SQL_LIKE, l ); }
 |  pred_exp NOT_ILIKE like_exp
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_symbol(l, $3);
		  append_int(l, TRUE);  /* case insensitive */
		  append_int(l, TRUE);  /* anti */
		  $$ = _symbol_create_list( SQL_LIKE, l ); }
 |  pred_exp LIKE like_exp
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_symbol(l, $3);
		  append_int(l, FALSE);  /* case sensitive */
		  append_int(l, FALSE);  /* anti */
		  $$ = _symbol_create_list( SQL_LIKE, l ); }
 |  pred_exp ILIKE like_exp
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_symbol(l, $3);
		  append_int(l, TRUE);  /* case insensitive */
		  append_int(l, FALSE);  /* anti */
		  $$ = _symbol_create_list( SQL_LIKE, l ); }
 ;

like_exp:
    scalar_exp
	{ dlist *l = L();
	  append_symbol(l, $1);
	  $$ = _symbol_create_list(SQL_ESCAPE, l ); }
 |  scalar_exp ESCAPE string
 	{ const char *s = sql2str($3);
	  if (_strlen(s) != 1) {
		yyerror(m, SQLSTATE(22019) "ESCAPE must be one character");
		$$ = NULL;
		YYABORT;
	  } else {
		dlist *l = L();
		append_symbol(l, $1);
		append_string(l, s);
		$$ = _symbol_create_list(SQL_ESCAPE, l);
	  }
	}
 ;

test_for_null:
    scalar_exp IS NOT sqlNULL { $$ = _symbol_create_symbol( SQL_IS_NOT_NULL, $1 );}
 |  scalar_exp IS sqlNULL     { $$ = _symbol_create_symbol( SQL_IS_NULL, $1 ); }
 ;

in_predicate:
    pred_exp NOT_IN '(' value_commalist ')'
		{ dlist *l = L();

		  append_symbol(l, $1);
		  append_list(l, $4);
		  $$ = _symbol_create_list(SQL_NOT_IN, l ); }
 |  pred_exp sqlIN '(' value_commalist ')'
		{ dlist *l = L();

		  append_symbol(l, $1);
		  append_list(l, $4);
		  $$ = _symbol_create_list(SQL_IN, l ); }
 |  '(' pred_exp_list ')' NOT_IN '(' value_commalist ')'
		{ dlist *l = L();
		  append_list(l, $2);
		  append_list(l, $6);
		  $$ = _symbol_create_list(SQL_NOT_IN, l ); }
 |  '(' pred_exp_list ')' sqlIN '(' value_commalist ')'
		{ dlist *l = L();
		  append_list(l, $2);
		  append_list(l, $6);
		  $$ = _symbol_create_list(SQL_IN, l ); }
 ;

pred_exp_list:
    pred_exp
			{ $$ = append_symbol( L(), $1);}
 |  pred_exp_list ',' pred_exp
			{ $$ = append_symbol( $1, $3); }
 ;

all_or_any_predicate:
    pred_exp COMPARISON any_all_some subquery

		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, $2);
		  append_symbol(l, $4);
		  append_int(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 |  pred_exp '=' any_all_some pred_exp
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, sa_strdup(SA, "="));
		  append_symbol(l, $4);
		  append_int(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 ;

any_all_some:
    ANY		{ $$ = 0; }
 |  SOME	{ $$ = 0; }
 |  ALL		{ $$ = 1; }
 ;

existence_test:
    EXISTS subquery 	{ $$ = _symbol_create_symbol( SQL_EXISTS, $2 ); }
 ;

filter_arg_list:
       pred_exp				{ $$ = append_symbol(L(), $1); }
 |     filter_arg_list ',' pred_exp	{ $$ = append_symbol($1, $3);  }
 ;

filter_args:
	'[' filter_arg_list ']' 	{ $$ = $2; }
 ;

filter_exp:
 filter_args qname filter_args
		{ dlist *l = L();
		  append_list(l, $1);
		  append_list(l, $2);
		  append_list(l, $3);
		  $$ = _symbol_create_list(SQL_FILTER, l ); }
 ;


subquery_with_orderby:
    '(' select_no_parens_orderby ')'	{ $$ = $2; }
 |  '(' VALUES row_commalist ')'	
				{ $$ = _symbol_create_list( SQL_VALUES, $3); }
 |  '(' with_query ')'	
				{ $$ = $2; }
 ;

subquery:
    '(' select_no_parens ')'	{ $$ = $2; }
 |  '(' VALUES row_commalist ')'	
				{ $$ = _symbol_create_list( SQL_VALUES, $3); }
 |  '(' with_query ')'	
				{ $$ = $2; }
 ;

	/* simple_scalar expressions */
simple_scalar_exp:
    value_exp
 |  scalar_exp '+' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_add")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '-' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_sub")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '*' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_mul")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '/' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_div")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '%' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "mod")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '^' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "bit_xor")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '&' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "bit_and")));
	  		  append_symbol(l, $1);
			  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp GEOM_OVERLAP scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_overlap")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp GEOM_OVERLAP_OR_LEFT scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_overlap_or_left")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp  GEOM_OVERLAP_OR_RIGHT scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_overlap_or_right")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp GEOM_OVERLAP_OR_BELOW scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_overlap_or_below")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp GEOM_BELOW scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "mbr_below")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp GEOM_OVERLAP_OR_ABOVE scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_overlap_or_above")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp GEOM_ABOVE scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_above")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp GEOM_DIST scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_distance")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp AT scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_contained")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '|' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "bit_or")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '~' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_contains")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp GEOM_MBR_EQUAL scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_equal")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  '~' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "bit_not")));
	  		  append_symbol(l, $2);
	  		  $$ = _symbol_create_list( SQL_UNOP, l ); }
 |  scalar_exp LEFT_SHIFT scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "left_shift")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp RIGHT_SHIFT scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "right_shift")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp LEFT_SHIFT_ASSIGN scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "left_shift_assign")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp RIGHT_SHIFT_ASSIGN scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "right_shift_assign")));
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  '+' scalar_exp %prec UMINUS 
			{ $$ = $2; }
 |  '-' scalar_exp %prec UMINUS 
			{ 
 			  $$ = NULL;
			  assert($2->token != SQL_COLUMN || $2->data.lval->h->type != type_lng);
			  if ($2->token == SQL_COLUMN && $2->data.lval->h->type == type_int) {
				atom *a = sql_bind_arg(m, $2->data.lval->h->data.i_val);
				if (!atom_neg(a)) {
					$$ = $2;
				} else {
					yyerror(m, SQLSTATE(22003) "value too large or not a number");
					$$ = NULL;
					YYABORT;
				}
			  } 
			  if (!$$) {
				dlist *l = L();
			  	append_list(l, 
			  		append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_neg")));
	  		  	append_symbol(l, $2);
	  		  	$$ = _symbol_create_list( SQL_UNOP, l ); 
			  }
			}
 |  '(' search_condition ')' 	{ $$ = $2; }
 ;

scalar_exp:
    simple_scalar_exp 	{ $$ = $1; }
 |  subquery	%prec UMINUS
 ;

value_exp:
    atom
 |  user		{ $$ = _symbol_create_list( SQL_COLUMN, 
			  append_string(L(), sa_strdup(SA, "current_user"))); }
 |  CURRENT_ROLE	{ $$ = _symbol_create_list( SQL_COLUMN, 
			  append_string(L(), sa_strdup(SA, "current_role"))); }
 |  window_function
 |  column_ref 		{ $$ = _symbol_create_list( SQL_COLUMN, $1); }
 |  var_ref		
 |  aggr_ref
 |  func_ref
 |  NEXT VALUE FOR qname	{ $$ = _symbol_create_list( SQL_NEXT, $4); }
 |  datetime_funcs
 |  string_funcs
 |  case_exp
 |  cast_exp
 |  XML_value_function
 |  param
 |  null
 ;

param:  
   '?'			
	{ 
	  int nr = (m->params)?list_length(m->params):0;

	  sql_add_param(m, NULL, NULL);
	  $$ = _symbol_create_int( SQL_PARAMETER, nr ); 
	}

/*
<window function> ::= <window function type> OVER <window name or specification>

<window function type> ::=
		<rank function type> <left paren> <right paren>
	|	ROW_NUMBER <left paren> <right paren>
	|	<aggregate function>

<rank function type> ::= RANK | DENSE_RANK | PERCENT_RANK | CUME_DIST

<window name or specification> ::= <window name> | <in-line window specification>

<in-line window specification> ::= <window specification>


<window specification> ::= <left paren> <window specification details> <right paren>

<window specification details> ::=
                [ <existing window name> ] [ <window partition clause> ] [ <window order clause> ] [ <window frame clause> ]

<existing window name> ::= <window name>

<window partition clause> ::= PARTITION BY <window partition column reference list>

<window partition column reference list> ::= <window partition column reference> [ { <comma> <window partition column reference> }... ]

<window partition column reference> ::= <column reference> [ <collate clause> ]

<window order clause> ::= ORDER BY <sort specification list>

<window frame clause> ::= <window frame units> <window frame extent> [ <window frame exclusion> ]

<window frame units> ::= ROWS | RANGE

<window frame extent> ::= <window frame start> | <window frame between>

<window frame start> ::= UNBOUNDED PRECEDING | <window frame preceding> | CURRENT ROW

<window frame preceding> ::= <unsigned value specification> PRECEDING

<window frame between> ::= BETWEEN <window frame bound 1> AND <window frame bound 2>

<window frame bound 1> ::= <window frame bound>

<window frame bound 2> ::= <window frame bound>

<window frame bound> ::=
                <window frame start>
        |       UNBOUNDED FOLLOWING
        |       <window frame following>

<window frame following> ::= <unsigned value specification> FOLLOWING

<window frame exclusion> ::=
                EXCLUDE CURRENT ROW
        |       EXCLUDE GROUP
        |       EXCLUDE TIES
        |       EXCLUDE NO OTHERS

*/

window_function: 
	window_function_type OVER '(' window_specification ')'
	{ $$ = _symbol_create_list( SQL_RANK, 
		append_list(append_symbol(L(), $1), $4)); }
  ;

window_function_type:
	qrank '(' ')' 	{ $$ = _symbol_create_list( SQL_RANK, $1 ); }
  |	aggr_ref
  ;

window_specification:
	window_partition_clause window_order_clause window_frame_clause
	{ $$ = append_symbol(append_symbol(append_symbol(L(), $1), $2), $3); }
  ;

window_partition_clause:
	/* empty */ 	{ $$ = NULL; }
  |	PARTITION BY column_ref_commalist
	{ $$ = _symbol_create_list( SQL_GROUPBY, $3 ); }
  ;

window_order_clause:
	/* empty */ 	{ $$ = NULL; }
  |	ORDER BY sort_specification_list
	{ $$ = _symbol_create_list( SQL_ORDERBY, $3 ); }
  ;

window_frame_clause:
	/* empty */ 	{ $$ = NULL; }
  |	window_frame_units window_frame_extent window_frame_exclusion
	{ $$ = _symbol_create_list( SQL_FRAME, append_int(append_int($2, $1), $3)); }
  ;

window_frame_units:
	ROWS		{ $$ = FRAME_ROWS; }
  |	RANGE		{ $$ = FRAME_RANGE; }
  ;

window_frame_extent:
	window_frame_start	{ $$ = append_symbol(append_symbol(L(), $1), _symbol_create_int(SQL_FRAME, -1)); }
  |	window_frame_between	{ $$ = $1; }
  ;

window_frame_start:
	UNBOUNDED PRECEDING	{ $$ = _symbol_create_int(SQL_FRAME, -1); }
  |	window_frame_preceding  { $$ = $1; }
  |	CURRENT ROW		{ $$ = _symbol_create_int(SQL_FRAME, 0); }
  ;

window_frame_preceding:
	value_exp PRECEDING	{ $$ = $1; }
  ;
	
window_frame_between:
	BETWEEN window_frame_start AND window_frame_end
				{ $$ = append_symbol(append_symbol(L(), $2), $4); }
  ;

window_frame_end:
	UNBOUNDED FOLLOWING	{ $$ = _symbol_create_int(SQL_FRAME, -1); }
  | 	window_frame_following	{ $$ = $1; }
  |	CURRENT ROW		{ $$ = _symbol_create_int(SQL_FRAME, 0); }
  ;

window_frame_following:
	value_exp FOLLOWING	{ $$ = $1; }
  ;

window_frame_exclusion:
 	/* empty */		{ $$ = EXCLUDE_NONE; }
 |      EXCLUDE CURRENT ROW	{ $$ = EXCLUDE_CURRENT_ROW; }
 |      EXCLUDE sqlGROUP	{ $$ = EXCLUDE_GROUP; }
 |      EXCLUDE TIES		{ $$ = EXCLUDE_TIES; }
 |      EXCLUDE NO OTHERS	{ $$ = EXCLUDE_NO_OTHERS; }
 ;
	
var_ref:
	AT ident 	{ $$ = _symbol_create( SQL_NAME, $2 ); }
 ;

func_ref:
    qfunc '(' ')'
	{ dlist *l = L();
  	  append_list(l, $1);
	  $$ = _symbol_create_list( SQL_OP, l ); }
|   qfunc '(' scalar_exp_list ')'
	{ dlist *l = L();
  	  append_list(l, $1);
	  if (dlist_length($3) == 1) {
  	  	append_symbol(l, $3->h->data.sym);
	  	$$ = _symbol_create_list( SQL_UNOP, l ); 
	  } else if (dlist_length($3) == 2) {
  	  	append_symbol(l, $3->h->data.sym);
  	  	append_symbol(l, $3->h->next->data.sym);
	  	$$ = _symbol_create_list( SQL_BINOP, l ); 
	  } else {
  	  	append_list(l, $3);
	  	$$ = _symbol_create_list( SQL_NOP, l ); 
	  }
	}
/*
|   '(' '(' scalar_exp_list ')' qfunc '(' scalar_exp_list ')' ')'
	{ dlist *l = L();
  	  append_list(l, $5);
  	  append_list(l, $3);
  	  append_list(l, $7);
  	  append_int(l, 0);	
	  $$ = _symbol_create_list( SQL_JOIN, l ); 
	}
*/
 ;

qfunc:
	func_ident		{ $$ = append_string(L(), $1); }
 |      ident '.' func_ident	{ $$ = append_string(
					append_string(L(), $1), $3);}
 ;

func_ident:
	ident 	{ $$ = $1; }
 |	LEFT	{ $$ = sa_strdup(SA, "left"); }
 |	RIGHT	{ $$ = sa_strdup(SA, "right"); }
 |	INSERT	{ $$ = sa_strdup(SA, "insert"); }
 ;

datetime_funcs:
    EXTRACT '(' extract_datetime_field FROM scalar_exp ')'
			{ dlist *l = L();
			  const char *ident = datetime_field((itype)$3);
			  append_list(l,
  		  	  	append_string(L(), sa_strdup(SA, ident)));
  		  	  append_symbol(l, $5);
		  	  $$ = _symbol_create_list( SQL_UNOP, l ); }
 |  CURRENT_DATE opt_brackets
 			{ dlist *l = L();
			  append_list(l,
			  	append_string(L(), sa_strdup(SA, "current_date")));
	  		  $$ = _symbol_create_list( SQL_OP, l ); }
 |  CURRENT_TIME opt_brackets
 			{ dlist *l = L();
			  append_list(l,
			  	append_string(L(), sa_strdup(SA, "current_time")));
	  		  $$ = _symbol_create_list( SQL_OP, l ); }
 |  CURRENT_TIMESTAMP opt_brackets
 			{ dlist *l = L();
			  append_list(l,
			  	append_string(L(), sa_strdup(SA, "current_timestamp")));
	  		  $$ = _symbol_create_list( SQL_OP, l ); }
 |  LOCALTIME opt_brackets
 			{ dlist *l = L();
			  append_list(l,
			  	append_string(L(), sa_strdup(SA, "localtime")));
	  		  $$ = _symbol_create_list( SQL_OP, l ); }
 |  LOCALTIMESTAMP opt_brackets
 			{ dlist *l = L();
			  append_list(l,
			  	append_string(L(), sa_strdup(SA, "localtimestamp")));
	  		  $$ = _symbol_create_list( SQL_OP, l ); }
 ;

opt_brackets:
   /* empty */	{ $$ = 0; }
 | '(' ')'	{ $$ = 1; }
 ;

string_funcs:
    SUBSTRING '(' scalar_exp FROM scalar_exp FOR scalar_exp ')'
			{ dlist *l = L();
			  dlist *ops = L();
  		  	  append_list(l,
				append_string(L(), sa_strdup(SA, "substring")));
  		  	  append_symbol(ops, $3);
  		  	  append_symbol(ops, $5);
  		  	  append_symbol(ops, $7);
			  append_list(l, ops);
		  	  $$ = _symbol_create_list( SQL_NOP, l ); }
  | SUBSTRING '(' scalar_exp ',' scalar_exp ',' scalar_exp ')'
			{ dlist *l = L();
			  dlist *ops = L();
  		  	  append_list(l,
  		  	  	append_string(L(), sa_strdup(SA, "substring")));
  		  	  append_symbol(ops, $3);
  		  	  append_symbol(ops, $5);
  		  	  append_symbol(ops, $7);
			  append_list(l, ops);
		  	  $$ = _symbol_create_list( SQL_NOP, l ); }
  | SUBSTRING '(' scalar_exp FROM scalar_exp ')'
			{ dlist *l = L();
  		  	  append_list(l,
  		  	  	append_string(L(), sa_strdup(SA, "substring")));
  		  	  append_symbol(l, $3);
  		  	  append_symbol(l, $5);
		  	  $$ = _symbol_create_list( SQL_BINOP, l ); }
  | SUBSTRING '(' scalar_exp ',' scalar_exp ')'
			{ dlist *l = L();
  		  	  append_list(l,
  		  	  	append_string(L(), sa_strdup(SA, "substring")));
  		  	  append_symbol(l, $3);
  		  	  append_symbol(l, $5);
		  	  $$ = _symbol_create_list( SQL_BINOP, l ); }
  | POSITION '(' scalar_exp sqlIN scalar_exp ')'
			{ dlist *l = L();
  		  	  append_list(l,
  		  	  	append_string(L(), sa_strdup(SA, "locate")));
  		  	  append_symbol(l, $3);
  		  	  append_symbol(l, $5);
		  	  $$ = _symbol_create_list( SQL_BINOP, l ); }
  | scalar_exp CONCATSTRING scalar_exp
			{ dlist *l = L();
  		  	  append_list(l,
  		  	  	append_string(L(), sa_strdup(SA, "concat")));
  		  	  append_symbol(l, $1);
  		  	  append_symbol(l, $3);
		  	  $$ = _symbol_create_list( SQL_BINOP, l ); }
  | SPLIT_PART '(' scalar_exp ',' scalar_exp ',' scalar_exp ')'
			{ dlist *l = L();
			  dlist *ops = L();
  		  	  append_list(l,
				append_string(L(), sa_strdup(SA, "splitpart")));
  		  	  append_symbol(ops, $3);
  		  	  append_symbol(ops, $5);
  		  	  append_symbol(ops, $7);
			  append_list(l, ops);
		  	  $$ = _symbol_create_list( SQL_NOP, l ); }
 ;

column_exp_commalist:
    column_exp 		{ $$ = append_symbol(L(), $1 ); }
 |  column_exp_commalist ',' column_exp
			{ $$ = append_symbol( $1, $3 ); }
 ;

column_exp:
    '*'
		{ dlist *l = L();
  		  append_string(l, NULL);
  		  append_string(l, NULL);
  		  $$ = _symbol_create_list( SQL_TABLE, l ); }
 |  ident '.' '*'
		{ dlist *l = L();
  		  append_string(l, $1);
  		  append_string(l, NULL);
  		  $$ = _symbol_create_list( SQL_TABLE, l ); }
 |  func_ref '.' '*'
		{ dlist *l = L();
  		  append_symbol(l, $1);
  		  append_string(l, NULL);
  		  $$ = _symbol_create_list( SQL_TABLE, l ); }
 |  search_condition opt_alias_name
		{ dlist *l = L();
  		  append_symbol(l, $1);
  		  append_string(l, $2);
  		  $$ = _symbol_create_list( SQL_COLUMN, l ); }
 ;

opt_alias_name:
    /* empty */	{ $$ = NULL; }
 |  AS ident	{ $$ = $2; }
 ;

atom:
    literal
	{ 
	  if (m->emode == m_normal && m->caching) { 
	  	/* replace by argument */
	  	AtomNode *an = (AtomNode*)$1;
	
	  	sql_add_arg( m, an->a);
		an->a = NULL;
	  	/* we miss use SQL_COLUMN also for param's, maybe
	     		change SQL_COLUMN to SQL_IDENT */
 	  	$$ = _symbol_create_list( SQL_COLUMN,
			append_int(L(), m->argc-1));
	   } else {
	  	AtomNode *an = (AtomNode*)$1;
		atom *a = an->a; 
		an->a = atom_dup(SA, a); 
		$$ = $1;
	   }
	}
 ;

qrank:
	RANK		{ $$ = append_string(L(), $1); }
 |      ident '.' RANK	{ $$ = append_string(
			  append_string(L(), $1), $3);}
 ;

qaggr:
	AGGR		{ $$ = append_string(L(), $1); }
 |      ident '.' AGGR	{ $$ = append_string(
			  append_string(L(), $1), $3);}
 ;

qaggr2:
	AGGR2		{ $$ = append_string(L(), $1); }
 |      ident '.' AGGR2	{ $$ = append_string(
			  append_string(L(), $1), $3);}
 ;

/* change to set function */
aggr_ref:
    qaggr '(' '*' ')'
		{ dlist *l = L();
  		  append_list(l, $1);
  		  append_int(l, FALSE);
  		  append_symbol(l, NULL);
		  $$ = _symbol_create_list( SQL_AGGR, l ); }
 |  qaggr '(' ident '.' '*' ')'
		{ dlist *l = L();
  		  append_list(l, $1);
  		  append_int(l, FALSE);
  		  append_symbol(l, NULL);
		  $$ = _symbol_create_list( SQL_AGGR, l ); }
 |  qaggr '(' DISTINCT case_scalar_exp ')'
		{ dlist *l = L();
  		  append_list(l, $1);
  		  append_int(l, TRUE);
  		  append_symbol(l, $4);
		  $$ = _symbol_create_list( SQL_AGGR, l ); }
 |  qaggr '(' ALL case_scalar_exp ')'
		{ dlist *l = L();
  		  append_list(l, $1);
  		  append_int(l, FALSE);
  		  append_symbol(l, $4);
		  $$ = _symbol_create_list( SQL_AGGR, l ); }
 |  qaggr '(' case_scalar_exp ')'
		{ dlist *l = L();
  		  append_list(l, $1);
  		  append_int(l, FALSE);
  		  append_symbol(l, $3);
		  $$ = _symbol_create_list( SQL_AGGR, l ); }
 |  qaggr2 '(' case_scalar_exp ',' case_scalar_exp ')'
		{ dlist *l = L();
  		  append_list(l, $1);
  		  append_int(l, FALSE);
  		  append_symbol(l, $3);
  		  append_symbol(l, $5);
		  $$ = _symbol_create_list( SQL_AGGR, l ); }
 |  XML_aggregate
 ;

opt_sign:
   '+'		{ $$ = 1; }
 | '-' 		{ $$ = -1; }
 | /* empty */	{ $$ = 1; }
 ;

tz:
	WITH TIME ZONE	{ $$ = 1; }
 | /* empty */		{ $$ = 0; }
 ;

/* note: the maximum precision for interval, time and timestamp should be equal
 *       and at minimum 6.  The SQL standard prescribes that at least two
 *       fractional precisions are supported: 0 and 6, where 0 is the
 *       default for time, and 6 the default precision for timestamp and interval.
 *       It might be nice to check for a certain maximum of precision in
 *       the future here.
 */
time_precision:
	'(' intval ')' 	{ $$ = $2+1; }
/* a time defaults to a fractional precision of 0 */
 | /* empty */		{ $$ = 0+1; }
 ;

timestamp_precision:
	'(' intval ')' 	{ $$ = $2+1; }
/* a timestamp defaults to a fractional precision of 6 */
 | /* empty */		{ $$ = 6+1; }
 ;

datetime_type:
    sqlDATE		{ sql_find_subtype(&$$, "date", 0, 0); }
 |  TIME time_precision tz 	
			{ if ($3)
				sql_find_subtype(&$$, "timetz", $2, 0); 
			  else
				sql_find_subtype(&$$, "time", $2, 0); 
			}
 |  TIMESTAMP timestamp_precision tz 
			{ if ($3)
				sql_find_subtype(&$$, "timestamptz", $2, 0); 
			  else
				sql_find_subtype(&$$, "timestamp", $2, 0); 
			}
 ;

non_second_datetime_field:
    YEAR		{ $$ = iyear; }
 |  MONTH		{ $$ = imonth; }
 |  DAY			{ $$ = iday; }
 |  HOUR		{ $$ = ihour; }
 |  MINUTE		{ $$ = imin; }
 ;

datetime_field:
    non_second_datetime_field
 |  SECOND		{ $$ = isec; }
 ;

extract_datetime_field:
    datetime_field
 |  QUARTER		{ $$ = iquarter; }
 |  WEEK		{ $$ = iweek; }
 ;

start_field:
    non_second_datetime_field time_precision
		{ $$ = append_int(
			 	append_int( L(), $1), $2-1);  }
 ;

end_field:
    non_second_datetime_field
		{ $$ = append_int(
			 	append_int( L(), $1), 0);  }
 |  SECOND timestamp_precision
		{ $$ = append_int(
			 	append_int( L(), isec), $2-1);  }
 ;

single_datetime_field:
    non_second_datetime_field time_precision
		{ $$ = append_int(
			 	append_int( L(), $1), $2-1);  }
 |  SECOND timestamp_precision
		{ $$ = append_int(
			 	append_int( L(), isec), $2-1);  }
 ;

interval_qualifier:
    start_field TO end_field
	{ $$ =  append_list(
			append_list( L(), $1), $3 ); }
 |  single_datetime_field
	{ $$ =  append_list( L(), $1); }
 ;

interval_type:
    INTERVAL interval_qualifier	{
		int sk, ek, sp, ep;
	  	int tpe;

		$$.type = NULL;
	  	if ( (tpe = parse_interval_qualifier( m, $2, &sk, &ek, &sp, &ep )) < 0){
			yyerror(m, SQLSTATE(22006) "incorrect interval");
			YYABORT;
	  	} else {
			int d = inttype2digits(sk, ek);
			if (tpe == 0){
				sql_find_subtype(&$$, "month_interval", d, 0);
			} else {
				sql_find_subtype(&$$, "sec_interval", d, 0);
			}
	  	}
	}
 ;

user:
    USER 
 |  SESSION_USER
 |  CURRENT_USER 
 ;

literal:
    string 	{ const char *s = sql2str($1);
		  int len = UTF8_strlen(s);
		  sql_subtype t;
		  sql_find_subtype(&t, "char", len, 0 );
		  $$ = _newAtomNode( _atom_string(&t, s)); }

 |  HEXADECIMAL { int len = _strlen($1), i = 2, err = 0;
		  char * hexa = $1;
	 	  sql_subtype t;
#ifdef HAVE_HGE
		  hge res = 0;
#else
		  lng res = 0;
#endif
		  /* skip leading '0' */
		  while (i < len && hexa[i] == '0')
		  	i++;

		  /* we only support positive values that fit in a signed 128-bit type,
		   * i.e., max. 63/127 bit => < 2^63/2^127 => < 0x800...
		   * (leading sign (-0x...) is handled separately elsewhere)
		   */
		  if (len - i < MAX_HEX_DIGITS || (len - i == MAX_HEX_DIGITS && hexa[i] < '8'))
		  	while (err == 0 && i < len)
		  	{
				res <<= 4;
				if ('0'<= hexa[i] && hexa[i] <= '9')
					res = res + (hexa[i] - '0');
				else if ('A' <= hexa[i] && hexa[i] <= 'F')
					res = res + (hexa[i] - 'A' + 10);
				else if ('a' <= hexa[i] && hexa[i] <= 'f')
					res = res + (hexa[i] - 'a' + 10);
				else
					err = 1;
		  		i++;
			}
		  else
			err = 1;

		  if (err == 0) {
		  	assert(res >= 0);

		  	/* use smallest type that can accommodate the given value */
			if (res <= GDK_bte_max)
				sql_find_subtype(&t, "tinyint", 8, 0 );
			else if (res <= GDK_sht_max)
				sql_find_subtype(&t, "smallint", 16, 0 );
		  	else if (res <= GDK_int_max)
				sql_find_subtype(&t, "int", 32, 0 );
			else if (res <= GDK_lng_max)
				sql_find_subtype(&t, "bigint", 64, 0 );
#ifdef HAVE_HGE
			else if (res <= GDK_hge_max && have_hge)
				sql_find_subtype(&t, "hugeint", 128, 0 );
#endif
			else
				err = 1;
		  }

		  if (err != 0) {
			char *msg = sql_message(SQLSTATE(22003) "Invalid hexadecimal number or hexadecimal too large (%s)", $1);

			yyerror(m, msg);
			_DELETE(msg);
			$$ = NULL;
			YYABORT;
		  } else {
			$$ = _newAtomNode( atom_int(SA, &t, res));
		  }
		}
 |  OIDNUM
		{ int err = 0;
		  size_t len = sizeof(lng);
		  lng value, *p = &value;
		  sql_subtype t;

		  if (lngFromStr($1, &len, &p) < 0 || is_lng_nil(value))
		  	err = 2;

		  if (!err) {
		    if ((value >= GDK_lng_min && value <= GDK_lng_max))
#if SIZEOF_OID == SIZEOF_INT
		  	  sql_find_subtype(&t, "oid", 31, 0 );
#else
		  	  sql_find_subtype(&t, "oid", 63, 0 );
#endif
		    else
			  err = 1;
		  }

		  if (err) {
			char *msg = sql_message(SQLSTATE(22003) "OID value too large or not a number (%s)", $1);

			yyerror(m, msg);
			_DELETE(msg);
			$$ = NULL;
			YYABORT;
		  } else {
		  	$$ = _newAtomNode( atom_int(SA, &t, value));
		  }
		}
 |  sqlINT
		{ int digits = _strlen($1), err = 0;
#ifdef HAVE_HGE
		  hge value, *p = &value;
		  size_t len = sizeof(hge);
		  const hge one = 1;
#else
		  lng value, *p = &value;
		  size_t len = sizeof(lng);
		  const lng one = 1;
#endif
		  sql_subtype t;

#ifdef HAVE_HGE
		  if (hgeFromStr($1, &len, &p) < 0 || is_hge_nil(value))
		  	err = 2;
#else
		  if (lngFromStr($1, &len, &p) < 0 || is_lng_nil(value))
		  	err = 2;
#endif

		  /* find the most suitable data type for the given number */
		  if (!err) {
		    int bits = digits2bits(digits), obits = bits;

		    while (bits > 0 &&
			   (bits == sizeof(value) * 8 ||
			    (one << (bits - 1)) > value))
			  bits--;

 		    if (bits != obits &&
		       (bits == 8 || bits == 16 || bits == 32 || bits == 64))
				bits++;
		
		    if (value >= GDK_bte_min && value <= GDK_bte_max)
		  	  sql_find_subtype(&t, "tinyint", bits, 0 );
		    else if (value >= GDK_sht_min && value <= GDK_sht_max)
		  	  sql_find_subtype(&t, "smallint", bits, 0 );
		    else if (value >= GDK_int_min && value <= GDK_int_max)
		  	  sql_find_subtype(&t, "int", bits, 0 );
		    else if (value >= GDK_lng_min && value <= GDK_lng_max)
		  	  sql_find_subtype(&t, "bigint", bits, 0 );
#ifdef HAVE_HGE
		    else if (value >= GDK_hge_min && value <= GDK_hge_max && have_hge)
		  	  sql_find_subtype(&t, "hugeint", bits, 0 );
#endif
		    else
			  err = 1;
		  }

		  if (err) {
			char *msg = sql_message(SQLSTATE(22003) "integer value too large or not a number (%s)", $1);

			yyerror(m, msg);
			_DELETE(msg);
			$$ = NULL;
			YYABORT;
		  } else {
		  	$$ = _newAtomNode( atom_int(SA, &t, value));
		  }
		}
 |  INTNUM
		{ char *s = strip_extra_zeros(sa_strdup(SA, $1));
		  char *dot = strchr(s, '.');
		  int digits = _strlen(s) - 1;
		  int scale = digits - (int) (dot-s);
		  sql_subtype t;

		  if (digits <= 0)
			digits = 1;
		  if (digits <= MAX_DEC_DIGITS) {
		  	double val = strtod($1,NULL);
#ifdef HAVE_HGE
		  	hge value = decimal_from_str(s, NULL);
#else
		  	lng value = decimal_from_str(s, NULL);
#endif

		  	if (*s == '+' || *s == '-')
				digits --;
		  	sql_find_subtype(&t, "decimal", digits, scale );
		  	$$ = _newAtomNode( atom_dec(SA, &t, value, val));
		   } else {
			char *p = $1;
			double val;

			errno = 0;
			val = strtod($1,&p);
			if (p == $1 || is_dbl_nil(val) || (errno == ERANGE && (val < -1 || val > 1))) {
				char *msg = sql_message(SQLSTATE(22003) "Double value too large or not a number (%s)", $1);

				yyerror(m, msg);
				_DELETE(msg);
				$$ = NULL;
				YYABORT;
			}
		  	sql_find_subtype(&t, "double", 51, 0 );
		  	$$ = _newAtomNode(atom_float(SA, &t, val));
		   }
		}
 |  APPROXNUM
		{ sql_subtype t;
  		  char *p = $1;
		  double val;

		  errno = 0;
 		  val = strtod($1,&p);
		  if (p == $1 || is_dbl_nil(val) || (errno == ERANGE && (val < -1 || val > 1))) {
			char *msg = sql_message(SQLSTATE(22003) "Double value too large or not a number (%s)", $1);

			yyerror(m, msg);
			_DELETE(msg);
			$$ = NULL;
			YYABORT;
		  }
		  sql_find_subtype(&t, "double", 51, 0 );
		  $$ = _newAtomNode(atom_float(SA, &t, val)); }
 |  sqlDATE string
		{ sql_subtype t;
		  atom *a;
		  int r;

 		  r = sql_find_subtype(&t, "date", 0, 0 );
		  if (!r || (a = atom_general(SA, &t, $2)) == NULL) {
			char *msg = sql_message(SQLSTATE(22007) "Incorrect date value (%s)", $2);

			yyerror(m, msg);
			_DELETE(msg);
			$$ = NULL;
			YYABORT;
		  } else {
		  	$$ = _newAtomNode(a);
		} }
 |  TIME time_precision tz string
		{ sql_subtype t;
		  atom *a;
		  int r;

	          r = sql_find_subtype(&t, ($3)?"timetz":"time", $2, 0);
		  if (!r || (a = atom_general(SA, &t, $4)) == NULL) {
			char *msg = sql_message(SQLSTATE(22007) "Incorrect time value (%s)", $4);

			yyerror(m, msg);
			_DELETE(msg);
			$$ = NULL;
			YYABORT;
		  } else {
		  	$$ = _newAtomNode(a);
		} }
 |  TIMESTAMP timestamp_precision tz string
		{ sql_subtype t;
		  atom *a;
		  int r;

 		  r = sql_find_subtype(&t, ($3)?"timestamptz":"timestamp",$2,0);
		  if (!r || (a = atom_general(SA, &t, $4)) == NULL) {
			char *msg = sql_message(SQLSTATE(22007) "Incorrect timestamp value (%s)", $4);

			yyerror(m, msg);
			_DELETE(msg);
			$$ = NULL;
			YYABORT;
		  } else {
		  	$$ = _newAtomNode(a);
		} }
 |  interval_expression
 |  blob string
		{ sql_subtype t;
		  atom *a= 0;
		  int r;

		  $$ = NULL;
 		  r = sql_find_subtype(&t, "blob", 0, 0);
	          if (r && (a = atom_general(SA, &t, $2)) != NULL)
			$$ = _newAtomNode(a);
		  if (!$$) {
			char *msg = sql_message(SQLSTATE(22M28) "incorrect blob %s", $2);

			yyerror(m, msg);
			_DELETE(msg);
			YYABORT;
		  }
		}
 |  aTYPE string
		{ sql_subtype t;
		  atom *a= 0;
		  int r;

		  $$ = NULL;
		  r = sql_find_subtype(&t, $1, 0, 0);
	          if (r && (a = atom_general(SA, &t, $2)) != NULL)
			$$ = _newAtomNode(a);
		  if (!$$) {
			char *msg = sql_message(SQLSTATE(22000) "incorrect %s %s", $1, $2);

			yyerror(m, msg);
			_DELETE(msg);
			YYABORT;
		  }
		}
 | type_alias string
		{ sql_subtype t; 
		  atom *a = 0;
		  int r;

		  $$ = NULL;
		  r = sql_find_subtype(&t, $1, 0, 0);
	          if (r && (a = atom_general(SA, &t, $2)) != NULL)
			$$ = _newAtomNode(a);
		  if (!$$) {
			char *msg = sql_message(SQLSTATE(22000) "incorrect %s %s", $1, $2);

			yyerror(m, msg);
			_DELETE(msg);
			YYABORT;
		  }
		}
 | IDENT string
		{
		  sql_type *t = mvc_bind_type(m, $1);
		  atom *a;

		  $$ = NULL;
		  if (t) {
		  	sql_subtype tpe;
			sql_init_subtype(&tpe, t, 0, 0);
			a = atom_general(SA, &tpe, $2);
			if (a)
				$$ = _newAtomNode(a);
		  }
		  if (!t || !$$) {
			char *msg = sql_message(SQLSTATE(22000) "type (%s) unknown", $1);

			yyerror(m, msg);
			_DELETE(msg);
			YYABORT;
		  }
		}
 |  BOOL_FALSE
		{ sql_subtype t;
		  sql_find_subtype(&t, "boolean", 0, 0 );
		  $$ = _newAtomNode( atom_bool(SA, &t, FALSE)); }
 |  BOOL_TRUE
		{ sql_subtype t;
		  sql_find_subtype(&t, "boolean", 0, 0 );
		  $$ = _newAtomNode( atom_bool(SA, &t, TRUE)); }
 ;

interval_expression:
   INTERVAL opt_sign string interval_qualifier { 
		sql_subtype t;
		int sk, ek, sp, ep, tpe;
	  	lng i = 0;
		int r = 0;

		$$ = NULL;
	  	if ( (tpe = parse_interval_qualifier( m, $4, &sk, &ek, &sp, &ep )) < 0){
			yyerror(m, "incorrect interval");
			YYABORT;
	  	} else {
			int d = inttype2digits(sk, ek);
			if (tpe == 0){
				r=sql_find_subtype(&t, "month_interval", d, 0);
			} else {
				r=sql_find_subtype(&t, "sec_interval", d, 0);
			}
	  	}
	  	if (!r || (tpe = parse_interval( m, $2, $3, sk, ek, sp, ep, &i)) < 0) { 
			yyerror(m, "incorrect interval");
			$$ = NULL;
			YYABORT;
	  	} else {
			/* count the number of digits in the input */
/*
			lng cpyval = i, inlen = 1;

			cpyval /= qualifier2multiplier(ek);
			while (cpyval /= 10)
				inlen++;
		    	if (inlen > t.digits) {
				char *msg = sql_message(SQLSTATE(22006) "incorrect interval (" LLFMT " > %d)", inlen, t.digits);
				yyerror(m, msg);
				$$ = NULL;
				YYABORT;
			}
*/
	  		$$ = _newAtomNode( atom_int(SA, &t, i));
	  	}
	}

	/* miscellaneous */
 ;

qname:
    ident			{ $$ = append_string(L(), $1); }
 |  ident '.' ident		{
				  m->scanner.schema = $1;
				  $$ = append_string(
					append_string(L(), $1), $3);}
 |  ident '.' ident '.' ident	{
				  m->scanner.schema = $1;
				  $$ = append_string(
					append_string(
						append_string(L(), $1), 
						$3), 
					$5)
				;}
 ;

column_ref:
    ident		{ $$ = append_string(
				L(), $1); }

 |  ident '.' ident	{ $$ = append_string(
				append_string(
				 L(), $1), $3);}

 |  ident '.' ident '.' ident
    			{ $$ = append_string(
				append_string(
				 append_string(
				  L(), $1), $3), $5);}
 ;

cast_exp:
     CAST '(' cast_value AS data_type ')'
 	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_type(l, &$5);
	  $$ = _symbol_create_list( SQL_CAST, l ); }
 |
     CONVERT '(' cast_value ',' data_type ')'
 	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_type(l, &$5);
	  $$ = _symbol_create_list( SQL_CAST, l ); }
 ;

cast_value:
  	search_condition
 ;

case_exp:
     NULLIF '(' scalar_exp ',' scalar_exp ')'
		{ $$ = _symbol_create_list(SQL_NULLIF,
		   append_symbol(
		    append_symbol(
		     L(), $3), $5)); }
 |   COALESCE '(' case_scalar_exp_list ')'
		{ $$ = _symbol_create_list(SQL_COALESCE, $3); }
 |   CASE scalar_exp when_value_list case_opt_else END
		{ $$ = _symbol_create_list(SQL_CASE,
		   append_symbol(
		    append_list(
		     append_symbol(
		      L(),$2),$3),$4)); }
 |   CASE when_search_list case_opt_else END
		 { $$ = _symbol_create_list(SQL_CASE,
		   append_symbol(
		    append_list(
		     L(),$2),$3)); }
 ;

scalar_exp_list:
    simple_atom
			{ $$ = append_symbol( L(), $1); }
 |  scalar_exp_list ',' simple_atom
			{ $$ = append_symbol( $1, $3); }
 ;

case_scalar_exp_list: /* at least 2 scalar_exp (or null) */
    simple_atom ',' simple_atom
			{ $$ = append_symbol( L(), $1);
			  $$ = append_symbol( $$, $3);
			}
 |  case_scalar_exp_list ',' simple_atom
			{ $$ = append_symbol( $1, $3); }
 ;


when_value:
    WHEN scalar_exp THEN simple_atom
			{ $$ = _symbol_create_list( SQL_WHEN,
			   append_symbol(
			    append_symbol(
			     L(), $2),$4)); }
 ;

when_value_list:
    when_value
			{ $$ = append_symbol( L(), $1);}
 |  when_value_list when_value
			{ $$ = append_symbol( $1, $2); }
 ;

when_search:
    WHEN search_condition THEN simple_atom
			{ $$ = _symbol_create_list( SQL_WHEN,
			   append_symbol(
			    append_symbol(
			     L(), $2),$4)); }
 ;

when_search_list:
    when_search
			{ $$ = append_symbol( L(), $1); }
 |  when_search_list when_search
			{ $$ = append_symbol( $1, $2); }
 ;

case_opt_else:
    /* empty */	        { $$ = NULL; }
 |  ELSE scalar_exp	{ $$ = $2; }
 ;

case_scalar_exp:
    scalar_exp	
 ;
		/* data types, more types to come */

nonzero:
	intval
		{ $$ = $1;
		  if ($$ <= 0) {
			$$ = -1;
			yyerror(m, "Positive value greater than 0 expected");
			YYABORT;
		  }
		}
	;

nonzerolng:
	lngval
		{ $$ = $1;
		  if ($$ <= 0) {
			$$ = -1;
			yyerror(m, "Positive value greater than 0 expected");
			YYABORT;
		  }
		}
	;

poslng:
	lngval 	{ $$ = $1;
		  if ($$ < 0) {
			$$ = -1;
			yyerror(m, "Positive value expected");
			YYABORT;
		  }
		}
	;

posint:
	intval 	{ $$ = $1;
		  if ($$ < 0) {
			$$ = -1;
			yyerror(m, "Positive value expected");
			YYABORT;
		  }
		}
	;

data_type:
    CHARACTER
			{ sql_find_subtype(&$$, "char", 1, 0); }
 |  varchar
			{ $$.type = NULL;
			  yyerror(m, "CHARACTER VARYING needs a mandatory length specification");
			  YYABORT;
			}
 |  clob		{ sql_find_subtype(&$$, "clob", 0, 0); }
 |  CHARACTER '(' nonzero ')'
			{ sql_find_subtype(&$$, "char", $3, 0); }
 |  varchar '(' nonzero ')'
			{ sql_find_subtype(&$$, "varchar", $3, 0); }
 |  clob '(' nonzero ')'
			{ sql_find_subtype(&$$, "clob", $3, 0);
			  /* NOTE: CLOB may be called as CLOB(2K) which is equivalent
			   *       to CLOB(2048).  Due to 'nonzero' it is not possible
			   *       to enter this as the parser rejects it.  However it
			   *       might be a ToDo for the future.
			   *       See p. 125 SQL-99
			   */
			}
 |  blob		{ sql_find_subtype(&$$, "blob", 0, 0); }
 |  blob '(' nonzero ')'
			{ sql_find_subtype(&$$, "blob", $3, 0);
			  /* NOTE: BLOB may be called as BLOB(2K) which is equivalent
			   *       to BLOB(2048).  Due to 'nonzero' it is not possible
			   *       to enter this as the parser rejects it.  However it
			   *       might be a ToDo for the future.
			   *       See p. 85 SQL-99
			   */
			}
 |  TINYINT		{ sql_find_subtype(&$$, "tinyint", 0, 0); }
 |  SMALLINT		{ sql_find_subtype(&$$, "smallint", 0, 0); }
 |  sqlINTEGER		{ sql_find_subtype(&$$, "int", 0, 0); }
 |  BIGINT		{ sql_find_subtype(&$$, "bigint", 0, 0); }
 |  HUGEINT		{ sql_find_subtype(&$$, "hugeint", 0, 0); }

 |  sqlDECIMAL		{ sql_find_subtype(&$$, "decimal", 18, 3); }
 |  sqlDECIMAL '(' nonzero ')'
			{ 
			  int d = $3;
			  if (d > MAX_DEC_DIGITS) {
				char *msg = sql_message(SQLSTATE(22003) "Decimal of %d digits are not supported", d);
				yyerror(m, msg);
				_DELETE(msg);
				$$.type = NULL;
				YYABORT;
			  } else {
			        sql_find_subtype(&$$, "decimal", d, 0); 
			  }
			}
 |  sqlDECIMAL '(' nonzero ',' posint ')'
			{ 
			  int d = $3;
			  int s = $5;
			  if (s > d || d > MAX_DEC_DIGITS) {
				char *msg = NULL;
				if (s > d)
					msg = sql_message(SQLSTATE(22003) "Scale (%d) should be less or equal to the precision (%d)", s, d);
				else
					msg = sql_message(SQLSTATE(22003) "Decimal(%d,%d) isn't supported because P=%d > %d", d, s, d, MAX_DEC_DIGITS);
				yyerror(m, msg);
				_DELETE(msg);
				$$.type = NULL;
				YYABORT;
			  } else {
				sql_find_subtype(&$$, "decimal", d, s);
			  }
			}
 |  sqlFLOAT		{ sql_find_subtype(&$$, "double", 0, 0); }
 |  sqlFLOAT '(' nonzero ')'
			{ if ($3 > 0 && $3 <= 24) {
				sql_find_subtype(&$$, "real", $3, 0);
			  } else if ($3 > 24 && $3 <= 53) {
				sql_find_subtype(&$$, "double", $3, 0);
			  } else {
				char *msg = sql_message(SQLSTATE(22003) "Number of digits for FLOAT values should be between 1 and 53");

				yyerror(m, msg);
				_DELETE(msg);
				$$.type = NULL;
				YYABORT;
			  }
			}
 |  sqlFLOAT '(' intval ',' intval ')'
			{ if ($5 >= $3) {
				char *msg = sql_message(SQLSTATE(22003) "Precision(%d) should be less than number of digits(%d)", $5, $3);

				yyerror(m, msg);
				_DELETE(msg);
				$$.type = NULL;
				YYABORT;
			  } else if ($3 > 0 && $3 <= 24) {
				sql_find_subtype(&$$, "real", $3, $5);
			  } else if ($3 > 24 && $3 <= 53) {
				sql_find_subtype(&$$, "double", $3, $5);
			  } else {
				char *msg = sql_message(SQLSTATE(22003) "Number of digits for FLOAT values should be between 1 and 53");
				yyerror(m, msg);
				_DELETE(msg);
				$$.type = NULL;
				YYABORT;
			  }
			}
 | sqlDOUBLE 		{ sql_find_subtype(&$$, "double", 0, 0); }
 | sqlDOUBLE PRECISION	{ sql_find_subtype(&$$, "double", 0, 0); }
 | sqlREAL 		{ sql_find_subtype(&$$, "real", 0, 0); }
 | datetime_type
 | interval_type
 | aTYPE		{ sql_find_subtype(&$$, $1, 0, 0); }
 | aTYPE '(' nonzero ')'
			{ sql_find_subtype(&$$, $1, $3, 0); }
 | type_alias 		{ sql_find_subtype(&$$, $1, 0, 0); }
 | type_alias '(' nonzero ')'
			{ sql_find_subtype(&$$, $1, $3, 0); }
 | type_alias '(' intval ',' intval ')'
			{ if ($5 >= $3) {
				char *msg = sql_message(SQLSTATE(22003) "Precision(%d) should be less than number of digits(%d)", $5, $3);

				yyerror(m, msg);
				_DELETE(msg);
				$$.type = NULL;
				YYABORT;
			  } else {
			 	sql_find_subtype(&$$, $1, $3, $5);
			  }
			}
 | IDENT		{
			  sql_type *t = mvc_bind_type(m, $1);
			  if (!t) {
				char *msg = sql_message(SQLSTATE(22000) "Type (%s) unknown", $1);

				yyerror(m, msg);
				_DELETE(msg);
				$$.type = NULL;
				YYABORT;
			  } else {
				sql_init_subtype(&$$, t, 0, 0);
			  }
			}

 | IDENT '(' nonzero ')'
			{
			  sql_type *t = mvc_bind_type(m, $1);
			  if (!t) {
				char *msg = sql_message(SQLSTATE(22000) "Type (%s) unknown", $1);

				yyerror(m, msg);
				_DELETE(msg);
				$$.type = NULL;
				YYABORT;
			  } else {
				sql_init_subtype(&$$, t, $3, 0);
			  }
			}
| GEOMETRY {
		if (!sql_find_subtype(&$$, "geometry", 0, 0 )) {
			yyerror(m, SQLSTATE(22000) "type (geometry) unknown");
			$$.type = NULL;
			YYABORT;
		}
	}
| GEOMETRY '(' subgeometry_type ')' {
		int geoSubType = $3; 

		if(geoSubType == 0) {
			$$.type = NULL;
			YYABORT;
		} else if (!sql_find_subtype(&$$, "geometry", geoSubType, 0 )) {
			char *msg = sql_message(SQLSTATE(22000) "Type (%s) unknown", $1);
			yyerror(m, msg);
			_DELETE(msg);
			$$.type = NULL;
			YYABORT;
		}
		
	}
| GEOMETRY '(' subgeometry_type ',' intval ')' {
		int geoSubType = $3; 
		int srid = $5; 

		if(geoSubType == 0) {
			$$.type = NULL;
			YYABORT;
		} else if (!sql_find_subtype(&$$, "geometry", geoSubType, srid )) {
			char *msg = sql_message(SQLSTATE(22000) "Type (%s) unknown", $1);
			yyerror(m, msg);
			_DELETE(msg);
			$$.type = NULL;
			YYABORT;
		}
	}
| GEOMETRYA {
		if (!sql_find_subtype(&$$, "geometrya", 0, 0 )) {
			yyerror(m, SQLSTATE(22000) "type (geometrya) unknown");
			$$.type = NULL;
			YYABORT;
		}
	}
| GEOMETRYSUBTYPE {
	int geoSubType = find_subgeometry_type($1);

	if(geoSubType == 0) {
		char *msg = sql_message(SQLSTATE(22000) "Type (%s) unknown", $1);
		$$.type = NULL;
		yyerror(m, msg);
		_DELETE(msg);
		YYABORT;
	} else if (geoSubType == -1) {
		char *msg = sql_message("allocation failure");
		$$.type = NULL;
		yyerror(m, msg);
		_DELETE(msg);
		YYABORT;
	}  else if (!sql_find_subtype(&$$, "geometry", geoSubType, 0 )) {
		char *msg = sql_message(SQLSTATE(22000) "Type (%s) unknown", $1);
		yyerror(m, msg);
		_DELETE(msg);
		$$.type = NULL;
		YYABORT;
	}
}
 ;

subgeometry_type:
  GEOMETRYSUBTYPE {
	int subtype = find_subgeometry_type($1);
	char* geoSubType = $1;

	if(subtype == 0) {
		char *msg = sql_message(SQLSTATE(22000) "Type (%s) unknown", geoSubType);
		yyerror(m, msg);
		_DELETE(msg);
		YYABORT;
	} else if(subtype == -1) {
		char *msg = sql_message("allocation failure");
		yyerror(m, msg);
		_DELETE(msg);
		YYABORT;
	} 
	$$ = subtype;	
}
| string {
	int subtype = find_subgeometry_type($1);
	char* geoSubType = $1;

	if(subtype == 0) {
		char *msg = sql_message(SQLSTATE(22000) "Type (%s) unknown", geoSubType);
		yyerror(m, msg);
		_DELETE(msg);
		YYABORT;
	} else if (subtype == -1) {
		char *msg = sql_message("allocation failure");
		yyerror(m, msg);
		_DELETE(msg);
		YYABORT;
	} 
	$$ = subtype;	
}
;

type_alias:
 ALIAS
	{ 	char *t = sql_bind_alias($1);
	  	if (!t) {
			char *msg = sql_message(SQLSTATE(22000) "Type (%s) unknown", $1);

			yyerror(m, msg);
			_DELETE(msg);
			$$ = NULL;
			YYABORT;
		}
		$$ = t;
	}
 ;

varchar:
	VARCHAR				{ $$ = $1; }
 |	CHARACTER VARYING		{ $$ = $1; }
;

clob:
	CLOB				{ $$ = $1; }
 |	sqlTEXT				{ $$ = $1; }
 |	CHARACTER LARGE OBJECT		{ $$ = $1; }
;
blob:
	sqlBLOB				{ $$ = $1; }
 |	BINARY LARGE OBJECT		{ $$ = $1; }
;

column:			ident ;

authid: 		restricted_ident ;

restricted_ident:
    IDENT	{ $$ = $1; }
 |  aTYPE	{ $$ = $1; }
 |  ALIAS	{ $$ = $1; }
 |  AGGR	{ $$ = $1; } 	/* without '(' */
 |  AGGR2	{ $$ = $1; } 	/* without '(' */
 |  RANK	{ $$ = $1; }	/* without '(' */
 ;

ident:
    IDENT	{ $$ = $1; }
 |  aTYPE	{ $$ = $1; }
 |  FILTER_FUNC	{ $$ = $1; }
 |  ALIAS	{ $$ = $1; }
 |  AGGR	{ $$ = $1; } 	/* without '(' */
 |  AGGR2	{ $$ = $1; } 	/* without '(' */
 |  RANK	{ $$ = $1; }	/* without '(' */
 |  non_reserved_word
 ;

non_reserved_word: 
  LARGE		{ $$ = sa_strdup(SA, "large"); }	/* sloppy: officially reserved */
| sqlNAME	{ $$ = sa_strdup(SA, "name"); }
| OBJECT	{ $$ = sa_strdup(SA, "object"); }	/* sloppy: officially reserved */
| PASSWORD	{ $$ = sa_strdup(SA, "password"); }	/* neither reserved nor non-reserv. */
| PATH		{ $$ = sa_strdup(SA, "path"); }		/* sloppy: officially reserved */
| PRECISION 	{ $$ = sa_strdup(SA, "precision"); }	/* sloppy: officially reserved */
| PRIVILEGES	{ $$ = sa_strdup(SA, "privileges"); }	/* sloppy: officially reserved */
| ROLE		{ $$ = sa_strdup(SA, "role"); }	 	/* neither reserved nor non-reserv. */
| sqlSIZE	{ $$ = sa_strdup(SA, "size"); }		/* sloppy: officially reserved */
| TYPE		{ $$ = sa_strdup(SA, "type"); }
| RELEASE	{ $$ = sa_strdup(SA, "release"); }	/* sloppy: officially reserved */
| VALUE		{ $$ = sa_strdup(SA, "value"); }	/* sloppy: officially reserved */
| ZONE		{ $$ = sa_strdup(SA, "zone"); }		/* sloppy: officially reserved */

| ACTION	{ $$ = sa_strdup(SA, "action"); }	/* sloppy: officially reserved */
| AS		{ $$ = sa_strdup(SA, "as"); }		/* sloppy: officially reserved */
| AUTHORIZATION	{ $$ = sa_strdup(SA, "authorization"); }/* sloppy: officially reserved */
| COLUMN	{ $$ = sa_strdup(SA, "column"); }	/* sloppy: officially reserved */
| CYCLE		{ $$ = sa_strdup(SA, "cycle"); }	/* sloppy: officially reserved */
| DISTINCT	{ $$ = sa_strdup(SA, "distinct"); }	/* sloppy: officially reserved */
| INCREMENT	{ $$ = sa_strdup(SA, "increment"); }	/* sloppy: officially reserved */
| MAXVALUE	{ $$ = sa_strdup(SA, "maxvalue"); }	/* sloppy: officially reserved */
| MINVALUE	{ $$ = sa_strdup(SA, "minvalue"); }	/* sloppy: officially reserved */
| SQL_PLAN	{ $$ = sa_strdup(SA, "plan"); } 	/* sloppy: officially reserved */
| SCHEMA	{ $$ = sa_strdup(SA, "schema"); }	/* sloppy: officially reserved */
| START		{ $$ = sa_strdup(SA, "start"); }	/* sloppy: officially reserved */
| STATEMENT	{ $$ = sa_strdup(SA, "statement"); }	/* sloppy: officially reserved */
| TABLE		{ $$ = sa_strdup(SA, "table"); } 	/* sloppy: officially reserved */

|  CACHE	{ $$ = sa_strdup(SA, "cache"); }
|  DATA 	{ $$ = sa_strdup(SA, "data"); }
|  DIAGNOSTICS 	{ $$ = sa_strdup(SA, "diagnostics"); }
|  MATCH	{ $$ = sa_strdup(SA, "match"); }
|  OPTIONS	{ $$ = sa_strdup(SA, "options"); }
|  ROW		{ $$ = sa_strdup(SA, "row"); }
|  KEY		{ $$ = sa_strdup(SA, "key"); }
|  LANGUAGE	{ $$ = sa_strdup(SA, "language"); }
|  LEVEL	{ $$ = sa_strdup(SA, "level"); }
|  sqlSESSION	{ $$ = sa_strdup(SA, "session"); }
|  sqlDATE	{ $$ = sa_strdup(SA, "date"); }
|  TIME 	{ $$ = sa_strdup(SA, "time"); }
|  TIMESTAMP	{ $$ = sa_strdup(SA, "timestamp"); }
|  INTERVAL	{ $$ = sa_strdup(SA, "interval"); }
|  QUARTER	{ $$ = sa_strdup(SA, "quarter"); }
|  WEEK 	{ $$ = sa_strdup(SA, "week"); }
|  IMPRINTS	{ $$ = sa_strdup(SA, "imprints"); }

|  PREP		{ $$ = sa_strdup(SA, "prep"); }
|  PREPARE	{ $$ = sa_strdup(SA, "prepare"); }
|  EXEC		{ $$ = sa_strdup(SA, "exec"); }
|  EXECUTE	{ $$ = sa_strdup(SA, "execute"); }
|  SQL_EXPLAIN	{ $$ = sa_strdup(SA, "explain"); }
|  SQL_DEBUG	{ $$ = sa_strdup(SA, "debug"); }
|  SQL_TRACE	{ $$ = sa_strdup(SA, "trace"); }
|  sqlTEXT     	{ $$ = sa_strdup(SA, "text"); }
|  AUTO_COMMIT	{ $$ = sa_strdup(SA, "auto_commit"); }
|  NO		{ $$ = sa_strdup(SA, "no"); }
/* SQL/XML non reserved words */
|  STRIP	{ $$ = sa_strdup(SA, "strip"); }
|  WHITESPACE	{ $$ = sa_strdup(SA, "whitespace"); }
|  PASSING	{ $$ = sa_strdup(SA, "passing"); }
|  NIL		{ $$ = sa_strdup(SA, "nil"); }
|  REF		{ $$ = sa_strdup(SA, "ref"); }
|  ABSENT	{ $$ = sa_strdup(SA, "absent"); }
|  EMPTY	{ $$ = sa_strdup(SA, "empty"); }
|  DOCUMENT	{ $$ = sa_strdup(SA, "document"); }
|  ELEMENT	{ $$ = sa_strdup(SA, "element"); }
|  CONTENT	{ $$ = sa_strdup(SA, "content"); }
|  NAMESPACE	{ $$ = sa_strdup(SA, "namespace"); }
|  RETURNING	{ $$ = sa_strdup(SA, "returning"); }
|  LOCATION	{ $$ = sa_strdup(SA, "location"); }
|  ID		{ $$ = sa_strdup(SA, "id"); }
|  ACCORDING	{ $$ = sa_strdup(SA, "according"); }
|  URI		{ $$ = sa_strdup(SA, "uri"); }
|  FILTER	{ $$ = sa_strdup(SA, "filter"); }
|  TEMPORARY	{ $$ = sa_strdup(SA, "temporary"); }
|  TEMP		{ $$ = sa_strdup(SA, "temp"); }
|  ANALYZE	{ $$ = sa_strdup(SA, "analyze"); }
|  MINMAX	{ $$ = sa_strdup(SA, "MinMax"); }
|  STORAGE	{ $$ = sa_strdup(SA, "storage"); }
|  GEOMETRY	{ $$ = sa_strdup(SA, "geometry"); }
|  REPLACE	{ $$ = sa_strdup(SA, "replace"); }
;

name_commalist:
    ident	{ $$ = append_string(L(), $1); }
 |  name_commalist ',' ident
			{ $$ = append_string($1, $3); }
 ;

lngval:
	sqlINT	
 		{
		  char *end = NULL, *s = $1;
		  int l = _strlen(s);
		  // errno might be non-zero due to other people's code
		  errno = 0;
		  if (l <= 19) {
		  	$$ = strtoll(s,&end,10);
		  } else {
			$$ = 0;
		  }
		  if (s+l != end || errno == ERANGE) {
			char *msg = sql_message(SQLSTATE(22003) "Integer value too large or not a number (%s)", $1);

			errno = 0;
			yyerror(m, msg);
			_DELETE(msg);
			$$ = 0;
			YYABORT;
		  }
		}

intval:
	sqlINT	
 		{
		  char *end = NULL, *s = $1;
		  int l = _strlen(s);
		  // errno might be non-zero due to other people's code
		  errno = 0;
		  if (l <= 10) {
		  	$$ = strtol(s,&end,10);
		  } else {
			$$ = 0;
		  }
		  if (s+l != end || errno == ERANGE) {
			char *msg = sql_message(SQLSTATE(22003) "Integer value too large or not a number (%s)", $1);

			errno = 0;
			yyerror(m, msg);
			_DELETE(msg);
			$$ = 0;
			YYABORT;
		  }
		}
 |	IDENT	{
		  char *name = $1;
		  sql_subtype *tpe;

		  if (!stack_find_var(m, name)) {
			char *msg = sql_message(SQLSTATE(22000) "Constant (%s) unknown", $1);

			yyerror(m, msg);
			_DELETE(msg);
			$$ = 0;
			YYABORT;
		  }
		  tpe = stack_find_type(m, name);
		  if (tpe->type->localtype == TYPE_lng ||
		      tpe->type->localtype == TYPE_int ||
		      tpe->type->localtype == TYPE_sht ||
		      tpe->type->localtype == TYPE_bte ) {
			lng sgn = stack_get_number(m, name);
			assert((lng) GDK_int_min <= sgn && sgn <= (lng) GDK_int_max);
			$$ = (int) sgn;
		  } else {
			char *msg = sql_message(SQLSTATE(22000) "Constant (%s) has wrong type (number expected)", $1);

			yyerror(m, msg);
			_DELETE(msg);
			$$ = 0;
			YYABORT;
		  }
		}
 ;

string:
    STRING
		{ $$ = $1; }
 |  STRING string
		{ char *s = strconcat($1,$2); 
	 	  $$ = sa_strdup(SA, s);	
		  _DELETE(s);
		}
 ;

exec:
     execute exec_ref
		{
		  m->emode = m_execute;
		  $$ = $2; }
 ;

exec_ref:
    intval '(' ')'
	{ dlist *l = L();
  	  append_int(l, $1);
  	  append_list(l, NULL);
	  $$ = _symbol_create_list( SQL_NOP, l ); }
|   intval '(' value_commalist ')'
	{ dlist *l = L();
  	  append_int(l, $1);
  	  append_list(l, $3);
	  $$ = _symbol_create_list( SQL_NOP, l ); }
 ;

/* path specification> 

Specify an order for searching for an SQL-invoked routine.

CURRENTLY only parsed 
*/

opt_path_specification: 
	/* empty */ 	{ $$ = NULL; }
   |	path_specification
   ;

path_specification: 
        PATH schema_name_list 	{ $$ = _symbol_create_list( SQL_PATH, $2); }
   ;

schema_name_list: name_commalist ;

XML_value_expression:
  XML_primary	
  ;

XML_value_expression_list:
    XML_value_expression	
		{ $$ = append_symbol(L(), $1); }
  | XML_value_expression_list ',' XML_value_expression
		{ $$ = append_symbol($1, $3); }
  ;

XML_primary:
    scalar_exp
  ;

XML_value_function:
    XML_comment
  | XML_concatenation
  | XML_document
  | XML_element
  | XML_forest
  | XML_parse
  | XML_PI
  | XML_query
  | XML_text
  | XML_validate
  ;

XML_comment:
  XMLCOMMENT '(' value_exp /* should be a string */ opt_XML_returning_clause ')'
	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_int(l, $4);
	  $$ = _symbol_create_list( SQL_XMLCOMMENT, l); }
 ;

XML_concatenation:
  XMLCONCAT '(' XML_value_expression_list opt_XML_returning_clause ')'
	{ dlist *l = L();
	  append_list(l, $3);
	  append_int(l, $4);
	  $$ = _symbol_create_list( SQL_XMLCONCAT, l); } 
  ;

XML_document:
  XMLDOCUMENT '(' XML_value_expression opt_XML_returning_clause ')'
	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_int(l, $4);
	  $$ = _symbol_create_list( SQL_XMLDOCUMENT, l); } 

XML_element:
  XMLELEMENT '(' sqlNAME XML_element_name 
	opt_comma_XML_namespace_declaration_attributes_element_content
	opt_XML_returning_clause ')'

	{ dlist *l = L();
	  append_string(l, $4);
	  append_list(l, $5);
	  append_int(l, $6);
	  $$ = _symbol_create_list( SQL_XMLELEMENT, l);
	}
  ;

opt_comma_XML_namespace_declaration_attributes_element_content:
     /* empty */						
	{ $$ = NULL; }
  |  ',' XML_namespace_declaration 				
 	{ $$ = append_symbol(L(), $2);
	  $$ = append_symbol($$, NULL);
	  $$ = append_list($$, NULL); }
  |  ',' XML_namespace_declaration ',' XML_attributes		
 	{ $$ = append_symbol(L(), $2);
	  $$ = append_symbol($$, $4);
	  $$ = append_list($$, NULL); }
  |  ',' XML_namespace_declaration ',' XML_attributes ',' XML_element_content_and_option
 	{ $$ = append_symbol(L(), $2);
	  $$ = append_symbol($$, $4);
	  $$ = append_list($$, $6); }
  |  ',' XML_namespace_declaration ',' XML_element_content_and_option
 	{ $$ = append_symbol(L(), $2);
	  $$ = append_symbol($$, NULL);
	  $$ = append_list($$, $4); }
  |  ',' XML_attributes					
 	{ $$ = append_symbol(L(), NULL);
	  $$ = append_symbol($$, $2);
	  $$ = append_list($$, NULL); }
  |  ',' XML_attributes ',' XML_element_content_and_option 
 	{ $$ = append_symbol(L(), NULL);
	  $$ = append_symbol($$, $2);
	  $$ = append_list($$, $4); }
  |  ',' XML_element_content_and_option 			
 	{ $$ = append_symbol(L(), NULL);
	  $$ = append_symbol($$, NULL);
	  $$ = append_list($$, $2); }
  ;

XML_element_name: 
    ident 		
  ;

XML_attributes:
  XMLATTRIBUTES '(' XML_attribute_list ')'	{ $$ = $3; }
  ;

XML_attribute_list:
    XML_attribute 				{ $$ = $1; }
  | XML_attribute_list ',' XML_attribute 	
		{ dlist *l = L();
		  append_list(l, 
		  	append_string(L(), sa_strdup(SA, "concat")));
	  	  append_symbol(l, $1);
	  	  append_symbol(l, $3);
	  	  $$ = _symbol_create_list( SQL_BINOP, l ); }
  ;

XML_attribute:
  XML_attribute_value opt_XML_attribute_name	
	{ dlist *l = L();
	  append_string(l, $2);
	  append_symbol(l, $1);
	  $$ = _symbol_create_list( SQL_XMLATTRIBUTE, l ); }
  ;

opt_XML_attribute_name:
     /* empty */ 				{ $$ = NULL; }
  | AS XML_attribute_name 			{ $$ = $2; }
  ; 

XML_attribute_value:
     scalar_exp
  ;

XML_attribute_name:
    ident
  ;

XML_element_content_and_option:
    XML_element_content_list opt_XML_content_option
		{ $$ = L();
		  $$ = append_list($$, $1);
		  $$ = append_int($$, $2); 	}
  ;

XML_element_content_list:
    XML_element_content
		{ $$ = append_symbol(L(), $1); }
  | XML_element_content_list ',' XML_element_content	
		{ $$ = append_symbol($1, $3); }
  ;

XML_element_content:
     scalar_exp
  ;

opt_XML_content_option:
    /* empty */			{ $$ = 0; }
  | OPTION XML_content_option	{ $$ = $2; }
  ;

XML_content_option:
    sqlNULL ON sqlNULL		{ $$ = 0; }
  | EMPTY ON sqlNULL		{ $$ = 1; }
  | ABSENT ON sqlNULL		{ $$ = 2; }
  | NIL ON sqlNULL		{ $$ = 3; }
  | NIL ON NO CONTENT		{ $$ = 4; }
  ;

XML_forest:
    XMLFOREST '(' opt_XML_namespace_declaration_and_comma
      forest_element_list opt_XML_content_option
      opt_XML_returning_clause ')'
	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_list(l, $4);
	  append_int(l, $5);
	  append_int(l, $6);
	  $$ = _symbol_create_list( SQL_XMLFOREST, l);
	}
  ;

opt_XML_namespace_declaration_and_comma: 
     /* empty */			{ $$ = NULL; }
  |  XML_namespace_declaration ','	{ $$ = $1; }
  ;

forest_element_list:
     forest_element 				
		{ $$ = append_list(L(), $1); }
  |  forest_element_list ',' forest_element
		{ $$ = append_list($1, $3); }
  ;

forest_element:
    forest_element_value opt_forest_element_name
		{ $$ = append_symbol(L(), $1);
		  $$ = append_string($$, $2); }
  ;

forest_element_value:
    scalar_exp	{ $$ = $1; }
  ;

opt_forest_element_name:
    /* empty */			{ $$ = NULL; }
  | AS forest_element_name	{ $$ = $2; }
  ;

forest_element_name:
    ident			{ $$ = $1; }
  ;

 /* | XML_value_function */
XML_parse:
  XMLPARSE '(' document_or_content value_exp /* should be a string */
      XML_whitespace_option ')'
	{ dlist *l = L();
	  append_int(l, $3 );
	  append_symbol(l, $4);
	  append_int(l, $5);
	  $$ = _symbol_create_list( SQL_XMLPARSE, l); }

XML_whitespace_option:
    PRESERVE WHITESPACE		{ $$ = 0; }
  | STRIP WHITESPACE		{ $$ = 1; }
  ;

XML_PI:
  XMLPI '(' sqlNAME XML_PI_target
	opt_comma_string_value_expression
	opt_XML_returning_clause ')'
	{ dlist *l = L();
	  append_string(l, $4);
	  append_symbol(l, $5);
	  append_int(l, $6);
	  $$ = _symbol_create_list( SQL_XMLPI, l); }
  ;

XML_PI_target:
    ident
  ;

opt_comma_string_value_expression:
	/* empty */ 	{ $$ = NULL; }
  | ',' value_exp /* should be a string */
			{ $$ = $2; }
  ;

XML_query:
  XMLQUERY '('
      XQuery_expression
      opt_XML_query_argument_list
      opt_XML_returning_clause	/* not correct, ie need to combine with next */
      opt_XML_query_returning_mechanism
      XML_query_empty_handling_option
      ')'
	{ $$ = NULL; }

XQuery_expression:
	STRING
  ;

opt_XML_query_argument_list:
	/* empty */
  | PASSING XML_query_default_passing_mechanism XML_query_argument_list
  ;

XML_query_default_passing_mechanism:
     XML_passing_mechanism
  ;

XML_query_argument_list:
    XML_query_argument
  | XML_query_argument_list ',' XML_query_argument
  ;

XML_query_argument:
    XML_query_context_item
  | XML_query_variable
  ;

XML_query_context_item:
     value_exp opt_XML_passing_mechanism
  ;

XML_query_variable:
    value_exp AS ident opt_XML_passing_mechanism
  ; 

opt_XML_query_returning_mechanism:
   /* empty */
 | XML_passing_mechanism
 ;

XML_query_empty_handling_option:
    sqlNULL ON EMPTY
  | EMPTY ON EMPTY
  ;

XML_text:
  XMLTEXT '(' value_exp /* should be a string */
      opt_XML_returning_clause ')'
	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_int(l, $4);
	  $$ = _symbol_create_list( SQL_XMLTEXT, l); } 

XML_validate:
  XMLVALIDATE '('
      document_or_content_or_sequence
      XML_value_expression
      opt_XML_valid_according_to_clause
      ')'
	{ $$ = NULL; }
  ;

document_or_content_or_sequence:
    document_or_content
  | SEQUENCE		{ $$ = 2; }
  ;

document_or_content:
    DOCUMENT		{ $$ = 0; }
  | CONTENT		{ $$ = 1; }
  ;

opt_XML_returning_clause:
   /* empty */			{ $$ = 0; }
 | RETURNING CONTENT		{ $$ = 0; }
 | RETURNING SEQUENCE		{ $$ = 1; }
 ;

/*
<XML lexically scoped options> ::=
  <XML lexically scoped option> [ <comma> <XML lexically scoped option> ]
<XML lexically scoped option> ::=
    <XML namespace declaration>
  | <XML binary encoding>

<XML binary encoding> ::=
  XMLBINARY [ USING ] { BASE64 | HEX }
*/

XML_namespace_declaration:
  XMLNAMESPACES '(' XML_namespace_declaration_item_list ')' 	{ $$ = $3; }
  ;

XML_namespace_declaration_item_list:
 	XML_namespace_declaration_item 	{ $$ = $1; }
  |     XML_namespace_declaration_item_list ',' XML_namespace_declaration_item
		{ dlist *l = L();
		  append_list(l, 
		  	append_string(L(), sa_strdup(SA, "concat")));
	  	  append_symbol(l, $1);
	  	  append_symbol(l, $3);
	  	  $$ = _symbol_create_list( SQL_BINOP, l ); }
  ;

XML_namespace_declaration_item:
    	XML_regular_namespace_declaration_item
  | 	XML_default_namespace_declaration_item
  ;

XML_namespace_prefix:
    	ident
  ;

XML_namespace_URI:
	scalar_exp
  ;

XML_regular_namespace_declaration_item:
    XML_namespace_URI AS XML_namespace_prefix
				{ char *s = strconcat("xmlns:", $3);
				  dlist *l = L();
	  			  append_string(l, sa_strdup(SA, s));
				  _DELETE(s);
	  			  append_symbol(l, $1);
	  			  $$ = _symbol_create_list( SQL_XMLATTRIBUTE, l ); }
  ;

XML_default_namespace_declaration_item:
    DEFAULT XML_namespace_URI	{ dlist *l = L();
	  			  append_string(l, sa_strdup(SA, "xmlns" ));
	  			  append_symbol(l, $2);
	  			  $$ = _symbol_create_list( SQL_XMLATTRIBUTE, l ); }
  | NO DEFAULT			{ $$ = NULL; }
  ;

opt_XML_passing_mechanism:
    /* empty */
  | XML_passing_mechanism
  ;

XML_passing_mechanism:
    BY REF
  | BY VALUE
  ;

opt_XML_valid_according_to_clause:
    /* empty */
  | XML_valid_according_to_clause
  ;

XML_valid_according_to_clause:
    ACCORDING TO XMLSCHEMA XML_valid_according_to_what
      opt_XML_valid_element_clause
  ;

XML_valid_according_to_what:
    XML_valid_according_to_URI
  | XML_valid_according_to_identifier
  ;

XML_valid_according_to_URI:
    URI XML_valid_target_namespace_URI opt_XML_valid_schema_location
  | NO NAMESPACE opt_XML_valid_schema_location
  ;

XML_valid_target_namespace_URI:
    XML_URI
  ;

XML_URI:
    STRING
  ;

opt_XML_valid_schema_location:
   /* empty */
 | LOCATION XML_valid_schema_location_URI
 ;

XML_valid_schema_location_URI:
   XML_URI
 ;

XML_valid_according_to_identifier:
   ID registered_XML_Schema_name
 ;

registered_XML_Schema_name:
    ident
  ;

opt_XML_valid_element_clause:
    /* empty */
 |  XML_valid_element_clause
 ;

XML_valid_element_clause:
    XML_valid_element_name_specification
  | XML_valid_element_namespace_specification
      opt_XML_valid_element_name_specification
  ;

opt_XML_valid_element_name_specification:
    /* empty */
 |  XML_valid_element_name_specification
 ;

XML_valid_element_name_specification:
   ELEMENT XML_valid_element_name
 ;

XML_valid_element_namespace_specification:
    NO NAMESPACE
  | NAMESPACE XML_valid_element_namespace_URI
  ;

XML_valid_element_namespace_URI:
   XML_URI
 ;

XML_valid_element_name:
   ident
 ;

XML_aggregate:
  XMLAGG '(' XML_value_expression
      opt_order_by_clause
      opt_XML_returning_clause
      ')'
	{ 
          dlist *aggr = L();

          if ($4) {
	  	if ($3 != NULL && $3->token == SQL_SELECT) {
			SelectNode *s = (SelectNode*)$3;
	
			s->orderby = $4;
	  	} else {
			yyerror(m, "ORDER BY: missing select operator");
			YYABORT;
		}
	  }
          append_list(aggr, append_string(append_string(L(), "sys"), "xmlagg"));
  	  append_int(aggr, FALSE);
	  append_symbol(aggr, $3);
	  /* int returning not used */
	  $$ = _symbol_create_list( SQL_AGGR, aggr);
	}
 ;

%%
int find_subgeometry_type(char* geoSubType) {
	int subType = 0;
	if(strcmp(geoSubType, "point") == 0 )
		subType = (1 << 2);
	else if(strcmp(geoSubType, "linestring") == 0)
		subType = (2 << 2);
	else if(strcmp(geoSubType, "polygon") == 0)
		subType = (4 << 2);
	else if(strcmp(geoSubType, "multipoint") == 0)
		subType = (5 << 2);
	else if(strcmp(geoSubType, "multilinestring") == 0)
		subType = (6 << 2);
	else if(strcmp(geoSubType, "multipolygon") == 0)
		subType = (7 << 2);
	else if(strcmp(geoSubType, "geometrycollection") == 0)
		subType = (8 << 2);
	else {
		size_t strLength = strlen(geoSubType);
		if(strLength > 0 ) {
			char *typeSubStr = GDKmalloc(strLength);
			char flag = geoSubType[strLength-1]; 

			if (typeSubStr == NULL) {
				return -1;
			}
			memcpy(typeSubStr, geoSubType, strLength-1);
			typeSubStr[strLength-1]='\0';
			if(flag == 'z' || flag == 'm' ) {
				subType = find_subgeometry_type(typeSubStr);
				if (subType == -1) {
					GDKfree(typeSubStr);
					return -1;
				}
				if(flag == 'z')
					SET_Z(subType);
				if(flag == 'm')
					SET_M(subType);
			}
			GDKfree(typeSubStr);
		}

	}
	return subType;	
}

char *token2string(int token)
{
	switch (token) {
#define SQL(TYPE) case SQL_##TYPE : return #TYPE
	SQL(CREATE_SCHEMA);
	SQL(CREATE_TABLE);
	SQL(CREATE_VIEW);
	SQL(CREATE_INDEX);
	SQL(CREATE_ROLE);
	SQL(CREATE_USER);
	SQL(CREATE_TYPE);
	SQL(CREATE_FUNC);
	SQL(CREATE_SEQ);
	SQL(CREATE_TRIGGER);
	SQL(DROP_SCHEMA);
	SQL(DROP_TABLE);
	SQL(DROP_VIEW);
	SQL(DROP_INDEX);
	SQL(DROP_ROLE);
	SQL(DROP_USER);
	SQL(DROP_TYPE);
	SQL(DROP_FUNC);
	SQL(DROP_SEQ);
	SQL(DROP_TRIGGER);
	SQL(ALTER_TABLE);
	SQL(ALTER_SEQ);
	SQL(ALTER_USER);
	SQL(DROP_COLUMN);
	SQL(DROP_CONSTRAINT);
	SQL(DROP_DEFAULT);
	SQL(DECLARE);
	SQL(SET);
	SQL(PREP);
	SQL(PREPARE);
	SQL(NAME);
	SQL(USER);
	SQL(PATH);
	SQL(CHARSET);
	SQL(SCHEMA);
	SQL(TABLE);
	SQL(TYPE);
	SQL(CASE);
	SQL(CAST);
	SQL(RETURN);
	SQL(IF);
	SQL(ELSE);
	SQL(WHILE);
	SQL(COLUMN);
	SQL(COLUMN_OPTIONS);
	SQL(COALESCE);
	SQL(CONSTRAINT);
	SQL(CHECK);
	SQL(DEFAULT);
	SQL(NOT_NULL);
	SQL(NULL);
	SQL(NULLIF);
	SQL(UNIQUE);
	SQL(PRIMARY_KEY);
	SQL(FOREIGN_KEY);
	SQL(BEGIN);
#define TR(TYPE) case TR_##TYPE : return #TYPE
	TR(COMMIT);
	TR(ROLLBACK);
	TR(SAVEPOINT);
	TR(RELEASE);
	TR(START);
	TR(MODE);
	SQL(INSERT);
	SQL(DELETE);
	SQL(UPDATE);
	SQL(CROSS);
	SQL(JOIN);
	SQL(SELECT);
	SQL(WHERE);
	SQL(FROM);
	SQL(UNIONJOIN);
	SQL(UNION);
	SQL(EXCEPT);
	SQL(INTERSECT);
	SQL(VALUES);
	SQL(ASSIGN);
	SQL(ORDERBY);
	SQL(GROUPBY);
	SQL(DESC);
	SQL(AND);
	SQL(OR);
	SQL(NOT);
	SQL(EXISTS);
	SQL(NOT_EXISTS);
	SQL(OP);
	SQL(UNOP);
	SQL(BINOP);
	SQL(NOP);
	SQL(BETWEEN);
	SQL(NOT_BETWEEN);
	SQL(LIKE);
	SQL(IN);
	SQL(NOT_IN);
	SQL(GRANT);
	SQL(GRANT_ROLES);
	SQL(REVOKE);
	SQL(REVOKE_ROLES);
	SQL(EXEC);
	SQL(EXECUTE);
	SQL(PRIVILEGES);
	SQL(ROLE);
	SQL(PARAMETER);
	SQL(FUNC);
	SQL(AGGR);
	SQL(RANK);
	SQL(FRAME);
	SQL(COMPARE);
	SQL(FILTER);
	SQL(TEMP_LOCAL);
	SQL(TEMP_GLOBAL);
	SQL(INT_VALUE);
	SQL(ATOM);
	SQL(USING);
	SQL(WHEN);
	SQL(ESCAPE);
	SQL(COPYFROM);
	SQL(BINCOPYFROM);
	SQL(COPYTO);
	SQL(EXPORT);
	SQL(NEXT);
	SQL(MULSTMT);
	SQL(WITH);
	SQL(XMLCOMMENT);
	SQL(XMLCONCAT);
	SQL(XMLDOCUMENT);
	SQL(XMLELEMENT);
	SQL(XMLATTRIBUTE);
	SQL(XMLFOREST);
	SQL(XMLPARSE);
	SQL(XMLPI);
	SQL(XMLQUERY);
	SQL(XMLTEXT);
	SQL(XMLVALIDATE);
	SQL(XMLNAMESPACES);
	}
	return "unknown";	/* just needed for broken compilers ! */
}

void *sql_error( mvc * sql, int error_code, char *format, ... )
{
	va_list	ap;

	va_start (ap,format);
	if (sql->errstr[0] == '\0')
		vsnprintf(sql->errstr, ERRSIZE-1, _(format), ap);
	if (!sql->session->status)
		sql->session->status = -error_code;
	va_end (ap);
	return NULL;
}

int sqlerror(mvc * c, const char *err)
{
	const char *sqlstate;

	if (err && strlen(err) > 6 && err[5] == '!') {
		/* sql state provided */
		sqlstate = "";
	} else {
		/* default: Syntax error or access rule violation */
		sqlstate = SQLSTATE(42000);
	}
	if (c->scanner.errstr) {
		if (c->scanner.errstr[0] == '!'){
			assert(0);// catch it
			(void)sql_error(c, 4,
					"%s%s: %s\n",
					sqlstate, err, c->scanner.errstr + 1);
		} else
			(void)sql_error(c, 4,
					"%s%s: %s in \"%.80s\"\n",
					sqlstate, err, c->scanner.errstr,
					QUERY(c->scanner));
	} else
		(void)sql_error(c, 4,
				"%s%s in: \"%.80s\"\n",
				sqlstate, err, QUERY(c->scanner));
	return 1;
}

