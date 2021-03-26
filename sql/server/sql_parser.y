/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

%{
#include "monetdb_config.h"
#include "sql_mem.h"
#include "sql_parser.h"
#include "sql_symbol.h"
#include "sql_datetime.h"
#include "sql_decimal.h"	/* for decimal_from_str() */
#include "sql_semantic.h"	/* for sql_add_param() */
#include "sql_env.h"
#include "rel_sequence.h"	/* for sql_next_seq_name() */

static int sqlerror(mvc *sql, const char *err);
static int sqlformaterror(mvc *sql, _In_z_ _Printf_format_string_ const char *format, ...)
	        __attribute__((__format__(__printf__, 2, 3)));

static void *ma_alloc(sql_allocator *sa, size_t sz);
static void ma_free(void *p);

#include <unistd.h>
#include <string.h>

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

#define Malloc(sz) ma_alloc(m->ta,sz)
#define YYMALLOC Malloc
#define YYFREE ma_free 

#define YY_parse_LSP_NEEDED	/* needed for bison++ 1.21.11-3 */

#define SET_Z(info)(info = info | 0x02)
#define SET_M(info)(info = info | 0x01)

#ifdef HAVE_HGE
#define MAX_DEC_DIGITS 38
#define MAX_HEX_DIGITS 32
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


static char *
uescape_xform(char *restrict s, const char *restrict esc)
{
	size_t i, j;

	for (i = j = 0; s[i]; i++) {
		if (s[i] == *esc) {
			if (s[i + 1] == *esc) {
				s[j++] = *esc;
				i++;
			} else {
				int c = 0;
				int n;
				if (s[i + 1] == '+') {
					n = 6;
					i++;
				} else {
					n = 4;
				}
				do {
					i++;
					c <<= 4;
					if ('0' <= s[i] && s[i] <= '9')
						c |= s[i] - '0';
					else if ('a' <= s[i] && s[i] <= 'f')
						c |= s[i] - 'a' + 10;
					else if ('A' <= s[i] && s[i] <= 'F')
						c |= s[i] - 'A' + 10;
					else
						return NULL;
				} while (--n > 0);
				if (c == 0 || c > 0x10FFFF || (c & 0xFFF800) == 0xD800)
					return NULL;
				if (c < 0x80) {
					s[j++] = c;
				} else {
					if (c < 0x800) {
						s[j++] = 0xC0 | (c >> 6);
					} else {
						if (c < 0x10000) {
							s[j++] = 0xE0 | (c >> 12);
						} else {
							s[j++] = 0xF0 | (c >> 18);
							s[j++] = 0x80 | ((c >> 12) & 0x3F);
						}
						s[j++] = 0x80 | ((c >> 6) & 0x3F);
					}
					s[j++] = 0x80 | (c & 0x3F);
				}
			}
		} else {
			s[j++] = s[i];
		}
	}
	s[j] = 0;
	return s;
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
%define api.pure
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
	add_table_element
	aggr_or_window_ref
	alter_statement
	alter_table_element
	and_exp
	assignment
	atom
	between_predicate
	call_procedure_statement
	call_statement
	case_exp
	case_opt_else
	case_statement
	cast_exp
	catalog_object
	column_constraint
	column_constraint_type
	column_def
	column_exp
	column_option
	column_options
	comment_on_statement
	comparison_predicate
	control_statement
	copyfrom_stmt
	create_statement
	datetime_funcs
	dealloc
	declare_statement
	default
	default_value
	delete_stmt
	domain_constraint_type 
	drop_statement
	drop_table_element
	exec
	exec_ref
	existence_test
	filter_exp
	forest_element_value
	func_data_type
	func_def
	func_def_opt_return
	func_ref
	generated_column
	grant
	group_by_element
	grouping_set_element
	if_opt_else
	if_statement
	in_predicate
	index_def
	insert_atom
	insert_stmt
	interval_expression
	join_spec
	joined_table
	like_exp
	like_predicate
	like_table
	literal
	merge_insert
	merge_match_clause
	merge_stmt
	merge_update_or_delete
	null
	object_name
	operation
	opt_alt_seq_param
	opt_as_partition
	opt_comma_string_value_expression
	opt_from_clause
	opt_group_by_clause
	opt_having_clause
	opt_limit
	opt_offset
	opt_order_by_clause
	opt_over
	opt_partition_by
	opt_partition_spec
	opt_path_specification
	opt_sample
	opt_schema_default_char_set
	opt_search_condition
	opt_seed
	opt_seq_common_param
	opt_seq_param
	opt_table_name
	opt_when
	opt_where_clause
	opt_window_clause
	opt_XML_namespace_declaration_and_comma
	ordering_spec
	ordinary_grouping_element
	param
	partition_expression
	partition_list_value
	partition_on
	partition_range_from
	partition_range_to
	path_specification
	pred_exp
	predicate
	procedure_statement
	query_expression
	query_expression_def
	return_statement
	return_value
	revoke
	role_def
	routine_invocation
	scalar_exp
	schema
	schema_element
	search_condition
	select_no_parens
	select_no_parens_orderby
	select_statement_single_row
	seq_def
	set_statement
	simple_scalar_exp
	simple_select
	sql
	sqlstmt
	string_funcs
	subquery
	subquery_with_orderby
	table_constraint
	table_constraint_type
	table_content_source
	table_def
	table_element
	table_name
	table_ref
	test_for_null
	transaction_statement
	transaction_stmt
	trigger_def
	trigger_event
	trigger_procedure_statement
	truncate_stmt
	type_def
	update_statement
	update_stmt
	value
	value_exp
	values_or_query_spec
	view_def
	when_search
	when_search_statement
	when_statement
	when_value
	while_statement
	window_bound
	window_definition
	window_following_bound
	window_frame_clause
	window_frame_start
	window_order_clause
	window_partition_clause
	with_list_element
	with_query
	with_query_expression
	XML_aggregate
	XML_attribute
	XML_attribute_list
	XML_attribute_value
	XML_attributes
	XML_comment
	XML_default_namespace_declaration_item
	XML_element_content
	XML_namespace_declaration
	XML_namespace_declaration_item
	XML_namespace_declaration_item_list
	XML_namespace_URI
	XML_primary
	XML_regular_namespace_declaration_item
	XML_value_expression
	XML_value_function
	XML_concatenation
	XML_document
	XML_element
	XML_forest
	XML_parse
	XML_PI
	XML_query
	XML_text
	XML_validate

%type <type>
	data_type
	datetime_type
	interval_type

%type <sval>
	authid
	authorization_identifier
	blob
	blobstring
	calc_ident
	calc_restricted_ident
	clob
	column
	forest_element_name
	func_ident
	function_body
	grantee
	ident
	ident_or_uident
	non_reserved_word
	opt_alias_name
	opt_begin_label
	opt_constraint_name
	opt_end_label
	opt_forest_element_name
	opt_null_string
	opt_to_savepoint
	opt_uescape
	opt_using
	opt_XML_attribute_name
	restricted_ident
	sstring
	string
	type_alias
	user_schema
	user_schema_path
	ustring
	varchar
	window_ident_clause
	XML_attribute_name
	XML_element_name
	XML_namespace_prefix
	XML_PI_target

%type <l>
	argument_list
	as_subquery_clause
	assignment_commalist
	atom_commalist
	authid_list
	case_opt_else_statement
	case_search_condition_commalist
	column_commalist_parens
	column_def_opt_list
	column_exp_commalist
	column_option_list
	column_ref
	column_ref_commalist
	end_field
	external_function_name
	filter_arg_list
	filter_args
	forest_element
	forest_element_list
	fwf_widthlist
	global_privileges
	grantee_commalist
	group_by_list
	grouping_set_list
	header
	header_list
	ident_commalist
	interval_qualifier
	merge_when_list
	object_privileges
	old_or_new_values_alias
	old_or_new_values_alias_list
	operation_commalist
	opt_alt_seq_params
	opt_column_def_opt_list
	opt_column_list
	opt_comma_XML_namespace_declaration_attributes_element_content
	opt_corresponding
	opt_fwf_widths
	opt_header_list
	opt_nr
	opt_paramlist
	opt_referencing_list
	opt_schema_element_list
	opt_seps
	opt_seq_params
	opt_typelist
	opt_with_encrypted_password
	ordinary_grouping_set
	paramlist
	params_list
	partition_list
	pred_exp_list
	privileges
	procedure_statement_list
	qfunc
	qname
	qrank
	routine_body
	routine_designator
	routine_name
	row_commalist
	schema_element_list
	schema_name_clause
	schema_name_list
	search_condition_commalist
	selection
	serial_opt_params
	single_datetime_field
	sort_specification_list
	start_field
	string_commalist
	string_commalist_contents
	table_element_list
	table_exp
	table_function_column_list
	table_ref_commalist
	trigger_procedure_statement_list
	triggered_action
	triggered_statement
	typelist
	value_commalist
	variable_list
	variable_ref
	variable_ref_commalist
	variable_ref_commalist_parens
	when_search_list
	when_search_statements
	when_statements
	when_value_list
	window_definition_list
	window_frame_between
	window_frame_extent
	window_specification
	with_list
	with_opt_credentials
	XML_element_content_and_option
	XML_element_content_list
	XML_value_expression_list

%type <i_val>
	_transaction_mode_list
	check_identity
	datetime_field
	dealloc_ref
	document_or_content
	document_or_content_or_sequence
	drop_action
	extract_datetime_field
	func_def_type
	global_privilege
	grantor
	intval
	join_type
	non_second_datetime_field
	nonzero
	opt_any_all_some
	opt_bounds
	opt_column
	opt_encrypted
	opt_endianness
	opt_for_each
	opt_from_grantor
	opt_grantor	
	opt_index_type
	opt_match
	opt_match_type
	opt_minmax
	opt_on_commit
	opt_outer
	opt_ref_action
	opt_sign
	opt_temp
	opt_XML_content_option
	opt_XML_returning_clause
	outer_join_type
	partition_type
	posint
	ref_action
	ref_on_delete
	ref_on_update
	row_or_statement
	serial_or_bigserial
	subgeometry_type
	time_precision
	timestamp_precision
	transaction_mode
	transaction_mode_list
	trigger_action_time
	window_frame_exclusion
	window_frame_units
	with_or_without_data
	XML_content_option
	XML_whitespace_option

%type <l_val>
	lngval
	poslng
	nonzerolng

%type <bval>
	create
	create_or_replace
	if_exists
	if_not_exists
	opt_admin_for
	opt_asc_desc
	opt_best_effort
	opt_brackets
	opt_chain
	opt_constraint
	opt_distinct
	opt_escape
	opt_grant_for
	opt_nulls_first_last
	opt_on_location
	opt_with_admin
	opt_with_check_option
	opt_with_grant
	opt_with_nulls
	opt_work
	set_distinct
	tz

%right <sval> STRING USTRING XSTRING
%right <sval> X_BODY

/* sql prefixes to avoid name clashes on various architectures */
%token <sval>
	IDENT UIDENT aTYPE ALIAS RANK sqlINT OIDNUM HEXADECIMAL INTNUM APPROXNUM
	USING 
	GLOBAL CAST CONVERT
	CHARACTER VARYING LARGE OBJECT VARCHAR CLOB sqlTEXT BINARY sqlBLOB
	sqlDECIMAL sqlFLOAT
	TINYINT SMALLINT BIGINT HUGEINT sqlINTEGER
	sqlDOUBLE sqlREAL PRECISION PARTIAL SIMPLE ACTION CASCADE RESTRICT
	BOOL_FALSE BOOL_TRUE
	CURRENT_DATE CURRENT_TIMESTAMP CURRENT_TIME LOCALTIMESTAMP LOCALTIME
	BIG LITTLE NATIVE ENDIAN
	LEX_ERROR 
	
/* the tokens used in geom */
%token <sval> GEOMETRY GEOMETRYSUBTYPE GEOMETRYA 

%token	USER CURRENT_USER SESSION_USER LOCAL BEST EFFORT
%token  CURRENT_ROLE sqlSESSION CURRENT_SCHEMA CURRENT_TIMEZONE
%token <sval> sqlDELETE UPDATE SELECT INSERT MATCHED
%token <sval> LATERAL LEFT RIGHT FULL OUTER NATURAL CROSS JOIN INNER
%token <sval> COMMIT ROLLBACK SAVEPOINT RELEASE WORK CHAIN NO PRESERVE ROWS
%token  START TRANSACTION READ WRITE ONLY ISOLATION LEVEL
%token  UNCOMMITTED COMMITTED sqlREPEATABLE SERIALIZABLE DIAGNOSTICS sqlSIZE STORAGE

%token <sval> ASYMMETRIC SYMMETRIC ORDER ORDERED BY IMPRINTS
%token <operation> EXISTS ESCAPE UESCAPE HAVING sqlGROUP ROLLUP CUBE sqlNULL
%token <operation> GROUPING SETS FROM FOR MATCH

%token <operation> EXTRACT

/* sequence operations */
%token SEQUENCE INCREMENT RESTART CONTINUE
%token MAXVALUE MINVALUE CYCLE
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
%left UNION EXCEPT INTERSECT CORRESPONDING
%left JOIN CROSS LEFT FULL RIGHT INNER NATURAL
%left WITH DATA
%left <operation> '(' ')'

%left <operation> NOT
%left <operation> '='
%left <operation> ALL ANY NOT_BETWEEN BETWEEN NOT_IN sqlIN NOT_LIKE LIKE NOT_ILIKE ILIKE OR SOME
%left <operation> AND
%left <sval> COMPARISON /* <> < > <= >= */
%left <operation> '+' '-' '&' '|' '^' LEFT_SHIFT RIGHT_SHIFT LEFT_SHIFT_ASSIGN RIGHT_SHIFT_ASSIGN CONCATSTRING SUBSTRING POSITION SPLIT_PART
%left <operation> '*' '/' '%'
%left UMINUS
%left <operation> '~'

%left <operation> GEOM_OVERLAP GEOM_OVERLAP_OR_ABOVE GEOM_OVERLAP_OR_BELOW GEOM_OVERLAP_OR_LEFT
%left <operation> GEOM_OVERLAP_OR_RIGHT GEOM_BELOW GEOM_ABOVE GEOM_DIST GEOM_MBR_EQUAL

/* literal keyword tokens */
/*
CONTINUE CURRENT CURSOR FOUND GOTO GO LANGUAGE
SQLCODE SQLERROR UNDER WHENEVER
*/

%token TEMP TEMPORARY MERGE REMOTE REPLICA
%token<sval> ASC DESC AUTHORIZATION
%token CHECK CONSTRAINT CREATE COMMENT NULLS FIRST LAST
%token TYPE PROCEDURE FUNCTION sqlLOADER AGGREGATE RETURNS EXTERNAL sqlNAME DECLARE
%token CALL LANGUAGE
%token ANALYZE MINMAX SQL_EXPLAIN SQL_PLAN SQL_DEBUG SQL_TRACE PREP PREPARE EXEC EXECUTE DEALLOCATE
%token DEFAULT DISTINCT DROP TRUNCATE
%token FOREIGN
%token RENAME ENCRYPTED UNENCRYPTED PASSWORD GRANT REVOKE ROLE ADMIN INTO
%token IS KEY ON OPTION OPTIONS
%token PATH PRIMARY PRIVILEGES
%token<sval> PUBLIC REFERENCES SCHEMA SET AUTO_COMMIT
%token RETURN 

%token ALTER ADD TABLE COLUMN TO UNIQUE VALUES VIEW WHERE WITH
%token<sval> sqlDATE TIME TIMESTAMP INTERVAL
%token CENTURY DECADE YEAR QUARTER DOW DOY MONTH WEEK DAY HOUR MINUTE SECOND EPOCH ZONE
%token LIMIT OFFSET SAMPLE SEED

%token CASE WHEN THEN ELSE NULLIF COALESCE IF ELSEIF WHILE DO
%token ATOMIC BEGIN END
%token COPY RECORDS DELIMITERS STDIN STDOUT FWF CLIENT SERVER
%token INDEX REPLACE

%token AS TRIGGER OF BEFORE AFTER ROW STATEMENT sqlNEW OLD EACH REFERENCING
%token OVER PARTITION CURRENT EXCLUDE FOLLOWING PRECEDING OTHERS TIES RANGE UNBOUNDED GROUPS WINDOW

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
			}
   sqlstmt		{ $$ = $3; YYACCEPT; }
 | SQL_TRACE 		{
		  	  m->emod |= mod_trace;
			  m->scanner.as = m->scanner.yycur; 
			}
   sqlstmt		{ $$ = $3; YYACCEPT; }
 | exec SCOLON		{ m->sym = $$ = $1; YYACCEPT; }
 | dealloc SCOLON	{ m->sym = $$ = $1; YYACCEPT; }
 | /*empty*/		{ m->sym = $$ = NULL; YYACCEPT; }
 | SCOLON		{ m->sym = $$ = NULL; YYACCEPT; }
 | error SCOLON		{ m->sym = $$ = NULL; YYACCEPT; }
 | LEX_ERROR		{ m->sym = $$ = NULL; YYABORT; }
 ;

prepare:
   PREPARE
 | PREP
 ; 

execute:
   EXECUTE
 | EXEC
 ; 

opt_prepare:
   /* empty */
 | prepare
 ;

deallocate:
   DEALLOCATE
 ;

create:
    CREATE  { $$ = FALSE; }

create_or_replace:
	create
|	CREATE OR REPLACE { $$ = TRUE; }
;

if_exists:
	/* empty */   { $$ = FALSE; }
|	IF EXISTS     { $$ = TRUE; }
;

if_not_exists:
	/* empty */   { $$ = FALSE; }
|	IF NOT EXISTS { $$ = TRUE; }
;

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
 |  comment_on_statement
 ;

opt_minmax:
   /* empty */  	{ $$ = 0; }
 | MINMAX		{ $$ = 1; }
 ;

declare_statement:
	declare variable_list { $$ = _symbol_create_list( SQL_DECLARE, $2); }
  | declare table_def     { $$ = $2; if ($$) $$->token = SQL_DECLARE_TABLE; }
 ;

variable_ref_commalist:
    variable_ref                            { $$ = append_list(L(), $1); }
 |  variable_ref_commalist ',' variable_ref { $$ = append_list( $1, $3 ); }
 ;

variable_list:
	variable_ref_commalist data_type
		{ dlist *l = L();
		append_list(l, $1 );
		append_type(l, &$2 );
		$$ = append_symbol(L(), _symbol_create_list( SQL_DECLARE, l)); }
    |	variable_list ',' variable_ref_commalist data_type
		{ dlist *l = L();
		append_list(l, $3 );
		append_type(l, &$4 );
		$$ = append_symbol($1, _symbol_create_list( SQL_DECLARE, l)); }
 ;

opt_equal:
    '='
  |
  ;

set_statement:
    set variable_ref '=' search_condition
		{ dlist *l = L();
		append_list(l, $2 );
		append_symbol(l, $4 );
		$$ = _symbol_create_list( SQL_SET, l); }
  | set variable_ref_commalist_parens '=' subquery
		{ dlist *l = L();
	  	append_list(l, $2);
	  	append_symbol(l, $4);
	  	$$ = _symbol_create_list( SQL_SET, l ); }
  |	set sqlSESSION AUTHORIZATION opt_equal ident
		{ dlist *l = L();
		  sql_subtype t;
		sql_find_subtype(&t, "char", UTF8_strlen($5), 0 );
		append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_user")));
		append_symbol(l, _newAtomNode( _atom_string(&t, $5)) );
		$$ = _symbol_create_list( SQL_SET, l); }
  |	set session_schema opt_equal ident
		{ dlist *l = L();
		  sql_subtype t;
		sql_find_subtype(&t, "char", UTF8_strlen($4), 0 );
		append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_schema")));
		append_symbol(l, _newAtomNode( _atom_string(&t, $4)) );
		$$ = _symbol_create_list( SQL_SET, l); }
  |	set session_user opt_equal ident
		{ dlist *l = L();
		  sql_subtype t;
		sql_find_subtype(&t, "char", UTF8_strlen($4), 0 );
		append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_user")));
		append_symbol(l, _newAtomNode( _atom_string(&t, $4)) );
		$$ = _symbol_create_list( SQL_SET, l); }
  |	set session_role opt_equal ident
		{ dlist *l = L();
		  sql_subtype t;
		sql_find_subtype(&t, "char", UTF8_strlen($4), 0);
		append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_role")));
		append_symbol(l, _newAtomNode( _atom_string(&t, $4)) );
		$$ = _symbol_create_list( SQL_SET, l); }
  |	set session_timezone opt_equal LOCAL
		{ dlist *l = L();
		  sql_subtype t;
		sql_find_subtype(&t, "sec_interval", inttype2digits(ihour, isec), 0);
		append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_timezone")));
		append_symbol(l, _newAtomNode(atom_int(SA, &t, 0)));
		$$ = _symbol_create_list( SQL_SET, l); }
  |	set session_timezone opt_equal literal
		{ dlist *l = L();
		append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_timezone")));
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
		append_int(l, $5);
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
 |  TRUNCATE			    { $$ = _symbol_create(SQL_TRUNCATE,NULL); }
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
   ALTER TABLE if_exists qname ADD opt_column add_table_element
	{ dlist *l = L();
	  append_list(l, $4);
	  append_symbol(l, $7);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
 | ALTER TABLE if_exists qname ADD TABLE qname opt_as_partition
	{ dlist *l = L(), *part;
	  append_list(l, $4);
	  append_symbol(l, _symbol_create_list( SQL_TABLE, append_list(L(),$7)));
	  append_int(l, $3);
	  if($8) {
	      part = $8->data.lval;
	      append_int(part, FALSE);
	  }
	  append_symbol(l, $8);
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
 | ALTER TABLE if_exists qname ALTER alter_table_element
	{ dlist *l = L();
	  append_list(l, $4);
	  append_symbol(l, $6);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
 | ALTER TABLE if_exists qname DROP drop_table_element
	{ dlist *l = L();
	  append_list(l, $4);
	  append_symbol(l, $6);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
 | ALTER TABLE if_exists qname SET READ ONLY
	{ dlist *l = L();
	  append_list(l, $4);
	  append_symbol(l, _symbol_create_int(SQL_ALTER_TABLE, tr_readonly));
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
 | ALTER TABLE if_exists qname SET INSERT ONLY
	{ dlist *l = L();
	  append_list(l, $4);
	  append_symbol(l, _symbol_create_int(SQL_ALTER_TABLE, tr_append));
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
 | ALTER TABLE if_exists qname SET READ WRITE
	{ dlist *l = L();
	  append_list(l, $4);
	  append_symbol(l, _symbol_create_int(SQL_ALTER_TABLE, tr_writable));
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
 | ALTER TABLE if_exists qname SET TABLE qname opt_as_partition
	{ dlist *l = L(), *part;
	  append_list(l, $4);
	  append_symbol(l, _symbol_create_list( SQL_TABLE, append_list(L(),$7)));
	  append_int(l, $3);
	  if($8) {
	      part = $8->data.lval;
	      append_int(part, TRUE);
	  }
	  append_symbol(l, $8);
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
 | ALTER TABLE if_exists qname RENAME TO ident
	{ dlist *l = L();
	  append_list(l, $4);
	  append_string(l, $7);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_RENAME_TABLE, l ); }
 | ALTER TABLE if_exists qname RENAME opt_column ident TO ident
	{ dlist *l = L();
	  append_list(l, $4);
	  append_string(l, $7);
	  append_string(l, $9);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_RENAME_COLUMN, l); }
 | ALTER TABLE if_exists qname SET SCHEMA ident
	{ dlist *l = L();
	  append_list(l, $4);
	  append_string(l, $7);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_SET_TABLE_SCHEMA, l ); }
 | ALTER USER ident opt_with_encrypted_password user_schema user_schema_path
	{ dlist *l = L(), *p = L();
	  if (!$4 && !$5 && !$6) {
		yyerror(m, "ALTER USER: At least one property should be updatd");
		YYABORT;
	  }
	  append_string(l, $3);
	  append_string(p, $4 ? $4->h->data.sval : NULL);
	  append_string(p, $5);
	  append_string(p, $6);
	  append_int(p, $4 ? $4->h->next->data.i_val : 0);
	  append_string(p, NULL);
	  append_list(l, p);
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
	  append_string(p, NULL);
	  append_int(p, $4);
	  append_string(p, $10);
	  append_list(l, p);
	  $$ = _symbol_create_list( SQL_ALTER_USER, l ); }
 | ALTER SCHEMA if_exists ident RENAME TO ident
	{ dlist *l = L();
	  append_string(l, $4);
	  append_string(l, $7);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_RENAME_SCHEMA, l ); }
  ;

opt_with_encrypted_password:
	WITH opt_encrypted PASSWORD string	{ $$ = append_int(append_string(L(), $4), $2); }
 |  /* empty */							{ $$ = NULL; }
 ;

user_schema:
	SET SCHEMA ident	{ $$ = $3; }
 |  /* empty */			{ $$ = NULL; }
 ;

user_schema_path:
	SCHEMA PATH string	{ $$ = $3; }
 |  /* empty */			{ $$ = NULL; }
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
 |	opt_column ident SET STORAGE string
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
  |  TABLE qname drop_action
	{ dlist *l = L();
	  append_list(l, $2 );
	  append_int(l, $3 );
	  append_int(l, FALSE); /* no if exists check */
	  $$ = _symbol_create_list( SQL_DROP_TABLE, l ); }
  ;

opt_column:
     COLUMN	 { $$ = 0; }
 |   /* empty */ { $$ = 0; }
 ;

create_statement:	
   create role_def 	{ $$ = $2; }
 | create table_def 	{ $$ = $2; }
 | view_def 	{ $$ = $1; }
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
	params_list  { $$ = $1; }
  |              { $$ = NULL; }

params_list:
	opt_seq_param			  { $$ = append_symbol(L(), $1); }
  |	params_list opt_seq_param { $$ = append_symbol($1, $2); }
  ;

opt_alt_seq_params:
	opt_alt_seq_param			{ $$ = append_symbol(L(), $1); }
  |	opt_alt_seq_params opt_alt_seq_param	{ $$ = append_symbol($1, $2); }
  ;

opt_seq_param:
	AS data_type 			{ $$ = _symbol_create_list(SQL_TYPE, append_type(L(),&$2)); }
  |	START WITH opt_sign lngval 	{ $$ = _symbol_create_lng(SQL_START, is_lng_nil($4) ? $4 : $3 * $4); }
  |	opt_seq_common_param		{ $$ = $1; }
  ;

opt_alt_seq_param:
	AS data_type 			{ $$ = _symbol_create_list(SQL_TYPE, append_type(L(),&$2)); }
  |	RESTART 			{ $$ = _symbol_create_list(SQL_START, append_int(L(),0)); /* plain restart now */ }
  |	RESTART WITH opt_sign lngval 	{ $$ = _symbol_create_list(SQL_START, append_lng(append_int(L(),2), is_lng_nil($4) ? $4 : $3 * $4));  }
  |	RESTART WITH subquery 		{ $$ = _symbol_create_list(SQL_START, append_symbol(append_int(L(),1), $3));  }
  |	opt_seq_common_param		{ $$ = $1; }
  ;

opt_seq_common_param:
	INCREMENT BY opt_sign lngval	{ $$ = _symbol_create_lng(SQL_INC, is_lng_nil($4) ? $4 : $3 * $4); }
  |	MINVALUE opt_sign lngval	{ $$ = _symbol_create_lng(SQL_MINVALUE, is_lng_nil($3) ? $3 : $2 * $3); }
  |	NO MINVALUE			{ $$ = _symbol_create_lng(SQL_MINVALUE, 0); }
  |	MAXVALUE opt_sign lngval	{ $$ = _symbol_create_lng(SQL_MAXVALUE, is_lng_nil($3) ? $3 : $2 * $3); }
  |	NO MAXVALUE			{ $$ = _symbol_create_lng(SQL_MAXVALUE, 0); }
  |	CACHE nonzerolng		{ $$ = _symbol_create_lng(SQL_CACHE, $2); }
  |	CYCLE				{ $$ = _symbol_create_int(SQL_CYCLE, 1); }
  |	NO CYCLE			{ $$ = _symbol_create_int(SQL_CYCLE, 0); }
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
 |  USER ident WITH opt_encrypted PASSWORD string sqlNAME string SCHEMA ident user_schema_path
	{ dlist *l = L();
	  append_string(l, $2);
	  append_string(l, $6);
	  append_string(l, $8);
	  append_string(l, $10);
	  append_string(l, $11);
	  append_int(l, $4);
	  $$ = _symbol_create_list( SQL_CREATE_USER, l ); }
 ;

opt_encrypted:
    /* empty */		{ $$ = SQL_PW_UNENCRYPTED; }
 |  UNENCRYPTED		{ $$ = SQL_PW_UNENCRYPTED; }
 |  ENCRYPTED		{ $$ = SQL_PW_ENCRYPTED; }
 ;

table_def:
    TABLE if_not_exists qname table_content_source
	{ int commit_action = CA_COMMIT;
	  dlist *l = L();

	  append_int(l, SQL_PERSIST);
	  append_list(l, $3);
	  append_symbol(l, $4);
	  append_int(l, commit_action);
	  append_string(l, NULL);
	  append_list(l, NULL);
	  append_int(l, $2);
	  append_symbol(l, NULL); /* only used for merge table */
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
 |  TABLE if_not_exists qname FROM sqlLOADER func_ref
    {
      dlist *l = L();
      append_list(l, $3);
      append_symbol(l, $6);
      $$ = _symbol_create_list( SQL_CREATE_TABLE_LOADER, l);
    }
 |  MERGE TABLE if_not_exists qname table_content_source opt_partition_by
	{ int commit_action = CA_COMMIT, tpe = SQL_MERGE_TABLE;
	  dlist *l = L();

	  append_int(l, tpe);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  append_int(l, commit_action);
	  append_string(l, NULL);
	  append_list(l, NULL);
	  append_int(l, $3);
	  append_symbol(l, $6);
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
 |  REPLICA TABLE if_not_exists qname table_content_source 
	{ int commit_action = CA_COMMIT, tpe = SQL_REPLICA_TABLE;
	  dlist *l = L();

	  append_int(l, tpe);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  append_int(l, commit_action);
	  append_string(l, NULL);
	  append_list(l, NULL);
	  append_int(l, $3);
	  append_symbol(l, NULL); /* only used for merge table */
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
 /* mapi:monetdb://host:port/database[/schema[/table]] 
    This also allows access via monetdbd. 
    We assume the monetdb user with default password */
 |  REMOTE TABLE if_not_exists qname table_content_source ON string with_opt_credentials
	{ int commit_action = CA_COMMIT, tpe = SQL_REMOTE;
	  dlist *l = L();

	  append_int(l, tpe);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  append_int(l, commit_action);
	  append_string(l, $7);
	  append_list(l, $8);
	  append_int(l, $3);
	  append_symbol(l, NULL); /* only used for merge table */
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
	  append_list(l, NULL);
	  append_int(l, $3);
	  append_symbol(l, NULL); /* only used for merge table */
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
 ;

partition_type:
   RANGE	{ $$ = PARTITION_RANGE; }
 | VALUES	{ $$ = PARTITION_LIST; }
 ;

partition_expression:
   search_condition 	{ $$ = $1; }
 ;

partition_on:
   ON '(' ident ')'                   { $$ = _symbol_create_list( SQL_PARTITION_COLUMN, append_string(L(), $3) ); }
 | USING '(' partition_expression ')' { $$ = _symbol_create_list( SQL_PARTITION_EXPRESSION, append_symbol(L(), $3) ); }
 ;

opt_partition_by:
 /* empty */									 { $$ = NULL; }
 | PARTITION BY partition_type partition_on
   { dlist *l = L();
     int properties = $3;
     append_int(l, $3);
     append_symbol(l, $4);

     assert($3 == PARTITION_RANGE || $3 == PARTITION_LIST);
     if($4->token == SQL_PARTITION_COLUMN) {
        properties |= PARTITION_COLUMN;
     } else if($4->token == SQL_PARTITION_EXPRESSION) {
        properties |= PARTITION_EXPRESSION;
     } else {
        assert(0);
     }
     append_int(l, properties);

     $$ = _symbol_create_list( SQL_MERGE_PARTITION, l ); }
 ;

partition_list_value:
   search_condition { $$ = $1; }
 ;

partition_range_from:
   search_condition { $$ = $1; }
 | RANGE MINVALUE    { $$ = _symbol_create(SQL_MINVALUE, NULL ); }
 ;

partition_range_to:
   search_condition { $$ = $1; }
 | RANGE MAXVALUE    { $$ = _symbol_create(SQL_MAXVALUE, NULL ); }
 ;

partition_list:
   partition_list_value						{ $$ = append_symbol(L(), $1 ); }
 | partition_list ',' partition_list_value  { $$ = append_symbol($1, $3 ); }
 ;

opt_with_nulls:
    /* empty */		{ $$ = FALSE; }
 |  WITH sqlNULL VALUES	{ $$ = TRUE; }
 ;

opt_partition_spec:
   sqlIN '(' partition_list ')' opt_with_nulls
    { dlist *l = L();
      append_list(l, $3);
      append_int(l, $5);
      $$ = _symbol_create_list( SQL_PARTITION_LIST, l ); }
 | FROM partition_range_from TO partition_range_to opt_with_nulls
    { dlist *l = L();
      append_symbol(l, $2);
      append_symbol(l, $4);
      append_int(l, $5);
      $$ = _symbol_create_list( SQL_PARTITION_RANGE, l ); }
 | FOR sqlNULL VALUES
    { dlist *l = L();
      append_symbol(l, NULL);
      append_symbol(l, NULL);
      append_int(l, TRUE);
      $$ = _symbol_create_list( SQL_MERGE_PARTITION, l ); }
 ;

opt_as_partition:
 /* empty */						 { $$ = NULL; }
 | AS PARTITION opt_partition_spec	 { $$ = $3; }
 ;

with_opt_credentials:
  /* empty */
  {
	  $$ = append_string(L(), NULL);
	  append_int($$, SQL_PW_ENCRYPTED);
	  append_string($$, NULL);
  }
  | WITH USER string opt_encrypted PASSWORD string
  {
	  $$ = append_string(L(), $3);
	  append_int($$, $4);
	  append_string($$, $6);
  }
  | WITH opt_encrypted PASSWORD string
  {
	  $$ = append_string(L(), NULL);
	  append_int($$, $2);
	  append_string($$, $4);
  }
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
    search_condition 	{ $$ = $1; }
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
		append_int(l, 1); /* to be dropped */
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
		append_int(l, 1); /* to be dropped */
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
  |	'(' params_list ')'					{ $$ = $2; }
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
    CHECK '(' search_condition ')' { $$ = _symbol_create_symbol(SQL_CHECK, $3); }
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
    create_or_replace VIEW qname opt_column_list AS query_expression_def opt_with_check_option
	{  dlist *l = L();
	  append_list(l, $3);
	  append_list(l, $4);
	  append_symbol(l, $6);
	  append_int(l, $7);
	  append_int(l, TRUE);	/* persistent view */
	  append_int(l, $1);
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

variable_ref_commalist_parens:
   '(' variable_ref_commalist ')'	{ $$ = $2; }
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

func_def_type:
	FUNCTION			{ $$ = F_FUNC; }
|	PROCEDURE			{ $$ = F_PROC; }
|	AGGREGATE			{ $$ = F_AGGR; }
|	AGGREGATE FUNCTION	{ $$ = F_AGGR; }
|	FILTER				{ $$ = F_FILT; }
|	FILTER FUNCTION		{ $$ = F_FILT; }
|	WINDOW				{ $$ = F_ANALYTIC; }
|	WINDOW FUNCTION		{ $$ = F_ANALYTIC; }
|	sqlLOADER			{ $$ = F_LOADER; }
|	sqlLOADER FUNCTION	{ $$ = F_LOADER; }

func_def_opt_return:
	RETURNS func_data_type	{ $$ = $2; }
|							{ $$ = NULL; }

func_def:
    create_or_replace func_def_type qname
	'(' opt_paramlist ')'
    func_def_opt_return
    EXTERNAL sqlNAME external_function_name
			{ dlist *f = L();
				append_list(f, $3);
				append_list(f, $5);
				append_symbol(f, $7);
				append_list(f, $10);
				append_list(f, NULL);
				append_int(f, $2);
				append_int(f, FUNC_LANG_MAL);
				append_int(f, $1);
			  $$ = _symbol_create_list( SQL_CREATE_FUNC, f ); }
 |  create_or_replace func_def_type qname
	'(' opt_paramlist ')'
    func_def_opt_return
    routine_body
			{ dlist *f = L();
				append_list(f, $3);
				append_list(f, $5);
				append_symbol(f, $7);
				append_list(f, NULL);
				append_list(f, $8);
				append_int(f, $2);
				append_int(f, FUNC_LANG_SQL);
				append_int(f, $1);
			  $$ = _symbol_create_list( SQL_CREATE_FUNC, f ); }
  | create_or_replace func_def_type qname
	'(' opt_paramlist ')'
    func_def_opt_return
    LANGUAGE IDENT function_body
		{
			int lang = 0;
			dlist *f = L();
			char l = *$9;

			if (l == 'R' || l == 'r')
				lang = FUNC_LANG_R;
			else if (l == 'P' || l == 'p') {
				// code does not get cleaner than this people
				if (strcasecmp($9, "PYTHON_MAP") == 0) {
					lang = FUNC_LANG_MAP_PY;
				} else if (strcasecmp($9, "PYTHON3_MAP") == 0) {
					lang = FUNC_LANG_MAP_PY3;
				} else if (strcasecmp($9, "PYTHON3") == 0) {
					lang = FUNC_LANG_PY3;
				} else {
					lang = FUNC_LANG_PY;
				}
			} else if (l == 'C' || l == 'c') {
				if (strcasecmp($9, "CPP") == 0) {
					lang = FUNC_LANG_CPP;
				} else {
					lang = FUNC_LANG_C;
				}
			}
			else if (l == 'J' || l == 'j')
				lang = FUNC_LANG_J;
			else {
				sqlformaterror(m, "Language name R, C, PYTHON[3], PYTHON[3]_MAP or J(avascript):expected, received '%c'", l);
			}

			append_list(f, $3);
			append_list(f, $5);
			append_symbol(f, $7);
			append_list(f, NULL);
			append_list(f, append_string(L(), $10));
			append_int(f, $2);
			append_int(f, lang);
			append_int(f, $1);
			$$ = _symbol_create_list( SQL_CREATE_FUNC, f );
		}
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
    /* empty */							{ $$ = L(); }
 |  search_condition 					{ $$ = append_symbol( L(), $1); }
 |  argument_list ',' search_condition	{ $$ = append_symbol( $1, $3); }
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
     CASE search_condition when_statements case_opt_else_statement END CASE
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
    WHEN search_condition THEN procedure_statement_list
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
    create_or_replace TRIGGER qname trigger_action_time trigger_event
    ON qname opt_referencing_list triggered_action
	{ dlist *l = L();
	  append_list(l, $3);
	  append_int(l, $4);
	  append_symbol(l, $5);
	  append_list(l, $7);
	  append_list(l, $8);
	  append_list(l, $9);
	  append_int(l, $1);
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
 |  sqlDELETE 		{ $$ = _symbol_create_list(SQL_DELETE, NULL); }
 |  TRUNCATE 		{ $$ = _symbol_create_list(SQL_TRUNCATE, NULL); }
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
	func_def_type qname opt_typelist
	{ dlist *l = L();
	  append_list(l, $2 );	
	  append_list(l, $3 );
	  append_int(l, $1 );
	  $$ = l; }
 ;

drop_statement:
   drop TABLE if_exists qname drop_action
	{ dlist *l = L();
	  append_list(l, $4 );
	  append_int(l, $5 );
	  append_int(l, $3 );
	  $$ = _symbol_create_list( SQL_DROP_TABLE, l ); }
 | drop func_def_type if_exists qname opt_typelist drop_action
	{ dlist *l = L();
	  append_list(l, $4 );
	  append_list(l, $5 );
	  append_int(l, $2 );
	  append_int(l, $3 );
	  append_int(l, 0 ); /* not all */
	  append_int(l, $6 );
	  $$ = _symbol_create_list( SQL_DROP_FUNC, l ); }
 | drop ALL func_def_type qname drop_action
	{ dlist *l = L();
	  append_list(l, $4 );
	  append_list(l, NULL );
	  append_int(l, $3 );
	  append_int(l, FALSE );
	  append_int(l, 1 ); /* all */
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
 |  drop TRIGGER if_exists qname
	{ dlist *l = L();
	  append_list(l, $4 );
	  append_int(l, $3 );
	  $$ = _symbol_create_list( SQL_DROP_TRIGGER, l );
	}
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
   delete_stmt
 | truncate_stmt
 | insert_stmt
 | update_stmt
 | merge_stmt
 | copyfrom_stmt
 ;

transaction_statement:
   transaction_stmt
	{
	  $$ = $1;
	  m->type = Q_TRANS;					}
 ;

start_transaction:
   START
 | BEGIN
 ;

transaction_stmt:
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
 |  start_transaction TRANSACTION transaction_mode_list
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

opt_on_location:
    /* empty */		{ $$ = 0; }
  | ON CLIENT		{ $$ = 1; }
  | ON SERVER		{ $$ = 0; }
  ;

copyfrom_stmt:
    COPY opt_nr INTO qname opt_column_list FROM string_commalist opt_header_list opt_on_location opt_seps opt_escape opt_null_string opt_best_effort opt_constraint opt_fwf_widths
	{ dlist *l = L();
	  append_list(l, $4);
	  append_list(l, $5);
	  append_list(l, $7);
	  append_list(l, $8);
	  append_list(l, $10);
	  append_list(l, $2);
	  append_string(l, $12);
	  append_int(l, $13);
	  append_int(l, $14);
	  append_list(l, $15);
	  append_int(l, $9);
	  append_int(l, $11);
	  $$ = _symbol_create_list( SQL_COPYFROM, l ); }
  | COPY opt_nr INTO qname opt_column_list FROM STDIN  opt_header_list opt_seps opt_escape opt_null_string opt_best_effort opt_constraint
	{ dlist *l = L();
	  append_list(l, $4);
	  append_list(l, $5);
	  append_list(l, NULL);
	  append_list(l, $8);
	  append_list(l, $9);
	  append_list(l, $2);
	  append_string(l, $11);
	  append_int(l, $12);
	  append_int(l, $13);
	  append_list(l, NULL);
	  append_int(l, 0);
	  append_int(l, $10);
	  $$ = _symbol_create_list( SQL_COPYFROM, l ); }
  | COPY sqlLOADER INTO qname FROM func_ref
	{ dlist *l = L();
	  append_list(l, $4);
	  append_symbol(l, $6);
	  $$ = _symbol_create_list( SQL_COPYLOADER, l ); }
   | COPY opt_endianness BINARY INTO qname opt_column_list FROM string_commalist opt_on_location opt_constraint
	{ dlist *l = L();
	  append_list(l, $5);
	  append_list(l, $6);
	  append_list(l, $8);
	  append_int(l, $10);
	  append_int(l, $9);
	  append_int(l, $2);
	  $$ = _symbol_create_list( SQL_BINCOPYFROM, l ); }
  | COPY query_expression_def INTO string opt_on_location opt_seps opt_null_string
	{ dlist *l = L();
	  append_symbol(l, $2);
	  append_string(l, $4);
	  append_list(l, $6);
	  append_string(l, $7);
	  append_int(l, $5);
	  $$ = _symbol_create_list( SQL_COPYTO, l ); }
  | COPY query_expression_def INTO STDOUT opt_seps opt_null_string
	{ dlist *l = L();
	  append_symbol(l, $2);
	  append_string(l, NULL);
	  append_list(l, $5);
	  append_string(l, $6);
	  append_int(l, 0);
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
 |	ident string
			{ dlist *l = L();
			  append_string(l, $1 );
			  append_string(l, $2 );
			  $$ = l; }
 ;

opt_seps:
    /* empty */
				{ dlist *l = L();
				  append_string(l, sa_strdup(SA, "|"));
				  append_string(l, sa_strdup(SA, "\n"));
				  $$ = l; }
 |  opt_using DELIMITERS string
				{ dlist *l = L();
				  append_string(l, $3);
				  append_string(l, sa_strdup(SA, "\n"));
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
				  append_string(l, $7);
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

opt_escape:
	/* empty */	{ $$ = TRUE; }		/* ESCAPE is default */
 |  	ESCAPE		{ $$ = TRUE; }
 |  	NO ESCAPE	{ $$ = FALSE; }
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

opt_endianness:
	/* empty */		{ $$ = endian_native; }
	| BIG ENDIAN		{ $$ = endian_big; }
	| LITTLE ENDIAN	{ $$ = endian_little; }
	| NATIVE ENDIAN	{ $$ = endian_native; }
	;

delete_stmt:
    sqlDELETE FROM qname opt_alias_name opt_where_clause

	{ dlist *l = L();
	  append_list(l, $3);
	  append_string(l, $4);
	  append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_DELETE, l ); }
 ;

check_identity:
    /* empty */			{ $$ = 0; }
 |  CONTINUE IDENTITY	{ $$ = 0; }
 |  RESTART IDENTITY	{ $$ = 1; }
 ;

truncate_stmt:
   TRUNCATE TABLE qname check_identity drop_action
	{ dlist *l = L();
	  append_list(l, $3 );
	  append_int(l, $4 );
	  append_int(l, $5 );
	  $$ = _symbol_create_list( SQL_TRUNCATE, l ); }
 | TRUNCATE qname check_identity drop_action
	{ dlist *l = L();
	  append_list(l, $2 );
	  append_int(l, $3 );
	  append_int(l, $4 );
	  $$ = _symbol_create_list( SQL_TRUNCATE, l ); }
 ;

update_stmt:
    UPDATE qname opt_alias_name SET assignment_commalist opt_from_clause opt_where_clause

	{ dlist *l = L();
	  append_list(l, $2);
	  append_string(l, $3);
	  append_list(l, $5);
	  append_symbol(l, $6);
	  append_symbol(l, $7);
	  $$ = _symbol_create_list( SQL_UPDATE, l ); }
 ;

opt_search_condition:
 /* empty */            { $$ = NULL; }
 | AND search_condition { $$ = $2; }
 ;

merge_update_or_delete:
   UPDATE SET assignment_commalist
   { dlist *l = L();
     append_list(l, $3);
     $$ = _symbol_create_list( SQL_UPDATE, l ); }
 | sqlDELETE
   { $$ = _symbol_create_list( SQL_DELETE, NULL ); }
 ;

merge_insert:
   INSERT opt_column_list values_or_query_spec
   { dlist *l = L();
     append_list(l, $2);
     append_symbol(l, $3);
     $$ = _symbol_create_list( SQL_INSERT, l ); }
 ;

merge_match_clause:
   WHEN MATCHED opt_search_condition THEN merge_update_or_delete
   { dlist *l = L();
     append_symbol(l, $3);
     append_symbol(l, $5);
     $$ = _symbol_create_list( SQL_MERGE_MATCH, l ); }
 | WHEN NOT MATCHED opt_search_condition THEN merge_insert
   { dlist *l = L();
     append_symbol(l, $4);
     append_symbol(l, $6);
     $$ = _symbol_create_list( SQL_MERGE_NO_MATCH, l ); }
 ;

merge_when_list:
   merge_match_clause                 { $$ = append_symbol(L(), $1); }
 | merge_when_list merge_match_clause { $$ = append_symbol($1, $2); }
 ;

merge_stmt:
    MERGE INTO qname opt_alias_name USING table_ref ON search_condition merge_when_list

	{ dlist *l = L();
	  append_list(l, $3);
	  append_string(l, $4);
	  append_symbol(l, $6);
	  append_symbol(l, $8);
	  append_list(l, $9);
	  $$ = _symbol_create_list( SQL_MERGE, l ); }

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
   sqlNULL 		{ $$ = _symbol_create(SQL_NULL, NULL ); }
 ;

insert_atom:
    search_condition
 |  DEFAULT		{ $$ = _symbol_create(SQL_DEFAULT, NULL ); }
 ;

value:
    search_condition
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

assignment:
   column '=' insert_atom
	{ dlist *l = L();
	  append_symbol(l, $3);
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
	  append_int(l, FALSE); /* no replace clause */
	  $$ = _symbol_create_list( SQL_CREATE_VIEW, l ); 
	}
 ;

with_query_expression:
   select_no_parens_orderby
 | select_statement_single_row
 | delete_stmt
 | insert_stmt
 | update_stmt
 | merge_stmt
 ;

sql:
   select_statement_single_row
 | select_no_parens_orderby
 ;

simple_select:
    SELECT opt_distinct selection table_exp
	{ $$ = newSelectNode( SA, $2, $3, NULL,
		$4->h->data.sym,
		$4->h->next->data.sym,
		$4->h->next->next->data.sym,
		$4->h->next->next->next->data.sym,
		NULL, NULL, NULL, NULL, NULL, NULL,
		$4->h->next->next->next->next->data.sym);
	}
    ;

select_statement_single_row:
    SELECT opt_distinct selection INTO variable_ref_commalist table_exp
	{ $$ = newSelectNode( SA, $2, $3, $5,
		$6->h->data.sym,
		$6->h->next->data.sym,
		$6->h->next->next->data.sym,
		$6->h->next->next->next->data.sym,
		NULL, NULL, NULL, NULL, NULL, NULL,
		$6->h->next->next->next->next->data.sym);
	}
    ;

select_no_parens_orderby:
     select_no_parens opt_order_by_clause opt_limit opt_offset opt_sample opt_seed
	 { 
	  $$ = $1;
	  if ($2 || $3 || $4 || $5 || $6) {
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
				s -> seed = $6;
			} else { /* Add extra select * from .. in case of UNION, EXCEPT, INTERSECT */
				$$ = newSelectNode( 
					SA, 0, 
					append_symbol(L(), _symbol_create_list(SQL_TABLE, append_string(append_string(L(),NULL),NULL))), NULL,
					_symbol_create_list( SQL_FROM, append_symbol(L(), $1)), NULL, NULL, NULL, $2, _symbol_create_list(SQL_NAME, append_list(append_string(L(),"inner"),NULL)), $3, $4, $5, $6, NULL);
			}
	  	} else {
			yyerror(m, "missing SELECT operator");
			YYABORT;
	  	}
	 } 
	}
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
 |  VALUES row_commalist     { $$ = _symbol_create_list( SQL_VALUES, $2); }
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
    opt_from_clause opt_window_clause opt_where_clause opt_group_by_clause opt_having_clause

	{ $$ = L();
	  append_symbol($$, $1);
	  append_symbol($$, $3);
	  append_symbol($$, $4);
	  append_symbol($$, $5);
	  append_symbol($$, $2); }
 ;

window_definition:
    ident AS '(' window_specification ')' { dlist *l = L(); append_string(l, $1); append_list(l, $4);
                                            $$ = _symbol_create_list(SQL_NAME, l); }
 ;

window_definition_list:
    window_definition                            { $$ = append_symbol(L(), $1); }
 |  window_definition_list ',' window_definition { $$ = append_symbol($1, $3); }
 ;

opt_window_clause:
    /* empty */                   { $$ = NULL; }
 |  WINDOW window_definition_list { $$ = _symbol_create_list( SQL_WINDOW, $2); }
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
 ;

table_name:
    AS ident '(' ident_commalist ')'
				{ dlist *l = L();
		  		  append_string(l, $2);
		  	  	  append_list(l, $4);
		  		  $$ = _symbol_create_list(SQL_NAME, l); }
 |  AS ident
				{ dlist *l = L();
		  		  append_string(l, $2);
		  	  	  append_list(l, NULL);
		  		  $$ = _symbol_create_list(SQL_NAME, l); }
 |  ident '(' ident_commalist ')'
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
	/* empty */ { $$ = NULL; }
 |  table_name  { $$ = $1; }
 ;

opt_group_by_clause:
	/* empty */               { $$ = NULL; }
 |  sqlGROUP BY group_by_list { $$ = _symbol_create_list(SQL_GROUPBY, $3); }
 ;

group_by_list:
	group_by_element                   { $$ = append_symbol(L(), $1); }
 |  group_by_list ',' group_by_element { $$ = append_symbol($1, $3); }
 ;

group_by_element:
    search_condition                        { $$ = _symbol_create_list(SQL_GROUPBY, append_symbol(L(), $1)); }
 |  ROLLUP '(' ordinary_grouping_set ')'    { $$ = _symbol_create_list(SQL_ROLLUP, $3); }
 |  CUBE '(' ordinary_grouping_set ')'      { $$ = _symbol_create_list(SQL_CUBE, $3); }
 |  GROUPING SETS '(' grouping_set_list ')' { $$ = _symbol_create_list(SQL_GROUPING_SETS, $4); }
 |  '(' ')'                                 { $$ = _symbol_create_list(SQL_GROUPBY, NULL); }
 ;

ordinary_grouping_set:
    ordinary_grouping_element                           { $$ = append_symbol(L(), $1); }
 |  ordinary_grouping_set ',' ordinary_grouping_element { $$ = append_symbol($1, $3); }
 ;

ordinary_grouping_element:
    '(' column_ref_commalist ')' { $$ = _symbol_create_list(SQL_COLUMN_GROUP, $2); }
 |  column_ref                   { $$ = _symbol_create_list(SQL_COLUMN, $1); }
 ;

column_ref_commalist:
    column_ref		                    { $$ = append_symbol(L(), _symbol_create_list(SQL_COLUMN,$1)); }
 |  column_ref_commalist ',' column_ref { $$ = append_symbol($1, _symbol_create_list(SQL_COLUMN,$3)); }
 ;

grouping_set_list:
	grouping_set_element                       { $$ = append_symbol(L(), $1); }
 |  grouping_set_list ',' grouping_set_element { $$ = append_symbol($1, $3); }
 ;

grouping_set_element:
    ordinary_grouping_element               { $$ = _symbol_create_list(SQL_GROUPBY, append_symbol(L(), $1)); }
 |  ROLLUP '(' ordinary_grouping_set ')'    { $$ = _symbol_create_list(SQL_ROLLUP, $3); }
 |  CUBE '(' ordinary_grouping_set ')'      { $$ = _symbol_create_list(SQL_CUBE, $3); }
 |  GROUPING SETS '(' grouping_set_list ')' { $$ = _symbol_create_list(SQL_GROUPING_SETS, $4); }
 |  '(' ')'                                 { $$ = _symbol_create_list(SQL_GROUPBY, NULL); }
 ;

opt_having_clause:
    /* empty */             { $$ = NULL; }
 |  HAVING search_condition { $$ = $2; }
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
			  $$ = _newAtomNode( atom_float(SA, t, strtod($2, NULL)));
			}
 |  SAMPLE param	{ $$ = $2; }
 ;

opt_seed:
	/* empty */	{ $$ = NULL; }
 |  SEED intval	{ 
		  	  sql_subtype *t = sql_bind_localtype("int");
			  $$ = _newAtomNode( atom_int(SA, t, $2)); 
			}
 |  SEED param	{ $$ = $2; }
 ;

sort_specification_list:
    ordering_spec	 { $$ = append_symbol(L(), $1); }
 |  sort_specification_list ',' ordering_spec
			 { $$ = append_symbol( $1, $3 ); }
 ;

ordering_spec:
    search_condition opt_asc_desc opt_nulls_first_last
	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, $2 | (($3 == -1 ? !$2 : $3) << 1));
	  $$ = _symbol_create_list(SQL_COLUMN, l ); }
 ;

opt_asc_desc:
    /* empty */ { $$ = TRUE; }
 |  ASC			{ $$ = TRUE; }
 |  DESC		{ $$ = FALSE; }
 ;

opt_nulls_first_last:
    /* empty */ 	{ $$ = -1; }
 |  NULLS LAST		{ $$ = TRUE; }
 |  NULLS FIRST		{ $$ = FALSE; }
 ;

predicate:
    comparison_predicate
 |  between_predicate
 |  like_predicate
 |  test_for_null
 |  in_predicate
 |  existence_test
 |  filter_exp
 |  scalar_exp
 ;

pred_exp:
    NOT pred_exp { $$ = _symbol_create_symbol(SQL_NOT, $2); }
 |  predicate	 { $$ = $1; }
 ;

opt_any_all_some:
    		{ $$ = -1; }
 |  ANY		{ $$ = 0; }
 |  SOME	{ $$ = 0; }
 |  ALL		{ $$ = 1; }
 ;

comparison_predicate:
    pred_exp COMPARISON opt_any_all_some pred_exp
		{ dlist *l = L();

		  append_symbol(l, $1);
		  append_string(l, $2);
		  append_symbol(l, $4);
		  if ($3 > -1)
		     append_int(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 |  pred_exp '=' opt_any_all_some pred_exp
		{ dlist *l = L();

		  append_symbol(l, $1);
		  append_string(l, sa_strdup(SA, "="));
		  append_symbol(l, $4);
		  if ($3 > -1)
		     append_int(l, $3);
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
 	{ const char *s = $3;
	  if (_strlen(s) != 1) {
		sqlformaterror(m, SQLSTATE(22019) "%s", "ESCAPE must be one character");
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
    pred_exp IS NOT sqlNULL { $$ = _symbol_create_symbol( SQL_IS_NOT_NULL, $1 );}
 |  pred_exp IS sqlNULL     { $$ = _symbol_create_symbol( SQL_IS_NULL, $1 ); }
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
 |  '(' with_query ')'			{ $$ = $2; }
 ;

subquery:
    '(' select_no_parens ')'	{ $$ = $2; }
 |  '(' with_query ')'		{ $$ = $2; }
 ;

	/* simple_scalar expressions */
simple_scalar_exp:
    value_exp
 |  scalar_exp '+' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_add")));
	  		  append_int(l, FALSE); /* ignore distinct */
			  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '-' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_sub")));
	  		  append_int(l, FALSE); /* ignore distinct */
			  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '*' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_mul")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '/' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_div")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '%' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "mod")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '^' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "bit_xor")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '&' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "bit_and")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
			  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp GEOM_OVERLAP scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_overlap")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp GEOM_OVERLAP_OR_LEFT scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_overlap_or_left")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp  GEOM_OVERLAP_OR_RIGHT scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_overlap_or_right")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp GEOM_OVERLAP_OR_BELOW scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_overlap_or_below")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp GEOM_BELOW scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "mbr_below")));
			  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp GEOM_OVERLAP_OR_ABOVE scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_overlap_or_above")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp GEOM_ABOVE scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_above")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 | scalar_exp GEOM_DIST scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_distance")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp AT scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_contained")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '|' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "bit_or")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '~' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_contains")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp GEOM_MBR_EQUAL scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(L(), sa_strdup(SA, "mbr_equal")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  '~' scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "bit_not")));
	  		  append_int(l, FALSE); /* ignore distinct */
			  append_symbol(l, $2);
	  		  $$ = _symbol_create_list( SQL_UNOP, l ); }
 |  scalar_exp LEFT_SHIFT scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "left_shift")));
			  	append_int(l, FALSE); /* ignore distinct */
				append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp RIGHT_SHIFT scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "right_shift")));
	  		   append_int(l, FALSE); /* ignore distinct */
			   append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp LEFT_SHIFT_ASSIGN scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "left_shift_assign")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp RIGHT_SHIFT_ASSIGN scalar_exp
			{ dlist *l = L();
			  append_list(l, 
			  	append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "right_shift_assign")));
				  append_int(l, FALSE); /* ignore distinct */
	  		  append_symbol(l, $1);
	  		  append_symbol(l, $3);
	  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  '+' scalar_exp %prec UMINUS 
			{ $$ = $2; }
 |  '-' scalar_exp %prec UMINUS 
			{ 
 			  $$ = NULL;
			  assert(($2->token != SQL_COLUMN && $2->token != SQL_IDENT) || $2->data.lval->h->type != type_lng);
			  if (!$$) {
				dlist *l = L();
			  	append_list(l, 
			  		append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_neg")));
	  		  	append_int(l, FALSE); /* ignore distinct */
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

opt_over:
	OVER '(' window_specification ')' { $$ = _symbol_create_list(SQL_WINDOW, append_list(L(), $3)); }
 |  OVER ident                        { $$ = _symbol_create_list(SQL_NAME, append_string(L(), $2)); }
 |                                    { $$ = NULL; }
 ;

value_exp:
    atom
 |  aggr_or_window_ref opt_over {
	 								if ($2 && $2->token == SQL_NAME)
										$$ = _symbol_create_list(SQL_WINDOW, append_string(append_symbol(L(), $1), $2->data.lval->h->data.sval));
									else if ($2)
										$$ = _symbol_create_list(SQL_WINDOW, append_list(append_symbol(L(), $1), $2->data.lval->h->data.lval));
									else
										$$ = $1;
 								}
 |  case_exp
 |  cast_exp
 |  column_ref       { $$ = _symbol_create_list(SQL_COLUMN, $1); }
 |  session_user     { $$ = _symbol_create_list(SQL_NAME, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_user"))); }
 |  CURRENT_SCHEMA   { $$ = _symbol_create_list(SQL_NAME, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_schema"))); }
 |  CURRENT_ROLE     { $$ = _symbol_create_list(SQL_NAME, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_role"))); }
 |  CURRENT_TIMEZONE { $$ = _symbol_create_list(SQL_NAME, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_timezone"))); }
 |  datetime_funcs
 |  GROUPING '(' column_ref_commalist ')' { dlist *l = L();
										    append_list(l, append_string(L(), "grouping"));
											append_int(l, FALSE); /* ignore distinct */
											for (dnode *dn = $3->h ; dn ; dn = dn->next) {
												symbol *sym = dn->data.sym; /* do like a aggrN */
												append_symbol(l, _symbol_create_list(SQL_COLUMN, sym->data.lval));
											}
										    $$ = _symbol_create_list(SQL_AGGR, l); }
 |  NEXT VALUE FOR qname                  { $$ = _symbol_create_list(SQL_NEXT, $4); }
 |  null
 |  param
 |  string_funcs
 |  XML_value_function
 ;

param:
   '?'
	{ 
	  int nr = (m->params)?list_length(m->params):0;

	  sql_add_param(m, NULL, NULL);
	  $$ = _symbol_create_int( SQL_PARAMETER, nr ); 
	}

window_specification:
	window_ident_clause window_partition_clause window_order_clause window_frame_clause
	{ $$ = append_symbol(append_symbol(append_symbol(append_string(L(), $1), $2), $3), $4); }
  ;

window_ident_clause:
	/* empty */ { $$ = NULL; }
  |	ident       { $$ = $1; }
  ;

search_condition_commalist:
    search_condition                                { $$ = append_symbol(L(), $1); }
 |  search_condition_commalist ',' search_condition { $$ = append_symbol($1, $3); }
 ;

window_partition_clause:
	/* empty */ 	{ $$ = NULL; }
  |	PARTITION BY search_condition_commalist
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
  |	GROUPS		{ $$ = FRAME_GROUPS; }
  ;

window_frame_extent:
	window_frame_start    { dlist *l = L(); append_symbol(l, $1);
                            symbol *s = _symbol_create_int( SQL_FOLLOWING, CURRENT_ROW_BOUND);
                            dlist *l2 = append_symbol(L(), s);
                            symbol *sym = _symbol_create_list( SQL_CURRENT_ROW, l2);
                            append_symbol(l, sym);
                            $$ = l; }
  | window_frame_between  { $$ = $1; }
  ;

window_frame_start:
	UNBOUNDED PRECEDING   { symbol *s = _symbol_create_int( SQL_PRECEDING, UNBOUNDED_PRECEDING_BOUND);
                            dlist *l2 = append_symbol(L(), s);
                            $$ = _symbol_create_list( SQL_PRECEDING, l2); }
  | search_condition PRECEDING { dlist *l2 = append_symbol(L(), $1);
                            $$ = _symbol_create_list( SQL_PRECEDING, l2); }
  | CURRENT ROW           { symbol *s = _symbol_create_int( SQL_PRECEDING, CURRENT_ROW_BOUND);
                            dlist *l = append_symbol(L(), s);
                            $$ = _symbol_create_list( SQL_CURRENT_ROW, l); }
  ;

window_bound:
	window_frame_start
  | window_following_bound
  ;

window_frame_between:
	BETWEEN window_bound AND window_bound { $$ = append_symbol(append_symbol(L(), $2), $4); }
  ;

window_following_bound:
	UNBOUNDED FOLLOWING   { symbol *s = _symbol_create_int( SQL_FOLLOWING, UNBOUNDED_FOLLOWING_BOUND);
                            dlist *l2 = append_symbol(L(), s);
                            $$ = _symbol_create_list( SQL_FOLLOWING, l2); }
  | search_condition FOLLOWING { dlist *l2 = append_symbol(L(), $1);
                            $$ = _symbol_create_list( SQL_FOLLOWING, l2); }
  ;

window_frame_exclusion:
	/* empty */			{ $$ = EXCLUDE_NONE; }
  |	EXCLUDE CURRENT ROW	{ $$ = EXCLUDE_CURRENT_ROW; }
  |	EXCLUDE sqlGROUP	{ $$ = EXCLUDE_GROUP; }
  |	EXCLUDE TIES		{ $$ = EXCLUDE_TIES; }
  |	EXCLUDE NO OTHERS	{ $$ = EXCLUDE_NONE; }
  ;

func_ref:
    qfunc '(' ')'
	{ dlist *l = L();
  	  append_list(l, $1);
      append_int(l, FALSE); /* ignore distinct */
	  $$ = _symbol_create_list( SQL_OP, l ); }
|   qfunc '(' search_condition_commalist ')'
	{ dlist *l = L();
  	  append_list(l, $1);
	  append_int(l, FALSE); /* ignore distinct */
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
  		  	  append_int(l, FALSE); /* ignore distinct */
			  append_symbol(l, $5);
		  	  $$ = _symbol_create_list( SQL_UNOP, l ); }
 |  CURRENT_DATE opt_brackets
 			{ dlist *l = L();
			  append_list(l,
			  	append_string(L(), sa_strdup(SA, "current_date")));
			 append_int(l, FALSE); /* ignore distinct */
	  		  $$ = _symbol_create_list( SQL_OP, l ); }
 |  CURRENT_TIME opt_brackets
 			{ dlist *l = L();
			  append_list(l,
			  	append_string(L(), sa_strdup(SA, "current_time")));
			  append_int(l, FALSE); /* ignore distinct */
	  		  $$ = _symbol_create_list( SQL_OP, l ); }
 |  CURRENT_TIMESTAMP opt_brackets
 			{ dlist *l = L();
			  append_list(l,
			  	append_string(L(), sa_strdup(SA, "current_timestamp")));
			  append_int(l, FALSE); /* ignore distinct */
	  		  $$ = _symbol_create_list( SQL_OP, l ); }
 |  LOCALTIME opt_brackets
 			{ dlist *l = L();
			  append_list(l,
			  	append_string(L(), sa_strdup(SA, "localtime")));
			  append_int(l, FALSE); /* ignore distinct */
	  		  $$ = _symbol_create_list( SQL_OP, l ); }
 |  LOCALTIMESTAMP opt_brackets
 			{ dlist *l = L();
			  append_list(l,
			  	append_string(L(), sa_strdup(SA, "localtimestamp")));
			  append_int(l, FALSE); /* ignore distinct */
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
			  append_int(l, FALSE); /* ignore distinct */
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
			append_int(l, FALSE); /* ignore distinct */
  		  	  append_symbol(ops, $3);
  		  	  append_symbol(ops, $5);
  		  	  append_symbol(ops, $7);
			  append_list(l, ops);
		  	  $$ = _symbol_create_list( SQL_NOP, l ); }
  | SUBSTRING '(' scalar_exp FROM scalar_exp ')'
			{ dlist *l = L();
  		  	  append_list(l,
  		  	  	append_string(L(), sa_strdup(SA, "substring")));
					  append_int(l, FALSE); /* ignore distinct */
  		  	  append_symbol(l, $3);
  		  	  append_symbol(l, $5);
		  	  $$ = _symbol_create_list( SQL_BINOP, l ); }
  | SUBSTRING '(' scalar_exp ',' scalar_exp ')'
			{ dlist *l = L();
  		  	  append_list(l,
  		  	  	append_string(L(), sa_strdup(SA, "substring")));
					  append_int(l, FALSE); /* ignore distinct */
  		  	  append_symbol(l, $3);
  		  	  append_symbol(l, $5);
		  	  $$ = _symbol_create_list( SQL_BINOP, l ); }
  | POSITION '(' scalar_exp sqlIN scalar_exp ')'
			{ dlist *l = L();
  		  	  append_list(l,
  		  	  	append_string(L(), sa_strdup(SA, "locate")));
					  append_int(l, FALSE); /* ignore distinct */
  		  	  append_symbol(l, $3);
  		  	  append_symbol(l, $5);
		  	  $$ = _symbol_create_list( SQL_BINOP, l ); }
  | scalar_exp CONCATSTRING scalar_exp
			{ dlist *l = L();
  		  	  append_list(l,
  		  	  	append_string(L(), sa_strdup(SA, "concat")));
					  append_int(l, FALSE); /* ignore distinct */
  		  	  append_symbol(l, $1);
  		  	  append_symbol(l, $3);
		  	  $$ = _symbol_create_list( SQL_BINOP, l ); }
  | SPLIT_PART '(' scalar_exp ',' scalar_exp ',' scalar_exp ')'
			{ dlist *l = L();
			  dlist *ops = L();
  		  	  append_list(l,
				append_string(L(), sa_strdup(SA, "splitpart")));
				append_int(l, FALSE); /* ignore distinct */
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
 |  ident	{ $$ = $1; }
 ;

atom:
    literal
	{ 
		AtomNode *an = (AtomNode*)$1;
		atom *a = an->a; 
		an->a = atom_dup(SA, a); 
		$$ = $1;
	}
 ;

qrank:
	RANK		{ $$ = append_string(L(), $1); }
 |      ident '.' RANK	{ $$ = append_string(
			  append_string(L(), $1), $3);}
 ;

aggr_or_window_ref:
    qrank '(' ')'
		{ dlist *l = L();
  		  append_list(l, $1);
  		  append_int(l, FALSE); /* ignore distinct */
  		  append_list(l, NULL);
  		  $$ = _symbol_create_list( SQL_RANK, l ); }
 |  qrank '(' search_condition_commalist ')'
		{ dlist *l = L();
  		  append_list(l, $1);
  		  append_int(l, FALSE); /* ignore distinct */
  		  append_list(l, $3);
  		  $$ = _symbol_create_list( SQL_RANK, l ); }
 |  qrank '(' DISTINCT search_condition_commalist ')'
		{ dlist *l = L();
  		  append_list(l, $1);
  		  append_int(l, TRUE);
  		  append_list(l, $4);
  		  $$ = _symbol_create_list( SQL_RANK, l ); }
 |  qrank '(' ALL search_condition_commalist ')'
		{ dlist *l = L();
  		  append_list(l, $1);
  		  append_int(l, FALSE);
  		  append_list(l, $4);
  		  $$ = _symbol_create_list( SQL_RANK, l ); }
 |  qfunc '(' '*' ')'
		{ dlist *l = L();
  		  append_list(l, $1);
		  append_int(l, FALSE); /* ignore distinct */
  		  append_symbol(l, NULL);
		  $$ = _symbol_create_list( SQL_AGGR, l ); }
 |  qfunc '(' ident '.' '*' ')'
		{ dlist *l = L();
  		  append_list(l, $1);
		  append_int(l, FALSE); /* ignore distinct */
  		  append_symbol(l, NULL);
		  $$ = _symbol_create_list( SQL_AGGR, l ); }
 |  qfunc '(' ')'
		{ dlist *l = L();
  		  append_list(l, $1);
		  append_int(l, FALSE); /* ignore distinct */
		  append_list(l, NULL);
		  $$ = _symbol_create_list( SQL_OP, l ); }
 |  qfunc '(' search_condition_commalist ')'
		{ dlist *l = L();
		  append_list(l, $1);
		  append_int(l, FALSE); /* ignore distinct */
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
 |  qfunc '(' DISTINCT search_condition_commalist ')'
		{ dlist *l = L();
		  append_list(l, $1);
		  append_int(l, TRUE);
 		  if (dlist_length($4) == 1) {
		  	append_symbol(l, $4->h->data.sym);
			$$ = _symbol_create_list( SQL_UNOP, l ); 
		  } else if (dlist_length($4) == 2) {
		  	append_symbol(l, $4->h->data.sym);
		  	append_symbol(l, $4->h->next->data.sym);
			$$ = _symbol_create_list( SQL_BINOP, l ); 
		  } else {
		  	append_list(l, $4);
		  	$$ = _symbol_create_list( SQL_NOP, l ); 
		  }
		}
 |  qfunc '(' ALL search_condition_commalist ')'
		{ dlist *l = L();
		  append_list(l, $1);
		  append_int(l, FALSE);
 		  if (dlist_length($4) == 1) {
		  	append_symbol(l, $4->h->data.sym);
			$$ = _symbol_create_list( SQL_UNOP, l ); 
		  } else if (dlist_length($4) == 2) {
		  	append_symbol(l, $4->h->data.sym);
		  	append_symbol(l, $4->h->next->data.sym);
			$$ = _symbol_create_list( SQL_BINOP, l ); 
		  } else {
		  	append_list(l, $4);
		  	$$ = _symbol_create_list( SQL_NOP, l ); 
		  }
		}
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
 |  CENTURY		{ $$ = icentury; }
 |  DECADE		{ $$ = idecade; }
 |  QUARTER		{ $$ = iquarter; }
 |  WEEK		{ $$ = iweek; }
 |  DOW			{ $$ = idow; }
 /* |  DAY OF WEEK		{ $$ = idow; } */
 |  DOY			{ $$ = idoy; }
 /* |  DAY OF YEAR		{ $$ = idoy; } */
 |  EPOCH		{ $$ = iepoch; }
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
			sqlformaterror(m, SQLSTATE(22006) "%s", "incorrect interval");
			YYABORT;
	  	} else {
			int d = inttype2digits(sk, ek);
			if (tpe == 0){
				sql_find_subtype(&$$, "month_interval", d, 0);
			} else if (d == 4) {
				sql_find_subtype(&$$, "day_interval", d, 0);
			} else {
				sql_find_subtype(&$$, "sec_interval", d, 0);
			}
	  	}
	}
 ;

session_user:
    USER 
 |  SESSION_USER
 |  CURRENT_USER 
 ;

session_timezone:
    TIME ZONE
 |  CURRENT_TIMEZONE
 ;

session_schema:
    SCHEMA
 |  CURRENT_SCHEMA
 ;

session_role:
    ROLE
 |  CURRENT_ROLE
 ;

literal:
    string 	{ const char *s = $1;
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
				if (isdigit((unsigned char) hexa[i]))
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
			else if (res <= GDK_hge_max)
				sql_find_subtype(&t, "hugeint", 128, 0 );
#endif
			else
				err = 1;
		  }

		  if (err != 0) {
			sqlformaterror(m, SQLSTATE(22003) "Invalid hexadecimal number or hexadecimal too large (%s)", $1);
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

		  if (lngFromStr($1, &len, &p, false) < 0 || is_lng_nil(value))
		  	err = 2;

		  if (!err) {
		    if (value >= (lng) GDK_oid_min && value <= (lng) GDK_oid_max)
#if SIZEOF_OID == SIZEOF_INT
		  	  sql_find_subtype(&t, "oid", 31, 0 );
#else
		  	  sql_find_subtype(&t, "oid", 63, 0 );
#endif
		    else
			  err = 1;
		  }

		  if (err) {
			sqlformaterror(m, SQLSTATE(22003) "OID value too large or not a number (%s)", $1);
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
		  if (hgeFromStr($1, &len, &p, false) < 0 || is_hge_nil(value))
		  	err = 2;
#else
		  if (lngFromStr($1, &len, &p, false) < 0 || is_lng_nil(value))
		  	err = 2;
#endif

		  /* find the most suitable data type for the given number */
		  if (!err) {
		    int bits = (int) digits2bits(digits), obits = bits;

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
		    else if (value >= GDK_hge_min && value <= GDK_hge_max)
		  	  sql_find_subtype(&t, "hugeint", bits, 0 );
#endif
		    else
			  err = 1;
		  }

		  if (err) {
			sqlformaterror(m, SQLSTATE(22003) "integer value too large or not a number (%s)", $1);
			$$ = NULL;
			YYABORT;
		  } else {
		  	$$ = _newAtomNode( atom_int(SA, &t, value));
		  }
		}
 |  INTNUM
		{ char *s = sa_strdup(SA, $1);
			int digits;
			int scale;
			int has_errors;
			sql_subtype t;

			DEC_TPE value = decimal_from_str(s, &digits, &scale, &has_errors);

			if (!has_errors && digits <= MAX_DEC_DIGITS) {
				// The float-like value seems to fit in decimal storage
				sql_find_subtype(&t, "decimal", digits, scale );
				$$ = _newAtomNode( atom_dec(SA, &t, value));
			}
			else {
				/*
				* The float-like value either doesn't fit in integer decimal storage
				* or it is not a valid float representation.
				*/
				char *p = $1;
				double val;

				errno = 0;
				val = strtod($1,&p);
				if (p == $1 || is_dbl_nil(val) || (errno == ERANGE && (val < -1 || val > 1))) {
					sqlformaterror(m, SQLSTATE(22003) "Double value too large or not a number (%s)", $1);
					$$ = NULL;
					YYABORT;
				} else {
					sql_find_subtype(&t, "double", 51, 0 );
					$$ = _newAtomNode(atom_float(SA, &t, val));
				}
		   }
		}
 |  APPROXNUM
		{ sql_subtype t;
  		  char *p = $1;
		  double val;

		  errno = 0;
 		  val = strtod($1,&p);
		  if (p == $1 || is_dbl_nil(val) || (errno == ERANGE && (val < -1 || val > 1))) {
			sqlformaterror(m, SQLSTATE(22003) "Double value too large or not a number (%s)", $1);
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
			sqlformaterror(m, SQLSTATE(22007) "Incorrect date value (%s)", $2);
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
			sqlformaterror(m, SQLSTATE(22007) "Incorrect time value (%s)", $4);
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
			sqlformaterror(m, SQLSTATE(22007) "Incorrect timestamp value (%s)", $4);
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
			sqlformaterror(m, SQLSTATE(22M28) "incorrect blob %s", $2);
			YYABORT;
		  }
		}
 |  blobstring
		{ sql_subtype t;
		  atom *a= 0;
		  int r;

		  $$ = NULL;
 		  r = sql_find_subtype(&t, "blob", 0, 0);
	          if (r && (a = atom_general(SA, &t, $1)) != NULL)
			$$ = _newAtomNode(a);
		  if (!$$) {
			sqlformaterror(m, SQLSTATE(22M28) "incorrect blob %s", $1);
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
			sqlformaterror(m, SQLSTATE(22000) "incorrect %s %s", $1, $2);
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
			sqlformaterror(m, SQLSTATE(22000) "incorrect %s %s", $1, $2);
			YYABORT;
		  }
		}
 | ident_or_uident string
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
			sqlformaterror(m, SQLSTATE(22000) "type (%s) unknown", $1);
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
			} else if (d == 4) {
				r=sql_find_subtype(&t, "day_interval", d, 0);
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
				sqlformaterror(m, SQLSTATE(22006) "incorrect interval (" LLFMT " > %d)", inlen, t.digits);
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

variable_ref:
    ident		{ $$ = append_string(
				L(), $1); }

 |  ident '.' ident	{ $$ = append_string(
				append_string(
				 L(), $1), $3);}
 ;

cast_exp:
     CAST '(' search_condition AS data_type ')'
 	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_type(l, &$5);
	  $$ = _symbol_create_list( SQL_CAST, l ); }
 |
     CONVERT '(' search_condition ',' data_type ')'
 	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_type(l, &$5);
	  $$ = _symbol_create_list( SQL_CAST, l ); }
 ;

case_exp:
     NULLIF '(' search_condition ',' search_condition ')'
		{ $$ = _symbol_create_list(SQL_NULLIF,
		   append_symbol(
		    append_symbol(
		     L(), $3), $5)); }
 |   COALESCE '(' case_search_condition_commalist ')'
		{ $$ = _symbol_create_list(SQL_COALESCE, $3); }
 |   CASE search_condition when_value_list case_opt_else END
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

case_search_condition_commalist: /* at least 2 search_condition (or null) */
    search_condition ',' search_condition
			{ $$ = append_symbol( L(), $1);
			  $$ = append_symbol( $$, $3);
			}
 |  case_search_condition_commalist ',' search_condition
			{ $$ = append_symbol( $1, $3); }
 ;

when_value:
    WHEN search_condition THEN search_condition
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
    WHEN search_condition THEN search_condition
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
 |  ELSE search_condition	{ $$ = $2; }
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
				sqlformaterror(m, SQLSTATE(22003) "Decimal of %d digits are not supported", d);
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
				if (s > d)
					sqlformaterror(m, SQLSTATE(22003) "Scale (%d) should be less or equal to the precision (%d)", s, d);
				else
					sqlformaterror(m, SQLSTATE(22003) "Decimal(%d,%d) isn't supported because P=%d > %d", d, s, d, MAX_DEC_DIGITS);
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
				sqlformaterror(m, SQLSTATE(22003) "Number of digits for FLOAT values should be between 1 and 53");
				$$.type = NULL;
				YYABORT;
			  }
			}
 |  sqlFLOAT '(' intval ',' intval ')'
			{ if ($5 >= $3) {
				sqlformaterror(m, SQLSTATE(22003) "Precision(%d) should be less than number of digits(%d)", $5, $3);
				$$.type = NULL;
				YYABORT;
			  } else if ($3 > 0 && $3 <= 24) {
				sql_find_subtype(&$$, "real", $3, $5);
			  } else if ($3 > 24 && $3 <= 53) {
				sql_find_subtype(&$$, "double", $3, $5);
			  } else {
				sqlformaterror(m, SQLSTATE(22003) "Number of digits for FLOAT values should be between 1 and 53");
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
				sqlformaterror(m, SQLSTATE(22003) "Precision(%d) should be less than number of digits(%d)", $5, $3);
				$$.type = NULL;
				YYABORT;
			  } else {
			 	sql_find_subtype(&$$, $1, $3, $5);
			  }
			}
 | ident_or_uident	{
			  sql_type *t = mvc_bind_type(m, $1);
			  if (!t) {
				sqlformaterror(m, SQLSTATE(22000) "Type (%s) unknown", $1);
				$$.type = NULL;
				YYABORT;
			  } else {
				sql_init_subtype(&$$, t, 0, 0);
			  }
			}

 | ident_or_uident '(' nonzero ')'
			{
			  sql_type *t = mvc_bind_type(m, $1);
			  if (!t) {
				sqlformaterror(m, SQLSTATE(22000) "Type (%s) unknown", $1);
				$$.type = NULL;
				YYABORT;
			  } else {
				sql_init_subtype(&$$, t, $3, 0);
			  }
			}
| GEOMETRY {
		if (!sql_find_subtype(&$$, "geometry", 0, 0 )) {
			sqlformaterror(m, "%s", SQLSTATE(22000) "type (geometry) unknown");
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
			sqlformaterror(m, SQLSTATE(22000) "Type (%s) unknown", $1);
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
			sqlformaterror(m, SQLSTATE(22000) "Type (%s) unknown", $1);
			$$.type = NULL;
			YYABORT;
		}
	}
| GEOMETRYA {
		if (!sql_find_subtype(&$$, "geometrya", 0, 0 )) {
			sqlformaterror(m, "%s", SQLSTATE(22000) "type (geometrya) unknown");
			$$.type = NULL;
			YYABORT;
		}
	}
| GEOMETRYSUBTYPE {
	int geoSubType = find_subgeometry_type(m, $1);

	if(geoSubType == 0) {
		$$.type = NULL;
		sqlformaterror(m, SQLSTATE(22000) "Type (%s) unknown", $1);
		YYABORT;
	} else if (geoSubType == -1) {
		$$.type = NULL;
		sqlformaterror(m, SQLSTATE(HY013) "%s", "allocation failure");
		YYABORT;
	}  else if (!sql_find_subtype(&$$, "geometry", geoSubType, 0 )) {
		sqlformaterror(m, SQLSTATE(22000) "Type (%s) unknown", $1);
		$$.type = NULL;
		YYABORT;
	}
}
 ;

subgeometry_type:
  GEOMETRYSUBTYPE {
	int subtype = find_subgeometry_type(m, $1);
	char* geoSubType = $1;

	if(subtype == 0) {
		sqlformaterror(m, SQLSTATE(22000) "Type (%s) unknown", geoSubType);
		YYABORT;
	} else if(subtype == -1) {
		sqlformaterror(m, SQLSTATE(HY013) "%s", "allocation failure");
		YYABORT;
	} 
	$$ = subtype;	
}
| string {
	int subtype = find_subgeometry_type(m, $1);
	char* geoSubType = $1;

	if(subtype == 0) {
		sqlformaterror(m, SQLSTATE(22000) "Type (%s) unknown", geoSubType);
		YYABORT;
	} else if (subtype == -1) {
		sqlformaterror(m, SQLSTATE(HY013) "%s", "allocation failure");
		YYABORT;
	} 
	$$ = subtype;	
}
;

type_alias:
 ALIAS
	{ 	char *t = sql_bind_alias($1);
	  	if (!t) {
			sqlformaterror(m, SQLSTATE(22000) "Type (%s) unknown", $1);
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

calc_restricted_ident:
    IDENT	{ $$ = $1; }
 |  UIDENT opt_uescape
		{ $$ = uescape_xform($1, $2); }
 |  aTYPE	{ $$ = $1; }
 |  ALIAS	{ $$ = $1; }
 |  RANK	{ $$ = $1; }	/* without '(' */
 ;

restricted_ident:
	calc_restricted_ident
	{
		$$ = $1;
		if (!$1 || _strlen($1) == 0) {
			sqlformaterror(m, SQLSTATE(42000) "An identifier cannot be empty");
			YYABORT;
		}
	}
 ;

calc_ident:
    IDENT	{ $$ = $1; }
 |  UIDENT opt_uescape
		{ $$ = uescape_xform($1, $2); }
 |  aTYPE	{ $$ = $1; }
 |  ALIAS	{ $$ = $1; }
 |  RANK	{ $$ = $1; }	/* without '(' */
 |  non_reserved_word
 ;

ident:
	calc_ident
	{
		$$ = $1;
		if (!$1 || _strlen($1) == 0) {
			sqlformaterror(m, SQLSTATE(42000) "An identifier cannot be empty");
			YYABORT;
		}
	}
 ;

non_reserved_word:
  AS		{ $$ = sa_strdup(SA, "as"); }		/* sloppy: officially reserved */
| AUTHORIZATION	{ $$ = sa_strdup(SA, "authorization"); }/* sloppy: officially reserved */
| COLUMN	{ $$ = sa_strdup(SA, "column"); }	/* sloppy: officially reserved */
| CYCLE		{ $$ = sa_strdup(SA, "cycle"); }	/* sloppy: officially reserved */
| sqlDATE	{ $$ = sa_strdup(SA, "date"); }		/* sloppy: officially reserved */
| DEALLOCATE    { $$ = sa_strdup(SA, "deallocate"); }	/* sloppy: officially reserved */
| DISTINCT	{ $$ = sa_strdup(SA, "distinct"); }	/* sloppy: officially reserved */
| EXEC		{ $$ = sa_strdup(SA, "exec"); }		/* sloppy: officially reserved */
| EXECUTE	{ $$ = sa_strdup(SA, "execute"); }	/* sloppy: officially reserved */
| FILTER	{ $$ = sa_strdup(SA, "filter"); }	/* sloppy: officially reserved */
| INTERVAL	{ $$ = sa_strdup(SA, "interval"); }	/* sloppy: officially reserved */
| LANGUAGE	{ $$ = sa_strdup(SA, "language"); }	/* sloppy: officially reserved */
| LARGE		{ $$ = sa_strdup(SA, "large"); }	/* sloppy: officially reserved */
| MATCH		{ $$ = sa_strdup(SA, "match"); }	/* sloppy: officially reserved */
| NO		{ $$ = sa_strdup(SA, "no"); }		/* sloppy: officially reserved */
| PRECISION 	{ $$ = sa_strdup(SA, "precision"); }	/* sloppy: officially reserved */
| PREPARE	{ $$ = sa_strdup(SA, "prepare"); }	/* sloppy: officially reserved */
| RELEASE	{ $$ = sa_strdup(SA, "release"); }	/* sloppy: officially reserved */
| ROW		{ $$ = sa_strdup(SA, "row"); }		/* sloppy: officially reserved */
| START		{ $$ = sa_strdup(SA, "start"); }	/* sloppy: officially reserved */
| TABLE		{ $$ = sa_strdup(SA, "table"); } 	/* sloppy: officially reserved */
| TIME 		{ $$ = sa_strdup(SA, "time"); }		/* sloppy: officially reserved */
| TIMESTAMP	{ $$ = sa_strdup(SA, "timestamp"); }	/* sloppy: officially reserved */
| UESCAPE	{ $$ = sa_strdup(SA, "uescape"); }	/* sloppy: officially reserved */
| VALUE		{ $$ = sa_strdup(SA, "value"); }	/* sloppy: officially reserved */

| ACTION	{ $$ = sa_strdup(SA, "action"); }
| ANALYZE	{ $$ = sa_strdup(SA, "analyze"); }
| AUTO_COMMIT	{ $$ = sa_strdup(SA, "auto_commit"); }
| BIG	{ $$ = sa_strdup(SA, "big"); }
| CACHE		{ $$ = sa_strdup(SA, "cache"); }
| CENTURY	{ $$ = sa_strdup(SA, "century"); }
| CLIENT	{ $$ = sa_strdup(SA, "client"); }
| COMMENT	{ $$ = sa_strdup(SA, "comment"); }
| DATA 		{ $$ = sa_strdup(SA, "data"); }
| DECADE	{ $$ = sa_strdup(SA, "decade"); }
| ENDIAN		{ $$ = sa_strdup(SA, "endian"); }
| EPOCH		{ $$ = sa_strdup(SA, "epoch"); }
| SQL_DEBUG	{ $$ = sa_strdup(SA, "debug"); }
| DIAGNOSTICS 	{ $$ = sa_strdup(SA, "diagnostics"); }
| SQL_EXPLAIN	{ $$ = sa_strdup(SA, "explain"); }
| FIRST		{ $$ = sa_strdup(SA, "first"); }
| GEOMETRY	{ $$ = sa_strdup(SA, "geometry"); }
| IMPRINTS	{ $$ = sa_strdup(SA, "imprints"); }
| INCREMENT	{ $$ = sa_strdup(SA, "increment"); }
| KEY		{ $$ = sa_strdup(SA, "key"); }
| LAST		{ $$ = sa_strdup(SA, "last"); }
| LEVEL		{ $$ = sa_strdup(SA, "level"); }
| LITTLE		{ $$ = sa_strdup(SA, "little"); }
| MAXVALUE	{ $$ = sa_strdup(SA, "maxvalue"); }
| MINMAX	{ $$ = sa_strdup(SA, "MinMax"); }
| MINVALUE	{ $$ = sa_strdup(SA, "minvalue"); }
| sqlNAME	{ $$ = sa_strdup(SA, "name"); }
| NATIVE		{ $$ = sa_strdup(SA, "native"); }
| NULLS		{ $$ = sa_strdup(SA, "nulls"); }
| OBJECT	{ $$ = sa_strdup(SA, "object"); }
| OPTIONS	{ $$ = sa_strdup(SA, "options"); }
| PASSWORD	{ $$ = sa_strdup(SA, "password"); }
| PATH		{ $$ = sa_strdup(SA, "path"); }
| SQL_PLAN	{ $$ = sa_strdup(SA, "plan"); }
| PREP		{ $$ = sa_strdup(SA, "prep"); }
| PRIVILEGES	{ $$ = sa_strdup(SA, "privileges"); }
| QUARTER	{ $$ = sa_strdup(SA, "quarter"); }
| REPLACE	{ $$ = sa_strdup(SA, "replace"); }
| ROLE		{ $$ = sa_strdup(SA, "role"); }
| SCHEMA	{ $$ = sa_strdup(SA, "schema"); }
| SERVER	{ $$ = sa_strdup(SA, "server"); }
| sqlSESSION	{ $$ = sa_strdup(SA, "session"); }
| sqlSIZE	{ $$ = sa_strdup(SA, "size"); }
| STATEMENT	{ $$ = sa_strdup(SA, "statement"); }
| STORAGE	{ $$ = sa_strdup(SA, "storage"); }
| TEMP		{ $$ = sa_strdup(SA, "temp"); }
| TEMPORARY	{ $$ = sa_strdup(SA, "temporary"); }
| sqlTEXT	{ $$ = sa_strdup(SA, "text"); }
| SQL_TRACE	{ $$ = sa_strdup(SA, "trace"); }
| TYPE		{ $$ = sa_strdup(SA, "type"); }
| WEEK 		{ $$ = sa_strdup(SA, "week"); }
| DOW 		{ $$ = sa_strdup(SA, "dow"); }
| DOY 		{ $$ = sa_strdup(SA, "doy"); }
| ZONE		{ $$ = sa_strdup(SA, "zone"); }

/* SQL/XML non reserved words */
| ABSENT	{ $$ = sa_strdup(SA, "absent"); }
| ACCORDING	{ $$ = sa_strdup(SA, "according"); }
| CONTENT	{ $$ = sa_strdup(SA, "content"); }
| DOCUMENT	{ $$ = sa_strdup(SA, "document"); }
| ELEMENT	{ $$ = sa_strdup(SA, "element"); }
| EMPTY		{ $$ = sa_strdup(SA, "empty"); }
| ID		{ $$ = sa_strdup(SA, "id"); }
| LOCATION	{ $$ = sa_strdup(SA, "location"); }
| NAMESPACE	{ $$ = sa_strdup(SA, "namespace"); }
| NIL		{ $$ = sa_strdup(SA, "nil"); }
| PASSING	{ $$ = sa_strdup(SA, "passing"); }
| REF		{ $$ = sa_strdup(SA, "ref"); }
| RETURNING	{ $$ = sa_strdup(SA, "returning"); }
| STRIP		{ $$ = sa_strdup(SA, "strip"); }
| URI		{ $$ = sa_strdup(SA, "uri"); }
| WHITESPACE	{ $$ = sa_strdup(SA, "whitespace"); }
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
			errno = 0;
			sqlformaterror(m, SQLSTATE(22003) "Integer value too large or not a number (%s)", $1);
			$$ = 0;
			YYABORT;
		  }
		}

ident_or_uident:
	IDENT			{ $$ = $1; }
    |	UIDENT opt_uescape	{ $$ = uescape_xform($1, $2); }
    ;

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
			errno = 0;
			sqlformaterror(m, SQLSTATE(22003) "Integer value too large or not a number (%s)", $1);
			$$ = 0;
			YYABORT;
		  }
		}
 ;

opt_uescape:
/* empty */	{ $$ = "\\"; }
| UESCAPE string
		{ char *s = $2;
		  if (strlen(s) != 1 || strchr("\"'0123456789abcdefABCDEF+ \t\n\r\f", *s) != NULL) {
			sqlformaterror(m, SQLSTATE(22019) "%s", "UESCAPE must be one character");
			$$ = NULL;
			YYABORT;
		  } else {
			$$ = s;
		  }
		}

ustring:
    USTRING
		{ $$ = $1; }
 |  USTRING sstring
		{ $$ = sa_strconcat(SA, $1, $2); }
 ;

blobstring:
    XSTRING	/* X'<hexit>...' */
		{ $$ = $1; }
 |  XSTRING sstring
		{ $$ = sa_strconcat(SA, $1, $2); }
 ;

sstring:
    STRING
		{ $$ = $1; }
 |  STRING sstring
		{ $$ = sa_strconcat(SA, $1, $2); }
 ;

string:
   sstring	{ $$ = $1; }
 | ustring opt_uescape
		{ $$ = uescape_xform($1, $2);
		  if ($$ == NULL) {
			sqlformaterror(m, SQLSTATE(22019) "%s", "Bad Unicode string");
			YYABORT;
		  }
		}
 ;

exec:
     execute exec_ref
		{ $$ = _symbol_create_symbol(SQL_CALL, $2); }
 ;

dealloc_ref:
   posint { $$ = $1; }
 | ALL    { $$ = -1; } /* prepared statements numbers cannot be negative, so set -1 to deallocate all */
 ;

dealloc:
     deallocate opt_prepare dealloc_ref
		{
		  m->emode = m_deallocate;
		  $$ = _newAtomNode(atom_int(SA, sql_bind_localtype("int"), $3)); }
 ;

exec_ref:
    posint '(' ')'
	{ dlist *l = L();
  	  append_int(l, $1);
	  append_int(l, FALSE); /* ignore distinct */
  	  append_list(l, NULL);
	  $$ = _symbol_create_list( SQL_NOP, l ); }
|   posint '(' value_commalist ')'
	{ dlist *l = L();
  	  append_int(l, $1);
  	  append_int(l, FALSE); /* ignore distinct */
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

schema_name_list: ident_commalist ;

comment_on_statement:
	COMMENT ON catalog_object IS string
	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_string(l, $5);
	  $$ = _symbol_create_list( SQL_COMMENT, l );
	}
	| COMMENT ON catalog_object IS sqlNULL
	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_string(l, NULL);
	  $$ = _symbol_create_list( SQL_COMMENT, l );
	}
	;

catalog_object:
	  SCHEMA ident { $$ = _symbol_create( SQL_SCHEMA, $2 ); }
	| TABLE qname { $$ = _symbol_create_list( SQL_TABLE, $2 ); }
	| VIEW qname { $$ = _symbol_create_list( SQL_VIEW, $2 ); }
	| COLUMN ident '.' ident
	{ dlist *l = L();
	  append_string(l, $2);
	  append_string(l, $4);
	  $$ = _symbol_create_list( SQL_COLUMN, l );
	}
	| COLUMN ident '.' ident '.' ident
	{ dlist *l = L();
	  append_string(l, $2);
	  append_string(l, $4);
	  append_string(l, $6);
	  $$ = _symbol_create_list( SQL_COLUMN, l );
	}
	| INDEX qname { $$ = _symbol_create_list( SQL_INDEX, $2 ); }
	| SEQUENCE qname { $$ = _symbol_create_list( SQL_SEQUENCE, $2 ); }
	| routine_designator { $$ = _symbol_create_list( SQL_ROUTINE, $1 ); }
	;

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
				{ dlist *l = L();
	  			  append_string(l, sa_strconcat(SA, "xmlns:", $3));
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
      opt_XML_returning_clause ')'
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
	  append_int(aggr, FALSE); /* ignore distinct */
	  append_symbol(aggr, $3);
	  /* int returning not used */
	  $$ = _symbol_create_list( SQL_AGGR, aggr);
	}
 ;

%%
int find_subgeometry_type(mvc *m, char* geoSubType) {
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
			char *typeSubStr = SA_NEW_ARRAY(m->ta, char, strLength);
			char flag = geoSubType[strLength-1]; 

			if (typeSubStr == NULL) {
				return -1;
			}
			memcpy(typeSubStr, geoSubType, strLength-1);
			typeSubStr[strLength-1]='\0';
			if(flag == 'z' || flag == 'm' ) {
				subType = find_subgeometry_type(m, typeSubStr);
				if (subType == -1) {
					return -1;
				}
				if(flag == 'z')
					SET_Z(subType);
				if(flag == 'm')
					SET_M(subType);
			}
		}
	}
	return subType;	
}

char *token2string(tokens token)
{
	switch (token) {
	// Please keep this list sorted for easy of maintenance
#define SQL(TYPE) case SQL_##TYPE : return #TYPE
	SQL(AGGR);
	SQL(ALTER_SEQ);
	SQL(ALTER_TABLE);
	SQL(ALTER_USER);
	SQL(ANALYZE);
	SQL(AND);
	SQL(ASSIGN);
	SQL(ATOM);
	SQL(BETWEEN);
	SQL(BINCOPYFROM);
	SQL(BINOP);
	SQL(CACHE);
	SQL(CALL);
	SQL(CASE);
	SQL(CAST);
	SQL(CHARSET);
	SQL(CHECK);
	SQL(COALESCE);
	SQL(COLUMN);
	SQL(COLUMN_GROUP);
	SQL(COLUMN_OPTIONS);
	SQL(COMMENT);
	SQL(COMPARE);
	SQL(CONSTRAINT);
	SQL(COPYFROM);
	SQL(COPYLOADER);
	SQL(COPYTO);
	SQL(CREATE_FUNC);
	SQL(CREATE_INDEX);
	SQL(CREATE_ROLE);
	SQL(CREATE_SCHEMA);
	SQL(CREATE_SEQ);
	SQL(CREATE_TABLE);
	SQL(CREATE_TABLE_LOADER);
	SQL(CREATE_TRIGGER);
	SQL(CREATE_TYPE);
	SQL(CREATE_USER);
	SQL(CREATE_VIEW);
	SQL(CROSS);
	SQL(CUBE);
	SQL(CURRENT_ROW);
	SQL(CYCLE);
	SQL(DECLARE);
	SQL(DECLARE_TABLE);
	SQL(DEFAULT);
	SQL(DELETE);
	SQL(DROP_COLUMN);
	SQL(DROP_CONSTRAINT);
	SQL(DROP_DEFAULT);
	SQL(DROP_FUNC);
	SQL(DROP_INDEX);
	SQL(DROP_ROLE);
	SQL(DROP_SCHEMA);
	SQL(DROP_SEQ);
	SQL(DROP_TABLE);
	SQL(DROP_TRIGGER);
	SQL(DROP_TYPE);
	SQL(DROP_USER);
	SQL(DROP_VIEW);
	SQL(ELSE);
	SQL(ESCAPE);
	SQL(EXCEPT);
	SQL(EXECUTE);
	SQL(EXISTS);
	SQL(FILTER);
	SQL(FOLLOWING);
	SQL(FOREIGN_KEY);
	SQL(FRAME);
	SQL(FROM);
	SQL(FUNC);
	SQL(GRANT);
	SQL(GRANT_ROLES);
	SQL(GROUPBY);
	SQL(GROUPING_SETS);
	SQL(IDENT);
	SQL(IF);
	SQL(IN);
	SQL(INC);
	SQL(INDEX);
	SQL(INSERT);
	SQL(INTERSECT);
	SQL(IS_NOT_NULL);
	SQL(IS_NULL);
	SQL(JOIN);
	SQL(LIKE);
	SQL(MAXVALUE);
	SQL(MERGE);
	SQL(MERGE_MATCH);
	SQL(MERGE_NO_MATCH);
	SQL(MERGE_PARTITION);
	SQL(MINVALUE);
	SQL(MULSTMT);
	SQL(NAME);
	SQL(NEXT);
	SQL(NOP);
	SQL(NOT);
	SQL(NOT_BETWEEN);
	SQL(NOT_EXISTS);
	SQL(NOT_IN);
	SQL(NOT_LIKE);
	SQL(NOT_NULL);
	SQL(NULL);
	SQL(NULLIF);
	SQL(OP);
	SQL(OR);
	SQL(ORDERBY);
	SQL(PARAMETER);
	SQL(PARTITION_COLUMN);
	SQL(PARTITION_EXPRESSION);
	SQL(PARTITION_LIST);
	SQL(PARTITION_RANGE);
	SQL(PATH);
	SQL(PRECEDING);
	SQL(PREP);
	SQL(PRIMARY_KEY);
	SQL(PW_ENCRYPTED);
	SQL(PW_UNENCRYPTED);
	SQL(RANK);
	SQL(RENAME_COLUMN);
	SQL(RENAME_SCHEMA);
	SQL(RENAME_TABLE);
	SQL(RENAME_USER);
	SQL(RETURN);
	SQL(REVOKE);
	SQL(REVOKE_ROLES);
	SQL(ROLLUP);
	SQL(ROUTINE);
	SQL(SCHEMA);
	SQL(SELECT);
	SQL(SEQUENCE);
	SQL(SET);
	SQL(SET_TABLE_SCHEMA);
	SQL(START);
	SQL(STORAGE);
	SQL(TABLE);
	SQL(TRUNCATE);
	SQL(TYPE);
	SQL(UNION);
	SQL(UNIQUE);
	SQL(UNOP);
	SQL(UPDATE);
	SQL(USING);
	SQL(VALUES);
	SQL(VIEW);
	SQL(WHEN);
	SQL(WHILE);
	SQL(WINDOW);
	SQL(WITH);
	SQL(XMLATTRIBUTE);
	SQL(XMLCOMMENT);
	SQL(XMLCONCAT);
	SQL(XMLDOCUMENT);
	SQL(XMLELEMENT);
	SQL(XMLFOREST);
	SQL(XMLPARSE);
	SQL(XMLPI);
	SQL(XMLTEXT);
#define TR(TYPE) case TR_##TYPE : return #TYPE
	TR(COMMIT);
	TR(MODE);
	TR(RELEASE);
	TR(ROLLBACK);
	TR(SAVEPOINT);
	TR(START);
	// Please keep this list sorted for easy of maintenance
	}
	return "unknown";	/* just needed for broken compilers ! */
}

void *sql_error( mvc * sql, int error_code, char *format, ... )
{
	va_list	ap;

	va_start (ap,format);
	if (sql->errstr[0] == '\0' || error_code == 5 || error_code == ERR_NOTFOUND)
		vsnprintf(sql->errstr, ERRSIZE-1, _(format), ap);
	if (!sql->session->status || error_code == 5 || error_code == ERR_NOTFOUND)
		sql->session->status = -error_code;
	va_end (ap);
	return NULL;
}

static int 
sqlformaterror(mvc * sql, _In_z_ _Printf_format_string_ const char *format, ...)
{
	va_list	ap;
	const char *sqlstate = NULL;
	size_t len = 0;

	va_start (ap,format);
	if (format && strlen(format) > 6 && format[5] == '!') {
		/* sql state provided */
		sqlstate = NULL;
	} else {
		/* default: Syntax error or access rule violation */
		sqlstate = SQLSTATE(42000);
	}
	//assert(sql->scanner.errstr == NULL);
	if (sql->errstr[0] == '\0') {
		if (sqlstate)
			len += snprintf(sql->errstr+len, ERRSIZE-1-len, "%s", sqlstate);
		len += vsnprintf(sql->errstr+len, ERRSIZE-1-len, _(format), ap);
		snprintf(sql->errstr+len, ERRSIZE-1-len, " in: \"%.80s\"\n", QUERY(sql->scanner));
	}
	if (!sql->session->status)
		sql->session->status = -4;
	va_end (ap);
	return 1;
}

static int 
sqlerror(mvc * sql, const char *err)
{
	return sqlformaterror(sql, "%s", err);
}

static void *ma_alloc(sql_allocator *sa, size_t sz)
{
	return sa_alloc(sa, sz);
}
static void ma_free(void *p)
{
	(void)p;
}
