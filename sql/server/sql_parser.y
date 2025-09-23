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

static void *sql_alloc(allocator *sa, size_t sz);
static void sql_free(void *p);
static inline symbol*
makeAtomNode(mvc *m, const char* type, const char* val, unsigned int digits, unsigned int scale, bool bind);

#include <unistd.h>
#include <string.h>

#define SA	m->sa
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

#define Malloc(sz) sql_alloc(m->ta,sz)
#define YYMALLOC Malloc
#define YYFREE sql_free

#define YY_parse_LSP_NEEDED	/* needed for bison++ 1.21.11-3 */

#define SET_Z(info)(info = info | 0x02)
#define SET_M(info)(info = info | 0x01)

#ifdef HAVE_HGE
#define MAX_DEC_DIGITS 38
#define MAX_HEX_DIGITS 32
#define MAX_OCT_DIGITS 64 /* TODO */
#else
#define MAX_DEC_DIGITS 18
#define MAX_HEX_DIGITS 16
#define MAX_OCT_DIGITS 32 /* TODO */
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

static lng
size_unit(const char *suffix)
{
	if (suffix[0] == '\0')
		return 1;
	else if (strcasecmp("k", suffix) == 0)
		return 1000L;
	else if (strcasecmp("kib", suffix) == 0)
		return 1024L;
	else if (strcasecmp("m", suffix) == 0)
		return 1000L * 1000L;
	else if (strcasecmp("mib", suffix) == 0)
		return 1024L * 1024L;
	else if (strcasecmp("g", suffix) == 0)
		return 1000L * 1000L * 1000L;
	else if (strcasecmp("gib", suffix) == 0)
		return 1024L * 1024L * 1024L;
	else
		return -1;
}

static bool
looks_like_url(const char *text)
{
	if (text == NULL)
		return false;
	for (const char *p = text; *p != '\0'; p++) {
		if (*p == ':') {
			// Exclude :bla and c:\temp
			return p - text > 1;
		}
		if (*p < 'a' || *p > 'z') {
			return false;
		}
	}
	// we ran out of string looking for the colon
	return false;
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

/* only possible from bison 3.0 and up */
%define parse.error verbose

/* reentrant parser */
%define api.pure
%union {
	int		i_val,bval;
	lng		l_val,operation;
	lng		lpair[2];
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
	analyze_statement
	assignment
	atom
	call_statement
	case_exp
	case_arg
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
	check_parenthesis_open
	check_search_condition
	comment_on_statement
	control_statement
	import_stmt
	load_stmt
	copyfrom_stmt
	copybinfrom_stmt
	copyto_stmt
	create_statement
	create_statement_in_schema
	datetime_funcs
	dealloc
	declare_statement
	default
	default_value
	delete_stmt
	drop_statement
	drop_table_element
	exec
	exec_ref
	arg_list_ref
	named_arg_list_ref
	filter_exp
	forest_element_value
	func_data_type
	func_def
	func_def_opt_returns
	func_ref
	generated_column
	grant
	group_by_element
	grouping_set_element
	if_opt_else
	if_statement
	index_def
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
	opt_order_by_clause
	opt_within_group
	order_by_clause
	opt_over
	opt_partition_by
	opt_partition_spec
	opt_path_specification
	opt_sample
	sample
	opt_schema_default_char_set
	opt_search_condition
	opt_seed
	opt_seq_common_param
	opt_seq_param
	opt_table_name
	opt_when
	opt_where_clause
	opt_window_clause
	opt_qualify_clause
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
        poslng_or_param
        poslng_or_param_rows
	procedure_statement
	return_statement
	return_value
	revoke
	role_def
	scalar_exp
	scalar_exp_no_and
	schema
	schema_element
	select_no_parens
	select_with_parens
	seq_def
	set_statement
	simple_select
	values_clause
	select_clause
	sql
	SelectStmt
	sqlstmt
	string_funcs
	table_constraint
	table_constraint_type
	table_content_source
	table_def
	table_element
	table_name
	table_ref
	transaction_statement
	transaction_stmt
	trigger_def
	trigger_event
	trigger_procedure_statement
	truncate_stmt
	type_def
	update_statement
	update_stmt
	value_exp
	view_def
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
	opt_with_clause
	with_clause
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
	check_parenthesis_close
	clob
	column
	variable
	forest_element_name
	function_body
	grantee
	column_label
	column_id
	bare_column_label
	reduced_keywords
	ident
	non_reserved_keyword
	column_name_keyword
	function_name_keyword
	type_function_name
	reserved_keyword
	alias_name
	opt_alias_name
	opt_begin_label
	opt_constraint_name
	opt_end_label
	opt_forest_element_name
	opt_null_string
	opt_to_savepoint
	opt_using
	opt_XML_attribute_name
	restricted_ident
	sstring
	string
	user_schema
	opt_schema_path
	varchar
	window_ident_clause
	XML_attribute_name
	XML_element_name
	XML_namespace_prefix
	XML_PI_target
	opt_optimizer
	opt_default_role

%type <lpair>
	opt_max_memory_max_workers

%type <l>
	as_subquery_clause
	assignment_commalist
	authid_list
	case_opt_else_statement
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
	insert_rest
	interval_qualifier
	in_expr
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
	opt_header_list
	limit_clause
	opt_nr
	opt_paramlist
	opt_referencing_list
	opt_schema_element_list
	opt_seps
	opt_returning_clause
	opt_seq_params
	opt_typelist
	opt_with_encrypted_password
	ordinary_grouping_set
	paramlist
	params_list
	partition_list
	privileges
	procedure_statement_list
	qfunc
	qname
	routine_body
	procedure_body
	routine_designator
	expr_list
	schema_element_list
	schema_name_clause
	schema_name_list
	selection
	serial_opt_params
	single_datetime_field
	sort_specification_list
	string_commalist
	string_commalist_contents
	table_element_list
	table_exp
	table_function_column_list
	table_ref_commalist
	trigger_procedure_statement_list
	triggered_action
	triggered_statement
	trim_list
	typelist
	named_value_commalist
	variable_list
	variable_ref
	variable_ref_commalist
	variable_ref_commalist_parens
	into_clause
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
	opt_schema_details_list
	opt_qname

%type <i_val>
	_transaction_mode_list
	any_all_some
	check_identity
	datetime_field
	dealloc_ref
	document_or_content
	document_or_content_or_sequence
	drop_action
	extract_datetime_field
	func_def_type
	func_def_opt_order_spec
	func_def_type_no_proc
	global_privilege
	grantor
	intval
	join_type
	local_global_temp
	local_temp
	non_second_datetime_field
	nonzero
	asymmetric
	opt_column
	opt_encrypted
	opt_endianness
	opt_for_each
	opt_from_grantor
	opt_grantor
	opt_index_type
	opt_match
	opt_match_type
	opt_on_commit
	opt_outer
	opt_recursive
	opt_ref_action
	opt_sign
	opt_XML_content_option
	opt_XML_returning_clause
	XML_returning_clause
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
	iso_level
	transaction_mode_list
	trigger_action_time
	window_frame_exclusion
	window_frame_units
	with_or_without_data
	XML_content_option
	XML_whitespace_option
	max_workers


%type <l_val>
	lngval
	poslng
	nonzerolng
	max_memory

%type <bval>
	create
	create_or_replace
	if_exists
	if_not_exists
	table_if_not_exists
	opt_admin_for
	opt_asc_desc
	opt_brackets
	opt_chain
	all_distinct
	opt_distinct
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

%right <sval> STRING XSTRING
%right <sval> X_BODY

/* sql prefixes to avoid name clashes on various architectures */
%token <sval>
	IDENT aTYPE RANK MARGFUNC sqlINT OIDNUM HEXADECIMALNUM OCTALNUM BINARYNUM INTNUM APPROXNUM
	USING
	GLOBAL CAST CONVERT
	CHARACTER VARYING LARGE OBJECT VARCHAR CLOB sqlTEXT BINARY sqlBLOB
	sqlDECIMAL sqlFLOAT
	TINYINT SMALLINT BIGINT HUGEINT sqlINTEGER
	sqlDOUBLE sqlREAL PRECISION PARTIAL SIMPLE ACTION CASCADE RESTRICT
	sqlBOOL BOOL_FALSE BOOL_TRUE
	CURRENT_DATE CURRENT_TIMESTAMP CURRENT_TIME LOCALTIMESTAMP LOCALTIME
	BIG LITTLE NATIVE ENDIAN
	LEX_ERROR

/* the tokens used in geom */
%token <sval> GEOMETRY GEOMETRYSUBTYPE

%token <sval> ALL ANY SOME DATA
%token <sval> USER CURRENT_USER SESSION_USER LOCAL BEST EFFORT
%token <sval>  CURRENT_ROLE sqlSESSION CURRENT_SCHEMA CURRENT_TIMEZONE
%token <sval> sqlDELETE UPDATE SELECT INSERT MATCHED LOGIN
%token <sval> LATERAL LEFT RIGHT FULL OUTER NATURAL CROSS JOIN INNER
%token <sval> COMMIT ROLLBACK SAVEPOINT RELEASE WORK CHAIN NO PRESERVE ROWS
%token  START TRANSACTION READ WRITE ONLY ISOLATION LEVEL
%token  UNCOMMITTED COMMITTED sqlREPEATABLE SERIALIZABLE DIAGNOSTICS sqlSIZE STORAGE SNAPSHOT

%token <sval> ASYMMETRIC SYMMETRIC ORDER ORDERED BY IMPRINTS
%token <sval> ESCAPE UESCAPE HAVING sqlGROUP ROLLUP CUBE sqlNULL
%token <sval> GROUPING SETS FROM FOR MATCH

%token <sval> EXTRACT

/* sequence operations */
%token SEQUENCE INCREMENT RESTART CONTINUE
%token MAXVALUE MINVALUE CYCLE
%token NEXT VALUE CACHE
%token GENERATED ALWAYS IDENTITY
%token SERIAL BIGSERIAL AUTO_INCREMENT /* PostgreSQL and MySQL imitators */

/* SQL's terminator, the semi-colon */
%token SCOLON AT

/* SQL/XML tokens */
%token <sval> XMLCOMMENT XMLCONCAT XMLDOCUMENT XMLELEMENT XMLATTRIBUTES XMLFOREST
%token <sval> XMLPARSE STRIP WHITESPACE XMLPI XMLQUERY PASSING XMLTEXT
%token <sval> NIL REF ABSENT EMPTY DOCUMENT ELEMENT CONTENT XMLNAMESPACES NAMESPACE
%token <sval> XMLVALIDATE RETURNING LOCATION ID ACCORDING XMLSCHEMA URI XMLAGG
%token <sval> FILTER
%token <sval> CORRESPONDING

/* literal keyword tokens */
/*
CONTINUE CURRENT CURSOR FOUND GOTO GO LANGUAGE
SQLCODE SQLERROR UNDER WHENEVER
*/

%token TEMP TEMPORARY MERGE REMOTE REPLICA UNLOGGED
%token<sval> ASC DESC AUTHORIZATION
%token CHECK CONSTRAINT CREATE COMMENT NULLS FIRST LAST
%token TYPE PROCEDURE FUNCTION sqlLOADER AGGREGATE RETURNS EXTERNAL sqlNAME DECLARE
%token CALL LANGUAGE
%token ANALYZE SQL_EXPLAIN SQL_PLAN SQL_TRACE PREP PREPARE EXEC EXECUTE DEALLOCATE
%token DEFAULT DISTINCT DROP TRUNCATE
%token FOREIGN
%token RENAME ENCRYPTED UNENCRYPTED PASSWORD GRANT REVOKE ROLE ADMIN INTO
%token IS KEY ON OPTION OPTIONS
%token PATH PRIMARY PRIVILEGES
%token<sval> PUBLIC REFERENCES SCHEMA SET AUTO_COMMIT
%token RETURN
%token LEADING TRAILING BOTH

%token <sval> ALTER ADD TABLE COLUMN TO UNIQUE VALUES VIEW WHERE WITH WITHIN WITHOUT RECURSIVE
%token <sval> sqlDATE TIME TIMESTAMP INTERVAL
%token CENTURY DECADE YEAR QUARTER DOW DOY MONTH WEEK DAY HOUR MINUTE SECOND EPOCH ZONE
%token LIMIT OFFSET SAMPLE SEED FETCH
%token <sval> CASE WHEN THEN ELSE NULLIF COALESCE IFNULL IF ELSEIF WHILE DO
%token <sval> ATOMIC BEGIN END
%token <sval> COPY RECORDS DELIMITERS STDIN STDOUT FWF CLIENT SERVER
%token <sval> INDEX REPLACE

%token <sval> AS TRIGGER OF BEFORE AFTER ROW STATEMENT sqlNEW OLD EACH REFERENCING
%token <sval> OVER PARTITION CURRENT EXCLUDE FOLLOWING PRECEDING OTHERS TIES RANGE UNBOUNDED GROUPS WINDOW QUALIFY

%token X_BODY
%token MAX_MEMORY MAX_WORKERS OPTIMIZER
/* odbc tokens */
%token DAYNAME MONTHNAME TIMESTAMPADD TIMESTAMPDIFF ODBC_TIMESTAMPADD ODBC_TIMESTAMPDIFF 
/* odbc data type tokens */
%token <sval>
	SQL_BIGINT
	SQL_BINARY
	SQL_BIT
	SQL_CHAR
	SQL_DATE
	SQL_DECIMAL
	SQL_DOUBLE
	SQL_FLOAT
	SQL_GUID
	SQL_HUGEINT
	SQL_INTEGER
	SQL_INTERVAL_DAY
	SQL_INTERVAL_DAY_TO_HOUR
	SQL_INTERVAL_DAY_TO_MINUTE
	SQL_INTERVAL_DAY_TO_SECOND
	SQL_INTERVAL_HOUR
	SQL_INTERVAL_HOUR_TO_MINUTE
	SQL_INTERVAL_HOUR_TO_SECOND
	SQL_INTERVAL_MINUTE
	SQL_INTERVAL_MINUTE_TO_SECOND
	SQL_INTERVAL_MONTH
	SQL_INTERVAL_SECOND
	SQL_INTERVAL_YEAR
	SQL_INTERVAL_YEAR_TO_MONTH
	SQL_LONGVARBINARY
	SQL_LONGVARCHAR
	SQL_NUMERIC
	SQL_REAL
	SQL_SMALLINT
	SQL_TIME
	SQL_TIMESTAMP
	SQL_TINYINT
	SQL_VARBINARY
	SQL_VARCHAR
	SQL_WCHAR
	SQL_WLONGVARCHAR
	SQL_WVARCHAR
	SQL_TSI_FRAC_SECOND
	SQL_TSI_SECOND
	SQL_TSI_MINUTE
	SQL_TSI_HOUR
	SQL_TSI_DAY
	SQL_TSI_WEEK
	SQL_TSI_MONTH
	SQL_TSI_QUARTER
	SQL_TSI_YEAR

%type <type>
	odbc_data_type

%type <i_val>
    odbc_tsi_qualifier

/* odbc escape prefix tokens */
%token <sval>
    ODBC_DATE_ESCAPE_PREFIX
    ODBC_TIME_ESCAPE_PREFIX
    ODBC_TIMESTAMP_ESCAPE_PREFIX
    ODBC_GUID_ESCAPE_PREFIX
    ODBC_FUNC_ESCAPE_PREFIX
    ODBC_OJ_ESCAPE_PREFIX

/* odbc symbolic types */
%type <sym>
    odbc_date_escape
    odbc_time_escape
    odbc_timestamp_escape
    odbc_guid_escape
    odbc_interval_escape
    odbc_scalar_func_escape
    odbc_scalar_func
    odbc_datetime_func

%token <sval> POSITION SUBSTRING TRIM SPLIT_PART

/* scanner rewrite NOT * -> NOT_*
   WITH TIME -> WITH_LA TIME
   INTO STRING -> INTO_LA STRING
   OUTER UNION -> OUTER_UNION
   TO (MONTH etc) -> TO_LA MONTH ..
 */
%token <sval> WITH_LA INTO_LA OUTER_UNION TO_LA

/* operators (highest precedence at the end) */
%left OUTER_UNION UNION EXCEPT
%left INTERSECT
%left OR
%left <operation> AND
%right NOT
%nonassoc IS
%nonassoc <operation> '='
%nonassoc <sval> COMPARISON /* <> < > <= >= */
%nonassoc <sval> NOT_BETWEEN BETWEEN NOT_IN sqlIN NOT_EXISTS EXISTS NOT_LIKE LIKE NOT_ILIKE ILIKE
%nonassoc ESCAPE

%nonassoc UNBOUNDED
%nonassoc IDENT PARTITION RANGE ROWS GROUPS PRECEDING FOLLOWING CUBE ROLLUP
                        SET OBJECT VALUE WITH WITHOUT PATH 
%left '+' '-' '&' '|' '^' LEFT_SHIFT RIGHT_SHIFT LEFT_SHIFT_ASSIGN RIGHT_SHIFT_ASSIGN CONCATSTRING
%left '*' '/' '%'
%left AT
%right UMINUS
%left '[' ']'
%left '(' ')' '{' '}'
%left '.'

%left JOIN CROSS LEFT FULL RIGHT INNER NATURAL

%left <operation> '~'

%left <operation> GEOM_OVERLAP GEOM_OVERLAP_OR_ABOVE GEOM_OVERLAP_OR_BELOW GEOM_OVERLAP_OR_LEFT
%left <operation> GEOM_OVERLAP_OR_RIGHT GEOM_BELOW GEOM_ABOVE GEOM_DIST GEOM_MBR_EQUAL


%%

sqlstmt:
   sql SCOLON
	{
		(void)yynerrs;
		if (m->sym) {
			append_symbol(m->sym->data.lval, $1);
			$$ = m->sym;
		} else {
			m->sym = $$ = $1;
		}
		YYACCEPT;
	}

 | sql ':' named_arg_list_ref SCOLON
	{
		(void)yynerrs;
		 if (!m->emode) /* don't replace m_deps/instantiate */
			m->emode = m_prepare;
		if (m->sym) {
			append_symbol(m->sym->data.lval, $1);
			$$ = m->sym;
		} else {
			dlist* stmts = L();
			append_symbol(stmts, $$ = $1);
			m->sym = _symbol_create_list(SQL_MULSTMT, stmts);
		}
		/* call( query, nop(-1, false, parameters) ) */
		if (m->sym->data.lval) {
			m->emod |= mod_exec;
			dlist* l = L();
			append_symbol(l, m->sym);
			append_symbol(l, $3);
			m->sym = _symbol_create_list(SQL_CALL, l);
		}
		YYACCEPT;
	}

 | prepare		{
			  if (!m->emode) /* don't replace m_deps/instantiate */
				m->emode = m_prepare;
			  m->scanner.as = m->scanner.yycur;
			}
   sql SCOLON	{
			  if (m->sym) {
				append_symbol(m->sym->data.lval, $3);
				$$ = m->sym;
			  } else {
				m->sym = $$ = $3;
			  }
			  YYACCEPT;
			}
 | SQL_PLAN		{
			  m->emode = m_plan;
			  m->scanner.as = m->scanner.yycur;
			}
   sql SCOLON	{
			  if (m->sym) {
				append_symbol(m->sym->data.lval, $3);
				$$ = m->sym;
			  } else {
				m->sym = $$ = $3;
			  }
			  YYACCEPT;
			}

 | SQL_EXPLAIN		{
			  m->emod |= mod_explain;
			  m->scanner.as = m->scanner.yycur;
			}
   sql SCOLON		{
			  if (m->sym) {
				append_symbol(m->sym->data.lval, $3);
				$$ = m->sym;
			  } else {
				m->sym = $$ = $3;
			  }
			  YYACCEPT;
			}

 | SQL_TRACE		{
			  m->emod |= mod_trace;
			  m->scanner.as = m->scanner.yycur;
			}
   sql SCOLON		{
			  if (m->sym) {
				append_symbol(m->sym->data.lval, $3);
				$$ = m->sym;
			  } else {
				m->sym = $$ = $3;
			  }
			  YYACCEPT;
			}
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
|	IF NOT_EXISTS { $$ = TRUE; }
;

table_if_not_exists:
	TABLE   { $$ = FALSE; }
|	TABLE IF NOT_EXISTS { $$ = TRUE; }
;

drop:
    DROP

set:
    SET

declare:
    DECLARE

	/* schema definition language */
analyze_statement:
   ANALYZE qname opt_column_list
		{ dlist *l = L();
		append_list(l, $2);
		append_list(l, $3);
		$$ = _symbol_create_list( SQL_ANALYZE, l); }
 ;

sql:
    schema
 |  grant
 |  revoke
 |  create_statement
 |  drop_statement
 |  alter_statement
 |  declare_statement
 |  set_statement
 |  call_statement
 |  analyze_statement
 |  comment_on_statement
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
    set variable_ref '=' scalar_exp
		{ dlist *l = L();
		append_list(l, $2 );
		append_symbol(l, $4 );
		$$ = _symbol_create_list( SQL_SET, l); }
  | set variable_ref_commalist_parens '=' select_with_parens
		{ dlist *l = L();
		append_list(l, $2);
		append_symbol(l, $4);
		$$ = _symbol_create_list( SQL_SET, l ); }
  | set sqlSESSION AUTHORIZATION opt_equal ident
		{ dlist *l = L();
		  sql_subtype t;
		sql_find_subtype(&t, "char", UTF8_strlen($5), 0 );
		append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_role")));
		append_symbol(l, _newAtomNode( _atom_string(&t, $5)) );
		$$ = _symbol_create_list( SQL_SET, l); }
  | set session_schema ident
		{ dlist *l = L();
		  sql_subtype t;
		sql_find_subtype(&t, "char", UTF8_strlen($3), 0 );
		append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_schema")));
		append_symbol(l, _newAtomNode( _atom_string(&t, $3)) );
		$$ = _symbol_create_list( SQL_SET, l); }
  | set session_user opt_equal ident
		{ dlist *l = L();
		  sql_subtype t;
		sql_find_subtype(&t, "char", UTF8_strlen($4), 0 );
		append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_user")));
		append_symbol(l, _newAtomNode( _atom_string(&t, $4)) );
		$$ = _symbol_create_list( SQL_SET, l); }
  | set ROLE ident
		{ dlist *l = L();
		  sql_subtype t;
		sql_find_subtype(&t, "char", UTF8_strlen($3), 0);
		append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_role")));
		append_symbol(l, _newAtomNode( _atom_string(&t, $3)) );
		$$ = _symbol_create_list( SQL_SET, l); }
  | set CURRENT_ROLE opt_equal ident
		{ dlist *l = L();
		  sql_subtype t;
		sql_find_subtype(&t, "char", UTF8_strlen($4), 0);
		append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_role")));
		append_symbol(l, _newAtomNode( _atom_string(&t, $4)) );
		$$ = _symbol_create_list( SQL_SET, l); }
  | set session_timezone opt_equal LOCAL
		{ dlist *l = L();
		  sql_subtype t;
		sql_find_subtype(&t, "sec_interval", inttype2digits(ihour, isec), 0);
		append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_timezone")));
		append_symbol(l, _newAtomNode(atom_int(SA, &t, 0)));
		$$ = _symbol_create_list( SQL_SET, l); }
  | set session_timezone opt_equal literal
		{ dlist *l = L();
		append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_timezone")));
		append_symbol(l, $4 );
		$$ = _symbol_create_list( SQL_SET, l); }
 ;

schema:
    create SCHEMA if_not_exists schema_name_clause opt_schema_default_char_set opt_path_specification opt_schema_element_list
		{ dlist *l = L();
		append_list(l, $4);
		append_symbol(l, $5);
		append_symbol(l, $6);
		append_list(l, $7);
		append_int(l, $3);
		$$ = _symbol_create_list( SQL_CREATE_SCHEMA, l); }
  | drop SCHEMA if_exists qname drop_action
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

schema_element: grant | revoke | create_statement_in_schema | drop_statement | alter_statement ;

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
	/* empty */		    { $$ = 0; }
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
	COPY FROM	{ $$ = PRIV_COPYFROMFILE; }
 |	COPY INTO	{ $$ = PRIV_COPYINTOFILE; }
 ;

object_name:
     TABLE qname		{ $$ = _symbol_create_list(SQL_TABLE, $2); }
 |   qname			{ $$ = _symbol_create_list(SQL_NAME, $1); }
 |   routine_designator		{ $$ = _symbol_create_list(SQL_FUNC, $1); }
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
 |  REFERENCES opt_column_list	    { $$ = _symbol_create_list(SQL_SELECT,$2); }
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
 | ALTER TABLE if_exists qname RENAME TO column_id
	{ dlist *l = L();
	  append_list(l, $4);
	  append_string(l, $7);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_RENAME_TABLE, l ); }
 | ALTER TABLE if_exists qname RENAME opt_column column_id TO column_id
	{ dlist *l = L();
	  append_list(l, $4);
	  append_string(l, $7);
	  append_string(l, $8);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_RENAME_COLUMN, l); }
 | ALTER TABLE if_exists qname SET SCHEMA column_id
	{ dlist *l = L();
	  append_list(l, $4);
	  append_string(l, $7);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_SET_TABLE_SCHEMA, l ); }
 | ALTER USER if_exists column_id opt_with_encrypted_password user_schema opt_schema_path opt_default_role opt_max_memory_max_workers
	{ dlist *l = L(), *p = L();
	  if (!$5 && !$6 && !$7 && !$8 && $9[0] < 0 && $9[1] < 0) {
		yyerror(m, "ALTER USER: At least one property should be updated");
		YYABORT;
	  }
	  append_string(l, $4);
	  append_string(p, $5 ? $5->h->data.sval : NULL);
	  append_string(p, $6);
	  append_string(p, $7);
	  append_int(p, $5 ? $5->h->next->data.i_val : 0);
	  append_string(p, NULL);
	  append_list(l, p);
	  append_string(l, $8);
	  append_lng(l, $9[0]);
	  append_int(l, (int)$9[1]);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_ALTER_USER, l ); }
 | ALTER USER if_exists column_id RENAME TO column_id
	{ dlist *l = L();
	  append_string(l, $4);
	  append_string(l, $7);
	  append_int(l, $3);
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
	  append_string(l, NULL);
	  append_lng(l, -1);
	  append_int(l, -1);
	  append_int(l, FALSE);
	  $$ = _symbol_create_list( SQL_ALTER_USER, l ); }
 | ALTER SCHEMA if_exists column_id RENAME TO column_id
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

opt_schema_path:
	SCHEMA PATH string	{ $$ = $3; }
 |  /* empty */			{ $$ = NULL; }
 ;


alter_table_element:
	column SET DEFAULT default_value
	{ dlist *l = L();
	  append_string(l, $1);
	  append_symbol(l, $4);
	  $$ = _symbol_create_list( SQL_DEFAULT, l); }
 |	column SET sqlNULL
	{ dlist *l = L();
	  append_string(l, $1);
	  $$ = _symbol_create_list( SQL_NULL, l); }
 |	column SET NOT sqlNULL
	{ dlist *l = L();
	  append_string(l, $1);
	  $$ = _symbol_create_list( SQL_NOT_NULL, l); }
 |	column DROP DEFAULT
	{ $$ = _symbol_create( SQL_DROP_DEFAULT, $1); }
 |	column SET STORAGE string
	{ dlist *l = L();
	  append_string(l, $1);
	  if (!strlen($4))
		append_string(l, NULL);
	  else
		append_string(l, $4);
	  $$ = _symbol_create_list( SQL_STORAGE, l); }
 |	column SET STORAGE sqlNULL
	{ dlist *l = L();
	  append_string(l, $1);
	  append_string(l, NULL);
	  $$ = _symbol_create_list( SQL_STORAGE, l); }
 |	column data_type
	{ dlist *l = L();
	  append_string(l, $1);
	  append_type(l, &$2);
	  $$ = _symbol_create_list( SQL_TYPE, l); }
 ;

drop_table_element:
     COLUMN column_id drop_action
	{ dlist *l = L();
	  append_string(l, $2 );
	  append_int(l, $3 );
	  $$ = _symbol_create_list( SQL_DROP_COLUMN, l ); }
  |  column_id drop_action
	{ dlist *l = L();
	  append_string(l, $1 );
	  append_int(l, $2 );
	  $$ = _symbol_create_list( SQL_DROP_COLUMN, l ); }
  |  CONSTRAINT column_id drop_action
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

column:
     column_id
 |   COLUMN column_id	 { $$ = $2; }
 ;

create_statement:
   create role_def	{ $$ = $2; }
 | create table_def	{ $$ = $2; }
 | view_def	{ $$ = $1; }
 | type_def
 | func_def
 | index_def
 | seq_def
 | trigger_def
 ;

create_statement_in_schema:
   create table_def	{ $$ = $2; }
 | view_def	{ $$ = $1; }
 | index_def
 | seq_def
 | trigger_def
 ;

/*=== BEGIN SEQUENCES ===*/
seq_def:
/*
 * CREATE SEQUENCE [ IF NOT EXISTS ] name
 *      [ AS datatype ]
 *	[ START WITH start ]
 *	[ INCREMENT BY increment ]
 *	[ MINVALUE minvalue | NO MINVALUE ]
 *	[ MAXVALUE maxvalue | NO MAXVALUE ]
 *	[ CACHE cache ]		* not part of standard -- will be dropped *
 *	[ [ NO ] CYCLE ]
 * start may be a value or subquery
 */
    create SEQUENCE if_not_exists qname opt_seq_params
	{
		dlist *l = L();
		append_list(l, $4);
		append_list(l, $5);
		append_int(l, 0); /* to be dropped */
		append_int(l, $3);
		$$ = _symbol_create_list(SQL_CREATE_SEQ, l);
	}
/*
 * DROP SEQUENCE [ IF EXISTS ] name
 */
  | drop SEQUENCE if_exists qname
	{
		dlist *l = L();
		append_list(l, $4);
		append_int(l, $3);
		$$ = _symbol_create_list(SQL_DROP_SEQ, l);
	}
/*
 * ALTER SEQUENCE [ IF EXISTS ] name
 *      [ AS datatype ]
 *	[ RESTART [ WITH start ] ]
 *	[ INCREMENT BY increment ]
 *	[ MINVALUE minvalue | NO MINVALUE ]
 *	[ MAXVALUE maxvalue | NO MAXVALUE ]
 *	[ CACHE cache ]		* not part of standard -- will be dropped *
 *	[ [ NO ] CYCLE ]
 * start may be a value or subquery
 */
  | ALTER SEQUENCE if_exists qname opt_alt_seq_params
	{
		dlist *l = L();
		append_list(l, $4);
		append_list(l, $5);
		append_int(l, $3);
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
	AS data_type			{ $$ = _symbol_create_list(SQL_TYPE, append_type(L(),&$2)); }
  |	START WITH opt_sign lngval	{ $$ = _symbol_create_lng(SQL_START, is_lng_nil($4) ? $4 : $3 * $4); }
  |	opt_seq_common_param		{ $$ = $1; }
  ;

opt_alt_seq_param:
	AS data_type			{ $$ = _symbol_create_list(SQL_TYPE, append_type(L(),&$2)); }
  |	RESTART				{ $$ = _symbol_create_list(SQL_START, append_int(L(),0)); /* plain restart now */ }
  |	RESTART WITH opt_sign lngval	{ $$ = _symbol_create_list(SQL_START, append_lng(append_int(L(),2), is_lng_nil($4) ? $4 : $3 * $4));  }
  |	RESTART WITH select_with_parens	{ $$ = _symbol_create_list(SQL_START, append_symbol(append_int(L(),1), $3));  }
  |	opt_seq_common_param		{ $$ = $1; }
  ;

opt_seq_common_param:
	INCREMENT BY opt_sign lngval	{ $$ = _symbol_create_lng(SQL_INC, is_lng_nil($4) ? $4 : $3 * $4); }
  |	MINVALUE opt_sign lngval	{ $$ = _symbol_create_lng(SQL_MINVALUE, is_lng_nil($3) ? $3 : $2 * $3); }
  |	NO MINVALUE			{ $$ = _symbol_create_int(SQL_MINVALUE, int_nil); /* Hack: SQL_MINVALUE + int_nil signals NO MINVALUE */ }
  |	MAXVALUE opt_sign lngval	{ $$ = _symbol_create_lng(SQL_MAXVALUE, is_lng_nil($3) ? $3 : $2 * $3); }
  |	NO MAXVALUE			{ $$ = _symbol_create_int(SQL_MAXVALUE, int_nil); /* Hack: SQL_MAXVALUE + int_nil signals NO MAXVALUE */ }
  |	CACHE nonzerolng		{ $$ = _symbol_create_lng(SQL_CACHE, $2); }
  |	CYCLE				{ $$ = _symbol_create_int(SQL_CYCLE, 1); }
  |	NO CYCLE			{ $$ = _symbol_create_int(SQL_CYCLE, 0); }
  ;

/*=== END SEQUENCES ===*/


index_def:
    create opt_index_type INDEX if_not_exists ident ON qname '(' ident_commalist ')'
	{ dlist *l = L();
	  append_string(l, $5);
	  append_int(l, $2);
	  append_list(l, $7);
	  append_list(l, $9);
	  append_int(l, $4);
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
    ROLE if_not_exists column_id opt_grantor
	{ dlist *l = L();
	  append_string(l, $3);
	  append_int(l, $4);
	  append_int(l, $2);
	  $$ = _symbol_create_list( SQL_CREATE_ROLE, l ); }
 |  USER if_not_exists column_id WITH opt_encrypted PASSWORD string sqlNAME string opt_schema_details_list opt_max_memory_max_workers opt_optimizer opt_default_role
    { dlist *l = L();
	  append_string(l, $3);
	  append_string(l, $7);
	  append_string(l, $9);
	  append_list(l, $10);
	  append_int(l, $5);
	  append_lng(l, $11[0]);
	  append_int(l, (int)$11[1]);
	  append_string(l, $12);
	  append_string(l, $13);
	  append_int(l, $2);
	  $$ = _symbol_create_list( SQL_CREATE_USER, l ); }
 ;

opt_max_memory_max_workers: /* pair of (max_workers -1, max_memory default -1) */
    /* empty */         		{ $$[0] = -1; $$[1] = -1; }
 |  NO MAX_MEMORY 			{ $$[0] = 0;  $$[1] = -1; }
 |  NO MAX_MEMORY max_workers  		{ $$[0] = 0; $$[1] = $3; }
 |  NO MAX_MEMORY NO MAX_WORKERS      	{ $$[0] = 0; $$[1] = 0; }
 |  max_memory NO MAX_WORKERS		{ $$[0] = $1; $$[1] = 0; }
 |  max_memory 				{ $$[0] = $1; $$[1] = -1; }
 |  NO MAX_WORKERS     			{ $$[0] = -1; $$[1] = 0; }
 |  NO MAX_WORKERS NO MAX_MEMORY      	{ $$[0] = 0; $$[1] = 0; }
 |  max_workers 			{ $$[0] = -1; $$[1] = $1; }
 |  max_memory max_workers		{ $$[0] = $1; $$[1] = $2; }
 ;

max_memory:
    MAX_MEMORY poslng   { $$ = $2; }
 |  MAX_MEMORY string   {
		char *end = NULL;
		errno = 0;
		lng size = strtoll($2, &end, 10);
		lng unit;
		if (errno == ERANGE || size < 0 || (unit = size_unit(end)) < 0) {
			$$ = -1;
			yyerror(m, "Invalid size");
			YYABORT;
		}
		$$ = size * unit;
	}
 ;

max_workers:
    MAX_WORKERS posint  { $$ = $2; }
 ;

opt_optimizer:
    /* empty */         { $$ = NULL; }
 |  OPTIMIZER string    { $$ = $2; }
 ;

opt_default_role:
    /* empty */           { $$ = NULL; }
 |  DEFAULT ROLE ident    { $$ = $3; }
 ;

opt_schema_details_list:
    opt_schema_path
    { dlist *l = L();
      append_string(l, NULL);
      $$ = append_string(l, $1);}
 |  SCHEMA ident opt_schema_path
    { dlist *l = L();
      append_string(l, $2);
      $$ = append_string(l, $3);}
 ;

opt_encrypted:
    /* empty */		{ $$ = SQL_PW_UNENCRYPTED; }
 |  UNENCRYPTED		{ $$ = SQL_PW_UNENCRYPTED; }
 |  ENCRYPTED		{ $$ = SQL_PW_ENCRYPTED; }
 ;

table_def:
    table_if_not_exists qname table_content_source
	{ int commit_action = CA_COMMIT;
	  dlist *l = L();
	  append_int(l, SQL_PERSIST);
	  append_list(l, $2);
	  append_symbol(l, $3);
	  append_int(l, commit_action);
	  append_string(l, NULL);
	  append_list(l, NULL);
	  append_int(l, $1);
	  append_symbol(l, NULL); /* only used for merge table */
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
 |  table_if_not_exists qname FROM sqlLOADER func_ref
    {
      dlist *l = L();
      append_list(l, $2);
      append_symbol(l, $5);
      $$ = _symbol_create_list( SQL_CREATE_TABLE_LOADER, l);
    }
 |  MERGE table_if_not_exists qname table_content_source opt_partition_by
	{ int commit_action = CA_COMMIT, tpe = SQL_MERGE_TABLE;
	  dlist *l = L();
	  append_int(l, tpe);
	  append_list(l, $3);
	  append_symbol(l, $4);
	  append_int(l, commit_action);
	  append_string(l, NULL);
	  append_list(l, NULL);
	  append_int(l, $2);
	  append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
 |  REPLICA table_if_not_exists qname table_content_source
	{ int commit_action = CA_COMMIT, tpe = SQL_REPLICA_TABLE;
	  dlist *l = L();
	  append_int(l, tpe);
	  append_list(l, $3);
	  append_symbol(l, $4);
	  append_int(l, commit_action);
	  append_string(l, NULL);
	  append_list(l, NULL);
	  append_int(l, $2);
	  append_symbol(l, NULL); /* only used for merge table */
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
 /* mapi:monetdb://host:port/database[/schema[/table]]
    This also allows access via monetdbd.
    We assume the monetdb user with default password */
 |  REMOTE table_if_not_exists qname table_content_source ON string with_opt_credentials
	{ int commit_action = CA_COMMIT, tpe = SQL_REMOTE;
	  dlist *l = L();
	  append_int(l, tpe);
	  append_list(l, $3);
	  append_symbol(l, $4);
	  append_int(l, commit_action);
	  append_string(l, $6);
	  append_list(l, $7);
	  append_int(l, $2);
	  append_symbol(l, NULL); /* only used for merge table */
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
 |  UNLOGGED table_if_not_exists qname table_content_source
	{ int commit_action = CA_COMMIT, tpe = SQL_UNLOGGED_TABLE;
	  dlist *l = L();
	  append_int(l, tpe);
	  append_list(l, $3);
	  append_symbol(l, $4);
	  append_int(l, commit_action);
	  append_string(l, NULL);
	  append_list(l, NULL);
	  append_int(l, $2);
	  append_symbol(l, NULL); /* only used for merge table */
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
  | local_global_temp table_if_not_exists qname table_content_source opt_on_commit
	{ int commit_action = CA_COMMIT;
	  dlist *l = L();
	  append_int(l, $1);
	  append_list(l, $3);
	  append_symbol(l, $4);
	  if ($1 != SQL_PERSIST)
		commit_action = $5;
	  append_int(l, commit_action);
	  append_string(l, NULL);
	  append_list(l, NULL);
	  append_int(l, $2);
	  append_symbol(l, NULL); /* only used for merge table */
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
  | local_temp VIEW qname opt_column_list AS SelectStmt
	{  dlist *l = L();
	  append_int(l, $1);
	  append_list(l, $3);
	  append_list(l, $4);
	  append_symbol(l, $6);
	  append_int(l, FALSE);
	  append_int(l, TRUE);
	  append_int(l, FALSE);
	  $$ = _symbol_create_list( SQL_CREATE_VIEW, l );
	}
 ;

partition_type:
   RANGE	{ $$ = PARTITION_RANGE; }
 | VALUES	{ $$ = PARTITION_LIST; }
 ;

partition_expression:
   scalar_exp	{ $$ = $1; }
 ;

partition_on:
   ON '(' column_id ')'                   { $$ = _symbol_create_list( SQL_PARTITION_COLUMN, append_string(L(), $3) ); }
 | USING '(' partition_expression ')' { $$ = _symbol_create_list( SQL_PARTITION_EXPRESSION, append_symbol(L(), $3) ); }
 ;

opt_partition_by:
 /* empty */		{ $$ = NULL; }
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
   scalar_exp { $$ = $1; }
 ;

partition_range_from:
   scalar_exp { $$ = $1; }
 | RANGE MINVALUE    { $$ = _symbol_create(SQL_MINVALUE, NULL ); }
 ;

partition_range_to:
   scalar_exp { $$ = $1; }
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

local_temp:
    TEMPORARY		{ $$ = SQL_LOCAL_TEMP; }
 |  TEMP		{ $$ = SQL_LOCAL_TEMP; }
 |  LOCAL TEMPORARY	{ $$ = SQL_LOCAL_TEMP; }
 |  LOCAL TEMP		{ $$ = SQL_LOCAL_TEMP; }
 ;

local_global_temp:
    local_temp		{ $$ = SQL_LOCAL_TEMP; }
 |  GLOBAL TEMPORARY	{ $$ = SQL_GLOBAL_TEMP; }
 |  GLOBAL TEMP		{ $$ = SQL_GLOBAL_TEMP; }
 ;

opt_on_commit: /* only for temporary tables */
    /* empty */			 { $$ = CA_COMMIT; }
 |  ON COMMIT sqlDELETE ROWS	 { $$ = CA_DELETE; }
 |  ON COMMIT PRESERVE ROWS	 { $$ = CA_PRESERVE; }
 |  ON COMMIT DROP		 { $$ = CA_DROP; }
 ;

table_content_source:
    '(' table_element_list ')'	{ $$ = _symbol_create_list( SQL_CREATE_TABLE, $2); }
 |  as_subquery_clause		{ $$ = _symbol_create_list( SQL_SELECT, $1); }
 ;

as_subquery_clause:
	opt_column_list
	AS
	SelectStmt
	with_or_without_data
			{ $$ = append_list(L(), $1);
			  append_symbol($$, $3);
			  append_int($$, $4); }
 ;

with_or_without_data:
	 /* empty */	{ $$ = 1; }
 |   WITH NO DATA	{ $$ = 0; }
 |   WITH DATA		{ $$ = 1; }
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
    column_id data_type opt_column_def_opt_list
		{
			dlist *l = L();
			append_string(l, $1);
			append_type(l, &$2);
			append_list(l, $3);
			$$ = _symbol_create_list(SQL_COLUMN, l);
		}
 |  column_id serial_or_bigserial
		{ /* SERIAL = INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY */
		  /* BIGSERIAL = BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY */
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
				sql_find_subtype(&it, "bigint", 63, 0);
			else
				sql_find_subtype(&it, "int", 31, 0);
			append_symbol(o, _symbol_create_list(SQL_TYPE, append_type(L(),&it)));
			append_list(l, o);
			append_int(l, 1); /* to be dropped */
			append_int(l, 0); /* if not exists */

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
    scalar_exp_no_and	{ $$ = $1; }
 ;

column_constraint:
    opt_constraint_name column_constraint_type  /*opt_constraint_attributes*/

	{ dlist *l = L();
	  append_string(l, $1 );
	  append_symbol(l, $2 );
	  $$ = _symbol_create_list( SQL_CONSTRAINT, l ); }
 ;

always_or_by_default:
	ALWAYS
   |	BY DEFAULT
   ;

generated_column:
		/* we handle both by default and always alike, ie inserts/updates are allowed */
	GENERATED always_or_by_default AS IDENTITY serial_opt_params
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
		sql_find_subtype(&it, "int", 31, 0);
		append_symbol($5, _symbol_create_list(SQL_TYPE, append_type(L(),&it)));

		/* finally all the options */
		append_list(l, $5);
		append_int(l, 1); /* to be dropped */
		append_int(l, 0); /* if not exists */
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
		sql_find_subtype(&it, "int", 31, 0);
		append_symbol(o, _symbol_create_list(SQL_TYPE, append_type(L(),&it)));
		append_list(l, o);
		append_int(l, 1); /* to be dropped */
		append_int(l, 0); /* if not exists */
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
	/* empty: return the defaults */	{ $$ = NULL; }
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

check_parenthesis_open:
	'('
	{
		struct scanner *lc = &m->scanner;
		lc->as = lc->rs->pos + lc->yycur;
	}
;

check_parenthesis_close:
	')'
	{
		struct scanner *lc = &m->scanner;
		char* check_sql = sa_strndup(SA, lc->rs->buf+lc->as, lc->rs->pos + lc->yycur - lc->as - 1);
		$$ = check_sql;
	}
;

 check_search_condition:
 check_parenthesis_open scalar_exp check_parenthesis_close
 	{
		dlist *l = L();
		append_symbol(l, $2);
		append_string(l, $3);
		$$ = _symbol_create_list(SQL_CHECK, l);
	}

column_constraint_type:
    NOT sqlNULL	{ $$ = _symbol_create( SQL_NOT_NULL, NULL); }
 |  sqlNULL	{ $$ = _symbol_create( SQL_NULL, NULL); }
 |  UNIQUE	{ $$ = _symbol_create( SQL_UNIQUE, NULL ); }
 |  UNIQUE NULLS DISTINCT	{ $$ = _symbol_create( SQL_UNIQUE, NULL ); }
 |  UNIQUE NULLS NOT DISTINCT	{ $$ = _symbol_create( SQL_UNIQUE_NULLS_NOT_DISTINCT, NULL ); }
 |  PRIMARY KEY	{ $$ = _symbol_create( SQL_PRIMARY_KEY, NULL ); }
 |  REFERENCES qname opt_column_list opt_match opt_ref_action

			{ dlist *l = L();
			  append_list(l, $2 );
			  append_list(l, $3 );
			  append_int(l, $4 );
			  append_int(l, $5 );
			  $$ = _symbol_create_list( SQL_FOREIGN_KEY, l); }
 |  CHECK check_search_condition { $$ = $2; }
 ;

table_constraint_type:
    UNIQUE column_commalist_parens
			{ $$ = _symbol_create_list( SQL_UNIQUE, $2); }
 |  UNIQUE NULLS DISTINCT column_commalist_parens
			{ $$ = _symbol_create_list( SQL_UNIQUE, $4); }
 |  UNIQUE NULLS NOT DISTINCT column_commalist_parens
			{ $$ = _symbol_create_list( SQL_UNIQUE_NULLS_NOT_DISTINCT, $5); }
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
 |  CHECK check_search_condition { $$ = $2; }
 ;

ident_commalist:
    column_id
			{ $$ = append_string(L(), $1); }
 |  ident_commalist ',' column_id
			{ $$ = append_string( $1, $3 ); }
 ;

like_table:
	LIKE qname	{ $$ = _symbol_create_list( SQL_LIKE, $2 ); }
 ;

view_def:
    create_or_replace VIEW qname opt_column_list AS SelectStmt opt_with_check_option
	{  dlist *l = L();
	  append_int(l, SQL_PERSIST);
	  append_list(l, $3);
	  append_list(l, $4);
	  append_symbol(l, $6);
	  append_int(l, $7);
	  append_int(l, TRUE);	/* persistent view */
	  append_int(l, $1);
	  $$ = _symbol_create_list( SQL_CREATE_VIEW, l );
	}
  | CREATE OR REPLACE local_temp VIEW qname opt_column_list AS SelectStmt
	{  dlist *l = L();
	  append_int(l, $4);
	  append_list(l, $6);
	  append_list(l, $7);
	  append_symbol(l, $9);
	  append_int(l, FALSE);
	  append_int(l, TRUE);	/* persistent view */
	  append_int(l, TRUE);  /* replace */
	  $$ = _symbol_create_list( SQL_CREATE_VIEW, l );
	}
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
    create TYPE if_not_exists qname EXTERNAL sqlNAME ident
	{ dlist *l = L();
	  append_list(l, $4);
	  append_string(l, $7);
	  append_int(l, $3);
	  $$ = _symbol_create_list( SQL_CREATE_TYPE, l ); }
 ;

external_function_name:
	column_id '.' type_function_name { $$ = append_string(append_string(L(), $1), $3); }
 ;

function_body:
	X_BODY
|	string
;

func_def_type_no_proc:
	FUNCTION		{ $$ = F_FUNC; }
|	AGGREGATE		{ $$ = F_AGGR; }
|	AGGREGATE FUNCTION	{ $$ = F_AGGR; }
|	FILTER			{ $$ = F_FILT; }
|	FILTER FUNCTION		{ $$ = F_FILT; }
|	WINDOW			{ $$ = F_ANALYTIC; }
|	WINDOW FUNCTION		{ $$ = F_ANALYTIC; }
|	sqlLOADER		{ $$ = F_LOADER; }
|	sqlLOADER FUNCTION	{ $$ = F_LOADER; }
;

func_def_type:
	func_def_type_no_proc
|	PROCEDURE		{ $$ = F_PROC; }
;

func_def_opt_returns:
	RETURNS func_data_type	{ $$ = $2; }
|				{ $$ = NULL; }
;

func_def_opt_order_spec:
	WITH ORDER		{ $$ = 1; }
|	ORDERED			{ $$ = 2; }
|				{ $$ = 0; }
;

func_def:
    create_or_replace func_def_type_no_proc qfunc
	'(' opt_paramlist ')'
    func_def_opt_returns
    func_def_opt_order_spec
    EXTERNAL sqlNAME external_function_name
			{ dlist *f = L();
				append_list(f, $3);
				append_list(f, $5);
				append_symbol(f, $7);
				append_list(f, $11);
				append_list(f, NULL);
				append_int(f, $2);
				append_int(f, FUNC_LANG_MAL);
				append_int(f, $1);
				append_int(f, $8);
			  $$ = _symbol_create_list( SQL_CREATE_FUNC, f ); }
 |  create_or_replace PROCEDURE qfunc
	'(' opt_paramlist ')'
    EXTERNAL sqlNAME external_function_name
			{ dlist *f = L();
				append_list(f, $3);
				append_list(f, $5);
				append_symbol(f, NULL);
				append_list(f, $9);
				append_list(f, NULL);
				append_int(f, F_PROC);
				append_int(f, FUNC_LANG_MAL);
				append_int(f, $1);
				append_int(f, 0);
			  $$ = _symbol_create_list( SQL_CREATE_FUNC, f ); }
 |  create_or_replace func_def_type_no_proc qfunc
	'(' opt_paramlist ')'
    func_def_opt_returns
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
				append_int(f, 0);
			  $$ = _symbol_create_list( SQL_CREATE_FUNC, f ); }
 |  create_or_replace PROCEDURE qfunc
	'(' opt_paramlist ')'
    procedure_body
			{ dlist *f = L();
				append_list(f, $3);
				append_list(f, $5);
				append_symbol(f, NULL);
				append_list(f, NULL);
				append_list(f, $7);
				append_int(f, F_PROC);
				append_int(f, FUNC_LANG_SQL);
				append_int(f, $1);
				append_int(f, 0);
			  $$ = _symbol_create_list( SQL_CREATE_FUNC, f ); }
  | create_or_replace func_def_type_no_proc qfunc
	'(' opt_paramlist ')'
    func_def_opt_returns
    LANGUAGE IDENT function_body
		{
			int lang = 0;
			dlist *f = L();
			char l = *$9;

			if (l == 'R' || l == 'r')
				lang = FUNC_LANG_R;
			else if (l == 'P' || l == 'p') {
				if (strcasecmp($9, "PYTHON3") == 0) {
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
				lang = FUNC_LANG_J;	/* Javascript */
			else {
				sqlformaterror(m, "Language name C, CPP, PYTHON, PYTHON3, R or J(avascript) expected, received '%s'", $9);
			}

			append_list(f, $3);
			append_list(f, $5);
			append_symbol(f, $7);
			append_list(f, NULL);
			append_list(f, append_string(L(), $10));
			append_int(f, $2);
			append_int(f, lang);
			append_int(f, $1);
			append_int(f, 0);
			$$ = _symbol_create_list( SQL_CREATE_FUNC, f );
		}
;

routine_body:
	return_statement
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

procedure_body:
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
    /* empty*/	 { $$ = L(); }
 |  procedure_statement_list
    procedure_statement SCOLON	{ $$ = append_symbol($1,$2);}
 ;

trigger_procedure_statement_list:
    /* empty*/	 { $$ = L(); }
 |  trigger_procedure_statement_list
    trigger_procedure_statement SCOLON	{ $$ = append_symbol($1,$2);}
 ;

procedure_statement:
	transaction_statement
    |	update_statement
    |	schema
    |	grant
    |	revoke
    |	create_statement
    |	drop_statement
    |	alter_statement
    |   declare_statement
    |   set_statement
    |	control_statement
    |	call_statement
    |   analyze_statement
    |   SelectStmt
    {
	$$ = $1;
	if ($$ && $$->token == SQL_SELECT) {
		SelectNode *s = (SelectNode*)$$;
		if (s->into == NULL) {
			$$ = NULL;
			yyerror(m, "regular select statements not allowed inside procedures");
			YYABORT;
		}
	}
    }
    ;

trigger_procedure_statement:
	transaction_statement
    |	update_statement
    |	grant
    |	revoke
    |   declare_statement
    |   set_statement
    |	control_statement
    |	call_statement
    |   analyze_statement
    |   SelectStmt
    ;

control_statement:
        while_statement
    |   if_statement
    |   case_statement
    |	return_statement
/*
    |   for_statement		fetch tuples, not supported because of cursors

    |   loop_statement		while (true)
    |   repeat_statement	do while

    |   leave_statement	multilevel break
    |   iterate_statement	multilevel continue
*/
    ;

call_statement:
	CALL func_ref			{$$ = _symbol_create_symbol(SQL_CALL, $2);}
    /* odbc procedure call escape */
    | '{' CALL func_ref '}' {$$ = _symbol_create_symbol(SQL_CALL, $3);}
    ;

return_statement:
        RETURN return_value { $$ = _symbol_create_symbol(SQL_RETURN, $2); }
   ;

return_value:
      select_no_parens
   |  scalar_exp
   |  TABLE select_with_parens
		{ $$ = _symbol_create_symbol(SQL_TABLE, $2); }
   ;

case_statement:
     CASE scalar_exp when_statements case_opt_else_statement END CASE
		{ $$ = _symbol_create_list(SQL_CASE,
		   append_list(
		    append_list(
		     append_symbol(
		      L(),$2),$3),$4)); }
 |   CASE when_statements case_opt_else_statement END CASE
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

case_opt_else_statement:
    /* empty */				{ $$ = NULL; }
 |  ELSE procedure_statement_list	{ $$ = $2; }
 ;

		/* data types, more types to come */

if_statement:
	IF scalar_exp THEN procedure_statement_list
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
   |	ELSEIF scalar_exp THEN procedure_statement_list
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
	WHILE scalar_exp DO
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
	/* empty */	{ $$ = NULL; }
 |	ident ':'
 ;

opt_end_label:
	/* empty */	{ $$ = NULL; }
 |	ident
 ;

table_function_column_list:
	column_id data_type	{ $$ = L();
				  append_string($$, $1);
				  append_type($$, &$2);
				}
  |     table_function_column_list ',' column_id data_type
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
    paramlist ',' column_id data_type
			{ dlist *p = L();
			  append_string(p, $3);
			  append_type(p, &$4);
			  $$ = append_list($1, p); }
 |  column_id data_type
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
    opt_qname opt_referencing_list triggered_action
	{ dlist *l = L();
	  append_list(l, $3);
	  append_int(l, $4);
	  append_symbol(l, $5);
	  append_list(l, $6);
	  append_list(l, $7);
	  append_list(l, $8);
	  append_int(l, $1);
	  $$ = _symbol_create_list(SQL_CREATE_TRIGGER, l);
	}
 ;

opt_qname:
    /* empty */ { $$ = NULL; }
    | ON qname  { $$ = $2; }
    ;

trigger_action_time:
    BEFORE	{ $$ = 0; }
 |  AFTER	{ $$ = 1; }
/* | INSTEAD OF { $$ = 2; } */
 ;

trigger_event:
    INSERT			{ $$ = _symbol_create_list(SQL_INSERT, NULL); }
 |  sqlDELETE		{ $$ = _symbol_create_list(SQL_DELETE, NULL); }
 |  TRUNCATE		{ $$ = _symbol_create_list(SQL_TRUNCATE, NULL); }
 |  UPDATE			{ $$ = _symbol_create_list(SQL_UPDATE, NULL); }
 |  UPDATE OF ident_commalist	{ $$ = _symbol_create_list(SQL_UPDATE, $3); }
 |  LOGIN			{ $$ = _symbol_create_list(SQL_LOGIN, NULL); }
 ;

opt_referencing_list:
    /* empty */				{ $$ = NULL; }
 |  REFERENCING old_or_new_values_alias_list	{ $$ = $2; }
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
 |     OLD TABLE opt_as ident	{ $$ = append_string(append_int(L(), 0), $4); }
 |  sqlNEW TABLE opt_as ident	{ $$ = append_string(append_int(L(), 1), $4); }
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
    /* default for each statement */	{ $$ = 1; }
 |  FOR EACH row_or_statement		{ $$ = $3; }
 ;

row_or_statement:
    ROW	{ $$ = 0; }
 |  STATEMENT	{ $$ = 1; }
 ;

opt_when:
    /* empty */			{ $$ = NULL; }
 |  WHEN  '(' scalar_exp ')'	{ $$ = $3; }
 ;

triggered_statement:
    trigger_procedure_statement
				{ $$ = append_symbol(L(), $1); }
 |  BEGIN ATOMIC
    trigger_procedure_statement_list
    END			{ $$ = $3; }
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
 |  drop TYPE if_exists qname drop_action
	{ dlist *l = L();
	  append_list(l, $4 );
	  append_int(l, $5 );
	  append_int(l, $3 );
	  $$ = _symbol_create_list( SQL_DROP_TYPE, l ); }
 |  drop ROLE if_exists column_id
	{ dlist *l = L();
	  append_string(l, $4 );
	  append_int(l, $3 );
	  $$ = _symbol_create_list( SQL_DROP_ROLE, l ); }
 |  drop USER if_exists column_id
	{ dlist *l = L();
	  append_string(l, $4 );
	  append_int(l, $3 );
	  $$ = _symbol_create_list( SQL_DROP_USER, l ); }
 |  drop INDEX if_exists qname
	{ dlist *l = L();
	  append_list(l, $4 );
	  append_int(l, $3 );
	  $$ = _symbol_create_list( SQL_DROP_INDEX, l ); }
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
 | import_stmt
 | copyto_stmt
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
 |  SAVEPOINT column_id
		{ $$ = _symbol_create( TR_SAVEPOINT, $2); }
 |  RELEASE SAVEPOINT column_id
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
	/* empty */		{ $$ = tr_serializable; }
 |	_transaction_mode_list  { $$ = $1; }
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
 |	ISOLATION LEVEL iso_level	{ $$ = $3; }
 |	DIAGNOSTICS sqlSIZE intval	{ $$ = tr_none; /* not supported */ }
 ;

iso_level:
	READ UNCOMMITTED	{ $$ = tr_snapshot; }
 |	READ COMMITTED		{ $$ = tr_snapshot; }
 |	sqlREPEATABLE READ	{ $$ = tr_snapshot; }
 |	SNAPSHOT		{ $$ = tr_snapshot; }
 |	SERIALIZABLE		{ $$ = tr_serializable; }
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

import_stmt:
	copyfrom_stmt { $$ = $1; }
  | copybinfrom_stmt { $$ = $1; }
  | load_stmt { $$ = $1; }
  ;

copyfrom_stmt:
/*  1    2      3    4     5               6    7                8               9 */
    COPY opt_nr INTO qname opt_column_list FROM string_commalist opt_header_list opt_on_location
	{ CopyFromNode *copy = newCopyFromNode(SA,
		/* qname */ $4,
		/* column_list */ $5,
		/* sources */ $7,
		/* header_list */ $8,
		/* nr_offset */ $2);
	  copy->on_client = !!$9;
	  $$ = (symbol*)copy; }
/*  1    2      3    4     5               6    7      8 */
  | COPY opt_nr INTO qname opt_column_list FROM STDIN opt_header_list
	{ CopyFromNode *copy = newCopyFromNode(SA,
		/* qname */ $4,
		/* column_list */ $5,
		/* sources */ NULL,
		/* header_list */ $8,
		/* nr_offset */ $2);
	  $$ = (symbol*)copy; }
  | copyfrom_stmt opt_using DELIMITERS string
    { CopyFromNode *copy = (CopyFromNode*)$1;
	  copy->tsep = $4;
	  $$ = $1; }
  | copyfrom_stmt opt_using DELIMITERS string ',' string
    { CopyFromNode *copy = (CopyFromNode*)$1;
	  copy->tsep = $4;
	  copy->rsep = $6;
	  $$ = $1; }
  | copyfrom_stmt opt_using DELIMITERS string ',' string ',' string
    { CopyFromNode *copy = (CopyFromNode*)$1;
	  copy->tsep = $4;
	  copy->rsep = $6;
	  copy->ssep = $8;
	  $$ = $1; }
  | copyfrom_stmt sqlDECIMAL opt_as string
  { CopyFromNode *copy = (CopyFromNode*)$1;
    copy->decsep = $4;
    $$ = $1;}
  | copyfrom_stmt sqlDECIMAL opt_as string ',' string
  { CopyFromNode *copy = (CopyFromNode*)$1;
    copy->decsep = $4;
    copy->decskip = $6;
    $$ = $1;}
  | copyfrom_stmt ESCAPE
  { CopyFromNode *copy = (CopyFromNode*)$1;
    copy->escape = true;
    $$ = $1;}
  | copyfrom_stmt NO ESCAPE
  { CopyFromNode *copy = (CopyFromNode*)$1;
    copy->escape = false;
    $$ = $1;}
  | copyfrom_stmt sqlNULL opt_as string
  { CopyFromNode *copy = (CopyFromNode*)$1;
    copy->null_string = $4;
    $$ = $1;}
  | copyfrom_stmt BEST EFFORT
  { CopyFromNode *copy = (CopyFromNode*)$1;
    copy->best_effort = true;
    $$ = $1;}
  | copyfrom_stmt FWF '(' fwf_widthlist ')'
  { CopyFromNode *copy = (CopyFromNode*)$1;
    if (copy->sources == NULL) {
		yyerror(m, "cannot use FWF with FROM STDIN");
		YYABORT;
	}
    copy->fwf_widths = $4;
    $$ = $1;}
  ;

load_stmt:
/*   1    2         3    4     5    6 */
    COPY sqlLOADER INTO qname FROM func_ref
	{ dlist *l = L();
	  append_list(l, $4);
	  append_symbol(l, $6);
	  $$ = _symbol_create_list( SQL_COPYLOADER, l ); }

copybinfrom_stmt:
/*   1      2              3      4    5     6               7    8                9 */
    COPY opt_endianness BINARY INTO qname opt_column_list FROM string_commalist opt_on_location
	{ dlist *l = L();
	  append_list(l, $5);
	  append_list(l, $6);
	  append_list(l, $8);
	  append_int(l, $9);
	  append_int(l, $2);
	  $$ = _symbol_create_list( SQL_BINCOPYFROM, l ); }
  ;

copyto_stmt:
/*  1    2               3    4      5               6        7 */
    COPY SelectStmt INTO_LA string opt_on_location opt_seps opt_null_string
	{ dlist *l = L();
	  append_symbol(l, $2);
	  append_string(l, $4);
	  append_list(l, $6);
	  append_string(l, $7);
	  append_int(l, $5);
	  $$ = _symbol_create_list( SQL_COPYINTO, l ); }
/*  1    2                    3    4      5        6 */
  | COPY SelectStmt INTO_LA STDOUT opt_seps opt_null_string
	{ dlist *l = L();
	  append_symbol(l, $2);
	  append_string(l, NULL);
	  append_list(l, $5);
	  append_string(l, $6);
	  append_int(l, 0);
	  $$ = _symbol_create_list( SQL_COPYINTO, l ); }
/*  1    2              3    4              5      6                7 */
  | COPY SelectStmt INTO_LA opt_endianness BINARY string_commalist opt_on_location
	{ dlist *l = L();
	  append_symbol(l, $2);
	  append_int(l, $4);
	  append_list(l, $6);
	  append_int(l, $7);
	  $$ = _symbol_create_list( SQL_BINCOPYINTO, l ); }
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
   header			{ $$ = append_list(L(), $1); }
 | header_list ',' header	{ $$ = append_list($1, $3); }
 ;

header:
	column_id
			{ dlist *l = L();
			  append_string(l, $1 );
			  $$ = l; }
 |	column_id string
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
 |  OFFSET poslng		{ $$ = append_lng(append_lng(L(), -1), $2); }
 |  poslng OFFSET poslng RECORDS
				{ $$ = append_lng(append_lng(L(), $1), $3); }
 |  poslng RECORDS OFFSET poslng
				{ $$ = append_lng(append_lng(L(), $1), $4); }
 ;

opt_null_string:
	/* empty */		{ $$ = NULL; }
 |	sqlNULL opt_as string	{ $$ = $3; }
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

opt_returning_clause:
    /* empty */				{ $$ = NULL; }
	| RETURNING selection	{ $$ = $2; }
	;

delete_stmt:
    opt_with_clause sqlDELETE FROM qname opt_alias_name opt_where_clause opt_returning_clause

	{ dlist *l = L();
	  append_list(l, $4);
	  append_string(l, $5);
	  append_symbol(l, $6);
	  append_list(l, $7);
	  $$ = _symbol_create_list( SQL_DELETE, l );
	  if ($1) {
	  	$1->data.lval->h->next->data.sym = $$;
		$$ = $1;
	  }
	}
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
    opt_with_clause UPDATE qname opt_alias_name SET assignment_commalist opt_from_clause opt_where_clause opt_returning_clause
	{ dlist *l = L();
	  append_list(l, $3);
	  append_string(l, $4);
	  append_list(l, $6);
	  append_symbol(l, $7);
	  append_symbol(l, $8);
	  append_list(l, $9);
	  $$ = _symbol_create_list( SQL_UPDATE, l );
	  if ($1) {
	  	$1->data.lval->h->next->data.sym = $$;
		$$ = $1;
	  }
	}
 ;

opt_search_condition:
 /* empty */            { $$ = NULL; }
 | AND scalar_exp { $$ = $2; }
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
   INSERT insert_rest
   { dlist *l = L();
     append_list(l, $2?$2->h->data.lval:NULL);
     append_symbol(l, $2?$2->h->next->data.sym:NULL);
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
    opt_with_clause MERGE INTO qname opt_alias_name USING table_ref ON scalar_exp merge_when_list /* TODO handle returning !! */
	{ dlist *l = L();
	  append_list(l, $4);
	  append_string(l, $5);
	  append_symbol(l, $7);
	  append_symbol(l, $9);
	  append_list(l, $10);
	  $$ = _symbol_create_list( SQL_MERGE, l );
	  if ($1) {
	  	$1->data.lval->h->next->data.sym = $$;
		$$ = $1;
	  }
	}
 ;

insert_stmt:
    opt_with_clause INSERT INTO qname insert_rest opt_returning_clause
	{ dlist *l = L();
	  append_list(l, $4);
	  append_list(l, $5?$5->h->data.lval:NULL);
	  append_symbol(l, $5?$5->h->next->data.sym:NULL);
	  append_list(l, $6);
	  $$ = _symbol_create_list( SQL_INSERT, l );
	  if ($1) {
	  	$1->data.lval->h->next->data.sym = $$;
		$$ = $1;
	  }
	}
 ;

insert_rest:
/* empty values list */
	{
          dlist *p = L();
	  dlist *l = L();
	  append_list(p, NULL); /* no column spec */
	  append_symbol(p, _symbol_create_list(SQL_VALUES, l));
	  $$ = p;
        }
 |   DEFAULT VALUES
	{
          dlist *p = L();
          dlist *l = L();
	  append_list(p, NULL); /* no column spec */
	  append_symbol(p, _symbol_create_list(SQL_VALUES, l));
	  $$ = p;
	}
 |  SelectStmt
	{
          dlist *p = L();
	  append_list(p, NULL); /* no column spec */
	  append_symbol(p, $1);
	  $$ = p;
	}
 |  column_commalist_parens SelectStmt
	{
          dlist *p = L();
	  append_list(p, $1); /* no column spec */
	  append_symbol(p, $2);
	  $$ = p;
	}
 ;

expr_list:
	scalar_exp		 { $$ = append_symbol(L(), $1); }
 |	expr_list ',' scalar_exp { $$ = append_symbol($1, $3); }
 ;

named_value_commalist:
    ident scalar_exp		{ $$ = append_string(append_symbol(L(), $2), $1); }
 |  named_value_commalist ',' ident scalar_exp
			{ $$ = append_string(append_symbol($1, $4), $3); }
 ;

null:
   sqlNULL		{ $$ = _symbol_create(SQL_NULL, NULL ); }
 ;

opt_distinct:
    /* empty */		{ $$ = FALSE; }
 |  ALL			{ $$ = FALSE; }
 |  DISTINCT		{ $$ = TRUE; }
 ;

all_distinct:
    ALL			{ $$ = FALSE; }
 |  DISTINCT		{ $$ = TRUE; }
 ;


assignment_commalist:
    assignment		{ $$ = append_symbol(L(), $1 ); }
 |  assignment_commalist ',' assignment
			{ $$ = append_symbol($1, $3 ); }
 ;

assignment:
   column_id '=' scalar_exp
	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_string(l, $1);
	  $$ = _symbol_create_list( SQL_ASSIGN, l); }
 |  column_commalist_parens '=' scalar_exp
	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_list(l, $1);
	  $$ = _symbol_create_list( SQL_ASSIGN, l ); }
 ;

opt_where_clause:
    /* empty */			{ $$ = NULL; }
 |  WHERE scalar_exp	{ $$ = $2; }
 ;

	/* query expressions */

joined_table:
   '(' joined_table ')'
	{ $$ = $2; }
 |  table_ref CROSS JOIN table_ref
	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, 0);
	  append_int(l, 4);
	  append_symbol(l, $4);
	  append_symbol(l, NULL);
	  $$ = _symbol_create_list( SQL_JOIN, l); }
 |  table_ref join_type JOIN table_ref join_spec
	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, 0);
	  append_int(l, $2);
	  append_symbol(l, $4);
	  append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_JOIN, l); }
 |  table_ref JOIN table_ref join_spec
	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, 0);
	  append_int(l, 0);
	  append_symbol(l, $3);
	  append_symbol(l, $4);
	  $$ = _symbol_create_list( SQL_JOIN, l); }
 |  table_ref NATURAL join_type JOIN table_ref
	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, 1);
	  append_int(l, $3);
	  append_symbol(l, $5);
	  append_symbol(l, NULL);
	  $$ = _symbol_create_list( SQL_JOIN, l); }
 |  table_ref NATURAL JOIN table_ref
	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, 1);
	  append_int(l, 0);
	  append_symbol(l, $4);
	  append_symbol(l, NULL);
	  $$ = _symbol_create_list( SQL_JOIN, l); }
 | '{' ODBC_OJ_ESCAPE_PREFIX table_ref join_type JOIN table_ref join_spec '}' /* we allow normal join syntax here to solve parser conflicts */
	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_int(l, 0);
	  append_int(l, $4 + 1);
	  append_symbol(l, $6);
	  append_symbol(l, $7);
	  $$ = _symbol_create_list( SQL_JOIN, l); }
  ;

join_type:
    INNER			{ $$ = 0; }
  | outer_join_type opt_outer	{ $$ = 1 + $1; }
  ;

opt_outer:
    /* empty */		{ $$ = 0; }
  | OUTER			{ $$ = 0; }
  ;

outer_join_type:
    LEFT		{ $$ = 0; }
  | RIGHT		{ $$ = 1; }
  | FULL		{ $$ = 2; }
  ;

join_spec:
    ON scalar_exp			{ $$ = $2; }
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

opt_with_clause:
	with_clause
 | /* empty */		{ $$ = NULL; }
 ;

with_clause:
	WITH opt_recursive with_list
	{
		dlist *l = L();
		append_list(l, $3);
		append_symbol(l, NULL); /* filled in later */
		append_int(l, $2);
		$$ = _symbol_create_list( SQL_WITH, l );
	}
  ;

opt_recursive:
   /* empty */  { $$ = false; }
 | RECURSIVE 	{ $$ = true; }
 ;

with_list:
	with_list ',' with_list_element	 { $$ = append_symbol($1, $3); }
 |	with_list_element		 { $$ = append_symbol(L(), $1); }
 ;

with_list_element:
    column_id opt_column_list AS select_with_parens
	{  dlist *l = L();
 	  append_int(l, 0);
	  append_list(l, append_string(L(), $1));
	  append_list(l, $2);
	  append_symbol(l, $4);
	  append_int(l, FALSE);	/* no with check */
	  append_int(l, FALSE);	/* inlined view  (ie not persistent) */
	  append_int(l, FALSE); /* no replace clause */
	  $$ = _symbol_create_list( SQL_CREATE_VIEW, l );
	}
 ;

sql:
   SelectStmt
 ;

SelectStmt:
	select_no_parens		%prec UMINUS
 |	select_with_parens		%prec UMINUS
 ;

select_with_parens:
    '(' select_no_parens ')'		{ $$ = $2; }
 |  '(' select_with_parens ')'		{ $$ = $2; }
 ;

into_clause:
     INTO variable_ref_commalist 	{ $$ = $2; }
 |   /* empty */			{ $$ = NULL; }
 ;

values_clause:
    VALUES '(' expr_list ')'
	{ dlist *l = append_list(L(), $3);
	  $$ = _symbol_create_list(SQL_VALUES, l); }
  | values_clause ',' '(' expr_list ')'
	{ append_list($1->data.lval, $4);
          $$ = $1; }
  ;

simple_select:
    SELECT opt_distinct selection into_clause table_exp
	{ $$ = newSelectNode( SA, $2, $3, $4,
		$5->h->data.sym,
		$5->h->next->data.sym,
		$5->h->next->next->data.sym,
		$5->h->next->next->next->data.sym,
		NULL, NULL, NULL, NULL, NULL, NULL,
		$5->h->next->next->next->next->data.sym,
		$5->h->next->next->next->next->next->data.sym);
	}
  |  values_clause 		{ $$ = $1; }
  |  select_clause UNION set_distinct opt_corresponding select_clause
	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, $3);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  append_int(l, 0);
	  $$ = _symbol_create_list( SQL_UNION, l); }
  |  select_clause OUTER_UNION set_distinct opt_corresponding select_clause
	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, $3);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  append_int(l, 1);
	  $$ = _symbol_create_list( SQL_UNION, l); }
  |  select_clause EXCEPT set_distinct opt_corresponding select_clause
	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, $3);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_EXCEPT, l); }
  |  select_clause INTERSECT set_distinct opt_corresponding select_clause
	{ dlist *l = L();
	  append_symbol(l, $1);
	  append_int(l, $3);
	  append_list(l, $4);
	  append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_INTERSECT, l); }
  ;

select_no_parens:
     simple_select
 |   select_clause order_by_clause
     {
	  $$ = $1;
	  if ($2) {
		if ($1 != NULL &&
		    ($1->token == SQL_SELECT ||
		     $1->token == SQL_UNION  ||
		     $1->token == SQL_EXCEPT ||
		     $1->token == SQL_INTERSECT)) {
			if ($1->token == SQL_SELECT) {
				SelectNode *s = (SelectNode*)$1;
				s -> orderby = $2;
			} else { /* Add extra select * from .. in case of UNION, EXCEPT, INTERSECT */
				$$ = newSelectNode(
					SA, 0,
					append_symbol(L(), _symbol_create_list(SQL_TABLE, append_string(append_string(L(),NULL),NULL))), NULL,
					_symbol_create_list( SQL_FROM, append_symbol(L(), $1)), NULL, NULL, NULL, $2, _symbol_create_list(SQL_NAME, append_list(append_string(L(),"inner"),NULL)), NULL, NULL, NULL, NULL, NULL, NULL);
			}
		} else {
			yyerror(m, "missing SELECT operator");
			YYABORT;
		}
	  }
    }
 |   select_clause opt_order_by_clause limit_clause opt_sample opt_seed
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
				s -> offset = $3?$3->h->next->data.sym:NULL;
				s -> limit = $3?$3->h->data.sym:NULL;
				s -> sample = $4;
				s -> seed = $5;
			} else { /* Add extra select * from .. in case of UNION, EXCEPT, INTERSECT */
				$$ = newSelectNode(
					SA, 0,
					append_symbol(L(), _symbol_create_list(SQL_TABLE, append_string(append_string(L(),NULL),NULL))), NULL,
					_symbol_create_list( SQL_FROM, append_symbol(L(), $1)), NULL, NULL, NULL, $2, _symbol_create_list(SQL_NAME, append_list(append_string(L(),"inner"),NULL)), $3?$3->h->data.sym:NULL, $3?$3->h->next->data.sym:NULL, $4, $5, NULL, NULL);
			}
		} else {
			yyerror(m, "missing SELECT operator");
			YYABORT;
		}
	  }
    }
 |   select_clause opt_order_by_clause sample opt_seed
     {
	  $$ = $1;
	  if ($2 || $3 || $4) {
		if ($1 != NULL &&
		    ($1->token == SQL_SELECT ||
		     $1->token == SQL_UNION  ||
		     $1->token == SQL_EXCEPT ||
		     $1->token == SQL_INTERSECT)) {
			if ($1->token == SQL_SELECT) {
				SelectNode *s = (SelectNode*)$1;
				s -> orderby = $2;
				s -> sample = $3;
				s -> seed = $4;
			} else { /* Add extra select * from .. in case of UNION, EXCEPT, INTERSECT */
				$$ = newSelectNode(
					SA, 0,
					append_symbol(L(), _symbol_create_list(SQL_TABLE, append_string(append_string(L(),NULL),NULL))), NULL,
					_symbol_create_list( SQL_FROM, append_symbol(L(), $1)), NULL, NULL, NULL, $2, _symbol_create_list(SQL_NAME, append_list(append_string(L(),"inner"),NULL)), NULL, NULL, $3, $4, NULL, NULL);
			}
		} else {
			yyerror(m, "missing SELECT operator");
			YYABORT;
		}
	  }
    }
 |   with_clause select_clause
     {
 	$$ = $1;
	$$->data.lval->h->next->data.sym = $2;
     }
 |   with_clause select_clause order_by_clause
     {
 	$$ = $1;
	if ($3) {
		if ($2 != NULL &&
		    ($2->token == SQL_SELECT ||
		     $2->token == SQL_UNION  ||
		     $2->token == SQL_EXCEPT ||
		     $2->token == SQL_INTERSECT)) {
			if ($2->token == SQL_SELECT) {
				SelectNode *s = (SelectNode*)$2;
				s -> orderby = $3;
			} else { /* Add extra select * from .. in case of UNION, EXCEPT, INTERSECT */
				$2 = newSelectNode(
					SA, 0,
					append_symbol(L(), _symbol_create_list(SQL_TABLE, append_string(append_string(L(),NULL),NULL))), NULL,
					_symbol_create_list( SQL_FROM, append_symbol(L(), $2)), NULL, NULL, NULL, $3, _symbol_create_list(SQL_NAME, append_list(append_string(L(),"inner"),NULL)), NULL, NULL, NULL, NULL, NULL, NULL);
			}
		} else {
			yyerror(m, "missing SELECT operator");
			YYABORT;
		}
	}
	$$->data.lval->h->next->data.sym = $2;
     }
 |   with_clause select_clause opt_order_by_clause limit_clause opt_sample opt_seed
     { 
	  $$ = $1;
	  if ($2 || $3 || $4 || $5 || $6) {
		if ($2 != NULL &&
		    ($2->token == SQL_SELECT ||
		     $2->token == SQL_UNION  ||
		     $2->token == SQL_EXCEPT ||
		     $2->token == SQL_INTERSECT)) {
			if ($2->token == SQL_SELECT) {
				SelectNode *s = (SelectNode*)$2;
				s -> orderby = $3;
				s -> offset = $4?$4->h->next->data.sym:NULL;
				s -> limit = $4?$4->h->data.sym:NULL;
				s -> sample = $5;
				s -> seed = $6;
			} else { /* Add extra select * from .. in case of UNION, EXCEPT, INTERSECT */
				$2 = newSelectNode(
					SA, 0,
					append_symbol(L(), _symbol_create_list(SQL_TABLE, append_string(append_string(L(),NULL),NULL))), NULL,
					_symbol_create_list( SQL_FROM, append_symbol(L(), $2)), NULL, NULL, NULL, $3, _symbol_create_list(SQL_NAME, append_list(append_string(L(),"inner"),NULL)), $4?$4->h->data.sym:NULL, $4?$4->h->next->data.sym:NULL, $5, $6, NULL, NULL);
			}
		} else {
			yyerror(m, "missing SELECT operator");
			YYABORT;
		}
	  }
	  $$->data.lval->h->next->data.sym = $2;
     }
 |   with_clause select_clause opt_order_by_clause sample opt_seed
     {
	  $$ = $1;
	  if ($2 || $3 || $4 || $5) {
		if ($2 != NULL &&
		    ($2->token == SQL_SELECT ||
		     $2->token == SQL_UNION  ||
		     $2->token == SQL_EXCEPT ||
		     $2->token == SQL_INTERSECT)) {
			if ($2->token == SQL_SELECT) {
				SelectNode *s = (SelectNode*)$2;
				s -> orderby = $3;
				s -> sample = $4;
				s -> seed = $5;
			} else { /* Add extra select * from .. in case of UNION, EXCEPT, INTERSECT */
				$2 = newSelectNode(
					SA, 0,
					append_symbol(L(), _symbol_create_list(SQL_TABLE, append_string(append_string(L(),NULL),NULL))), NULL,
					_symbol_create_list( SQL_FROM, append_symbol(L(), $2)), NULL, NULL, NULL, $3, _symbol_create_list(SQL_NAME, append_list(append_string(L(),"inner"),NULL)), NULL, NULL, $4, $5, NULL, NULL);
			}
		} else {
			yyerror(m, "missing SELECT operator");
			YYABORT;
		}
	  }
	  $$->data.lval->h->next->data.sym = $2;
    }
 ;

select_clause:
   simple_select      { $$ = $1; }
 | select_with_parens { $$ = $1; }
 ;

set_distinct:
    ALL			{ $$ = FALSE; }
 |  DISTINCT		{ $$ = TRUE; }
 |  /* empty */		{ $$ = TRUE; }
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
    opt_from_clause opt_where_clause opt_group_by_clause opt_having_clause opt_window_clause opt_qualify_clause
	{ $$ = L();
	  append_symbol($$, $1);
	  append_symbol($$, $2);
	  append_symbol($$, $3);
	  append_symbol($$, $4);
	  append_symbol($$, $5);
	  append_symbol($$, $6);
}
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

opt_qualify_clause:
    QUALIFY scalar_exp		{ $$ = $2; }
 |  /* empty */			{ $$ = NULL; }
 ;

opt_from_clause:
    /* empty */			 { $$ = NULL; }
 |  FROM table_ref_commalist	 { $$ = _symbol_create_list( SQL_FROM, $2); }
 ;

table_ref_commalist:
    table_ref		{ $$ = append_symbol(L(), $1); }
 |  table_ref_commalist ',' table_ref
			{ $$ = append_symbol($1, $3); }
 ;

table_ref:
    qname opt_table_name	{ dlist *l = L();
				  append_list(l, $1);
				  append_int(l, 0);
				  append_symbol(l, $2);
				  $$ = _symbol_create_list(SQL_NAME, l); }
 |  string opt_table_name	{
				  dlist *f = L();
				  const char *s = $1;
				  const char *loader = looks_like_url(s) ? "proto_loader" : "file_loader";
				  append_list(f, append_string(L(), loader));
				  append_int(f, FALSE); /* ignore distinct */
				  int len = UTF8_strlen(s);
				  sql_subtype t;
				  sql_find_subtype(&t, "char", len, 0);

				  dlist *args = L();
				  append_symbol(args, _newAtomNode( _atom_string(&t, s)));
				  append_list(f, args);

				  dlist *l = L();
				  append_symbol(l, _symbol_create_list( SQL_NOP, f));

				  append_int(l, 0);
				  append_symbol(l, $2);
				  $$ = _symbol_create_list(SQL_TABLE, l); }
 |  func_ref opt_table_name
			        { dlist *l = L();
				  append_symbol(l, $1);
				  append_int(l, 0);
				  append_symbol(l, $2);
				  $$ = _symbol_create_list(SQL_TABLE, l); }
 |  LATERAL func_ref opt_table_name
			        { dlist *l = L();
				  append_symbol(l, $2);
				  append_int(l, 1);
				  append_symbol(l, $3);
				  $$ = _symbol_create_list(SQL_TABLE, l); }
 |  select_with_parens opt_table_name
				{
				  $$ = $1;
				  if ($$->token == SQL_SELECT) {
					SelectNode *sn = (SelectNode*)$1;
					sn->name = $2;
				  } else if ($$->token == SQL_VALUES || $$->token == SQL_NOP) {
					symbol *s = $$;
			        	dlist *l = L();
				  	append_symbol(l, s);
				  	append_int(l, 0);
				  	append_symbol(l, $2);
				  	$$ = _symbol_create_list(SQL_TABLE, l);
				  } else {
					append_int($$->data.lval, 0);
					append_symbol($$->data.lval, $2);
				  }
				}
 |  LATERAL select_with_parens opt_table_name
				{
				  $$ = $2;
				  if ($$->token == SQL_SELECT) {
					SelectNode *sn = (SelectNode*)$2;
					sn->name = $3;
					sn->lateral = 1;
				  } else if ($$->token == SQL_VALUES || $$->token == SQL_NOP) {
					symbol *s = $$;
			        	dlist *l = L();
				  	append_symbol(l, s);
				  	append_int(l, 1);
				  	append_symbol(l, $3);
				  	$$ = _symbol_create_list(SQL_TABLE, l);
				  } else {	/* setops */
					append_int($$->data.lval, 1);
					append_symbol($$->data.lval, $3);
				  }
				}
 |  joined_table		{ $$ = $1;
				  append_symbol($1->data.lval, NULL); }
 |  '(' joined_table ')' table_name	{ $$ = $2;
				  append_symbol($2->data.lval, $4); }
 ;

table_name:
    AS column_id '(' ident_commalist ')'
				{ dlist *l = L();
				  append_string(l, $2);
				  append_list(l, $4);
				  $$ = _symbol_create_list(SQL_NAME, l); }
 |  AS column_id
				{ dlist *l = L();
				  append_string(l, $2);
				  append_list(l, NULL);
				  $$ = _symbol_create_list(SQL_NAME, l); }
 |  column_id '(' ident_commalist ')'
				{ dlist *l = L();
				  append_string(l, $1);
				  append_list(l, $3);
				  $$ = _symbol_create_list(SQL_NAME, l); }
 |  column_id
				{ dlist *l = L();
				  append_string(l, $1);
				  append_list(l, NULL);
				  $$ = _symbol_create_list(SQL_NAME, l); }
 ;

opt_table_name:
	/* empty */ { $$ = NULL; }
 |  table_name  { $$ = $1; }
 ;

all:
    ALL
 |  '*'
 ;

opt_group_by_clause:
    sqlGROUP BY all 		{ $$ = _symbol_create_list(SQL_GROUPBY, NULL); }
 |  sqlGROUP BY group_by_list 	{ $$ = _symbol_create_list(SQL_GROUPBY, $3); }
 |      /* empty */             { $$ = NULL; }
 ;

group_by_list:
    group_by_element                   { $$ = append_symbol(L(), $1); }
 |  group_by_list ',' group_by_element { $$ = append_symbol($1, $3); }
 ;

group_by_element:
    scalar_exp                              { $$ = _symbol_create_list(SQL_GROUPBY, append_symbol(L(), $1)); }
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
 |  HAVING scalar_exp { $$ = $2; }
 ;

opt_order_by_clause:
    /* empty */			  	{ $$ = NULL; }
 |  order_by_clause			{ $$ = $1; }
 ;

order_by_clause:
    ORDER BY all		  	{ $$ = _symbol_create_list( SQL_ORDERBY, NULL); }
 |  ORDER BY sort_specification_list 	{ $$ = _symbol_create_list( SQL_ORDERBY, $3); }
 ;

first_next:
    FIRST
 |  NEXT
 ;

rows:
    ROW
 |  ROWS
 ;

opt_rows:
   rows
 | /* empty */
 ;

/* TODO add support for limit start, end */
poslng_or_param:
     poslng                { sql_subtype *t = sql_fetch_localtype(TYPE_lng);
			     $$ = _newAtomNode( atom_int(SA, t, $1));
			   }
 |   param                 { $$ = $1; }
 ;

poslng_or_param_rows:
     rows ONLY             { sql_subtype *t = sql_fetch_localtype(TYPE_lng);
		             $$ = _newAtomNode( atom_int(SA, t, 1));
		           }
 |   nonzerolng rows ONLY  { sql_subtype *t = sql_fetch_localtype(TYPE_lng);
			     $$ = _newAtomNode( atom_int(SA, t, $1));
			   }

 |   param rows ONLY 	   { $$ = $1; }
 ;

limit_clause:
     LIMIT poslng_or_param                                              { $$ = append_symbol(append_symbol(L(), $2), NULL); }
 |   LIMIT poslng_or_param OFFSET poslng_or_param	                { $$ = append_symbol(append_symbol(L(), $2), $4); }
 |   OFFSET poslng_or_param opt_rows                                    { $$ = append_symbol(append_symbol(L(), NULL), $2); }
 |   OFFSET poslng_or_param FETCH first_next poslng_or_param_rows       { $$ = append_symbol(append_symbol(L(), $5), $2); }
 |   OFFSET poslng_or_param rows FETCH first_next poslng_or_param_rows  { $$ = append_symbol(append_symbol(L(), $6), $2); }
 |   FETCH first_next poslng_or_param_rows  				{ $$ = append_symbol(append_symbol(L(), $3), NULL); }
 ;

sample:
    SAMPLE poslng	{
			  sql_subtype *t = sql_fetch_localtype(TYPE_lng);
			  $$ = _newAtomNode( atom_int(SA, t, $2));
			}
 |  SAMPLE INTNUM	{
			  sql_subtype *t = sql_fetch_localtype(TYPE_dbl);
			  $$ = _newAtomNode( atom_float(SA, t, strtod($2, NULL)));
			}
 |  SAMPLE param	{ $$ = $2; }
 ;

opt_sample:
	  sample
 | /* empty */	{ $$ = NULL; }
 ;

opt_seed:
	/* empty */	{ $$ = NULL; }
 |  SEED intval	{
			  sql_subtype *t = sql_fetch_localtype(TYPE_int);
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
    scalar_exp opt_asc_desc opt_nulls_first_last
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
    /* empty */	{ $$ = -1; }
 |  NULLS LAST		{ $$ = TRUE; }
 |  NULLS FIRST		{ $$ = FALSE; }
 ;

any_all_some:
    ANY		{ $$ = 0; }
 |  SOME	{ $$ = 0; }
 |  ALL		{ $$ = 1; }
 ;

like_predicate:
    scalar_exp NOT_LIKE scalar_exp			%prec LIKE
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_symbol(l, _symbol_create_list(SQL_ESCAPE, append_symbol(L(), $3)));
		  append_int(l, FALSE);  /* case sensitive */
		  append_int(l, TRUE);  /* anti */
		  $$ = _symbol_create_list( SQL_LIKE, l ); }
 |  scalar_exp NOT_LIKE like_exp			%prec LIKE
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_symbol(l, $3);
		  append_int(l, FALSE);  /* case sensitive */
		  append_int(l, TRUE);  /* anti */
		  $$ = _symbol_create_list( SQL_LIKE, l ); }
 |  scalar_exp NOT_ILIKE scalar_exp			%prec LIKE
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_symbol(l, _symbol_create_list(SQL_ESCAPE, append_symbol(L(), $3)));
		  append_int(l, TRUE);  /* case insensitive */
		  append_int(l, TRUE);  /* anti */
		  $$ = _symbol_create_list( SQL_LIKE, l ); }
 |  scalar_exp NOT_ILIKE like_exp			%prec LIKE
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_symbol(l, $3);
		  append_int(l, TRUE);  /* case insensitive */
		  append_int(l, TRUE);  /* anti */
		  $$ = _symbol_create_list( SQL_LIKE, l ); }
 |  scalar_exp LIKE scalar_exp				%prec LIKE
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_symbol(l, _symbol_create_list(SQL_ESCAPE, append_symbol(L(), $3)));
		  append_int(l, FALSE);  /* case sensitive */
		  append_int(l, FALSE);  /* anti */
		  $$ = _symbol_create_list( SQL_LIKE, l ); }
 |  scalar_exp LIKE like_exp				%prec LIKE
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_symbol(l, $3);
		  append_int(l, FALSE);  /* case sensitive */
		  append_int(l, FALSE);  /* anti */
		  $$ = _symbol_create_list( SQL_LIKE, l ); }
 |  scalar_exp ILIKE scalar_exp				%prec LIKE
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_symbol(l, _symbol_create_list(SQL_ESCAPE, append_symbol(L(), $3)));
		  append_int(l, TRUE);  /* case insensitive */
		  append_int(l, FALSE);  /* anti */
		  $$ = _symbol_create_list( SQL_LIKE, l ); }
 |  scalar_exp ILIKE like_exp				%prec LIKE
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_symbol(l, $3);
		  append_int(l, TRUE);  /* case insensitive */
		  append_int(l, FALSE);  /* anti */
		  $$ = _symbol_create_list( SQL_LIKE, l ); }
 ;

like_exp:
   scalar_exp ESCAPE string
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

 /* odbc like escape */
 |  scalar_exp '{' ESCAPE string '}'
    {
	const char* esc = $4;
	if (_strlen(esc) != 1) {
		    sqlformaterror(m, SQLSTATE(22019) "%s", "ESCAPE must be one character");
	    $$= NULL;
	    YYABORT;
	} else {
	    dlist *l = L();
	    append_symbol(l, $1);
	    append_string(l, esc);
	    $$ = _symbol_create_list(SQL_ESCAPE, l);
	}
    }
 ;

filter_arg_list:
       scalar_exp				{ $$ = append_symbol(L(), $1); }
 |     filter_arg_list ',' scalar_exp	{ $$ = append_symbol($1, $3);  }
 ;

filter_args:
	'[' filter_arg_list ']'	{ $$ = $2; }
 ;

filter_exp:
 filter_args qname filter_args
		{ dlist *l = L();
		  append_list(l, $1);
		  append_list(l, $2);
		  append_list(l, $3);
		  $$ = _symbol_create_list(SQL_FILTER, l ); }
 ;

/* expressions */
scalar_exp:
    value_exp
 |  scalar_exp '+' scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_add")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp '-' scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_sub")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp '*' scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_mul")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp '/' scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_div")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp '%' scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "mod")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp '^' scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "bit_xor")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp '&' scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "bit_and")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
  | scalar_exp CONCATSTRING scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "concat")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 | scalar_exp GEOM_OVERLAP scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "mbr_overlap")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 | scalar_exp GEOM_OVERLAP_OR_LEFT scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "mbr_overlap_or_left")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 | scalar_exp  GEOM_OVERLAP_OR_RIGHT scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "mbr_overlap_or_right")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 | scalar_exp GEOM_OVERLAP_OR_BELOW scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "mbr_overlap_or_below")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 | scalar_exp GEOM_BELOW scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "mbr_below")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 | scalar_exp GEOM_OVERLAP_OR_ABOVE scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "mbr_overlap_or_above")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 | scalar_exp GEOM_ABOVE scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "mbr_above")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 | scalar_exp GEOM_DIST scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "mbr_distance")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp AT scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "mbr_contained")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp '|' scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "bit_or")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp '~' scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "mbr_contains")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp GEOM_MBR_EQUAL scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "mbr_equal")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  '~' scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "bit_not")));
			  append_int(l, FALSE); /* ignore distinct */
	  		  append_list(l, append_symbol(L(), $2));
	  		  $$ = _symbol_create_list( SQL_NOP, l );
			}
 |  scalar_exp LEFT_SHIFT scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "left_shift")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp RIGHT_SHIFT scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "right_shift")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp LEFT_SHIFT_ASSIGN scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "left_shift_assign")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp RIGHT_SHIFT_ASSIGN scalar_exp
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "right_shift_assign")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
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
	  		  	append_list(l, append_symbol(L(), $2));
	  		  	$$ = _symbol_create_list( SQL_NOP, l );
			  }
			}
  | scalar_exp AND scalar_exp					 	%prec AND
		{
		  if ($1->token == SQL_AND) {
 			append_symbol($1->data.lval, $3);
			$$ = $1;
		  } else {
 		  	dlist *l = L();
		  	append_symbol(l, $1);
		  	append_symbol(l, $3);
		  	$$ = _symbol_create_list(SQL_AND, l );
		  }
		}
  | scalar_exp OR scalar_exp
		{
		  if ($1->token == SQL_OR) {
 			append_symbol($1->data.lval, $3);
			$$ = $1;
		  } else {
 		  	dlist *l = L();
		  	append_symbol(l, $1);
		  	append_symbol(l, $3);
		  	$$ = _symbol_create_list(SQL_OR, l );
		  }
		}
  | NOT scalar_exp 	{ $$ = _symbol_create_symbol(SQL_NOT, $2); }
  | like_predicate
  | filter_exp
  | scalar_exp COMPARISON scalar_exp					%prec COMPARISON
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, $2);
		  append_symbol(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 | scalar_exp COMPARISON any_all_some select_with_parens		%prec COMPARISON
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, $2);
		  append_symbol(l, $4);
		  append_int(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 | scalar_exp COMPARISON any_all_some '(' scalar_exp ')'		%prec COMPARISON
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, $2);
		  append_symbol(l, $5);
		  append_int(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 |  scalar_exp '=' scalar_exp						%prec '='
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, sa_strdup(SA, "="));
		  append_symbol(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 | scalar_exp '=' any_all_some select_with_parens			%prec '='
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, sa_strdup(SA, "="));
		  append_symbol(l, $4);
		  append_int(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 | scalar_exp '=' any_all_some '(' scalar_exp ')'			%prec '='
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, sa_strdup(SA, "="));
		  append_symbol(l, $5);
		  append_int(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 | scalar_exp IS NOT DISTINCT FROM scalar_exp				%prec IS
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, sa_strdup(SA, "="));
		  append_symbol(l, $6);
		  append_int(l, 2);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 | scalar_exp IS DISTINCT FROM scalar_exp				%prec IS
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, sa_strdup(SA, "<>"));
		  append_symbol(l, $5);
		  append_int(l, 3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
  | scalar_exp IS NOT sqlNULL { $$ = _symbol_create_symbol( SQL_IS_NOT_NULL, $1 );}
  | scalar_exp IS sqlNULL     { $$ = _symbol_create_symbol( SQL_IS_NULL, $1 ); }
  | scalar_exp NOT_IN in_expr
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_list(l, $3);
		  $$ = _symbol_create_list(SQL_NOT_IN, l ); }
  | scalar_exp sqlIN in_expr
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_list(l, $3);
		  $$ = _symbol_create_list(SQL_IN, l ); }
  | scalar_exp NOT_BETWEEN SYMMETRIC scalar_exp_no_and AND scalar_exp  %prec NOT_BETWEEN
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_int(l, 1);
		  append_symbol(l, $4);
		  append_symbol(l, $6);
		  $$ = _symbol_create_list(SQL_NOT_BETWEEN, l ); }
  | scalar_exp NOT_BETWEEN asymmetric scalar_exp_no_and AND scalar_exp  %prec NOT_BETWEEN
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_int(l, 0);
		  append_symbol(l, $4);
		  append_symbol(l, $6);
		  $$ = _symbol_create_list(SQL_NOT_BETWEEN, l ); }
 |  scalar_exp BETWEEN SYMMETRIC scalar_exp_no_and AND scalar_exp  	%prec BETWEEN
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_int(l, 1);
		  append_symbol(l, $4);
		  append_symbol(l, $6);
		  $$ = _symbol_create_list(SQL_BETWEEN, l ); }
 |  scalar_exp BETWEEN asymmetric scalar_exp_no_and AND scalar_exp  	%prec BETWEEN
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_int(l, 0);
		  append_symbol(l, $4);
		  append_symbol(l, $6);
		  $$ = _symbol_create_list(SQL_BETWEEN, l ); }
 |  DEFAULT	{ $$ = _symbol_create(SQL_DEFAULT, NULL ); }
 ;

in_expr:
     select_with_parens { $$ = append_symbol(L(), $1); }
 | '(' expr_list ')'	{ $$ = $2; }
 ;

asymmetric:
   /* empty */	{ $$ = 0; }
 | ASYMMETRIC	{ $$ = 0; }
 ;

scalar_exp_no_and:
    value_exp		{ $$ = $1; }
 |  '+' scalar_exp_no_and %prec UMINUS
			{ $$ = $2; }
 |  '-' scalar_exp_no_and %prec UMINUS
			{
			  $$ = NULL;
			  assert(($2->token != SQL_COLUMN && $2->token != SQL_IDENT) || $2->data.lval->h->type != type_lng);
			  if (!$$) {
				dlist *l = L();
				append_list(l,
					append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_neg")));
				append_int(l, FALSE); /* ignore distinct */
	  		  	append_list(l, append_symbol(L(), $2));
	  		  	$$ = _symbol_create_list( SQL_NOP, l );
			  }
			}
 |  scalar_exp_no_and '+' scalar_exp_no_and
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_add")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp_no_and '-' scalar_exp_no_and
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_sub")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp_no_and '*' scalar_exp_no_and
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_mul")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp_no_and '/' scalar_exp_no_and
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "sql_div")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp_no_and '%' scalar_exp_no_and
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "mod")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  scalar_exp_no_and '^' scalar_exp_no_and
			{ dlist *l = L();
			  append_list(l, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "bit_xor")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $1);
			  append_symbol(args, $3);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 | scalar_exp_no_and COMPARISON scalar_exp_no_and
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, $2);
		  append_symbol(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 | scalar_exp_no_and '=' scalar_exp_no_and
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, sa_strdup(SA, "="));
		  append_symbol(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 | scalar_exp_no_and COMPARISON any_all_some '(' scalar_exp ')'		%prec COMPARISON
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, $2);
		  append_symbol(l, $5);
		  append_int(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 | scalar_exp_no_and '=' any_all_some select_with_parens			%prec '='
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, sa_strdup(SA, "="));
		  append_symbol(l, $4);
		  append_int(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 | scalar_exp_no_and '=' any_all_some '(' scalar_exp ')'			%prec '='
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, sa_strdup(SA, "="));
		  append_symbol(l, $5);
		  append_int(l, $3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 | scalar_exp_no_and IS NOT DISTINCT FROM scalar_exp_no_and		%prec IS
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, sa_strdup(SA, "="));
		  append_symbol(l, $6);
		  append_int(l, 2);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 | scalar_exp_no_and IS DISTINCT FROM scalar_exp_no_and			%prec IS
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, sa_strdup(SA, "<>"));
		  append_symbol(l, $5);
		  append_int(l, 3);
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 ;

opt_over:
	OVER '(' window_specification ')' { $$ = _symbol_create_list(SQL_WINDOW, append_list(L(), $3)); }
 |  OVER ident                        { $$ = _symbol_create_list(SQL_NAME, append_string(L(), $2)); }
 |                                    { $$ = NULL; }
 ;

value_exp:
    atom
 |  cast_exp
 |  aggr_or_window_ref opt_over {
		if ($2 && $2->token == SQL_NAME)
			$$ = _symbol_create_list(SQL_WINDOW, append_string(append_symbol(L(), $1), $2->data.lval->h->data.sval));
		else if ($2)
			$$ = _symbol_create_list(SQL_WINDOW, append_list(append_symbol(L(), $1), $2->data.lval->h->data.lval));
		else
			$$ = $1;
	}
 |  case_exp
 |  column_ref       { $$ = _symbol_create_list(SQL_COLUMN, $1); }
 |  '(' scalar_exp ')'{ $$ = $2; }
 |  '(' expr_list ',' scalar_exp ')'{ $$ = _symbol_create_list(SQL_VALUES, append_list(L(), append_symbol($2, $4))); }
 |  session_user     { $$ = _symbol_create_list(SQL_NAME, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_user"))); }
 |  CURRENT_SCHEMA   { $$ = _symbol_create_list(SQL_NAME, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_schema"))); }
 |  CURRENT_ROLE     { $$ = _symbol_create_list(SQL_NAME, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_role"))); }
 |  CURRENT_TIMEZONE { $$ = _symbol_create_list(SQL_NAME, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_timezone"))); }
 |  datetime_funcs
 |  EXISTS select_with_parens		{ $$ = _symbol_create_symbol( SQL_EXISTS, $2 ); }
 |  NOT_EXISTS select_with_parens	{ $$ = _symbol_create_symbol( SQL_NOT_EXISTS, $2 ); }
 |  GROUPING '(' column_ref_commalist ')'
	{ dlist *l = L();
		append_list(l, append_string(L(), "grouping"));
		append_int(l, FALSE); /* ignore distinct */
		append_list(l, $3);
		$$ = _symbol_create_list(SQL_AGGR, l); }
 |  NEXT VALUE FOR qname                  { $$ = _symbol_create_list(SQL_NEXT, $4); }
 |  null
 |  param
 |  string_funcs
 |  XML_value_function
 |  odbc_scalar_func_escape
 |  select_with_parens  %prec UMINUS { $$ = $1; }
 ;

param:
   '?'
	{
	  int nr = (m->params)?list_length(m->params):0;

	  sql_add_param(m, NULL, NULL);
	  $$ = _symbol_create_int( SQL_PARAMETER, nr );
	}
  | ':' ident
	{
	  int nr = sql_bind_param( m, $2);

	  if (nr < 0) {
		nr = (m->params)?list_length(m->params):0;
		sql_add_param(m, $2, NULL);
	  }
	  $$ = _symbol_create_int( SQL_PARAMETER, nr );
	}
  ;

window_specification:
	window_ident_clause window_partition_clause window_order_clause window_frame_clause
	{ $$ = append_symbol(append_symbol(append_symbol(append_string(L(), $1), $2), $3), $4); }
  ;

window_ident_clause:
	/* empty */ { $$ = NULL; }
  |	ident       { $$ = $1; }
  ;

window_partition_clause:
	/* empty */	{ $$ = NULL; }
  |	PARTITION BY expr_list
	{ $$ = _symbol_create_list( SQL_GROUPBY, $3 ); }
  ;

window_order_clause:
	/* empty */	{ $$ = NULL; }
  |	ORDER BY sort_specification_list
	{ $$ = _symbol_create_list( SQL_ORDERBY, $3 ); }
  ;

window_frame_clause:
	/* empty */	{ $$ = NULL; }
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
  | scalar_exp PRECEDING { dlist *l2 = append_symbol(L(), $1);
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
  | scalar_exp FOLLOWING { dlist *l2 = append_symbol(L(), $1);
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
	  append_list(l, NULL);
	  $$ = _symbol_create_list( SQL_NOP, l ); }
|   qfunc '(' expr_list ')'
	{ dlist *l = L();
	  append_list(l, $1);
	  append_int(l, FALSE); /* ignore distinct */
	  append_list(l, $3);
	  $$ = _symbol_create_list( SQL_NOP, l );
	}
 ;

qfunc:
	type_function_name	{ $$ = append_string(L(), $1); }
 |      column_id '.' column_id	{ $$ = append_string(
					append_string(L(), $1), $3);}
 ;

datetime_funcs:
    EXTRACT '(' extract_datetime_field FROM scalar_exp ')'
			{ dlist *l = L();
			  const char *ident = datetime_field((itype)$3);
			  append_list(l, append_string(L(), sa_strdup(SA, ident)));
			  append_int(l, FALSE); /* ignore distinct */
	  		  append_list(l, append_symbol(L(), $5));
	  		  $$ = _symbol_create_list( SQL_NOP, l );
			}
 |  CURRENT_DATE opt_brackets
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "current_date")));
			  append_int(l, FALSE); /* ignore distinct */
		  	  append_list(l, NULL);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  CURRENT_TIME opt_brackets
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "current_time")));
			  append_int(l, FALSE); /* ignore distinct */
		  	  append_list(l, NULL);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  CURRENT_TIMESTAMP opt_brackets
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "current_timestamp")));
			  append_int(l, FALSE); /* ignore distinct */
		  	  append_list(l, NULL);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  LOCALTIME opt_brackets
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "localtime")));
			  append_int(l, FALSE); /* ignore distinct */
		  	  append_list(l, NULL);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  LOCALTIMESTAMP opt_brackets
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "localtimestamp")));
			  append_int(l, FALSE); /* ignore distinct */
		  	  append_list(l, NULL);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 ;

opt_brackets:
   /* empty */	{ $$ = 0; }
 | '(' ')'	{ $$ = 1; }
 ;

trim_list:
	scalar_exp FROM expr_list 	{ $$ = append_symbol($3, $1); }
  | 	FROM expr_list 			{ $$ = $2; }
  |	expr_list			{ $$ = $$; }
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
			  append_list(l, append_string(L(), sa_strdup(SA, "substring")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $3);
			  append_symbol(args, $5);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
  | SUBSTRING '(' scalar_exp ',' scalar_exp ')'
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "substring")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $3);
			  append_symbol(args, $5);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
  | POSITION '(' scalar_exp_no_and sqlIN scalar_exp_no_and ')'
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "locate")));
			  append_int(l, FALSE); /* ignore distinct */
			  dlist *args = append_symbol(L(), $3);
			  append_symbol(args, $5);
			  append_list(l, args);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
  | SPLIT_PART '(' scalar_exp ',' scalar_exp ',' scalar_exp ')'
			{ dlist *l = L();
			  dlist *ops = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "splitpart")));
			  append_int(l, FALSE); /* ignore distinct */
			  append_symbol(ops, $3);
			  append_symbol(ops, $5);
			  append_symbol(ops, $7);
			  append_list(l, ops);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
| TRIM '(' trim_list ')'
			{ dlist *l = L();
			  append_list(l, append_string(L(), sa_strdup(SA, "btrim")));
			  append_int(l, FALSE); /* ignore distinct */
			  append_list(l, $3);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
| TRIM '(' BOTH trim_list ')'
			{ dlist *l = L();
			  append_list(l, append_string(L(), "btrim"));
			  append_int(l, FALSE); /* ignore distinct */
			  append_list(l, $4);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
| TRIM '(' LEADING trim_list ')'
			{ dlist *l = L();
			  append_list(l, append_string(L(), "ltrim"));
			  append_int(l, FALSE); /* ignore distinct */
			  append_list(l, $4);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
| TRIM '(' TRAILING trim_list ')'
			{ dlist *l = L();
			  append_list(l, append_string(L(), "ltrim"));
			  append_int(l, FALSE); /* ignore distinct */
			  append_list(l, $4);
			  $$ = _symbol_create_list( SQL_NOP, l ); }
 ;

column_exp_commalist:
    column_exp		{ $$ = append_symbol(L(), $1 ); }
 |  column_exp_commalist ',' column_exp
			{ $$ = append_symbol( $1, $3 ); }
 ;

column_exp:
    scalar_exp AS column_label
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, $3);
		  $$ = _symbol_create_list( SQL_COLUMN, l ); }
 |  scalar_exp bare_column_label
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, $2);
		  $$ = _symbol_create_list( SQL_COLUMN, l ); }
 |  scalar_exp
		{ dlist *l = L();
		  append_symbol(l, $1);
		  append_string(l, NULL);
		  $$ = _symbol_create_list( SQL_COLUMN, l ); }
 |  '*'
		{ dlist *l = L();
		  append_string(l, NULL);
		  append_string(l, NULL);
		  $$ = _symbol_create_list( SQL_TABLE, l ); }
 ;

alias_name:
	  AS column_label	{ $$ = $2; }
 |        bare_column_label	{ $$ = $1; }
 ;

opt_alias_name:
    /* empty */	{ $$ = NULL; }
 |  alias_name  { $$ = $1; }
 ;

atom:
    literal
 ;

opt_within_group:
	WITHIN sqlGROUP '(' order_by_clause ')' /* TODO add where part */ 	{ $$ = $4; }
 | 	/* empty */								{ $$ = NULL; }
 ;

aggr_or_window_ref:
    qfunc '(' '*' ')'
		{ dlist *l = L();
		  append_list(l, $1);
		  append_int(l, FALSE); /* ignore distinct */
		  append_list(l, NULL);
		  $$ = _symbol_create_list(SQL_AGGR, l ); }
 |  qfunc '(' column_id '.' '*' ')'
		{ dlist *l = L();
		  append_list(l, $1);
		  append_int(l, FALSE); /* ignore distinct */
		  append_list(l, NULL);
		  $$ = _symbol_create_list(SQL_AGGR, l ); }
 |  qfunc '(' ')'
		{ dlist *l = L();
		  append_list(l, $1);
		  append_int(l, FALSE); /* ignore distinct */
		  append_list(l, NULL);
		  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  qfunc '(' expr_list opt_order_by_clause ')' opt_within_group
		{ dlist *l = L();
		  append_list(l, $1);
		  append_int(l, FALSE); /* ignore distinct */
		  if ($4 && $6) {
			yyerror(m, "Cannot have both order by clause and within group clause");
			YYABORT;
		  }
		  append_list(l, $3);
		  if ($4)
		  	append_symbol(l, $4);
		  else
		  	append_symbol(l, $6);
		  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  qfunc '(' all_distinct expr_list opt_order_by_clause ')' opt_within_group
		{ dlist *l = L();
		  append_list(l, $1);
		  append_int(l, $3);
		  if ($5 && $7) {
			yyerror(m, "Cannot have both order by clause and within group clause");
			YYABORT;
		  }
		  append_list(l, $4);
		  if ($5)
		  	append_symbol(l, $5);
		  else
		  	append_symbol(l, $7);
		  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  XML_aggregate
 ;

opt_sign:
   '+'		{ $$ = 1; }
 | '-'		{ $$ = -1; }
 | /* empty */	{ $$ = 1; }
 ;

tz:
	WITH_LA TIME ZONE		{ $$ = 1; }
 |	WITHOUT TIME ZONE	{ $$ = 0; } /* the default */
 | /* empty */			{ $$ = 0; }
 ;

/* note: the maximum precision for interval, time and timestamp should be equal
 *       and at minimum 6.  The SQL standard prescribes that at least two
 *       fractional precisions are supported: 0 and 6, where 0 is the
 *       default for time, and 6 the default precision for timestamp and interval.
 *       It might be nice to check for a certain maximum of precision in
 *       the future here.
 */
time_precision:
	'(' intval ')'	{ $$ = $2+1; }
/* a time defaults to a fractional precision of 0 */
 | /* empty */		{ $$ = 0+1; }
 ;

timestamp_precision:
	'(' intval ')'	{ $$ = $2+1; }
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

end_field:
    non_second_datetime_field
		{ $$ = append_int( append_int( L(), $1), 0);  }
 |  SECOND timestamp_precision
		{ $$ = append_int( append_int( L(), isec), $2-1);  }
 ;

single_datetime_field:
    non_second_datetime_field time_precision
		{ $$ = append_int( append_int( L(), $1), $2-1);  }
 |  SECOND timestamp_precision
		{ $$ = append_int( append_int( L(), isec), $2-1);  }
 ;

interval_qualifier:
    non_second_datetime_field time_precision TO_LA end_field
	{ $$ =  append_list(append_list( L(), append_int( append_int( L(), $1), $2-1)), $4 ); }
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

literal:
    string	{ const char *s = $1;
		  int len = UTF8_strlen(s);
		  sql_subtype t;
		  sql_find_subtype(&t, "varchar", len, 0 );
		  $$ = _newAtomNode( _atom_string(&t, s)); }

 |  BINARYNUM { int len = _strlen($1), i = 2, err = 0;
		  char * binary = $1;
		  sql_subtype t;
#ifdef HAVE_HGE
		  hge res = 0;
#else
		  lng res = 0;
#endif
		  /* skip leading '0' */
		  while (i < len && binary[i] == '0')
			i++;

		  if (len - i < MAX_OCT_DIGITS || (len - i == MAX_OCT_DIGITS && binary[i] < '2'))
			while (err == 0 && i < len)
			{
				if (binary[i] == '_') {
					i++;
					continue;
				}
				res <<= 1;
				if (binary[i] == '0' || binary[i] == '1') // TODO: an be asserted
					res = res + (binary[i] - '0');
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
				sql_find_subtype(&t, "tinyint", 7, 0 );
			else if (res <= GDK_sht_max)
				sql_find_subtype(&t, "smallint", 15, 0 );
			else if (res <= GDK_int_max)
				sql_find_subtype(&t, "int", 31, 0 );
			else if (res <= GDK_lng_max)
				sql_find_subtype(&t, "bigint", 63, 0 );
#ifdef HAVE_HGE
			else if (res <= GDK_hge_max)
				sql_find_subtype(&t, "hugeint", 127, 0 );
#endif
			else
				err = 1;
		  }

		  if (err != 0) {
			sqlformaterror(m, SQLSTATE(22003) "Invalid binary number or binary too large (%s)", $1);
			$$ = NULL;
			YYABORT;
		  } else {
			$$ = _newAtomNode( atom_int(SA, &t, res));
		  }

 }
 |  OCTALNUM { int len = _strlen($1), i = 2, err = 0;
		  char * octal = $1;
		  sql_subtype t;
#ifdef HAVE_HGE
		  hge res = 0;
#else
		  lng res = 0;
#endif
		  /* skip leading '0' */
		  while (i < len && octal[i] == '0')
			i++;

		  if (len - i < MAX_OCT_DIGITS || (len - i == MAX_OCT_DIGITS && octal[i] < '8'))
			while (err == 0 && i < len)
			{
				if (octal[i] == '_') {
					i++;
					continue;
				}
				res <<= 3;
				if ('0' <= octal[i] && octal[i] < '8')
					res = res + (octal[i] - '0');
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
				sql_find_subtype(&t, "tinyint", 7, 0 );
			else if (res <= GDK_sht_max)
				sql_find_subtype(&t, "smallint", 15, 0 );
			else if (res <= GDK_int_max)
				sql_find_subtype(&t, "int", 31, 0 );
			else if (res <= GDK_lng_max)
				sql_find_subtype(&t, "bigint", 63, 0 );
#ifdef HAVE_HGE
			else if (res <= GDK_hge_max)
				sql_find_subtype(&t, "hugeint", 127, 0 );
#endif
			else
				err = 1;
		  }

		  if (err != 0) {
			sqlformaterror(m, SQLSTATE(22003) "Invalid octal number or octal too large (%s)", $1);
			$$ = NULL;
			YYABORT;
		  } else {
			$$ = _newAtomNode( atom_int(SA, &t, res));
		  }

 }
 |  HEXADECIMALNUM { int len = _strlen($1), i = 2, err = 0; 
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
				if (hexa[i] == '_') {
					i++;
					continue;
				}
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
				sql_find_subtype(&t, "tinyint", 7, 0 );
			else if (res <= GDK_sht_max)
				sql_find_subtype(&t, "smallint", 15, 0 );
			else if (res <= GDK_int_max)
				sql_find_subtype(&t, "int", 31, 0 );
			else if (res <= GDK_lng_max)
				sql_find_subtype(&t, "bigint", 63, 0 );
#ifdef HAVE_HGE
			else if (res <= GDK_hge_max)
				sql_find_subtype(&t, "hugeint", 127, 0 );
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

		  if (lngFromStr(SA, $1, &len, &p, false) < 0 || is_lng_nil(value))
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
		{ 
		  int err = 0;
#ifdef HAVE_HGE
		  hge value, *p = &value;
		  size_t len = sizeof(hge);
#else
		  lng value, *p = &value;
		  size_t len = sizeof(lng);
#endif
		  sql_subtype t;

#ifdef HAVE_HGE
		  if (hgeFromStr(SA, $1, &len, &p, false) < 0 || is_hge_nil(value))
			err = 2;
#else
		  if (lngFromStr(SA, $1, &len, &p, false) < 0 || is_lng_nil(value))
			err = 2;
#endif

		  /* find the most suitable data type for the given number */
		  if (!err) {
		    int bits = number_bits(value);

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
			sqlformaterror(m, SQLSTATE(22003) "Integer value too large or not a number (%s)", $1);
			$$ = NULL;
			YYABORT;
		  } else {
			$$ = _newAtomNode( atom_int(SA, &t, value));
		  }
		}
 |  INTNUM
		{
			char *s = $1;

			int digits;
			int scale;
			int has_errors;
			sql_subtype t;

			DEC_TPE value = decimal_from_str(s, &digits, &scale, &has_errors);

			if (!has_errors && digits <= MAX_DEC_DIGITS) {
				/* The float-like value seems to fit in decimal storage */
				sql_find_subtype(&t, "decimal", digits, scale );
				$$ = _newAtomNode( atom_dec(SA, &t, value));
			}
			else {
				/*
				* The float-like value either doesn't fit in integer decimal storage
				* or it is not a valid float representation.
				*/
				char *p = s;
				double val;

				errno = 0;
				val = strtod(s,&p);
				if (p == s || is_dbl_nil(val) || (errno == ERANGE && (val < -1 || val > 1))) {
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
		{
		  char *filtered = strdup($1);
		  if (filtered == NULL) {
			  sqlformaterror(m, SQLSTATE(HY013) "Malloc failed");
			  $$ = NULL;
			  YYABORT;
		  }
		  int j = 0;
		  for (int i = 0; $1[i]; i++) {
			  char d = $1[i];
			  if (d == '_')
				  continue;
			  filtered[j++] = d;
		  }
		  filtered[j] = 0;
		  sql_subtype t;
		  char *p = filtered;
		  double val;

		  errno = 0;
		  val = strtod(filtered,&p);
		  free(filtered);
		  if (p == filtered || is_dbl_nil(val) || (errno == ERANGE && (val < -1 || val > 1))) {
			sqlformaterror(m, SQLSTATE(22003) "Double value too large or not a number (%s)", $1);
			$$ = NULL;
			YYABORT;
		  }
		  sql_find_subtype(&t, "double", 51, 0 );
		  $$ = _newAtomNode(atom_float(SA, &t, val)); }
 |  sqlDATE string
	{
	    symbol* node = makeAtomNode(m, "date", $2, 0, 0, false);
	    if (node == NULL)
		YYABORT;
	    $$ = node;
	}
 |  odbc_date_escape
 |  TIME time_precision tz string
		{ sql_subtype t;
		  atom *a;
		  int r;
		  int precision = $2;

		  if (precision == 1 && strlen($4) > 9)
			precision += (int) strlen($4) - 9;
		  r = sql_find_subtype(&t, ($3)?"timetz":"time", precision, 0);
		  if (!r || (a = atom_general(SA, &t, $4, m->timezone)) == NULL) {
			sqlformaterror(m, SQLSTATE(22007) "Incorrect time value (%s)", $4);
			$$ = NULL;
			YYABORT;
		  } else {
			$$ = _newAtomNode(a);
		} }
 |  odbc_time_escape
 |  TIMESTAMP timestamp_precision tz string
		{ sql_subtype t;
		  atom *a;
		  int r;

		  r = sql_find_subtype(&t, ($3)?"timestamptz":"timestamp",$2,0);
		  if (!r || (a = atom_general(SA, &t, $4, m->timezone)) == NULL) {
			sqlformaterror(m, SQLSTATE(22007) "Incorrect timestamp value (%s)", $4);
			$$ = NULL;
			YYABORT;
		  } else {
			$$ = _newAtomNode(a);
		} }
 |  odbc_timestamp_escape
 |  interval_expression
 |  odbc_interval_escape
 |  blob string
		{ sql_subtype t;
		  atom *a= 0;
		  int r;

		  $$ = NULL;
		  r = sql_find_subtype(&t, "blob", 0, 0);
		  if (r && (a = atom_general(SA, &t, $2, m->timezone)) != NULL)
			$$ = _newAtomNode(a);
		  if (!$$) {
			sqlformaterror(m, SQLSTATE(22M28) "Incorrect blob (%s)", $2);
			YYABORT;
		  }
		}
 |  blobstring
		{ sql_subtype t;
		  atom *a= 0;
		  int r;

		  $$ = NULL;
		  r = sql_find_subtype(&t, "blob", 0, 0);
		  if (r && (a = atom_general(SA, &t, $1, m->timezone)) != NULL)
			$$ = _newAtomNode(a);
		  if (!$$) {
			sqlformaterror(m, SQLSTATE(22M28) "Incorrect blob (%s)", $1);
			YYABORT;
		  }
		}
 |  aTYPE string
		{ sql_subtype t;
		  atom *a = NULL;
		  int r;

		  if (!(r = sql_find_subtype(&t, $1, 0, 0))) {
			sqlformaterror(m, SQLSTATE(22000) "Type (%s) unknown", $1);
			YYABORT;
		  }
		  if (!(a = atom_general(SA, &t, $2, m->timezone))) {
			sqlformaterror(m, SQLSTATE(22000) "Incorrect %s (%s)", $1, $2);
			YYABORT;
		  }
		  $$ = _newAtomNode(a);
		}
 |  ident string
		{
		  sql_type *t = NULL;
		  sql_subtype tpe;
		  atom *a = NULL;

		  if (!(t = mvc_bind_type(m, $1))) {
			sqlformaterror(m, SQLSTATE(22000) "Type (%s) unknown", $1);
			YYABORT;
		  }
		  sql_init_subtype(&tpe, t, 0, 0);
		  if (!(a = atom_general(SA, &tpe, $2, m->timezone))) {
			sqlformaterror(m, SQLSTATE(22000) "Incorrect %s (%s)", $1, $2);
			YYABORT;
		  }
		  $$ = _newAtomNode(a);
		}
 | sqlBOOL string
		{ sql_subtype t;
		  atom *a = NULL;
		  int r;

		  if (!(r = sql_find_subtype(&t, "boolean", 0, 0))) {
			sqlformaterror(m, SQLSTATE(22000) "Type (%s) unknown", $1);
			YYABORT;
		  }
		  if (!(a = atom_general(SA, &t, $2, m->timezone))) {
			sqlformaterror(m, SQLSTATE(22000) "Incorrect %s (%s)", $1, $2);
			YYABORT;
		  }
		  $$ = _newAtomNode(a);
		}
 |  odbc_guid_escape
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
   INTERVAL /*opt_sign*/ string interval_qualifier {
		sql_subtype t;
		int sk, ek, sp, ep, tpe;
		lng i = 0;
		int r = 0;

		$$ = NULL;
		if ( (tpe = parse_interval_qualifier( m, $3, &sk, &ek, &sp, &ep )) < 0){
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
		if (!r || (tpe = parse_interval( m, 1, $2, sk, ek, sp, ep, &i)) < 0) {
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
    column_id			{ $$ = append_string(L(), $1); }
 |  column_id '.' column_id		{ m->scanner.schema = $1;
				  $$ = append_string(
					append_string(L(), $1), $3);}
 |  column_id '.' column_id '.' column_id	{ m->scanner.schema = $1;
				  $$ = append_string(
					append_string(
						append_string(L(), $1), $3),
					$5);}
 ;

column_ref:
    column_id	{ $$ = append_string(L(), $1); }

 |  column_id '.' column_id	{ $$ = append_string(
				append_string(
				 L(), $1), $3);}

 |  column_id '.' column_id '.' column_id
			{ $$ = append_string(
				append_string(
				 append_string(
				  L(), $1), $3), $5);}
  |  column_id '.' '*'	{ $$ = append_string(
				append_string(
				 L(), $1), NULL); }
 ;

variable_ref:
    variable		{ $$ = append_string(
				L(), $1); }

 |  column_id '.' column_id	{ $$ = append_string(
				append_string(
				 L(), $1), $3);}
 ;

cast_exp:
     CAST '(' scalar_exp AS data_type ')'
	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_type(l, &$5);
	  $$ = _symbol_create_list( SQL_CAST, l ); }
 |
     CONVERT '(' scalar_exp ',' data_type ')'
	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_type(l, &$5);
	  $$ = _symbol_create_list( SQL_CAST, l ); }
 ;

case_exp:
     NULLIF '(' scalar_exp ',' scalar_exp ')'
		{ $$ = _symbol_create_list(SQL_NULLIF,
		   append_symbol(
		    append_symbol(
		     L(), $3), $5)); }
 |   IFNULL '(' scalar_exp ',' scalar_exp ')'
		{ $$ = _symbol_create_list(SQL_COALESCE,
		   append_symbol(
		    append_symbol(
		     L(), $3), $5)); }
 |   COALESCE '(' expr_list ')'
		{ $$ = _symbol_create_list(SQL_COALESCE, $3); }
 |   CASE case_arg when_value_list case_opt_else END
		{ dlist *l = L();
 		  if ($2)
		  	l = append_symbol(l, $2);
		  l = append_list(l, $3);
		  l = append_symbol(l, $4);
		  $$ = _symbol_create_list(SQL_CASE, l); }
 ;

when_value_list:
    when_value
			{ $$ = append_symbol( L(), $1);}
 |  when_value_list when_value
			{ $$ = append_symbol( $1, $2); }
 ;

when_value:
    WHEN scalar_exp THEN scalar_exp
			{ $$ = _symbol_create_list( SQL_WHEN,
			   append_symbol(
			    append_symbol(
			     L(), $2),$4)); }
 ;

case_arg:
    scalar_exp		{ $$ = $1; }
 |  /* empty */	        { $$ = NULL; }
 ;

case_opt_else:
    ELSE scalar_exp	{ $$ = $2; }
 |  /* empty */	        { $$ = NULL; }
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
	lngval	{ $$ = $1;
		  if ($$ < 0) {
			$$ = -1;
			yyerror(m, "Positive value expected");
			YYABORT;
		  }
		}
	;

posint:
	intval	{ $$ = $1;
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
 |  varchar		{ sql_find_subtype(&$$, "varchar", 0, 0); }
 |  clob		{ sql_find_subtype(&$$, "varchar", 0, 0); }
 |  CHARACTER '(' nonzero ')'
			{ sql_find_subtype(&$$, "char", $3, 0); }
 |  varchar '(' nonzero ')'
			{ sql_find_subtype(&$$, "varchar", $3, 0); }
 |  clob '(' nonzero ')'
			{ sql_find_subtype(&$$, "varchar", $3, 0);
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

 |  sqlDECIMAL		{ sql_find_subtype(&$$, "decimal", 0, 0); }
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
 | sqlDOUBLE		{ sql_find_subtype(&$$, "double", 0, 0); }
 | sqlDOUBLE PRECISION	{ sql_find_subtype(&$$, "double", 0, 0); }
 | sqlREAL		{ sql_find_subtype(&$$, "real", 0, 0); }
 | datetime_type
 | interval_type
 | aTYPE		{ sql_find_subtype(&$$, $1, 0, 0); }
 | aTYPE '(' nonzero ')'
			{ sql_find_subtype(&$$, $1, $3, 0); }
 | sqlBOOL		{ sql_find_subtype(&$$, "boolean", 0, 0); }
 | ident	{
			  sql_type *t = mvc_bind_type(m, $1);
			  if (!t) {
				sqlformaterror(m, SQLSTATE(22000) "Type (%s) unknown", $1);
				$$.type = NULL;
				YYABORT;
			  } else {
				sql_init_subtype(&$$, t, 0, 0);
			  }
			}

 | ident '(' nonzero ')'
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
			sqlformaterror(m, "%s", SQLSTATE(22000) "Type (geometry) unknown");
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

variable:		column_id;

authid:		restricted_ident ;

restricted_ident:
   ident
 | non_reserved_keyword
 ;

bare_column_label:
    ident { $$ = $1; }
 |  reduced_keywords
 ;

type_function_name:
   ident
 | non_reserved_keyword
 | function_name_keyword
 ;

column_label:
   ident
 | non_reserved_keyword
 | column_name_keyword
 | function_name_keyword
 | reserved_keyword
 ;

column_id:
   ident
 | non_reserved_keyword
 | column_name_keyword
 ;

ident:
   IDENT
	{
		$$ = $1;
		if (!$1 || _strlen($1) == 0) {
			sqlformaterror(m, SQLSTATE(42000) "An identifier cannot be empty");
			YYABORT;
		}
	}
 ;

non_reserved_keyword:
  AUTHORIZATION	{ $$ = sa_strdup(SA, "authorization"); }/* sloppy: officially reserved */
| ANALYZE	{ $$ = sa_strdup(SA, "analyze"); }	/* sloppy: officially reserve */
| CACHE		{ $$ = sa_strdup(SA, "cache"); }
| CYCLE		{ $$ = sa_strdup(SA, "cycle"); }	/* sloppy: officially reserved */
| sqlDATE	{ $$ = sa_strdup(SA, "date"); }		/* sloppy: officially reserved */
| DEALLOCATE    { $$ = sa_strdup(SA, "deallocate"); }	/* sloppy: officially reserved */
| FILTER	{ $$ = sa_strdup(SA, "filter"); }	/* sloppy: officially reserved */
| LANGUAGE	{ $$ = sa_strdup(SA, "language"); }	/* sloppy: officially reserved */
| LARGE		{ $$ = sa_strdup(SA, "large"); }	/* sloppy: officially reserved */
| MATCH		{ $$ = sa_strdup(SA, "match"); }	/* sloppy: officially reserved */
| NO		{ $$ = sa_strdup(SA, "no"); }		/* sloppy: officially reserved */
| PREPARE	{ $$ = sa_strdup(SA, "prepare"); }	/* sloppy: officially reserved */
| RELEASE	{ $$ = sa_strdup(SA, "release"); }	/* sloppy: officially reserved */
| START		{ $$ = sa_strdup(SA, "start"); }	/* sloppy: officially reserved */
| UESCAPE	{ $$ = sa_strdup(SA, "uescape"); }	/* sloppy: officially reserved */
| VALUE		{ $$ = sa_strdup(SA, "value"); }	/* sloppy: officially reserved */
| WITHOUT	{ $$ = sa_strdup(SA, "without"); }	/* sloppy: officially reserved */

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
| STRIP		{ $$ = sa_strdup(SA, "strip"); }
| URI		{ $$ = sa_strdup(SA, "uri"); }
| WHITESPACE	{ $$ = sa_strdup(SA, "whitespace"); }

| ACTION	{ $$ = sa_strdup(SA, "action"); }
| AUTO_COMMIT	{ $$ = sa_strdup(SA, "auto_commit"); }
| BIG		{ $$ = sa_strdup(SA, "big"); }
| sqlBOOL	{ $$ = sa_strdup(SA, "bool"); }
| CENTURY	{ $$ = sa_strdup(SA, "century"); }
| CLIENT	{ $$ = sa_strdup(SA, "client"); }
| COMMENT	{ $$ = sa_strdup(SA, "comment"); }
| DATA		{ $$ = sa_strdup(SA, "data"); }
| DECADE	{ $$ = sa_strdup(SA, "decade"); }
| DESC		{ $$ = sa_strdup(SA, "desc"); }
| DIAGNOSTICS	{ $$ = sa_strdup(SA, "diagnostics"); }
| DOW		{ $$ = sa_strdup(SA, "dow"); }
| DOY		{ $$ = sa_strdup(SA, "doy"); }
| ENDIAN	{ $$ = sa_strdup(SA, "endian"); }
| EPOCH		{ $$ = sa_strdup(SA, "epoch"); }
| SQL_EXPLAIN	{ $$ = sa_strdup(SA, "explain"); }
| FIRST		{ $$ = sa_strdup(SA, "first"); }
| GEOMETRY	{ $$ = sa_strdup(SA, "geometry"); }
| IMPRINTS	{ $$ = sa_strdup(SA, "imprints"); }
| INCREMENT	{ $$ = sa_strdup(SA, "increment"); }
| KEY		{ $$ = sa_strdup(SA, "key"); }
| LAST		{ $$ = sa_strdup(SA, "last"); }
| LEVEL		{ $$ = sa_strdup(SA, "level"); }
| LITTLE	{ $$ = sa_strdup(SA, "little"); }
| LOGIN		{ $$ = sa_strdup(SA, "login"); }
| MAX_MEMORY	{ $$ = sa_strdup(SA, "max_memory"); }
| MAXVALUE	{ $$ = sa_strdup(SA, "maxvalue"); }
| MAX_WORKERS	{ $$ = sa_strdup(SA, "max_workers"); }
| MINVALUE	{ $$ = sa_strdup(SA, "minvalue"); }
| sqlNAME	{ $$ = sa_strdup(SA, "name"); }
| NATIVE	{ $$ = sa_strdup(SA, "native"); }
| NULLS		{ $$ = sa_strdup(SA, "nulls"); }
| OBJECT	{ $$ = sa_strdup(SA, "object"); }
| OPTIMIZER	{ $$ = sa_strdup(SA, "optimizer"); }
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
| UNLOGGED	{ $$ = sa_strdup(SA, "unlogged"); }
| WEEK		{ $$ = sa_strdup(SA, "week"); }
| ZONE		{ $$ = sa_strdup(SA, "zone"); }

/* odbc escape sequence non reserved words */
| ODBC_DATE_ESCAPE_PREFIX { $$ = sa_strdup(SA, "d"); }
| ODBC_TIME_ESCAPE_PREFIX { $$ = sa_strdup(SA, "t"); }
| ODBC_TIMESTAMP_ESCAPE_PREFIX { $$ = sa_strdup(SA, "ts"); }
| ODBC_GUID_ESCAPE_PREFIX { $$ = sa_strdup(SA, "guid"); }
| ODBC_FUNC_ESCAPE_PREFIX { $$ = sa_strdup(SA, "fn"); }
| ODBC_OJ_ESCAPE_PREFIX { $$ = sa_strdup(SA, "oj"); }
| DAYNAME { $$ = sa_strdup(SA, "dayname"); }
| MONTHNAME { $$ = sa_strdup(SA, "monthname"); }
| TIMESTAMPADD { $$ = sa_strdup(SA, "timestampadd"); }
| TIMESTAMPDIFF { $$ = sa_strdup(SA, "timestampdiff"); }

| aTYPE	{ $$ = $1; }	/* non reserved */
| RANK	{ $$ = $1; }	/* without '(' */
| MARGFUNC	{ $$ = $1; }	/* without '(' */
;

/* column identifiers */
column_name_keyword:
  BETWEEN 	{ $$ = sa_strdup(SA, "between"); }
| CHARACTER 	{ $$ = sa_strdup(SA, "character"); }
| COALESCE 	{ $$ = sa_strdup(SA, "coalesce"); }
| EXISTS 	{ $$ = sa_strdup(SA, "exists"); }
| EXTRACT 	{ $$ = sa_strdup(SA, "extract"); }
| GROUPING 	{ $$ = sa_strdup(SA, "grouping"); }
| INTERVAL 	{ $$ = sa_strdup(SA, "interval"); }
| NULLIF 	{ $$ = sa_strdup(SA, "nullif"); }
| POSITION 	{ $$ = sa_strdup(SA, "position"); }
| PRECISION	{ $$ = sa_strdup(SA, "precision"); }	/* sloppy: officially reserved */
| sqlREAL
| ROW		{ $$ = sa_strdup(SA, "row"); }		/* sloppy: officially reserved */
| SUBSTRING	{ $$ = sa_strdup(SA, "substring"); }
| TIME		{ $$ = sa_strdup(SA, "time"); }	
| TIMESTAMP	{ $$ = sa_strdup(SA, "timestamp"); }
| TRIM		{ $$ = sa_strdup(SA, "trim"); }
| VALUES	{ $$ = sa_strdup(SA, "values"); }
| XMLATTRIBUTES	{ $$ = sa_strdup(SA, "xmlatributes"); }
| XMLCONCAT	{ $$ = sa_strdup(SA, "xmlconcat"); }
| XMLELEMENT	{ $$ = sa_strdup(SA, "xmlelement"); }
| XMLFOREST	{ $$ = sa_strdup(SA, "xmlforest"); }
| XMLPARSE	{ $$ = sa_strdup(SA, "xmlparse"); }
| XMLPI		{ $$ = sa_strdup(SA, "xmlpi"); }
/* odbc */
| IFNULL { $$ = sa_strdup(SA, "ifnull"); }
;

function_name_keyword:
  LEFT	{ $$ = sa_strdup(SA, "left"); }
| RIGHT	{ $$ = sa_strdup(SA, "right"); }
| INSERT	{ $$ = sa_strdup(SA, "insert"); }
;

reserved_keyword:
  ALL		{ $$ = sa_strdup(SA, "all"); }
| AND		{ $$ = sa_strdup(SA, "and"); }
| ANY		{ $$ = sa_strdup(SA, "ANY"); }
| AS		{ $$ = sa_strdup(SA, "as"); }
| ASC		{ $$ = sa_strdup(SA, "asc"); }
| ASYMMETRIC	{ $$ = sa_strdup(SA, "asymmetric"); }
| BOTH		{ $$ = sa_strdup(SA, "both"); }
| CASE		{ $$ = sa_strdup(SA, "case"); }
| CAST		{ $$ = sa_strdup(SA, "cast"); }
| CURRENT_ROLE	{ $$ = sa_strdup(SA, "current_role"); }
| COLUMN	{ $$ = sa_strdup(SA, "column"); }
| DISTINCT	{ $$ = sa_strdup(SA, "distinct"); }
| EXEC		{ $$ = sa_strdup(SA, "exec"); }
| EXECUTE	{ $$ = sa_strdup(SA, "execute"); }
| TABLE		{ $$ = sa_strdup(SA, "table"); }
| FROM		{ $$ = sa_strdup(SA, "from"); }
;

reduced_keywords:
  AUTHORIZATION	{ $$ = sa_strdup(SA, "authorization"); }
| COLUMN	{ $$ = sa_strdup(SA, "column"); }
| CYCLE		{ $$ = sa_strdup(SA, "cycle"); }
| DISTINCT	{ $$ = sa_strdup(SA, "distinct"); }
| INTERVAL	{ $$ = sa_strdup(SA, "interval"); }
| PRECISION	{ $$ = sa_strdup(SA, "precision"); }
| ROW		{ $$ = sa_strdup(SA, "row"); }
| CACHE		{ $$ = sa_strdup(SA, "cache"); }
/* SQL/XML non reserved words */
| ABSENT	{ $$ = sa_strdup(SA, "absent"); }
| ACCORDING	{ $$ = sa_strdup(SA, "according"); }
| CONTENT	{ $$ = sa_strdup(SA, "content"); }
| DOCUMENT	{ $$ = sa_strdup(SA, "document"); }
| ELEMENT	{ $$ = sa_strdup(SA, "element"); }
| EMPTY		{ $$ = sa_strdup(SA, "empty"); }
| EXEC		{ $$ = sa_strdup(SA, "exec"); }
| EXECUTE	{ $$ = sa_strdup(SA, "execute"); }
| ID		{ $$ = sa_strdup(SA, "id"); }
| LOCATION	{ $$ = sa_strdup(SA, "location"); }
| NAMESPACE	{ $$ = sa_strdup(SA, "namespace"); }
| NIL		{ $$ = sa_strdup(SA, "nil"); }
| PASSING	{ $$ = sa_strdup(SA, "passing"); }
| REF		{ $$ = sa_strdup(SA, "ref"); }
| STRIP		{ $$ = sa_strdup(SA, "strip"); }
| TABLE		{ $$ = sa_strdup(SA, "table"); }
| TIME		{ $$ = sa_strdup(SA, "time"); }	
| TIMESTAMP	{ $$ = sa_strdup(SA, "timestamp"); }
| UESCAPE	{ $$ = sa_strdup(SA, "uescape"); }
| URI		{ $$ = sa_strdup(SA, "uri"); }
| WHITESPACE	{ $$ = sa_strdup(SA, "whitespace"); }
/* odbc */
| IFNULL { $$ = sa_strdup(SA, "ifnull"); }

| ODBC_DATE_ESCAPE_PREFIX { $$ = sa_strdup(SA, "d"); }
| ODBC_TIME_ESCAPE_PREFIX { $$ = sa_strdup(SA, "t"); }
| ODBC_TIMESTAMP_ESCAPE_PREFIX { $$ = sa_strdup(SA, "ts"); }
| ODBC_GUID_ESCAPE_PREFIX { $$ = sa_strdup(SA, "guid"); }
| ODBC_FUNC_ESCAPE_PREFIX { $$ = sa_strdup(SA, "fn"); }
| ODBC_OJ_ESCAPE_PREFIX { $$ = sa_strdup(SA, "oj"); }
| DAYNAME { $$ = sa_strdup(SA, "dayname"); }
| MONTHNAME { $$ = sa_strdup(SA, "monthname"); }
| TIMESTAMPADD { $$ = sa_strdup(SA, "timestampadd"); }
| TIMESTAMPDIFF { $$ = sa_strdup(SA, "timestampdiff"); }
;


lngval:
	sqlINT
		{
		  char *end = NULL, *s = $1;
		  int l = _strlen(s);
		  /* errno might be non-zero due to other people's code */
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
   ;

intval:
	sqlINT
		{
		  char *end = NULL, *s = $1;
		  int l = _strlen(s);
		  /* errno might be non-zero due to other people's code */
		  errno = 0;
		  if (l <= 10) {
			long v = strtol(s,&end,10);
#if SIZEOF_LONG > SIZEOF_INT
			if (v > INT_MAX)
				errno = ERANGE;
#endif
			$$ = (int) v;
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
 ;


exec:
     execute exec_ref
		{
		  m->emod |= mod_exec;
		  $$ = _symbol_create_symbol(SQL_CALL, $2); }
 ;

dealloc_ref:
   posint { $$ = $1; }
 | ALL    { $$ = -1; } /* prepared statements numbers cannot be negative, so set -1 to deallocate all */
 ;

dealloc:
     deallocate opt_prepare dealloc_ref
		{
		  m->emode = m_deallocate;
		  $$ = _newAtomNode(atom_int(SA, sql_fetch_localtype(TYPE_int), $3)); }
 ;

exec_ref:
    posint arg_list_ref { $$ = $2; $$->data.lval->h->data.i_val = $1; }
 ;

arg_list_ref:
    '(' ')'
	{ dlist *l = L();
	  append_int(l, -1);
	  append_int(l, FALSE); /* ignore distinct */
	  append_list(l, NULL);
	  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  '(' expr_list ')'
	{ dlist *l = L();
	  append_int(l, -1);
	  append_int(l, FALSE); /* ignore distinct */
	  append_list(l, $2);
	  $$ = _symbol_create_list( SQL_NOP, l ); }
 ;

named_arg_list_ref:
    '(' ')'
	{ dlist *l = L();
	  append_int(l, -1);
	  append_int(l, FALSE); /* ignore distinct */
	  append_list(l, NULL);
	  $$ = _symbol_create_list( SQL_NOP, l ); }
 |  '(' named_value_commalist ')'
	{ dlist *l = L();
	  append_int(l, -1);
	  append_int(l, FALSE); /* ignore distinct */
	  append_list(l, $2);
	  $$ = _symbol_create_list( SQL_NOP, l ); }
 ;

/* path specification>

Specify an order for searching for an SQL-invoked routine.

CURRENTLY only parsed
*/

opt_path_specification:
	/* empty */	{ $$ = NULL; }
   |	path_specification
   ;

path_specification:
        PATH schema_name_list	{ $$ = _symbol_create_list( SQL_PATH, $2); }
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
	| COLUMN column_id '.' ident
	{ dlist *l = L();
	  append_string(l, $2);
	  append_string(l, $4);
	  $$ = _symbol_create_list( SQL_COLUMN, l );
	}
	| COLUMN column_id '.' ident '.' ident
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
    column_id
  ;

XML_attributes:
  XMLATTRIBUTES '(' XML_attribute_list ')'	{ $$ = $3; }
  ;

XML_attribute_list:
    XML_attribute				{ $$ = $1; }
  | XML_attribute_list ',' XML_attribute
		{ dlist *l = L();
		  append_list(l, append_string(L(), sa_strdup(SA, "concat")));
		  append_int(l, FALSE); /* ignore distinct */
		  dlist *args = append_symbol(L(), $1);
		  append_symbol(args, $3);
		  append_list(l, args);
		  $$ = _symbol_create_list( SQL_NOP, l ); }
  ;

XML_attribute:
  XML_attribute_value opt_XML_attribute_name
	{ dlist *l = L();
	  append_string(l, $2);
	  append_symbol(l, $1);
	  $$ = _symbol_create_list( SQL_XMLATTRIBUTE, l ); }
  ;

opt_XML_attribute_name:
     /* empty */				{ $$ = NULL; }
  | AS XML_attribute_name			{ $$ = $2; }
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
		  $$ = append_int($$, $2);	}
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
    column_label		{ $$ = $1; }
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
	/* empty */	{ $$ = NULL; }
  | ',' value_exp /* should be a string */
			{ $$ = $2; }
  ;

XML_query:
  XMLQUERY '('
      XQuery_expression
      opt_XML_query_argument_list
      opt_XML_returning_clause_query_returning_mechanism
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
     value_exp
  |  value_exp XML_passing_mechanism
  |  value_exp AS ident
  |  value_exp AS ident XML_passing_mechanism
  ;

opt_XML_query_returning_mechanism:
   /* empty */
 | XML_passing_mechanism
 ;

opt_XML_returning_clause_query_returning_mechanism:
   /* empty */
 | XML_returning_clause opt_XML_query_returning_mechanism
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
 | XML_returning_clause
 ;

XML_returning_clause:
   RETURNING CONTENT		{ $$ = 0; }
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
  XMLNAMESPACES '(' XML_namespace_declaration_item_list ')'	{ $$ = $3; }
  ;

XML_namespace_declaration_item_list:
	XML_namespace_declaration_item	{ $$ = $1; }
  |     XML_namespace_declaration_item_list ',' XML_namespace_declaration_item
		{ dlist *l = L();
		  append_list(l, append_string(L(), sa_strdup(SA, "concat")));
		  append_int(l, FALSE); /* ignore distinct */
		  dlist *args = append_symbol(L(), $1);
		  append_symbol(args, $3);
		  append_list(l, args);
		  $$ = _symbol_create_list( SQL_NOP, l ); }
  ;

XML_namespace_declaration_item:
    XML_regular_namespace_declaration_item
  | XML_default_namespace_declaration_item
  ;

XML_namespace_prefix:
    ident
  ;

XML_namespace_URI:
    scalar_exp_no_and
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

	  append_list(aggr, append_string(append_string(L(), "sys"), "xmlagg"));
	  append_int(aggr, FALSE); /* ignore distinct */
	  append_list(aggr, append_symbol(L(), $3));
	  append_symbol(aggr, $4);
	  /* int returning not used */
	  $$ = _symbol_create_list( SQL_NOP, aggr);
	}
 ;

odbc_date_escape:
    '{' ODBC_DATE_ESCAPE_PREFIX string '}'
	{
	    symbol* node = makeAtomNode(m, "date", $3, 0, 0, false);
	    if (node == NULL)
	        YYABORT;
	    $$ = node;
	}
    ;

odbc_time_escape:
    '{' ODBC_TIME_ESCAPE_PREFIX string '}'
	{
	    unsigned int pr = get_time_precision($3) + 1;
	    symbol* node = makeAtomNode(m, "time", $3, pr, 0, false);
	    if (node == NULL)
	        YYABORT;
	    $$ = node;
	}
    ;

odbc_timestamp_escape:
    '{' ODBC_TIMESTAMP_ESCAPE_PREFIX string '}'
	{
	    unsigned int pr = get_timestamp_precision($3);
	    pr = pr ? (pr + 1) : (pr + 6);
	    symbol* node = makeAtomNode(m, "timestamp", $3, pr, 0, false);
	    if (node == NULL)
	        YYABORT;
	    $$ = node;
	}
    ;

odbc_guid_escape:
    '{' ODBC_GUID_ESCAPE_PREFIX string '}'
	{
	    symbol* node = makeAtomNode(m, "uuid", $3, 0, 0, true);
	    if (node == NULL)
	        YYABORT;
	    $$ = node;
	}
    ;

odbc_interval_escape:
    '{' interval_expression '}' {$$ = $2;}
    ;


odbc_scalar_func_escape:
    '{' ODBC_FUNC_ESCAPE_PREFIX odbc_scalar_func '}' {$$ = $3;}
    ;

odbc_datetime_func:
    HOUR '(' scalar_exp ')'
	{ dlist *l = L();
	  append_list( l, append_string(L(), sa_strdup(SA, "hour")));
	  append_int(l, FALSE); /* ignore distinct */
	  append_list(l, append_symbol(L(), $3));
	  $$ = _symbol_create_list( SQL_NOP, l );
	}
    | MINUTE '(' scalar_exp ')'
	{ dlist *l = L();
	  append_list( l, append_string(L(), sa_strdup(SA, "minute")));
	  append_int(l, FALSE); /* ignore distinct */
	  append_list(l, append_symbol(L(), $3));
	  $$ = _symbol_create_list( SQL_NOP, l );
	}
    | SECOND '(' scalar_exp ')'
	{ dlist *l = L();
	  append_list( l, append_string(L(), sa_strdup(SA, "second")));
	  append_int(l, FALSE); /* ignore distinct */
	  append_list(l, append_symbol(L(), $3));
	  $$ = _symbol_create_list( SQL_NOP, l );
	}
    | MONTH '(' scalar_exp ')'
	{ dlist *l = L();
	  append_list( l, append_string(L(), sa_strdup(SA, "month")));
	  append_int(l, FALSE); /* ignore distinct */
	  append_list(l, append_symbol(L(), $3));
	  $$ = _symbol_create_list( SQL_NOP, l );
	}
    | YEAR '(' scalar_exp ')'
	{ dlist *l = L();
	  append_list( l, append_string(L(), sa_strdup(SA, "year")));
	  append_int(l, FALSE); /* ignore distinct */
	  append_list(l, append_symbol(L(), $3));
	  $$ = _symbol_create_list( SQL_NOP, l );
	}
    | ODBC_TIMESTAMPADD '(' odbc_tsi_qualifier ',' scalar_exp ',' scalar_exp ')'
	{ dlist *l = L();
	  append_list( l, append_string(L(), sa_strdup(SA, "timestampadd")));
	  append_int(l, FALSE); /* ignore distinct */
	  sql_subtype t;
	  lng i = 0;
	  if (process_odbc_interval(m, $3, 1, &t, &i) < 0) {
		yyerror(m, "incorrect interval");
		$$ = NULL;
		YYABORT;
	  }

	  dlist *args = append_symbol(L(), _newAtomNode(atom_int(SA, &t, i)));
	  append_symbol(args, $5);

	  dlist *nargs = append_symbol(L(), $7);
	  nargs = append_symbol(nargs, _symbol_create_list( SQL_NOP,
		    append_list(
		      append_int(
			append_list(L(), append_string(L(), sa_strdup(SA, "sql_mul"))),
			   FALSE), /* ignore distinct */
			    args)));
	  append_list(l, nargs);
	  $$ = _symbol_create_list( SQL_NOP, l );
	}
    | ODBC_TIMESTAMPDIFF '(' odbc_tsi_qualifier ',' scalar_exp ',' scalar_exp ')'
	{ dlist *l = L();
	  switch($3) {
	    case iyear:
		append_list( l, append_string(L(), sa_strdup(SA, "timestampdiff_year")));
		break;
	    case iquarter:
		append_list( l, append_string(L(), sa_strdup(SA, "timestampdiff_quarter")));
		break;
	    case imonth:
		append_list( l, append_string(L(), sa_strdup(SA, "timestampdiff_month")));
		break;
	    case iweek:
		append_list( l, append_string(L(), sa_strdup(SA, "timestampdiff_week")));
		break;
	    case iday:
		append_list( l, append_string(L(), sa_strdup(SA, "timestampdiff_day")));
		break;
	    case ihour:
		append_list( l, append_string(L(), sa_strdup(SA, "timestampdiff_hour")));
		break;
	    case imin:
		append_list( l, append_string(L(), sa_strdup(SA, "timestampdiff_min")));
		break;
	    case isec:
		append_list( l, append_string(L(), sa_strdup(SA, "timestampdiff_sec")));
		break;
	    default:
		/* diff in ms */
		append_list( l, append_string(L(), sa_strdup(SA, "timestampdiff")));
	  }
	  append_int(l, FALSE); /* ignore distinct */
	  dlist *args = append_symbol(L(), $7);
	  append_symbol(args, $5);
	  append_list(l, args);
	  $$ = _symbol_create_list( SQL_NOP, l );
	}
;

odbc_scalar_func:
      func_ref { $$ = $1;}
    | string_funcs { $$ = $1;}
    | datetime_funcs { $$ = $1;}
    | odbc_datetime_func { $$ = $1;}
    | CONVERT '(' scalar_exp ',' odbc_data_type ')'
	{ dlist *l = L();
	  append_symbol(l, $3);
	  append_type(l, &$5);
	  $$ = _symbol_create_list( SQL_CAST, l );
	}
    | USER '(' ')'
	{ $$ = _symbol_create_list(SQL_NAME, append_string(append_string(L(), sa_strdup(SA, "sys")), sa_strdup(SA, "current_user"))); }
    | CHARACTER '(' scalar_exp ')'
	{ dlist *l = L();
	  append_list( l, append_string(L(), sa_strdup(SA, "code")));
	  append_int(l, FALSE); /* ignore distinct */
	  append_list(l, append_symbol(L(), $3));
	  $$ = _symbol_create_list( SQL_NOP, l );
	}
    | TRUNCATE '(' scalar_exp ',' scalar_exp ')'
	{ dlist *l = L();
	  append_list( l, append_string(L(), sa_strdup(SA, "ms_trunc")));
	  append_int(l, FALSE); /* ignore distinct */
	  dlist *args = append_symbol(L(), $3);
	  append_symbol(args, $5);
	  append_list(l, args);
	  $$ = _symbol_create_list( SQL_NOP, l );
	}
    | IFNULL '(' scalar_exp ',' scalar_exp ')'
	{ dlist *l = L();
	  append_symbol( l, $3);
	  append_symbol( l, $5);
	  $$ = _symbol_create_list(SQL_COALESCE, l);
	}
;

odbc_data_type:
      SQL_BIGINT
	{ sql_find_subtype(&$$, "bigint", 0, 0); }
    | SQL_BINARY
	{ sql_find_subtype(&$$, "blob", 0, 0); }
    | SQL_BIT
	{ sql_find_subtype(&$$, "boolean", 0, 0); }
    | SQL_CHAR
	{ sql_find_subtype(&$$, "char", 0, 0); }
    | SQL_DATE
	{ sql_find_subtype(&$$, "date", 0, 0); }
    | SQL_DECIMAL
	{ sql_find_subtype(&$$, "decimal", 0, 0); }
    | SQL_DOUBLE
	{ sql_find_subtype(&$$, "double", 0, 0); }
    | SQL_FLOAT
	{ sql_find_subtype(&$$, "double", 0, 0); }
    | SQL_GUID
	{
	    sql_type* t = NULL;
	    if (!(t = mvc_bind_type(m, "uuid"))) {
	        sqlformaterror(m, SQLSTATE(22000) "Type uuid unknown");
	        YYABORT;
	    }
	    sql_init_subtype(&$$, t, 0, 0);
	}
    | SQL_HUGEINT  /* Note: SQL_HUGEINT is not part of or defined in ODBC. This is a MonetDB extension. */
	{ sql_find_subtype(&$$, "hugeint", 0, 0); }
    | SQL_INTEGER
	{ sql_find_subtype(&$$, "int", 0, 0); }
    | SQL_INTERVAL_YEAR
	{ sql_find_subtype(&$$, "month_interval", 1, 0); }
    | SQL_INTERVAL_YEAR_TO_MONTH
	{ sql_find_subtype(&$$, "month_interval", 2, 0); }
    | SQL_INTERVAL_MONTH
	{ sql_find_subtype(&$$, "month_interval", 3, 0); }
    | SQL_INTERVAL_DAY
	{ sql_find_subtype(&$$, "day_interval", 4, 0); }
    | SQL_INTERVAL_DAY_TO_HOUR
	{ sql_find_subtype(&$$, "sec_interval", 5, 0); }
    | SQL_INTERVAL_DAY_TO_MINUTE
	{ sql_find_subtype(&$$, "sec_interval", 6, 0); }
    | SQL_INTERVAL_DAY_TO_SECOND
	{ sql_find_subtype(&$$, "sec_interval", 7, 0); }
    | SQL_INTERVAL_HOUR
	{ sql_find_subtype(&$$, "sec_interval", 8, 0); }
    | SQL_INTERVAL_HOUR_TO_MINUTE
	{ sql_find_subtype(&$$, "sec_interval", 9, 0); }
    | SQL_INTERVAL_HOUR_TO_SECOND
	{ sql_find_subtype(&$$, "sec_interval", 10, 0); }
    | SQL_INTERVAL_MINUTE
	{ sql_find_subtype(&$$, "sec_interval", 11, 0); }
    | SQL_INTERVAL_MINUTE_TO_SECOND
	{ sql_find_subtype(&$$, "sec_interval", 12, 0); }
    | SQL_INTERVAL_SECOND
	{ sql_find_subtype(&$$, "sec_interval", 13, 0); }
    | SQL_LONGVARBINARY
	{ sql_find_subtype(&$$, "blob", 0, 0); }
    | SQL_LONGVARCHAR
	{ sql_find_subtype(&$$, "varchar", 0, 0); }
    | SQL_NUMERIC
	{ sql_find_subtype(&$$, "decimal", 0, 0); }
    | SQL_REAL
	{ sql_find_subtype(&$$, "real", 0, 0); }
    | SQL_SMALLINT
	{ sql_find_subtype(&$$, "smallint", 0, 0); }
    | SQL_TIME
	{ sql_find_subtype(&$$, "time", 0, 0); }
    | SQL_TIMESTAMP
	{ sql_find_subtype(&$$, "timestamp", 6, 0); }
    | SQL_TINYINT
	{ sql_find_subtype(&$$, "tinyint", 0, 0); }
    | SQL_VARBINARY
	{ sql_find_subtype(&$$, "blob", 0, 0); }
    | SQL_VARCHAR
	{ sql_find_subtype(&$$, "varchar", 0, 0); }
    | SQL_WCHAR
	{ sql_find_subtype(&$$, "char", 0, 0); }
    | SQL_WLONGVARCHAR
	{ sql_find_subtype(&$$, "varchar", 0, 0); }
    | SQL_WVARCHAR
	{ sql_find_subtype(&$$, "varchar", 0, 0); }
;

odbc_tsi_qualifier:
      SQL_TSI_FRAC_SECOND
	{ $$ = insec; }
    | SQL_TSI_SECOND
	{ $$ = isec; }
    | SQL_TSI_MINUTE
	{ $$ = imin; }
    | SQL_TSI_HOUR
	{ $$ = ihour; }
    | SQL_TSI_DAY
	{ $$ = iday; }
    | SQL_TSI_WEEK
	{ $$ = iweek; }
    | SQL_TSI_MONTH
	{ $$ = imonth; }
    | SQL_TSI_QUARTER
	{ $$ = iquarter; }
    | SQL_TSI_YEAR
	{ $$ = iyear; }
;


%%


static inline symbol*
makeAtomNode(mvc *m, const char* typename, const char* val, unsigned int digits, unsigned int scale, bool bind) {
    sql_subtype sub_t;
    atom *a;
    int sub_t_found = 0;
    if (bind) {
        sql_type* t = NULL;
        if (!(t = mvc_bind_type(m, typename))) {
            sqlformaterror(m, SQLSTATE(22000) "Type (%s) unknown", typename);
            return NULL;
        }
        sql_init_subtype(&sub_t, t, 0, 0);
    } else {
        sub_t_found = sql_find_subtype(&sub_t, typename, digits, scale);
    }
    if ((!bind && !sub_t_found) || (a = atom_general(m->sa, &sub_t, val, m->timezone)) == NULL) {
        sqlformaterror(m, SQLSTATE(22007) "Incorrect %s value (%s)", typename, val);
        return NULL;
    }
    return _newAtomNode(a);
}

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
	/* Please keep this list sorted for easy of maintenance */
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
	SQL(BINCOPYINTO);
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
	SQL(COPYINTO);
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
	SQL(LOGIN);
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
	SQL(RECURSIVE);
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
	SQL(UNIQUE_NULLS_NOT_DISTINCT);
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
	/* Please keep this list sorted for easy of maintenance */
	}
	return "unknown";	/* just needed for broken compilers ! */
}

void *
sql_error( mvc * sql, int error_code, const char *format, ... )
{
	va_list	ap;

	va_start (ap,format);
	if (sql->errstr[0] == '\0' || error_code == 5 || error_code == ERR_NOTFOUND)
		vsnprintf(sql->errstr, ERRSIZE-1, _(format), ap);
	if (!sql->session->status || error_code == 5 || error_code == ERR_NOTFOUND) {
		if (error_code < 0)
			error_code = -error_code;
		sql->session->status = -error_code;
	}
	va_end (ap);
	return NULL;
}

int
sqlformaterror(mvc * sql, const char *format, ...)
{
	va_list	ap;
	const char *sqlstate = NULL;
	size_t len = 0;

	if (sql->scanner.aborted) {
		snprintf(sql->errstr, ERRSIZE, "Query aborted\n");
		return 1;
	}
	va_start (ap,format);
	if (format && strlen(format) > 6 && format[5] == '!') {
		/* sql state provided */
		sqlstate = NULL;
	} else {
		/* default: Syntax error or access rule violation */
		sqlstate = SQLSTATE(42000);
	}
	if (sql->errstr[0] == '\0') {
		if (sqlstate)
			len += snprintf(sql->errstr+len, ERRSIZE-1-len, "%s", sqlstate);
		len += vsnprintf(sql->errstr+len, ERRSIZE-1-len, _(format), ap);
		snprintf(sql->errstr+len, ERRSIZE-1-len, " in: \"%.800s\"\n", QUERY(sql->scanner));
	}
	if (!sql->session->status)
		sql->session->status = -4;
	va_end (ap);
	return 1;
}

static int
sqlerror(mvc *sql, const char *err)
{
	return sqlformaterror(sql, "%s", sql->scanner.errstr ? sql->scanner.errstr : err);
}

static void *sql_alloc(allocator *sa, size_t sz)
{
	return sa_alloc(sa, sz);
}

static void sql_free(void *p)
{
	(void)p;
}
