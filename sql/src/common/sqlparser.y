%{
/* todo 
 * use table/column constrains (such as not null)
 * use primary/foreign keys
 */
#include <unistd.h>
#include "sql.h"
#include "sqlexecute.h"
#include "symbol.h"
#include <mem.h>
#include <stdlib.h>
#include <datetime.h>

#define _symbol_create(t,d)         symbol_create( (context*)parm, t, d)
#define _symbol_create_list(t,d)    symbol_create_list( (context*)parm, t, d)
#define _symbol_create_int(t,d)     symbol_create_int( (context*)parm, t, d)
#define _symbol_create_symbol(t,d)  symbol_create_symbol( (context*)parm, t, d)
#define _symbol_create_atom(t,d)    symbol_create_atom( (context*)parm, t, d)
#define _symbol_init(s,t)    	    symbol_init( s, (context*)parm, t)

extern int parse_error(void *lc, char *s);

#define YYPARSE_PARAM parm
#define YYLEX_PARAM parm
#ifdef yyerror
#undef yyerror
#endif
#define yyerror(s) parse_error(YYLEX_PARAM, s)

#define FALSE 0
#define TRUE 1

%}
/* KNOWN NOT DONE OF sql'99
 * 
 * TRIGGERS
 * COLLATION
 * TRANSLATION
 * REF/SCOPE
 * UDT
 */

/* reentrant parser */
%pure_parser
%union {
	int 		ival,operation,bval;
	double 		fval;
	char *		sval;
	symbol*		sym;
	dlist*		l;
}
%{
extern int sqllex( YYSTYPE *yylval, void *lc );
%}

	/* symbolic tokens */
%type <sym> 
	alter
	assignment
	create
	drop
	sql
	sqlstmt
	schema
	opt_schema_default_char_set
	opt_schema_path
	schema_element
	delete_stmt
	copyfrom_stmt
	table_def
	view_def
	all_or_any_predicate
	atom_exp
	between_predicate
	comparison_predicate
	opt_from_clause
	existence_test
	in_predicate
	insert_stmt
	transaction_stmt
	like_predicate
	opt_where_clause
	opt_having_clause
	opt_group_by_clause
	predicate
	joined_table
	join_spec
	search_condition
	update_stmt
	select_clause
	select_stmt
	select_with_parens
	select_no_parens
	simple_select
	subquery
	target
	test_for_null
	values_or_query_spec
	privilege_def
	operation
	table_element
	table_constraint
	table_constraint_type
	column_def
	column_options
	column_option
	column_constraint
	column_constraint_type 
	domain_constraint_type
	opt_order_by_clause
	default
	default_value
	function_ref
	datetime_funcs
	string_funcs
	scalar_exp
	value_exp
	column_exp
	atom
	insert_atom
	literal
	ordering_spec
	simple_table
	table_ref 
	opt_temp
	case_exp
	cast_exp
	when_value
	when_search
	opt_else
	table_name
	opt_ref_action
	opt_ref_delete
	opt_ref_update

%type <sval>
	any_all_some
	non_reserved_word
	ident
	column
	user
	data_type
	datetime_type
	interval_type
	grantee
	opt_column_name
	opt_to_savepoint
	opt_using

%type <l>
	schema_name
	assignment_commalist
	opt_column_commalist
	column_commalist_parens
	column_commalist
	column_ref_commalist
	name_commalist 
	column_ref
	atom_commalist
	opt_constraint_name 
	qname 
	ordering_spec_commalist
	opt_schema_element_list
	schema_element_list
	operation_commalist
	privileges
	target_commalist
	opt_into
	grantee_commalist
	column_def_opt_list
	opt_column_def_opt_list
	table_exp
	table_ref_commalist 
	table_element_list
	table_content_source
	column_exp_commalist
	column_option_list
	selection
	insert_atom_commalist
	start_field
	end_field
	single_datetime_field
	interval_qualifier
	scalar_exp_list
	when_value_list
	when_search_list
	opt_seps

%type <ival>
	drop_action 
	ref_action 
	join_type
	outer_join_type
	time_persision
	non_second_datetime_field
	datetime_field
	opt_bounds
	opt_sign
	opt_all
	intval
	opt_nr
	opt_match
	opt_match_type

%type <bval>
	opt_trans
	opt_chain
	opt_distinct
	opt_with_check_option
	opt_with_grant_option
	opt_asc_desc
	tz

%token <sval> 
	IDENT TYPE STRING AMMSC INT INTNUM APPROXNUM USER USING
	ALL DISTINCT ANY SOME CHECK GLOBAL LOCAL CAST
	CHARACTER NUMERIC DECIMAL INTEGER SMALLINT FLOAT REAL
	DOUBLE PRECISION VARCHAR PARTIAL SIMPLE ACTION CASCADE RESTRICT
	BOOL_FALSE BOOL_TRUE

/*
OPEN CLOSE FETCH 
*/
%token <sval> DELETE UPDATE SELECT INSERT
%token <sval> LEFT RIGHT FULL OUTER NATURAL CROSS JOIN INNER UNIONJOIN
%token <sval> COMMIT ROLLBACK SAVEPOINT RELEASE WORK CHAIN NO
	
%token <operation> '+' '-' '*' '/'
%token <sval> LIKE BETWEEN ASYMMETRIC SYMMETRIC ORDER BY
%token <operation> IN EXISTS ESCAPE HAVING GROUP NULLX 
%token <operation> FROM FOR MATCH

/* datetime operations */
%token <operation> EXTRACT
/* string operations */
%token <operation> SUBSTRING CONCATSTRING

	/* operators */

%left UNION
%left JOIN UNIONJOIN CROSS LEFT FULL RIGHT INNER NATURAL
%left <operation> OR
%left <operation> AND
%left <operation> NOT
%left <operation> '('
%left <sval> COMPARISON /* <> < > <= >= */
%left <operation> '='
%left <operation> '+' '-'
%left <operation> '*' '/'
%left <operation> SUBSTRING CONCATSTRING
%right UMINUS

	/* literal keyword tokens */

/*
CONTINUE CURRENT CURSOR DECLARE FOUND GOTO GO
LANGUAGE OF PROCEDURE
SQLCODE SQLERROR 
UNDER WHENEVER 
*/
%token TEMPORARY 
%token<sval> AMMSC AS ASC DESC AUTHORIZATION 
%token CHECK CONSTRAINT CREATE 
%token DEFAULT DISTINCT DROP
%token FOREIGN 
%token GRANT HAVING INTO
%token IS KEY ON OPTION OPTIONS
%token PATH PRIMARY PRIVILEGES 
%token<sval> PUBLIC REFERENCES SCHEMA SET
%token ALTER ADD TABLE TO UNION UNIQUE USER VALUES VIEW WHERE WITH 
%token<sval> DATE TIME TIMESTAMP INTERVAL
%token YEAR MONTH DAY HOUR MINUTE SECOND ZONE

%token CASE WHEN THEN ELSE END NULLIF COALESCE
%token COPY RECORDS DELIMITERS 

%%

/*
sql_list:
    sqlstmt 	{ context *lc = (context*)parm;
			  lc->l = $$ = dlist_append_symbol(dlist_create(), $1 );
			  sql_stmt_init(lc); }
 |  sql_list sqlstmt { context *lc = (context*)parm;
			  lc->l = $$ = dlist_append_symbol($1, $2); 
			  sql_stmt_init(lc); }
 ;
*/

sqlstmt:
   sql ';' 		{ context *lc = (context*)parm; 
			  lc->sym = $$ = $1; YYACCEPT; }
 | /*empty*/		{ context *lc = (context*)parm; 
			  lc->sym = $$ = NULL; YYACCEPT; }
 ;


	/* schema definition language */
sql: schema | privilege_def | create | drop ;
	
schema:
    CREATE SCHEMA schema_name opt_schema_default_char_set
	opt_schema_path	opt_schema_element_list	   

	{ dlist *l = dlist_create();
	  dlist_append_list(l, $3);
	  dlist_append_symbol(l, $4);
	  dlist_append_symbol(l, $5);
	  dlist_append_list(l, $6);
	  $$ = _symbol_create_list( SQL_CREATE_SCHEMA, l); }
 |  DROP SCHEMA qname drop_action 

	{ dlist *l = dlist_create();
	  dlist_append_list(l, $3);
	  dlist_append_int(l, $4);
	  $$ = _symbol_create_list( SQL_DROP_SCHEMA, l); }
 ;

schema_name:
    ident		    	
	{ $$ = dlist_create();
	  dlist_append_string($$, $1 );
	  dlist_append_string($$, NULL ); }
 |  AUTHORIZATION ident  	
	{ $$ = dlist_create();
	  dlist_append_string($$, NULL );
	  dlist_append_string($$, $2 ); }
 |  ident AUTHORIZATION ident 
	{ $$ = dlist_create();
	  dlist_append_string($$, $1 );
	  dlist_append_string($$, $3 ); }
 ;

opt_schema_default_char_set:
    /* empty */		   	{ $$ = NULL; }
 |  DEFAULT CHARACTER SET ident { $$ = _symbol_create( SQL_CHARSET, $4 ); }
 ;

opt_schema_path:
    /* empty */			{ $$ = NULL; }
 |  PATH name_commalist 	{ $$ = _symbol_create_list( SQL_PATH, $2 ); }
 ;

opt_schema_element_list:
    /* empty */			{ $$ = dlist_create(); }
 |  schema_element_list 	
 ;

schema_element_list:
    schema_element		{ $$ = dlist_append_symbol(dlist_create(), $1); }
 |  schema_element_list schema_element 	{ $$ = dlist_append_symbol( $1, $2 ); }
 ;

schema_element: privilege_def | create | drop | alter;
/* | add */

privilege_def:
    GRANT privileges ON qname TO grantee_commalist opt_with_grant_option 			
	{ dlist *l = dlist_create();
	  dlist_append_list(l, $2);
	  dlist_append_list(l, $4);
	  dlist_append_list(l, $6);
	  dlist_append_int(l, $7);
	$$ = _symbol_create_list( SQL_GRANT, l); }
 ;

opt_with_grant_option:
    /* empty */				{ $$ = FALSE; }
 |	WITH GRANT OPTION		{ $$ = TRUE; }
 ;

privileges:
    ALL PRIVILEGES			{ $$ = NULL; }
 |  ALL 				{ $$ = NULL; }
 |  operation_commalist			
 ;

operation_commalist:
    operation			{ $$ = dlist_append_symbol(dlist_create(), $1); }
 |  operation_commalist ',' operation   { $$ = dlist_append_symbol($1, $3); }
 ;

operation:
    SELECT 			    { $$ = _symbol_create(SQL_SELECT,NULL); }
 |  INSERT 			    { $$ = _symbol_create(SQL_INSERT,NULL); }
 |  DELETE 			    { $$ = _symbol_create(SQL_DELETE,NULL); }
 |  UPDATE opt_column_commalist     { $$ = _symbol_create_list(SQL_UPDATE,$2); }
 |  REFERENCES opt_column_commalist { $$ = _symbol_create_list(SQL_SELECT,$2); }
 ;

grantee_commalist:			
    grantee			{ $$ = dlist_append_string(dlist_create(), $1); }
 |  grantee_commalist ',' grantee	{ $$ = dlist_append_string($1, $3); }
 ;

grantee:
    PUBLIC			{ $$ = NULL; }
 |  user			{ $$ = $1; }
 ;

/* DOMAIN, TABLE, VIEW, ASSERTION, CHARACTER SET, TRANSLATION, 
 * TRIGGER, PROCEDURE, FUNCTION, ROLE */ 

alter: 
  ALTER TABLE qname ADD table_element 

	{ dlist *l = dlist_create();
	  dlist_append_list(l, $3);
	  dlist_append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_ALTER_TABLE, l ); }
  ;

create:	table_def | view_def ;

table_def:
    CREATE opt_temp TABLE qname table_content_source

	{ dlist *l = dlist_create();
	  dlist_append_symbol(l, $2);
	  dlist_append_list(l, $4);
	  dlist_append_list(l, $5);
	  $$ = _symbol_create_list( SQL_CREATE_TABLE, l ); }
 ;

opt_temp:
    /* empty */ 	{ $$ = NULL; }
 |  LOCAL TEMPORARY 	{ $$ = _symbol_create( SQL_TEMP_LOCAL, NULL); }
 |  GLOBAL TEMPORARY	{ $$ = _symbol_create( SQL_TEMP_GLOBAL, NULL); }
 ;
	
table_content_source:
    '(' table_element_list ')' 	{ $$ = $2; }
 ;

table_element_list:
    table_element	 { $$ = dlist_append_symbol(dlist_create(), $1); }
 |  table_element_list ',' table_element { $$ = dlist_append_symbol( $1, $3 ); }
 ;

table_element: column_def | column_options | table_constraint ;

column_def:
    column data_type opt_column_def_opt_list
   	{ dlist *l = dlist_create();
	  dlist_append_string(l, $1 );
	  dlist_append_string(l, _strdup($2) );
	  dlist_append_list(l, $3 );
	  $$ = _symbol_create_list( SQL_COLUMN, l ); }
 ;

opt_column_def_opt_list:
    /* empty */			{ $$ = NULL; }
 | column_def_opt_list
 ;

column_def_opt_list:
    column_option		{ $$ = dlist_append_symbol(dlist_create(), $1 ); }
 |  column_def_opt_list column_option 	{ $$ = dlist_append_symbol( $1, $2 ); }
 ;

column_options:
    ident WITH OPTIONS '(' column_option_list ')'  

   	{ dlist *l = dlist_create();
	  dlist_append_string(l, $1 );
	  dlist_append_list(l, $5 );
	  $$ = _symbol_create_list( SQL_COLUMN_OPTIONS, l ); }
 ;

column_option_list:
    column_option		{ $$ = dlist_append_symbol(dlist_create(), $1 ); }
 |  column_option_list ',' column_option   { $$ = dlist_append_symbol($1, $3 ); }
 ;

column_option: default | column_constraint ;

default:
    DEFAULT default_value { $$ = $2; }
 ;

/* TODO add auto increment */
default_value:
    literal 
 |  USER     { $$ = _symbol_create_atom( SQL_ATOM, atom_string( 
			sql_bind_type("STRING" ), $1)); }
 |  NULLX 	{ $$ = _symbol_create_atom( SQL_ATOM, NULL);  }
 ;
	
column_constraint:
    opt_constraint_name column_constraint_type  /*opt_constraint_attributes*/

   	{ dlist *l = dlist_create();
 	  dlist_append_list(l, $1 );
	  dlist_append_symbol(l, $2 );
	  $$ = _symbol_create_list( SQL_CONSTRAINT, l ); }
 ;

table_constraint:
    opt_constraint_name table_constraint_type  /*opt_constraint_attributes*/

   	{ dlist *l = dlist_create();
	  dlist_append_list(l, $1 );
	  dlist_append_symbol(l, $2 );
	  $$ = _symbol_create_list( SQL_CONSTRAINT, l ); }
 ;

/* opt_constraint_attributes: ; */

opt_constraint_name:
    /* empty */	 		{ $$ = NULL; }
 |  CONSTRAINT qname 		{ $$ = $2; }
 ;

ref_action:
	NO ACTION		{ $$ = 0; }
 | 	CASCADE			{ $$ = 1; }
 | 	RESTRICT		{ $$ = 2; }
 | 	SET NULLX		{ $$ = 3; }
 | 	SET DEFAULT		{ $$ = 4; }
 ;

opt_ref_delete:
    /* empty */			{ $$ = NULL; }
 | ON DELETE ref_action		{ $$ = _symbol_create_int(SQL_DELETE, $3); }
 ;

opt_ref_update:
    /* empty */			{ $$ = NULL; }
 | ON UPDATE ref_action		{ $$ = _symbol_create_int(SQL_UPDATE, $3); }
 ;

opt_ref_action:
 	opt_ref_delete opt_ref_update
 |	opt_ref_update opt_ref_delete
 ;

opt_match_type:
    /* empty */			{ $$ = 0; }
 | FULL				{ $$ = 1; }
 | PARTIAL			{ $$ = 2; }
 | SIMPLE			{ $$ = 0; }
 ;

opt_match:
    /* empty */			{ $$ = 0; }
 | MATCH opt_match_type	 	{ $$ = $2; }
 ;

column_constraint_type:
    NOT NULLX	{ $$ = _symbol_create( SQL_NOT_NULL, NULL); }
 |  UNIQUE	{ $$ = _symbol_create( SQL_UNIQUE, NULL ); }
 |  PRIMARY KEY	{ $$ = _symbol_create( SQL_PRIMARY_KEY, NULL ); }
 |  REFERENCES qname '(' column_ref ')' opt_match opt_ref_action

			{ dlist *l = dlist_create();
			  dlist_append_list(l, $2 );
			  dlist_append_list(l, $4 );
			  dlist_append_int(l, $6 );
			  dlist_append_symbol(l, $7 );
			  $$ = _symbol_create_list( SQL_FOREIGN_KEY, l); }
 | domain_constraint_type
 ;

table_constraint_type:
    UNIQUE column_commalist_parens  
			{ $$ = _symbol_create_list( SQL_UNIQUE, $2); }
 |  PRIMARY KEY column_commalist_parens 
			{ $$ = _symbol_create_list( SQL_PRIMARY_KEY, $3); }
 |  FOREIGN KEY column_commalist_parens 
    REFERENCES qname opt_match opt_ref_action

			{ dlist *l = dlist_create();
			  dlist_append_list(l, $5 );
			  dlist_append_list(l, $3 );
			  dlist_append_list(l, NULL );
			  dlist_append_int(l, $6 );
			  dlist_append_symbol(l, $7 );
			  $$ = _symbol_create_list( SQL_FOREIGN_KEY, l); }

 |  FOREIGN KEY column_commalist_parens 
    REFERENCES qname column_commalist_parens opt_match opt_ref_action

			{ dlist *l = dlist_create();
			  dlist_append_list(l, $5 );
			  dlist_append_list(l, $3 );
			  dlist_append_list(l, $6 );
			  dlist_append_int(l, $7 );
			  dlist_append_symbol(l, $8 );
			  $$ = _symbol_create_list( SQL_FOREIGN_KEY, l); }

 |  domain_constraint_type
 ;
		
domain_constraint_type:
    CHECK '(' search_condition ')' { $$ = _symbol_create_symbol(SQL_CHECK, $3); }
 ;
		
column_commalist:
    column		     { $$ = dlist_append_string(dlist_create(), $1); }
 |  column_commalist ',' column  { $$ = dlist_append_string( $1, $3 ); }
 ;

view_def:
    CREATE VIEW qname opt_column_commalist AS select_stmt opt_with_check_option

	{ dlist *l = dlist_create();
	  dlist_append_list(l, $3);
	  dlist_append_list(l, $4);
	  dlist_append_symbol(l, $6);
	  dlist_append_int(l, $7);
	  $$ = _symbol_create_list( SQL_CREATE_VIEW, l ); }
    ;
	
opt_with_check_option:
    /* empty */ 		{ $$ = FALSE; }
 |  WITH CHECK OPTION		{ $$ = TRUE; }
 ;

opt_column_commalist:
    /* empty */			{ $$ = NULL; }
 | column_commalist_parens
 ;

column_commalist_parens:
   '(' column_commalist ')' 	{ $$ = $2; } 
 ;

drop:
    DROP TABLE qname drop_action
	
	{ dlist *l = dlist_create();
	  dlist_append_list(l, $3 );
	  dlist_append_int(l, $4 );
	  $$ = _symbol_create_list( SQL_DROP_TABLE, l ); }

 |  DROP VIEW qname	  { $$ = _symbol_create_list( SQL_DROP_VIEW, $3 ); }
 ;

drop_action:
    /* empty */ 	{ $$ = 0; }
 |  RESTRICT  		{ $$ = 0; }
 |  CASCADE	  	{ $$ = 1; }
 ;

	/* cursor definition */
/*
sql: cursor_def ;
cursor_def: DECLARE cursor CURSOR FOR select_stmt opt_order_by_clause ;
sql: close_stmt | open_stmt |	fetch_stmt | delete_stmt_positioned ;
close_stmt: CLOSE cursor		;
open_stmt: OPEN cursor		;
delete_stmt_positioned: DELETE FROM qname WHERE CURRENT OF cursor ;
fetch_stmt: FETCH cursor INTO target_commalist ;
update_stmt: UPDATE qname SET assignment_commalist CURRENT OF cursor ;
cursor:	ident ;
*/


	/* data manipulative stmts */

sql: 
   transaction_stmt 
 | delete_stmt
 | insert_stmt 
 | update_stmt 
 | copyfrom_stmt
 ;

transaction_stmt:
    COMMIT opt_trans opt_chain 	{ $$ = _symbol_create_int( SQL_COMMIT, $3); }
 |  SAVEPOINT ident 		{ $$ = _symbol_create( SQL_SAVEPOINT, $2); }
 |  RELEASE SAVEPOINT ident 	{ $$ = _symbol_create( SQL_RELEASE, $3); }
 |  ROLLBACK opt_trans opt_chain opt_to_savepoint
		{ $$ = _symbol_create_list( SQL_ROLLBACK, 
		   dlist_append_string(
		   	dlist_append_int(dlist_create(), $3), $4 )); }
 ;

opt_trans: /* pure syntax sugar */
    WORK		{ $$ = 0; }
 |  /* empty */		{ $$ = 0; }
 ;

opt_chain:
    AND CHAIN		{ $$ = 1; }
 |  AND NO CHAIN	{ $$ = 0; }
 |  /* empty */		{ $$ = 0; }
 ;

opt_to_savepoint:
    /* empty */ 	{ $$ = NULL; }
 |  TO SAVEPOINT ident  { $$ = $3; }
 ;

copyfrom_stmt:
    COPY opt_nr INTO qname FROM STRING opt_seps 
	{ dlist *l = dlist_create();
	  dlist_append_list(l, $4);
	  dlist_append_string(l, $6);
	  dlist_append_list(l, $7);
	  dlist_append_int(l, $2);
	  $$ = _symbol_create_list( SQL_COPYFROM, l ); }
 ;

opt_seps:
    /* empty */		
				{ dlist *l = dlist_create(); 
				  dlist_append_string(l, _strdup("|")); 
				  dlist_append_string(l, _strdup("\n")); 
				  $$ = l; }
 |  opt_using DELIMITERS STRING 
				{ dlist *l = dlist_create(); 
				  dlist_append_string(l, $3); 
				  dlist_append_string(l, _strdup("\n")); 
				  $$ = l; }
 |  opt_using DELIMITERS STRING ',' STRING 
				{ dlist *l = dlist_create(); 
				  dlist_append_string(l, $3); 
				  dlist_append_string(l, $5);
				  $$ = l; }
 ;

opt_using:
    /* empty */			{ $$ = NULL; }
 |  USING			{ $$ = NULL; }
 ;

opt_nr:
    /* empty */			{ $$ = -1; }
 |  intval RECORDS		{ $$ = $1; }
 ;

delete_stmt:
    DELETE FROM qname opt_where_clause 

	{ dlist *l = dlist_create();
	  dlist_append_list(l, $3);
	  dlist_append_symbol(l, $4);
	  $$ = _symbol_create_list( SQL_DELETE, l ); }
 ;

update_stmt:
    UPDATE qname SET assignment_commalist opt_where_clause

	{ dlist *l = dlist_create();
	  dlist_append_list(l, $2);
	  dlist_append_list(l, $4);
	  dlist_append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_UPDATE, l ); }
 ;


insert_stmt:
    INSERT INTO qname values_or_query_spec

	{ dlist *l = dlist_create();
	  dlist_append_list(l, $3);
	  dlist_append_list(l, NULL);
	  dlist_append_symbol(l, $4);
	  $$ = _symbol_create_list( SQL_INSERT, l ); }

 |  INSERT INTO qname column_commalist_parens values_or_query_spec

	{ dlist *l = dlist_create();
	  dlist_append_list(l, $3);
	  dlist_append_list(l, $4);
	  dlist_append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_INSERT, l ); }
 ;

values_or_query_spec:
    VALUES '(' insert_atom_commalist ')' 
		{ $$ = _symbol_create_list( SQL_VALUES, $3); }
 |  select_stmt
 ;

insert_atom_commalist:
    insert_atom		{ $$ = dlist_append_symbol(dlist_create(), $1); }
 |  insert_atom_commalist ',' insert_atom  { $$ = dlist_append_symbol($1, $3); }
 ;

insert_atom:
    scalar_exp	
 |  NULLX		{ $$ = _symbol_create(SQL_NULL, NULL ); }
 ;

opt_distinct:
    /* empty */ 	{ $$ = FALSE; }	
 |  ALL			{ $$ = FALSE; }
 |  DISTINCT		{ $$ = TRUE; }
 ;

assignment_commalist:
    assignment		 { $$ = dlist_append_symbol(dlist_create(), $1 ); }
 |  assignment_commalist ',' assignment  { $$ = dlist_append_symbol($1, $3 ); }
 ;

assignment:
    column '=' scalar_exp 	

	{ dlist *l = dlist_create();
	  dlist_append_string(l, $1); 
	  dlist_append_symbol(l, $3); 
	  $$ = _symbol_create_list( SQL_ASSIGN, l); }

 |  column '=' NULLX 
	{ dlist *l = dlist_create();
	  dlist_append_string(l, $1); 
	  dlist_append_symbol(l, NULL); 
	  $$ = _symbol_create_list( SQL_ASSIGN, l); }
 ;

target_commalist:
    target		{ $$ = dlist_append_symbol(dlist_create(), $1); }
 |  target_commalist ',' target  { $$ = dlist_append_symbol($1, $3);}
 ;

target:
   TABLE qname  	{ $$ = _symbol_create_list( SQL_TABLE, $2); }
 ;

opt_where_clause:
    /* empty */ 		{ $$ = NULL; }
 |  WHERE search_condition	{ $$ = $2; }
 ;

	/* query expressions */

/* sql: select_stmt | IF '(' predicate ')' sql ; */

joined_table:
   '(' joined_table ')' 	
	{ $$ = $2; }
 |  table_ref CROSS JOIN table_ref

	{ dlist *l = dlist_create();
	  dlist_append_symbol(l, $1);
	  dlist_append_symbol(l, $4);
	  $$ = _symbol_create_list( SQL_CROSS, l); }
 |  table_ref UNIONJOIN table_ref join_spec 
	{ dlist *l = dlist_create();
	  dlist_append_symbol(l, $1);
	  dlist_append_int(l, 0);
	  dlist_append_int(l, 4);
	  dlist_append_symbol(l, $3);
	  dlist_append_symbol(l, $4);
	  $$ = _symbol_create_list( SQL_JOIN, l); }
 |  table_ref JOIN table_ref join_spec 
	{ dlist *l = dlist_create();
	  dlist_append_symbol(l, $1);
	  dlist_append_int(l, 0);
	  dlist_append_int(l, 0);
	  dlist_append_symbol(l, $3);
	  dlist_append_symbol(l, $4);
	  $$ = _symbol_create_list( SQL_JOIN, l); }
 |  table_ref NATURAL JOIN table_ref 
	{ dlist *l = dlist_create();
	  dlist_append_symbol(l, $1);
	  dlist_append_int(l, 1);
	  dlist_append_int(l, 0);
	  dlist_append_symbol(l, $4);
	  dlist_append_symbol(l, NULL);
	  $$ = _symbol_create_list( SQL_JOIN, l); }
 |  table_ref join_type JOIN table_ref join_spec 
	{ dlist *l = dlist_create();
	  dlist_append_symbol(l, $1);
	  dlist_append_int(l, 0);
	  dlist_append_int(l, $2);
	  dlist_append_symbol(l, $4);
	  dlist_append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_JOIN, l); }
 |  table_ref NATURAL join_type JOIN table_ref 
	{ dlist *l = dlist_create();
	  dlist_append_symbol(l, $1);
	  dlist_append_int(l, 1);
	  dlist_append_int(l, $3);
	  dlist_append_symbol(l, $5);
	  dlist_append_symbol(l, NULL);
	  $$ = _symbol_create_list( SQL_JOIN, l); }
  ;

join_type:
    INNER			{ $$ = 0; }
  | outer_join_type OUTER 	{ $$ = 1 + $1; }
  ;

outer_join_type:
    LEFT		{ $$ = 0; }
  | RIGHT		{ $$ = 1; }
  | FULL		{ $$ = 2; }
  ;

join_spec:
    ON search_condition 		{ $$ = $2; }
  | USING column_commalist_parens  	
		{ $$ = _symbol_create_list( SQL_USING, $2); }
  ;

sql: 
    select_stmt 
 ;

select_stmt: 
    select_no_parens                    %prec UMINUS
 |  select_with_parens                    %prec UMINUS
 ;

select_with_parens:
    '(' select_no_parens ')' 	{ $$ = $2; }
 |  '(' select_with_parens ')'  { $$ = $2; }
 ;

select_no_parens:
    simple_select  			
 ;

select_clause:
    simple_select  			
 |  select_with_parens
 ; 

opt_all:
     /* empty */		{ $$ = 0; }
  |  ALL			{ $$ = 1; }
  ;

simple_select:
    SELECT opt_distinct selection opt_into
           table_exp opt_order_by_clause

	{ SelectNode *sn = NEW(SelectNode);
	  sn->distinct = $2;
	  sn->selection = $3;
	  sn->into = $4;
	  sn->from = $5->h->data.sym;
	  sn->where = $5->h->next->data.sym;
	  sn->groupby = $5->h->next->next->data.sym;
	  sn->having = $5->h->next->next->next->data.sym;
	  sn->orderby = $6;
	  sn->name = NULL;

	  $$ = (symbol*)sn;
	  _symbol_init($$, SQL_SELECT); }

 |  select_clause UNION opt_all select_clause

	{ dlist *l = dlist_create();
	  dlist_append_symbol(l, $1);
	  dlist_append_int(l, $3);
	  dlist_append_symbol(l, $4);
	  $$ = _symbol_create_list( SQL_UNION, l); }
 ;

opt_into:
   INTO target_commalist 	{ $$ = $2; }
 | /* empty */			{ $$ = NULL; }
 ;

selection:
    column_exp_commalist
 |  '*' 			{ $$ = NULL; }
 ;

table_exp:
    opt_from_clause opt_where_clause opt_group_by_clause opt_having_clause

	{ $$ = dlist_create();
	  dlist_append_symbol($$, $1);
	  dlist_append_symbol($$, $2);
	  dlist_append_symbol($$, $3);
	  dlist_append_symbol($$, $4); }
 ;

opt_from_clause: 
    /* empty */			 { $$ = NULL; }
 |  FROM table_ref_commalist 	 { $$ = _symbol_create_list( SQL_FROM, $2); }
 ;

table_ref_commalist:
    table_ref			{ $$ = dlist_append_symbol(dlist_create(), $1); }
 |  table_ref_commalist ',' table_ref  { $$ = dlist_append_symbol($1, $3); }
 ;

simple_table:
   qname 			{ dlist *l = dlist_create();
		  		  dlist_append_list(l, $1);  
		  	  	  dlist_append_symbol(l, NULL);  
		  		  $$ = _symbol_create_list(SQL_NAME, l); }
 | qname table_name 		{ dlist *l = dlist_create();
		  		  dlist_append_list(l, $1);  
		  	  	  dlist_append_symbol(l, $2);  
		  		  $$ = _symbol_create_list(SQL_NAME, l); }
 ;

table_ref:
    simple_table
 |  select_with_parens table_name	
				{ SelectNode *sn = (SelectNode*)$1;
				  $$ = $1; 
				  sn->name = $2; }
 |  joined_table 		{ $$ = $1; 
				  dlist_append_symbol($1->data.lval, NULL); }
 |  '(' joined_table ')' table_name	
				{ $$ = $2; 
				  dlist_append_symbol($2->data.lval, $4); }
 ;

table_name:	
    AS ident '(' name_commalist ')'
				{ dlist *l = dlist_create();
		  		  dlist_append_string(l, $2);  
		  	  	  dlist_append_list(l, $4);  
		  		  $$ = _symbol_create_list(SQL_NAME, l); }
 |  AS ident 
				{ dlist *l = dlist_create();
		  		  dlist_append_string(l, $2);  
		  	  	  dlist_append_list(l, NULL);  
		  		  $$ = _symbol_create_list(SQL_NAME, l); }
 |  ident '(' name_commalist ')'
				{ dlist *l = dlist_create();
		  		  dlist_append_string(l, $1);  
		  	  	  dlist_append_list(l, $3);  
		  		  $$ = _symbol_create_list(SQL_NAME, l); }
 |  ident 
				{ dlist *l = dlist_create();
		  		  dlist_append_string(l, $1);  
		  	  	  dlist_append_list(l, NULL);  
		  		  $$ = _symbol_create_list(SQL_NAME, l); }
 ;

opt_group_by_clause:
    /* empty */ 		  { $$ = NULL; }
 |  GROUP BY column_ref_commalist { $$ = _symbol_create_list( SQL_GROUPBY, $3 );}
 ;

column_ref_commalist:
    column_ref			{ $$ = dlist_append_symbol(dlist_create(), 
				       _symbol_create_list(SQL_COLUMN,$1)); }
 |  column_ref_commalist ',' column_ref  { $$ = dlist_append_symbol( $1, 
				       _symbol_create_list(SQL_COLUMN,$3)); }
 ;

opt_having_clause:
    /* empty */ 		 { $$ = NULL; }
 |  HAVING search_condition	 { $$ = $2; }
 ;

	/* search conditions 
 |  NOT search_condition    	{ $$ = _symbol_create_symbol(SQL_NOT, $2); }
*/

search_condition:
    search_condition OR search_condition  	
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_symbol(l, $3); 
		  $$ = _symbol_create_list(SQL_OR, l ); }
 |  search_condition AND search_condition
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_symbol(l, $3); 
		  $$ = _symbol_create_list(SQL_AND, l ); }
 |  '(' search_condition ')'	{ $$ = $2; }
 | predicate			
 ;

opt_order_by_clause:
    /* empty */ 			  { $$ = NULL; }
 |  ORDER BY ordering_spec_commalist  
		{ $$ = _symbol_create_list( SQL_ORDERBY, $3); }
 ;

ordering_spec_commalist:
    ordering_spec		 { $$ = dlist_append_symbol(dlist_create(), $1); }
 |  ordering_spec_commalist ',' ordering_spec 
				 { $$ = dlist_append_symbol( $1, $3 ); }
 ;

ordering_spec:
    intval opt_asc_desc 
	{ dlist *l = dlist_create();
	  dlist_append_symbol(l, _symbol_create_int(SQL_INT_VALUE, (int)$1)); 
	  dlist_append_int(l, $2); 
	  $$ = _symbol_create_list(SQL_COLUMN, l ); }
 |  column_ref opt_asc_desc 
	{ dlist *l = dlist_create();
	  dlist_append_symbol(l, _symbol_create_list(SQL_COLUMN, $1)); 
	  dlist_append_int(l, $2); 
	  $$ = _symbol_create_list(SQL_COLUMN, l ); }
					  
 ;

opt_asc_desc:
    /* empty */ 	{ $$ = TRUE; }
 |  ASC			{ $$ = TRUE; }
 |  DESC		{ $$ = FALSE; }
 ;

predicate: comparison_predicate | between_predicate | like_predicate
 |  test_for_null | in_predicate | all_or_any_predicate | existence_test ;

comparison_predicate:
    scalar_exp COMPARISON scalar_exp	 
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_string(l, $2); 
		  dlist_append_symbol(l, $3); 
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 |  scalar_exp '=' scalar_exp
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_string(l, _strdup("=")); 
		  dlist_append_symbol(l, $3); 
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 ;

between_predicate:
    scalar_exp NOT BETWEEN opt_bounds scalar_exp AND scalar_exp
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_int(l, $4); 
		  dlist_append_symbol(l, $5); 
		  dlist_append_symbol(l, $7); 
		  $$ = _symbol_create_list(SQL_NOT_BETWEEN, l ); }
 |  scalar_exp BETWEEN opt_bounds scalar_exp AND scalar_exp
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_int(l, $3); 
		  dlist_append_symbol(l, $4); 
		  dlist_append_symbol(l, $6); 
		  $$ = _symbol_create_list(SQL_BETWEEN, l ); }
 ;

opt_bounds:
   /* empty */ 	{ $$ = 0; }
 | ASYMMETRIC 	{ $$ = 0; }
 | SYMMETRIC 	{ $$ = 1; }
 ;

like_predicate:
    scalar_exp NOT LIKE atom_exp
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_symbol(l, $4); 
		  $$ = _symbol_create_list(SQL_NOT_LIKE, l ); }
 |  scalar_exp LIKE atom_exp
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_symbol(l, $3); 
		  $$ = _symbol_create_list(SQL_LIKE, l ); }
 ;


atom_exp:
    atom 		
 |  atom ESCAPE atom 	 
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_symbol(l, $3); 
		  $$ = _symbol_create_list(SQL_ESCAPE, l ); }
 ;

test_for_null:
    column_ref IS NOT NULLX  { symbol *s =_symbol_create_list(SQL_COLUMN, $1);
			      $$ = _symbol_create_symbol( SQL_NOT_NULL, s ); }
 |  column_ref IS NULLX	     { symbol *s =_symbol_create_list(SQL_COLUMN, $1);
			       $$ = _symbol_create_symbol( SQL_NULL, s ); }
 ;

in_predicate:
    scalar_exp NOT IN subquery
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_symbol(l, $4); 
		  $$ = _symbol_create_list(SQL_NOT_IN, l ); }
 |  scalar_exp IN subquery 
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_symbol(l, $3); 
		  $$ = _symbol_create_list(SQL_IN, l ); }
 |  scalar_exp NOT IN '(' atom_commalist ')'
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_list(l, $5); 
		  $$ = _symbol_create_list(SQL_NOT_IN, l ); }
 |  scalar_exp IN '(' atom_commalist ')'
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_list(l, $4); 
		  $$ = _symbol_create_list(SQL_IN, l ); }
 ;

atom_commalist:
    atom			{ $$ = dlist_append_symbol(dlist_create(), $1); }
 |  atom_commalist ',' atom 	{ $$ = dlist_append_symbol($1, $3); }
 ;

all_or_any_predicate:
    scalar_exp COMPARISON any_all_some subquery

		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_string(l, $2); 
		  dlist_append_string(l, $3); 
		  dlist_append_symbol(l, $4); 
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 ;
			
any_all_some:
    ANY		{ $$ = $1; }
 |  ALL		{ $$ = $1; }
 |  SOME	{ $$ = $1; }
 ;

existence_test:
    EXISTS subquery 	{ $$ = _symbol_create_symbol( SQL_EXISTS, $2 ); }
 |  NOT EXISTS subquery { $$ = _symbol_create_symbol( SQL_NOT_EXISTS, $3 ); }
 ;

subquery:
    '(' SELECT opt_distinct  selection table_exp ')' 

	{ SelectNode *sn = NEW(SelectNode);
	  sn->distinct = $3;
	  sn->selection = $4;
	  sn->into = NULL;
	  sn->from = $5->h->data.sym;
	  sn->where = $5->h->next->data.sym;
	  sn->groupby = $5->h->next->next->data.sym;
	  sn->having = $5->h->next->next->next->data.sym;
	  sn->orderby = NULL;
	  $$ = (symbol*)sn;
	  _symbol_init($$, SQL_SELECT); }
 ;

	/* scalar expressions */

scalar_exp:
    value_exp

 |  scalar_exp '+' scalar_exp	
				{ dlist *l = dlist_create();
				  dlist_append_string(l, _strdup("add"));
	  			  dlist_append_symbol(l, $1);
	  			  dlist_append_symbol(l, $3);
	  			  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '-' scalar_exp   
				{ dlist *l = dlist_create();
				  dlist_append_string(l, _strdup("sub"));
	  			  dlist_append_symbol(l, $1);
	  			  dlist_append_symbol(l, $3);
	  			  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '*' scalar_exp
				{ dlist *l = dlist_create();
				  dlist_append_string(l, _strdup("mul"));
	  			  dlist_append_symbol(l, $1);
	  			  dlist_append_symbol(l, $3);
	  			  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  scalar_exp '/' scalar_exp
				{ dlist *l = dlist_create();
				  dlist_append_string(l, _strdup("div"));
	  			  dlist_append_symbol(l, $1);
	  			  dlist_append_symbol(l, $3);
	  			  $$ = _symbol_create_list( SQL_BINOP, l ); }
 |  '+' scalar_exp %prec UMINUS { dlist *l = dlist_create();
				  dlist_append_string(l, _strdup("pos"));
	  			  dlist_append_symbol(l, $2);
	  			  $$ = _symbol_create_list( SQL_UNOP, l ); }
 |  '-' scalar_exp %prec UMINUS { dlist *l = dlist_create();
				  dlist_append_string(l, _strdup("neg"));
	  			  dlist_append_symbol(l, $2);
	  			  $$ = _symbol_create_list( SQL_UNOP, l ); }
 |  '(' scalar_exp ')' 		{ $$ = $2; }
 |  subquery			%prec UMINUS
 ;

value_exp:
    atom
 |  column_ref 			{ $$ = _symbol_create_list( SQL_COLUMN, $1); }
 |  function_ref 
 |  datetime_funcs
 |  string_funcs
 |  case_exp	
 |  cast_exp
 ;


datetime_funcs:
    EXTRACT '(' datetime_field FROM scalar_exp ')' 
				{ dlist *l = dlist_create();
				  const char *ident = datetime_field($3);
  		  		  dlist_append_string(l, _strdup(ident));
  		  		  dlist_append_symbol(l, $5);
		  		  $$ = _symbol_create_list( SQL_UNOP, l ); }
 ;

string_funcs:
    SUBSTRING '(' scalar_exp FROM intval FOR intval ')' 
				{ dlist *l = dlist_create();
				  sql_type *t = sql_bind_type("INTEGER");
  		  		  dlist_append_string(l, _strdup("substring"));
  		  		  dlist_append_symbol(l, $3);
  		  		  dlist_append_symbol(l, _symbol_create_atom(
					SQL_ATOM, atom_int(t, $5 -1 )));
  		  		  dlist_append_symbol(l, _symbol_create_atom(
					SQL_ATOM, atom_int(t, $7 )));
		  		  $$ = _symbol_create_list( SQL_TRIOP, l ); }
 |  scalar_exp CONCATSTRING scalar_exp  
				{ dlist *l = dlist_create();
  		  		  dlist_append_string(l, _strdup("concat"));
  		  		  dlist_append_symbol(l, $1);
  		  		  dlist_append_symbol(l, $3);
		  		  $$ = _symbol_create_list( SQL_BINOP, l ); }
 ;

column_exp_commalist:
    column_exp 		{ $$ = dlist_append_symbol(dlist_create(), $1 ); }
 |  column_exp_commalist ',' column_exp  { $$ = dlist_append_symbol( $1, $3 ); }
 ;

column_exp:
    ident '.' '*'	
		{ dlist *l = dlist_create();
  		  dlist_append_string(l, $1);
  		  dlist_append_string(l, NULL);
  		  $$ = _symbol_create_list( SQL_TABLE, l ); }
 |  scalar_exp opt_column_name	
		{ dlist *l = dlist_create();
  		  dlist_append_symbol(l, $1);
  		  dlist_append_string(l, $2);
  		  $$ = _symbol_create_list( SQL_COLUMN, l ); }
 ;

opt_column_name:
    /* empty */	{ $$ = NULL; }
 |  AS ident	{ $$ = $2; }
 ;

atom:
    literal 	
 |  USER     { $$ = _symbol_create_atom( SQL_ATOM, atom_string( 
			sql_bind_type("STRING" ), $1)); }
 ;

/* change to set function */
function_ref:
    AMMSC '(' '*' ')' 	
		{ dlist *l = dlist_create();
  		  dlist_append_string(l, $1);
  		  dlist_append_int(l, FALSE);
  		  dlist_append_symbol(l, NULL);
		  $$ = _symbol_create_list( SQL_AGGR, l ); }
 |  AMMSC '(' DISTINCT column_ref ')' 
		{ dlist *l = dlist_create();
  		  dlist_append_string(l, $1);
  		  dlist_append_int(l, TRUE);
  		  dlist_append_symbol(l, _symbol_create_list(SQL_COLUMN, $4));
		  $$ = _symbol_create_list( SQL_AGGR, l ); }
 |  AMMSC '(' ALL scalar_exp ')'
		{ dlist *l = dlist_create();
  		  dlist_append_string(l, $1);
  		  dlist_append_int(l, FALSE);
  		  dlist_append_symbol(l, $4);
		  $$ = _symbol_create_list( SQL_AGGR, l ); }
 |  AMMSC '(' scalar_exp ')' 
		{ dlist *l = dlist_create();
  		  dlist_append_string(l, $1);
  		  dlist_append_int(l, FALSE);
  		  dlist_append_symbol(l, $3);
		  $$ = _symbol_create_list( SQL_AGGR, l ); }
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

time_persision:
	'(' intval ')' 	{ $$ = $2; }
 | /* empty */		{ $$ = 0; }
 ;

datetime_type:
    DATE			{ $$ = sql_bind_type("DATE")->sqlname; }
 |  TIME time_persision tz 	{ $$ = sql_bind_type("TIME")->sqlname; }
 |  TIMESTAMP time_persision tz { $$ = sql_bind_type("TIMESTAMP")->sqlname; }
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

start_field:
    non_second_datetime_field time_persision
		{ $$ = dlist_append_int( 
			 	dlist_append_int( dlist_create(), $1), $2);  }
 ;

end_field:
    non_second_datetime_field 	
		{ $$ = dlist_append_int( 
			 	dlist_append_int( dlist_create(), $1), 0);  }
 |  SECOND time_persision	
		{ $$ = dlist_append_int( 
			 	dlist_append_int( dlist_create(), isec), $2);  }
 ;

single_datetime_field:
    non_second_datetime_field time_persision 
		{ $$ = dlist_append_int( 
			 	dlist_append_int( dlist_create(), $1), $2);  }
 |  SECOND time_persision	
		{ $$ = dlist_append_int( 
			 	dlist_append_int( dlist_create(), isec), $2);  }
 ;

interval_qualifier:
    start_field TO end_field  
	{ $$ =  dlist_append_list(
			dlist_append_list( dlist_create(), $1), $3 ); }
 |  single_datetime_field    
	{ $$ =  dlist_append_list( dlist_create(), $1); }
 ;

interval_type:
    INTERVAL interval_qualifier	{ $$ = "INTERVAL"; }
 ;

literal:
    STRING   { $$ = _symbol_create_atom( SQL_ATOM, atom_string( 
			sql_bind_type("STRING" ), $1)); }
 |  intval   { $$ = _symbol_create_atom( SQL_ATOM, atom_int( 
			sql_bind_type("INTEGER" ), $1)); }
 |  INTNUM   { $$ = _symbol_create_atom( SQL_ATOM, atom_float(
			sql_bind_type("FLOAT" ), strtod($1,&$1))); }
 |  APPROXNUM{ $$ = _symbol_create_atom( SQL_ATOM, atom_float(
			sql_bind_type("DOUBLE" ), strtod($1,&$1))); }
 |  DATE STRING { $$ = _symbol_create_atom( SQL_ATOM, atom_general(
			sql_bind_type("DATE" ),$2));  }
 |  TIME STRING { $$ = _symbol_create_atom( SQL_ATOM, atom_general(
			sql_bind_type("TIME" ),$2));  }
 |  TIMESTAMP STRING { $$ = _symbol_create_atom( SQL_ATOM, atom_general(
			sql_bind_type("TIMESTAMP" ),$2));  }
 |  INTERVAL opt_sign STRING interval_qualifier
	{ context *lc = (context*)parm;
	  int i,tpe;
	  if ( (tpe = parse_interval( lc, $2, $3, $4, &i)) < 0 ){
		yyerror("incorrect interval");
		$$ = NULL;
	  } else {
		sql_type *t = NULL;
		if (tpe == 0){
			t = sql_bind_type("MONTH_INTERVAL");
		} else {
			t = sql_bind_type("SEC_INTERVAL");
		}
	  	$$ = _symbol_create_atom( SQL_ATOM, atom_int(t,i));
	  }
	}
 |  TYPE STRING { $$ = _symbol_create_atom( SQL_ATOM, atom_general(
		  sql_bind_type($1 ),$2)); 
	  	  _DELETE($1); }
 |  BOOL_FALSE  { $$ = _symbol_create_atom( SQL_ATOM, atom_general(
		  sql_bind_type("BOOL"), "false"));	}
 |  BOOL_TRUE  { $$ = _symbol_create_atom( SQL_ATOM, atom_general(
		  sql_bind_type("BOOL"), "true"));	}
 ;

	/* miscellaneous */

qname:
    ident		{ $$ = dlist_append_string(dlist_create(), $1); }
 |  ident '.' ident	{ $$ = dlist_append_string(
				dlist_append_string(dlist_create(), $1), $3);}
 ;

column_ref:
    ident		{ $$ = dlist_append_string(
				dlist_create(), $1); }

 |  ident '.' ident	{ $$ = dlist_append_string(
				dlist_append_string(
				 dlist_create(), $1), $3);}

 |  ident '.' ident '.' ident
    			{ $$ = dlist_append_string(
				dlist_append_string(
				 dlist_append_string(
				  dlist_create(), $1), $3), $5);}
 ;

cast_exp:
     CAST '(' scalar_exp AS data_type ')'
 	{ dlist *l = dlist_create();
	  dlist_append_symbol(l, $3);
	  dlist_append_string(l, _strdup($5));
	  $$ = _symbol_create_list( SQL_CAST, l ); }
 ;

case_exp: /* could rewrite NULLIF and COALESCE to normal CASE stmts */
     NULLIF '(' scalar_exp ',' scalar_exp ')' 
		{ $$ = _symbol_create_list(SQL_NULLIF,
		   dlist_append_symbol(
		    dlist_append_symbol(
		     dlist_create(), $3), $5)); }
 |   COALESCE '(' scalar_exp_list ')'
		{ $$ = _symbol_create_list(SQL_COALESCE, $3); }
 |   CASE scalar_exp when_value_list opt_else END
		{ $$ = _symbol_create_list(SQL_CASE, 
		   dlist_append_symbol(
		    dlist_append_list(
		     dlist_append_symbol(
		      dlist_create(),$2),$3),$4)); }
 |   CASE when_search_list opt_else END
		 { $$ = _symbol_create_list(SQL_CASE, 
		   dlist_append_symbol(
		    dlist_append_list(
		     dlist_create(),$2),$3)); }
 ;

scalar_exp_list:
    scalar_exp			
			{ $$ = dlist_append_symbol( dlist_create(), $1);}
 |  scalar_exp_list ',' scalar_exp
			{ $$ = dlist_append_symbol( $1, $3);}
 ;

when_value:
    WHEN scalar_exp THEN scalar_exp
			{ $$ = _symbol_create_list( SQL_WHEN,
			   dlist_append_symbol(
			    dlist_append_symbol(
			     dlist_create(), $2),$4)); }
 ;

when_value_list:
    when_value
			{ $$ = dlist_append_symbol( dlist_create(), $1);}
 |  when_value_list when_value
			{ $$ = dlist_append_symbol( $1, $2); }
 ;

when_search:
    WHEN search_condition THEN scalar_exp
			{ $$ = _symbol_create_list( SQL_WHEN,
			   dlist_append_symbol(
			    dlist_append_symbol(
			     dlist_create(), $2),$4)); }
 ;

when_search_list:
    when_search
			{ $$ = dlist_append_symbol( dlist_create(), $1);}
 |  when_search_list when_search
			{ $$ = dlist_append_symbol( $1, $2);}
 ;

opt_else:
    ELSE scalar_exp	{ $$ = $2; }
 ;

		/* data types, more types to come */

data_type:
    CHARACTER			{ $$ = "CHARACTER"; }
 |  CHARACTER '(' intval ')'	{ $$ = "CHARACTER"; }
 |  NUMERIC			{ $$ = "NUMERIC"; }
 |  NUMERIC '(' intval ')'		{ $$ = "NUMERIC"; }
 |  NUMERIC '(' intval ',' intval ')' { $$ = "NUMERIC"; }
 |  DECIMAL			{ $$ = "DECIMAL"; }
 |  DECIMAL '(' intval ')'		{ $$ = "DECIMAL"; }
 |  DECIMAL '(' intval ',' intval ')' { $$ = "DECIMAL"; }
 |  INTEGER '(' intval ')'		{ $$ = "INTEGER"; }
 |  INTEGER			{ $$ = "INTEGER"; }
 |  SMALLINT			{ $$ = "SMALLINT"; }
 |  SMALLINT '(' intval ')'	{ $$ = "SMALLINT"; }
 |  FLOAT			{ $$ = "FLOAT"; }
 |  FLOAT '(' intval ')'		{ $$ = "FLOAT"; }
 |  FLOAT '(' intval ',' intval ')'	{ $$ = "FLOAT"; }
 |  REAL			{ $$ = "REAL"; }
 |  DOUBLE 			{ $$ = "DOUBLE"; }
 |  DOUBLE '(' intval ',' intval ')' 	{ $$ = "DOUBLE"; }
 |  DOUBLE PRECISION		{ $$ = "DOUBLE"; }
 |  VARCHAR			{ $$ = "VARCHAR"; }
 |  VARCHAR '(' intval ')'	{ if ($3 == 1) $$ = "VARCHAR(1)";
				  else $$ = "VARCHAR"; }
 | datetime_type
 | interval_type
 | TYPE				{ $$ = $1; }
 ;

column:	ident ;

user: 		ident ;

ident: 
    IDENT	{ $$ = $1; }	
 |  TYPE	{ $$ = $1; }
 |  non_reserved_word
 ;

non_reserved_word: 
  CHARACTER 	{ $$ = _strdup("character"); }
| NUMERIC 	{ $$ = _strdup("numeric"); }
| DECIMAL 	{ $$ = _strdup("decimal"); }
| INTEGER 	{ $$ = _strdup("integer"); }
| SMALLINT 	{ $$ = _strdup("smallint"); }
| FLOAT 	{ $$ = _strdup("float"); }
| REAL 		{ $$ = _strdup("real"); }
| DOUBLE 	{ $$ = _strdup("double"); }
| PRECISION 	{ $$ = _strdup("precision"); }
| VARCHAR 	{ $$ = _strdup("varchar"); }
| DATE 		{ $$ = _strdup("date"); }
| TIME 		{ $$ = _strdup("time"); }
| TIMESTAMP	{ $$ = _strdup("timestamp"); }
| PATH		{ $$ = _strdup("path"); }
;

name_commalist:
    ident			{ $$ = dlist_append_string(dlist_create(), $1); }
 |  name_commalist ',' ident  	{ $$ = dlist_append_string($$, $3); } 
 ;

intval:
	INT			{ $$ = strtol($1,&$1,10); }
 ;

	/* embedded condition things */
/* sql: WHENEVER NOT FOUND when_action |	WHENEVER SQLERROR when_action ;
when_action:	GOTO IDENT |	CONTINUE ; */
%%
