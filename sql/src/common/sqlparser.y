%{
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
	sqlstatement
	schema
	opt_schema_default_char_set
	opt_schema_path
	schema_element
	delete_statement_searched
	table_def
	view_def
	all_or_any_predicate
	atom_exp
	between_predicate
	comparison_predicate
	from_clause
	existence_test
	in_predicate
	insert_statement
	commit_statement
	rollback_statement
	like_predicate
	opt_where_clause
	opt_having_clause
	opt_group_by_clause
	predicate
	query_spec
	query_term
	query_exp
    	non_join_query_exp
	joined_table
    	cross_table
	join_spec
    	qualified_join
	search_condition
	update_statement
	select_statement
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
	scalar_exp
	column_exp
	atom
	insert_atom
	parameter_ref
	literal
	ordering_spec
	table_ref 
	opt_temp

%type <sval>
	any_all_some
	non_reserved_word
	ident
	column
	parameter
	range_variable
	user
	name
	data_type
	datetime_type
	interval_type
	grantee
	opt_name

%type <l>
	schema_name
	sql_list 
	assignment_commalist
	opt_column_commalist
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
	opt_into
	target_commalist
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

%type <ival>
	drop_action 
	opt_natural
	opt_join_type
	outer_join_type
	time_persision
	non_second_datetime_field
	opt_sign

%type <bval>
	opt_distinct
	opt_with_check_option
	opt_with_grant_option
	opt_asc_desc
	tz

%token <sval> 
	NAME STRING AMMSC PARAMETER INT INTNUM APPROXNUM USER USING
	ALL DISTINCT ANY SOME CHECK GLOBAL LOCAL CASCADE RESTRICT
	CHARACTER NUMERIC DECIMAL INTEGER SMALLINT FLOAT REAL
	DOUBLE PRECISION VARCHAR 

/*
OPEN CLOSE FETCH 
*/
%token <sval> DELETE UPDATE COMMIT ROLLBACK SELECT INSERT
%token <sval> LEFT RIGHT FULL OUTER NATURAL CROSS JOIN INNER
	
%token <operation> '+' '-' '*' '/'
%token <sval> LIKE BETWEEN ORDER BY
%token <operation> IN EXISTS ESCAPE HAVING GROUP NULLX 
%token <operation> FROM

	/* operators */

%left <operation> OR
%left <operation> AND
%left <operation> NOT
%left <operation> '('
%left <sval> COMPARISON /* <> < > <= >= */
%left <operation> '='
%left '+' '-'
%left '*' '/'
%nonassoc UMINUS

	/* literal keyword tokens */

/*
CONTINUE CURRENT CURSOR DECLARE FOR FOUND GOTO GO
LANGUAGE OF PROCEDURE
SQLCODE SQLERROR 
UNDER WHENEVER 
*/
%token TEMPORARY 
%token<sval> AMMSC AS ASC DESC AUTHORIZATION 
%token CHECK CONSTRAINT CREATE 
%token DEFAULT DISTINCT DROP
%token FOREIGN 
%token GRANT HAVING INDICATOR INTO
%token IS KEY ON OPTION OPTIONS
%token PATH PARAMETER PRIMARY PRIVILEGES 
%token<sval> PUBLIC REFERENCES SCHEMA SET
%token ALTER ADD TABLE TO UNION UNIQUE USER VALUES VIEW WHERE WITH WORK
%token<sval> DATE TIME TIMESTAMP INTERVAL
%token YEAR MONTH DAY HOUR MINUTE SECOND ZONE

%%

sql_list:
    sqlstatement 	{ context *lc = (context*)parm;
			  lc->l = $$ = dlist_append_symbol(dlist_create(), $1 );
			  sql_statement_init(lc); }
 |  sql_list sqlstatement { context *lc = (context*)parm;
			  lc->l = $$ = dlist_append_symbol($1, $2); 
			  sql_statement_init(lc); }
 ;

sqlstatement:
   sql ';' 		
 | /*empty*/		{ $$ = NULL; }


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
    name		    	
	{ $$ = dlist_create();
	  dlist_append_string($$, $1 );
	  dlist_append_string($$, NULL ); }
 |  AUTHORIZATION name  	
	{ $$ = dlist_create();
	  dlist_append_string($$, NULL );
	  dlist_append_string($$, $2 ); }
 |  name AUTHORIZATION name 
	{ $$ = dlist_create();
	  dlist_append_string($$, $1 );
	  dlist_append_string($$, $3 ); }

opt_schema_default_char_set:
    /* empty */		   	{ $$ = NULL; }
 |  DEFAULT CHARACTER SET name 	{ $$ = _symbol_create( SQL_CHARSET, $4 ); }
 ;

opt_schema_path:
    /* empty */			{ $$ = NULL; }
 |  PATH name_commalist 	{ $$ = _symbol_create_list( SQL_PATH, $2 ); }
 ;

opt_schema_element_list:
    /* empty */			{ $$ = NULL; }
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
 |  UPDATE opt_column_commalist     { $$ = _symbol_create_list(SQL_UPDATE_SET,$2); }
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
 * TRIGGER, TYPE, PROCEDURE, FUNCTION, ROLE */ 

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
 |  USER 	{ $$ = _symbol_create_atom( SQL_ATOM, atom_string($1)); }
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

column_constraint_type:
    NOT NULLX	{ $$ = _symbol_create( SQL_NOT_NULL, NULL); }
 |  UNIQUE	{ $$ = _symbol_create( SQL_UNIQUE, NULL ); }
 |  PRIMARY KEY	{ $$ = _symbol_create( SQL_PRIMARY_KEY, NULL ); }
 |  REFERENCES qname '(' column_ref ')'

			{ dlist *l = dlist_create();
			  dlist_append_list(l, $2 );
			  dlist_append_list(l, $4 );
			  $$ = _symbol_create_list( SQL_FOREIGN_KEY, l); }
 | domain_constraint_type
 ;

table_constraint_type:
    UNIQUE '(' column_commalist ')' 
			{ $$ = _symbol_create_list( SQL_UNIQUE, $3); }
 |  PRIMARY KEY '(' column_commalist ')' 
			{ $$ = _symbol_create_list( SQL_PRIMARY_KEY, $4); }
 |  FOREIGN KEY '(' column_commalist ')' REFERENCES qname 

			{ dlist *l = dlist_create();
			  dlist_append_list(l, $4 );
			  dlist_append_list(l, $7 );
			  $$ = _symbol_create_list( SQL_FOREIGN_KEY, l); }

 |  FOREIGN KEY '(' column_commalist ')'
    REFERENCES qname '(' column_commalist ')'

			{ dlist *l = dlist_create();
			  dlist_append_list(l, $4 );
			  dlist_append_list(l, $7 );
			  dlist_append_list(l, $9 );
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
    CREATE VIEW qname opt_column_commalist AS query_spec opt_with_check_option

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
 |  '(' column_commalist ')' 	{ $$ = $2; } 
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
cursor_def: DECLARE cursor CURSOR FOR query_exp opt_order_by_clause ;
sql: close_statement | open_statement |	fetch_statement | delete_statement_positioned ;
close_statement: CLOSE cursor		;
open_statement: OPEN cursor		;
delete_statement_positioned: DELETE FROM qname WHERE CURRENT OF cursor ;
fetch_statement: FETCH cursor INTO target_commalist ;
update_statement: UPDATE qname SET assignment_commalist CURRENT OF cursor ;
cursor:	ident ;
*/


	/* data manipulative statements */

sql: commit_statement |	rollback_statement | delete_statement_searched 
 | insert_statement | select_statement | update_statement ;

commit_statement:
    COMMIT WORK 	{ $$ = _symbol_create( SQL_COMMIT, NULL); }
 ;

rollback_statement:
    ROLLBACK WORK 	{ $$ = _symbol_create( SQL_ROLLBACK, NULL); }
 ;

delete_statement_searched:
    DELETE FROM qname opt_where_clause 

	{ dlist *l = dlist_create();
	  dlist_append_list(l, $3);
	  dlist_append_symbol(l, $4);
	  $$ = _symbol_create_list( SQL_DELETE, l ); }
 ;

update_statement:
    UPDATE qname SET assignment_commalist opt_where_clause

	{ dlist *l = dlist_create();
	  dlist_append_list(l, $2);
	  dlist_append_list(l, $4);
	  dlist_append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_UPDATE_SET, l ); }
 ;


insert_statement:
    INSERT INTO qname opt_column_commalist values_or_query_spec

	{ dlist *l = dlist_create();
	  dlist_append_list(l, $3);
	  dlist_append_list(l, $4);
	  dlist_append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_INSERT_INTO, l ); }
 ;

values_or_query_spec:
    VALUES '(' insert_atom_commalist ')' 
		{ $$ = _symbol_create_list( SQL_VALUES, $3); }
 |  query_spec
 ;

insert_atom_commalist:
    insert_atom		{ $$ = dlist_append_symbol(dlist_create(), $1); }
 |  insert_atom_commalist ',' insert_atom  { $$ = dlist_append_symbol($1, $3); }
 ;

insert_atom:
    atom	
 |  NULLX		{ $$ = _symbol_create(SQL_NULL, NULL ); }
 ;

select_statement:
    SELECT opt_distinct selection opt_into table_exp opt_order_by_clause

	{ dlist *l = dlist_create();
	  dlist_append_int(l, $2);
	  dlist_append_list(l, $3);
	  dlist_append_list(l, $4);
	  dlist_append_list(l, $5);
	  dlist_append_symbol(l, $6);
	  $$ = _symbol_create_list( SQL_SELECT, l ); 		}
 ;

opt_into:
    /* empty */ 		{ $$ = NULL; }
 |  INTO target_commalist 	{ $$ = $2; }

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
    parameter_ref	
 |  TABLE qname  	{ $$ = _symbol_create_list( SQL_TABLE, $2); }
 ;

opt_where_clause:
    /* empty */ 		{ $$ = NULL; }
 |  WHERE search_condition	{ $$ = $2; }
 ;

	/* query expressions */

/* sql: query_exp | IF '(' predicate ')' sql ; */


joined_table:
    cross_table
  | qualified_join
  | '(' joined_table ')' 		{ $$ = $2; }
  ;

cross_table:
    table_ref CROSS JOIN table_ref

	{ dlist *l = dlist_create();
	  dlist_append_symbol(l, $1);
	  dlist_append_symbol(l, $4);
	  $$ = _symbol_create_list( SQL_CROSS, l); }
  ;

qualified_join:
    table_ref opt_natural opt_join_type JOIN table_ref join_spec
	{ dlist *l = dlist_create();
	  dlist_append_symbol(l, $1);
	  dlist_append_int(l, $2);
	  dlist_append_int(l, $3);
	  dlist_append_symbol(l, $5);
	  dlist_append_symbol(l, $6);
	  $$ = _symbol_create_list( SQL_JOIN, l); }
  ;

opt_natural:
    /* empty */	{ $$ = 0; }
  | NATURAL	{ $$ = 1; }
  ;

opt_join_type:
    /* empty */			{ $$ = 0; }
  | INNER			{ $$ = 0; }
  | outer_join_type OUTER 	{ $$ = 1 + $1; }
  | UNION 			{ $$ = 4; }
  ;

outer_join_type:
    LEFT		{ $$ = 0; }
  | RIGHT		{ $$ = 1; }
  | FULL		{ $$ = 2; }
  ;

join_spec:
    ON search_condition 		{ $$ = $2; }
  | USING '(' column_commalist ')' 	
		{ $$ = _symbol_create_list( SQL_USING, $3); }
  ;

sql: query_exp 

query_exp:
    non_join_query_exp
 |  joined_table

non_join_query_exp:
    query_term				
 |  query_exp UNION query_term		

	{ dlist *l = dlist_create();
	  dlist_append_symbol(l, $1);
	  dlist_append_symbol(l, $3);
	  $$ = _symbol_create_list( SQL_UNION, l); }

 |  query_exp UNION ALL query_term		

	{ dlist *l = dlist_create();
	  dlist_append_symbol(l, $1);
	  dlist_append_symbol(l, $4);
	  $$ = _symbol_create_list( SQL_UNION_ALL, l); }
 ;

query_term:
    query_spec 
 |  '(' query_exp ')'  		{ $$ = $2; }
 ;

query_spec:
    SELECT opt_distinct	selection table_exp opt_order_by_clause

	{ dlist *l = dlist_create();
	  dlist_append_int(l, $2);
	  dlist_append_list(l, $3);
	  dlist_append_list(l, NULL); /* no INTO */
	  dlist_append_list(l, $4);
	  dlist_append_symbol(l, $5);
	  $$ = _symbol_create_list( SQL_SELECT, l ); 		}
 ;

selection:
    column_exp_commalist
 |  '*' 			{ $$ = NULL; }
 ;

table_exp:
    from_clause opt_where_clause opt_group_by_clause opt_having_clause

	{ $$ = dlist_create();
	  dlist_append_symbol($$, $1);
	  dlist_append_symbol($$, $2);
	  dlist_append_symbol($$, $3);
	  dlist_append_symbol($$, $4); }
 ;

from_clause: 
    FROM table_ref_commalist 	 { $$ = _symbol_create_list( SQL_FROM, $2); }
 ;

table_ref_commalist:
    table_ref			{ $$ = dlist_append_symbol(dlist_create(), $1); }
 |  table_ref_commalist ',' table_ref  { $$ = dlist_append_symbol($1, $3); }
 ;

table_ref:
    qname			{ $$ = _symbol_create_list(SQL_TABLE, $1 ); }
 |  qname range_variable 	{ dlist *l = dlist_create();
		  		  dlist_append_list(l, $1);  
		  	  	  dlist_append_string(l, $2);  
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
 |  HAVING search_condition	 { $$ = _symbol_create(SQL_HAVING, (char*)$2); }
 ;

	/* search conditions */

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
 |  NOT search_condition    	{ $$ = _symbol_create_symbol(SQL_NOT, $2); }
 |  '(' search_condition ')'	{ $$ = $2; }
 |  predicate			
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
    INT opt_asc_desc 
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
 |  scalar_exp COMPARISON subquery
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
 |  scalar_exp '=' subquery
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_string(l, _strdup("=")); 
		  dlist_append_symbol(l, $3); 
		  $$ = _symbol_create_list(SQL_COMPARE, l ); }
 ;

between_predicate:
    scalar_exp NOT BETWEEN scalar_exp AND scalar_exp
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_symbol(l, $4); 
		  dlist_append_symbol(l, $6); 
		  $$ = _symbol_create_list(SQL_NOT_BETWEEN, l ); }
 |  scalar_exp BETWEEN scalar_exp AND scalar_exp
		{ dlist *l = dlist_create();
		  dlist_append_symbol(l, $1); 
		  dlist_append_symbol(l, $3); 
		  dlist_append_symbol(l, $5); 
		  $$ = _symbol_create_list(SQL_BETWEEN, l ); }
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
    column_ref IS NOT NULLX  { $$ = _symbol_create( SQL_NOT_NULL, (char*)$1 ); }
 |  column_ref IS NULLX	     { $$ = _symbol_create( SQL_NULL, (char*)$1 ); }
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
 ;

subquery:
    '(' SELECT opt_distinct  selection table_exp ')' 

	{ dlist *l = dlist_create();
	  dlist_append_int(l, $3);
	  dlist_append_list(l, $4);
	  dlist_append_list(l, NULL); /* no INTO */
	  dlist_append_list(l, $5);
	  dlist_append_symbol(l, NULL); /* no ordering*/
	  $$ = _symbol_create_list( SQL_SELECT, l ); 		}
 ;

	/* scalar expressions */

scalar_exp:
    scalar_exp '+' scalar_exp	
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
 |  atom
 |  column_ref 			{ $$ = _symbol_create_list( SQL_COLUMN, $1); }
 |  function_ref 
 |  '(' scalar_exp ')' 		{ $$ = $2; }
 ;

column_exp_commalist:
    column_exp 			{ $$ = dlist_append_symbol(dlist_create(), $1 ); }
 |  column_exp_commalist ',' column_exp  { $$ = dlist_append_symbol( $1, $3 ); }
 ;

column_exp:
    ident '.' '*'	
		{ dlist *l = dlist_create();
  		  dlist_append_string(l, $1);
  		  dlist_append_string(l, NULL);
  		  $$ = _symbol_create_list( SQL_TABLE, l ); }
 |  scalar_exp opt_name	
		{ dlist *l = dlist_create();
  		  dlist_append_symbol(l, $1);
  		  dlist_append_string(l, $2);
  		  $$ = _symbol_create_list( SQL_COLUMN, l ); }
 ;

opt_name:
    /* empty */	{ $$ = NULL; }
 |  AS ident	{ $$ = $2; }
 ;

atom:
    parameter_ref	
 |  literal 	
 |  USER 	{ $$ = _symbol_create_atom( SQL_ATOM, atom_string($1)); }
 ;

parameter_ref:
    parameter			  
		{ dlist *l = dlist_create();
  		  dlist_append_string(l, $1);
		  $$ = _symbol_create_list( SQL_PARAMETER, l ); }
 |  parameter parameter 	  
		{ dlist *l = dlist_create();
  		  dlist_append_string(l, $1);
  		  dlist_append_string(l, $2);
		  $$ = _symbol_create_list( SQL_PARAMETER, l ); }
 |  parameter INDICATOR parameter 
		{ dlist *l = dlist_create();
  		  dlist_append_string(l, $1);
  		  dlist_append_string(l, $3);
		  $$ = _symbol_create_list( SQL_PARAMETER, l ); }
 ;

function_ref:
    AMMSC '(' '*' ')' 	
		{ dlist *l = dlist_create();
  		  dlist_append_string(l, toLower($1));
  		  dlist_append_int(l, FALSE);
  		  dlist_append_symbol(l, NULL);
		  $$ = _symbol_create_list( SQL_AMMSC, l ); }
 |  AMMSC '(' DISTINCT column_ref ')' 
		{ dlist *l = dlist_create();
  		  dlist_append_string(l, toLower($1));
  		  dlist_append_int(l, TRUE);
  		  dlist_append_symbol(l, _symbol_create_list(SQL_COLUMN, $4));
		  $$ = _symbol_create_list( SQL_AMMSC, l ); }
 |  AMMSC '(' ALL scalar_exp ')'
		{ dlist *l = dlist_create();
  		  dlist_append_string(l, toLower($1));
  		  dlist_append_int(l, FALSE);
  		  dlist_append_symbol(l, $4);
		  $$ = _symbol_create_list( SQL_AMMSC, l ); }
 |  AMMSC '(' scalar_exp ')'
		{ dlist *l = dlist_create();
  		  dlist_append_string(l, toLower($1));
  		  dlist_append_int(l, FALSE);
  		  dlist_append_symbol(l, $3);
		  $$ = _symbol_create_list( SQL_AMMSC, l ); }
 ;

opt_sign:
   '+'		{ $$ = 1; }
 | '-' 		{ $$ = -1; }
 | /* empty */	{ $$ = 1; }


tz:
	WITH TIME ZONE	{ $$ = 1; }
 | /* empty */		{ $$ = 0; }
 ;

time_persision:
	'(' INT ')' 	{ $$ = strtol($2,&$2,10); }
 | /* empty */		{ $$ = 0; }
 ;

datetime_type:
    DATE			{ $$ = "DATE"; }
 |  TIME time_persision tz 	{ $$ = "TIME"; }
 |  TIMESTAMP time_persision tz { $$ = "TIMESTAMP"; } 
 ;

non_second_datetime_field:
    YEAR		{ $$ = iyear; }
 |  MONTH		{ $$ = imonth; }
 |  DAY			{ $$ = iday; }
 |  HOUR		{ $$ = ihour; }
 |  MINUTE		{ $$ = imin; }
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
    STRING   { $$ = _symbol_create_atom( SQL_ATOM, atom_string($1) );}
 |  INT      { $$ = _symbol_create_atom( SQL_ATOM, atom_int(strtol($1,&$1,10)));}
 |  INTNUM   { $$ = _symbol_create_atom( SQL_ATOM, atom_float(strtod($1,&$1)));}
 |  APPROXNUM{ $$ = _symbol_create_atom( SQL_ATOM, atom_float(strtod($1,&$1)));}
 |  DATE STRING { $$ = _symbol_create_atom( SQL_ATOM, atom_date($2)); }
 |  TIME STRING { $$ = _symbol_create_atom( SQL_ATOM, atom_time($2)); }
 |  TIMESTAMP STRING 
	{ $$ = _symbol_create_atom( SQL_ATOM, atom_timestamp($2)); }
 |  INTERVAL opt_sign STRING interval_qualifier
	{ context *lc = (context*)parm;
	  int i,type;
	  if ( (type = parse_interval( lc, $2, $3, $4, &i)) < 0 ){
		yyerror("incorrect interval");
		$$ = NULL;
	  } else {
		atom *a = NULL;
		if (type == 0){
	  		a = atom_month_interval(i);
		} else {
	  		a = atom_sec_interval(i);
		}
	  	$$ = _symbol_create_atom( SQL_ATOM, a);
	  }
	}
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

		/* data types, more types to come */

data_type:
    CHARACTER			{ $$ = "CHARACTER"; }
 |  CHARACTER '(' INT ')'	{ $$ = "CHARACTER"; }
 |  NUMERIC			{ $$ = "NUMERIC"; }
 |  NUMERIC '(' INT ')'		{ $$ = "NUMERIC"; }
 |  NUMERIC '(' INT ',' INT ')' { $$ = "NUMERIC"; }
 |  DECIMAL			{ $$ = "DECIMAL"; }
 |  DECIMAL '(' INT ')'		{ $$ = "DECIMAL"; }
 |  DECIMAL '(' INT ',' INT ')' { $$ = "DECIMAL"; }
 |  INTEGER '(' INT ')'		{ $$ = "INTEGER"; }
 |  INTEGER			{ $$ = "INTEGER"; }
 |  SMALLINT			{ $$ = "SMALLINT"; }
 |  SMALLINT '(' INT ')'	{ $$ = "SMALLINT"; }
 |  FLOAT			{ $$ = "FLOAT"; }
 |  FLOAT '(' INT ')'		{ $$ = "FLOAT"; }
 |  FLOAT '(' INT ',' INT ')'	{ $$ = "FLOAT"; }
 |  REAL			{ $$ = "REAL"; }
 |  DOUBLE 			{ $$ = "DOUBLE"; }
 |  DOUBLE '(' INT ',' INT ')' 	{ $$ = "DOUBLE"; }
 |  DOUBLE PRECISION		{ $$ = "DOUBLE"; }
 |  VARCHAR			{ $$ = "VARCHAR"; }
 |  VARCHAR '(' INT ')'		{ $$ = "VARCHAR"; }
 | datetime_type
 | interval_type
 ;

	/* the various things you can name */
ident: name	
 |  non_reserved_word	{ $$ = _strdup($1); }
 ;

column:	ident ;
/* :name handled in parser */
parameter: PARAMETER ;
range_variable:	name ;
user: name 	;
name: NAME 	{ $$ = $1; } 		;


non_reserved_word: CHARACTER | NUMERIC | DECIMAL | INTEGER | SMALLINT	
 | FLOAT | REAL | DOUBLE | PRECISION | VARCHAR | DATE | TIME | TIMESTAMP;

name_commalist:
    ident			{ $$ = dlist_append_string(dlist_create(), $1); }
 |  name_commalist ',' ident  	{ $$ = dlist_append_string($$, $3); } 
 ;

	/* embedded condition things */
/* sql: WHENEVER NOT FOUND when_action |	WHENEVER SQLERROR when_action ;
when_action:	GOTO NAME |	CONTINUE ; */
%%
