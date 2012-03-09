%define api.pure
%locations
%defines
%error-verbose

%parse-param { struct _jc *j }
%lex-param { void *scanner }

%union {
	long long int  j_number;
	double         j_double;
	char          *j_string;
	char          *j_json;
	char          *j_ident;
	void          *j_tree;
}


%start stmt

%token LEX_ERROR

%token EACH FILTER TRANSFORM EXPAND GROUP INTO BY AS JOIN WHERE IN
%token SORT TOP DESC ASC EXPLAIN PLAN PLANF UNROLL PRESERVE

%token _ARROW _DOLLAR _ASSIGN _EQUALS _NEQUAL _TRUE _FALSE
%token _GREATER _GEQUAL _LESS _LEQUAL _NOT _AND _OR _SCOLON _DOT

%token <j_ident>  _IDENT
%token <j_json>   _ARRAY _OBJECT

%token <j_number> _NUMBER
%token <j_double> _DOUBLE
%token <j_string> _STRING

%type <j_tree> stmt jaql jaqlpipe opt_actions actions action predicates
	predicate variable and_or opt_not comparison value literal
	opt_each opt_command sort_arg arith_op val_var_arith
	obj_list arr_list obj_pair json_value join_var_refs join_var_ref
	opt_join_in group_var_refs group_var_ref opt_group_by
	group_by opt_group_as func_call opt_func_args func_args func_arg
%type <j_ident> ident
%type <j_number> opt_asc_desc opt_preserve

/* get it right:
http://www.cs.man.ac.uk/~pjj/cs211/ho/node8.html
http://www.phpcompiler.org/articles/reentrantparser.html
http://www.usualcoding.eu/post/2007/09/03/Building-a-reentrant-parser-in-C-with-Flex/Bison
*/

%{
#include <string.h>

#include "jaql.h"

#define YYLEX_PARAM j->scanner

int yylex(YYSTYPE* lvalp, YYLTYPE* llocp, void *scanner);

void
yyerror(YYLTYPE* locp, struct _jc* j, char const *msg)
{
	if (j->err[0] == '\0')
		snprintf(j->err, sizeof(j->err), "%s at or around %d",
			msg, locp->first_line);
}

%}

%%

stmt: jaql _SCOLON
	{
		j->p = $$ = $1;
		YYACCEPT;
	}
	| EXPLAIN jaql _SCOLON
	{
		j->p = $$ = $2;
		j->explain = 1;
		YYACCEPT;
	}
	| PLAN jaql _SCOLON
	{
		j->p = $$ = $2;
		j->explain = 2;
		YYACCEPT;
	}
	| PLANF jaql _SCOLON
	{
		j->p = $$ = $2;
		j->explain = 3;
		YYACCEPT;
	}
	| LEX_ERROR
	{
		j->p = $$ = NULL;
		YYABORT;
	}
	| _SCOLON
	{
		j->p = $$ = NULL;
		YYACCEPT;
	}
	|
	{
		j->p = $$ = NULL;
		YYACCEPT;
	};

jaql: jaqlpipe                  {$$ = append_jaql_pipe($1, make_json_output(NULL));}
	| _IDENT _ASSIGN jaqlpipe   {$$ = append_jaql_pipe($3, make_json_output($1));}
	;

jaqlpipe: _IDENT opt_actions    {$$ = append_jaql_pipe(make_varname($1), $2);}
		| func_call opt_actions {$$ = append_jaql_pipe($1, $2);}
		| '[' {j->expect_json = '[';}
		  _ARRAY opt_actions    {$$ = append_jaql_pipe(make_json($3), $4);}
		| GROUP group_var_refs INTO json_value opt_actions
		    {$$ = append_jaql_pipe(make_jaql_group($2, $4, make_varname(GDKstrdup("$"))), $5);}
		| JOIN join_var_refs WHERE predicates INTO json_value opt_actions
		    {$$ = append_jaql_pipe(make_jaql_join($2, $4, $6), $7);}
		;

opt_actions: /* empty */        {$$ = NULL;}
		   | actions            {$$ = $1;}
		   ;

actions: _ARROW action          {$$ = $2;}
	   | actions _ARROW action  {$$ = append_jaql_pipe($1, $3);}
	   ;

action: FILTER opt_each predicates        {$$ = make_jaql_filter($2, $3);}
	  | TRANSFORM opt_each json_value     {$$ = make_jaql_transform($2, $3);}
	  | EXPAND opt_each opt_command       {$$ = make_jaql_expand($2, $3);}
	  | GROUP opt_each opt_group_by INTO json_value
	                                      {$$ = make_jaql_group($3, $5, $2);}
	  | SORT opt_each BY '[' sort_arg ']' {$$ = make_jaql_sort($2, $5);}
	  | TOP _NUMBER                       {$$ = make_jaql_top($2);}
	  | TOP _NUMBER opt_each BY '[' sort_arg ']'
	        {$$ = append_jaql_pipe(make_jaql_sort($3, $6), make_jaql_top($2));}
	  | func_call                         {$$ = set_func_input_from_pipe($1);}
	  ;

join_var_refs: join_var_refs ',' join_var_ref  {$$ = append_join_input($1, $3);}
			 | join_var_ref                    {$$ = $1;}
			 ;

join_var_ref: opt_preserve _IDENT opt_join_in
			     {$$ = make_join_input($1 == PRESERVE, make_varname($2), $3);}
			;

group_var_refs: group_var_refs ',' group_var_ref
			                        {$$ = append_group_input($1, $3);}
			  | group_var_ref       {$$ = $1;}
			  ;

group_var_ref: _IDENT group_by
			     {$$ = set_group_input_var($2, make_varname($1));}
			 ;

opt_group_by: /* empty */           {$$ = NULL;}
			| group_by              {$$ = $1;}
			;

group_by: BY _IDENT _ASSIGN variable opt_group_as
		                   {$$ = make_group_input($2, $4, $5);};

opt_group_as: /* empty */  {$$ = make_varname(GDKstrdup("$"));}
			| AS _IDENT    {$$ = make_varname($2);}

opt_preserve: /* empty */           {$$ = 0;}
			| PRESERVE              {$$ = PRESERVE;}
			;

opt_join_in: /* empty */            {$$ = NULL;}
		   | IN _IDENT              {$$ = make_varname($2);}
		   ;

opt_command: /* empty */            {$$ = NULL;}
		   | variable               {$$ = $1;}
		   | UNROLL variable        {$$ = make_unroll($2);}
		   | '(' ident actions ')'  {$$ = append_jaql_pipe(make_varname($2), $3);}
		   ;

opt_each: /* empty */  {$$ = make_varname(GDKstrdup("$"));}
		| EACH _IDENT  {$$ = make_varname($2);}
		;

sort_arg: variable opt_asc_desc
		                   {$$ = make_sort_arg($1, $2 == ASC);}
		| sort_arg ',' variable opt_asc_desc
		                   {$$ = append_sort_arg($1, make_sort_arg($3, $4));}
		;

opt_asc_desc: /* empty */  {$$ = ASC;}
			| ASC          {$$ = ASC;}
			| DESC         {$$ = DESC;}
			;

ident: _IDENT   {$$ = $1;}
	 | _DOLLAR  {$$ = GDKstrdup("$");}
	 ;

func_call: _IDENT '(' opt_func_args ')'  {$$ = make_func_call($1, $3);}
		 ;

opt_func_args: /* empty */  {$$ = NULL;}
			 | func_args    {$$ = $1;}
			 ;

func_args: func_arg                {$$ = make_func_arg($1);}
		 | func_args ',' func_arg  {$$ = append_func_arg($1, make_func_arg($3));}
		 ;

func_arg: val_var_arith            {$$ = $1;}
		| val_var_arith actions    {$$ = append_jaql_pipe($1, $2);}
		| '[' {j->expect_json = '[';}
		  _ARRAY                   {$$ = make_json($3);}
		| '{' {j->expect_json = '{';}
		  _OBJECT                  {$$ = make_json($3);}
		;

predicates: opt_not predicate  {$$ = make_pred(NULL, $1, $2);}
		  | opt_not '(' predicates ')' 
                               {$$ = make_pred(NULL, $1, $3);}
		  | predicates and_or predicates
                               {$$ = make_pred($1, $2, $3);}
		  ;

predicate: opt_not variable
		       {$$ = make_pred(NULL, $1, make_pred($2, make_comp(j_equals), make_bool(1)));}
		 | opt_not variable comparison value
		       {$$ = make_pred(NULL, $1, make_pred($2, $3, $4));}
		 | opt_not val_var_arith comparison value
		       {$$ = make_pred(NULL, $1, make_pred($2, $3, $4));}
		 ;

variable: ident                     {$$ = make_varname($1);}
		| variable _DOT _IDENT      {$$ = append_varname($1, $3);}
		| variable '[' '*' ']'      {$$ = append_vararray($1, 0, 1);}
		| variable '[' _NUMBER ']'  {$$ = append_vararray($1, $3, 0);}
		;

and_or: _AND  {$$ = make_comp(j_and);}
	  | _OR   {$$ = make_comp(j_or);}
	  ;

opt_not: /* empty */ {$$ = NULL;}
	   | _NOT        {$$ = make_comp(j_not);}
	   ;

comparison: _EQUALS   {$$ = make_comp(j_equals);}
		  | _NEQUAL   {$$ = make_comp(j_nequal);}
		  | _GREATER  {$$ = make_comp(j_greater);}
		  | _GEQUAL   {$$ = make_comp(j_gequal);}
		  | _LESS     {$$ = make_comp(j_less);}
		  | _LEQUAL   {$$ = make_comp(j_lequal);}
		  ;

value: variable  {$$ = $1;}
	 | literal   {$$ = $1;}
	 ;

literal: _NUMBER  {$$ = make_number($1);}
	   | _DOUBLE  {$$ = make_double($1);}
	   | _STRING  {$$ = make_string($1);}
	   | _TRUE    {$$ = make_bool(1);}
	   | _FALSE   {$$ = make_bool(0);}
	   ;

arith_op: '+'     {$$ = make_op(j_plus);}
		| '-'     {$$ = make_op(j_min);}
		| '*'     {$$ = make_op(j_multiply);}
		| '/'     {$$ = make_op(j_divide);}
		;

val_var_arith: val_var_arith arith_op val_var_arith
			                                 {$$ = make_operation($1, $2, $3);}
			 | '(' val_var_arith ')'         {$$ = $2;}
			 | value                         {$$ = $1;}
			 | func_call                     {$$ = $1;}
			 ;

obj_list: obj_list ',' obj_pair       {$$ = append_pair($1, $3);}
		| obj_pair                    {$$ = $1;}
		;

arr_list: arr_list ',' val_var_arith  {$$ = append_elem($1, $3);}
		| val_var_arith               {$$ = $1;}
		;

obj_pair: variable                    {$$ = make_pair(NULL, $1);}
		| variable _DOT '*'           {$$ = make_pair(NULL, append_varname($1, NULL));}
		| _STRING ':' val_var_arith   {$$ = make_pair($1, $3);}
		;

json_value: val_var_arith             {$$ = $1;}
		  | '{' obj_list '}'          {$$ = make_json_object($2);}
		  | '[' arr_list ']'          {$$ = make_json_array($2);}
		  ;
