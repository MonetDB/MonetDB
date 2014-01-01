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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

%{
#define _CRT_SECURE_NO_WARNINGS 1

#include <string.h>
#include <stdio.h>
#include <ctype.h>  /* isspace */
#include "jaqltree.h"
#ifdef _MSC_VER
#define snprintf _snprintf
#define jaql_import __declspec(dllimport)
#else
#define jaql_import extern
#endif
jaql_import char *GDKstrdup(const char *);
jaql_import void GDKfree(const char *);
%}

%define api.pure
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

%destructor { GDKfree($$); } <j_string>;
%destructor { GDKfree($$); } <j_json>;
%destructor { GDKfree($$); } <j_ident>;
%destructor { freetree($$); } <j_tree>;

%start stmt

%token EACH FILTER TRANSFORM EXPAND GROUP INTO BY AS JOIN WHERE IN
%token SORT TOP DESC ASC EXPLAIN PLAN PLANF DEBUG TRACE TIME UNROLL PRESERVE

%token ARROW ASSIGN EQUALS NEQUAL TRUE FALSE NIL
%token GREATER GEQUAL LESS LEQUAL NOT AND OR

%token <j_ident>  IDENT
%token <j_json>   ARRAY OBJECT

%token <j_number> NUMBER
%token <j_double> DOUBLE
%token <j_string> STRING

%type <j_tree> jaql jaqlpipe opt_actions actions action predicates
	predicate variable and_or opt_not comparison value literal
	opt_each opt_command sort_arg arith_op val_var_arith
	obj_list arr_list obj_pair json_value join_var_refs join_var_ref
	opt_join_in group_var_refs group_var_ref opt_group_by
	group_by opt_group_as func_call opt_func_args func_args func_arg
%type <j_ident> ident
%type <j_number> opt_asc_desc opt_preserve


%{
#define YYLEX_PARAM j->scanner

int jaqllex(YYSTYPE* lvalp, void *scanner);

void
jaqlerror(struct _jc* j, char const *msg)
{
	if (j->err[0] == '\0') {
		char around[32];
		char *p;
		size_t off = j->start + (j->tokstart - j->scanbuf);
		char hadend = 0;
		if (off < 13)
			off = 13;
		off -= 13;
		if (snprintf(around, sizeof(around), "%s", j->buf + off)
				<= (int)(sizeof(around)))
			hadend = 1;
		/* wrap at newline */
		for (p = around; *p != '\0'; p++)
			if (*p == '\n' || *p == '\r')
				*p = ' ';
		/* trim */
		for (--p; p > around && isspace(*p); p--)
			*p = '\0';
		for (p = around; *p != '\0' && isspace(*p); p++);
		snprintf(j->err, sizeof(j->err), "%s at or around '%s%s%s'",
				msg, off == 0 ? "" : "...", p, hadend == 0 ? "..." : "");
	}
}

%}

%%

stmt: jaql ';'
	{
		j->p = $1;
		YYACCEPT;
	}
	| EXPLAIN jaql ';'
	{
		j->p = $2;
		j->explain = 1;
		YYACCEPT;
	}
	| PLAN jaql ';'
	{
		j->p = $2;
		j->plan = 1;
		YYACCEPT;
	}
	| PLANF jaql ';'
	{
		j->p = $2;
		j->planf = 1;
		YYACCEPT;
	}
	| DEBUG jaql ';'
	{
		j->p = $2;
		j->debug = 1;
		YYACCEPT;
	}
	| TRACE INTO IDENT jaql ';'
	{
		j->p = make_json_output($3);
		j->p->next = $4;
		j->trace = 1;
		YYACCEPT;
	}
	| TRACE jaql ';'
	{
		j->p = make_json_output(NULL);
		j->p->next = $2;
		j->trace = 1;
		YYACCEPT;
	}
	| TIME jaql ';'
	{
		j->p = $2;
		j->time = 1;
		YYACCEPT;
	}
	| error ';'
	| ';'
	|
	{
		j->p = NULL;
		YYACCEPT;
	};

jaql: jaqlpipe              {$$ = append_jaql_pipe($1, make_json_output(NULL));}
	| IDENT ASSIGN jaqlpipe {$$ = append_jaql_pipe($3, make_json_output($1));}
	;

jaqlpipe: IDENT opt_actions     {$$ = append_jaql_pipe(make_varname($1), $2);}
		| func_call opt_actions {$$ = append_jaql_pipe($1, $2);}
		| '[' {j->expect_json = '[';}
		  ARRAY opt_actions     {$$ = append_jaql_pipe(make_json($3), $4);}
		| GROUP group_var_refs INTO json_value opt_actions
		    {$$ = append_jaql_pipe(make_jaql_group($2, $4, make_varname(GDKstrdup("$"))), $5);}
		| JOIN join_var_refs WHERE predicates INTO json_value opt_actions
		    {$$ = append_jaql_pipe(make_jaql_join($2, $4, $6), $7);}
		;

opt_actions: /* empty */        {$$ = NULL;}
		   | actions            {$$ = $1;}
		   ;

actions: ARROW action           {$$ = $2;}
	   | actions ARROW action   {$$ = append_jaql_pipe($1, $3);}
	   ;

action: FILTER opt_each predicates        {$$ = make_jaql_filter($2, $3);}
	  | TRANSFORM opt_each json_value     {$$ = make_jaql_transform($2, $3);}
	  | EXPAND opt_each opt_command       {$$ = make_jaql_expand($2, $3);}
	  | GROUP opt_each opt_group_by INTO json_value
	                                      {$$ = make_jaql_group($3, $5, $2);}
	  | SORT opt_each BY '[' sort_arg ']' {$$ = make_jaql_sort($2, $5);}
	  | TOP NUMBER                        {$$ = make_jaql_top($2);}
	  | TOP NUMBER opt_each BY '[' sort_arg ']'
	        {$$ = append_jaql_pipe(make_jaql_sort($3, $6), make_jaql_top($2));}
	  | func_call                         {$$ = set_func_input_from_pipe($1);}
	  ;

join_var_refs: join_var_refs ',' join_var_ref  {$$ = append_join_input($1, $3);}
			 | join_var_ref                    {$$ = $1;}
			 ;

join_var_ref: opt_preserve IDENT opt_join_in
			     {$$ = make_join_input($1 == PRESERVE, make_varname($2), $3);}
			;

group_var_refs: group_var_refs ',' group_var_ref
			                        {$$ = append_group_input($1, $3);}
			  | group_var_ref       {$$ = $1;}
			  ;

group_var_ref: IDENT group_by
			     {$$ = set_group_input_var($2, make_varname($1));}
			 ;

opt_group_by: /* empty */           {$$ = NULL;}
			| group_by              {$$ = $1;}
			;

group_by: BY IDENT ASSIGN variable opt_group_as
		                   {$$ = make_group_input($2, $4, $5);};

opt_group_as: /* empty */  {$$ = make_varname(GDKstrdup("$"));}
			| AS IDENT     {$$ = make_varname($2);}

opt_preserve: /* empty */           {$$ = 0;}
			| PRESERVE              {$$ = PRESERVE;}
			;

opt_join_in: /* empty */            {$$ = NULL;}
		   | IN IDENT               {$$ = make_varname($2);}
		   ;

opt_command: /* empty */            {$$ = NULL;}
		   | variable               {$$ = $1;}
		   | UNROLL variable        {$$ = make_unroll($2);}
		   | '(' ident actions ')'  {$$ = append_jaql_pipe(make_varname($2), $3);}
		   ;

opt_each: /* empty */  {$$ = make_varname(GDKstrdup("$"));}
		| EACH IDENT   {$$ = make_varname($2);}
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

ident: IDENT               {$$ = $1;}
	 | '$'                 {$$ = GDKstrdup("$");}
	 ;

func_call: IDENT '(' opt_func_args ')'  {$$ = make_func_call($1, $3);}
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
		  ARRAY                    {$$ = make_json($3);}
		| '{' {j->expect_json = '{';}
		  OBJECT                   {$$ = make_json($3);}
		;

predicates: opt_not predicate  {$$ = make_pred(NULL, $1, $2);}
		  | opt_not '(' predicates ')' 
                               {$$ = make_pred(NULL, $1, $3);}
		  | predicates and_or predicates
                               {$$ = make_pred($1, $2, $3);}
		  ;

predicate: opt_not variable
		       {$$ = make_pred(NULL, $1, make_pred($2, make_comp(j_equals), make_bool(1)));}
		 | opt_not val_var_arith comparison value
		       {$$ = make_pred(NULL, $1, make_pred($2, $3, $4));}
		 | opt_not json_value IN json_value
		       {$$ = make_pred(NULL, $1, make_pred($2, make_comp(j_in), $4));}
		 ;

variable: ident                     {$$ = make_varname($1);}
		| variable '.' IDENT        {$$ = append_varname($1, $3);}
		| variable '[' '*' ']'      {$$ = append_vararray($1, 0, 1);}
		| variable '[' NUMBER ']'   {$$ = append_vararray($1, $3, 0);}
		;

and_or: AND          {$$ = make_comp(j_and);}
	  | OR           {$$ = make_comp(j_or);}
	  ;

opt_not: /* empty */ {$$ = NULL;}
	   | NOT         {$$ = make_comp(j_not);}
	   ;

comparison: EQUALS   {$$ = make_comp(j_equals);}
		  | NEQUAL   {$$ = make_comp(j_nequal);}
		  | GREATER  {$$ = make_comp(j_greater);}
		  | GEQUAL   {$$ = make_comp(j_gequal);}
		  | LESS     {$$ = make_comp(j_less);}
		  | LEQUAL   {$$ = make_comp(j_lequal);}
		  ;

value: variable      {$$ = $1;}
	 | literal       {$$ = $1;}
	 ;

literal: NUMBER      {$$ = make_number($1);}
	   | DOUBLE      {$$ = make_double($1);}
	   | STRING      {$$ = make_string($1);}
	   | TRUE        {$$ = make_bool(1);}
	   | FALSE       {$$ = make_bool(0);}
	   | NIL         {$$ = make_null();}
	   ;

arith_op: '+'        {$$ = make_op(j_plus);}
		| '-'        {$$ = make_op(j_min);}
		| '*'        {$$ = make_op(j_multiply);}
		| '/'        {$$ = make_op(j_divide);}
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
		| variable '.' '*'            {$$ = make_pair(NULL, append_varname($1, NULL));}
		| STRING ':' json_value       {$$ = make_pair($1, $3);}
		;

json_value: val_var_arith             {$$ = $1;}
		  | '{' obj_list '}'          {$$ = make_json_object($2);}
		  | '[' arr_list ']'          {$$ = make_json_array($2);}
		  ;
