%{
/**
* Copyright Notice:
* -----------------
*
* The contents of this file are subject to the PfTijah Public License
* Version 1.1 (the "License"); you may not use this file except in
* compliance with the License. You may obtain a copy of the License at
* http://dbappl.cs.utwente.nl/Legal/PfTijah-1.1.html
*
* Software distributed under the License is distributed on an "AS IS"
* basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
* License for the specific language governing rights and limitations
* under the License.
*
* The Original Code is the PfTijah system.
*
* The Initial Developer of the Original Code is the "University of Twente".
* Portions created by the "University of Twente" are
* Copyright (C) 2006-2009 "University of Twente".
*
* Portions created by the "CWI" are
* Copyright (C) 2008-2009 "CWI".
*
* All Rights Reserved.
* 
* Author(s): Henning Rode 
*            Jan Flokstra
*            Vojkan Mihajlovic
*            Roel van Os
*
*/

/* #define YYDEBUG 1 */

#include <pf_config.h>

#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif

#include <gdk.h>

#include <stdio.h>
#include <string.h>
#include "tjc_abssyn.h"

extern int tjclex(void);

/* temporary string in memory */
static char *ent;

/* temporary node memory */
static TJpnode_t *c;

/* parse tree */
static TJptree_t *tjc_tree;

void tjcerror(char *err) /* 'yyerror()' called by yyparse in error */
{
  char* b = &tjc_c_GLOBAL->errBUFF[0];
  *b = '\0';
  sprintf(b,err);
}

%}

%union {
	int			num;
	double			dbl;
	char			*str;
	struct TJpnode_t	*pnode;
	struct TJqnode_t	*qnode;
	struct TJpfixme_t	*pfixme;
}

%token TERM 		
%token TAG
%token ENTITY
%token WEIGHT
%token star		"*"
%token dot		"."
%token comma		","
%token slash_slash	"//"
%token pipe_		"|"
%token lbracket		"["
%token rbracket		"]"
%token lparen		"("
%token rparen		")"
%token or		"or"
%token and		"and"
%token about		"about"

%type <str>
	TERM
	ENTITY
	TAG	

%type <dbl>
	WEIGHT

%type <qnode>
	QueryClause

%type <pnode>
	NexiQuery
	RelativePath_
	RelativePathNoPred
	NameTest
	TagnameSeq
	PredicateList
	Predicate
	OrClause 
	AndClause
	AboutClause

%type <pfixme>
	Path
	PathNoPred
	OptPathNoPred

%%/* Grammar rules and actions */

NexiQuery		: Path OptPathNoPred 
			  { *($1->fixme) = tjcp_leaf (tjc_tree, p_root); 
			    if ($2) { *($2->fixme) = $1->root; 
			    $$ = tjcp_wire1 (tjc_tree, p_nexi, $2->root); TJCfree($1); TJCfree($2); }
			    else { $$ = tjcp_wire1 (tjc_tree, p_nexi, $1->root); TJCfree($1); }
			  }  
	   		| RelativePath_
			  { $$ = tjcp_wire1 (tjc_tree, p_nexi, $1); }
			| RelativePath_ Path OptPathNoPred
			  { *($2->fixme) = $1; 
			    if ($3) { *($3->fixme) = $2->root; $$ = tjcp_wire1 (tjc_tree, p_nexi, $3->root); 
			    TJCfree($2); TJCfree($3); }
			    else { $$ = tjcp_wire1 (tjc_tree, p_nexi, $2->root); TJCfree($2); }
			  }  
			;
RelativePath_ 		: "."
	     		  { $$ = tjcp_leaf (tjc_tree, p_ctx); }
			| "." PredicateList 
	     		  { c = tjcp_leaf (tjc_tree, p_ctx); $$ = tjcp_wire2 (tjc_tree, p_pred, c, $2); }
			;
Path			: PathNoPred PredicateList
       			  { $$ = $1; $$->root = tjcp_wire2 (tjc_tree, p_pred, $1->root, $2); }
	      		| Path PathNoPred PredicateList
       			  { $$ = $1; *($2->fixme) = $1->root;
			    $$->root = tjcp_wire2 (tjc_tree, p_pred, $2->root, $3); TJCfree($2); }
			;
OptPathNoPred		: /* empty */ 
	       		  { $$ = NULL; }
	       		| PathNoPred
			  { $$ = $1; }
			;
PathNoPred		: "//" NameTest
	     		  { c = tjcp_wire2 (tjc_tree, p_desc, NULL, $2); $$ = tjcp_fixme (c, &(c->child[0])); }
	    		| PathNoPred "//" NameTest
	     		  { $$ = $1; $$->root = tjcp_wire2 (tjc_tree, p_desc, $1->root, $3); }
			;
RelativePathNoPred	: "."
	     		  { $$ = tjcp_leaf (tjc_tree, p_ctx); }
		   	| "." PathNoPred
	     		  { *($2->fixme) = tjcp_leaf (tjc_tree, p_ctx); $$ = $2->root; TJCfree($2); }
			;
NameTest		: "*"
       			  { ($$ = tjcp_leaf (tjc_tree, p_tag))->sem.str = "*"; } 
	  		| TAG
       			  { ($$ = tjcp_leaf (tjc_tree, p_tag))->sem.str = $1; } 
			| "(" TagnameSeq ")"
	     		  { $$ = $2; }
			;
TagnameSeq		: TAG
       			  { ($$ = tjcp_leaf (tjc_tree, p_tag))->sem.str = $1; } 
	    		| TagnameSeq "|" TAG
       			  { $$ = tjcp_wire2 (tjc_tree, p_union, $1, (c = tjcp_leaf (tjc_tree, p_tag), c->sem.str = $3, c)); } 
	    		;
PredicateList		: Predicate
	     		  { $$ = $1; }
	       		| PredicateList Predicate
	     		  { $$ = tjcp_wire2 (tjc_tree, p_and, $1, $2); }
			;
Predicate		: "[" OrClause "]"
	     		  { $$ = $2; }
	   		;
OrClause		: AndClause
	     		  { $$ = $1; }
	     		| OrClause "or" AndClause
	     		  { $$ = tjcp_wire2 (tjc_tree, p_or, $1, $3); }
			;
AndClause		: AboutClause
	     		  { $$ = $1; }
	   		| AndClause "and" AboutClause
	     		  { $$ = tjcp_wire2 (tjc_tree, p_and, $1, $3); }
			;
AboutClause		: "about" "(" RelativePathNoPred "," QueryClause ")"
	     		  { c = tjcp_leaf (tjc_tree, p_query); c->sem.qnode = $5;
			  $$ = tjcp_wire2 (tjc_tree, p_about, $3, c); }
	   		| "(" OrClause ")" 
	     		  { $$ = $2; }
	     		;
QueryClause		: TERM
       			  { $$ = tjcq_firstterm ($1, "!t", 1.0); } 
			| TERM WEIGHT
       			  { $$ = tjcq_firstterm ($1, "!t", $2); } 
			| ENTITY
       			  { ent = strtok ($1, ":");
			    $$ = tjcq_firstterm (strtok (NULL, ":"), ent, 1.0); } 
       			| ENTITY WEIGHT
       			  { ent = strtok ($1, ":");
			    $$ = tjcq_firstterm (strtok (NULL, ":"), ent, $2); } 
			| QueryClause TERM
       			  { $$ = tjcq_addterm ($1, $2, "!t", 1.0); } 
			| QueryClause TERM WEIGHT
       			  { $$ = tjcq_addterm ($1, $2, "!t", $3); } 
			| QueryClause ENTITY
       			  { ent = strtok ($2, ":");
			    $$ = tjcq_addterm ($1, strtok (NULL, ":"), ent, 1.0); } 
       			| QueryClause ENTITY WEIGHT
       			  { ent = strtok ($2, ":");
			    $$ = tjcq_addterm ($1, strtok (NULL, ":"), ent, $3); } 
			;

%%

int tjc_parser (char* input, TJptree_t **res)
{
  setTJCscanstring(input);

  tjc_tree   = tjcp_inittree();
  int result = tjcparse();
  *res = tjc_tree;
  tjc_tree = NULL;

  return result;
}
