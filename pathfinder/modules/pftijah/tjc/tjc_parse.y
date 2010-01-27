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
* Copyright (C) 2006-2010 "University of Twente".
*
* Portions created by the "CWI" are
* Copyright (C) 2008-2010 "CWI".
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

#include <stdlib.h>

#include <gdk.h>

#include <stdio.h>
#include <string.h>
#include "tjc_abssyn.h"

/* tell bison which malloc/free to use (needed for bison 2.4.1 on Windows) */
#define YYMALLOC malloc
#define YYFREE free

extern int tjclex(void);

/* temporary string in memory */
static char *enttype;
static double weight;
static TJqkind_t kind; 

/* temporary node memory */
static TJpnode_t *c;

/* parse tree */
static TJptree_t *tjc_tree;

void tjcerror(char *err) /* 'yyerror()' called by yyparse in error */
{
  char* b = &tjc_c_GLOBAL->errBUFF[0];
  *b = '\0';
  sprintf(b,"%s",err);
}

%}

%union {
	double			dbl;
	char			*str;
	struct TJpnode_t	*pnode;
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
%token plus		"+"
%token minus		"-"
%token apos		"'"
%token quote		"\""

%type <str>
	TERM
	ENTITY
	TAG
	QueryTerm
	QueryEntity

%type <dbl>
	WEIGHT

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
	QueryClause
	QueryClause_p
	QueryClause_s
	PhraseQuery
	SimpleQuery

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
			  { tjc_tree->is_rel_path_exp = 1; $$ = tjcp_wire1 (tjc_tree, p_nexi, $1); }
			| RelativePath_ Path OptPathNoPred
			  { tjc_tree->is_rel_path_exp = 1; *($2->fixme) = $1; 
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
	     		  { $$ = tjcp_wire2 (tjc_tree, p_about, $3, $5); }
	   		| "(" OrClause ")" 
	     		  { $$ = $2; }
	     		;
QueryClause		: QueryClause_s
	     		  { $$ = $1; }
	     		| QueryClause_p
	     		  { $$ = $1; }
			;
QueryClause_s		: SimpleQuery
	     		  { c = tjcp_leaf (tjc_tree, p_nil); $$ = tjcp_wire2 (tjc_tree, p_and, $1, c); }
	       		| QueryClause_p SimpleQuery
			  { $$ = tjcp_wire2 (tjc_tree, p_and, $2, $1); }
			;
QueryClause_p		: PhraseQuery
	     		  { c = tjcp_leaf (tjc_tree, p_nil); $$ = tjcp_wire2 (tjc_tree, p_and, $1, c); }
	       		| QueryClause_s PhraseQuery
			  { $$ = tjcp_wire2 (tjc_tree, p_and, $2, $1); }
	       		| QueryClause_p PhraseQuery
			  { $$ = tjcp_wire2 (tjc_tree, p_and, $2, $1); }
			;
PhraseQuery		: quote SimpleQuery quote
	                  { $$ = $2; $$->sem.qnode->kind = q_phrase;}
			| apos SimpleQuery apos
			  { $$ = $2; $$->sem.qnode->kind = q_phrase; }
			;
SimpleQuery		: QueryTerm
       			  { $$ = tjcp_leaf (tjc_tree, p_query);
			    $$->sem.qnode = tjcq_firstterm($1, "!t", weight, kind); } 
			| QueryEntity
			  { $$ = tjcp_leaf (tjc_tree, p_query);
       			    $$->sem.qnode = tjcq_firstterm($1, enttype, weight, kind); } 
			| SimpleQuery QueryTerm
       			  { $1->sem.qnode = tjcq_addterm ($1->sem.qnode, $2, "!t", weight, kind);
			    $$ = $1; } 
			| SimpleQuery QueryEntity
       			  { $1->sem.qnode = tjcq_addterm ($1->sem.qnode, $2, enttype, weight, kind);
			    $$ = $1; } 
			;
QueryTerm		: TERM
       			  { $$ = $1; 
			    weight = 1.0;
			    kind = q_normal;} 
			| TERM WEIGHT
       			  { $$ = $1; 
			    weight = $2;
			    kind = q_normal;}
			| plus TERM
       			  { $$ = $2; 
			    weight = 1.0;
			    kind = q_mandatory;}
			| plus TERM WEIGHT
       			  { $$ = $2; 
			    weight = $3;
			    kind = q_mandatory;}
			| minus TERM
       			  { $$ = $2; 
			    weight = 1.0;
			    kind = q_negated;}
			;
QueryEntity		: ENTITY
       			  { enttype = strtok ($1, ":");
			    $$ = strtok (NULL, ":");
			    weight = 1.0;
			    kind = q_normal;} 
       			| ENTITY WEIGHT
       			  { enttype = strtok ($1, ":");
			    $$ = strtok (NULL, ":");
			    weight = $2;
			    kind = q_normal;}
       			| plus ENTITY
       			  { enttype = strtok ($2, ":");
			    $$ = strtok (NULL, ":");
			    weight = 1.0;
			    kind = q_mandatory;}
       			| plus ENTITY WEIGHT
       			  { enttype = strtok ($2, ":");
			    $$ = strtok (NULL, ":");
			    weight = $3;
			    kind = q_mandatory;}
       			| minus ENTITY
       			  { enttype = strtok ($2, ":");
			    $$ = strtok (NULL, ":");
			    weight = 1.0;
			    kind = q_negated;}
			;    

%%

int tjc_parser (char* input, TJptree_t **res)
{
  int result = 0;
  setTJCscanstring(input);

  tjc_tree   = tjcp_inittree();
  if (tjcparse()) {
     result = 1; // parsing failed
  }
  destroyTJCscanBuffer();

  *res = tjc_tree;
  tjc_tree = NULL;

  return result;
}
