/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2008 CWI.
 * All Rights Reserved.
 */

#ifndef SQL_RELATION_H
#define SQL_RELATION_H

#include <sql_catalog.h>

#define BASETABLE 0
#define RELATION 1

typedef enum expression_type {
	e_atom,
	e_column,
	e_cmp,
	e_func,
	e_aggr,
	e_convert
} expression_type;

#define CARD_ATOM 1
#define CARD_AGGR 2
#define CARD_MULTI 3

typedef struct expression {
	sql_ref ref;

	expression_type  type;	/* atom, cmp, func/aggr */
	char *name;
	char *rname;
	void *l;
	void *r;
	void *f; 	/* =,!=, but also func's and aggr's and column type */
	int  flag;	/* DISTINCT, NO_NIL, ASCENDING */
	char card;	/* card 
				(0 truth value!)
				(1 atoms) 
				(2 aggr)
				(3 multi value)
			*/
	void *p;	/* properties for the optimizer */
} sql_exp;

#define DISTINCT	1
#define NO_NIL		2
/* ASCENDING > 8 else we have problems with cmp types */
#define ASCENDING	16
 
#define MAXOPS 16

typedef enum operator_type {
	op_basetable = 0,
	op_table,
	op_project,
	op_select,	/* includes order by */
	op_join,
	op_left,
	op_right,
	op_full,
	op_semi,	
	op_anti,
	op_union,
	op_inter,
	op_except,
	op_groupby,	/* currently includes the projection (aggr) */
	op_topn
} operator_type;

#define is_column(et) \
	(et != e_cmp)
#define is_base(op) \
	(op == op_basetable || op == op_table)
#define is_join(op) \
	(op == op_join || op == op_left || op == op_right || op == op_full || op == op_semi || op == op_anti)
#define is_select(op) \
	(op == op_select)
#define is_set(op) \
	(op == op_union || op == op_inter || op == op_except)
#define is_project(op) \
	(op == op_project || op == op_groupby || is_set(op))
#define is_groupby(op) \
	(op == op_groupby)
#define is_sort(rel) \
	((rel->op == op_project && rel->r) || rel->op == op_topn)

#define is_no_nil(e) \
	((e->flag&NO_NIL))
#define set_no_nil(e) \
	e->flag |= NO_NIL

#define is_ascending(e) \
	((e->flag&ASCENDING))
#define set_direction(e, dir) \
	e->flag |= (dir?ASCENDING:0)

/* used for expressions and relations */
#define is_distinct(e) \
	((e->flag&DISTINCT))
#define set_distinct(e) \
	e->flag |= DISTINCT

#define is_processed(rel) \
	(rel->processed)
#define set_processed(rel) \
	rel->processed = 1
#define is_subquery(rel) \
	(rel->subquery)
#define set_subquery(rel) \
	rel->subquery = 1

typedef struct relation {
	sql_ref ref;

	operator_type op;	
	char *name;   
	void *l;
	void *r;
	list *exps; 
	int nrcols;	/* nr of cols */	
	char flag;	/* DISTINCT */ 
	char card;	/* 0, 1 (row), 2 aggr, 3 */
	char processed; /* fully processed or still in the process of building */
	char subquery;	/* is this part a subquery, this is needed for proper name binding */
	void *p;	/* properties for the optimizer */
} sql_rel;

#endif /* SQL_RELATION_H */
