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
	e_exp,
	e_atom,
	e_relation,
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
	void *l;
	void *r;
	void *f; 	/* =,!=, but also func's and aggr's and column type */
	int  flag;
	char card;	/* card 
				(0 truth value!)
				(1 atoms) 
				(2 aggr)
				(3 multi value)
			*/
} sql_exp;

#define DISTINCT	1
#define NO_NIL		2
 
#define MAXOPS 16

typedef enum operator_type {
	op_basetable = 0,
	op_table,
	op_project,
	op_select,
	op_join,
	op_left,
	op_right,
	op_full,
	op_union,
	op_inter,
	op_except,
	op_groupby,	/* currently includes the projection (aggr) */
	op_orderby,
	op_topn
} operator_type;

#define is_join(op) \
	(op == op_join || op == op_left || op == op_right || op == op_full)
#define is_select(op) \
	(op == op_select)
#define is_set(op) \
	(op == op_union || op == op_inter || op == op_except)
#define is_project(op) \
	(op == op_project || op == op_groupby)
#define is_groupby(op) \
	(op == op_groupby)
#define is_sort(op) \
	(op == op_orderby || op == op_topn)
#define is_distinct(rel) \
	(rel->op == op_project && rel->r != NULL)

typedef struct relation {
	sql_ref ref;

	operator_type op;	
	char *name;   
	void *l;
	void *r;
	list *exps; 
	int nrcols;	/* nr of cols */	
	char card;	/* 0, 1 (row), 2 aggr, 3 */
	char processed; /* fully processed or still in the process of building */
	char subquery;	/* is this part a subquery, this is needed for proper name binding */
	void *p;	/* properties for the optimizer */
} sql_rel;

#endif /* SQL_RELATION_H */
