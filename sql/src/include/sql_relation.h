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
 * Portions created by CWI are Copyright (C) 1997-2005 CWI.
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
	e_unique,
	e_convert,
} expression_type;

typedef struct expression {
	char *name;
	void *l;
	void *r;
	expression_type  type;	/* atom, cmp, func/aggr */
	void *f; 	/* =,!=, but also func's and aggr's and column type */
	int  flag;
	char card;	/* card 
				(0 truth value!)
				(1 atoms (some aggr too))) 
				(2 ie unknown)
			*/
} sql_exp;
 
typedef enum operator_type {
	op_basetable,
	op_crossproduct,
	op_project,
	op_select,
	op_join,
	op_union,
	op_inter,
	op_diff,
	op_groupby,
	op_orderby,
	op_topn
} operator_type;

typedef struct relation {
	char *name;   
	void *l;
	void *r;
	operator_type op;	
	list *exps; 
	int nrcols;	/* nr of cols */	
	int card;	/* 0, 1 (row), 2 unkown */
} sql_rel;

#endif /* SQL_RELATION_H */
