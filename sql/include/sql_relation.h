/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef SQL_RELATION_H
#define SQL_RELATION_H

#include "sql_catalog.h"

#define BASETABLE 0
#define RELATION 1

typedef enum expression_type {
	e_atom,
	e_column,
	e_cmp,
	e_func,
	e_aggr,
	e_convert,
	e_psm
} expression_type;

#define CARD_ATOM 1
#define CARD_AGGR 2
#define CARD_MULTI 3

typedef struct expression {
	expression_type  type;	/* atom, cmp, func/aggr */
	const char *name;
	const char *rname;
	void *l;
	void *r;
	void *f; 	/* func's and aggr's */
			/* e_cmp may have have 2 arguments */
	int  flag;	/* EXP_DISTINCT, NO_NIL, ASCENDING, cmp types */
	unsigned char card;	/* card 
				   (0 truth value!)
				   (1 atoms) 
				   (2 aggr)
				   (3 multi value)
			*/
	sql_subtype 	tpe;
	int used;	/* used for quick dead code removal */
	void *p;	/* properties for the optimizer */
} sql_exp;

#define EXP_DISTINCT	1
#define NO_NIL		2
#define TOPN_INCLUDING	4
#define ZERO_IF_EMPTY	8

#define LEFT_JOIN	4

#define APPLY_JOIN 	8
#define APPLY_LOJ 	16
#define APPLY_EXISTS	32
#define APPLY_NOTEXISTS	64

/* ASCENDING > 15 else we have problems with cmp types */
#define ASCENDING	16
#define CMPMASK		(ASCENDING-1)
#define get_cmp(e) 	(e->flag&CMPMASK)
#define ANTISEL	32
#define HAS_NO_NIL	64
#define EXP_INTERN	128

#define UPD_COMP		1
#define UPD_LOCKED		2
#define UPD_NO_CONSTRAINT	4
 
#define REL_PARTITION	8

/* We need bit wise exclusive numbers as we merge the level also in the flag */
#define PSM_SET 1
#define PSM_VAR 2
#define PSM_RETURN 4
#define PSM_WHILE 8
#define PSM_IF 16
#define PSM_REL 32

#define SET_PSM_LEVEL(level)	(level<<8)
#define GET_PSM_LEVEL(level)	(level>>8)

/* todo make enum */
#define DDL_OUTPUT	               1
#define DDL_LIST	               2	
#define DDL_PSM		               3	

#define DDL_CREATE_SEQ             5
#define DDL_ALTER_SEQ              6
#define DDL_DROP_SEQ               7

#define DDL_RELEASE	               11
#define DDL_COMMIT	               12
#define DDL_ROLLBACK	           13
#define DDL_TRANS	               14

#define DDL_CREATE_SCHEMA          21
#define DDL_DROP_SCHEMA_IF_EXISTS  22
#define DDL_DROP_SCHEMA            23

#define DDL_CREATE_TABLE           24
#define DDL_DROP_TABLE_IF_EXISTS   25
#define DDL_DROP_TABLE 	           26
#define DDL_CREATE_VIEW            27
#define DDL_DROP_VIEW_IF_EXISTS    28
#define DDL_DROP_VIEW              29
#define DDL_DROP_CONSTRAINT        30
#define DDL_ALTER_TABLE            31

#define DDL_CREATE_TYPE            32 
#define DDL_DROP_TYPE              33 

#define DDL_DROP_INDEX             34

#define DDL_CREATE_FUNCTION        41 
#define DDL_DROP_FUNCTION          42 
#define DDL_CREATE_TRIGGER         43 
#define DDL_DROP_TRIGGER           44 

#define DDL_GRANT_ROLES            51
#define DDL_REVOKE_ROLES           52
#define DDL_GRANT 	               53
#define DDL_REVOKE 	               54
#define DDL_GRANT_FUNC 	           55
#define DDL_REVOKE_FUNC            56
#define DDL_CREATE_USER            57
#define DDL_DROP_USER 	           58
#define DDL_ALTER_USER 	           59
#define DDL_RENAME_USER            60
#define DDL_CREATE_ROLE            61
#define DDL_DROP_ROLE 	           62

#define DDL_ALTER_TABLE_ADD_TABLE  63
#define DDL_ALTER_TABLE_DEL_TABLE  64
#define DDL_ALTER_TABLE_SET_ACCESS 65

#define DDL_EMPTY 100

#define MAXOPS 21

typedef enum operator_type {
	op_basetable = 0,
	op_table,
	op_ddl,
	op_project, 		/* includes order by */
	op_select,	
	op_join,
	op_left,
	op_right,
	op_full,
	op_semi,
	op_anti,
	op_apply,
	op_union,
	op_inter,
	op_except,
	op_groupby,	
	op_topn,
	op_sample,
	op_insert, 	/* insert(l=table, r insert expressions) */ 
	op_update, 	/* update(l=table, r update expressions) */
	op_delete 	/* delete(l=table, r delete expression) */
} operator_type;

#define is_atom(et) \
	(et == e_atom)
/* a simple atom is a literal or on the query stack */
#define is_simple_atom(e) \
	(is_atom(e->flag) && !e->r && !e->f)
#define is_func(et) \
	(et == e_func)
#define is_map_op(et) \
	(et == e_func || et == e_convert)
#define is_column(et) \
	(et != e_cmp)
#define is_analytic(e) \
	(e->type == e_func && ((sql_subfunc*)e->f)->func->type == F_ANALYTIC)
#define is_base(op) \
	(op == op_basetable || op == op_table)
#define is_basetable(op) \
	(op == op_basetable)
#define is_ddl(op) \
	(op == op_ddl)
#define is_output(rel) \
	(rel->op == op_ddl && rel->flag == DDL_OUTPUT)
#define is_outerjoin(op) \
	(op == op_left || op == op_right || op == op_full)
#define is_left(op) \
	(op == op_left)
#define is_right(op) \
	(op == op_right)
#define is_full(op) \
	(op == op_full)
#define is_join(op) \
	(op == op_join || is_outerjoin(op))
#define is_semi(op) \
	(op == op_semi || op == op_anti)
#define is_joinop(op) \
	(is_join(op) || is_semi(op))
#define is_apply(op) \
	(op == op_apply)
#define is_select(op) \
	(op == op_select)
#define is_set(op) \
	(op == op_union || op == op_inter || op == op_except)
#define is_union(op) \
	(op == op_union)
#define is_inter(rel) \
	(op == op_inter)
#define is_except(rel) \
	(op == op_except)
#define is_simple_project(op) \
	(op == op_project)
#define is_project(op) \
	(op == op_project || op == op_groupby || is_set(op))
#define is_groupby(op) \
	(op == op_groupby)
#define is_sort(rel) \
	((rel->op == op_project && rel->r) || rel->op == op_topn)
#define is_topn(op) \
	(op == op_topn)
#define is_modify(op) \
	(op == op_insert || op == op_update || op == op_delete)
#define is_sample(op) \
	(op == op_sample)

/* NO NIL semantics of aggr operations */
#define need_no_nil(e) \
	((e->flag&NO_NIL)==NO_NIL)
#define set_no_nil(e) \
	e->flag |= NO_NIL

/* ZERO on empty sets, needed for sum (of counts)). */
#define zero_if_empty(e) \
	((e->flag&ZERO_IF_EMPTY)==ZERO_IF_EMPTY)
#define set_zero_if_empty(e) \
	e->flag |= ZERO_IF_EMPTY

/* does the expression (possibly) have nils */
#define has_nil(e) \
	((e->flag&HAS_NO_NIL) == 0)
#define set_has_no_nil(e) \
	e->flag |= HAS_NO_NIL
#define set_has_nil(e) \
	e->flag &= (~HAS_NO_NIL)

#define is_ascending(e) \
	((e->flag&ASCENDING)==ASCENDING)
#define set_direction(e, dir) \
	e->flag |= (dir?ASCENDING:0)

#define is_anti(e) \
	((e->flag&ANTISEL)==ANTISEL)
#define set_anti(e) \
	e->flag |= ANTISEL

/* used for expressions and relations */
#define need_distinct(e) \
	((e->flag&EXP_DISTINCT)==EXP_DISTINCT)
#define set_distinct(e) \
	e->flag |= EXP_DISTINCT
#define set_nodistinct(e) \
	e->flag &= (~EXP_DISTINCT)

/* used for expressions and relations */
#define is_intern(e) \
	(e->type != e_atom && (e->flag&EXP_INTERN)==EXP_INTERN)
#define set_intern(e) \
	e->flag |= EXP_INTERN

#define is_processed(rel) \
	(rel->processed)
#define set_processed(rel) \
	rel->processed = 1
#define reset_processed(rel) \
	rel->processed = 0
#define is_subquery(rel) \
	(rel->subquery)
#define set_subquery(rel) \
	rel->subquery = 1
#define reset_subquery(rel) \
	rel->subquery = 0

#define rel_is_ref(rel) 	(((sql_rel*)rel)->ref.refcnt > 1)

typedef struct relation {
	sql_ref ref;

	operator_type op;	
	void *l;
	void *r;
	list *exps; 
	int nrcols;	/* nr of cols */	
	unsigned int
	 flag:8,	/* EXP_DISTINCT */ 
	 card:4,	/* 0, 1 (row), 2 aggr, 3 */
	 processed:1, /* fully processed or still in the process of building */
	 subquery:1;	/* is this part a subquery, this is needed for proper name binding */
	void *p;	/* properties for the optimizer, distribution */
} sql_rel;

#endif /* SQL_RELATION_H */
