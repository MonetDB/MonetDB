/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef SQL_RELATION_H
#define SQL_RELATION_H

#include "sql_catalog.h"

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

typedef struct sql_exp_name {
	unsigned int label;
	const char *name;
	const char *rname;
} sql_exp_name;

typedef struct sql_var_name {
	const char *name;
	const char *sname;
} sql_var_name;

typedef struct expression {
	expression_type type;	/* atom, cmp, func/aggr */
	sql_exp_name alias;
	void *l;
	void *r;
	void *f;	/* func's and aggr's, also e_cmp may have have 2 arguments */
	unsigned int flag; /* cmp types, PSM types/level */
	unsigned int
	 card:2,	/* card (0 truth value!) (1 atoms) (2 aggr) (3 multi value) */
	 freevar:4,	/* free variable, ie binds to the upper dependent join */
	 intern:1,
	 anti:1,
	 ascending:1,	/* order direction */
	 nulls_last:1,	/* return null after all other rows */
	 zero_if_empty:1, 	/* in case of partial aggregator computation, some aggregators need to return 0 instead of NULL */
	 distinct:1,

	 semantics:1,	/* is vs = semantics (nil = nil vs unknown != unknown), ranges with or without nil, aggregation with or without nil */
	 need_no_nil:1,
	 has_no_nil:1,

	 base:1,
	 ref:1,		/* used to indicate an other expression may reference this one */
	 used:1;	/* used for quick dead code removal */
	sql_subtype	tpe;
	void *p;	/* properties for the optimizer */
} sql_exp;

#define TABLE_PROD_FUNC		1
#define TABLE_FROM_RELATION	2
#define TRIGGER_WRAPPER		4

#define IS_TABLE_PROD_FUNC(X)  ((X & TABLE_PROD_FUNC) == TABLE_PROD_FUNC)

/* or-ed with the above TABLE_PROD_FUNC */
#define UPD_COMP		2
#define UPD_NO_CONSTRAINT	4

#define LEFT_JOIN		4
#define REL_PARTITION		8

/* We need bit wise exclusive numbers as we merge the level also in the flag */
#define PSM_SET 1
#define PSM_VAR 2
#define PSM_RETURN 4
#define PSM_WHILE 8
#define PSM_IF 16
#define PSM_REL 32
#define PSM_EXCEPTION 64

#define SET_PSM_LEVEL(level)	(level<<8)
#define GET_PSM_LEVEL(level)	(level>>8)

typedef enum ddl_statement {
	ddl_output,
	ddl_list,
	ddl_psm,
	ddl_exception,
	ddl_create_seq,
	ddl_alter_seq,
	ddl_drop_seq,
	ddl_alter_table_add_range_partition,
	ddl_alter_table_add_list_partition,
	ddl_release,
	ddl_commit,
	ddl_rollback,
	ddl_trans,
	ddl_create_schema,
	ddl_drop_schema,
	ddl_create_table,
	ddl_drop_table,
	ddl_create_view,
	ddl_drop_view,
	ddl_drop_constraint,
	ddl_alter_table,
	ddl_create_type,
	ddl_drop_type,
	ddl_drop_index,
	ddl_create_function,
	ddl_drop_function,
	ddl_create_trigger,
	ddl_drop_trigger,
	ddl_grant_roles,
	ddl_revoke_roles,
	ddl_grant,
	ddl_revoke,
	ddl_grant_func,
	ddl_revoke_func,
	ddl_create_user,
	ddl_drop_user,
	ddl_alter_user,
	ddl_rename_user,
	ddl_create_role,
	ddl_drop_role,
	ddl_alter_table_add_table,
	ddl_alter_table_del_table,
	ddl_alter_table_set_access,
	ddl_comment_on,
	ddl_rename_schema,
	ddl_rename_table,
	ddl_rename_column,
	ddl_maxops /* evaluated to the max value, should be always kept at the bottom */
} ddl_statement;

typedef enum operator_type {
	op_basetable = 0,
	op_table,
	op_ddl,
	op_project,		/* includes order by */
	op_select,
	op_join,
	op_left,
	op_right,
	op_full,
	op_semi,
	op_anti,
	op_union,
	op_inter,
	op_except,
	op_groupby,
	op_topn,
	op_sample,
	op_insert,	/* insert(l=table, r insert expressions) */
	op_update,	/* update(l=table, r update expressions) */
	op_delete,	/* delete(l=table, r delete expression) */
	op_truncate /* truncate(l=table) */
} operator_type;

#define is_atom(et) 		(et == e_atom)
/* a simple atom is a literal or on the query stack */
#define is_simple_atom(e) 	(is_atom((e)->type) && !(e)->r && !(e)->f)
#define is_values(e) 		((e)->type == e_atom && (e)->f)
#define is_func(et) 		(et == e_func)
#define is_aggr(et) 		(et == e_aggr)
#define is_convert(et) 		(et == e_convert)
#define is_map_op(et) 		(et == e_func || et == e_convert)
#define is_compare(et) 		(et == e_cmp)
#define is_column(et)		(et != e_cmp)
#define is_alias(et) 		(et == e_column)
#define is_analytic(e) 		((e)->type == e_func && ((sql_subfunc*)(e)->f)->func->type == F_ANALYTIC)

#define is_base(op)  		(op == op_basetable || op == op_table)
#define is_basetable(op) 	(op == op_basetable)
#define is_ddl(op)	 		(op == op_ddl)
#define is_outerjoin(op) 	(op == op_left || op == op_right || op == op_full)
#define is_left(op) 		(op == op_left)
#define is_right(op) 		(op == op_right)
#define is_full(op) 		(op == op_full)
#define is_innerjoin(op)	(op == op_join)
#define is_join(op) 		(op == op_join || is_outerjoin(op))
#define is_semi(op) 		(op == op_semi || op == op_anti)
#define is_joinop(op) 		(is_join(op) || is_semi(op))
#define is_select(op) 		(op == op_select)
#define is_set(op) 			(op == op_union || op == op_inter || op == op_except)
#define is_union(op) 		(op == op_union)
#define is_inter(op) 		(op == op_inter)
#define is_except(op) 		(op == op_except)
#define is_simple_project(op) 	(op == op_project)
#define is_project(op) 		(op == op_project || op == op_groupby || is_set(op))
#define is_groupby(op) 		(op == op_groupby)
#define is_topn(op) 		(op == op_topn)
#define is_modify(op) 	 	(op == op_insert || op == op_update || op == op_delete || op == op_truncate)
#define is_sample(op) 		(op == op_sample)
#define is_insert(op) 		(op == op_insert)
#define is_update(op) 		(op == op_update)
#define is_delete(op) 		(op == op_delete)
#define is_truncate(op) 	(op == op_truncate)

/* ZERO on empty sets, needed for sum (of counts)). */
#define zero_if_empty(e) 	((e)->zero_if_empty)
#define set_zero_if_empty(e) 	(e)->zero_if_empty = 1

/* NO NIL semantics of aggr operations */
#define need_no_nil(e) 		((e)->need_no_nil)
#define set_no_nil(e) 		(e)->need_no_nil = 1

/* does the expression (possibly) have nils */
#define has_nil(e)			((e)->has_no_nil==0)
#define set_has_no_nil(e) 	(e)->has_no_nil = 1
#define set_has_nil(e) 		(e)->has_no_nil = 0

#define is_ascending(e) 	((e)->ascending)
#define set_ascending(e) 	((e)->ascending = 1)
#define set_descending(e) 	((e)->ascending = 0)
#define nulls_last(e) 		((e)->nulls_last)
#define set_nulls_last(e) 	((e)->nulls_last=1)
#define set_nulls_first(e) 	((e)->nulls_last=0)
#define set_direction(e, dir) 	((e)->ascending = (dir&1), (e)->nulls_last = (dir&2)?1:0)

#define is_anti(e) 		((e)->anti)
#define set_anti(e)  		(e)->anti = 1
#define is_semantics(e) 	((e)->semantics)
#define set_semantics(e) 	(e)->semantics = 1
#define is_intern(e) 		((e)->intern)
#define set_intern(e) 		(e)->intern = 1
#define is_basecol(e) 		((e)->base)
#define set_basecol(e) 		(e)->base = 1

#define has_label(e)  		((e)->alias.label > 0)

/* used for expressions and relations */
#define need_distinct(er) 	((er)->distinct)
#define set_distinct(er) 	(er)->distinct = 1;
#define set_nodistinct(er)	(er)->distinct = 0;

#define is_processed(rel) 	((rel)->processed)
#define set_processed(rel) 	(rel)->processed = 1
#define reset_processed(rel) 	(rel)->processed = 0
#define is_dependent(rel) 	((rel)->dependent)
#define set_dependent(rel) 	(rel)->dependent = 1
#define reset_dependent(rel) 	(rel)->dependent = 0
#define is_outer(rel)		((rel)->outer)
#define set_outer(rel)		(rel)->outer = 1
#define reset_outer(rel)	(rel)->outer = 0
#define is_single(rel) 		((rel)->single)
#define set_single(rel) 	(rel)->single = 1
#define reset_single(rel) 	(rel)->single = 0

#define is_freevar(e) 		((e)->freevar)
#define set_freevar(e,level) 	(e)->freevar = level+1
#define reset_freevar(e) 	(e)->freevar = 0

#define rel_is_ref(rel)		(((sql_rel*)(rel))->ref.refcnt > 1)

typedef struct relation {
	sql_ref ref;

	operator_type op;
	void *l;
	void *r;
	list *exps;
	int nrcols;	/* nr of cols */
	unsigned int
	 flag:16,
	 card:4,	/* 0, 1 (row), 2 aggr, 3 */
	 dependent:1, 	/* dependent join */
	 distinct:1,
	 processed:1,   /* fully processed or still in the process of building */
	 outer:1,	/* used as outer (ungrouped) */
	 grouped:1,	/* groupby processed all the group by exps */
	 single:1,
	 used:2;	/* used by rewriters at rel_unnest and rel_dce, so a relation is not modified twice */
	void *p;	/* properties for the optimizer, distribution */
} sql_rel;

#endif /* SQL_RELATION_H */
