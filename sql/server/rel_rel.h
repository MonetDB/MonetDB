/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _REL_REL_H_
#define _REL_REL_H_

#include "sql_mvc.h"
#include "sql_relation.h"
#include "sql_semantic.h"

#define sql_from         (1 << 0)  //ORed
#define sql_where        (1 << 1)
#define sql_sel          (1 << 2)
#define sql_having       (1 << 3)
#define sql_orderby      (1 << 4)
#define sql_groupby      (1 << 5)  //ORed
#define sql_aggr         (1 << 6)  //ORed
#define sql_farg         (1 << 7)  //ORed
#define sql_window       (1 << 8)  //ORed
#define sql_join         (1 << 9)  //ORed
#define sql_outer        (1 << 10) //ORed
#define sql_group_totals (1 << 11) //ORed
#define sql_update_set   (1 << 12) //ORed
#define sql_psm          (1 << 13) //ORed
#define sql_values       (1 << 14) //ORed
#define psm_call         (1 << 15) //ORed
#define sql_or           (1 << 16) //ORed

#define is_sql_from(X)         ((X & sql_from) == sql_from)
#define is_sql_where(X)        ((X & sql_where) == sql_where)
#define is_sql_sel(X)          ((X & sql_sel) == sql_sel)
#define is_sql_having(X)       ((X & sql_having) == sql_having)
#define is_sql_orderby(X)      ((X & sql_orderby) == sql_orderby)
#define is_sql_groupby(X)      ((X & sql_groupby) == sql_groupby)
#define is_sql_aggr(X)         ((X & sql_aggr) == sql_aggr)
#define is_sql_farg(X)         ((X & sql_farg) == sql_farg)
#define is_sql_window(X)       ((X & sql_window) == sql_window)
#define is_sql_join(X)         ((X & sql_join) == sql_join)
#define is_sql_outer(X)        ((X & sql_outer) == sql_outer)
#define is_sql_group_totals(X) ((X & sql_group_totals) == sql_group_totals)
#define is_sql_update_set(X)   ((X & sql_update_set) == sql_update_set)
#define is_sql_psm(X)          ((X & sql_psm) == sql_psm)
#define is_sql_values(X)       ((X & sql_values) == sql_values)
#define is_psm_call(X)         ((X & psm_call) == psm_call)
#define is_sql_or(X)           ((X & sql_or) == sql_or)

extern void rel_set_exps(sql_rel *rel, list *exps);
extern int project_unsafe(sql_rel *rel, int allow_identity);
extern const char *rel_name( sql_rel *r );
extern sql_rel *rel_distinct(sql_rel *l);

extern sql_rel *rel_dup(sql_rel *r);
extern void rel_destroy(sql_rel *rel);
extern sql_rel *rel_create(sql_allocator *sa);
extern sql_rel *rel_copy(mvc *sql, sql_rel *r, int deep);
extern sql_rel *rel_select_copy(sql_allocator *sa, sql_rel *l, list *exps);

extern sql_exp *rel_bind_column( mvc *sql, sql_rel *rel, const char *cname, int f, int no_tname);
extern sql_exp *rel_bind_column2( mvc *sql, sql_rel *rel, const char *tname, const char *cname, int f );
extern sql_exp *rel_first_column(mvc *sql, sql_rel *rel);

extern sql_rel *rel_inplace_setop(mvc *sql, sql_rel *rel, sql_rel *l, sql_rel *r, operator_type setop, list *exps);
extern sql_rel *rel_inplace_project(sql_allocator *sa, sql_rel *rel, sql_rel *l, list *e);
extern sql_rel *rel_inplace_groupby(sql_rel *rel, sql_rel *l, list *groupbyexps, list *exps );

extern int rel_convert_types(mvc *sql, sql_rel *ll, sql_rel *rr, sql_exp **L, sql_exp **R, int scale_fixing, check_type tpe);
extern sql_rel *rel_setop(sql_allocator *sa, sql_rel *l, sql_rel *r, operator_type setop);
extern sql_rel *rel_setop_check_types(mvc *sql, sql_rel *l, sql_rel *r, list *ls, list *rs, operator_type op);
extern void rel_setop_set_exps(mvc *sql, sql_rel *rel, list *exps);
extern sql_rel *rel_crossproduct(sql_allocator *sa, sql_rel *l, sql_rel *r, operator_type join);

/* in case e is an constant and rel is a simple project of only e, free rel */
extern sql_exp *rel_is_constant(sql_rel **rel, sql_exp *e);

extern sql_rel *rel_topn(sql_allocator *sa, sql_rel *l, list *exps );
extern sql_rel *rel_sample(sql_allocator *sa, sql_rel *l, list *exps );

extern sql_rel *rel_label( mvc *sql, sql_rel *r, int all);
extern sql_exp *rel_project_add_exp( mvc *sql, sql_rel *rel, sql_exp *e);
extern sql_rel *rel_select_add_exp(sql_allocator *sa, sql_rel *l, sql_exp *e);
extern void rel_join_add_exp(sql_allocator *sa, sql_rel *rel, sql_exp *e);
extern sql_exp *rel_groupby_add_aggr(mvc *sql, sql_rel *rel, sql_exp *e);

extern sql_rel *rel_select(sql_allocator *sa, sql_rel *l, sql_exp *e);
extern sql_rel *rel_groupby(mvc *sql, sql_rel *l, list *groupbyexps );
sql_export sql_rel *rel_project(sql_allocator *sa, sql_rel *l, list *e);
extern sql_rel *rel_project_exp(sql_allocator *sa, sql_exp *e);
extern sql_rel *rel_exception(sql_allocator *sa, sql_rel *l, sql_rel *r, list *exps);

extern sql_rel *rel_relational_func(sql_allocator *sa, sql_rel *l, list *exps);
extern sql_rel *rel_table_func(sql_allocator *sa, sql_rel *l, sql_exp *f, list *exps, int kind);

extern list *_rel_projections(mvc *sql, sql_rel *rel, const char *tname, int settname , int intern, int basecol);
extern list *rel_projections(mvc *sql, sql_rel *rel, const char *tname, int settname , int intern);

extern sql_rel *rel_push_select(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *e, int f);
extern sql_rel *rel_push_join(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *rs, sql_exp *rs2, sql_exp *e, int f);
extern sql_rel *rel_or(mvc *sql, sql_rel *rel, sql_rel *l, sql_rel *r, list *oexps, list *lexps, list *rexps);

extern sql_rel *rel_add_identity(mvc *sql, sql_rel *rel, sql_exp **exp);
extern sql_rel *rel_add_identity2(mvc *sql, sql_rel *rel, sql_exp **exp);
extern sql_exp * rel_find_column( sql_allocator *sa, sql_rel *rel, const char *tname, const char *cname );

extern int rel_in_rel(sql_rel *super, sql_rel *sub);
extern sql_rel *rel_parent(sql_rel *rel);
extern sql_exp *lastexp(sql_rel *rel);
extern sql_rel *rel_return_zero_or_one(mvc *sql, sql_rel *rel, exp_kind ek);
extern sql_rel *rel_zero_or_one(mvc *sql, sql_rel *rel, exp_kind ek);

extern list *rel_dependencies(mvc *sql, sql_rel *r);

typedef struct visitor {
	int changes;
	int depth;		/* depth of the current relation */
	sql_rel *parent;
	mvc *sql;
	void *data;
	bte value_based_opt:1, /* during rel optimizer it has to now if value based optimization is possible */
		storage_based_opt:1;
} visitor;

typedef sql_exp *(*exp_rewrite_fptr)(visitor *v, sql_rel *rel, sql_exp *e, int depth /* depth of the nested expression */);
extern sql_rel *rel_exp_visitor_topdown(visitor *v, sql_rel *rel, exp_rewrite_fptr exp_rewriter, bool relations_topdown);
extern sql_rel *rel_exp_visitor_bottomup(visitor *v, sql_rel *rel, exp_rewrite_fptr exp_rewriter, bool relations_topdown);

extern list *exps_exp_visitor_topdown(visitor *v, sql_rel *rel, list *exps, int depth, exp_rewrite_fptr exp_rewriter, bool relations_topdown);
extern list *exps_exp_visitor_bottomup(visitor *v, sql_rel *rel, list *exps, int depth, exp_rewrite_fptr exp_rewriter, bool relations_topdown);

typedef sql_rel *(*rel_rewrite_fptr)(visitor *v, sql_rel *rel);
extern sql_rel *rel_visitor_topdown(visitor *v, sql_rel *rel, rel_rewrite_fptr rel_rewriter);
extern sql_rel *rel_visitor_bottomup(visitor *v, sql_rel *rel, rel_rewrite_fptr rel_rewriter);

/* validate that all parts of the expression e can be bound to the relation rel (or are atoms) */
extern bool rel_rebind_exp(mvc *sql, sql_rel *rel, sql_exp *e);

extern int exp_freevar_offset(mvc *sql, sql_exp *e);

#endif /* _REL_REL_H_ */
