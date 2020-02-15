/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#ifndef _REL_REL_H_
#define _REL_REL_H_

#include "sql_mvc.h"
#include "sql_relation.h"
#include "sql_semantic.h"

#define sql_from     1
#define sql_where    2
#define sql_sel      4
#define sql_having   8
#define sql_orderby 16
#define sql_groupby 32 //ORed
#define sql_aggr    64 //ORed
#define sql_farg   128 //ORed
#define sql_window 256 //ORed
#define sql_join   512 //ORed
#define sql_outer 1024 //ORed
#define sql_group_totals 2048 //ORed

#define is_sql_from(X)    ((X & sql_from) == sql_from)
#define is_sql_where(X)   ((X & sql_where) == sql_where)
#define is_sql_sel(X)     ((X & sql_sel) == sql_sel)
#define is_sql_having(X)  ((X & sql_having) == sql_having)
#define is_sql_orderby(X) ((X & sql_orderby) == sql_orderby)
#define is_sql_groupby(X) ((X & sql_groupby) == sql_groupby)
#define is_sql_aggr(X)    ((X & sql_aggr) == sql_aggr)
#define is_sql_farg(X)    ((X & sql_farg) == sql_farg)
#define is_sql_window(X)  ((X & sql_window) == sql_window)
#define is_sql_join(X)    ((X & sql_join) == sql_join)
#define is_sql_outer(X)   ((X & sql_outer) == sql_outer)
#define is_sql_group_totals(X) ((X & sql_group_totals) == sql_group_totals)

#define is_updateble(rel) \
	(rel->op == op_basetable || \
	(rel->op == op_ddl && (rel->flag == ddl_create_table || rel->flag == ddl_alter_table)))

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

extern sql_rel *rel_inplace_setop(sql_rel *rel, sql_rel *l, sql_rel *r, operator_type setop, list *exps);
extern sql_rel *rel_inplace_project(sql_allocator *sa, sql_rel *rel, sql_rel *l, list *e);
extern sql_rel *rel_inplace_groupby(sql_rel *rel, sql_rel *l, list *groupbyexps, list *exps );

extern int rel_convert_types(mvc *sql, sql_rel *ll, sql_rel *rr, sql_exp **L, sql_exp **R, int scale_fixing, check_type tpe);
extern sql_rel *rel_setop(sql_allocator *sa, sql_rel *l, sql_rel *r, operator_type setop);
extern sql_rel *rel_setop_check_types(mvc *sql, sql_rel *l, sql_rel *r, list *ls, list *rs, operator_type op);
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
extern sql_rel *rel_basetable(mvc *sql, sql_table *t, const char *tname);
extern sql_rel *rel_groupby(mvc *sql, sql_rel *l, list *groupbyexps );
extern sql_rel *rel_project(sql_allocator *sa, sql_rel *l, list *e);
extern sql_rel *rel_project_exp(sql_allocator *sa, sql_exp *e);
extern sql_rel *rel_exception(sql_allocator *sa, sql_rel *l, sql_rel *r, list *exps);

extern sql_rel *rel_relational_func(sql_allocator *sa, sql_rel *l, list *exps);
extern sql_rel *rel_table_func(sql_allocator *sa, sql_rel *l, sql_exp *f, list *exps, int kind);

extern list *_rel_projections(mvc *sql, sql_rel *rel, const char *tname, int settname , int intern, int basecol);
extern list *rel_projections(mvc *sql, sql_rel *rel, const char *tname, int settname , int intern);
extern sql_rel *rel_safe_project(mvc *sql, sql_rel *rel);

extern sql_rel *rel_push_select(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *e);
extern sql_rel *rel_push_join(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *rs, sql_exp *rs2, sql_exp *e);
extern sql_rel *rel_or(mvc *sql, sql_rel *rel, sql_rel *l, sql_rel *r, list *oexps, list *lexps, list *rexps);

extern sql_table *rel_ddl_table_get(sql_rel *r);

extern sql_rel *rel_add_identity(mvc *sql, sql_rel *rel, sql_exp **exp);
extern sql_rel *rel_add_identity2(mvc *sql, sql_rel *rel, sql_exp **exp);
extern sql_exp * rel_find_column( sql_allocator *sa, sql_rel *rel, const char *tname, const char *cname );

extern int rel_in_rel(sql_rel *super, sql_rel *sub);

extern list *rel_dependencies(mvc *sql, sql_rel *r);
extern sql_exp * exps_find_match_exp(list *l, sql_exp *e);

typedef sql_exp *(*exp_rewrite_fptr)(mvc *sql, sql_rel *rel, sql_exp *e, int depth /* depth of the nested expression */ );
extern sql_rel *rel_exp_visitor(mvc *sql, sql_rel *rel, exp_rewrite_fptr exp_rewriter);

typedef sql_rel *(*rel_rewrite_fptr)(mvc *sql, sql_rel *rel, int *changes);
extern sql_rel *rel_visitor_topdown(mvc *sql, sql_rel *rel, rel_rewrite_fptr rel_rewriter, int *changes);
extern sql_rel *rel_visitor_bottomup(mvc *sql, sql_rel *rel, rel_rewrite_fptr rel_rewriter, int *changes);

#endif /* _REL_REL_H_ */
