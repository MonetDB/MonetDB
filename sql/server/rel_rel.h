/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _REL_REL_H_
#define _REL_REL_H_

#include "sql_relation.h"
#include "sql_mvc.h"

#define sql_from 	0
#define sql_where 	1
#define sql_sel   	2	
#define sql_having 	3
#define sql_orderby   	4	

#define ERR_AMBIGUOUS		050000

#define rel_groupby_gbe(m,r,e) rel_groupby(m, r, append(new_exp_list(m->sa), e))
#define new_rel_list(sa) sa_list(sa)

#define is_updateble(rel) \
	(rel->op == op_basetable || \
	(rel->op == op_ddl && (rel->flag == DDL_CREATE_TABLE || rel->flag == DDL_ALTER_TABLE)))

extern const char *rel_name( sql_rel *r );
extern sql_rel *rel_distinct(sql_rel *l);

extern sql_rel *rel_dup(sql_rel *r);
extern void rel_destroy(sql_rel *rel);
extern sql_rel *rel_create(sql_allocator *sa);
extern sql_rel *rel_copy(sql_allocator *sa, sql_rel *r);
extern sql_rel *rel_select_copy(sql_allocator *sa, sql_rel *l, list *exps);

extern sql_exp *rel_bind_column( mvc *sql, sql_rel *rel, const char *cname, int f );
extern sql_exp *rel_bind_column2( mvc *sql, sql_rel *rel, const char *tname, const char *cname, int f );

extern sql_rel *rel_inplace_setop(sql_rel *rel, sql_rel *l, sql_rel *r, operator_type setop, list *exps);
extern sql_rel *rel_inplace_project(sql_allocator *sa, sql_rel *rel, sql_rel *l, list *e);
extern sql_rel *rel_inplace_groupby(sql_rel *rel, sql_rel *l, list *groupbyexps, list *exps );

extern int rel_convert_types(mvc *sql, sql_exp **L, sql_exp **R, int scale_fixing, int tpe);
extern sql_rel *rel_setop(sql_allocator *sa, sql_rel *l, sql_rel *r, operator_type setop);
extern sql_rel *rel_setop_check_types(mvc *sql, sql_rel *l, sql_rel *r, list *ls, list *rs, operator_type op);
extern sql_rel *rel_crossproduct(sql_allocator *sa, sql_rel *l, sql_rel *r, operator_type join);

extern sql_rel *rel_topn(sql_allocator *sa, sql_rel *l, list *exps );
extern sql_rel *rel_sample(sql_allocator *sa, sql_rel *l, list *exps );

extern sql_rel *rel_label( mvc *sql, sql_rel *r, int all);
extern sql_exp *rel_project_add_exp( mvc *sql, sql_rel *rel, sql_exp *e);
extern void rel_select_add_exp(sql_allocator *sa, sql_rel *l, sql_exp *e);
extern void rel_join_add_exp(sql_allocator *sa, sql_rel *rel, sql_exp *e);
extern sql_exp *rel_groupby_add_aggr(mvc *sql, sql_rel *rel, sql_exp *e);

extern sql_rel *rel_select(sql_allocator *sa, sql_rel *l, sql_exp *e);
extern sql_rel *rel_basetable(mvc *sql, sql_table *t, const char *tname);
extern sql_rel *rel_groupby(mvc *sql, sql_rel *l, list *groupbyexps );
extern sql_rel *rel_project(sql_allocator *sa, sql_rel *l, list *e);

extern sql_rel *rel_relational_func(sql_allocator *sa, sql_rel *l, list *exps);
extern sql_rel *rel_table_func(sql_allocator *sa, sql_rel *l, sql_exp *f, list *exps, int kind);


extern list *rel_projections(mvc *sql, sql_rel *rel, const char *tname, int settname , int intern);

extern sql_rel *rel_push_select(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *e);
extern sql_rel *rel_push_join(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *rs, sql_exp *rs2, sql_exp *e);
extern sql_rel *rel_or(mvc *sql, sql_rel *rel, sql_rel *l, sql_rel *r, list *oexps, list *lexps, list *rexps);

extern sql_table *rel_ddl_table_get(sql_rel *r);

extern sql_rel *rel_add_identity(mvc *sql, sql_rel *rel, sql_exp **exp);
extern sql_exp * rel_find_column( sql_allocator *sa, sql_rel *rel, const char *tname, const char *cname );
#endif /* _REL_REL_H_ */
