/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _REL_SELECT_H_
#define _REL_SELECT_H_

#include "rel_semantic.h"
#include "sql_semantic.h"

#define rel_groupby_gbe(m,r,e) rel_groupby(m, r, append(new_exp_list(m->sa), e))

extern sql_rel* rel_create(sql_allocator *sa);
extern sql_rel* rel_setop(sql_allocator *sa, sql_rel *l, sql_rel *r, operator_type setop);
extern sql_rel* rel_inplace_setop(sql_rel *rel, sql_rel *l, sql_rel *r, operator_type setop, list *exps);

extern sql_rel *rel_selects(mvc *sql, symbol *sym);
extern sql_rel *schema_selects(mvc *sql, sql_schema *s, symbol *sym);
extern sql_rel * rel_subquery(mvc *sql, sql_rel *rel, symbol *sq, exp_kind ek, int apply);
extern sql_rel * rel_logical_exp(mvc *sql, sql_rel *rel, symbol *sc, int f);
extern sql_exp * rel_logical_value_exp(mvc *sql, sql_rel **rel, symbol *sc, int f);
extern sql_rel * rel_project(sql_allocator *sa, sql_rel *l, list *e);
extern sql_rel * rel_inplace_project(sql_allocator *sa, sql_rel *rel, sql_rel *l, list *e);
extern void rel_project_add_exp( mvc *sql, sql_rel *rel, sql_exp *e);
extern list * rel_projections(mvc *sql, sql_rel *rel, char *tname, int settname , int intern);
extern sql_rel * rel_label( mvc *sql, sql_rel *r, int all);
extern sql_exp *rel_column_exp(mvc *sql, sql_rel **rel, symbol *column_e, int f);

extern void rel_select_add_exp(sql_allocator *sa, sql_rel *l, sql_exp *e);
extern sql_rel *rel_select(sql_allocator *sa, sql_rel *l, sql_exp *e);
extern sql_rel *rel_select_copy(sql_allocator *sa, sql_rel *l, list *exps);
extern sql_rel *rel_basetable(mvc *sql, sql_table *t, char *tname);
extern sql_rel *rel_table_func(sql_allocator *sa, sql_rel *l, sql_exp *f, list *exps, int kind);
extern sql_rel *rel_relational_func(sql_allocator *sa, sql_rel *l, list *exps);

extern sql_exp *rel_bind_column( mvc *sql, sql_rel *rel, char *cname, int f );
extern sql_exp *rel_bind_column2( mvc *sql, sql_rel *rel, char *tname, char *cname, int f );

extern sql_exp * rel_value_exp(mvc *sql, sql_rel **rel, symbol *se, int f, exp_kind ek);
extern sql_exp * rel_value_exp2(mvc *sql, sql_rel **rel, symbol *se, int f, exp_kind ek, int *is_last);
extern sql_rel *rel_crossproduct(sql_allocator *sa, sql_rel *l, sql_rel *r, operator_type join);
extern void rel_join_add_exp(sql_allocator *sa, sql_rel *rel, sql_exp *e);

extern sql_rel *rel_push_select(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *e);
extern sql_rel *rel_push_join(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *rs, sql_exp *rs2, sql_exp *e);
extern sql_rel *rel_or(mvc *sql, sql_rel *l, sql_rel *r, list *oexps, list *lexps, list *rexps);
/* TODO rename to exp_check_type + move to rel_exp.mx */
extern sql_exp *rel_check_type(mvc *sql, sql_subtype *t, sql_exp *exp, int tpe);
extern int rel_convert_types(mvc *sql, sql_exp **L, sql_exp **R, int scale_fixing, int tpe);
extern sql_exp *rel_unop_(mvc *sql, sql_exp *e, sql_schema *s, char *fname, int card);
extern sql_exp *rel_binop_(mvc *sql, sql_exp *l, sql_exp *r, sql_schema *s, char *fname, int card);
extern sql_exp *rel_nop_(mvc *sql, sql_exp *l, sql_exp *r, sql_exp *r2, sql_exp *r3, sql_schema *s, char *fname, int card);

extern sql_rel *rel_topn(sql_allocator *sa, sql_rel *l, list *exps );
extern sql_rel *rel_sample(sql_allocator *sa, sql_rel *l, list *exps );

extern sql_rel *rel_dup(sql_rel *r);
extern sql_rel *rel_copy(sql_allocator *sa, sql_rel *r);
extern void rel_destroy(sql_rel *rel);

#define new_rel_list(sa) sa_list(sa)

/* TODO shouldn't be needed (isn't save) ! */
extern char * rel_name( sql_rel *r );

extern sql_rel *rel_groupby(mvc *sql, sql_rel *l, list *groupbyexps );
extern sql_exp *rel_groupby_add_aggr(mvc *sql, sql_rel *rel, sql_exp *e);
extern sql_rel *rel_inplace_groupby(sql_rel *rel, sql_rel *l, list *groupbyexps, list *exps );
extern sql_rel *rel_with_query(mvc *sql, symbol *q);

#endif /*_REL_SELECT_H_*/
