/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _REL_SELECT_H_
#define _REL_SELECT_H_

#include "rel_semantic.h"
#include "sql_semantic.h"
#include "rel_subquery.h"

extern sql_rel* rel_create(sql_allocator *sa);
extern sql_rel* rel_setop(sql_allocator *sa, sql_rel *l, sql_rel *r, operator_type setop);

extern sql_rel *rel_selects(mvc *sql, symbol *sym);
extern sql_rel * rel_subquery(mvc *sql, sql_rel *rel, symbol *sq, exp_kind ek);
extern sql_rel * rel_logical_exp(mvc *sql, sql_rel *rel, symbol *sc, int f);
extern sql_exp * rel_logical_value_exp(mvc *sql, sql_rel **rel, symbol *sc, int f);
extern sql_rel * rel_project(sql_allocator *sa, sql_rel *l, list *e);
extern void rel_project_add_exp( mvc *sql, sql_rel *rel, sql_exp *e);
extern list * rel_projections(mvc *sql, sql_rel *rel, char *tname, int settname , int intern);
extern sql_rel * rel_label( mvc *sql, sql_rel *r);
extern sql_exp *rel_column_exp(mvc *sql, sql_rel **rel, symbol *column_e, int f);

extern void rel_add_intern(mvc *sql, sql_rel *rel);

extern void rel_select_add_exp(sql_rel *l, sql_exp *e);
extern sql_rel *rel_select(sql_allocator *sa, sql_rel *l, sql_exp *e);
extern sql_rel *rel_select_copy(sql_allocator *sa, sql_rel *l, list *exps);
extern sql_rel *rel_basetable(mvc *sql, sql_table *t, char *tname);
extern sql_rel *_rel_basetable(sql_allocator *sa, sql_table *t, char *atname);
extern sql_rel *rel_recursive_func(sql_allocator *sa, list *exps);
extern sql_rel *rel_table_func(sql_allocator *sa, sql_rel *l, sql_exp *f, list *exps);

extern sql_exp *rel_bind_column( mvc *sql, sql_rel *rel, char *cname, int f );
extern sql_exp *rel_bind_column2( mvc *sql, sql_rel *rel, char *tname, char *cname, int f );

extern sql_exp * rel_value_exp(mvc *sql, sql_rel **rel, symbol *se, int f, exp_kind ek);
extern sql_exp * rel_value_exp2(mvc *sql, sql_rel **rel, symbol *se, int f, exp_kind ek, int *is_last);
extern sql_rel *rel_crossproduct(sql_allocator *sa, sql_rel *l, sql_rel *r, operator_type join);
extern void rel_join_add_exp(sql_allocator *sa, sql_rel *rel, sql_exp *e);

extern sql_rel *rel_push_select(sql_allocator *sa, sql_rel *rel, sql_exp *ls, sql_exp *e);
extern sql_rel *rel_push_join(sql_allocator *sa, sql_rel *rel, sql_exp *ls, sql_exp *rs, sql_exp *e);
/* TODO rename to exp_check_type + move to rel_exp.mx */
extern sql_exp *rel_check_type(mvc *sql, sql_subtype *t, sql_exp *exp, int tpe);
extern int rel_convert_types(mvc *sql, sql_exp **L, sql_exp **R, int scale_fixing, int tpe);
extern sql_exp *rel_unop_(mvc *sql, sql_exp *e, sql_schema *s, char *fname, int card);
extern sql_exp *rel_binop_(mvc *sql, sql_exp *l, sql_exp *r, sql_schema *s, char *fname, int card);
extern sql_exp *rel_nop_(mvc *sql, sql_exp *l, sql_exp *r, sql_exp *r2, sql_exp *r3, sql_schema *s, char *fname, int card);

extern sql_rel *rel_topn(sql_allocator *sa, sql_rel *l, list *exps );
extern sql_rel *rel_orderby(mvc *sql, sql_rel *l, list *orderbyexps);

extern sql_rel *rel_dup(sql_rel *r);
extern sql_rel *rel_copy(sql_allocator *sa, sql_rel *r);
extern void rel_destroy(sql_rel *rel);

#define new_rel_list(sa) list_new(sa)

/* TODO shouldn't be needed (isn't save) ! */
extern char * rel_name( sql_rel *r );

extern sql_rel *rel_groupby(sql_allocator *sa, sql_rel *l, list *groupbyexps );
extern sql_exp *rel_groupby_add_aggr(mvc *sql, sql_rel *rel, sql_exp *e);

#endif /*_REL_SELECT_H_*/
