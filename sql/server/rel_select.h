/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#ifndef _REL_SELECT_H_
#define _REL_SELECT_H_

#include "rel_semantic.h"
#include "sql_semantic.h"
#include "sql_query.h"

extern sql_rel *rel_selects(sql_query *query, symbol *sym);
extern sql_rel *schema_selects(sql_query *query, sql_schema *s, symbol *sym);
extern sql_rel * rel_subquery(sql_query *query, sql_rel *rel, symbol *sq, exp_kind ek);
extern sql_rel * rel_logical_exp(sql_query *query, sql_rel *rel, symbol *sc, int f);
extern sql_exp * rel_logical_value_exp(sql_query *query, sql_rel **rel, symbol *sc, int f, exp_kind ek);

extern sql_exp *rel_column_exp(sql_query *query, sql_rel **rel, symbol *column_e, int f);
extern sql_exp * rel_value_exp(sql_query *query, sql_rel **rel, symbol *se, int f, exp_kind ek);
extern sql_exp * rel_value_exp2(sql_query *query, sql_rel **rel, symbol *se, int f, exp_kind ek);

/* TODO rename to exp_check_type + move to rel_exp.c */
extern sql_exp *rel_check_type(mvc *sql, sql_subtype *t, sql_rel *rel, sql_exp *exp, check_type tpe);

extern sql_exp *rel_unop_(mvc *sql, sql_rel *rel, sql_exp *e, sql_schema *s, char *fname, int card);
extern sql_exp *rel_binop_(mvc *sql, sql_rel *rel, sql_exp *l, sql_exp *r, sql_schema *s, char *fname, int card);
extern sql_exp *rel_nop_(mvc *sql, sql_rel *rel, sql_exp *l, sql_exp *r, sql_exp *r2, sql_exp *r3, sql_schema *s,
						 char *fname, int card);
extern sql_rel *rel_with_query(sql_query *query, symbol *q);
extern sql_rel *rel_reduce_on_column_privileges(mvc *sql, sql_rel *rel, sql_table *t);
extern sql_rel *table_ref(sql_query *query, sql_rel *rel, symbol *tableref, int lateral);

extern sql_rel *rel_loader_function(sql_query* query, symbol* s, list *fexps, sql_subfunc **loader_function);

#endif /*_REL_SELECT_H_*/
