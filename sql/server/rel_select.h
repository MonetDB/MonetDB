/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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

extern sql_exp *rel_unop_(mvc *sql, sql_rel *rel, sql_exp *e, char *sname, char *fname, int card);
extern sql_exp *rel_binop_(mvc *sql, sql_rel *rel, sql_exp *l, sql_exp *r, char *sname, char *fname, int card);
extern sql_exp *rel_nop_(mvc *sql, sql_rel *rel, sql_exp *l, sql_exp *r, sql_exp *r2, sql_exp *r3, char *sname, char *fname, int card);
extern sql_rel *rel_with_query(sql_query *query, symbol *q);
extern sql_rel *table_ref(sql_query *query, sql_rel *rel, symbol *tableref, int lateral, list *refs);
extern sql_exp *find_table_function(mvc *sql, char *sname, char *fname, list *exps, list *tl, sql_ftype type);
extern sql_rel *rel_loader_function(sql_query* query, symbol* s, list *fexps, sql_subfunc **loader_function);

#endif /*_REL_SELECT_H_*/
