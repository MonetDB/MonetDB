/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _REL_REWRITER_H_
#define _REL_REWRITER_H_

#include "sql_relation.h"
#include "sql_mvc.h"
#include "rel_rel.h"

#define is_ifthenelse_func(sf) (strcmp((sf)->func->base.name, "ifthenelse") == 0)
#define is_isnull_func(sf) (strcmp((sf)->func->base.name, "isnull") == 0)
#define is_not_func(sf) (strcmp((sf)->func->base.name, "not") == 0)

typedef struct global_props {
	int cnt[ddl_maxops];
} global_props;

extern sql_exp *rewrite_simplify_exp(visitor *v, sql_rel *rel, sql_exp *e, int depth);
extern sql_rel *rewrite_simplify(visitor *v, sql_rel *rel);
extern sql_rel *rel_remove_empty_select(visitor *v, sql_rel *rel);

extern sql_exp *exp_push_down(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t);

extern sql_rel *rewrite_reset_used(visitor *v, sql_rel *rel);

extern void rel_properties(mvc *sql, global_props *gp, sql_rel *rel);

extern int find_member_pos(list *l, sql_table *t);
extern void *name_find_column( sql_rel *rel, const char *rname, const char *name, int pnr, sql_rel **bt);

#endif /*_REL_REWRITER_H_*/
