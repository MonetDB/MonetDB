/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _REL_REWRITER_H_
#define _REL_REWRITER_H_

#include "sql_relation.h"
#include "rel_rel.h"

#define is_ifthenelse_func(sf) (strcmp((sf)->func->base.name, "ifthenelse") == 0)
#define is_isnull_func(sf) (strcmp((sf)->func->base.name, "isnull") == 0)
#define is_not_func(sf) (strcmp((sf)->func->base.name, "not") == 0)
#define is_caselike_func(sf) (strcmp((sf)->func->base.name, "case") == 0 || \
                          strcmp((sf)->func->base.name, "casewhen") == 0 || \
                          strcmp((sf)->func->base.name, "coalesce") == 0 || \
                          strcmp((sf)->func->base.name, "nullif") == 0)
#define is_case_func(sf) (strcmp((sf)->func->base.name, "case") == 0)

extern sql_exp *rewrite_simplify_exp(visitor *v, sql_rel *rel, sql_exp *e, int depth);
extern sql_rel *rewrite_simplify(visitor *v, uint8_t cycle, bool value_based_opt, sql_rel *rel);

static inline sql_rel *
try_remove_empty_select(visitor *v, sql_rel *rel)
{
	if (is_select(rel->op) && !(rel_is_ref(rel)) && list_empty(rel->exps)) {
		sql_rel *l = rel->l;
		rel->l = NULL;
		rel_destroy(rel);
		v->changes++;
		rel = l;
	}
	return rel;
}

extern int find_member_pos(list *l, sql_table *t);
extern sql_column *name_find_column(sql_rel *rel, const char *rname, const char *name, int pnr, sql_rel **bt);

extern int exp_joins_rels(sql_exp *e, list *rels);
/* WARNING exps_unique doesn't check for duplicate NULL values */
extern int kc_column_cmp(sql_kc *kc, sql_column *c);
extern int exps_unique(mvc *sql, sql_rel *rel, list *exps);

extern sql_column *exp_find_column(sql_rel *rel, sql_exp *exp, int pnr);

extern BUN get_rel_count(sql_rel *rel);
extern void set_count_prop(sql_allocator *sa, sql_rel *rel, BUN val);

#endif /*_REL_REWRITER_H_*/
