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

extern sql_exp *rewrite_simplify_exp(visitor *v, sql_rel *rel, sql_exp *e, int depth);
extern sql_rel *rewrite_simplify(visitor *v, sql_rel *rel);

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

extern sql_rel *rewrite_reset_used(visitor *v, sql_rel *rel);

#endif /*_REL_REWRITER_H_*/
