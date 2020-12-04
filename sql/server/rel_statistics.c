
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_statistics.h"
#include "rel_optimizer.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_rewriter.h"
#include "sql_mvc.h"

static sql_exp *
rel_get_statisitics(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	mvc *sql = v->sql;
	sql_column *c = NULL;
	(void) depth;

	if (is_basetable(rel->op) && (c = name_find_column(rel, e->l, e->r, -2, NULL))) {
		ValPtr min = NULL, max = NULL;

		if (has_nil(e) && mvc_has_no_nil(sql, c))
			set_has_no_nil(e);

		if ((min = mvc_has_min_value(sql, c))) {
			prop *p = e->p = prop_create(sql->sa, PROP_MIN, e->p);
			p->value = min;
		}
		if ((max = mvc_has_max_value(sql, c))) {
			prop *p = e->p = prop_create(sql->sa, PROP_MAX, e->p);
			p->value = max;
		}
	}
	return e;
}

sql_rel *
rel_statistics(mvc *sql, sql_rel *rel)
{
	visitor v = { .sql = sql, .value_based_opt = 0, .storage_based_opt = 1 };

	rel = rel_exp_visitor_bottomup(&v, rel, &rel_get_statisitics, false);
	return rel;
}
