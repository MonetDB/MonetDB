/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "rel_predicates.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "mal_backend.h"

static sql_column *
bt_find_column( sql_rel *rel, char *tname, char *name)
{
	if (!rel || !rel->exps || !rel->l)
		return NULL;
	sql_exp *ne = NULL;
	sql_table *t = rel->l;
	if ((ne = exps_bind_column2(rel->exps, tname, name, NULL)) != NULL)
		return find_sql_column(t, ne->r);
	return NULL;
}

static sql_column *
exp_find_column( sql_rel *rel, sql_exp *exp)
{
	if (exp->type == e_column)
		return bt_find_column(is_basetable(rel->op)?rel:rel->l, exp->l, exp->r);
	if (exp->type == e_convert)
		return exp_find_column( rel, exp->l);
	return NULL;
}

static sql_rel *
rel_find_predicates(visitor *v, sql_rel *rel)
{
	bool needall = false;

	if (is_basetable(rel->op)) {
		sql_table *t = rel->l;

		if (!t || !rel->exps || isNew(t) || t->persistence == SQL_DECLARED_TABLE)
			return rel;
		sql_rel *parent = v->parent;

		/* select with basetable */
		if (is_select(parent->op)) {
			/* add predicates */
			for (node *n = parent->exps->h; n && !needall; n = n->next) {
				sql_exp *e = n->data, *r = e->r, *r2 = e->f;
				sql_column *c = NULL;

				if (!is_compare(e->type) || !is_theta_exp(e->flag) || r->type != e_atom || !r->l || (r2 && (r2->type != e_atom || !r2->l)) || is_symmetric(e) || !(c = exp_find_column(rel, e->l))) {
					needall = true;
				} else {
					atom *e1 = r && r->l ? atom_dup(NULL, r->l) : NULL, *e2 = r2 && r2->l ? atom_dup(NULL, r2->l) : NULL;

					if ((r && r->l && !e1) || (r2 && r2->l && !e2)) {
						if (e1) {
							VALclear(&e1->data);
							_DELETE(e1);
						}
						if (e2) {
							VALclear(&e2->data);
							_DELETE(e2);
						}
						return sql_error(v->sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					}

					if (sql_trans_add_predicate(v->sql->session->tr, c, e->flag, e1, e2, is_anti(e), is_semantics(e)) != LOG_OK) {
						if (e1) {
							VALclear(&e1->data);
							_DELETE(e1);
						}
						if (e2) {
							VALclear(&e2->data);
							_DELETE(e2);
						}
						return sql_error(v->sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					}
					*(int*)v->data = 1;
				}
			}
		}

		if (!is_select(parent->op) || needall) {
			/* any other case, add all predicates */
			sql_table *t = rel->l;

			if (!t || !rel->exps)
				return rel;
			for (node *n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (!is_intern(e)) {
					sql_column *c = find_sql_column(t, e->r);

					assert(c);
					if (sql_trans_add_predicate(v->sql->session->tr, c, 0, NULL, NULL, false, false) != LOG_OK)
						return sql_error(v->sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					*(int*)v->data = 1;
				}
			}
		}
	}
	return rel;
}

sql_rel *
rel_predicates(backend *be, sql_rel *rel)
{
	if (be->mvc->session->level < tr_serializable)
		return rel;
	int changes = 0;
	visitor v = { .sql = be->mvc, .data = &changes };
	rel = rel_visitor_topdown(&v, rel, &rel_find_predicates);
	return rel;
}

int
add_column_predicate(backend *be, sql_column *c)
{
	if (be->mvc->session->level < tr_serializable)
		return LOG_OK;
	return sql_trans_add_predicate(be->mvc->session->tr, c, 0, NULL, NULL, false, false);
}
