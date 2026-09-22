/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "rel_util.h"
#include "rel_exp.h"

bool
can_join_exp(sql_rel *rel, sql_exp *e, bool anti)
{
	bool can_join = 0;

	if (e->type == e_cmp) {
		int flag = e->flag;
		/* check if its a select or join expression, ie use only expressions of one relation left and of the other right (than join) */
		if (flag < cmp_filter) { /* theta and range joins */
			/* join or select ? */
			sql_exp *l = e->l, *r = e->r, *f = e->f;

			if (f) {
				int ll = rel_has_exp(rel->l, l, true) == 0;
				int rl = rel_has_exp(rel->r, l, true) == 0;
				int lr = rel_has_exp(rel->l, r, true) == 0;
				int rr = rel_has_exp(rel->r, r, true) == 0;
				int lf = rel_has_exp(rel->l, f, true) == 0;
				int rf = rel_has_exp(rel->r, f, true) == 0;
				int nrcr1 = 0, nrcr2 = 0, nrcl1 = 0, nrcl2 = 0;

				if ((ll && !rl &&
				   ((rr && !lr) || (nrcr1 = r->card == CARD_ATOM && exp_is_atom(r))) &&
				   ((rf && !lf) || (nrcr2 = f->card == CARD_ATOM && exp_is_atom(f))) && (nrcr1+nrcr2) <= 1) ||
					(rl && !ll &&
				   ((lr && !rr) || (nrcl1 = r->card == CARD_ATOM && exp_is_atom(r))) &&
				   ((lf && !rf) || (nrcl2 = f->card == CARD_ATOM && exp_is_atom(f))) && (nrcl1+nrcl2) <= 1)) {
					can_join = 1;
				}
			} else {
				int ll = 0, lr = 0, rl = 0, rr = 0, cst = 0;
				if (l->card != CARD_ATOM || !exp_is_atom(l)) {
					ll = rel_has_exp(rel->l, l, true) == 0;
					rl = rel_has_exp(rel->r, l, true) == 0;
				} else if (anti) {
					ll = 1;
					cst = 1;
				}
				if (r->card != CARD_ATOM || !exp_is_atom(r)) {
					lr = rel_has_exp(rel->l, r, true) == 0;
					rr = rel_has_exp(rel->r, r, true) == 0;
				} else if (anti) {
					rr = cst?0:1;
				}
				if ((ll && !lr && !rl && rr) || (!ll && lr && rl && !rr))
					can_join = 1;
			}
		} else if (flag == cmp_filter) {
			list *l = e->l, *r = e->r;
			int ll = 0, lr = 0, rl = 0, rr = 0;

			for (node *n = l->h ; n ; n = n->next) {
				sql_exp *ee = n->data;

				if (ee->card != CARD_ATOM || !exp_is_atom(ee)) {
					ll |= rel_has_exp(rel->l, ee, true) == 0;
					rl |= rel_has_exp(rel->r, ee, true) == 0;
				}
			}
			for (node *n = r->h ; n ; n = n->next) {
				sql_exp *ee = n->data;

				if (ee->card != CARD_ATOM || !exp_is_atom(ee)) {
					lr |= rel_has_exp(rel->l, ee, true) == 0;
					rr |= rel_has_exp(rel->r, ee, true) == 0;
				}
			}
			if ((ll && !lr && !rl && rr) || (!ll && lr && rl && !rr))
				can_join = 1;
		}
	}
	return can_join;
}

void
split_join_exps(sql_rel *rel, list *joinable, list *not_joinable, bool eqonly, bool anti)
{
	if (!list_empty(rel->exps)) {
		for (node *n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			/* eqonly -
			 *   TRUE: only handle equi-join expressions, e.g. in case of pipeline hash join
			 *   FALSE: we can (also) handle thetajoins, rangejoins and filter joins (like) */
			if (can_join_exp(rel, e, anti) && (!eqonly || (is_equi_exp_(e) && !exp_is_atom(e->r) && !exp_is_atom(e->l)))) {
				append(joinable, e);
			} else {
				append(not_joinable, e);
			}
		}
	}
}

list *
get_simple_equi_joins_first(mvc *sql, sql_rel *rel, list *exps)
{
	list *new_exps = sa_list(sql->sa);

	if (!exps)
		return new_exps;

	for (node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (can_join_exp(rel, e, false) && is_equi_exp_(e) && !is_any(e))
			list_append(new_exps, e);
	}
	for (node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (!is_equi_exp_(e) || !can_join_exp(rel, e, false) || is_any(e))
			list_append(new_exps, e);
	}
	return new_exps;
}

