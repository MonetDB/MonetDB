/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"

#include "rel_orderby.h"
#include "rel_bin.h"
#include "rel_exp.h"
#include "rel_rewriter.h"
#include "mal_builder.h"
#include "sql_pp_statement.h"
#include "bin_partition.h"

list*
rel2bin_project_prepare(backend *be, sql_rel *rel)
{
	assert(is_project(rel->op));
	list *shared = sa_list(be->mvc->sa);
	if (shared && !list_empty(rel->exps)) {
		BUN est = get_rel_count(rel);
		lng estimate;

		if (est == BUN_NONE || (ulng) est > (ulng) GDK_int_max) {
			estimate = 1024;
		} else {
			estimate = (lng) est;
		}
		for(node *n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_subtype *t = exp_subtype(e);

			stmt *s = stmt_bat_new(be, t, estimate);
			append(shared, s);
		}
	}
	return shared;
}

static int
find_matching_exp(list *exps, sql_exp *e)
{
	int i = 0;
	for (node *n = exps->h; n; n = n->next, i++) {
		if (exp_match(n->data, e))
			return i;
	}
	return -1;
}

stmt *
sql_reorder(backend *be, stmt *order, list *exps, stmt *s, list *oexps, list *ostmts)
{
	list *l = sa_list(be->mvc->sa);

	for (node *n = s->op4.lval->h, *m = exps->h; n && m; n = n->next, m = m->next) {
		int pos = 0;
		stmt *sc = n->data;
		sql_exp *pe = m->data;
		const char *cname = column_name(be->mvc->sa, sc);
		const char *tname = table_name(be->mvc->sa, sc);

		if (oexps && (pos = find_matching_exp(oexps, pe)) >= 0 && list_fetch(ostmts, pos)) {
			sc = list_fetch(ostmts, pos);
		} else {
			sc = stmt_project(be, order, sc);
		}
		sc = stmt_alias(be, sc, pe->alias.label, tname, cname);
		list_append(l, sc);
	}
	return stmt_list(be, l);
}

static InstrPtr
sop_add(backend *be, InstrPtr sop, stmt *sub, list *ostmts)
{
	list *l = sub->op4.lval;
	int nr = list_length(l) + list_length(ostmts);

	InstrPtr q = newStmtArgs(be->mb, "sop", "add", nr + 3);
	pushArgument(be->mb, q, getArg(sop, 0));
	if (ostmts) {
		for(node *n = ostmts->h; n; n = n->next) {
			stmt *col = n->data;
			pushArgument(be->mb, q, col->nr);
		}
	}
	for(node *n = l->h; n; n = n->next) {
		stmt *col = n->data;
		pushArgument(be->mb, q, col->nr);
	}
	pushInstruction(be->mb, q);
	getArg(q, 0) = getArg(sop, 0);
	return q;
}

static stmt *
fetch(backend *be, InstrPtr sop, sql_rel *rel, list *oexps, bool skip_oexps )
{
	list *l = sa_list(be->mvc->sa);
	int nr = list_length(rel->exps) + list_length(oexps), i = 0;
	InstrPtr q = newStmtArgs(be->mb, "sop", "fetch", nr + 2);

	for(node *n = oexps->h; n; n = n->next, i++) {
		sql_exp *e = n->data;
		sql_subtype *t = exp_subtype(e);
		int tt = t->type->localtype;
		if (i)
			q = pushReturn(be->mb, q, newTmpVariable(be->mb, newBatType(tt)));
		else
			setVarType(be->mb, getArg(q, 0), newBatType(tt));
		if (!skip_oexps)
			append(l, stmt_pp_alias(be, q, e, i));
	}
	for(node *n = rel->exps->h; n; n = n->next, i++) {
		sql_exp *e = n->data;
		sql_subtype *t = exp_subtype(e);
		int tt = t->type->localtype;
		if (i)
			q = pushReturn(be->mb, q, newTmpVariable(be->mb, newBatType(tt)));
		else
			setVarType(be->mb, getArg(q, 0), newBatType(tt));
		append(l, stmt_pp_alias(be, q, e, i));
	}
	pushArgument(be->mb, q, getArg(sop, 0));
	if (be->pipeline)
		pushArgument(be->mb, q, be->pipeline);
	pushInstruction(be->mb, q);
	return stmt_list(be, l);
}

stmt *
rel2bin_orderby(backend *be, sql_rel *rel, list *refs)
{
	assert(rel->r);
	InstrPtr sop = stmt_sop_new(be, GDKnr_threads); /* keep sorted parts in a set */
	//list *shared = rel2bin_project_prepare(be, rel);
	stmt *pp = NULL;

	set_need_pipeline(be);

	stmt *sub = NULL;
	if (rel->l) { /* first construct the sub relation */
		sub = subrel_bin(be, rel->l, refs);
		sub = subrel_project(be, sub, refs, rel->l);
		if (!sub)
			return NULL;
	}

	/* project */
	list *pl = sa_list(be->mvc->sa);
	if (pl == NULL)
		return NULL;
	if (sub)
		pl->expected_cnt = list_length(sub->op4.lval);
	stmt *psub = stmt_list(be, pl);
	if (psub == NULL)
		return NULL;
	for (node *en = rel->exps->h; en; en = en->next) {
		sql_exp *exp = en->data;
		stmt *s = exp_bin(be, exp, sub, NULL /*psub*/, NULL, NULL, NULL, NULL, 0, 0, 0);

		if (!s)
			s = exp_bin(be, exp, sub, psub, NULL, NULL, NULL, NULL, 0, 0, 0);
		if (!s) /* error */
			return NULL;
		/* single */
		if (sub && sub->nrcols >= 1 && s->nrcols == 0)
			s = stmt_const(be, bin_find_smallest_column(be, sub), s);

		if (!exp_name(exp))
			exp_label(be->mvc->sa, exp, ++be->mvc->label);
		if (exp_name(exp)) {
			s = stmt_rename(be, exp, s);
			s->label = exp->alias.label;
		}
		list_append(pl, s);
	}
	stmt_set_nrcols(psub);

	if (!pp)
		pp = get_pipeline(be);
	assert(pp);
	list *oexps = rel->r;
	stmt *orderby_ids = NULL, *orderby_grp = NULL;
	list *ostmts = sa_list(be->mvc->sa);
	for (node *en = oexps->h; en; en = en->next) {
		sql_exp *orderbycole = en->data;
		stmt *orderbycolstmt = exp_bin(be, orderbycole, sub, psub, NULL, NULL, NULL, NULL, 0, 0, 0);

		if (!orderbycolstmt) {
			assert(be->mvc->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
			return NULL;
		}
		/* single values don't need sorting */
		if (orderbycolstmt->nrcols == 0) {
			append(ostmts, NULL);
			continue;
		}
		stmt *orderby = NULL;
		if (orderby_ids)
			orderby = stmt_reorder(be, orderbycolstmt, is_ascending(orderbycole), nulls_last(orderbycole), orderby_ids, orderby_grp);
		else
			orderby = stmt_order(be, orderbycolstmt, is_ascending(orderbycole), nulls_last(orderbycole));
		stmt *orderby_vals = stmt_result(be, orderby, 0);
		append(ostmts, orderby_vals);
		orderby_ids = stmt_result(be, orderby, 1);
		orderby_grp = stmt_result(be, orderby, 2);
	}
	if (orderby_ids) {
		psub = sql_reorder(be, orderby_ids, rel->exps, psub, oexps, ostmts);
		sop = sop_add(be, sop, psub, ostmts);
	}
	(void)stmt_pp_jump(be, pp, be->nrparts);
	stmt_pp_end(be, pp);

	/* loop over sop */
	set_pipeline(be, pp = stmt_pp_start_generator(be, getArg(sop, 0), true));

	/* sort merge */
	// zigzag zz
	// (zzl1, zzb1, zza1) := sort.merge(lobc1, robc1) ;
	// (zzl2, zzb2, zza2) := sort.merge(lobc2, robc2, zzl1, zzb1, zza1) ;
	// (!res) := sort.mproject(zzln, lcol, rcol);
	stmt *zzl = NULL, *zzb = NULL, *zza = NULL;

	stmt *l = fetch(be, sop, rel, oexps, false);
	stmt *r = fetch(be, sop, rel, oexps, false);

	/* no more exp_bin just directly use last n-cols from list */
	node *ln = l->op4.lval->h, *rn = r->op4.lval->h;
	for (node *en = oexps->h; en; en = en->next, ln = ln->next, rn = rn->next) {
		sql_exp *obce = en->data;
		stmt *ol = ln->data; // exp_bin(be, obce, l, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		stmt *or = rn->data; // exp_bin(be, obce, r, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);

		if (ol->nrcols == 0) /* single values don't need sorting */
			continue;
		stmt *mergeby = stmt_merge(be, ol, or, is_ascending(obce), nulls_last(obce), zzl, zzb, zza);

		zzl = stmt_result(be, mergeby, 0);
		zzb = stmt_result(be, mergeby, 1);
		zza = stmt_result(be, mergeby, 2);
	}
	ostmts = sa_list(be->mvc->sa);
	ln = l->op4.lval->h;
	rn = r->op4.lval->h;
	for (node *en = oexps->h; en; en = en->next, ln = ln->next, rn = rn->next) {
		stmt *lc = ln->data;
		stmt *rc = rn->data;
		stmt *mcol = stmt_mproject(be, zzl, lc, rc, 0);
		append(ostmts, mcol);
	}

	list *nsub = sa_list(be->mvc->sa);
	/* no more exp bin use first n stmt directly */
	for (node *en = rel->exps->h; en && ln && rn; en = en->next, ln = ln->next, rn = rn->next) {
		//sql_exp *ce = en->data;
		stmt *lc = ln->data; //exp_bin(be, ce, l, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		stmt *rc = rn->data; //exp_bin(be, ce, r, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		stmt *mcol = stmt_mproject(be, zzl, lc, rc, 0);
		append(nsub, mcol);
	}
	sub = stmt_list(be, nsub);
	sop = sop_add(be, sop, sub, ostmts);

	/* end pp */
	if (pp) {
		(void)stmt_pp_jump(be, pp, be->nrparts);
		stmt_pp_end(be, pp);
	}
	return fetch(be, sop, rel, oexps, true);
}
