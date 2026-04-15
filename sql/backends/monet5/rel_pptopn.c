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

#include "rel_pptopn.h"
#include "rel_bin.h"
#include "rel_exp.h"
#include "rel_rewriter.h"
#include "rel_basetable.h"
#include "rel_orderby.h"
#include "mal_builder.h"
#include "sql_pp_statement.h"
#include "bin_partition.h"
#include "bin_partition_by_slice.h"
#include "bin_partition_by_value.h"

static bool
has_partitioning( list *exps )
{
	for(node *n = exps->h; n; n = n->next){
		sql_exp *gbe = n->data;
		if (is_partitioning(gbe))
			return true;
	}
	return false;
}

static stmt *
stmt_heapn_projection(backend *be, int pos, int sel, stmt *c)
{

	InstrPtr q = newStmt(be->mb, getName("heapn"), getName("projection"));
	sql_subtype *t = tail_type(c);
	int tt = newBatType(t->type->localtype);
	getArg(q, 0) = newTmpVariable(be->mb, tt);

	q->inout = 0;
	q = pushArgument(be->mb, q, pos);
	q = pushArgument(be->mb, q, sel);
	q = pushArgument(be->mb, q, c->nr);
	q = pushArgument(be->mb, q, be->pipeline);
	pushInstruction(be->mb, q);

	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_join);
		if (s == NULL) {
			freeInstruction(be->mb, q);
			return NULL;
		}

		s->op1 = c;
		s->op2 = c;
		s->flag = cmp_project;
		s->key = 0;
		s->nrcols = c->nrcols;
		s->nr = getDestVar(q);
		s->q = q;
		s->tname = c->tname;
		s->cname = c->cname;
		return s;
	}
	return NULL;
}

static stmt *
stmt_heapn_order(backend *be, int heap)
{
	InstrPtr q = newStmt(be->mb, getName("heapn"), getName("order"));
	int tt = newBatType(TYPE_oid);
	getArg(q, 0) = newTmpVariable(be->mb, tt);
	q = pushArgument(be->mb, q, heap);
	pushInstruction(be->mb, q);

	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_order);
		if (s == NULL) {
			freeInstruction(be->mb, q);
			return NULL;
		}

		s->key = 0;
		s->nrcols = 3;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

list *
rel_topn_prepare_pp(backend *be, sql_rel *rel, stmt *all)
{
	sql_rel *l = rel->l;

	/* subslice vs firstn and heapn.topn cases */
	/* (g,c, topn (bat+sink)) := subslice (b, offset, limit); # ro/rl are inout
	 *
	 * b1 := project(c,b); # for each attribute
	 * r1 := projection(g, b1); # r1 is inout
	 *
	 * or
	 * (s,d,i,hp) := (heapn.new ()/heapn.topn(n, attr, min,..)
	 * b1 := heapn.projection(s,d,i,v)
	 * (2phases)
	 * and do the steps for order by after the pipeline
	 */
	if (!is_simple_project(l->op)) {
		/*
		while (l && is_select(l->op))
			l = l->l;
			*/
		if (l && (!is_project(l->op) && !is_base(l->op)))
			rel->l = l = rel_project(be->mvc->sa, l, rel_projections(be->mvc, l, NULL, 1, 1));
	}
	if (l && ((is_simple_project(l->op) && list_empty(l->r)) || (!is_simple_project(l->op) && is_project(l->op)) || is_base(l->op)) && !list_empty(l->exps)) {
		list *projectresults = sa_list(be->mvc->sa);

		/* bat for topn sink */
		InstrPtr q = newStmt(be->mb, batRef, newRef);
		setVarType(be->mb, getArg(q, 0), newBatType(TYPE_oid));
		q = pushType(be->mb, q, TYPE_oid);
		append(projectresults, q->argv);
		pushInstruction(be->mb, q);

		for( node *n = l->exps->h; n; n = n->next ) {
			sql_exp *e = n->data;
			sql_subtype *t = exp_subtype(e);
			int tt = t->type->localtype;
			InstrPtr q = newStmt(be->mb, batRef, newRef);

			if (q == NULL)
				return NULL;
			setVarType(be->mb, getArg(q, 0), newBatType(tt));
			q = pushType(be->mb, q, tt);
			pushInstruction(be->mb, q);
			append(projectresults, q->argv);
		}
		return projectresults;
	} else if (l && is_simple_project(l->op) && !list_empty(l->r)) {
		list *projectresults = sa_list(be->mvc->sa);

		/* heap for topn sink */
		list *obexps = l->r;
		node *n = obexps->h;
		bool grouped = rel->grouped && has_partitioning(obexps);
		InstrPtr q = newStmt(be->mb, getName("heapn"), newRef);
		setVarType(be->mb, getArg(q, 0), newBatType(TYPE_oid));
		q = pushArgument(be->mb, q, all->nr);
		q = pushBit(be->mb, q, grouped);

		if (grouped) { /* create hash tables */
			BUN est = get_rel_count(l);
			lng estimate, card = 1;
			if (est == BUN_NONE || (ulng) est > (ulng) GDK_lng_max) {
				estimate = 85000000;
			} else {
				estimate = (lng) est;
			}
			int curhash = 0;
			for(; n; n = n->next ) {
				sql_exp *e = n->data;
				if (!is_partitioning(e))
					break;
				sql_subtype *t = exp_subtype(e);
				lng ncard = exp_getcard(be->mvc, rel, e);
				card *= ncard;
				if (card > estimate || ncard >= estimate)
					card = estimate;

				assert(card >= 0);
				if (card > INT_MAX)
					card = INT_MAX;

				stmt *s = stmt_oahash_new(be, t, card, curhash, 0);
				if (s == NULL)
					return NULL;
				curhash = e->shared = s->nr; /* pass hash table statment via expression */
			}
		}
		for(; n; n = n->next ) {
			sql_exp *e = n->data;
			sql_subtype *t = exp_subtype(e);
			int tt = t->type->localtype;
			q = pushNil(be->mb, q, tt);
			q = pushBit(be->mb, q, !is_ascending(e));
			q = pushBit(be->mb, q, nulls_last(e));

			if (q == NULL)
				return NULL;
		}
		pushInstruction(be->mb, q);
		append(projectresults, q->argv);
		for( node *n = l->exps->h; n; n = n->next ) {
			sql_exp *e = n->data;
			sql_subtype *t = exp_subtype(e);
			int tt = t->type->localtype;
			InstrPtr q = newStmt(be->mb, batRef, newRef);

			if (q == NULL)
				return NULL;
			setVarType(be->mb, getArg(q, 0), newBatType(tt));
			q = pushType(be->mb, q, tt);
			q = pushArgument(be->mb, q, all->nr);
			pushInstruction(be->mb, q);
			append(projectresults, q->argv);
		}
		for( node *n = obexps->h; n; n = n->next ) {
			sql_exp *e = n->data;
			sql_subtype *t = exp_subtype(e);
			int tt = t->type->localtype;
			InstrPtr q = newStmt(be->mb, batRef, newRef);

			if (q == NULL)
				return NULL;
			setVarType(be->mb, getArg(q, 0), newBatType(tt));
			q = pushType(be->mb, q, tt);
			q = pushArgument(be->mb, q, all->nr);
			pushInstruction(be->mb, q);
			append(projectresults, q->argv);
		}
		return projectresults;
	}
	return NULL;
}

stmt *
rel_pp_topn(backend *be, list *projectresults, stmt *sub, stmt *pp, stmt *o, stmt *l)
{
	node *n, *m = projectresults->h;
	(void)stmt_pp_jump(be, pp, be->nrparts);

	assert(pp);
	list *newl = sa_list(be->mvc->sa);
	list *cols = sub->op4.lval;
	if (list_length(cols) != list_length(projectresults)-1)
		return NULL;
	stmt *sc = cols->h->data;
	stmt *limit = stmt_limit_partitioned(be, sc, NULL, NULL, o, l);
	int resid = *(int*)m->data;
	limit->q->argv[2] = resid; /* shared topn */
	stmt *glimit = stmt_result(be, limit, 0);
	limit = stmt_result(be, limit, 1);

	for (n = cols->h, m = m->next; n && m; n = n->next, m = m->next) {
		stmt *sc = n->data;
		resid = *(int*)m->data;
		const char *cname = column_name(be->mvc->sa, sc);
		const char *tname = table_name(be->mvc->sa, sc);
		int label = sc->label;

		sc = column(be, sc);
		sc = stmt_project(be, limit, sc);
		sc = stmt_project(be, glimit, sc);
		sc->q->inout = 0;
		sc->q = pushArgument(be->mb, sc->q, be->pipeline);
		sc->nr = sc->q->argv[0] = resid; /* use shared result */
		list_append(newl, stmt_alias(be, sc, label, tname, cname));
	}
	sub = stmt_list(be, newl);

	(void)stmt_pp_end(be, pp);
	return sub;
}

stmt *
rel2bin_ordered_topn(backend *be, sql_rel *rel, list *refs, sql_rel *topn, stmt *all, stmt *offset, list *projectresults)
{
	mvc *sql = be->mvc;
	stmt *sub = NULL, *psub = NULL;
	list *pl, *oexps = rel->r, *osl = NULL;
	node *en, *n, *prl = projectresults->h;
	int gids = 0;

	if (rel->l) { /* first construct the sub relation */
		sql_rel *l = rel->l;
		if (l->op == op_ddl) {
			assert(0);
			sql_table *t = rel_ddl_table_get(l);

			if (t)
				sub = rel2bin_sql_table(be, t, rel->exps);
		} else {
			sub = subrel_bin(be, rel->l, refs);
		}
		sub = subrel_project(be, sub, refs, rel->l);
		if (!sub)
			return NULL;
	}
	stmt *pp = get_pipeline(be);
	assert(pp);

	pl = sa_list(sql->sa);
	if (sub)
		pl->expected_cnt = list_length(sub->op4.lval);
	psub = stmt_list(be, pl);
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *exp = en->data;
		stmt *s = exp_bin(be, exp, sub, NULL /*psub*/, NULL, NULL, NULL, NULL, 0, 0, 0);

		if (!s) /* try with own projection as well, but first clean leftover statements */
			s = exp_bin(be, exp, sub, psub, NULL, NULL, NULL, NULL, 0, 0, 0);
		if (!s) /* error */
			return NULL;
		/* single value with limit */
		if (topn && rel->r && sub && sub->nrcols == 0 && s->nrcols == 0)
			s = const_column(be, s);
		else if (sub && sub->nrcols >= 1 && s->nrcols == 0)
			s = stmt_const(be, bin_find_smallest_column(be, sub), s);

		if (!exp_name(exp))
			exp_label(sql->sa, exp, ++sql->label);
		s = stmt_rename(be, exp, s);
		column_name(sql->sa, s); /* save column name */
		list_append(pl, s);
	}
	stmt_set_nrcols(psub);

	bool grouped = false;
	int heap = 0;
	if (topn && rel->r) {
		n=oexps->h;
		grouped = topn->grouped && has_partitioning(oexps);
		if (grouped) { /* first group by */
			/* first n order by expressions are the partitioning expressions */
			stmt *s = NULL;
			for (; n; n = n->next) {
				sql_exp *gbe = n->data;
				stmt *ht = stmt_none(be);
				ht->nr = gbe->shared;
				ht->op4.typeval = *exp_subtype(gbe);

				if (!is_partitioning(gbe))
					break;
				/* build hash ! */
				stmt *key = exp_bin(be, gbe, sub, psub, NULL, NULL, NULL, NULL, 0, 0, 0);
				key = column(be, key);

				s = stmt_oahash_build_ht(be, ht, key, s, pp);
				if (s == NULL) return NULL;
				gids = s->nr;
			}
		}

		list *npl = sa_list(sql->sa);
		/* distinct, topn returns atleast N (unique groups) */

		InstrPtr q = newStmt(be->mb, getName("heapn"), getName("topn"));
		int oidbat = newBatType(TYPE_oid);
		getArg(q, 0) = newTmpVariable(be->mb, oidbat); /* pos bat */
		q = pushReturn(be->mb, q, newTmpVariable(be->mb, oidbat)); /* sel bat */
		node *on = n;
		list *nosl = sa_list(sql->sa);
		int i = 2;
		if (grouped) {
			q = pushReturn(be->mb, q, newTmpVariable(be->mb, oidbat)); /* group result bat */
			sql_subtype *t = sql_fetch_localtype(TYPE_oid);
			stmt *s = stmt_blackbox_result(be, q, i++, t);
			append(nosl, s);
		}
		for (; n; n = n->next, i++) {
			sql_exp *orderbycole = n->data;
			sql_subtype *t = exp_subtype(orderbycole);
			int resbat = newBatType(t->type->localtype);
			q = pushReturn(be->mb, q, newTmpVariable(be->mb, resbat));
			stmt *s = stmt_blackbox_result(be, q, i, t);
			append(nosl, s);
			append(nosl, orderbycole);
		}
		n = on;
		q = pushReturn(be->mb, q, heap = newTmpVariable(be->mb, oidbat)); /* heapn bat */
		q->inout = 2;
		q = pushArgument(be->mb, q, all->nr);
		q = pushArgument(be->mb, q, getArg(pp->q, 2));

		if (grouped)
			q = pushArgument(be->mb, q, gids);

		/* handle case without order by cols */
		osl = sa_list(sql->sa);
		for (; n; n = n->next) {
			sql_exp *orderbycole = n->data;
 			int last = (n->next == NULL);

			stmt *orderbycolstmt = exp_bin(be, orderbycole, sub, psub, NULL, NULL, NULL, NULL, 0, 0, 0);

			if (!orderbycolstmt)
				return NULL;

			/* handle constants */
			if (orderbycolstmt->nrcols == 0 && !last) /* no need to sort on constant */
				continue;
			orderbycolstmt = column(be, orderbycolstmt);
			q = pushArgument(be->mb, q, orderbycolstmt->nr);
			q = pushBit(be->mb, q, !is_ascending(orderbycole));
			q = pushBit(be->mb, q, nulls_last(orderbycole));
			append(osl, orderbycolstmt);
			append(osl, orderbycole);
		}
		pushInstruction(be->mb, q);
		int pos = getArg(q, 0);
		int sel = getArg(q, 1);

		/* heapn.projections */
		for (n=pl->h ; n; n = n->next)
			list_append(npl, stmt_heapn_projection(be, pos, sel, column(be, n->data)));
		psub = stmt_list(be, npl);

		osl = nosl;
	}
	/* now phase 2 */
	pp = get_pipeline(be);
	(void)stmt_pp_jump(be, pp, be->nrparts);

	if (grouped) {
		/* o := heapn.order(hp) */
		/* project(o, b); */
		stmt *o = stmt_heapn_order(be, heap);
		n = osl->h;
		stmt *s = n->data = stmt_project(be, o, n->data);
		getModuleId(s->q) = putName("heapn");
		getFunctionId(s->q) = putName("groups");
		n = n->next;
		for (  ; n; n = n->next->next) {
			n->data = stmt_project(be, o, n->data);
		}
		for (n = psub->op4.lval->h  ; n; n = n->next) {
			n->data = stmt_project(be, o, n->data);
		}
	}

	/* what todo with the group ids */
	InstrPtr q = newStmt(be->mb, getName("heapn"), getName("topn"));
	int oidbat = newBatType(TYPE_oid);
	getArg(q, 0) = newTmpVariable(be->mb, oidbat);	/* pos bat */
	q = pushReturn(be->mb, q, newTmpVariable(be->mb, oidbat)); /* sel bat */
	n = osl->h;
	if (grouped) {
		n = n->next;
		q = pushReturn(be->mb, q, newTmpVariable(be->mb, oidbat)); /* group bat */
	}
	for (  ; n; n = n->next->next) {
		sql_exp *orderbycole = n->next->data;
		sql_subtype *t = exp_subtype(orderbycole);
		int resbat = newBatType(t->type->localtype);
		q = pushReturn(be->mb, q, newTmpVariable(be->mb, resbat));
	}
	heap = *(int*)prl->data;
	q = pushReturn(be->mb, q, heap);
	q->inout = 2;
	q = pushArgument(be->mb, q, all->nr);
	q = pushArgument(be->mb, q, getArg(pp->q, 2));
	n = osl->h;
	if (grouped) {
		stmt *s = n->data;
		q = pushArgument(be->mb, q, s->nr);
		n = n->next;
	}
	for ( ; n; n = n->next->next) {
		stmt *s = n->data;
		sql_exp *e = n->next->data;
		q = pushArgument(be->mb, q, s->nr);
		q = pushBit(be->mb, q, !is_ascending(e));
		q = pushBit(be->mb, q, nulls_last(e));
	}
	pushInstruction(be->mb, q);
	int pos = getArg(q, 0);
	int sel = getArg(q, 1);

	/* heapn.projections */
	pl = psub->op4.lval;
	list *npl = sa_list(sql->sa);
	prl = prl->next; /* skip heap */
	for (n=pl->h ; n; n = n->next, prl = prl->next) {
		stmt *s = stmt_heapn_projection(be, pos, sel, n->data);
		s->nr = getArg(s->q, 0) = *(int*)prl->data;
		list_append(npl, s);
	}
	psub = stmt_list(be, npl);

	(void)stmt_pp_end(be, pp);

	if (heap) {
		/* o := heapn.order(hp) */
		/* project(o, b); */
		stmt *o = stmt_heapn_order(be, heap);
		if (offset)
			o->q = pushArgument(be->mb, o->q, offset->nr);
		psub = sql_reorder(be, o, rel->exps, psub, NULL, NULL);
	}
	return psub;
}

