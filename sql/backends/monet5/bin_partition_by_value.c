/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

/*
 * This file contains the value-based partitioned version of binary algebras
 * for GROUP BY, JOIN, ORDER BY, etc.
 */

#include "monetdb_config.h"

#include "bin_partition_by_value.h"
#include "bin_partition.h"
#include "rel_bin.h"
#include "rel_exp.h"
#include "mal_builder.h"
#include "opt_prelude.h"
#include "sql_pp_statement.h"

/* Generate the stmt to compute the number of dynamic slices, e.g.
 *   X_42:int := slicer.no_slices(X_24:bat[:...]);
 * and return the MAL variable number, e.g. 42.
 */
int
pp_dynamic_slices(backend *be, stmt *sub)
{
	if (sub && sub->cand)
		sub  = subrel_project(be, sub, NULL, NULL);
	node *n = sub->op4.lval->h;
	stmt *sc = n->data;

	sc = column(be, sc);
	sc = stmt_no_slices(be, sc);
	return sc->nr;
}

/* Generate for every projection column, eg.:
 *   (X_80:bat[:str], !X_19:bat[:str]) := slicer.nth_slice(X_77:int);
 */
stmt *
rel2bin_slicer(backend *be, stmt *sub, int slicer)
{
	if (slicer == 1) {
		if (sub && sub->cand)
			sub  = subrel_project(be, sub, NULL, NULL);
		list *newl = sa_list(be->mvc->sa);
		for (node *n = sub->op4.lval->h; n; n = n->next) {
			stmt *sc = n->data;
			const char *cname = column_name(be->mvc->sa, sc);
			const char *tname = table_name(be->mvc->sa, sc);

			sc = column(be, sc);
			sc = stmt_nth_slice(be, sc, slicer);
			list_append(newl, stmt_alias(be, sc, tname, cname));
		}
		sub = stmt_list(be, newl);
	}
	return sub;
}

bool
rel_groupby_partition(backend *be, sql_rel *rel)
{
	/* For now we assume partitioning into disjoint sets which are later grouped by independent workers */
	/* So we could/need to partition on high cardinality group by results and for complex cases (ie where the second
	 * pipeline will be single step per grouped result) stddev */
	(void)be;
	bool partition = false;

	for(node *n = rel->exps->h; n && !partition; n = n->next ) {
		sql_exp *e = n->data;

		if (is_aggr(e->type)) {
			sql_subfunc *sf = e->f;

			/* for now only on complex aggregation */
			if (!(strcmp(sf->func->base.name, "min") == 0 || strcmp(sf->func->base.name, "max") == 0 ||
			    strcmp(sf->func->base.name, "avg") == 0 || strcmp(sf->func->base.name, "count") == 0 ||
			    strcmp(sf->func->base.name, "sum") == 0 || strcmp(sf->func->base.name, "prod") == 0)) {
				partition = true;
			}
		}
	}
	/* check size */
	return partition;
}

/* part := part.new(nr_parts);
 * mat := mat.new(type:nil, nr_parts);
 */
static list *
partition_groupby_prepare(backend *be, sql_rel *rel, InstrPtr *part)
{
	/* prepare mat's for each input column */
	assert(is_groupby(rel->op) && !list_empty(rel->r) && !list_empty(rel->exps));

	int nr_parts = 256; /* later dynamic like hash size/ nr_parts pp */
	list *mats = sa_list(be->mvc->sa); /* list of ints (variable numbers* */
	*part = stmt_part_new(be, nr_parts);
	if (!*part || !mats)
		return NULL;
	sql_rel *p = rel->l;
	assert(is_project(p->op) || is_basetable(p->op));
	for(node *n = p->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_subtype *t = exp_subtype(e);
			InstrPtr q = stmt_mat_new(be, t->type->localtype, nr_parts);
			if (q == NULL)
				return NULL;
			append(mats, q->argv);
	}
	return mats;
}

/* partition (in pp) */
/* h := mkey.hash(gbc);
 * g := calc.%(h);
 * l := prefixsum(g, max);
 * p := claim(part, l);
 * ? := mat.project(m, p, b)
 */
static list *
partition_groupby_part(backend *be, sql_rel *rel, InstrPtr part, list *mats, stmt *sub)
{
	list *gbes = rel->r;
	list *res = sa_list(be->mvc->sa);
	stmt *h = NULL;
	sql_subtype *lng = sql_bind_localtype("lng");
	if (!res)
		return NULL;
	for(node *n = gbes->h; n; n = NULL) { /* first only for now */
		sql_exp *e = n->data;
		stmt *gbcol = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);

		if (!gbcol) {
			assert(be->mvc->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
			return NULL;
		}
		if (!gbcol->nrcols)
			gbcol = stmt_const(be, bin_find_smallest_column(be, sub), gbcol);

		sql_subtype *t = exp_subtype(e);
		sql_subfunc *hf = sql_bind_func_result(be->mvc, "sys", "hash", F_FUNC, true, lng, 1, &t->type);
		assert(hf);
		stmt *hn = stmt_unop(be, gbcol, NULL, hf);

		if (!h)
			h = hn;
		else {
			/* h = xor rotate (h, hn) etc */
			assert(0);
		}
	}
	sql_subfunc *hf = sql_bind_func_result(be->mvc, "sys", "mod", F_FUNC, true, lng, 2, lng, lng);
	stmt *g = stmt_binop(be, h, stmt_atom_lng(be, 256-1), NULL, hf);
	InstrPtr l = newStmt(be->mb, "part", "prefixsum");
	l = pushArgument(be->mb, l, g->nr);
	l = pushLng(be->mb, l, 256);
	InstrPtr p = newStmt(be->mb, "part", "partition");
	p = pushArgument(be->mb, p, getArg(part, 0));
	p = pushArgument(be->mb, p, getArg(l, 0));
	for(node *n = mats->h, *m = sub->op4.lval->h; n && m; n = n->next, m = m->next) {
		int *mat = (int*)n->data;
		stmt *s = m->data;
		InstrPtr mp = newStmt(be->mb, "mat", "project");
		//mp = pushArgument(be->mb, mp, *mat);
		getArg(mp, 0) = *mat;
		mp = pushArgument(be->mb, mp, getArg(p, 0));
		mp = pushArgument(be->mb, mp, getArg(l, 0));
		mp = pushArgument(be->mb, mp, g->nr);
		mp = pushArgument(be->mb, mp, s->nr);
		mp->inout = 0;
		append(res, mp->argv);
	}
	return res;
}

static stmt *
partition_groupby_fetch(backend *be, stmt *sub, list *mats)
{
	/* update output variable, to be used */
	for (node *n = sub->op4.lval->h, *m = mats->h; n && m; n = n->next, m = m->next) {
		stmt *s = n->data;
		/* call mat.fetch(m, i) */
		InstrPtr mp = newStmt(be->mb, "mat", "fetch");
		mp = pushArgument(be->mb, mp, *(int*)m->data);
		mp = pushArgument(be->mb, mp, be->pp);
		s->nr = getArg(mp, 0);
	}
	return sub;
}

/*
 * mat := mat.new(type:nil, nr_parts);
 */
static list *
partition_groupby_results(backend *be, sql_rel *rel)
{
	/* prepare mat's for each result column */
	assert(is_groupby(rel->op) && !list_empty(rel->r) && !list_empty(rel->exps));

	int nr_parts = 256; /* later dynamic like hash size/ nr_parts pp */
	list *mats = sa_list(be->mvc->sa); /* list of ints (variable numbers* */
	if (!mats)
		return NULL;
	for(node *n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_subtype *t = exp_subtype(e);
			InstrPtr q = stmt_mat_new(be, t->type->localtype, nr_parts);
			if (q == NULL)
				return NULL;
			append(mats, q->argv);
	}
	return mats;
}

static stmt *
partition_groupby(backend *be, sql_rel *rel, list *mats, stmt *sub)
{
	//sql_rel *p = rel->l;
	//int is_base = (p && is_basetable(p->op));
	stmt *grp = NULL, *ext = NULL, *cnt = NULL;
	list *gbexps = sa_list(be->mvc->sa);

	/*
	 * m = mat.new();  for each result
	 * loop
	 *  d = mat.fetch(m, i);
	 *  usual group by/aggr code
	 *  m = mat.add(aggr, i)
	 * done
	 * b = mat.pack(m)
	 */

	list *results = partition_groupby_results(be, rel);
	if (!results)
		return NULL;
	stmt *pp = stmt_pp_start_nrparts(be, 256);//be->nrparts);
	set_pipeline(be, pp);
	int ppnr = be->pipeline;
	be->pipeline = 0;

	sub = partition_groupby_fetch(be, sub, mats);
	/* Keep groupby columns, sub that they can be lookup in the aggr list */
	list *exps = rel->r;

	for(node *en = exps->h; en; en = en->next ) {
		sql_exp *e = en->data;
		stmt *gbcol = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);

		if (!gbcol) {
			assert(be->mvc->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
			return NULL;
		}
		if (!gbcol->nrcols)
			gbcol = stmt_const(be, bin_find_smallest_column(be, sub), gbcol);
		stmt *groupby = stmt_group(be, gbcol, grp, ext, cnt, !en->next);

		grp = stmt_result(be, groupby, 0);
		ext = stmt_result(be, groupby, 1);
		cnt = stmt_result(be, groupby, 2);
		gbcol = stmt_alias(be, gbcol, exp_find_rel_name(e), exp_name(e));
		list_append(gbexps, gbcol);
	}
	/* now aggregate */
	list *l = sa_list(be->mvc->sa);
	list *aggrs = rel->exps;
	stmt *cursub = stmt_list(be, l);

	for(node *n = aggrs->h, *m = results->h; n && m; n = n->next, m = m->next ) {
		sql_exp *aggrexp = n->data;
		stmt *aggrstmt = NULL;
		/* fetch next part of the mat, push into the update_sub above */
		/* also keep nr */

		/* first look in the current aggr list (l) and group by column list */
		if (l && !aggrstmt && aggrexp->type == e_column)
			aggrstmt = list_find_column(be, l, aggrexp->l, aggrexp->r);
		if (gbexps && !aggrstmt && aggrexp->type == e_column) {
			aggrstmt = list_find_column(be, gbexps, aggrexp->l, aggrexp->r);
			if (aggrstmt) {
				aggrstmt = stmt_project(be, ext, aggrstmt);
				if (list_length(gbexps) == 1)
					aggrstmt->key = 1;
			}
		}

		if (!aggrstmt)
			aggrstmt = exp_bin(be, aggrexp, sub, NULL, grp, ext, cnt, NULL, 0, 0, 0);
		/* maybe the aggr uses intermediate results of this group by,
		   therefore we pass the group by columns too
		 */
		if (!aggrstmt)
			aggrstmt = exp_bin(be, aggrexp, sub, cursub, grp, ext, cnt, NULL, 0, 0, 0);
		if (!aggrstmt) {
			assert(be->mvc->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
			return NULL;
		}

		if (!aggrstmt->nrcols && ext && ext->nrcols) {
			assert(!be->pipeline);
			aggrstmt = stmt_const(be, ext, aggrstmt);
		}

		InstrPtr mp = newStmt(be->mb, "mat", "add");
		getArg(mp, 0) = *(int*)m->data;
		mp = pushArgument(be->mb, mp, aggrstmt->nr);
		mp = pushArgument(be->mb, mp, be->pp);
		mp->inout = 0;
		aggrstmt->nr = getArg(mp, 0);

		aggrstmt = stmt_rename(be, aggrexp, aggrstmt);
		list_append(l, aggrstmt);
	}
	stmt_set_nrcols(cursub);
	/*
	if (neededpp) {
		set_pipeline(be, stmt_pp_start_dynamic(be, pp_dynamic_slices(be, cursub)));
		cursub = rel2bin_slicer(be, cursub, 1);
	}
	*/
	be->pipeline = ppnr;
	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);
	/* pack */

	list *nl = sa_list(be->mvc->sa);
	for(node *n = l->h, *m = results->h; n && m; n = n->next, m = m->next ) {
		stmt *aggrstmt = n->data;

		InstrPtr mp = newStmt(be->mb, "mat", "pack");
		mp = pushArgument(be->mb, mp, *(int*)m->data);
		/* keep names from aggrstmt */
		aggrstmt = stmt_instruction(be, mp, aggrstmt);

		list_append(nl, aggrstmt);
	}
	return stmt_list(be, nl);
}

stmt *
rel2bin_groupby_partition(backend *be, sql_rel *rel, list *refs)
{
	(void)refs;

	list *mats = NULL;
	stmt *sub = NULL;

	stmt *pp = NULL;
	InstrPtr part = NULL;

	printf("partition\n");

	mats = partition_groupby_prepare(be, rel, &part);
	if (!mats)
		return NULL;
	if (!rel->spb) {
		set_need_pipeline(be);
	} else {
		pp = stmt_pp_start_nrparts(be, pp_nr_slices(rel->l));
		set_pipeline(be, pp);
	}
	if (rel->l) { /* first construct the sub relation */
		sub = subrel_bin(be, rel->l, refs);
		sub = subrel_project(be, sub, refs, rel->l);
		if (!sub)
			return NULL;
	}
	pp = get_pipeline(be);
	if (!pp) {
		(void)get_need_pipeline(be);
		set_pipeline(be, pp = stmt_pp_start_dynamic(be, pp_dynamic_slices(be, sub)));
		sub = rel2bin_slicer(be, sub, 1);
	}
	mats = partition_groupby_part(be, rel, part, mats, sub);
	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);
	if (!mats)
		return NULL;

	sub = partition_groupby(be, rel, mats, sub);
	return sub;
}

stmt *
rel_pp_groupby(backend *be, sql_rel *rel, list *gbstmts, stmt *grp, stmt *ext, stmt *cnt, stmt *cursub, stmt *pp, list *sub, bool _2phases)
{
	node *o = sub->h;
	(void)grp; (void)cnt;
	list *shared = NULL;
	assert(be->pipeline);
	if (0 && rel && !list_empty(rel->r)) { /* for group by case re-project group by cols */
		list *ngbstmts = sa_list(be->mvc->sa);
		for( node *m = gbstmts->h; m; m = m->next) {
			stmt *gstmt = m->data;

			stmt *ngstmt = list_find_column(be, cursub->op4.lval, table_name(be->mvc->sa, gstmt), column_name(be->mvc->sa, gstmt));
			if (!ngstmt)
				ngstmt = stmt_project(be, ext, gstmt);
			append(ngbstmts, ngstmt);
		}
		gbstmts = ngbstmts;
	}
	(void)stmt_pp_jump(be, pp, be->nrparts);
	/* combine concurrent results */
	if (rel && list_empty(rel->r)) { /* global case */
		assert(list_empty(gbstmts));
		list *sub = cursub->op4.lval;

		/* phase 2 */
		shared = sa_list(be->mvc->sa);
		for (node *n = rel->exps->h, *m = o, *o = sub->h; n && m && o;
				n = n->next, m = m->next, o = o->next) {
			/* min -> min, max -> max, sum -> sum, count -> sum */
			sql_exp *e = n->data;
			sql_subtype *tpe = exp_subtype(e);
			sql_subfunc *sf = e->f;
			int *v = m->data;
			stmt *i = o->data;

			if (e->type == e_column) {
				stmt *s = list_find_column(be, shared, e->l, e->r);
				assert(s);
				s = stmt_alias(be, s, exp_relname(e), exp_name(e));
				append(shared, s);
				continue;
			}

			assert(e->type == e_aggr);
			char *name = NULL;
			int avg = 0;
			if (strcmp(sf->func->base.name, "min") == 0 ||
			    strcmp(sf->func->base.name, "max") == 0 ||
			    (avg= (strcmp(sf->func->base.name, "avg") == 0)) ||
			    strcmp(sf->func->base.name, "sum") == 0 ||
			    strcmp(sf->func->base.name, "prod") == 0) {
				name = sf->func->base.name;
			} else {
				assert(strcmp(sf->func->base.name, "count") == 0);
				name = "sum";
			}
			InstrPtr q = newStmt(be->mb, getName("lockedaggr"), getName(name));
			if (avg) {
				if (!EC_APPNUM(tpe->type->eclass)) {
					m = m->next;
					q = pushReturn(be->mb, q, *(int*)m->data);
				}
				m = m->next;
				q = pushReturn(be->mb, q, *(int*)m->data);
				q->inout = 0;
			}
			q = pushArgument(be->mb, q, getArg(pp->q, 2));
			q = pushArgument(be->mb, q, i->nr);
			if (avg) {
				/* remainder and count */
				q = pushArgument(be->mb, q, getArg(i->q, 1));
				if (!EC_APPNUM(tpe->type->eclass))
					q = pushArgument(be->mb, q, getArg(i->q, 2));
			}
			pushInstruction(be->mb, q);
			getArg(q, 0) = *v;
			stmt *s = stmt_none(be);
			s->op4.typeval = *tpe;
			s->nr = *v;
			s = stmt_alias(be, s, exp_find_rel_name(e), exp_name(e));
			append(shared, s);
		}
	} else if (rel && !list_empty(rel->r)) {
		if (!_2phases) {
			(void)stmt_pp_end(be, pp);
			return cursub;
		}
		list *sub = cursub->op4.lval;

		/* phase 2 of grouping */
		list *gbexps = rel->r, *ngbstmts = sa_list(be->mvc->sa);
		stmt *grp = NULL, *ext = NULL, *cnt = NULL;
		for(node *n = gbexps->h, *m = gbstmts->h; n && m; n = n->next, m = m->next, o = o->next) {
			sql_exp *e = n->data;
			stmt *gstmt = m->data;

			//stmt *groupby = stmt_group_locked(be, gstmt, grp, ext, cnt, pp);
			stmt *groupby = be->pipeline? stmt_group_partitioned(be, gstmt, grp, ext, cnt) : stmt_group(be, gstmt, grp, ext, cnt, 0);
			/* reuse extend ! */
			getArg(groupby->q, 1) = *(int*)o->data;
			groupby->q->inout = 1;
			grp = stmt_result(be, groupby, 0);
			ext = stmt_result(be, groupby, 1);
			//cnt = stmt_result(be, groupby, 2);
			gstmt = stmt_alias(be, gstmt, exp_find_rel_name(e), exp_name(e));
			list_append(ngbstmts, gstmt);
		}
		gbstmts = ngbstmts;

		shared = sa_list(be->mvc->sa);
		for (node *n = rel->exps->h, *m = o, *o = sub->h; n && m && o;
				n = n->next, m = m->next, o = o->next) {
			/* min -> min, max -> max, sum -> sum, count -> sum */
			sql_exp *e = n->data;
			sql_subtype *tpe = exp_subtype(e);
			sql_subfunc *sf = e->f;
			int *v = m->data;
			stmt *i = o->data;
			InstrPtr q;

			if (e->type == e_aggr) {
				char *name = NULL;
				int avg = 0;
				if (strcmp(sf->func->base.name, "min") == 0 ||
					strcmp(sf->func->base.name, "max") == 0 ||
					(avg= (strcmp(sf->func->base.name, "avg") == 0)) ||
					strcmp(sf->func->base.name, "sum") == 0 ||
					strcmp(sf->func->base.name, "prod") == 0) {
					name = sf->func->base.name;
				} else {
					assert(strcmp(sf->func->base.name, "count") == 0);
					name = "sum";
				}
				q = newStmt(be->mb, getName("aggr"), getName(name));
				if (avg) {
					if (!EC_APPNUM(tpe->type->eclass)) {
						m = m->next;
						q = pushReturn(be->mb, q, *(int*)m->data);
					}
					m = m->next;
					q = pushReturn(be->mb, q, *(int*)m->data);
					q->inout = 0;
				}
				q = pushArgument(be->mb, q, grp->nr);
				q = pushArgument(be->mb, q, i->nr);
				if (avg) {
					/* remainder and count */
					q = pushArgument(be->mb, q, getArg(i->q, 1));
					if (!EC_APPNUM(tpe->type->eclass))
						q = pushArgument(be->mb, q, getArg(i->q, 2));
				}
				q = pushArgument(be->mb, q, getArg(pp->q, 2));
				q = pushArgument(be->mb, q, grp->nr);
				//q = pushArgument(be->mb, q, ext->nr);
			} else {
				q = newStmt(be->mb, getName("algebra"), projectionRef);
				q = pushArgument(be->mb, q, grp->nr);
				q = pushArgument(be->mb, q, i->nr);
				q = pushArgument(be->mb, q, getArg(pp->q, 2));
			}
			getArg(q, 0) = *v;
			q->inout = 0;
			pushInstruction(be->mb, q);
			stmt *s = stmt_none(be);
			s->op4.typeval = *exp_subtype(e);
			s->nr = *v;
			s->key = s->nrcols = 1;
			s = stmt_alias(be, s, exp_find_rel_name(e), exp_name(e));
			append(shared, s);
		}
	} else {
		return NULL;
	}
	(void)stmt_pp_end(be, pp);

	/* we combined into a bat, ie lets fetch results */
	if (shared && list_empty(rel->r)) {
		list *nshared = sa_list(be->mvc->sa);
		for(node *n = shared->h; n; n = n->next) {
			stmt *s = n->data;
			s = stmt_fetch(be, s);
			append(nshared, s);
		}
		shared = nshared;
	}
	cursub = stmt_list(be, shared);
	stmt_set_nrcols(cursub);
	return cursub;
}

