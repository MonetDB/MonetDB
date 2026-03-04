/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
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
#include "rel_rewriter.h"
#include "mal_builder.h"
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
	node *n = sub->op1 ? sub->op4.lval->t : sub->op4.lval->h; /* for hash get last */
	stmt *sc = n->data;

	sc = column(be, sc);
	sc = stmt_no_slices(be, sc, sub->op1?true:false /* hash table on op1 */);
	return sc->nr;
}

/* Generate for every projection column, eg.:
 *   (X_80:bat[:str], !X_19:bat[:str]) := slicer.nth_slice(X_77:int);
 */
static stmt *
rel2bin_slicer(backend *be, stmt *sub)
{
	if (sub && sub->cand)
		sub  = subrel_project(be, sub, NULL, NULL);
	list *newl = sa_list(be->mvc->sa);
	if (sub->partition) {
		for (node *n = sub->op4.lval->h; n; n = n->next) {
			stmt *sc = n->data;
			const char *cname = column_name(be->mvc->sa, sc);
			const char *tname = table_name(be->mvc->sa, sc);
			int label = sc->label;

			sc = column(be, sc);
			sc = stmt_nth_slice(be, sc, false);
			list_append(newl, stmt_alias(be, sc, label, tname, cname));
		}
	} else {
		for (node *n = sub->op4.lval->h; n; n = n->next) {
			stmt *sc = n->data;
			const char *cname = column_name(be->mvc->sa, sc);
			const char *tname = table_name(be->mvc->sa, sc);
			int label = sc->label;

			sc = column(be, sc);
			sc = stmt_nth_slice(be, sc, false);
			list_append(newl, stmt_alias(be, sc, label, tname, cname));
		}
	}
	sub = stmt_list(be, newl);
	return sub;
}

int
mat_nr_parts(backend *be, int m)
{
	InstrPtr mp = newStmt(be->mb, "mat", "nr_parts");
	mp = pushArgument(be->mb, mp, m);
	mp = pushInt(be->mb, mp, 100000);
	pushInstruction(be->mb, mp);
	return getArg(mp, 0);
}

InstrPtr
mat_counters_get(backend *be, stmt *mat, int seqnr)
{
	InstrPtr mp = newStmt(be->mb, "mat", "counters_get");
	if (!mp)
		return NULL;
	mp = pushReturn(be->mb, mp, newTmpVariable(be->mb, TYPE_int));
	mp = pushArgument(be->mb, mp, mat->nr);
	mp = pushArgument(be->mb, mp, seqnr);
	pushInstruction(be->mb, mp);
	return mp;
}

stmt *
mats_fetch_slices(backend *be, stmt *mats, int mid, int sid)
{
	list *nmats = sa_list(be->mvc->sa); /* list of ints (variable numbers* */
	for(node *n = mats->op4.lval->h; n; n = n->next) {
		stmt *mat = n->data;
		InstrPtr mp = newStmt(be->mb, "mat", "fetch");
		mp = pushArgument(be->mb, mp, mat->nr);
		mp = pushArgument(be->mb, mp, mid);
		mp = pushArgument(be->mb, mp, sid);
		pushInstruction(be->mb, mp);
		stmt *n = stmt_blackbox_result(be, mp, 0, tail_type(mat));
		n = stmt_alias(be, n, mat->label, mat->tname, mat->cname);
		append(nmats, n);
	}
	return stmt_list(be, nmats);
}

stmt *
rel2bin_slicer_pp(backend *be, stmt *sub)
{
	(void)get_need_pipeline(be);
	int source = 0;
	if (sub->partition) {
		stmt *mat = sub->op4.lval->h->data;
		int nrparts = mat_nr_parts(be, mat->nr);
		source = pp_counter(be, 0, nrparts, false);
	} else {
		source = pp_counter(be, -1, pp_dynamic_slices(be, sub), false);
	}
	if (be->pp) {
		stmt_concat_add_source(be);
	} else {
		set_pipeline(be, stmt_pp_start_generator(be, source, true));
	}
	int seqnr = pp_counter_get(be, source);
	if (sub->partition) {
		stmt *mat = sub->op4.lval->h->data;
		InstrPtr ctr = mat_counters_get(be, mat, seqnr);
		return mats_fetch_slices(be, sub, getArg(ctr, 0), getArg(ctr, 1));
	} else {
		return rel2bin_slicer(be, sub);
	}
}

bool
rel_groupby_partition(sql_rel *rel)
{
	bool partition = true;

	for(node *n = rel->exps->h; n && partition; n = n->next ) {
		sql_exp *e = n->data;

		if (is_aggr(e->type)) {
			sql_subfunc *sf = e->f;

			/* for now only on complex aggregation */
			if (!(strcmp(sf->func->base.name, "min") == 0 || strcmp(sf->func->base.name, "max") == 0 ||
			    strcmp(sf->func->base.name, "avg") == 0 || strcmp(sf->func->base.name, "count") == 0 ||
			    strcmp(sf->func->base.name, "sum") == 0 || strcmp(sf->func->base.name, "prod") == 0)) {
				partition = false;
			}
		}
	}
	if (!partition)
		return false;
	if (list_empty(rel->r))
		return false;
	/* check size */
	BUN est = get_rel_count(rel);
	if (est == BUN_NONE)
		return false;
	if (est >= GDKL3_size)
		return true;
	return false;
}

#define PARTITION_NRPARTS 256
/* part := part.new(nr_parts);
 * mat := mat.new(type:nil, nr_parts);
 */
static list *
partition_groupby_prepare(backend *be, sql_rel *rel, InstrPtr *part)
{
	/* prepare mat's for each input column */
	assert(((is_groupby(rel->op) && !list_empty(rel->r)) || rel->op == op_partition) && !list_empty(rel->exps));

	int nr_parts = PARTITION_NRPARTS; /* later dynamic like hash size/ nr_parts pp */
	list *mats = sa_list(be->mvc->sa); /* list of ints (variable numbers* */
	*part = stmt_part_new(be, nr_parts);
	if (!*part || !mats)
		return NULL;
	sql_rel *p = rel;
	if (rel->op == op_groupby)
		p = rel->l;
	if (!is_project(p->op) && !is_basetable(p->op)) {
		rel->l = p = rel_project(be->mvc->sa, p,
			rel_projections(be->mvc, p, NULL, 1, 1));
	}
	assert(is_project(p->op) || is_basetable(p->op));
	for(node *n = p->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_subtype *t = exp_subtype(e);
			stmt *mat = stmt_mat_new(be, t, nr_parts);
			if (mat == NULL)
				return NULL;
			append(mats, mat);
	}
	return mats;
}

/* partition (in pp) */
/* h := mkey.hash(gbc);
 * g := calc.and(h); // and as module can be negative
 * l := prefixsum(g, max);
 * p := claim(part, l);
 * ? := mat.project(m, p, b)
 */
static list *
partition_groupby_part(backend *be, sql_rel *rel, InstrPtr part, list *mats, stmt *sub)
{
	list *gbes = is_groupby(rel->op)?rel->r:rel->attr;
	list *res = sa_list(be->mvc->sa);
	stmt *h = NULL;
	sql_subtype *lng = sql_fetch_localtype(TYPE_lng);
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
	sql_subfunc *hf = sql_bind_func_result(be->mvc, "sys", "bit_and", F_FUNC, true, lng, 2, lng, lng);
	stmt *g = stmt_binop(be, h, stmt_atom_lng(be, PARTITION_NRPARTS-1), NULL, hf);
	InstrPtr l = newStmt(be->mb, "part", "prefixsum");
	l = pushArgument(be->mb, l, g->nr);
	l = pushLng(be->mb, l, PARTITION_NRPARTS);
	pushInstruction(be->mb, l);
	InstrPtr p = newStmt(be->mb, "part", "partition");
	p = pushArgument(be->mb, p, getArg(part, 0));
	p = pushArgument(be->mb, p, getArg(l, 0));
	pushInstruction(be->mb, p);
	for(node *n = mats->h, *m = sub->op4.lval->h; n && m; n = n->next, m = m->next) {
		stmt *mat = n->data;
		stmt *s = m->data;
		InstrPtr mp = newStmt(be->mb, "mat", "project");
		getArg(mp, 0) = mat->nr;
		mp = pushArgument(be->mb, mp, getArg(p, 0));
		mp = pushArgument(be->mb, mp, getArg(l, 0));
		mp = pushArgument(be->mb, mp, g->nr);
		mp = pushArgument(be->mb, mp, s->nr);
		mp->inout = 0;
		pushInstruction(be->mb, mp);
		append(res, mp->argv);
	}
	return res;
}

static stmt *
partition_groupby_fetch(backend *be, stmt *sub)
{
	/* update output variable, to be used */
	for (node *n = sub->op4.lval->h; n; n = n->next) {
		stmt *mat = n->data;
		/* call mat.fetch(m, i) */
		InstrPtr mp = newStmt(be->mb, "mat", "fetch");
		mp = pushArgument(be->mb, mp, mat->nr);
		mp = pushArgument(be->mb, mp, be->pp);
		pushInstruction(be->mb, mp);
		mat->nr = getArg(mp, 0);
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

	int nr_parts = PARTITION_NRPARTS; /* later dynamic like hash size/ nr_parts pp */
	list *mats = sa_list(be->mvc->sa); /* list of ints (variable numbers* */
	if (!mats)
		return NULL;
	for(node *n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_subtype *t = exp_subtype(e);
			stmt *mat = stmt_mat_new(be, t, nr_parts);
			if (mat == NULL)
				return NULL;
			append(mats, mat);
	}
	return mats;
}

static stmt *
partition_groupby(backend *be, sql_rel *rel, stmt *mats, bool neededpp)
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
	 * if neededpp -> start new pipeline using mats
	 * else
	 * b = mat.pack(m)
	 */

	list *results = partition_groupby_results(be, rel);
	if (!results)
		return NULL;
	int source = pp_counter(be, PARTITION_NRPARTS, -1, false);
	stmt *pp = stmt_pp_start_generator(be, source, true);
	set_pipeline(be, pp);
	(void)pp_counter_get(be, source);

	int ppnr = be->pipeline;
	be->pipeline = 0;

	stmt *sub = partition_groupby_fetch(be, mats);
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
		gbcol = stmt_alias(be, gbcol, gbcol->label, exp_find_rel_name(e), exp_name(e));
		list_append(gbexps, gbcol);
	}
	/* now aggregate */
	list *l = sa_list(be->mvc->sa);
	list *aggrs = rel->exps;
	stmt *cursub = stmt_list(be, l);

	for(node *n = aggrs->h, *m = results->h; n && m; n = n->next, m = m->next ) {
		sql_exp *aggrexp = n->data;
		stmt *aggrstmt = NULL;
		stmt *mat = m->data;
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
		getArg(mp, 0) = mat->nr;
		mp = pushArgument(be->mb, mp, aggrstmt->nr);
		mp = pushArgument(be->mb, mp, be->pp);
		mp->inout = 0;
		pushInstruction(be->mb, mp);
		aggrstmt->nr = getArg(mp, 0);

		aggrstmt = stmt_rename(be, aggrexp, aggrstmt);
		list_append(l, aggrstmt);
	}
	stmt_set_nrcols(cursub);
	be->pipeline = ppnr;
	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);
	/* pack */

	if (!neededpp) {
		list *nl = sa_list(be->mvc->sa);
		for(node *n = l->h, *m = results->h; n && m; n = n->next, m = m->next ) {
			stmt *aggrstmt = n->data;
			stmt *mat = m->data;

			InstrPtr mp = newStmt(be->mb, "mat", "pack");
			mp = pushArgument(be->mb, mp, mat->nr);
			pushInstruction(be->mb, mp);
			/* keep names from aggrstmt */
			aggrstmt = stmt_instruction(be, mp, aggrstmt);

			list_append(nl, aggrstmt);
		}
		return stmt_list(be, nl);
	} else {
		stmt *pp = stmt_pp_start_nrparts(be, PARTITION_NRPARTS);
		set_pipeline(be, pp);
		int ppnr = be->pipeline;
		be->pipeline = 0;

		list *nl = sa_list(be->mvc->sa);
		for(node *n = l->h, *m = results->h; n && m; n = n->next, m = m->next ) {
			stmt *aggrstmt = n->data;
			stmt *mat = m->data;

			InstrPtr mp = newStmt(be->mb, "mat", "fetch");
			mp = pushArgument(be->mb, mp, mat->nr);
			mp = pushArgument(be->mb, mp, be->pp);
			pushInstruction(be->mb, mp);
			/* keep names from aggrstmt */
			aggrstmt = stmt_instruction(be, mp, aggrstmt);

			list_append(nl, aggrstmt);
		}
		be->pipeline = ppnr;
		return stmt_list(be, nl);
	}
}

stmt *
rel2bin_partition(backend *be, sql_rel *rel, list *refs)
{
	sql_rel *inner = rel->l, *part_rel = rel;

	if (inner && inner->op == op_partition)
		part_rel = inner;

	(void)refs;

	list *mats = NULL;
	stmt *sub = NULL;

	stmt *pp = NULL;
	InstrPtr part = NULL;

	mats = partition_groupby_prepare(be, part_rel, &part);
	if (!mats)
		return NULL;
	if (!rel->spb && !be->need_pipeline) {
		set_need_pipeline(be);
	} else {
		pp = stmt_pp_start_nrparts(be, pp_nr_slices(part_rel->l));
		set_pipeline(be, pp);
	}
	if (rel->l) { /* first construct the sub relation */
		sub = subrel_bin(be, part_rel->l, refs);
		sub = subrel_project(be, sub, refs, part_rel->l);
		if (!sub)
			return NULL;
	}
	pp = get_pipeline(be);
	if (!pp) {
		sub = rel2bin_slicer_pp(be, sub);
		pp = get_pipeline(be);
	}
	mats = partition_groupby_part(be, part_rel, part, mats, sub);
	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);
	if (!mats)
		return NULL;
	for (node *n = sub->op4.lval->h, *m = mats->h; n && m; n = n->next, m = m->next) {
		stmt *s = n->data;
		int *mat = m->data;
		s->nr = *mat;
	}
	return sub;
}

stmt *
rel2bin_groupby_partition(backend *be, sql_rel *rel, list *refs, bool neededpp)
{
	stmt *mats = rel2bin_partition(be, rel, refs);
	return partition_groupby(be, rel, mats, neededpp);
}

