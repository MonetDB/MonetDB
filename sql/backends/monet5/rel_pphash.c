/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "mal_instruction.h"
#include "rel_bin.h"
#include "rel_exp.h"
#include "rel_pphash.h"
#include "rel_rewriter.h"
#include "sql_pp_statement.h"
#include "bin_partition.h"
#include "bin_partition_by_value.h"
#include "mal_builder.h"

#if 0
static lng
get_max_bt_count(mvc *sql, sql_rel *rel, lng max)
{
	lng cur_max = max;

	if (rel->op == op_basetable) {
		sql_table *t = rel->l;
		if (t && isTable(t)) {
			sqlstore *store = sql->session->tr->store;
			lng nr =  (lng)store->storage_api.count_col(sql->session->tr, ol_first_node(t->columns)->data, 0);
			assert(nr >= 0);
			return nr > max? nr : max;
		}
	} else {
		if (rel->l)
			cur_max = get_max_bt_count(sql, rel->l, cur_max);
		if (rel->r)
			cur_max = get_max_bt_count(sql, rel->r, cur_max);
	}
	return cur_max;
}
#endif

static lng
_estimate(mvc *sql, sql_rel *rel)
{
	(void)sql;
	// TODO better estimation
	//lng est = get_max_bt_count(sql, rel, 0);
	lng est = get_rel_count(rel);

	if (est >= 85000000) {
		printf("#using large fallback\n");
		fflush(stdout);
		//est = 85000000;
		est = 8500;
	}
	/*
	if (est == 0) {
		if (rel->op != op_basetable) {
			printf("#est == 0, %d\n", rel->op);
			fflush(stdout);
		}
	}
	*/
	return est;
}

static stmt *
_start_pp(backend *be, sql_rel *rel, bit buildphase, list *refs, bool spb)
{
	stmt *sub = NULL, *pp = NULL;

	if (buildphase && get_pipeline(be)) {
        sql_error(be->mvc, 10, SQLSTATE(42000) "Internal error: hash-join cannot start within a pipelines block");
		return NULL;
	}
	if (1 || !spb || pp_can_not_start(be->mvc, rel)) {
		assert (!be->need_pipeline);
		set_need_pipeline(be);
	} else {
		int nr_parts = pp_nr_slices(rel);
		int source = pp_counter(be, nr_parts, -1);
		set_pipeline(be, stmt_pp_start_generator(be, source, true));
	}

	/* first construct the sub-relation */
	sub = subrel_bin(be, rel, refs);
	sub = subrel_project(be, sub, refs, rel);
	if (sub) {
		pp = get_pipeline(be);
		if (!pp) {
			int source = pp_counter(be, -1, pp_dynamic_slices(be, sub));
			pp = stmt_pp_start_generator(be, source, true);
			set_pipeline(be, pp);
			sub = rel2bin_slicer(be, sub, 1);
		}
	}

	(void)get_need_pipeline(be);
	return sub;
}

/* exps: hash-side of cmp exps or prj exps (e.g. in case of cross-product).
 */
static list *
oahash_prepare_bld_ht(backend *be, const list *exps, lng sz)
{
	assert(exps && exps->cnt);

	list *shared_ht = sa_list(be->mvc->sa);
	int curhash = 0;
	bit freq = false;
	for (node *n = exps->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);
		freq = (n->next == NULL); /* last ht also computes frequencies */
		InstrPtr q = stmt_oahash_new(be, t->type->localtype, sz, freq, curhash);
		if (q == NULL) return NULL;
		q->inout = 0;
		curhash = getArg(q, 0);
		append(shared_ht, q);
	}
	assert(shared_ht->cnt == exps->cnt);
	return shared_ht;
}

static list *
oahash_prepare_bld_hp(backend *be, const list *exps_prj_hsh, int prnt, lng sz)
{
	list *shared_hp = sa_list(be->mvc->sa);
	int previous = prnt;
	for (node *n = exps_prj_hsh->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);
		InstrPtr q = stmt_oahash_new_payload(be, t->type->localtype, sz, prnt, previous);
		if (q == NULL) return NULL;
		q->inout = 0;
		previous = getArg(q, 0);
		append(shared_hp, q);
	}
	assert(shared_hp->cnt == exps_prj_hsh->cnt);
	return shared_hp;
}

/* exps: hash-side of cmp exps or prj exps (e.g. in case of cross-product).
 */
static stmt *
oahash_build_ht(backend *be, int *slt_ids, const list *exps, const list *shared_ht, stmt *sub, const stmt *pp)
{
	list *l = sa_list(be->mvc->sa);

	int prev_ht = 0;
	for (node *n = exps->h, *inout = shared_ht->h; n && inout; n = n->next, inout = inout->next) {
		stmt *key = exp_bin(be, n->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		key = column(be, key);

		InstrPtr q = NULL;
		if (prev_ht == 0) {
			q = stmt_oahash_build_ht(be, getArg((InstrPtr)inout->data,0), key, pp);
		} else {
			q = stmt_oahash_build_combined_ht(be, getArg((InstrPtr)inout->data,0), key, *slt_ids, prev_ht, pp);
		}
		if (q == NULL) return NULL;

		sql_exp *e = n->data;
		stmt *s = stmt_none(be);
		if (s == NULL) return NULL;
		s->op4.typeval = *exp_subtype(e);
		s->nr = getArg(q, 1);
		s->nrcols = key->nrcols;
		s->q = q;
		if (e->alias.label)
			s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
		append(l, s);

		*slt_ids = getArg(q,0);
		prev_ht = getArg(q, 1);
	}
	return stmt_list(be, l);
}

static InstrPtr
oahash_build_freq(backend *be, stmt *ht, int slt_ids, int compute_pos, const stmt *pp)
{
	InstrPtr q = newStmt(be->mb, putName("oahash"), putName("compute_frequencies"));
	if (q == NULL)
		return NULL;
	if (!compute_pos) {
		/* No payload at the hash-side, hence no need to compute payload_pos.
		 * The oahash.expand() at the probe-side only needs the frequencies.
		 */
		int tt = tail_type(ht)->type->localtype;
		setVarType(be->mb, getArg(q, 0), newBatType(tt));
		getArg(q, 0) = ht->nr;
		q->inout = 0;
	} else {
		setVarType(be->mb, getArg(q, 0), newBatType(TYPE_oid));
		q = pushReturn(be->mb, q, ht->nr);
		q->inout = 1;
	}
	q = pushArgument(be->mb, q, slt_ids);
	q = pushArgument(be->mb, q, getArg(pp->q, 2) /* pipeline ptr*/);
	pushInstruction(be->mb, q);
	return q;
}

static stmt *
oahash_build_hp(backend *be, list *exps_prj_hsh, list *shared_hp, int pld_pos, stmt *sub, const stmt *pp)
{
	list *l = sa_list(be->mvc->sa);

	for (node *n = exps_prj_hsh->h, *inout = shared_hp->h; n && inout; n = n->next, inout = inout->next) {
		stmt *payload = exp_bin(be, n->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(payload); /* must find */
		payload = column(be, payload);

		/* pld_pos only exists when exps_prj_hsh->cnt > 0, which is True within
		 * this for loop */
		InstrPtr q = stmt_oahash_add_payload(be, getArg((InstrPtr)inout->data,0), payload, pld_pos, pp);
		if (q == NULL) return NULL;

		sql_exp *e = n->data;
		stmt *s = stmt_none(be);
		if (s == NULL) return NULL;
		s->op4.typeval = *exp_subtype(e);
		s->nr = getArg(q, 0);
		s->nrcols = 1;
		s->q = q;
		if (e->alias.label)
			s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}
	return stmt_list(be, l);
}

/* Generate for every projection column, eg.:
 *   (X_80:bat[:str], !X_19:bat[:str]) := slicer.nth_slice(X_77:int);
 */
stmt *
oahash_slicer(backend *be, stmt *sub)
{
	assert(sub->op1 && !sub->cand);
	list *newl = sa_list(be->mvc->sa);
	stmt *ht = sub->op4.lval->t->data;
	stmt *slice = stmt_nth_slice(be, ht, 1, true);
	sub = sub->op1; /* need the payload */
	stmt *pp = get_pipeline(be);
	for (node *n = sub->op4.lval->h; n; n = n->next) {
		stmt *sc = n->data;
		const char *cname = column_name(be->mvc->sa, sc);
		const char *tname = table_name(be->mvc->sa, sc);
		int label = sc->label;

		sc = column(be, sc);
		sc = stmt_oahash_fetch_payload(be, sc, slice->nr, ht, NULL, false, pp, tail_type(sc));
		list_append(newl, stmt_alias(be, sc, label, tname, cname));
	}
	sub = stmt_list(be, newl);
	return sub;
}

/* Generates the parallel block to probe the hash table
 */
static stmt *
oahash_probe(backend *be, sql_rel *rel, list *exps_cmp_prb, const stmt *stmts_ht, stmt *sub, const stmt *pp)
{
	sql_exp *e = NULL, *e2 = NULL;
	InstrPtr q = NULL;
	int matched = 0, rhs_slts = 0;
	bit single = false;

	/* stmts_ht is in the same order as the join columns */
	for (node *n = exps_cmp_prb->h, *m = stmts_ht->op4.lval->h, *o = rel->exps->h; n && m && o; n = n->next, m = m->next, o = o->next) {
		e = n->data;
		e2 = o->data;
		stmt *key = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		key = column(be, key);
		single = ((rel->single == 1) && (n->next == NULL));

		int rht = ((stmt *)m->data)->nr;
		if (!matched) {
			q = stmt_oahash_hash(be, key, pp);
			if (q == NULL) return NULL;
			q = stmt_oahash_probe(be, key, getDestVar(q), rht, single, e2->semantics, pp);
		} else {
			q = stmt_oahash_combined_hash(be, key, matched, rhs_slts, pp);
			if (q == NULL) return NULL;
			q = stmt_oahash_combined_probe(be, key, getDestVar(q), matched, rhs_slts, rht, single, e2->semantics, pp);
		}
		if (q == NULL) return NULL;
		matched = getArg(q, 0);
		rhs_slts = getArg(q, 1);
	}
	/* probe of last column is the final res, which also matches the type of the ht_sink containing the frequencies */
	stmt *s = stmt_none(be);
	if (s == NULL) return NULL;
	s->op4.typeval = *exp_subtype(e);
	s->nr = getArg(q, 0);
	s->nrcols = 1;
	s->q = q;
	return s;
}

static list *
oahash_project_hsh(backend *be, list *exps_prj_hsh, stmt *stmts_hp, int rhs_slts, const stmt *freq_sink, const stmt *norows_prb, bit outer, const stmt *pp)
{
	list *l = sa_list(be->mvc->sa);

	if (list_empty(exps_prj_hsh))
		return l;
	for (node *o = exps_prj_hsh->h; o; o = o->next) {
		stmt *hp_sink = exp_bin(be, o->data, stmts_hp, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(hp_sink); /* must find */

		sql_exp *e = o->data;
		stmt *s = stmt_oahash_fetch_payload(be, hp_sink, rhs_slts, freq_sink, norows_prb, outer, pp, exp_subtype(e));
		if (s == NULL) return NULL;

		if (e->alias.label)
			s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}
	return l;
}

static list *
oahash_project_prb(backend *be, list *exps_prj_prb, int matched, int rhs_slts, const stmt *freq_sink, bit outer, stmt *sub, const stmt *pp)
{
	list *l = sa_list(be->mvc->sa);

	for (node *o = exps_prj_prb->h; o; o = o->next) {
		stmt *key = exp_bin(be, o->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		key = column(be, key);

		InstrPtr q = stmt_oahash_expand(be, key, matched, rhs_slts, freq_sink, outer, pp);
		if (q == NULL) return NULL;

		sql_exp *e = o->data;
		stmt *s = stmt_none(be);
		if (s == NULL) return NULL;
		s->op4.typeval = *exp_subtype(e);
		s->nr = getArg(q, 0);
		s->nrcols = key->nrcols;
		s->q = q;
		if (e->alias.label)
			s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}
	return l;
}

static list *
oahash_project_single(backend *be, list *exps_prj, int selected, stmt *sub, const stmt *pp)
{
	list *l = sa_list(be->mvc->sa);

	for (node *o = exps_prj->h; o; o = o->next) {
		stmt *key = exp_bin(be, o->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		key = column(be, key);

		InstrPtr q = stmt_oahash_project(be, key, selected, pp);
		if (q == NULL) return NULL;

		sql_exp *e = o->data;
		stmt *s = stmt_none(be);
		if (s == NULL) return NULL;
		s->op4.typeval = *exp_subtype(e);
		s->nr = getArg(q, 0);
		s->nrcols = key->nrcols;
		s->q = q;
		if (e->alias.label)
			s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}
	return l;
}

static list *
oahash_project_cart(backend *be, str func, list *exps_prj, stmt *sub, stmt *norows, const stmt *pp)
{
	list *l = sa_list(be->mvc->sa);
	int tt;

	if (list_empty(exps_prj))
		return l;
	for (node *o = exps_prj->h; o; o = o->next) {
		stmt *key = exp_bin(be, o->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		key = column(be, key);

		InstrPtr q = newStmt(be->mb, putName("oahash"), putName(func));
		if (q == NULL) return NULL;
		tt = tail_type(key)->type->localtype;
		setVarType(be->mb, getArg(q, 0), newBatType(tt));
		q = pushArgument(be->mb, q, key->nr);
		q = pushArgument(be->mb, q, norows->nr);
		q = pushArgument(be->mb, q, getArg(pp->q, 2) /* pipeline ptr*/);
		pushInstruction(be->mb, q);

		sql_exp *e = o->data;
		stmt *s = stmt_none(be);
		if (s == NULL) return NULL;
		s->op4.typeval = *exp_subtype(e);
		s->nr = getArg(q, 0);
		s->nrcols = key->nrcols;
		s->q = q;
		if (e->alias.label)
			s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}
	return l;
}

stmt *
rel2bin_oahash_build(backend *be, sql_rel *rel, list *refs)
{
	/*** HASH PHASE ***/
	list *exps_cmp_hsh = rel->attr;
	list *exps_prj_hsh = rel->exps;

	if (!exps_cmp_hsh) { /* dummy case for cartisian product */
		sql_rel *l = rel->l;
		if (is_topn(l->op))
			l = rel_project(be->mvc->sa, l, rel_projections(be->mvc, l, NULL, 1, 1));
		return rel2bin_materialize(be, l, refs);
	}

	lng bld_sz = _estimate(be->mvc, rel); /* TODO: change into dynamic where possible ?? */
	list *shared_ht = oahash_prepare_bld_ht(be, exps_cmp_hsh, bld_sz);
	list *shared_hp = NULL;
	if (exps_prj_hsh)
		shared_hp = oahash_prepare_bld_hp(be, exps_prj_hsh, getArg((InstrPtr)shared_ht->t->data,0), bld_sz);

	stmt *sub = _start_pp(be, rel->l, 1, refs, rel->spb);
	if (!sub) return NULL;

	stmt *pp = get_pipeline(be);
	int slt_ids = 0;
	stmt *stmts_ht = oahash_build_ht(be, &slt_ids, exps_cmp_hsh, shared_ht, sub, pp);
	stmt *stmts_hp = NULL;
	/* freq/payload not needed for semi */
	if (rel->flag != (int)op_semi) {
		InstrPtr stmt_freq = oahash_build_freq(be, stmts_ht->op4.lval->t->data, slt_ids, exps_prj_hsh?exps_prj_hsh->cnt:0, pp);
		if (exps_prj_hsh)
			stmts_hp = oahash_build_hp(be, exps_prj_hsh, shared_hp, getArg(stmt_freq,0), sub, pp);
	}
	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);
	stmts_ht->op1 = stmts_hp;
	return stmts_ht;
}

static stmt *
rel2bin_oahash_equi(backend *be, sql_rel *rel, list *refs)
{
	sql_rel *rel_hsh = NULL, *rel_prb = NULL;

	int neededpp = (rel->spb || rel->partition) && get_need_pipeline(be); /* start new parallel block after join */
	(void)neededpp;

	/* find the hash- vs probe-side */
	if (rel->op == op_full) {
        sql_error(be->mvc, 10, SQLSTATE(42000) "rel2bin_oahash(): full outer-join not supported yet");
		return NULL;
	}
	if (rel->op == op_left) {
		rel_hsh = rel->r;
		rel_prb = rel->l;
	} else if (rel->op == op_right) {
		rel_hsh = rel->l;
		rel_prb = rel->r;
	} else {
		if (rel->oahash == 1) {
			rel_hsh = rel->l;
			rel_prb = rel->r;
		} else {
			assert(rel->oahash == 2);
			rel_hsh = rel->r;
			rel_prb = rel->l;
		}
	}

	list *exps_cmp_prb = rel_prb->attr;
	list *exps_prj_prb = rel_prb->exps;
	list *exps_prj_hsh = rel_hsh->exps;

	/* build-phase res: hash-table and hash-payload stmts */
	stmt *ht = refs_find_rel(refs, rel_hsh);
	assert(ht);
	stmt *stmts_ht = ht;
	stmt *stmts_hp = ht->op1;

	/*** PROBE PHASE ***/
	stmt *sub = _start_pp(be, rel_prb->l, 0, refs, false);
	if (!sub) return NULL;

	stmt *pp = get_pipeline(be);
	stmt *prb_res = oahash_probe(be, rel, exps_cmp_prb, stmts_ht, sub, pp);
	if (prb_res == NULL) return NULL;

	/*** PROJECT RESULT PHASE ***/
	int ppln = be->pipeline;
	be->pipeline = 0;
	stmt *col = exp_bin(be, exps_cmp_prb->h->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
	sql_subfunc *cnt = sql_bind_func(be->mvc, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR, true, true);
	stmt *norows_prb = col->nrcols == 0? stmt_atom_lng(be,1) : stmt_aggr(be, col, NULL, NULL, cnt, 1, 0, 1);
	be->pipeline = ppln;

	bit outer = is_outerjoin(rel->op);
	int matched = getArg(prb_res->q, 0), rhs_slts = getArg(prb_res->q, 1);
	list *lp = oahash_project_prb(be, exps_prj_prb, matched, rhs_slts, stmts_ht->op4.lval->t->data, outer, sub, pp);
	list *lh = oahash_project_hsh(be, exps_prj_hsh, stmts_hp, rhs_slts, stmts_ht->op4.lval->t->data, norows_prb, outer, pp);
	assert(lh->cnt || lp->cnt);

	if (rel->oahash == 1)
		lh = list_merge(lh, lp, NULL);
	else
		lh = list_merge(lp, lh, NULL);
	return stmt_list(be, lh);
}

static stmt *
rel2bin_oahash_cart(backend *be, sql_rel *rel, list *refs)
{
	sql_rel *rel_hsh = NULL, *rel_prb = NULL;

	int neededpp = (rel->spb || rel->partition) && get_need_pipeline(be); /* start new parallel block after join */
	(void)neededpp;

	assert(rel->l && rel->r);
	if (rel->oahash == 1) {
		rel_hsh = rel->l;
		rel_prb = rel->r;
	} else {
		assert(rel->oahash == 2);
		rel_hsh = rel->r;
		rel_prb = rel->l;
	}

	list *exps_prj_prb = rel_prb->exps;
	list *exps_prj_hsh = rel_hsh->exps;

	/*** (pseudo) HASH PHASE ***/
	/* nothing to hash, we just want to have a materialised table for this side */
	stmt *stmts_ht = subrel_bin(be, rel_hsh, refs);

	/*** (pseudo) PROBE PHASE ***/
	stmt *stmts_prb_res = _start_pp(be, rel_prb->l, 0, refs, false);
	if (!stmts_prb_res) return NULL;

	stmt *pp = get_pipeline(be);

	assert(list_length(stmts_ht->op4.lval)||list_length(stmts_prb_res->op4.lval)); /* at least one column will be projected */

	/*** PROJECT RESULT PHASE ***/
	assert(stmts_ht->type == st_list && stmts_prb_res->type == st_list);

	stmt *col = NULL;
	int ppln = be->pipeline;
	be->pipeline = 0;
	sql_subfunc *cnt = sql_bind_func(be->mvc, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR, true, true);
	col = stmts_prb_res->op4.lval->h->data;
	stmt *norows_prb = col->nrcols == 0? stmt_atom_lng(be,1) : stmt_aggr(be, col, NULL, NULL, cnt, 1, 0, 1);
	col = stmts_ht->op4.lval->h->data;
	stmt *norows_hsh = col->nrcols == 0? stmt_atom_lng(be,1) : stmt_aggr(be, col, NULL, NULL, cnt, 1, 0, 1);
	be->pipeline = ppln;

	list *lp = oahash_project_cart(be, "expand_cartesian", exps_prj_prb, stmts_prb_res, norows_hsh, pp);
	list *lh = oahash_project_cart(be, "fetch_payload_cartesian", exps_prj_hsh, stmts_ht, norows_prb, pp);
	assert(lh->cnt || lp->cnt);

	list_merge(lh, lp, NULL);
	return stmt_list(be, lh);
}

static stmt *
rel2bin_oahash_semi(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	sql_rel *rel_hsh = rel->r, *rel_prb = rel->l;
	stmt *sub = NULL, *pp = NULL;

	int neededpp = (rel->spb || rel->partition) && get_need_pipeline(be); /* start new parallel block after join */
	(void)neededpp;

	if (!rel->exps) { /* the always-true case. just return the LHS */
		// TODO we should already optimise-away this case of semi-join in PLAN
		sub = subrel_bin(be, rel_prb, refs);
		sub = subrel_project(be, sub, refs, rel_prb);
	} else {
		if (rel->op == op_anti) {
			sql_error(sql, 10, SQLSTATE(42000) "rel2bin_oahash(): anti-join not supported yet");
			return NULL;
		}

		list *exps_cmp_prb = rel_prb->attr;
		list *exps_prj_prb = rel_prb->exps;

		/* build-phase res: hash-table and hash-payload stmts */
		stmt *stmts_ht = refs_find_rel(refs, rel_hsh);
		assert(stmts_ht);

		/*** PROBE PHASE ***/
		sub = _start_pp(be, rel_prb->l, 0, refs, false);
		if (!sub) return NULL;

		pp = get_pipeline(be);
		stmt *prb_res = oahash_probe(be, rel, exps_cmp_prb, stmts_ht, sub, pp);
		if (prb_res == NULL) return NULL;

		/*** PROJECT RESULT PHASE ***/
		list *lp = oahash_project_single(be, exps_prj_prb, getArg(prb_res->q, 0), sub, pp);
		sub = stmt_list(be, lp);
	}
	return sub;
}

stmt *
rel2bin_oahash(backend *be, sql_rel *rel, list *refs)
{
	switch(rel->op) {
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		if (!list_empty(rel->exps))
			return rel2bin_oahash_equi(be, rel, refs);
		else
			return rel2bin_oahash_cart(be, rel, refs);
	case op_semi:
	case op_anti:
		return rel2bin_oahash_semi(be, rel, refs);
	default:
        sql_error(be->mvc, 10, SQLSTATE(42000) "rel2bin_oahash(): not a JOIN");
		return NULL;
	}
}
