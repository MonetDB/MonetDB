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
#include "opt_prelude.h"

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

static lng
_estimate(mvc *sql, sql_rel *rel)
{
	// TODO better estimation
	lng est = get_max_bt_count(sql, rel, 0);

	if (est == 0 || est >= GDK_int_max) {
		est = 85000000;
	}
	return est;
}

static void
find_cmp_exps(list **exps_hsh, list **exps_prb, list *exps, sql_rel *rel_hsh, sql_rel *rel_prb)
{
	assert(exps);

	/* Find out if a sub-expression of the (compare) exps belong to rel_hsh or
	 * rel_prb or is a constant. */
	for (node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;//, *cmp1st = NULL, *cmp2nd = NULL;

		assert(e->type == e_cmp && e->flag == cmp_equal);

		/* search first for the not-atom exp, otherwise rel_find_exp()
		 * incorrectly returns TRUE for an atom-typed exp */
		if (exp_is_atom(e->l)) {
			if (rel_find_exp(rel_hsh, e->r)) {
				append(*exps_hsh, e->r);
				append(*exps_prb, e->l);
			} else {
				assert(rel_find_exp(rel_prb, e->r));
				append(*exps_hsh, e->l);
				append(*exps_prb, e->r);
			}
		} else {
			if (rel_find_exp(rel_prb, e->l)) {
				append(*exps_hsh, e->r);
				append(*exps_prb, e->l);
			} else {
				assert(rel_find_exp(rel_hsh, e->l));
				append(*exps_hsh, e->l);
				append(*exps_prb, e->r);
			}
		}

	}
}

static stmt *
_start_pp(backend *be, sql_rel *rel, bit buildphase, list *refs)
{
	stmt *sub = NULL, *pp = NULL;

	if (buildphase && get_pipeline(be)) {
        sql_error(be->mvc, 10, SQLSTATE(42000) "Internal error: hash-join cannot start within a pipelines block");
		return NULL;
	}
	if (pp_can_not_start(be->mvc, rel)) {
		if (!be->need_pipeline)
			set_need_pipeline(be);
	} else {
		set_pipeline(be, stmt_pp_start_nrparts(be, pp_nr_slices(rel)));
	}

	/* first construct the sub-relation */
	sub = subrel_bin(be, rel, refs);
	sub = subrel_project(be, sub, refs, rel);
	if (sub) {
		pp = get_pipeline(be);
		if (!pp) {
			pp = stmt_pp_start_dynamic(be, pp_dynamic_slices(be, sub));
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
oahash_prepare_bld_ht(backend *be, list *exps, lng sz)
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
oahash_prepare_bld_hp(backend *be, list *exps_prj_hsh, int prnt, lng sz)
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
oahash_build_ht(backend *be, int *slt_ids, list *exps, list *shared_ht, stmt *sub, stmt *pp)
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
oahash_build_freq(backend *be, stmt *ht, int slt_ids, int compute_pos, stmt *pp)
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
oahash_build_hp(backend *be, list *exps_prj_hsh, list *shared_hp, int pld_pos, stmt *sub, stmt *pp)
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

/* Generates the parallel block to probe the hash table
 */
static stmt *
oahash_probe(backend *be, list *exps_cmp_prb, stmt *stmts_ht, stmt *sub, stmt *pp)
{
	sql_exp *e = NULL;
	InstrPtr q = NULL;
	int matched = 0, rhs_slts = 0;
	/* stmts_ht is in the same order as the join columns */
	for (node *n = exps_cmp_prb->h, *m = stmts_ht->op4.lval->h; n && m; n = n->next, m = m->next) {
		e = n->data;
		stmt *key = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		key = column(be, key);

		int rht = ((stmt *)m->data)->nr;
		if (!matched) {
			q = stmt_oahash_hash(be, key, pp);
			if (q == NULL) return NULL;
			q = stmt_oahash_probe(be, key, getDestVar(q), rht, pp);
		} else {
			q = stmt_oahash_combined_hash(be, key, matched, rhs_slts, pp);
			if (q == NULL) return NULL;
			q = stmt_oahash_combined_probe(be, key, getDestVar(q), matched, rht, pp);
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
oahash_project_hsh(backend *be, list *exps_prj_hsh, stmt *stmts_hp, int rhs_slts, stmt *freq_sink, stmt *norows_prb, bit outer, stmt *pp)
{
	list *l = sa_list(be->mvc->sa);

	for (node *o = exps_prj_hsh->h; o; o = o->next) {
		stmt *hp_sink = exp_bin(be, o->data, stmts_hp, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(hp_sink); /* must find */

		InstrPtr q = stmt_oahash_fetch_payload(be, hp_sink, rhs_slts, freq_sink, norows_prb, outer, pp);
		if (q == NULL) return NULL;

		sql_exp *e = o->data;
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
	return l;
}

static list *
oahash_project_prb(backend *be, list *exps_prj_prb, int matched, int rhs_slts, stmt *freq_sink, bit outer, stmt *sub, stmt *pp)
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
oahash_project_single(backend *be, list *exps_prj, int selected, stmt *freq_sink, stmt *sub, stmt *pp)
{
	list *l = sa_list(be->mvc->sa);

	for (node *o = exps_prj->h; o; o = o->next) {
		stmt *key = exp_bin(be, o->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		key = column(be, key);

		InstrPtr q = stmt_oahash_project(be, key, selected, freq_sink, pp);
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
oahash_project_cart(backend *be, str func, list *exps_prj, stmt *sub, stmt *norows, stmt *pp)
{
	list *l = sa_list(be->mvc->sa);
	int tt;

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

static stmt *
rel2bin_oahash_equi(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	sql_rel *rel_hsh = NULL, *rel_prb = NULL;
	/* compare and projection columns of hash- and probe-side */
	list *exps_cmp_hsh = sa_list(sql->sa), *exps_cmp_prb = sa_list(sql->sa);
	list *exps_prj_hsh = NULL, *exps_prj_prb = NULL;
	/* For the declaration of shared vars at the beginning */
	list *shared_ht = NULL, *shared_hp = NULL;
	/* build-phase res: hash-table and hash-payload stmts */
	stmt *stmts_ht = NULL, *stmts_hp = NULL;
	stmt *sub = NULL, *pp = NULL;

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

	find_cmp_exps(&exps_cmp_hsh, &exps_cmp_prb, rel->exps, rel_hsh, rel_prb);

	/*** PREPARE PHASE ***/
	/* find projection columns */
	// TODO remove join-only columns from these lists
	exps_prj_hsh = rel_projections(be->mvc, rel_hsh, 0, 1, 1);
	exps_prj_prb = rel_projections(be->mvc, rel_prb, 0, 1, 1);
	assert(exps_prj_hsh->cnt||exps_prj_prb->cnt); /* at least one column will be projected */

	lng bld_sz = _estimate(be->mvc, rel_hsh);
	shared_ht = oahash_prepare_bld_ht(be, exps_cmp_hsh, bld_sz);
	shared_hp = oahash_prepare_bld_hp(be, exps_prj_hsh, getArg((InstrPtr)shared_ht->t->data,0), bld_sz);

	/*** HASH PHASE ***/
	sub = _start_pp(be, rel_hsh, 1, refs);
	if (!sub) return NULL;

	pp = get_pipeline(be);
	int slt_ids = 0;
	stmts_ht = oahash_build_ht(be, &slt_ids, exps_cmp_hsh, shared_ht, sub, pp);
	InstrPtr stmt_freq = oahash_build_freq(be, stmts_ht->op4.lval->t->data, slt_ids, exps_prj_hsh->cnt, pp);
	stmts_hp = oahash_build_hp(be, exps_prj_hsh, shared_hp, getArg(stmt_freq,0), sub, pp);
	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);

	/*** PROBE PHASE ***/
	sub = _start_pp(be, rel_prb, 0, refs);
	if (!sub) return NULL;

	pp = get_pipeline(be);
	stmt *prb_res = oahash_probe(be, exps_cmp_prb, stmts_ht, sub, pp);
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

	list_merge(lh, lp, NULL);
	return stmt_list(be, lh);
}

static stmt *
rel2bin_oahash_cart(backend *be, sql_rel *rel, list *refs)
{
	(void)refs;

	sql_rel *rel_hsh = NULL, *rel_prb = NULL;
	list *exps_prj_hsh = NULL, *exps_prj_prb = NULL;

	assert(rel->l && rel->r);
	if (rel->oahash == 1) {
		rel_hsh = rel->l;
		rel_prb = rel->r;
	} else {
		assert(rel->oahash == 2);
		rel_hsh = rel->r;
		rel_prb = rel->l;
	}

	/*** PREPARE PHASE ***/
	/* find projection columns */
	exps_prj_hsh = rel_projections(be->mvc, rel_hsh, 0, 1, 1);
	exps_prj_prb = rel_projections(be->mvc, rel_prb, 0, 1, 1);
	assert(exps_prj_hsh->cnt||exps_prj_prb->cnt); /* at least one column will be projected */

	/*** (pseudo) HASH PHASE ***/
	/* nothing to hash, we just want to have a materialised table for this side */
	stmt *stmts_ht = rel2bin_materialize(be, rel_hsh);

	/*** (pseudo) PROBE PHASE ***/
	stmt *stmts_prb_res = _start_pp(be, rel_prb, 0, refs);
	if (!stmts_prb_res) return NULL;

	stmt *pp = get_pipeline(be);

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

	if (!rel->exps) { /* the always-true case. just return the LHS */
		// TODO we should already optimise-away this case of semi-join in PLAN
		sub = subrel_bin(be, rel_prb, refs);
		sub = subrel_project(be, sub, refs, rel_prb);
	} else {
		/* compare columns of hash- and probe-side */
		list *exps_cmp_hsh = sa_list(sql->sa), *exps_cmp_prb = sa_list(sql->sa);
		find_cmp_exps(&exps_cmp_hsh, &exps_cmp_prb, rel->exps, rel_hsh, rel_prb);

		/* projection columns of the probe-side */
		list *exps_prj_prb = rel_projections(sql, rel_prb, 0, 1, 1);
		assert(exps_prj_prb->cnt); /* at least one column should be projected */

		if (rel->op == op_anti) {
			sql_error(sql, 10, SQLSTATE(42000) "rel2bin_oahash(): anti-join not supported yet");
			return NULL;
		}

		/*** PREPARE PHASE ***/
		lng sz = _estimate(sql, rel_hsh);
		list *shared_ht = oahash_prepare_bld_ht(be, exps_cmp_hsh, sz);

		/*** HASH PHASE ***/
		sub = _start_pp(be, rel_hsh, 1, refs);
		if (!sub) return NULL;

		pp = get_pipeline(be);
		int slt_ids = 0;
		stmt *stmts_ht = oahash_build_ht(be, &slt_ids, exps_cmp_hsh, shared_ht, sub, pp);
		(void)stmt_pp_jump(be, pp, be->nrparts);
		(void)stmt_pp_end(be, pp);

		/*** PROBE PHASE ***/
		sub = _start_pp(be, rel_prb, 0, refs);
		if (!sub) return NULL;

		pp = get_pipeline(be);
		stmt *prb_res = oahash_probe(be, exps_cmp_prb, stmts_ht, sub, pp);
		if (prb_res == NULL) return NULL;

		/*** PROJECT RESULT PHASE ***/
		list *lp = oahash_project_single(be, exps_prj_prb, getArg(prb_res->q, 0), stmts_ht->op4.lval->t->data, sub, pp);
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
		if (rel->exps)
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
