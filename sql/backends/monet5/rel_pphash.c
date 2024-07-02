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

static list *
find_cmp_exps(backend *be, sql_rel *rel, sql_rel *rel_hsh)
{
	mvc *sql = be->mvc;
	list *exps = sa_list(sql->sa);
	list *exps_hsh = sa_list(sql->sa), *exps_prb = sa_list(sql->sa);

	/* Find out from which side, i.e hash- or probe-, the sub-expressions of the
	 * compare-exps belong. When we find one side of a comp-exp in rel_hsh, the
	 * other side must belong to rel_prb, so we don't verify this. */
	for (node *n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data, *cmpl = e->l, *cmpr = e->r;

		assert(e->type == e_cmp && e->flag == cmp_equal);

		if (rel_find_exp(rel_hsh, cmpl)) {
			append(exps_hsh, cmpl);
			append(exps_prb, cmpr);
		} else {
			assert(rel_find_exp(rel_hsh, cmpr));
			append(exps_hsh, cmpr);
			append(exps_prb, cmpl);
		}
	}

	append(exps, exps_hsh);
	append(exps, exps_prb);
	return exps;
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
		set_need_pipeline(be);
	} else {
		set_pipeline(be, stmt_pp_start_nrparts(be, pp_nr_slices(rel)));
	}

	/* first construct the sub-relation */
	sub = subrel_bin(be, rel, refs);
	sub = subrel_project(be, sub, refs, rel);
	if (!sub) return NULL;

	pp = get_pipeline(be);
	if (!pp) {
		(void)get_need_pipeline(be);
		pp = stmt_pp_start_dynamic(be, pp_dynamic_slices(be, sub));
		set_pipeline(be, pp);
		sub = rel2bin_slicer(be, sub, 1);
	}
	return sub;
}

static list *
oahash_prepare_bld_ht(backend *be, list *exps_cmp_hsh, lng sz)
{
	list *stmts_ht = sa_list(be->mvc->sa);

	int curhash = 0;
	for (node *n = exps_cmp_hsh->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);
		bit freq = (n->next == NULL); /* last ht also computes frequencies */

		InstrPtr q = stmt_oahash_new(be, t->type->localtype, sz, freq, curhash);
		if (q == NULL) return NULL;
		q->inout = 0;

		stmt *s = stmt_none(be);
		s->op4.typeval = *t;
		s->nrcols = 1;
		s->nr = curhash = getArg(q, 0);
		append(stmts_ht, s);
	}
	assert(stmts_ht->cnt == exps_cmp_hsh->cnt);
	return stmts_ht;
}

static list *
oahash_prepare_bld_hp(backend *be, list *exps_prj_hsh, list *stmts_ht, lng sz)
{
	list *stmts_hp = sa_list(be->mvc->sa);
	int prnt = ((stmt *)stmts_ht->t->data)->nr;

	int previous = prnt;
	for (node *n = exps_prj_hsh->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);

		InstrPtr q = stmt_oahash_new_payload(be, t->type->localtype, sz, prnt, previous);
		if (q == NULL) return NULL;
		q->inout = 0;

		stmt *s = stmt_none(be);
		s->op4.typeval = *t;
		s->nrcols = 1;
		s->nr = previous = getArg(q, 0);
		append(stmts_hp, s);
	}
	assert(stmts_hp->cnt == exps_prj_hsh->cnt);
	return stmts_hp;
}

static list *
oahash_prepare_res(backend *be, list *exps_prj, lng sz)
{
	list *stmts_res = sa_list(be->mvc->sa);

	for (node *n = exps_prj->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);
		InstrPtr q = stmt_bat_new(be, t->type->localtype, sz);
		if (q == NULL) return NULL;
		q->inout = 0;
		append(stmts_res, q);
	}
	assert(stmts_res->cnt == exps_prj->cnt);
	return stmts_res;
}

static int
oahash_build_ht(backend *be, list *exps_cmp_hsh, list *stmts_bld_ht, stmt *sub, stmt *pp)
{
	int err = 0;
	int slt_ids = 0;
	stmt *prev_ht = NULL;

	for (node *n = exps_cmp_hsh->h, *inout = stmts_bld_ht->h; n && inout; n = n->next, inout = inout->next) {
		stmt *key = exp_bin(be, n->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		InstrPtr q = NULL;
		if (slt_ids == 0) {
			q = stmt_oahash_build_table(be, inout->data, key, pp);
		} else {
			assert(slt_ids && prev_ht);
			q = stmt_oahash_build_combined_table(be, inout->data, key, slt_ids, prev_ht, pp);
		}
		if (q == NULL) return err;
		slt_ids = getDestVar(q);
		prev_ht = inout->data;
	}
	return slt_ids;
}

static InstrPtr
oahash_build_freq(backend *be, stmt *prnt_ht, int prnt_slts, int compute_pos, stmt *pp)
{
	InstrPtr q = newStmt(be->mb, putName("oahash"), putName("compute_frequencies"));
	if (q == NULL)
		return NULL;
	if (!compute_pos) {
		/* No payload at the hash-side, hence no need to compute payload_pos.
		 * The oahash.expand() at the probe-side only needs the frequencies.
		 */
		int tt = tail_type(prnt_ht)->type->localtype;
		setVarType(be->mb, getArg(q, 0), newBatType(tt));
		getArg(q, 0) = prnt_ht->nr;
		q->inout = 0;
	} else {
		setVarType(be->mb, getArg(q, 0), newBatType(TYPE_oid));
		q = pushReturn(be->mb, q, prnt_ht->nr);
		q->inout = 1;
	}
	q = pushArgument(be->mb, q, prnt_slts);
	q = pushArgument(be->mb, q, getArg(pp->q, 2) /* pipeline ptr*/);
	pushInstruction(be->mb, q);
	return q;
}

static int
oahash_build_hp(backend *be, InstrPtr stmt_freq, list *exps_prj_hsh, list *stmts_bld_hp, stmt *sub, stmt *pp)
{
	int err = 1;

	int payload_pos = getArg(stmt_freq, 0);
	for (node *n = exps_prj_hsh->h, *inout = stmts_bld_hp->h; n && inout; n = n->next, inout = inout->next) {
		stmt *payload = exp_bin(be, n->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(payload); /* must find */
		stmt *hp = stmt_oahash_add_payload(be, inout->data, payload, payload_pos, pp);
		if (hp == NULL) return err;
	}

	return 0;
}

/* Generates the parallel block to probe the hash table
 */
static InstrPtr
oahash_probe(backend *be, stmt **prb_sub, sql_rel *rel_prb, list *exps_cmp_prb, list *stmts_bld_ht, list *refs)
{
	InstrPtr last_prb = NULL;
	stmt *pp = NULL, *sub = NULL;

	*prb_sub = sub = _start_pp(be, rel_prb, 0, refs);

	/* stmts_bld_ht is in the same order as the join columns */
	int matched = 0, rhs_slts = 0;
	for (node *n = exps_cmp_prb->h, *m = stmts_bld_ht->h; n && m; n = n->next, m = m->next) {
		stmt *key = exp_bin(be, n->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		int rht = ((stmt *)m->data)->nr;
		InstrPtr q = NULL;
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
		last_prb = q;
	}

	return last_prb;
}

static list *
oahash_project_hsh(backend *be, list *exps_prj_hsh, list *stmts_bld_ht, list *stmts_bld_hp, InstrPtr prb_res,
bit *first)
{
	int matched = getArg(prb_res, 0), rhs_slts = getArg(prb_res, 1);
	assert(matched && rhs_slts); /* must be set */
	list *l = sa_list(be->mvc->sa);
	stmt *pp = get_pipeline(be);
	assert(pp);

	for (node *n = stmts_bld_hp->h, *o = exps_prj_hsh->h; n && o; n = n->next, o = o->next) {
		InstrPtr q = stmt_oahash_fetch_payload(be, rhs_slts, n->data, stmts_bld_ht->t->data, *first, pp);
		if (q == NULL) return NULL;
		*first = 0;

		sql_exp *e = o->data;
		stmt *s = stmt_none(be);
		if (s == NULL) return NULL;
		s->op4.typeval = *exp_subtype(e);
		s->nr = getArg(q, 1);
		s->nrcols = 1;
		s = stmt_alias(be, s, s->label, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}
	return l;
}

static list *
oahash_project_prb(backend *be, stmt *sub, list *exps_prj_prb, list *stmts_bld_ht, InstrPtr prb_res, bit *first)
{
	int matched = getArg(prb_res, 0), rhs_slts = getArg(prb_res, 1);
	assert(matched && rhs_slts); /* must be set */
	list *l = sa_list(be->mvc->sa);
	stmt *pp = get_pipeline(be);
	assert(pp);

	for (node *o = exps_prj_prb->h; o; o = o->next) {
		stmt *key = exp_bin(be, o->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		InstrPtr q = stmt_oahash_expand(be, key, matched, rhs_slts, stmts_bld_ht->t->data, *first, pp);
		if (q == NULL) return NULL;
		*first = 0;

		sql_exp *e = o->data;
		stmt *s = stmt_none(be);
		if (s == NULL) return NULL;
		s->op4.typeval = *exp_subtype(e);
		s->nr = getArg(q, 1);
		s->nrcols = 1;
		s->q = q;
		s = stmt_alias(be, s, s->label, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}
	return l;
}

/* Construct result relations:
 *  project only probe-side rows (semi-/anti-joins),
 *	repeat matched rows (outer/equi-joins),
 *  adding NULLs (outer joins),
 *	collect partial results (if no further parrallel blocks)
 */
static stmt *
oahash_collect(backend *be, list *exps_prj_hsh, list *exps_prj_prb, list *stmts_res_hsh, list *stmts_res_prb, list *lh, list *lp)
{
	list *l = sa_list(be->mvc->sa);
	stmt *pp = get_pipeline(be);
	assert(pp);


	int pos = lh->cnt?((stmt *)lh->h->data)->nr:((stmt *)lp->h->data)->nr;

	assert(lh->cnt == stmts_res_hsh->cnt && lh->cnt == exps_prj_hsh->cnt);
	for (node *n = lh->h, *m = stmts_res_hsh->h, *o = exps_prj_hsh->h; n && m && o; n = n->next, m = m->next, o = o->next) {
		InstrPtr q = newStmt(be->mb, getName("algebra"), projectionRef);
		if(q == NULL) return NULL;

		InstrPtr qIn = (InstrPtr) n->data;
		InstrPtr qRes = (InstrPtr) m->data;
		getArg(q,0) = getDestVar(qRes);
		setVarType(be->mb, getArg(q,0), getArgType(be->mb, qIn, 1));
		q = pushArgument(be->mb, q, pos);
		q = pushArgument(be->mb, q, getArg(qIn, 1));
		q = pushArgument(be->mb, q, getArg(pp->q, 2)); // pipeline ptr
		q->inout = 0;
		pushInstruction(be->mb, q);

		sql_exp *e = o->data;
		stmt *s = stmt_none(be);
		s->op4.typeval = *exp_subtype(e);
		s->nr = getDestVar(q);
		s->nrcols = 1;
		s = stmt_alias(be, s, s->label, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}

	assert(lp->cnt == stmts_res_prb->cnt && lp->cnt == exps_prj_prb->cnt);
	for (node *n = lp->h, *m = stmts_res_prb->h, *o = exps_prj_prb->h; n && m && o; n = n->next, m = m->next, o = o->next) {
		InstrPtr q = newStmt(be->mb, getName("algebra"), projectionRef);
		if(q == NULL) return NULL;

		InstrPtr qIn = (InstrPtr) n->data;
		InstrPtr qRes = (InstrPtr) m->data;
		getArg(q,0) = getDestVar(qRes);
		setVarType(be->mb, getArg(q,0), getArgType(be->mb, qIn, 1));
		q = pushArgument(be->mb, q, pos);
		q = pushArgument(be->mb, q, getArg(qIn, 1));
		q = pushArgument(be->mb, q, getArg(pp->q, 2)); // pipeline ptr
		q->inout = 0;
		pushInstruction(be->mb, q);

		sql_exp *e = o->data;
		stmt *s = stmt_none(be);
		s->op4.typeval = *exp_subtype(e);
		s->nr = getDestVar(q);
		s->nrcols = 1;
		s = stmt_alias(be, s, s->label, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}

	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);
	return stmt_list(be, l);
}

static stmt *
rel2bin_oahash_equi(backend *be, sql_rel *rel, list *refs)
{
	sql_rel *rel_hsh = NULL, *rel_prb = NULL;
	/* compare and projection columns of hash- and probe-side */
	list *exps_cmp = NULL, *exps_prj_hsh = NULL, *exps_prj_prb = NULL;
	/* build-phase res: hash-table and hash-payload stmts */
	list *stmts_bld_ht = NULL, *stmts_bld_hp = NULL;
	/* probe-phase res: hash- and probe-side stmts */
	list *stmts_res_hsh = NULL, *stmts_res_prb = NULL;
	stmt *sub = NULL, *pp = NULL;
	int neededpp = get_need_pipeline(be); /* remember and reset previous info. */

	/* find the hash- vs probe-side */
	if (rel->oahash == 1) {
		rel_hsh = rel->l;
		rel_prb = rel->r;
	} else {
		assert(rel->oahash == 2);
		rel_hsh = rel->r;
		rel_prb = rel->l;
	}

	exps_cmp = find_cmp_exps(be, rel, rel_hsh);

	/* find projection columns */
	// TODO remove join-only columns from these lists
	exps_prj_hsh = rel_projections(be->mvc, rel_hsh, 0, 1, 1);
	exps_prj_prb = rel_projections(be->mvc, rel_prb, 0, 1, 1);
	assert(exps_prj_hsh->cnt||exps_prj_prb->cnt); /* at least one column will be projected */

	/*** PREPARE PHASE ***/
	lng bld_sz = _estimate(be->mvc, rel_hsh);
	stmts_bld_ht = oahash_prepare_bld_ht(be, exps_cmp->h->data, bld_sz);
	stmts_bld_hp = oahash_prepare_bld_hp(be, exps_prj_hsh, stmts_bld_ht, bld_sz);
	/* If no one 'neededpp' in the super-tree, we gather the join results */
	// TODO delay gathering the join results until end of what can be parallelised
	if(!neededpp) {
		lng res_sz = _estimate(be->mvc, rel);
		stmts_res_hsh = oahash_prepare_res(be, exps_prj_hsh, res_sz);
		stmts_res_prb = oahash_prepare_res(be, exps_prj_prb, res_sz);
	}

	/*** HASH PHASE ***/
	sub = _start_pp(be, rel_hsh, 1, refs);
	pp = get_pipeline(be);
	int slt_ids = oahash_build_ht(be, exps_cmp->h->data, stmts_bld_ht, sub, pp);
	InstrPtr stmt_freq = oahash_build_freq(be, stmts_bld_ht->t->data, slt_ids, exps_prj_hsh->cnt, pp);
	(void)oahash_build_hp(be, stmt_freq, exps_prj_hsh, stmts_bld_hp, sub, pp);
	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);

	/*** PROBE PHASE ***/
	InstrPtr prb_res = oahash_probe(be, &sub, rel_prb, exps_cmp->t->data, stmts_bld_ht, refs);
	if (prb_res == NULL) return NULL;

	/*** PROJECT RESULT PHASE ***/
	bit first = 1;
	list *lh = oahash_project_hsh(be, exps_prj_hsh, stmts_bld_ht, stmts_bld_hp, prb_res, &first);
	list *lp = oahash_project_prb(be, sub, exps_prj_prb, stmts_bld_ht, prb_res, &first);
	assert(lh->cnt || lp->cnt);

	if(neededpp) {
		list_merge(lh, lp, NULL);
		sub = stmt_list(be, lh);
	} else {
		sub = oahash_collect(be, exps_prj_hsh, exps_prj_prb, stmts_res_hsh, stmts_res_prb, lh, lp);
	}
	return sub;
}

static stmt *
rel2bin_oahash_semi(backend *be, sql_rel *rel, list *refs)
{
	sql_rel *rel_hsh = rel->r, *rel_prb = rel->l;
	/* compare columns of hash- and probe-side */
	list *exps_cmp = find_cmp_exps(be, rel, rel_hsh);
	/* projection columns of the probe-side */
	list *exps_prj_prb = rel_projections(be->mvc, rel_prb, 0, 1, 1);
	assert(exps_prj_prb->cnt); /* at least one column should be projected */
	list *stmts_res_prb = NULL;
	stmt *sub = NULL, *pp = NULL;
	int neededpp = get_need_pipeline(be); /* remember and reset previous info. */

	/*** PREPARE PHASE ***/
	lng sz = _estimate(be->mvc, rel_hsh);
	list *stmts_bld_ht = oahash_prepare_bld_ht(be, exps_cmp->h->data, sz);
	if(!neededpp) {
		lng res_sz = _estimate(be->mvc, rel);
		stmts_res_prb = oahash_prepare_res(be, exps_prj_prb, res_sz);
	}

	/*** HASH PHASE ***/
	sub = _start_pp(be, rel_hsh, 1, refs);
	pp = get_pipeline(be); 
	(void)oahash_build_ht(be, exps_cmp->h->data, stmts_bld_ht, sub, pp);
	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);

	/*** PROBE PHASE ***/
	/*** PROJECT RESULT PHASE ***/
	(void)stmts_bld_ht;
	(void)stmts_res_prb;
	return NULL;
}

stmt *
rel2bin_oahash(backend *be, sql_rel *rel, list *refs)
{
	switch(rel->op) {
	case op_join:
		return rel2bin_oahash_equi(be, rel, refs);
	case op_semi:
		return rel2bin_oahash_semi(be, rel, refs);
	case op_anti:
	case op_left:
	case op_right:
	case op_full:
	default:
        sql_error(be->mvc, 10, SQLSTATE(42000) "JOIN type not supported by OAHash");
		return NULL;
	}
}
