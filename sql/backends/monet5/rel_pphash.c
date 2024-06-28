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

/* Initiate the global variables for the hash-table results (stmts_bld_ht and
 * stmts_bld_hp) and the hash-join results (stmts_res_hsh and stmts_res_prb).
 */
static int
oahash_prepare(backend *be, sql_rel *rel_hsh, sql_rel *rel_prb,
		list *exps_cmp_hsh, list *exps_prj_hsh, list *exps_prj_prb, int pp_continues,
		list **stmts_bld_ht, list **stmts_bld_hp,
		list **stmts_res_hsh, list **stmts_res_prb)
{
	mvc *sql = be->mvc;
	int curhash = 0;
	int err = 1;
	lng hsh_est = get_max_bt_count(sql, rel_hsh, 0);
	lng prb_est = get_max_bt_count(sql, rel_prb, 0);

	// TODO better estimation
	if (hsh_est == 0 || hsh_est >= GDK_int_max) {
		hsh_est = 85000000;
	}
	if (prb_est == 0 || prb_est >= GDK_int_max) {
		prb_est = 85000000;
	}

	*stmts_bld_ht = sa_list(sql->sa);
	for (node *n = exps_cmp_hsh->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);
		bit freq = (n->next == NULL); /* last ht also computes frequencies */

		InstrPtr q = stmt_oahash_new(be, t->type->localtype, hsh_est, freq, curhash);
		if (q == NULL) return err;
		q->inout = 0;

		stmt *s = stmt_none(be);
		s->op4.typeval = *t;
		s->nrcols = 1;
		s->nr = curhash = getArg(q, 0);
		append(*stmts_bld_ht, s);
	}
	*stmts_bld_hp = sa_list(sql->sa);
	int previous = curhash;
	for (node *n = exps_prj_hsh->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);

		InstrPtr q = stmt_oahash_new_payload(be, t->type->localtype, hsh_est, curhash, previous);
		if (q == NULL) return err;
		q->inout = 0;

		stmt *s = stmt_none(be);
		s->op4.typeval = *t;
		s->nrcols = 1;
		s->nr = previous = getArg(q, 0);
		append(*stmts_bld_hp, s);
	}
	assert((*stmts_bld_ht)->cnt == exps_cmp_hsh->cnt);
	assert((*stmts_bld_hp)->cnt == exps_prj_hsh->cnt);

	if (!pp_continues) {
		/* probe-res_hash-side */
		*stmts_res_hsh = sa_list(sql->sa);
		for (node *n = exps_prj_hsh->h; n; n = n->next) {
			sql_subtype *t = exp_subtype((sql_exp*)n->data);
			InstrPtr q = stmt_bat_new(be, t->type->localtype, hsh_est);
			if (q == NULL) return err;
			q->inout = 0;
			append(*stmts_res_hsh, q);
		}
		/* probe-res_probe-side */
		*stmts_res_prb = sa_list(sql->sa);
		for (node *n = exps_prj_prb->h; n; n = n->next) {
			sql_subtype *t = exp_subtype((sql_exp*)n->data);
			InstrPtr q = stmt_bat_new(be, t->type->localtype, prb_est);
			if (q == NULL) return err;
			q->inout = 0;
			append(*stmts_res_prb, q);
		}
		assert((*stmts_res_hsh)->cnt == exps_prj_hsh->cnt);
		assert((*stmts_res_prb)->cnt == exps_prj_prb->cnt);
	}

	return 0;
}

/* Generates the parallel block to compute the hash table */
static int
oahash_build(backend *be, sql_rel *rel_hsh, list *exps_cmp_hsh, list *exps_prj_hsh, list *stmts_bld_ht, list *stmts_bld_hp, list *refs, int compute_frequencies)
{
	int err = 1;
	int prnt_slts = 0;
	stmt *prnt_ht = NULL, *sub = NULL, *pp = NULL;

	if (get_pipeline(be)) {
        sql_error(be->mvc, 10, SQLSTATE(42000) "Internal error: hash-join cannot start within a pipelines block");
		return err;
	}
	if (pp_can_not_start(be->mvc, rel_hsh)) {
		set_need_pipeline(be);
	} else {
		set_pipeline(be, stmt_pp_start_nrparts(be, pp_nr_slices(rel_hsh)));
	}

	/* first construct the sub-relation */
	sub = subrel_bin(be, rel_hsh, refs);
	sub = subrel_project(be, sub, refs, rel_hsh);
	if (!sub) return err;

	pp = get_pipeline(be);
	if (!pp) {
		(void)get_need_pipeline(be);
		pp = stmt_pp_start_dynamic(be, pp_dynamic_slices(be, sub));
		set_pipeline(be, pp);
		sub = rel2bin_slicer(be, sub, 1);
	}

	for (node *n = exps_cmp_hsh->h, *inout = stmts_bld_ht->h; n && inout; n = n->next, inout = inout->next) {
		stmt *key = exp_bin(be, n->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		InstrPtr q = NULL;
		if (prnt_slts == 0) {
			q = stmt_oahash_build_table(be, inout->data, key, pp);
		} else {
			q = stmt_oahash_build_combined_table(be, inout->data, key, prnt_slts, prnt_ht, pp);
		}
		if (q == NULL) return err;
		prnt_slts = getDestVar(q);
		prnt_ht = inout->data;
	}
	assert(prnt_slts && prnt_ht); /* must be set */

	if (compute_frequencies) {
		InstrPtr stmt_freq = newStmt(be->mb, putName("oahash"), putName("compute_frequencies"));
		if (stmt_freq == NULL)
			return err;
		if (exps_prj_hsh->cnt == 0) {
			/* No payload at the hash-side, hence no need to compute payload_pos.
			 * The oahash.expand() at the probe-side only needs the frequencies.
			 */
			int tt = tail_type(prnt_ht)->type->localtype;
			setVarType(be->mb, getArg(stmt_freq, 0), newBatType(tt));
			getArg(stmt_freq, 0) = prnt_ht->nr;
			stmt_freq->inout = 0;
		} else {
			setVarType(be->mb, getArg(stmt_freq, 0), newBatType(TYPE_oid));
			stmt_freq = pushReturn(be->mb, stmt_freq, prnt_ht->nr);
			stmt_freq->inout = 1;
		}
		stmt_freq = pushArgument(be->mb, stmt_freq, prnt_slts);
		stmt_freq = pushArgument(be->mb, stmt_freq, getArg(pp->q, 2) /* pipeline ptr*/);
		pushInstruction(be->mb, stmt_freq);

		if (exps_prj_hsh->cnt > 0) {
			int payload_pos = getArg(stmt_freq, 0);
			for (node *n = exps_prj_hsh->h, *inout = stmts_bld_hp->h; n && inout; n = n->next, inout = inout->next) {
				stmt *payload = exp_bin(be, n->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
				assert(payload); /* must find */
				stmt *hp = stmt_oahash_add_payload(be, inout->data, payload, payload_pos, pp);
				if (hp == NULL) return err;
			}
		}
	}

	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);

	return 0;
}

/* Generates the parallel block to probe the hash table
 */
static InstrPtr
oahash_probe(backend *be, stmt **prb_sub, sql_rel *rel_prb, list *exps_cmp_prb, list *stmts_bld_ht, list *refs)
{
	InstrPtr last_prb = NULL;
	stmt *pp = NULL, *sub = NULL;

	if (pp_can_not_start(be->mvc, rel_prb)) {
		set_need_pipeline(be);
	} else {
		set_pipeline(be, stmt_pp_start_nrparts(be, pp_nr_slices(rel_prb)));
	}

	/* first construct the sub-relation */
	sub = subrel_bin(be, rel_prb, refs);
	sub = subrel_project(be, sub, refs, rel_prb);
	if (!sub) return NULL;

	pp = get_pipeline(be);
	if (!pp) {
		(void)get_need_pipeline(be);
		set_pipeline(be, pp = stmt_pp_start_dynamic(be, pp_dynamic_slices(be, sub)));
		sub = rel2bin_slicer(be, sub, 1);
	}
	*prb_sub = sub;

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
	mvc *sql = be->mvc;
	sql_rel *rel_hsh = NULL, *rel_prb = NULL;
	stmt *sub = NULL;
	/* compare and projection columns of hash- and probe-side */
	list *exps_cmp_hsh = sa_list(sql->sa), *exps_cmp_prb = sa_list(sql->sa);
	list *exps_prj_hsh = sa_list(sql->sa), *exps_prj_prb = sa_list(sql->sa);
	/* build-phase res: hash-table and hash-payload stmts */
	list *stmts_bld_ht = NULL, *stmts_bld_hp = NULL;
	/* probe-phase res: hash- and probe-side stmts */
	list *stmts_res_hsh = NULL, *stmts_res_prb = NULL;
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

	/* find compare-exps of hash- and probe-side */
	for (node *n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data, *cmpl = e->l, *cmpr = e->r;

		assert(e->type == e_cmp && e->flag == cmp_equal);

		if (rel_find_exp(rel_hsh, cmpl)) {
			append(exps_cmp_hsh, cmpl);
			append(exps_cmp_prb, cmpr);
		} else {
			assert(rel_find_exp(rel_hsh, cmpr));
			append(exps_cmp_hsh, cmpr);
			append(exps_cmp_prb, cmpl);
		}
	}
	/* find projection columns */
	// TODO remove join-only columns from these lists
	if (!is_semi(rel->op)) /* no projection columns for semi or anti */
		exps_prj_hsh = rel_projections(sql, rel_hsh, 0, 1, 1);
	exps_prj_prb = rel_projections(sql, rel_prb, 0, 1, 1);
	assert(exps_prj_hsh->cnt||exps_prj_prb->cnt); /* at least one column will be projected */

	/* If no one 'neededpp' in the super-tree, we gather the join results */
	// TODO delay gathering the join results until end of what can be parallelised
	(void)oahash_prepare(be, rel_hsh, rel_prb,
			exps_cmp_hsh, exps_prj_hsh, exps_prj_prb, neededpp,
			&stmts_bld_ht, &stmts_bld_hp,
			&stmts_res_hsh, &stmts_res_prb);

	/*** HASH PHASE ***/
	(void)oahash_build(be, rel_hsh, exps_cmp_hsh, exps_prj_hsh, stmts_bld_ht, stmts_bld_hp, refs, !is_semi(rel->op));

	/*** PROBE PHASE ***/
	InstrPtr prb_res = oahash_probe(be, &sub, rel_prb, exps_cmp_prb, stmts_bld_ht, refs);
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
	(void) be;
	(void) rel;
	(void) refs;

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
