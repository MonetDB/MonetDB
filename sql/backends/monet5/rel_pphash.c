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

/* Initiate the global variables for the hash-table results (HSHres_hts and
 * HSHres_hps) and the hash-join results (JNres_hshs and JNres_prbs).
 */
static int
rel2bin_pphash_prepare(backend *be, sql_rel *rel_hsh, sql_rel *rel_prb,
	list **HSHres_hts, list **HSHres_hps, //list **JNres_hshs, list **JNres_prbs,
	list *exps_hsh_ht, list *exps_hsh_hp)//, list *exps_prb_hp)
{
	mvc *sql = be->mvc;
	int curhash = 0;
	int err = 1;
	BUN hsh_est = get_rel_count(rel_hsh);
	BUN prb_est = get_rel_count(rel_prb);

	// TODO better estimation
	if (hsh_est == BUN_NONE || (ulng) hsh_est > (ulng) GDK_lng_max) {
		hsh_est = 85000000;
	}
	if (prb_est == BUN_NONE || (ulng) prb_est > (ulng) GDK_lng_max) {
		prb_est = 85000000;
	}

	/* hash-side join-res */
/*
	*JNres_hshs = sa_list(sql->sa);
	for (node *n = exps_hsh_hp->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);
		InstrPtr q = stmt_bat_new(be, t->type->localtype, hsh_est * 1.1);
		if (q == NULL) return err;
		q->inout = 0;
		append(*JNres_hshs, q);
	}
*/
	/* probe-side join-res */
/*
	*JNres_prbs = sa_list(sql->sa);
	for (node *n = exps_prb_hp->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);
		InstrPtr q = stmt_bat_new(be, t->type->localtype, prb_est * 1.1);
		if (q == NULL) return err;
		q->inout = 0;
		append(*JNres_prbs, q);
	}
*/

	/* hash-table hash-columns */
	*HSHres_hts = sa_list(sql->sa);
	for (node *n = exps_hsh_ht->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);
		bit freq = (n->next == NULL); /* last ht also computes frequencies */

		InstrPtr q = stmt_hash_new(be, t->type->localtype, hsh_est * 1.1, freq, curhash);
		if (q == NULL) return err;
		q->inout = 0;
		curhash = getArg(q, 0);
		append(*HSHres_hts, q);
	}

	/* hash-table payload-columns */
	*HSHres_hps = sa_list(sql->sa);
	int previous = curhash;
	for (node *n = exps_hsh_hp->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);

		// TODO better and separate est. for nr_slots and pld_size
		InstrPtr q = stmt_hash_new_payload(be, t->type->localtype, hsh_est * 1.1, hsh_est * 1.1, curhash, previous);
		if (q == NULL) return err;
		q->inout = 0;
		append(*HSHres_hps, q);
		previous = getArg(q, 0);
	}

	return 0;
}

stmt *
rel2bin_pp_hashjoin(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	sql_rel *rel_hsh = NULL, *rel_prb = NULL;
	stmt *pp = NULL, *sub = NULL;
	list *eq_exps = sa_list(sql->sa);
	list *hsh_hts = sa_list(sql->sa), *prb_hts = sa_list(sql->sa); /* join column exps */
	list *hsh_hps = sa_list(sql->sa), *prb_hps = sa_list(sql->sa); /* payload column exps */
	list *HSHres_hts = NULL, *HSHres_hps = NULL;
	//list *JNres_hshs = NULL, *JNres_prbs = NULL;
	int neededpp = get_need_pipeline(be); /* remember and reset previous info. */

	/* find the hash- vs probe-side */
	if (((sql_rel*)rel->l)->hashjoin) {
		rel_hsh = rel->l;
		rel_prb = rel->r;
	} else {
		assert(((sql_rel*)rel->r)->hashjoin);
		rel_hsh = rel->r;
		rel_prb = rel->l;
	}

	/* get equi-joins */
	for (node *en = rel->exps->h; en; en = en->next) {
		sql_exp *e = en->data;
		if (e->type == e_cmp && e->flag == cmp_equal)
			append(eq_exps, e);
		else // TODO get no-equi-joins
			assert(0);
	}
	/* find join column exps of hash- vs probe- side */
	for (node *n = eq_exps->h; n; n = n->next) {
		sql_exp *e = n->data, *cmpl = e->l, *cmpr = e->r;
		if (rel_find_exp(rel_hsh, cmpl)) {
			append(hsh_hts, cmpl);
			append(prb_hts, cmpr);
		} else {
			assert(rel_find_exp(rel_hsh, cmpr));
			append(hsh_hts, cmpr);
			append(prb_hts, cmpl);
		}
	}
	/* find projection columns */
	// TODO remove false positives from hsh_hps
	hsh_hps = rel_projections(sql, rel_hsh, 0, 1, 0);
	prb_hps = rel_projections(sql, rel_prb, 0, 1, 0);
	assert(hsh_hps->cnt||prb_hps->cnt); /* at least one column will be projected */

	(void)rel2bin_pphash_prepare(be, rel_hsh, rel_prb, &HSHres_hts, &HSHres_hps,
			//&JNres_hshs, &JNres_prbs,
			hsh_hts, hsh_hps);//, prb_hps);
	assert(HSHres_hts->cnt == hsh_hts->cnt);
	assert(HSHres_hps->cnt == hsh_hps->cnt);
	//assert(JNres_hshs->cnt == hsh_hps->cnt);
	//assert(JNres_prbs->cnt == prb_hps->cnt);

	/*** HASH PHASE ***/
	/* Generates the parallel block to compute a hash table */
	if (get_pipeline(be)) {
        sql_error(be->mvc, 10, SQLSTATE(42000) "Internal error: hash-join cannot start within a pipelines block");
		return NULL;
	}
	if (pp_can_not_start(be->mvc, rel_hsh)) {
		set_need_pipeline(be);
	} else {
		pp = stmt_pp_start_nrparts(be, pp_nr_slices(rel_hsh));
		set_pipeline(be, pp);
	}

	/* first construct the sub-relation */
	sub = subrel_bin(be, rel_hsh, refs);
	sub = subrel_project(be, sub, refs, rel_hsh);
	if (!sub) return NULL;

	pp = get_pipeline(be);
	if (!pp) {
		(void)get_need_pipeline(be);
		set_pipeline(be, pp = stmt_pp_start_dynamic(be, pp_dynamic_slices(be, sub)));
		sub = rel2bin_slicer(be, sub, 1);
	}

	int prnt_slts = 0, prnt_ht = 0, prnt_tt = TYPE_void;
	for (node *n = hsh_hts->h, *inout = HSHres_hts->h; n && inout;
	     n = n->next, inout = inout->next) {
		stmt *k = exp_bin(be, n->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(k); /* must find */
		int sink = getDestVar((InstrPtr)inout->data);
		int key = k->nr;
		InstrPtr q = NULL;
		if (prnt_slts == 0) {
			q = stmt_hash_build_table(be, sink, key, pp);
		} else {
			q = stmt_hash_build_combined_table(be, sink, key, prnt_slts, prnt_ht, pp);
		}
		if (q == NULL) return NULL;
		prnt_slts = getDestVar(q);
		prnt_ht = sink;
		prnt_tt = tail_type(k)->type->localtype;
	}
	/* NB: after this for-loop, prnt_* point to the last ht-column.
	 *     DO NOT modify those pointers, as they are used below.
	 */
	assert(prnt_slts && prnt_ht); /* must be set */

	// TODO put this in a function? and pass (stmt *) iso numbers?
	/* START hash.compute_frequencies() with or without payload_pos */
	InstrPtr stmt_freq = newStmt(be->mb, putName("hash"), putName("compute_frequencies"));
	if (stmt_freq == NULL)
		return NULL;
	if (hsh_hps->cnt == 0) {
		setVarType(be->mb, getArg(stmt_freq, 0), newBatType(prnt_tt));
		getArg(stmt_freq, 0) = prnt_ht;
		stmt_freq->inout = 0;
	} else {
		setVarType(be->mb, getArg(stmt_freq, 0), newBatType(TYPE_oid));
		stmt_freq = pushReturn(be->mb, stmt_freq, prnt_ht);
		stmt_freq->inout = 1;
	}
	stmt_freq = pushArgument(be->mb, stmt_freq, prnt_slts);
	stmt_freq = pushArgument(be->mb, stmt_freq, getArg(pp->q, 2) /* pipeline ptr*/);
	pushInstruction(be->mb, stmt_freq);
	/* END */

	int payload_pos = getArg(stmt_freq, 0);
	list *l_hps = sa_list(sql->sa);
	for (node *n = hsh_hps->h, *inout = HSHres_hps->h; n && inout;
	     n = n->next, inout = inout->next) {
		stmt *payload = exp_bin(be, n->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(payload); /* must find */
		InstrPtr hp_sink = inout->data;
		stmt *hp = stmt_hash_add_payload(be, hp_sink, payload, payload_pos, pp);
		if (hp == NULL) return NULL;
		append(l_hps, hp);
	}
	HSHres_hps = l_hps; /* NB: HSHres_hps now contains stmt*-s iso InstrPtr-s */

	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);

	/*** PROBE PHASE ***/
	/* Generates the parallel block to probe the hash table and produce the
	 *   parallel-hash-join results.
	 */
	if (pp_can_not_start(be->mvc, rel_prb)) {
		set_need_pipeline(be);
	} else {
		pp = stmt_pp_start_nrparts(be, pp_nr_slices(rel));
		set_pipeline(be, pp);
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

	/* HSHres_hts is in the same order as the join columns */
	int matched = 0, rhs_slts = 0;
	for (node *n = prb_hts->h, *m = HSHres_hts->h; n && m; n = n->next, m = m->next) {
		/* find n->data in sub */
		stmt *k = exp_bin(be, n->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(k); /* must find */
		int key = k->nr;
		int rht = getDestVar((InstrPtr)m->data);
		InstrPtr q = NULL;
		if (!matched) {
			q = stmt_hash_hash(be, key, pp);
			if (q == NULL) return NULL;
			q = stmt_hash_probe(be, key, getDestVar(q), rht, pp);
		} else {
			q = stmt_hash_combined_hash(be, key, matched, rhs_slts, pp);
			if (q == NULL) return NULL;
			q = stmt_hash_combined_probe(be, key, getDestVar(q), matched, rht, pp);
		}
		if (q == NULL) return NULL;
		matched = getArg(q, 0);
		rhs_slts = getArg(q, 1);
	}

	/*** FINAL PHASE ***/
	/* Construct result relations */
	assert(matched && rhs_slts); /* must be set */
	bit first = 1;
	list *l = sa_list(sql->sa);
	//list *lh = sa_list(sql->sa); /* fetch hash-side values */
	for (node *n = HSHres_hps->h, *o = hsh_hps->h; n && o; n = n->next, o = o->next) {
		InstrPtr q = stmt_hash_fetch_payload(be, rhs_slts, n->data, prnt_ht, first, pp);
		if (q == NULL) return NULL;
		first = 0;

		/* TODO: once we're sure that the `pos` result of a
		   hash.fetch_payload() is not needed,the following code should be
		   integrated into stmt_hash_fetch_payload and cleaned up
		*/
		sql_exp *e = o->data;
		stmt *s = stmt_none(be);
		s->op4.typeval = *exp_subtype(e);
		s->nr = getArg(q, 1);
		s->key = s->nrcols = 1;
		s = stmt_alias(be, s, exp_find_rel_name(e), exp_name(e));
		append(l, s);
		//append(lh, q);
	}

	//list *lp = sa_list(sql->sa); /* fetch probe-side values */
	for (node *n = prb_hps->h; n; n = n->next) {
		stmt *key = exp_bin(be, n->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		InstrPtr q = stmt_hash_expand(be, key, matched, rhs_slts, prnt_ht, first, pp);
		if (q == NULL) return NULL;
		first = 0;

		/* TODO: once we're sure that the `pos` result of a
		   hash.expand() is not needed,the following code should be
		   integrated into stmt_hash_expand and cleaned up
		*/
		sql_exp *e = n->data;
		stmt *s = stmt_none(be);
		s->op4.typeval = *exp_subtype(e);
		s->nr = getArg(q, 1);
		s->key = s->nrcols = 1;
		s->q = q;
		s = stmt_alias(be, s, exp_find_rel_name(e), exp_name(e));
		append(l, s);
		//append(lp, q);
	}

	/* project the partial results into the global variables */
/*
	list *l = sa_list(sql->sa);
	int pos = lh->cnt?getDestVar((InstrPtr)lh->h->data):getDestVar((InstrPtr)lp->h->data);

	assert(lh->cnt == JNres_hshs->cnt && lh->cnt == hsh_hps->cnt);
	for (node *n = lh->h, *m = JNres_hshs->h, *o = hsh_hps->h; n && m && o; n = n->next, m = m->next, o = o->next) {
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
		s->key = s->nrcols = 1;
		s = stmt_alias(be, s, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}

	assert(lp->cnt == JNres_prbs->cnt && lp->cnt == prb_hps->cnt);
	for (node *n = lp->h, *m = JNres_prbs->h, *o = prb_hps->h; n && m && o; n = n->next, m = m->next, o = o->next) {
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
		s->key = s->nrcols = 1;
		s = stmt_alias(be, s, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}
*/

	sub = stmt_list(be, l);

	//(void)stmt_pp_jump(be, pp, be->nrparts);
	//(void)stmt_pp_end(be, pp);

	//if (neededpp) {
		//set_pipeline(be, stmt_pp_start_dynamic(be, pp_dynamic_slices(be, sub)));
		//sub = rel2bin_slicer(be, sub, 1);
	//}
	(void) neededpp;
	return sub;
}

