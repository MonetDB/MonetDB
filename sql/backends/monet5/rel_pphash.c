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
rel2bin_pphash_prepare(backend *be, BUN est,
	list **HSHres_hts, list **HSHres_hps, list **JNres_hshs, list **JNres_prbs,
	list *exps_hsh_ht, list *exps_hsh_hp, list *exps_prb_hp)
{
	mvc *sql = be->mvc;
	int curhash = 0;
	int err = 1;

	// TODO better estimation
	if (est == BUN_NONE || (ulng) est > (ulng) GDK_lng_max) {
		est = 85000000;
	}

	/* hash-side join-res */
	*JNres_hshs = sa_list(sql->sa);
	for (node *n = exps_hsh_hp->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);
		InstrPtr q = stmt_bat_new(be, t->type->localtype, est * 1.1);
		if (q == NULL) return err;
		q->inout = 0;
		append(*JNres_hshs, q);
	}

	/* probe-side join-res */
	*JNres_prbs = sa_list(sql->sa);
	for (node *n = exps_prb_hp->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);
		InstrPtr q = stmt_bat_new(be, t->type->localtype, est * 1.1);
		if (q == NULL) return err;
		q->inout = 0;
		append(*JNres_prbs, q);
	}

	/* hash-table hash-columns */
	*HSHres_hts = sa_list(sql->sa);
	for (node *n = exps_hsh_ht->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);

		InstrPtr q = stmt_hash_new(be, t->type->localtype, est, curhash);
		if (q == NULL) return err;
		q->inout = 0;
		curhash = getArg(q, 0);
		append(*HSHres_hts, q);
	}

	/* hash-table payload-columns */
	*HSHres_hps = sa_list(sql->sa);
	int previous = 0;
	for (node *n = exps_hsh_hp->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);

		// TODO better and separate est. for nr_slots and pld_size
		InstrPtr q = stmt_hash_new_payload(be, t->type->localtype, est, est, curhash, previous);
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
	BUN est = BUN_NONE;
	list *eq_exps = sa_list(sql->sa);
	list *hsh_hts = sa_list(sql->sa), *prb_hts = sa_list(sql->sa); /* join column exps */
	list *hsh_hps = sa_list(sql->sa), *prb_hps = sa_list(sql->sa); /* payload column exps */
	list *HSHres_hts = NULL, *HSHres_hps = NULL;
	list *JNres_hshs = NULL, *JNres_prbs = NULL;
	int neededpp = rel->partition && get_need_pipeline(be);

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
	est = get_rel_count(rel_hsh);

	/* Example: EXPLAIN SELECT l1+r3, r2 FROM r, l WHERE r1 = l1 AND r2 = l2;
	 *
	 * init the global variables:
	 *  !X_5:bat[:int] := bat.new(nil:int, 3:int);   # l1
	 *  !X_8:bat[:int] := bat.new(nil:int, 3:int);   # l2
	 *  !X_9:bat[:int] := bat.new(nil:int, 3:int);   # r1
	 *  !X_10:bat[:int] := bat.new(nil:int, 3:int);  # r2
	 *  !X_11:bat[:int] := bat.new(nil:int, 3:int);  # r3
	 *  !X_12:bat[:int] := hash.new(nil:int, 3:int); # l1
	 *  !X_13:bat[:int] := hash.new(nil:int, 3:int, X_12:bat[:int]); # l2
	 *  !X_14:bat[:int] := hash.new_payload(nil:int, 3:lng, 3:lng, X_13:bat[:int], X_13:bat[:int]); # l1
	 *  !X_17:bat[:int] := hash.new_payload(nil:int, 3:lng, 3:lng, X_13:bat[:int], X_14:bat[:int]); # l2
	 */
	(void)rel2bin_pphash_prepare(be, est, &HSHres_hts, &HSHres_hps, &JNres_hshs, &JNres_prbs, hsh_hts, hsh_hps, prb_hps);
	assert(HSHres_hts->cnt == hsh_hts->cnt);
	assert(HSHres_hps->cnt == hsh_hps->cnt);
	assert(JNres_hshs->cnt == hsh_hps->cnt);
	assert(JNres_prbs->cnt == prb_hps->cnt);

	/*** HASH PHASE ***/
	/* Generates the parallel block to compute a hash table:
	 *
	 * barrier (X_18:bit, X_19:int, X_20:ptr) := language.pipelines(2:int);
	 *   leave X_18:bit := calc.>=(X_19:int, 2:int);
	 *   ...
	 *   X_37:bat[:int] := algebra.projection(...); # l1
	 *   X_38:bat[:int] := algebra.projection(...); # l2
	 *   (X_39:bat[:oid], !X_12:bat[:int]) := hash.build_table(X_37:bat[:int], X_20:ptr);
	 *   (X_40:bat[:oid], !X_13:bat[:int]) := hash.build_combined_table(X_38:bat[:int], X_39:bat[:oid], X_12:bat[:int], X_20:ptr);
	 *   !X_14:bat[:int] := hash.add_payload(X_37:bat[:int], X_40:bat[:oid], X_13:bat[:int], X_20:ptr);
	 *   !X_17:bat[:int] := hash.add_payload(X_38:bat[:int], X_40:bat[:oid], X_13:bat[:int], X_20:ptr);
	 *   X_19:int := pipeline.counter(X_20:ptr);
	 *   redo X_18:bit := calc.<(X_19:int, 2:int);
	 * exit X_18:bit;
	 */
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

	int prnt_slts = 0, prnt_ht = 0;
	for (node *n = hsh_hts->h, *inout = HSHres_hts->h; n && inout;
	     n = n->next, inout = inout->next) {
		stmt *k = exp_bin(be, n->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(k); /* must find */
		int sink = getDestVar((InstrPtr)inout->data);
		int key = getDestVar(k->q);
		InstrPtr q = NULL;
		if (prnt_slts == 0) {
			q = stmt_hash_build_table(be, sink, key, pp);
		} else {
			q = stmt_hash_build_combined_table(be, sink, key, prnt_slts, prnt_ht, pp);
		}
		if (q == NULL) return NULL;
		prnt_slts = getDestVar(q);
		prnt_ht = sink;
	}

	assert(prnt_slts); /* must be set */
	list *l_hps = sa_list(sql->sa);
	for (node *n = hsh_hps->h, *inout = HSHres_hps->h; n && inout;
	     n = n->next, inout = inout->next) {
		stmt *payload = exp_bin(be, n->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(payload); /* must find */
		InstrPtr sink = inout->data;
		stmt *hp = stmt_hash_add_payload(be, sink, payload, prnt_slts, prnt_ht, pp);
		if (hp == NULL) return NULL;
		append(l_hps, hp);
	}
	HSHres_hps = l_hps; /* NB: HSHres_hps now contains stmt*-s iso InstrPtr-s */

	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);

	/*** PROBE PHASE ***/
	/* Generates the parallel block to probe the hash table and produce the
	 *   parallel-hash-join results.
	 *
	 * barrier (X_46:bit, X_47:int, X_48:ptr) := language.pipelines(2:int);
	 *   leave X_46:bit := calc.>=(X_47:int, 2:int);
	 *   ...
	 *   X_73:bat[:int] := algebra.projection(...); # r1
	 *   X_74:bat[:int] := algebra.projection(...); # r2
	 *   X_75:bat[:int] := algebra.projection(...); # r3
	 *   ### First LHS join column:
	 *   X_76:bat[:lng] := hash.hash(X_73:bat[:int], X_48:ptr);
	 *   (X_77:bat[:oid], X_78:bat[:oid]) := hash.probe(X_73:bat[:int], X_76:bat[:lng], X_12:bat[:int], X_48:ptr);
	 *   ### Subsequent LHS join columns:
	 *   X_79:bat[:lng] := hash.combined_hash(X_74:bat[:int], X_77:bat[:oid], X_78:bat[:oid], X_48:ptr);
	 *   (X_80:bat[:oid], X_81:bat[:oid]) := hash.combined_probe(X_74:bat[:int], X_79:bat[:lng], X_77:bat[:oid], X_13:bat[:int], X_48:ptr);
	 *
	 *   ### Generate output
	 *   # For the RHS columns, fetch values from hash-payload
	 *   (X_82:bat[:oid], X_83:bat[:int]) := hash.fetch_payload(X_81:bat[:oid], X_14:bat[:int], true:bit, X_48:ptr);
	 *   (X_85:bat[:oid], X_86:bat[:int]) := hash.fetch_payload(X_81:bat[:oid], X_17:bat[:int], false:bit, X_48:ptr);
	 *   # For the LHS columns, repeat each matched value.
	 *   (X_88:bat[:oid], X_89:bat[:int]) := hash.expand(X_73:bat[:int], X_80:bat[:oid], X_81:bat[:oid], X_14:bat[:int], false:bit, X_48:ptr);
	 *   (X_90:bat[:oid], X_91:bat[:int]) := hash.expand(X_74:bat[:int], X_80:bat[:oid], X_81:bat[:oid], X_14:bat[:int], false:bit, X_48:ptr);
	 *   (X_92:bat[:oid], X_93:bat[:int]) := hash.expand(X_75:bat[:int], X_80:bat[:oid], X_81:bat[:oid], X_14:bat[:int], false:bit, X_48:ptr);
	 *   # Put them all int the global BATs. NB, there are no holes, so, no thetaselect needed.
	 *   !X_5:bat[:int] := algebra.projection(X_82:bat[:oid], X_83:bat[:int], X_48:ptr);
	 *   !X_8:bat[:int] := algebra.projection(X_82:bat[:oid], X_86:bat[:int], X_48:ptr);
	 *   !X_9:bat[:int] := algebra.projection(X_82:bat[:oid], X_89:bat[:int], X_48:ptr);
	 *   !X_10:bat[:int] := algebra.projection(X_82:bat[:oid], X_91:bat[:int], X_48:ptr);
	 *   !X_11:bat[:int] := algebra.projection(X_82:bat[:oid], X_93:bat[:int], X_48:ptr);
	 *   X_47:int := pipeline.counter(X_48:ptr);
	 *   redo X_46:bit := calc.<(X_47:int, 2:int);
	 * exit X_46:bit;
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
		int key = getDestVar(k->q);
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
	list *lh = sa_list(sql->sa); /* fetch hash-side values */
	for (node *n = HSHres_hps->h; n; n = n->next) {
		InstrPtr q = stmt_hash_fetch_payload(be, rhs_slts, n->data, first, pp);
		if (q == NULL) return NULL;
		first = 0;
		append(lh, q);
	}
	list *lp = sa_list(sql->sa); /* fetch probe-side values */
	int rhp = getDestVar(((stmt *)HSHres_hps->h->data)->q);
	for (node *n = prb_hps->h; n; n = n->next) {
		stmt *key = exp_bin(be, n->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		InstrPtr q = stmt_hash_expand(be, key, matched, rhs_slts, rhp, first, pp);
		if (q == NULL) return NULL;
		first = 0;
		append(lp, q);
	}

	/* projet the partial results into the global variables */
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
		q = pushArgument(be->mb, q, getArg(pp->q, 2)); /* pipeline ptr */
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
		q = pushArgument(be->mb, q, getArg(pp->q, 2)); /* pipeline ptr */
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
	sub = stmt_list(be, l);

	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);

	if (neededpp) {
		set_pipeline(be, stmt_pp_start_dynamic(be, pp_dynamic_slices(be, sub)));
		sub = rel2bin_slicer(be, sub, 1);
	}
	return sub;
}

