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

/* Generates global shared variables:
 *   X_5:bat[:int] := hash.new(nil:int, 10:int); # <join-col1>
 *   X_6:bat[:int] := hash.new(nil:int, 10:int, X_5:bat[:int]); # <join-col2>
 *   #                                 tt       nr_slots  nr_payload  parent
 *   X_8:bat[:int] := hash.new_payload(nil:int, 10:int,   10:int,     X_6:bat[:int]); # <sel-col1>
 *   X_9:bat[:int] := hash.new_payload(nil:int, 10:int,   10:int,     X_6:bat[:int]); # <sel-col2>
 *
 * Returns: a list with two sublists
 *   `HTs`: the hash-table stmt-s
 *   `HPs`: the hash-payload stmt-s
 */
static list *
rel2bin_pphash_prepare(backend *be, BUN est, list *exps_ht, list *exps_hp)
{
	mvc *sql = be->mvc;
	list *HTres= sa_list(sql->sa), *HTs = sa_list(sql->sa), *HPs = sa_list(sql->sa);
	int curhash = 0;

	// TODO better estimation
	if (est == BUN_NONE || (ulng) est > (ulng) GDK_lng_max) {
		est = 85000000;
	}

	/* the hash columns */
	for (node *n = exps_ht->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);

		InstrPtr q = stmt_hash_new(be, t->type->localtype, est, curhash);
		if (q == NULL) return NULL;

		q->inout = 0;
		curhash = getArg(q,0);
		append(HTs, q);
	}

	/* the payload columns */
	for (node *n = exps_hp->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);

		// TODO better and separate est. for nr_slots and pld_size
		InstrPtr q = stmt_hash_new_payload(be, t->type->localtype, est, est, curhash);
		if (q == NULL) return NULL;
		q->inout = 0;

		append(HPs, q);
	}

	append(HTres, HTs);
	append(HTres, HPs);
	return HTres;
}

/* Generates the parallel block to compute a hash table:
 *
 * barrier (X_17:bit, X_18:int, X_19:ptr) := language.pipelines(2:int);
 *   leave X_17:bit := calc.>=(X_18:int, 2:int);
 *   C_21:bat[:oid] := sql.tid(X_4:int, "sys":str, "r":str, X_18:int, 2:int);
 *   X_24:bat[:int] := sql.bind(X_4:int, "sys":str, "r":str, "r1":str, 0:int, X_18:int, 2:int);
 *   X_30:bat[:int] := sql.bind(X_4:int, "sys":str, "r":str, "r2":str, 0:int, X_18:int, 2:int);
 *   X_35:bat[:int] := sql.bind(X_4:int, "sys":str, "r":str, "r3":str, 0:int, X_18:int, 2:int);
 *   X_43:bat[:int] := algebra.projection(C_21:bat[:oid], X_24:bat[:int]); # R1
 *   X_44:bat[:int] := algebra.projection(C_21:bat[:oid], X_30:bat[:int]); # R2
 *   X_45:bat[:int] := algebra.projection(C_21:bat[:oid], X_35:bat[:int]); # R3
 *   #slot_id         ht_sink                             key             PTR
 *   (X_46:bat[:oid], !X_5:bat[:int]) := hash.build_table(X_43:bat[:int], X_19:ptr);
 *   #slot_id         ht_sink                                      key             parent_slotid   parent_ht      PTR
 *   (X_48:bat[:oid], !C_6:bat[:int]) := hash.build_combined_table(X_44:bat[:int], X_46:bat[:oid], X_5:bat[:int], X_19:ptr);
 *   #hp_sink                           payload         parent_slotid   PTR
 *   !C_8:bat[:int] := hash.add_payload(X_44:bat[:int], X_48:bat[:oid], X_19:ptr);
 *   !C_9:bat[:int] := hash.add_payload(X_45:bat[:int], X_48:bat[:oid], X_19:ptr);
 *
 *   X_18:int := pipeline.counter(X_19:ptr);
 *   redo X_17:bit := calc.<(X_18:int, 2:int);
 * exit X_17:bit;
 */
stmt *
rel2bin_pp_hashjoin(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	sql_rel *rel_hsh = NULL, *rel_prb = NULL;
	stmt *pp = NULL, *sub = NULL, *cursub = NULL;
	BUN est = BUN_NONE;
	list *eq_exps = sa_list(sql->sa);
	list *hsh_hts = NULL, *hsh_hps = NULL; /* hash and payload columns of hash side */
	list *prb_hts = NULL, *prb_hps = NULL; /* hash and payload columns of probe side */
	list *htres = NULL, *htres_hts = NULL, *htres_hps = NULL; /* stmts of HT global vars in 2 sublists */
	int neededpp = rel->partition && get_and_disable_need_pipeline(be);

	/* get equi-joins */
	for (node *en = rel->exps->h; en; en = en->next) {
		sql_exp *e = en->data;
		if (e->type == e_cmp && e->flag == cmp_equal)
			append(eq_exps, e);
	}
	// TODO get no-equi-joins

	/* find the hash-table (i.e. hsh_*) vs hash-probe (i.e. prb_*) sub-rel, and
	 * for each side the hash (i.e. *_hts) and payload (i.e. *_hps) columns.
	 */
	// TODO delay deciding which side to compute hash until here?
	// TODO remove false positives from hsh_hps
	if (((sql_rel*)rel->l)->hashjoin) {
		rel_hsh = rel->l;
		rel_prb = rel->r;
	} else {
		assert(((sql_rel*)rel->r)->hashjoin);
		rel_hsh = rel->r;
		rel_prb = rel->l;
	}
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
	hsh_hps = rel_projections(sql, rel_hsh, 0, 0, 0);
	prb_hps = rel_projections(sql, rel_prb, 0, 0, 0);
	est = get_rel_count(rel_hsh);

	/* init the global variables */
	htres = rel2bin_pphash_prepare(be, est, hsh_hts, hsh_hps);
	htres_hts = htres->h->data;
	htres_hps = htres->h->next->data;

	(void)prb_hps;
	(void)htres_hts;
	(void)htres_hps;

	/* the pipelines() block to compute the hash table */
	if (pp_can_not_start(be->mvc, rel_hsh)) {
		set_need_pipeline(be);
	} else {
		pp = stmt_pp_start_nrparts(be, pp_nr_slices(rel));
		set_pipeline(be, pp);
	}

	/* first construct the sub-relation */
	sub = subrel_bin(be, rel_hsh, refs);
	sub = subrel_project(be, sub, refs, rel_hsh);
	if (!sub) return NULL;

	pp = get_pipeline(be);
	if (!pp) {
		(void)get_and_disable_need_pipeline(be);
		set_pipeline(be, pp = stmt_pp_start_dynamic(be, pp_dynamic_slices(be, sub)));
		sub = rel2bin_slicer(be, sub, 1);
	}

	/*
	for (node *n = rel->exps->h, *o = HTs->h, *p = sub->op4.lval->h;
	     n && o && p; n = n->next, o = o->next, p = p->next) {
		InstrPtr q;
		if (!parent_slt) {
			q = stmt_hash_build_table(be, *(int *) o->data, ((stmt *)p->data)->nr, pp);
		} else {
			q = stmt_hash_build_combined_table(be, *(int *) o->data, ((stmt *)p->data)->nr, parent_slt, parent_ht, pp);
		}
		if (q == NULL) return NULL;
		parent_slt = getArg(q,0);
		parent_ht = *(int *) o->data;
	}
	*/

	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);

	// FIXME what is cursub?????
	cursub = sub;

	if (neededpp) {
		set_pipeline(be, stmt_pp_start_dynamic(be, pp_dynamic_slices(be, cursub)));
		cursub = rel2bin_slicer(be, cursub, 1);
	}

	return cursub;
}

