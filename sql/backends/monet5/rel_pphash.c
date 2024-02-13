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

/* Generates global shared variables:
 *
 * X_5:bat[:int] := hash.new(nil:int, 10:int); # <join-col1>
 * X_6:bat[:int] := hash.new(nil:int, 10:int, X_5:bat[:int]); # <join-col2>
 * #                                 tt       nr_slots  nr_payload  parent
 * X_8:bat[:int] := hash.new_payload(nil:int, 10:int,   10:int,     X_6:bat[:int]); # <sel-col1>
 * X_9:bat[:int] := hash.new_payload(nil:int, 10:int,   10:int,     X_6:bat[:int]); # <sel-col2>
 */
static list *
rel2bin_pphash_prepare(backend *be, sql_rel *rel, sql_rel *rel2hsh)
{
	mvc *sql = be->mvc;
	list *HTres= sa_list(sql->sa); /* int IDs of shared HT BATs */
	BUN est = get_rel_count(rel2hsh);
	int curhash = 0;

	// TODO better estimation
	if (est == BUN_NONE || (ulng) est > (ulng) GDK_lng_max) {
		est = 85000000;
	}

	/* the join columns */
	for (node *n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_subtype *t = exp_subtype(e);

		InstrPtr q = stmt_hash_new(be, t->type->localtype, est, curhash);
		if (q == NULL) return NULL;
		q->inout = 0;
		curhash = getArg(q,0);
		append(HTres, q->argv);
	}

	/* the payload columns */
	// TODO remove the false positives!
	list *payloads = rel_projections(sql, rel2hsh, 0, 0, 0);
	for (node *n = payloads->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_subtype *t = exp_subtype(e);

		// TODO better and separate est. for nr_slots and pld_size
		InstrPtr q = stmt_hash_new_payload(be, t->type->localtype, est, est, curhash);
		if (q == NULL) return NULL;
		q->inout = 0;
		append(HTres, q->argv);
	}

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
 *   (X_46:bat[:oid], !C_5:bat[:int]) := hash.build_table(X_43:bat[:int], X_19:ptr);
 *   #slot_id         ht_sink                                      key             parent_slotid   parent_ht      PTR
 *   (X_48:bat[:oid], !C_6:bat[:int]) := hash.build_combined_table(X_44:bat[:int], X_46:bat[:oid], C_5:bat[:int], X_19:ptr);
 *   #hp_sink                           payload         parent_slotid   PTR
 *   !C_8:bat[:int] := hash.add_payload(X_44:bat[:int], X_48:bat[:oid], X_19:ptr);
 *   !C_9:bat[:int] := hash.add_payload(X_45:bat[:int], X_48:bat[:oid], X_19:ptr);
 *
 *   X_18:int := pipeline.counter(X_19:ptr);
 *   redo X_17:bit := calc.<(X_18:int, 2:int);
 * exit X_17:bit;
 */
static stmt *
rel2bin_pphash_table(list *htresults, backend *be, sql_rel *rel, list *refs)
{
	(void)htresults;
	//mvc *sql = be->mvc;
	stmt *res = NULL, *pp = NULL, *sub = NULL;
	int curhash = 0;

	/* Since we always generates a pipelines() block to compute the parallel
	 * hash table, no other active pipelines() block is allowed */
	assert(get_pipeline(be) == NULL && is_basetable(rel->op));

	pp = stmt_pp_start_nrparts(be, pp_nr_slices(rel));
	set_pipeline(be, pp);

	/* first construct the base table */
	sub = subrel_bin(be, rel, refs);
	sub = subrel_project(be, sub, refs, rel);
	if (!sub) return NULL;

	for (node *n = rel->exps->h; n; n = n->next) {
		if (!curhash) {
			// hash.build_table
		sql_exp *e = n->data;
		sql_subtype *t = exp_subtype(e);

		InstrPtr q = stmt_hash_new(be, t->type->localtype, est, curhash);
		if (q == NULL) return NULL;
		q->inout = 0;
		curhash = getArg(q,0);
		}
	}


	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);

	return res;
}

stmt *
rel2bin_pp_hashjoin(backend *be, sql_rel *rel, list *refs)
{
	//mvc *sql = be->mvc;
	sql_rel *l = rel->l, *r = rel->r;
	stmt *hsh_tbl = NULL, *res = NULL;
	list *htresults = NULL;

	if (l->hashjoin) {
		htresults = rel2bin_pphash_prepare(be, rel, rel->l);
		hsh_tbl = rel2bin_pphash_table(htresults, be, l, refs);
	} else {
		htresults = rel2bin_pphash_prepare(be, rel, rel->r);
		hsh_tbl = rel2bin_pphash_table(htresults, be, r, refs);
	}

	res = hsh_tbl;
	return res;
}

