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
#include "rel_physical.h"
#include "sql_pp_statement.h"
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
_start_pp(backend *be, sql_rel *rel, bit buildphase, list *refs)
{
	stmt *sub = NULL, *pp = NULL;

	if (buildphase && get_pipeline(be)) {
        sql_error(be->mvc, 10, SQLSTATE(42000) "Internal error: hash-join cannot start within a pipelines block");
		return NULL;
	}
	if (!be->pp)
		set_need_pipeline(be);

	/* first construct the sub-relation */
	sub = subrel_bin(be, rel, refs);
	sub = subrel_project(be, sub, refs, rel);
	if (sub) {
		pp = get_pipeline(be);
		if (!pp) {
		assert(0);
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
static stmt *
oahash_prepare_bld_ht(backend *be, const list *exps, lng sz)
{
	assert(exps && exps->cnt);

	list *l = sa_list(be->mvc->sa);
	int curhash = 0;
	for (node *n = exps->h; n; n = n->next) {
		stmt *s = stmt_oahash_new(be, exp_subtype((sql_exp*)n->data), sz, curhash);
		if (s == NULL) return NULL;
		curhash = s->nr;
		append(l, s);
	}
	assert(l->cnt == exps->cnt);
	return stmt_list(be, l);
}

static stmt *
oahash_prepare_bld_hp(backend *be, const list *exps_prj_hsh, lng sz)
{
	list *l = sa_list(be->mvc->sa);
	for (node *n = exps_prj_hsh->h; n; n = n->next) {
		sql_subtype *t = exp_subtype((sql_exp*)n->data);
		stmt *s = stmt_bat_new(be, t, sz);
		if (!s)
			return NULL;
		append(l, s);
	}
	assert(l->cnt == exps_prj_hsh->cnt);
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
	stmt *ht = sub->op2?sub->op2:sub->op4.lval->t->data;
	assert(ht);
	stmt *slice = stmt_nth_slice(be, ht, 1, true);
	sub = sub->op1; /* need the payload */
	for (node *n = sub->op4.lval->h; n; n = n->next) {
		stmt *sc = n->data;
		const char *cname = column_name(be->mvc->sa, sc);
		const char *tname = table_name(be->mvc->sa, sc);
		int label = sc->label;

		sc = column(be, sc);
		sc = stmt_project(be, slice, sc);
		list_append(newl, stmt_alias(be, sc, label, tname, cname));
	}
	sub = stmt_list(be, newl);
	return sub;
}

/* Generates the parallel block to probe the hash table
 */
static stmt *
oahash_probe(backend *be, sql_rel *rel, list *jexps, list *exps_cmp_prb, const stmt *stmts_ht, stmt *sub, const stmt *pp, bool anti, bool outer, bool groupjoin, bool has_outerselect, stmt **nulls, stmt **mrk)
{
	stmt *prb_res = NULL, *outerm = NULL;

	/* stmts_ht is in the same order as the join columns */
	for (node *n = exps_cmp_prb->h, *m = stmts_ht->op4.lval->h, *o = jexps->h; n && m && o; n = n->next, m = m->next, o = o->next) {
		sql_exp *e = n->data;
		sql_exp *e2 = o->data;
		stmt *key = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		key = column(be, key);
		bool single = ((rel->single == 1) && (n->next == NULL) && !has_outerselect);
		bool eq = (e2->flag == cmp_equal) && !anti;
		bool grpjoin = groupjoin && is_any(e2);

		stmt *hsh = stmt_oahash_hash(be, key, prb_res, m->data);
		if (hsh == NULL) return NULL;
		prb_res = stmt_oahash_probe(be, key, hsh, prb_res, m->data, stmts_ht->op3, outerm, single, e2->semantics, eq, outer, grpjoin, pp);
		if (prb_res == NULL) return NULL;

		if (outer || grpjoin) {
			assert (prb_res->q->retc == 3);
			outerm = stmt_blackbox_result(be, prb_res->q, 2, sql_fetch_localtype(TYPE_bit));
		}

		if (nulls && stmt_has_null(key) && is_any(e2))
			*nulls = stmt_selectnil(be, key, *nulls);
	}
	if (outer || groupjoin) {
		assert(mrk && outerm);
		*mrk = outerm;
	}
	/* probe of last column is the final res */
	return prb_res;
}

static list *
oahash_project_hsh(backend *be, list *exps_prj_hsh, stmt *stmts_hp, stmt *prb_res, const stmt *freq, const stmt *ht_sink, bool outer, bool groupedjoin, const stmt *pp, stmt **mrk /* returns outer match or not */)
{
	list *l = sa_list(be->mvc->sa);

	if (outer && mrk && !*mrk) /* ToDo some how expand the mrk */
		assert(0);

	if ((outer || groupedjoin) && mrk && *mrk) {
		stmt *s = stmt_oahash_expand(be, *mrk, prb_res, freq, outer, pp);
		if (s == NULL) return NULL;
		*mrk = s;
	}
	if (list_empty(exps_prj_hsh))
		return l;
	assert(ht_sink);
	stmt *sel = stmt_oahash_explode(be, prb_res, freq, ht_sink, outer, sql_fetch_localtype(TYPE_oid));

	for (node *o = exps_prj_hsh->h; o; o = o->next) {
		sql_exp *e = o->data;
		stmt *hp_sink = exp_bin(be, e, stmts_hp, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(hp_sink); /* must find */

		stmt *s = stmt_project(be, sel, hp_sink);
		if (s == NULL) return NULL;
		if (e->alias.label)
			s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}
	return l;
}

static list *
oahash_project_prb(backend *be, list *exps_prj_prb, stmt *prb_res, const stmt *freq, bit outer, stmt *sub, const stmt *pp, InstrPtr *probed_rowids)
{
	list *l = sa_list(be->mvc->sa);

	stmt *icol = NULL;

	for (node *o = exps_prj_prb->h; o; o = o->next) {
		sql_exp *e = o->data;
		stmt *key = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		key = column(be, key);

		icol = key;

		/* TODO split expand into parts ! */
		stmt *s = stmt_oahash_expand(be, key, prb_res, freq, outer, pp);
		if (s == NULL) return NULL;
		if (e->alias.label)
			s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}
	/* TODO !! */
	if (icol && probed_rowids) { /* the rowids are needed for post processing semi/anti joins based on the probe side row ids */
		stmt *rids = stmt_mirror(be, icol);

		stmt *s = stmt_oahash_expand(be, rids, prb_res, freq, outer, pp);
		if (s == NULL) return NULL;
		*probed_rowids = s->q;
	}
	return l;
}

static list *
oahash_project_single(backend *be, list *exps_prj, stmt *prb_res, stmt *sub, const stmt *pp, InstrPtr *probed_rowids)
{
	list *l = sa_list(be->mvc->sa);

	stmt *icol = NULL;
	for (node *o = exps_prj->h; o; o = o->next) {
		stmt *key = exp_bin(be, o->data, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		key = column(be, key);

		icol = key;
		stmt *s = stmt_oahash_project(be, key, prb_res, pp);
		if (s == NULL) return NULL;

		sql_exp *e = o->data;
		if (e->alias.label)
			s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}
	if (icol && probed_rowids) { /* the rowids are needed for post processing semi/anti joins based on the probe side row ids */
		stmt *rids = stmt_mirror(be, icol);

		stmt *s = stmt_oahash_project(be, rids, prb_res, pp);
		if (s == NULL) return NULL;
		*probed_rowids = s->q;
	}
	return l;
}

static list *
oahash_project_cart(backend *be, str func, list *exps_prj, stmt *sub, stmt *repeat, bit LRouter, const stmt *pp, InstrPtr *rowids)
{
	list *l = sa_list(be->mvc->sa);
	stmt *icol = NULL;

	stmt *rpt = column(be, repeat);
	if (list_empty(exps_prj)) {
		for (node *o = sub->op4.lval->h; o; o = o->next) {
			stmt *okey = o->data, *key = column(be, okey);
			icol = key;
			stmt *s = stmt_oahash_project_cart(be, key, rpt, func, LRouter, pp);
			if (s == NULL) return NULL;
			if (okey->label)
				s = stmt_alias(be, s, okey->label, table_name(be->mvc->sa, okey), column_name(be->mvc->sa, okey));
			append(l, s);
		}
		if (icol && rowids) { /* the rowids are needed for post processing semi/anti joins based on the probe side row ids */
			stmt *rids = stmt_mirror(be, icol);
			stmt *s = stmt_oahash_project_cart(be, rids, rpt, func, LRouter, pp);
			if (s == NULL) return NULL;
			*rowids = s->q;
		}
		return l;
	}

	for (node *o = exps_prj->h; o; o = o->next) {
		sql_exp *e = o->data;
		stmt *key = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		key = column(be, key);
		icol = key;

		stmt *s = stmt_oahash_project_cart(be, key, rpt, func, LRouter, pp);
		if (s == NULL) return NULL;
		if (e->alias.label)
			s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}
	if (icol && rowids) { /* the rowids are needed for post processing semi/anti joins based on the probe side row ids */
		stmt *rids = stmt_mirror(be, icol);
		stmt *s = stmt_oahash_project_cart(be, rids, rpt, func, LRouter, pp);
		if (s == NULL) return NULL;
		*rowids = s->q;
	}
	return l;
}

stmt *
rel2bin_oahash_build(backend *be, sql_rel *rel, list *refs)
{
	stmt *stmts_ht = NULL;

	/*** HASH PHASE ***/
	list *exps_cmp_hsh = rel->attr;
	list *exps_prj_hsh = rel->exps;

	if (list_empty(exps_cmp_hsh)) { /* dummy case for cartisian product */
		sql_rel *l = rel->l;
		if (is_topn(l->op))
			l = rel_project(be->mvc->sa, l, rel_projections(be->mvc, l, NULL, 1, 1));
		stmt *sub = rel2bin_materialize(be, l, refs, false);
		if (sub->cand)
			sub = subrel_project(be, sub, refs, rel);
		return sub;
	}

	bool need_freq = (rel->flag != (int)op_semi || rel->ref.refcnt > 2 || !list_empty(exps_prj_hsh));
	lng bld_sz = _estimate(be->mvc, rel); /* TODO: change into dynamic where possible ?? */
	stmt *shared_ht = oahash_prepare_bld_ht(be, exps_cmp_hsh, bld_sz);
	stmt *freq = NULL, *pld_sltid = NULL;
	if (need_freq) {
		freq = stmt_bat_new(be, sql_fetch_localtype(TYPE_lng), bld_sz);
		if (!list_empty(exps_prj_hsh)) {
			list *l = shared_ht->op4.lval;
			stmt *prnt = (stmt*)l->t->data;
			pld_sltid = stmt_oahash_new(be, sql_fetch_localtype(TYPE_oid), bld_sz, prnt->nr);
			if (pld_sltid == NULL) return NULL;
		}
	}
	stmt *shared_hp = NULL;
	if (exps_prj_hsh) {
		shared_hp = oahash_prepare_bld_hp(be, exps_prj_hsh, bld_sz);
	}

	stmt *sub = _start_pp(be, rel->l, true, refs);
	if (!sub) return NULL;

	stmt *pp = get_pipeline(be);
	/* BUILD HT */
	list *l = sa_list(be->mvc->sa);
	stmt *prnt = NULL;
	for (node *n = exps_cmp_hsh->h, *inout = shared_ht->op4.lval->h; n && inout; n = n->next, inout = inout->next) {
		sql_exp *e = n->data;
		stmt *ht = inout->data;
		stmt *key = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		key = column(be, key);

		prnt = stmt_oahash_build_ht(be, ht, key, prnt, pp);
		if (prnt == NULL) return NULL;

		if (e->alias.label)
			ht = stmt_alias(be, ht, e->alias.label, exp_find_rel_name(e), exp_name(e));
		append(l, ht);
	}
	stmts_ht = stmt_list(be, l);

	if (freq) {
		stmt *s = stmt_oahash_frequency(be, freq, prnt, (pld_sltid != NULL), pp);

		if (pld_sltid) {
			prnt = stmt_oahash_build_ht(be, pld_sltid, s, prnt, pp);
			if (prnt == NULL) return NULL;
		}
	}

	/* BUILD HP */
	stmt *stmts_hp = NULL;
	if (shared_hp) {
		list *ll = sa_list(be->mvc->sa);
		for (node *n = exps_prj_hsh->h, *inout = shared_hp->op4.lval->h; n && inout; n = n->next, inout = inout->next) {
			sql_exp *e = n->data;
			stmt *res = inout->data;
			stmt *payload = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
			assert(payload); /* must find */
			payload = column(be, payload);
			stmt *s = stmt_algebra_project(be, res, prnt, payload, pp);
			if (e->alias.label)
				s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
			append(ll, s);
		}
		stmts_hp = stmt_list(be, ll);
	}

	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);

	stmts_ht->op1 = stmts_hp;
	stmts_ht->op2 = pld_sltid;
	stmts_ht->op3 = freq;
	return stmts_ht;
}

static bool
groupjoin_mark( list *attr )
{
	bool mark = false;
	if (!list_empty(attr) && list_length(attr) == 1) {
        sql_exp *e = attr->h->data;
        if (exp_is_atom(e))
            mark = true;
	}
	return mark;
}

static stmt *
rel2bin_oahash_equi_join(backend *be, sql_rel *rel, list *refs, list *jexps, InstrPtr *probed_rowids, stmt **probe_sub, stmt **nulls, stmt **mrk, list **probe_side, list **hash_side, bool has_outerselect)
{
	sql_rel *rel_hsh = NULL, *rel_prb = NULL;

	/* start new parallel block after join. NB get_need_pipeline has side effect! */
	int neededpp = (rel->spb || rel->partition) && get_need_pipeline(be);
	(void)neededpp;

	assert(rel->oahash == 1 || rel->oahash == 2);
	if (rel->oahash == 1) {
		rel_hsh = rel->l;
		rel_prb = rel->r;
	} else { /* rel->oahash == 2 */
		rel_hsh = rel->r;
		rel_prb = rel->l;
	}
	list *exps_cmp_prb = rel_prb->attr;
	list *exps_prj_prb = rel_prb->exps;
	list *exps_prj_hsh = rel_hsh->exps;

	assert(list_length(jexps) == list_length(exps_cmp_prb));
	/* build-phase res: hash-table and hash-payload stmts */
	stmt *stmts_ht = refs_find_rel(refs, rel_hsh);
	assert(stmts_ht);
	stmt *stmts_hp = stmts_ht->op1;
	bool groupedjoin = (!list_empty(rel->attr)), mark = groupjoin_mark(rel->attr);

	/*** PROBE PHASE ***/
	stmt *sub = _start_pp(be, rel_prb->l, false, refs);
	if (!sub) return NULL;
	if (probe_sub)
		*probe_sub = sub;

	stmt *pp = get_pipeline(be);
	stmt *prb_res = oahash_probe(be, rel, jexps, exps_cmp_prb, stmts_ht, sub, pp, false, is_outerjoin(rel->op), mark, has_outerselect, nulls, mrk);
	if (prb_res == NULL) return NULL;

	/*** PROJECT RESULT PHASE ***/
	bit outer = is_outerjoin(rel->op);
	list *lp = oahash_project_prb(be, exps_prj_prb, prb_res, stmts_ht->op3, outer, sub, pp, probed_rowids);
	list *lh = oahash_project_hsh(be, exps_prj_hsh, stmts_hp, prb_res, stmts_ht->op3, stmts_ht->op2, outer, groupedjoin, pp, mrk);
	assert(lh->cnt || lp->cnt || mrk);

	if (probe_side)
		*probe_side = lp;
	if (hash_side)
		*hash_side = lh;
	list *ln = sa_list(be->mvc->sa);
	if (rel->oahash == 1)
		lh = list_merge(list_merge(ln, lh, NULL), lp, NULL);
	else
		lh = list_merge(list_merge(ln, lp, NULL), lh, NULL);
	return stmt_list(be, lh);
}

static stmt *
rel2bin_oahash_select(backend *be, stmt *sub, list *sexps, sql_rel *rel)
{
	stmt *sel = NULL;

	for (node *en = sexps->h ; en; en = en->next) {
		stmt *s = NULL;

		if (rel->op == op_anti) {
			sql_exp *e = en->data;
			s = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL /* sel */, 0, 0/* just the project call not the select*/, 0);
			if (is_any(e)) {
				/* ifthenelse if (not(predicate)) then false else true (needed for antijoin) */
				sql_subtype *bt = sql_fetch_localtype(TYPE_bit);
				sql_subfunc *not = sql_bind_func(be->mvc, "sys", "not", bt, NULL, F_FUNC, true, true);
				s = stmt_unop(be, s, NULL, not);
				s = sql_Nop_(be, "ifthenelse", s, stmt_bool(be, 0), stmt_bool(be, 1), NULL);
			}

            if (s && s->nrcols == 0) {
                stmt *l = bin_find_smallest_column(be, sub);
                s = stmt_uselect(be, stmt_const(be, l, s), stmt_bool(be, 1), cmp_equal, sel, 0, 0);
            } else if (s) {
                s = stmt_uselect(be, s, stmt_bool(be, 1), cmp_equal, sel, 0, 0);
            }
		} else {
			s = exp_bin(be, en->data, sub, NULL, NULL, NULL, NULL, sel, 0, 1, 0);
			if (s && s->nrcols == 0) {
				stmt *l = bin_find_smallest_column(be, sub);
				s = stmt_uselect(be, stmt_const(be, l, stmt_bool(be, 1)), s, cmp_equal, sel, 0, 0);
			}
		}

		if (!s) {
			assert(be->mvc->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
			return NULL;
		}
		sel = s;
	}
	return sel;
}

static stmt *
rel2bin_oahash_outerselect(backend *be, stmt *sub, list *sexps, sql_rel *rel, InstrPtr probed_ids, InstrPtr hash_ids, stmt **mrk, bool cart, bool mark)
{
	assert (rel->op == op_left || rel->op == op_right || !list_empty(rel->attr));
	stmt *sel = NULL, *gids = NULL, *m = mrk?*mrk:NULL;

	if (hash_ids) {
		/* 0 == empty (no matches possible), nil - no match (but has nil), 1 match */
		if (cart) {
			stmt *rids = stmt_blackbox_result(be, hash_ids, 0, sql_fetch_localtype(TYPE_oid));
			m = sql_Nop_(be, "ifthenelse", sql_unop_(be, "isnull", rids), stmt_bool(be, false), stmt_bool(be, true), NULL);
		}
	}

	if (probed_ids)
		gids = stmt_blackbox_result(be, probed_ids, 0, sql_fetch_localtype(TYPE_oid));
	if (!m && gids)
		m = stmt_project(be, gids, stmt_bool(be, true));
	for (node *en = sexps->h ; en; en = en->next) {
		stmt *s = NULL;
		sql_exp *e = en->data;
		stmt *p = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, sel, 0, 0, 0);
		if (sel && p && (!p->cand || p->cand != sel))
			p = stmt_project(be, sel, p);
		if (p && p->nrcols == 0)
			p = stmt_const(be, m, p);
		stmt *lgids = gids;
		if (sel)
			lgids = stmt_project(be, sel, lgids);
		stmt *outer = NULL;
		if (en->next || !mark)
			outer = stmt_outerselect(be, lgids, m, p, is_any(e), !en->next && is_single(rel));
		else
			outer = stmt_markselect(be, lgids, m, p, is_any(e));
		s = stmt_result(be, outer, 0);
		m = stmt_result(be, outer, 1);
		if (sel)
			s = stmt_project(be, s, sel);
		if (!s) {
			assert(be->mvc->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
			return NULL;
		}
		sel = s;
		if (mrk)
			*mrk = m;
	}
	return sel;
}

static stmt *
rel2bin_oahash_cart(backend *be, sql_rel *rel, list *refs, InstrPtr *probed_rowids, stmt **probe_sub, list **probe_side,
		list **hash_side, InstrPtr *hash_rowids)
{
	sql_rel *rel_hsh = NULL, *rel_prb = NULL;

	/* start new parallel block after join. NB get_need_pipeline has side effect! */
	int neededpp = (rel->spb || rel->partition) && get_need_pipeline(be);
	(void)neededpp;

	assert(rel->oahash == 1 || rel->oahash == 2);
	if (rel->oahash == 1) {
		rel_hsh = rel->l;
		rel_prb = rel->r;
	} else { /* rel->oahash == 2 */
		rel_hsh = rel->r;
		rel_prb = rel->l;
	}
	list *exps_prj_prb = rel_prb->exps;
	list *exps_prj_hsh = rel_hsh->exps;

	/*** (pseudo) HASH PHASE ***/
	/* nothing to hash, we just want to have a materialised table for this side */
	stmt *stmts_ht = subrel_bin(be, rel_hsh, refs);
	stmts_ht = subrel_project(be, stmts_ht, refs, rel);

	/*** (pseudo) PROBE PHASE ***/
	stmt *stmts_prb_res = _start_pp(be, rel_prb->l, false, refs);
	if (!stmts_prb_res) return NULL;
	if (probe_sub)
		*probe_sub = stmts_prb_res;

	stmt *pp = get_pipeline(be);

	/* at least one column should be projected */
	assert(list_length(stmts_ht->op4.lval)||list_length(stmts_prb_res->op4.lval));

	/*** PROJECT RESULT PHASE ***/
	assert(stmts_ht->type == st_list && stmts_prb_res->type == st_list);

	stmt *rowrepeat = stmts_ht->op4.lval->h->data;
	stmt *setrepeat = stmts_prb_res->op4.lval->h->data;
	assert(rowrepeat && setrepeat); /* must find */

	/* Check for single left/right outer join and crossproduct that either rel->r is single or rel->l is empty. */
	if (rel->single && list_empty(rel->exps)) {
		if (rowrepeat->nrcols > 0) {
			int ppln = be->pipeline;
			be->pipeline = 0;
			sql_subfunc *cnt_fnc = sql_bind_func(be->mvc, "sys", "count", sql_fetch_localtype(TYPE_void), NULL, F_AGGR, true, true);
			stmt *cnt = NULL;

			stmt *s = stmt_none(be);
			s->nrcols = 0;

			cnt = stmt_aggr(be, rowrepeat, NULL, NULL, cnt_fnc, 1, 0, 1);
			InstrPtr qR = newStmtArgs(be->mb, calcRef, ">", 3);
			qR = pushArgument(be->mb, qR, cnt->nr);
			qR = pushInt(be->mb, qR, 1);
			pushInstruction(be->mb, qR);
			s->nr = qR->argv[0];
			s->q = qR;

			if (setrepeat->nrcols > 0) {
				cnt = stmt_aggr(be, setrepeat, NULL, NULL, cnt_fnc, 1, 0, 1);
				InstrPtr qL = newStmtArgs(be->mb, calcRef, ">", 3);
				qL = pushArgument(be->mb, qL, cnt->nr);
				qL = pushInt(be->mb, qL, 0);
				pushInstruction(be->mb, qL);

				InstrPtr q = newStmtArgs(be->mb, calcRef, "and", 3);
				q = pushArgument(be->mb, q, qR->argv[0]);
				q = pushArgument(be->mb, q, qL->argv[0]);
				pushInstruction(be->mb, q);
				s->nr = q->argv[0];
				s->q = q;
			}
			(void)stmt_exception(be, s, SQLSTATE(42000) "more than one match", 00001);

			be->pipeline = ppln;
		}
	}

	bit LRouter = (is_left(rel->op) || is_right(rel->op) || (rel->op == op_anti && list_empty(rel->exps)));
	list *lp = oahash_project_cart(be, "expand_cartesian", exps_prj_prb, stmts_prb_res, rowrepeat, LRouter, pp, probed_rowids);
	list *lh = oahash_project_cart(be, "fetch_payload_cartesian", exps_prj_hsh, stmts_ht, setrepeat, LRouter, pp, hash_rowids);
	assert(lh->cnt || lp->cnt);

	if (probe_side)
		*probe_side = lp;
	if (hash_side)
		*hash_side = lh;
	list *ln = sa_list(be->mvc->sa);
	lh = list_merge(list_merge(ln, lh, NULL), lp, NULL);
	return stmt_list(be, lh);
}

static stmt *
rel2bin_oahash_groupjoin(backend *be, sql_rel *rel, list *refs)
{
	/* current code only handles markjoins here, no general groupjoin */
	/* todo start with normal join exps?? */
	assert(!list_empty(rel->attr));
	stmt *sub = NULL, *probe_sub = NULL;
	list *jexps = sa_list(be->mvc->sa), *sexps = sa_list(be->mvc->sa), *probe_side = NULL, *hash_side = NULL;
	split_join_exps_pp(rel, jexps, sexps, false);
	InstrPtr probed_ids = NULL, hash_ids = NULL;
	stmt *mrk = NULL;
	bool mark = false, exist = true;

	if (list_length(rel->attr) == 1) {
        sql_exp *e = rel->attr->h->data;
        if (exp_is_atom(e))
            mark = true;
        if (exp_is_atom(e) && exp_is_false(e))
            exist = false;
    }

	if (list_empty(jexps)) { /* cartesian */
		sub = rel2bin_oahash_cart(be, rel, refs, &probed_ids, &probe_sub, &probe_side, &hash_side, &hash_ids);
	} else {
		sub = rel2bin_oahash_equi_join(be, rel, refs, jexps, &probed_ids, &probe_sub, NULL, &mrk, &probe_side, &hash_side, !list_empty(sexps));
	}
	if (list_empty(jexps) && list_empty(sexps) && mark) {
		sql_exp *e = exp_atom_bool(be->mvc->sa, true);
		set_any(e);
		append(sexps,e);
	}
	if (!list_empty(sexps)) {
		stmt *sel = rel2bin_oahash_outerselect(be, sub, sexps, rel, probed_ids, hash_ids, &mrk, list_empty(jexps), mark);
		list *res = sa_list(be->mvc->sa);
		for (node *n = probe_side->h; n; n = n->next) {
			stmt *c = stmt_project(be, sel, n->data);
			const char *cname = column_name(be->mvc->sa, c);
			const char *tname = table_name(be->mvc->sa, c);
			int label = c->label;
			c = stmt_alias(be, c, label, tname, cname);
			append(res, c);
		}
		for (node *n = hash_side->h; n; n = n->next) {
			stmt *c = stmt_project(be, sel, n->data);
			const char *cname = column_name(be->mvc->sa, c);
			const char *tname = table_name(be->mvc->sa, c);
			int label = c->label;
			c = sql_Nop_(be, "ifthenelse", mrk, c, stmt_atom(be, atom_general(be->mvc->sa, tail_type(c), NULL, 0)), NULL);
			c = stmt_alias(be, c, label, tname, cname);
			append(res, c);
		}
		sub = stmt_list(be, res);
	}
	if (list_length(rel->attr)) {
		if (mark) {
			sql_exp *e = rel->attr->h->data;
			const char *rnme = exp_relname(e);
			const char *nme = exp_name(e);
			if (list_empty(jexps) && !mrk) {
				stmt *rids = stmt_blackbox_result(be, hash_ids, 0, sql_fetch_localtype(TYPE_oid));
				mrk = sql_Nop_(be, "ifthenelse", sql_unop_(be, "isnull", rids), stmt_bool(be, !exist), stmt_bool(be, exist), NULL);
			} else {
				assert(mrk);
				if (exp_is_atom(e) && need_no_nil(e)) /* exclude nulls */
					mrk = sql_Nop_(be, "ifthenelse", sql_unop_(be, "isnull", mrk), stmt_bool(be, false), mrk, NULL);
				if (!exist) {
					sql_subtype *bt = sql_fetch_localtype(TYPE_bit);
					sql_subfunc *not = sql_bind_func(be->mvc, "sys", "not", bt, NULL, F_FUNC, true, true);
					mrk = stmt_unop(be, mrk, NULL, not);
				}
			}
			stmt *s = stmt_alias(be, mrk, e->alias.label, rnme, nme);
			append(sub->op4.lval, s);
		} else {
			assert(0);
		}
	}
	return sub;
}

static stmt *
rel2bin_oahash_join(backend *be, sql_rel *rel, list *refs)
{
	if (!list_empty(rel->attr))
		return rel2bin_oahash_groupjoin(be, rel, refs);
	stmt *sub = NULL;
	list *jexps = sa_list(be->mvc->sa), *sexps = sa_list(be->mvc->sa), *probe_side = NULL, *hash_side = NULL;
	split_join_exps_pp(rel, jexps, sexps, false);

	assert(rel->op == op_join);
	if (list_empty(jexps)) { /* cartesian */
		sub = rel2bin_oahash_cart(be, rel, refs, NULL, NULL, &probe_side, &hash_side, NULL);
	} else {
		sub = rel2bin_oahash_equi_join(be, rel, refs, jexps, NULL, NULL, NULL, NULL, &probe_side, &hash_side, !list_empty(sexps));
	}
	if (!list_empty(sexps)) {
		sub->cand = rel2bin_oahash_select(be, sub, sexps, rel);
		sub = subrel_project(be, sub, refs, rel);
	}
	return sub;
}

static stmt *
rel2bin_oahash_outerjoin(backend *be, sql_rel *rel, list *refs)
{
	if (!list_empty(rel->attr))
		return rel2bin_oahash_groupjoin(be, rel, refs);
	stmt *sub = NULL, *probe_sub = NULL;
	list *jexps = sa_list(be->mvc->sa), *sexps = sa_list(be->mvc->sa), *probe_side = NULL, *hash_side = NULL;
	split_join_exps_pp(rel, jexps, sexps, false);
	InstrPtr probed_ids = NULL, hash_ids = NULL;
	stmt *mrk = NULL;

	//assert(!(rel->single && list_empty(jexps)));
	if (list_empty(jexps)) { /* cartesian */
		sub = rel2bin_oahash_cart(be, rel, refs, &probed_ids, &probe_sub, &probe_side, &hash_side, &hash_ids);
	} else {
		sub = rel2bin_oahash_equi_join(be, rel, refs, jexps, &probed_ids, &probe_sub, NULL, &mrk, &probe_side, &hash_side, !list_empty(sexps));
	}
	if (!list_empty(sexps)) {
		stmt *sel = rel2bin_oahash_outerselect(be, sub, sexps, rel, probed_ids, hash_ids, &mrk, list_empty(jexps), false);
		list *res = sa_list(be->mvc->sa);
		for (node *n = probe_side->h; n; n = n->next) {
			stmt *c = stmt_project(be, sel, n->data);
			const char *cname = column_name(be->mvc->sa, c);
			const char *tname = table_name(be->mvc->sa, c);
			int label = c->label;
			c = stmt_alias(be, c, label, tname, cname);
			append(res, c);
		}
		for (node *n = hash_side->h; n; n = n->next) {
			stmt *c = stmt_project(be, sel, n->data);
			const char *cname = column_name(be->mvc->sa, c);
			const char *tname = table_name(be->mvc->sa, c);
			int label = c->label;
			c = sql_Nop_(be, "ifthenelse", mrk, c, stmt_atom(be, atom_general(be->mvc->sa, tail_type(c), NULL, 0)), NULL);
			c = stmt_alias(be, c, label, tname, cname);
			append(res, c);
		}
		sub = stmt_list(be, res);
	}
	return sub;
}

static stmt *
rel2bin_oahash_semi(backend *be, sql_rel *rel, list *refs)
{
	sql_rel *rel_hsh = rel->r, *rel_prb = rel->l;
	stmt *probe_sub = NULL, *sub = NULL, *pp = NULL, *nulls = NULL;
	InstrPtr probed_ids = NULL;

	/* start new parallel block after join. NB get_need_pipeline has side effect! */
	int neededpp = (rel->spb || rel->partition) && get_need_pipeline(be);
	(void)neededpp;

	list *exps_cmp_prb = rel_prb->attr;
	list *exps_prj_prb = rel_prb->exps;
	list *probe_side = NULL, *hash_side = NULL;

	list *jexps = sa_list(be->mvc->sa), *sexps = sa_list(be->mvc->sa);
	split_join_exps_pp(rel, jexps, sexps, false);

	bool anti = (list_length(jexps) == 1 && rel->op == op_anti);
	if (list_empty(jexps)) { /* cartesian */
		sub = rel2bin_oahash_cart(be, rel, refs, &probed_ids, &probe_sub, &probe_side, &hash_side, NULL);
	} else if (!list_empty(sexps)) {
		sub = rel2bin_oahash_equi_join(be, rel, refs, jexps, &probed_ids, &probe_sub, &nulls, NULL, &probe_side, &hash_side, true);
	} else {
		assert(!list_empty(jexps) && list_length(jexps) == list_length(exps_cmp_prb));
		/* build-phase res: hash-table and hash-payload stmts */
		stmt *stmts_ht = refs_find_rel(refs, rel_hsh);
		assert(stmts_ht);

		/*** PROBE PHASE ***/
		probe_sub = sub = _start_pp(be, rel_prb->l, false, refs);
		if (!sub) return NULL;

		pp = get_pipeline(be);
		stmt *prb_res = oahash_probe(be, rel, rel->exps, exps_cmp_prb, stmts_ht, sub, pp, anti, false, false, !list_empty(sexps), &nulls, NULL);
		if (prb_res == NULL) return NULL;

		/*** PROJECT RESULT PHASE ***/
		list *lp = oahash_project_single(be, exps_prj_prb, prb_res, sub, pp, &probed_ids);
		sub = stmt_list(be, lp);
	}
	if (!sub)
		return NULL;
	if (list_empty(jexps) && list_empty(sexps) && rel->op == op_anti) {
		assert(hash_side->h);
		nulls = stmt_selectnil(be, hash_side->h->data, NULL);

		sub->cand = nulls;
		sub = subrel_project(be, sub, refs, rel);
		return sub;
	}
	if (list_empty(jexps) && list_empty(sexps)) {
		sql_exp *e = exp_atom_bool(be->mvc->sa, true);
		append(sexps,e);
	}
	/* continue with non equi-joins */
	if (!list_empty(sexps) || (rel->op == op_anti && !anti)) {
		stmt *rids = stmt_blackbox_result(be, probed_ids, 0, sql_fetch_localtype(TYPE_oid));
		if (!list_empty(sexps)) {
			stmt *sel = rel2bin_oahash_select(be, sub, sexps, rel);
			rids = stmt_project(be, sel, rids);
		}
		stmt *c = stmt_mirror(be, bin_find_smallest_column(be, probe_sub));
		if (rel->op == op_anti && nulls) {
			stmt *nonilcand = stmt_tdiff(be, c, nulls, NULL);
			c = stmt_project(be, nonilcand, c);
		}
		if (rel->op == op_anti) {
			probe_sub->cand = stmt_tdiff(be, c, rids, NULL);
		} else {
			probe_sub->cand = stmt_tinter(be, c, rids, false);
		}
		if (rel->op == op_anti && nulls)
			probe_sub->cand = stmt_project(be, probe_sub->cand, c);
		sub = subrel_project(be, probe_sub, refs, rel);
		/* clean up temporary overwrite of REF result
		 * TODO: find a more elegant way than this setting/unsetting of cand */
		probe_sub->cand = NULL;
	}
	return sub;
}

stmt *
rel2bin_oahash(backend *be, sql_rel *rel, list *refs)
{
	switch(rel->op) {
	case op_join:
		return rel2bin_oahash_join(be, rel, refs);
	case op_left:
	case op_right:
	case op_full:
		return rel2bin_oahash_outerjoin(be, rel, refs);
	case op_semi:
	case op_anti:
		return rel2bin_oahash_semi(be, rel, refs);
	default:
        sql_error(be->mvc, 10, SQLSTATE(42000) "rel2bin_oahash(): not a JOIN");
		return NULL;
	}
}
