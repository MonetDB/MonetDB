/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
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
_start_pp(backend *be, sql_rel *rel, bit buildphase, list *refs, stmt *shared_ht)
{
	if (buildphase && get_pipeline(be)) {
        sql_error(be->mvc, 10, SQLSTATE(42000) "Internal error: hash-join cannot start within a pipelines block");
		return NULL;
	}
	if (!be->pp) {
		set_need_pipeline(be);
		if (shared_ht) {
			stmt *ht = shared_ht->op4.lval->h->data;
			be->sink = ht->nr;
		}
	}

	/* first construct the sub-relation */
	stmt *sub = subrel_bin(be, rel, refs);
	sub = subrel_project(be, sub, refs, rel);
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
		stmt *s = stmt_oahash_new(be, exp_subtype((sql_exp*)n->data), sz, curhash, 0);
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
	stmt *slice = stmt_nth_slice(be, ht, true);
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
oahash_probe(backend *be, sql_rel *rel, list *jexps, list *exps_cmp_prb, const stmt *stmts_ht, stmt *sub, const stmt *pp, bool anti, bool outer, bool groupjoin, bool has_outerselect, stmt **nulls, stmt **prb_mrk)
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

		prb_res = stmt_oahash_probe(be, key, prb_res, m->data, stmts_ht->op3, outerm, single, e2->semantics, eq, outer, grpjoin, pp);
		if (prb_res == NULL) return NULL;

		if (outer || grpjoin) {
			assert (prb_res->q->retc == 3);
			outerm = stmt_blackbox_result(be, prb_res->q, 2, sql_fetch_localtype(TYPE_bit));
		}

		if (nulls && stmt_has_null(key) && is_any(e2))
			*nulls = stmt_selectnil(be, key, *nulls);
	}
	if ((outer || groupjoin) && outerm)
		*prb_mrk = outerm;
	/* probe of last column is the final res */
	return prb_res;
}

static list *
oahash_project_hsh(backend *be, list *exps_prj_hsh, stmt *stmts_ht, stmt *prb_res, bool outer, stmt **hsh_mrk)
{
	list *l = sa_list(be->mvc->sa);

	if (!list_empty(exps_prj_hsh)) {
		stmt *stmts_hp = stmts_ht->op1, *hp_gid = stmts_ht->op2, *freq = stmts_ht->op3;
		assert(stmts_hp && (!freq || hp_gid));

		stmt *sel = freq?stmt_oahash_explode(be, prb_res, freq, hp_gid, outer):stmt_blackbox_result(be, prb_res->q, 1, sql_fetch_localtype(TYPE_oid));
		if (sel == NULL) return NULL;
		if (hsh_mrk)
			*hsh_mrk = sel;

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
	}
	return l;
}

static list *
oahash_project_prb(backend *be, list *exps_prj_prb, stmt *prb_res, const stmt *freq, bit outer, stmt *sub, stmt **probed_rowids)
{
	list *l = sa_list(be->mvc->sa);

	stmt *expand = stmt_oahash_expand(be, prb_res, freq, outer);
	if (expand == NULL)
		return NULL;

	if (!list_empty(exps_prj_prb)) {
		for (node *o = exps_prj_prb->h; o; o = o->next) {
			sql_exp *e = o->data;
			stmt *key = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
			assert(key); /* must find */
			key = column(be, key);

			stmt *s = stmt_project(be, expand, key);
			if (s == NULL) return NULL;
			if (e->alias.label)
				s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
			append(l, s);
		}
	}
	if (probed_rowids) /* the rowids are needed for post processing semi/anti joins based on the probe side row ids */
		*probed_rowids = expand;
	return l;
}

static list *
oahash_project_cart(backend *be, list *exps_prj, stmt *sub, stmt *repeat, bit outer, stmt **rowids, bool expand)
{
	list *l = sa_list(be->mvc->sa);
	stmt *pos = NULL;
	stmt *rpt = column(be, repeat);
	if (list_empty(exps_prj)) {
		for (node *o = sub->op4.lval->h; o; o = o->next) {
			stmt *okey = o->data, *key = column(be, okey);
			if (!pos)
				pos = stmt_oahash_project_cart(be, key, rpt, outer, expand);
			stmt *s = stmt_project(be, pos, key);
			if (s == NULL) return NULL;
			if (okey->label)
				s = stmt_alias(be, s, okey->label, table_name(be->mvc->sa, okey), column_name(be->mvc->sa, okey));
			append(l, s);
		}
		if (pos && rowids) /* the rowids are needed for post processing semi/anti joins based on the probe side row ids */
			*rowids = pos;
		return l;
	}

	for (node *o = exps_prj->h; o; o = o->next) {
		sql_exp *e = o->data;
		stmt *key = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
		assert(key); /* must find */
		key = column(be, key);

		if (!pos)
			pos = stmt_oahash_project_cart(be, key, rpt, outer, expand);
		stmt *s = stmt_project(be, pos, key);
		if (s == NULL) return NULL;
		if (e->alias.label)
			s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
		append(l, s);
	}
	if (pos && rowids) /* the rowids are needed for post processing semi/anti joins based on the probe side row ids */
		*rowids = pos;
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
	if (need_freq && list_length(exps_cmp_hsh) == 1 && !is_single(rel)) {
		sql_exp *e = exps_cmp_hsh->h->data;
		if (e->unique)
			need_freq = false;
	}
	lng bld_sz = _estimate(be->mvc, rel); /* TODO: change into dynamic where possible ?? */
	stmt *shared_ht = oahash_prepare_bld_ht(be, exps_cmp_hsh, bld_sz);
	stmt *freq = NULL, *hp_gid = NULL;
	if (need_freq) {
		freq = stmt_bat_new(be, sql_fetch_localtype(TYPE_lng), bld_sz);
		if (!list_empty(exps_prj_hsh)) {
			list *l = shared_ht->op4.lval;
			stmt *prnt = (stmt*)l->t->data;
			hp_gid = stmt_oahash_new(be, sql_fetch_localtype(TYPE_oid), bld_sz, prnt->nr, 0);
			if (hp_gid == NULL) return NULL;
		}
	}
	stmt *shared_hp = NULL;
	if (exps_prj_hsh) {
		shared_hp = oahash_prepare_bld_hp(be, exps_prj_hsh, bld_sz);
	}

	stmt *sub = _start_pp(be, rel->l, true, refs, shared_ht);
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
		stmt *s = stmt_oahash_frequency(be, freq, prnt, (hp_gid != NULL), pp);

		if (hp_gid) {
			prnt = stmt_oahash_build_ht(be, hp_gid, s, prnt, pp);
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
			stmt *s = stmt_algebra_project(be, res, prnt, payload, projectionRef, pp);
			if (e->alias.label)
				s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
			append(ll, s);
		}
		stmts_hp = stmt_list(be, ll);
	}

	(void)stmt_pp_jump(be, pp, be->nrparts);
	(void)stmt_pp_end(be, pp);

	stmts_ht->op1 = stmts_hp;
	stmts_ht->op2 = hp_gid;
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
rel2bin_oahash_equi_join(backend *be, sql_rel *rel, list *refs, list *jexps, stmt **probed_rowids, stmt **probe_sub, stmt **nulls, stmt **prb_mrk, stmt **hsh_mrk, list **probe_side, list **hash_side, bool has_outerselect)
{
	sql_rel *rel_hsh = NULL, *rel_prb = NULL;

	/* start new parallel block after join. NB get_need_pipeline has side effect! */
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
	bool groupedjoin = (!list_empty(rel->attr)), mark = groupjoin_mark(rel->attr);

	/*** PROBE PHASE ***/
	stmt *sub = _start_pp(be, rel_prb->l, false, refs, NULL);
	if (!sub) return NULL;
	if (probe_sub)
		*probe_sub = sub;

	stmt *pp = get_pipeline(be);
	stmt *prb_res = oahash_probe(be, rel, jexps, exps_cmp_prb, stmts_ht, sub, pp, false, is_outerjoin(rel->op), mark, has_outerselect, nulls, prb_mrk);
	if (prb_res == NULL) return NULL;

	/*** PROJECT RESULT PHASE ***/
	bit outer = is_outerjoin(rel->op);
	list *lp = oahash_project_prb(be, exps_prj_prb, prb_res, stmts_ht->op3, outer, sub, probed_rowids);
	list *lh = oahash_project_hsh(be, exps_prj_hsh, stmts_ht, prb_res, outer, hsh_mrk);

	/* !exps_prj_hsh => !shared_hp => mark the last hash-column instead of a payload column */
	if (outer && hsh_mrk && !*hsh_mrk) {
		assert(list_empty(exps_prj_hsh));
		stmt *s = stmt_none(be);
		if (s == NULL) return NULL;
		s->op4.typeval = *sql_fetch_localtype(TYPE_oid);
		s->nr = getArg(prb_res->q, 1);
		s->nrcols = 1;
		s->q = prb_res->q;
		s->op1 = prb_res;
		*hsh_mrk = s;
	}

	if (outer && prb_mrk && !*prb_mrk) /* ToDo somehow expand the prb_mrk */
		assert(0);
	if ((outer || groupedjoin) && prb_mrk && *prb_mrk) {
		stmt *s = stmt_project(be, *probed_rowids, *prb_mrk);
		if (s == NULL) return NULL;
		*prb_mrk = s;
	}

	assert(lh->cnt || lp->cnt || prb_mrk);

	if (probe_side)
		*probe_side = lp;
	if (hash_side)
		*hash_side = lh;
	list *ln = sa_list(be->mvc->sa);
	ln = list_merge(list_merge(ln, lh, NULL), lp, NULL);
	return stmt_list(be, ln);
}

static stmt *
rel2bin_oahash_select(backend *be, stmt *sub, list *sexps, sql_rel *rel, bool handled_first)
{
	stmt *sel = NULL;
	node *en = sexps->h;

	if (handled_first && en)
		en = en->next;
	for ( ; en; en = en->next) {
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
rel2bin_oahash_outerselect(backend *be, stmt *sub, list *sexps, sql_rel *rel, stmt *probed_ids, stmt *hash_ids, stmt **prb_mrk, bool cart, bool mark)
{
	assert (is_outerjoin(rel->op) || !list_empty(rel->attr));
	stmt *sel = NULL, *m = prb_mrk?*prb_mrk:NULL;

	if (hash_ids && cart) {
		/* 0 == empty (no matches possible), nil - no match (but has nil), 1 match */
		m = sql_Nop_(be, "ifthenelse", sql_unop_(be, "isnull", hash_ids), stmt_bool(be, false), stmt_bool(be, true), NULL);
	}

	if (!m && probed_ids)
		m = stmt_project(be, probed_ids, stmt_bool(be, true));
	for (node *en = sexps->h ; en; en = en->next) {
		stmt *s = NULL;
		sql_exp *e = en->data;
		stmt *p = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, sel, 0, 0, 0);
		if (sel && p && (!p->cand || p->cand != sel))
			p = stmt_project(be, sel, p);
		if (p && p->nrcols == 0)
			p = stmt_const(be, m, p);
		stmt *lgids = probed_ids;
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
		if (prb_mrk)
			*prb_mrk = m;
	}
	return sel;
}

static stmt *
rel2bin_oahash_cart_build(backend *be, sql_rel *rel, list *refs)
{
	sql_rel *rel_hsh = NULL;

	assert(rel->oahash == 1 || rel->oahash == 2);
	if (rel->oahash == 1) {
		rel_hsh = rel->l;
	} else { /* rel->oahash == 2 */
		rel_hsh = rel->r;
	}

	/*** (pseudo) HASH PHASE ***/
	/* nothing to hash, we just want to have a materialised table for this side */
	stmt *stmts_ht = subrel_bin(be, rel_hsh, refs);
	stmts_ht = subrel_project(be, stmts_ht, refs, rel);
	return stmts_ht;
}

static stmt *
rel2bin_oahash_cart(backend *be, sql_rel *rel, list *refs, stmt *stmts_ht, stmt **probed_rowids, stmt **probe_sub, list **probe_side, list **hash_side, stmt **hash_rowids, bool *handled_first)
{
	sql_rel *rel_hsh = NULL, *rel_prb = NULL;

	/* start new parallel block after join. NB get_need_pipeline has side effect! */
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

	/*** (pseudo) PROBE PHASE ***/
	stmt *stmts_prb_res = _start_pp(be, rel_prb->l, false, refs, NULL);
	if (!stmts_prb_res) return NULL;
	if (probe_sub)
		*probe_sub = stmts_prb_res;

	bit outer = (is_full(rel->op) || is_left(rel->op) || is_right(rel->op) || (rel->op == op_anti && list_empty(rel->exps)));
	if (!list_empty(rel->exps) && !outer && !is_semi(rel->op)) {
		bool swap = false, cross = false, subexp = true;
		sql_exp *e = rel->exps->h->data;
		sql_rel *l = rel_prb, *r = rel_hsh;

		if (e->type != e_cmp) {
			cross = true;
		} else if (e->flag == cmp_filter) {
			if (!rel_has_all_exps(l, e->l, subexp)) {
				sql_rel *s = l;
				swap = true;
				l = r;
				r = s;
			}
			if ((swap && !rel_has_all_exps(l, e->l, subexp)) || !rel_has_all_exps(r, e->r, subexp))
				cross = true;
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			cross = true;
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			cross = true;
		} else {
			if (!rel_has_exp(l, e->l, subexp)) {
				sql_rel *s = l;
				swap = true;
				l = r;
				r = s;
			}
			if ((swap && !rel_has_exp(l, e->l, subexp)) ||
					!rel_has_exp(r, e->r, subexp) ||
					(e->f && !rel_has_exp(r, e->f, subexp))) {
				cross = true;
			}
		}
		if (!cross && swap && is_semi(rel->op))
			cross = true;
		if (!cross) {
			stmt *lh = stmts_prb_res, *rh = stmts_ht;
			if (swap) {
				stmt *s = lh;
				lh = rh;
				rh = s;
			}
			stmt *js = exp_bin(be, e, lh, rh, NULL, NULL, NULL, NULL, 0, 1, 0);
			if (!js) {
				assert(be->mvc->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
				return NULL;
			}
			stmt *jl = stmt_result(be, js, 0);
			stmt *jr = stmt_result(be, js, 1);

			/* construct relation */
			list *lp = sa_list(be->mvc->sa);
			/* first project using equi-joins */
			for (node *n = lh->op4.lval->h; n; n = n->next) {
				stmt *c = n->data;
				assert(c->label);
				const char *rnme = table_name(be->mvc->sa, c);
				const char *nme = column_name(be->mvc->sa, c);
				stmt *s = stmt_project(be, jl, column(be, c));

				s = stmt_alias(be, s, c->label, rnme, nme);
				list_append(lp, s);
			}
			if (probe_side)
				*probe_side = lp;
			/* construct relation */
			list *llh = sa_list(be->mvc->sa);
			for (node *n = rh->op4.lval->h; n; n = n->next) {
				stmt *c = n->data;
				assert(c->label);
				const char *rnme = table_name(be->mvc->sa, c);
				const char *nme = column_name(be->mvc->sa, c);
				stmt *s = stmt_project(be, jr, column(be, c));

				s = stmt_alias(be, s, c->label, rnme, nme);
				list_append(llh, s);
			}
			if (hash_side)
				*hash_side = llh;
			*handled_first = true;
			list *ln = sa_list(be->mvc->sa);
			ln = list_merge(list_merge(ln, llh, NULL), lp, NULL);
			return stmt_list(be, ln);
		}
	}

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

	list *lp = oahash_project_cart(be, exps_prj_prb, stmts_prb_res, rowrepeat, outer, probed_rowids, true);
	list *lh = oahash_project_cart(be, exps_prj_hsh, stmts_ht, setrepeat, outer, hash_rowids, false);
	assert(lh->cnt || lp->cnt);

	if (probe_side)
		*probe_side = lp;
	if (hash_side)
		*hash_side = lh;
	list *ln = sa_list(be->mvc->sa);
	ln = list_merge(list_merge(ln, lh, NULL), lp, NULL);
	return stmt_list(be, ln);
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
	stmt *probed_ids = NULL, *hash_ids = NULL;
	stmt *prb_mrk = NULL;
	bool mark = false, exist = true;

	if (list_length(rel->attr) == 1) {
        sql_exp *e = rel->attr->h->data;
        if (exp_is_atom(e))
            mark = true;
        if (exp_is_atom(e) && exp_is_false(e))
            exist = false;
    }

	int neededpp = (rel->spb || rel->partition) && get_need_pipeline(be);
	(void)neededpp;

	bool hf = false;
	if (list_empty(jexps)) { /* cartesian */
		stmt *stmts_ht = rel2bin_oahash_cart_build(be, rel, refs);
		sub = rel2bin_oahash_cart(be, rel, refs, stmts_ht, &probed_ids, &probe_sub, &probe_side, &hash_side, &hash_ids, &hf);
	} else {
		sub = rel2bin_oahash_equi_join(be, rel, refs, jexps, &probed_ids, &probe_sub, NULL /* nulls */, &prb_mrk, NULL /* hsh_mrk */, &probe_side, &hash_side, !list_empty(sexps) /* has_outerselect */);
	}
	if (list_empty(jexps) && list_empty(sexps) && mark) {
		sql_exp *e = exp_atom_bool(be->mvc->sa, true);
		set_any(e);
		append(sexps,e);
	}
	if (!list_empty(sexps)) {
		stmt *sel = rel2bin_oahash_outerselect(be, sub, sexps, rel, probed_ids, hash_ids, &prb_mrk, list_empty(jexps), mark);
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
			c = sql_Nop_(be, "ifthenelse", prb_mrk, c, stmt_atom(be, atom_general(be->mvc->sa, tail_type(c), NULL, 0)), NULL);
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
			if (list_empty(jexps) && !prb_mrk) {
				prb_mrk = sql_Nop_(be, "ifthenelse", sql_unop_(be, "isnull", hash_ids), stmt_bool(be, !exist), stmt_bool(be, exist), NULL);
			} else {
				assert(prb_mrk);
				if (exp_is_atom(e) && need_no_nil(e)) /* exclude nulls */
					prb_mrk = sql_Nop_(be, "ifthenelse", sql_unop_(be, "isnull", prb_mrk), stmt_bool(be, false), prb_mrk, NULL);
				if (!exist) {
					sql_subtype *bt = sql_fetch_localtype(TYPE_bit);
					sql_subfunc *not = sql_bind_func(be->mvc, "sys", "not", bt, NULL, F_FUNC, true, true);
					prb_mrk = stmt_unop(be, prb_mrk, NULL, not);
				}
			}
			stmt *s = stmt_alias(be, prb_mrk, e->alias.label, rnme, nme);
			append(sub->op4.lval, s);
		} else {
			assert(0);
		}
	}
	return sub;
}

static stmt *
rel2bin_oahash_innerjoin(backend *be, sql_rel *rel, list *refs)
{
	bool hf = false;
	stmt *sub = NULL, *probed_ids = NULL;
	list *jexps = sa_list(be->mvc->sa), *sexps = sa_list(be->mvc->sa), *probe_side = NULL, *hash_side = NULL;
	split_join_exps_pp(rel, jexps, sexps, false);
	bool single = rel->single && !list_empty(sexps);

	int neededpp = (rel->spb || rel->partition) && get_need_pipeline(be);
	(void)neededpp;

	assert(rel->op == op_join);
	if (list_empty(jexps)) { /* cartesian */
		stmt *stmts_ht = rel2bin_oahash_cart_build(be, rel, refs);
		sub = rel2bin_oahash_cart(be, rel, refs, stmts_ht, single?&probed_ids:NULL, NULL, &probe_side, &hash_side, NULL, &hf);
	} else {
		sub = rel2bin_oahash_equi_join(be, rel, refs, jexps, single?&probed_ids:NULL, NULL /* probe_sub */, NULL /* nulls */, NULL /* prb_mrk */, NULL /* hsh_mrk */, &probe_side, &hash_side, !list_empty(sexps) /* has_outerselect */);
	}
	if (!list_empty(sexps)) {
		sub->cand = rel2bin_oahash_select(be, sub, sexps, rel, hf);
		if (single && sub->cand) {
			assert(probed_ids);
			stmt *ps = stmt_project(be, sub->cand, probed_ids);
			sub->cand = stmt_single(be, sub->cand, ps);
		}
		sub = subrel_project(be, sub, refs, rel);
	}
	return sub;
}

static stmt *
rel2bin_oahash_outerjoin(backend *be, sql_rel *rel, list *refs)
{
	stmt *sub = NULL, *probe_sub = NULL;
	list *jexps = sa_list(be->mvc->sa), *sexps = sa_list(be->mvc->sa), *probe_side = NULL, *hash_side = NULL;
	split_join_exps_pp(rel, jexps, sexps, false);
	stmt *probed_ids = NULL, *hash_ids = NULL;
	stmt *prb_mrk = NULL;

	int neededpp = (rel->spb || rel->partition) && get_need_pipeline(be);
	(void)neededpp;

	//assert(!(rel->single && list_empty(jexps)));
	bool hf = false;
	if (list_empty(jexps)) { /* cartesian */
		stmt *stmts_ht = rel2bin_oahash_cart_build(be, rel, refs);
		sub = rel2bin_oahash_cart(be, rel, refs, stmts_ht, &probed_ids, &probe_sub, &probe_side, &hash_side, &hash_ids, &hf);
	} else {
		sub = rel2bin_oahash_equi_join(be, rel, refs, jexps, &probed_ids, &probe_sub, NULL /* nulls */, &prb_mrk, NULL /* hsh_mrk */, &probe_side, &hash_side, !list_empty(sexps) /* has_outerselect */);
	}
	if (!list_empty(sexps)) {
		stmt *sel = rel2bin_oahash_outerselect(be, sub, sexps, rel, probed_ids, hash_ids, &prb_mrk, list_empty(jexps), false);
		list *lp = sa_list(be->mvc->sa);
		for (node *n = probe_side->h; n; n = n->next) {
			stmt *c = stmt_project(be, sel, n->data);
			const char *cname = column_name(be->mvc->sa, c);
			const char *tname = table_name(be->mvc->sa, c);
			int label = c->label;
			c = stmt_alias(be, c, label, tname, cname);
			append(lp, c);
		}
		list *lh = sa_list(be->mvc->sa);
		for (node *n = hash_side->h; n; n = n->next) {
			stmt *c = stmt_project(be, sel, n->data);
			const char *cname = column_name(be->mvc->sa, c);
			const char *tname = table_name(be->mvc->sa, c);
			int label = c->label;
			c = sql_Nop_(be, "ifthenelse", prb_mrk, c, stmt_atom(be, atom_general(be->mvc->sa, tail_type(c), NULL, 0)), NULL);
			c = stmt_alias(be, c, label, tname, cname);
			append(lh, c);
		}
		sub = stmt_list(be, list_merge(lh, lp, NULL));
	}
	return sub;
}

static stmt *
rel2bin_oahash_fullouterjoin(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	sql_subtype *tpe_oid = sql_fetch_localtype(TYPE_oid);
	bool hf = false;

	int neededpp = (rel->spb || rel->partition) && get_need_pipeline(be);
	(void)neededpp;

	sql_rel *rel_hsh = NULL, *rel_prb = NULL;
	if (rel->oahash == 1) {
		rel_hsh = rel->l;
		rel_prb = rel->r;
	} else { /* rel->oahash == 2 */
		rel_hsh = rel->r;
		rel_prb = rel->l;
	}

	stmt *stmts_ht = refs_find_rel(refs, rel_hsh);

	if (!stmts_ht)
		stmts_ht = rel2bin_oahash_cart_build(be, rel, refs);

	list *shared_ht = stmts_ht->op4.lval;
	list *shared_hp = stmts_ht->op1?stmts_ht->op1->op4.lval:NULL;
	/* hash side mark if a join-match has been found.  NIL: unused; TRUE: matched; FALSE unmatched */
	/* Mark the payloads, if exist; otherwise the last hash column */
	stmt *hp_mrk = stmt_oahash_hshmrk_init(be, stmts_ht, be->pp > 0);
	// X_48:int := slicer.no_slices(X_9:bat[:int]);
	stmt *hp_mrk_slc = stmt_no_slices(be, hp_mrk, false);
	if (be->pp > 0) {
		moveInstruction(be->mb, be->mb->stop-1, be->pp_pc++);
	}

	/*** start pipeline.concat ***/
	int p_source = be->source, p_concatcnt = be->concatcnt;
	(void)stmt_concat(be, be->source, 2);

	/* create all results variables, hash-side first */
	list *vars = sa_list(sql->sa);
	if (!list_empty(rel_hsh->exps)) {
		for (node *n = rel_hsh->exps->h; n; n = n->next) {
			sql_subtype *st = exp_subtype(n->data);
			stmt *s = stmt_bat_declare(be, st);
			append(vars, s);
		}
	}
	if (!list_empty(rel_prb->exps)) {
		for (node *n = rel_prb->exps->h; n; n = n->next) {
			sql_subtype *st = exp_subtype(n->data);
			stmt *s = stmt_bat_declare(be, st);
			append(vars, s);
		}
	}

	/*** first concat_block: the join ***/
	int b = stmt_concat_barrier(be, be->source, 0, 0);

	// existing code, extended to mark the matched payloads
	stmt *sub = NULL, *probe_sub = NULL;
	list *jexps = sa_list(sql->sa), *sexps = sa_list(sql->sa), *probe_side = NULL, *hash_side = NULL;
	split_join_exps_pp(rel, jexps, sexps, false);
	stmt *probed_ids = NULL, *hash_ids = NULL;
	stmt *m = NULL, *hsh_mrk = NULL;

	if (list_empty(jexps)) { /* cartesian */
		sub = rel2bin_oahash_cart(be, rel, refs, stmts_ht, &probed_ids, &probe_sub, &probe_side, &hash_side, &hash_ids, &hf);
		hsh_mrk = hash_ids;
		if (!shared_hp && !list_empty(rel_hsh->exps) /*|| !list_empty(rel_hsh->attr)*/)
			shared_hp = shared_ht;
	} else {
		sub = rel2bin_oahash_equi_join(be, rel, refs, jexps, &probed_ids, &probe_sub, NULL /* nulls */, &m /* prb_mrk */, &hsh_mrk, &probe_side, &hash_side, !list_empty(sexps) /* has_outerselect */);
	}
	if (!list_empty(sexps)) {
		stmt *sel = rel2bin_oahash_outerselect(be, sub, sexps, rel, probed_ids, hash_ids, &m, list_empty(jexps), false);
		if (sel == NULL) return NULL;
		list *lp = sa_list(sql->sa);
		for (node *n = probe_side->h; n; n = n->next) {
			stmt *c = stmt_project(be, sel, n->data);
			const char *cname = column_name(sql->sa, c);
			const char *tname = table_name(sql->sa, c);
			int label = c->label;
			c = stmt_alias(be, c, label, tname, cname);
			append(lp, c);
		}
		list *lh = sa_list(sql->sa);
		for (node *n = hash_side->h; n; n = n->next) {
			stmt *c = stmt_project(be, sel, n->data);
			const char *cname = column_name(sql->sa, c);
			const char *tname = table_name(sql->sa, c);
			int label = c->label;
			c = sql_Nop_(be, "ifthenelse", m, c, stmt_atom(be, atom_general(sql->sa, tail_type(c), NULL, 0)), NULL);
			c = stmt_alias(be, c, label, tname, cname);
			append(lh, c);
		}
		sub = stmt_list(be, list_merge(lh, lp, NULL));

		/* mark the matched payload */
		assert(sel && m);
		/*  X_nn:bat[:oid] := algebra.thetaselect(<m>, nil:bat[:any], true:bit, "=":str);*/
		stmt *ss = stmt_thetaselect(be, m, NULL, stmt_bool(be, 1), "==", tpe_oid);
		if (!ss) return NULL;
		stmt *s_oids = stmt_project(be, ss, sel);
		if (!s_oids) return NULL;
		hsh_mrk = stmt_project(be, s_oids, hsh_mrk);
	} else {
		atom *a_null = atom_general(sql->sa, tpe_oid, NULL, 0);
		stmt *stmt_null = stmt_atom(be, a_null); /* we just need the count of unmatched */
		/*  X_nn:bat[:oid] := algebra.thetaselect(<m>, nil:bat[:any], true:bit, "=":str);*/
		stmt *sel = stmt_thetaselect(be, hsh_mrk, NULL, stmt_null, "ne", tpe_oid);
		hsh_mrk = stmt_project(be, sel, hsh_mrk);
	}
	if (!hsh_mrk) return NULL;
	hp_mrk = stmt_algebra_project(be, hp_mrk, hsh_mrk, stmt_bool(be,1), projectRef, get_pipeline(be));
	// END existing code

	/* NB stmts order in sub must match that in vars, i.e. hash-side first */
	if (!shared_hp && list_empty(jexps)) { /* skip useless hash side cols on empty payload */
		node *n, *m;
		for(n = sub->op4.lval->h, m = shared_ht->h; n && m ; n = n->next, m = m->next)
			;
		sub->op4.lval->h = n;
	}
	sub = subres_assign_resultvars(be, sub, vars);
	if (!sub) return NULL;

	if (be->concatcnt == 0) {/* add dummy source */
		int source = pp_counter(be, 1, -1);
		stmt_concat_add_source(be);
		(void)pp_counter_get(be, source); /* use source else statement gets garbage collected */
	}
	(void)stmt_concat_barrier_end(be, b);
	assert (be->concatcnt == 1);

	/*** second concat_block: add the NULLs ***/
	b = stmt_concat_barrier(be, be->source, 1, b);
	int source = pp_counter(be, -1, hp_mrk_slc->nr);
	stmt_concat_add_source(be);
	(void)pp_counter_get(be, source); /* use source else statement gets garbage collected */

	//output the unmatched rows with NULLs for the LHS columns
	stmt *slice = stmt_nth_slice(be, hp_mrk, false);
	stmt *unmatched = stmt_thetaselect(be, slice, NULL, stmt_bool(be, 0), "==", tpe_oid);
	list *res2 = sa_list(sql->sa);
	if (!list_empty(shared_hp)) {
		for (node *n = shared_hp->h; n; n = n->next) {
			stmt *c = stmt_project(be, unmatched, n->data);
			const char *cname = column_name(sql->sa, c);
			const char *tname = table_name(sql->sa, c);
			int label = c->label;
			c = stmt_alias(be, c, label, tname, cname);
			append(res2, c);
		}
	} else {
		/* without payload, we need to expand the count of unmatched freq if exists */
		if (stmts_ht->op3) {
			unmatched = stmt_oahash_explode_unmatched(be, shared_ht->t->data, unmatched, stmts_ht->op3);
			if (unmatched == NULL) return NULL;
		}
	}

	for (node *n = probe_side->h; n; n = n->next) {
		sql_subtype *tpe = tail_type(n->data);
		atom *a = atom_general(sql->sa, tpe, NULL, 0);
		stmt *c = stmt_const(be, unmatched, stmt_atom(be, a)); /* we just need the count of unmatched */
		const char *cname = column_name(sql->sa, n->data);
		const char *tname = table_name(sql->sa, n->data);
		int label = ((stmt*)n->data)->label;
		c = stmt_alias(be, c, label, tname, cname);
		append(res2, c);
	}
	sub = stmt_list(be, res2);

	/* NB stmts order in sub must match that in vars, i.e. hash-side first */
	sub = subres_assign_resultvars(be, sub, vars);
	if (!sub) return NULL;

//	if (be->concatcnt == 1) {/* add dummy source */
//			int source = pp_counter(be, -1, hp_mrk_slc->nr);
//			stmt_concat_add_source(be);
//			(void)pp_counter_get(be, source); /* use source else statement gets garbage collected */
//	}
	(void)stmt_concat_barrier_end(be, b);
	assert (be->concatcnt == 2);

	if (p_source)
		stmt_concat_add_subconcat(be, p_source, p_concatcnt);
	return sub;
}

static stmt *
rel2bin_oahash_semi(backend *be, sql_rel *rel, list *refs)
{
	sql_rel *rel_hsh = rel->r, *rel_prb = rel->l;
	stmt *probe_sub = NULL, *sub = NULL, *pp = NULL, *nulls = NULL;
	stmt *probed_ids = NULL;

	/* start new parallel block after join. NB get_need_pipeline has side effect! */
	int neededpp = (rel->spb || rel->partition) && get_need_pipeline(be);
	(void)neededpp;

	list *exps_cmp_prb = rel_prb->attr;
	list *exps_prj_prb = rel_prb->exps;
	list *probe_side = NULL, *hash_side = NULL;

	list *jexps = sa_list(be->mvc->sa), *sexps = sa_list(be->mvc->sa);
	split_join_exps_pp(rel, jexps, sexps, false);

	bool anti = (list_length(jexps) == 1 && rel->op == op_anti);
	bool hf = false;
	if (list_empty(jexps)) { /* cartesian */
		stmt *stmts_ht = rel2bin_oahash_cart_build(be, rel, refs);
		sub = rel2bin_oahash_cart(be, rel, refs, stmts_ht, &probed_ids, &probe_sub, &probe_side, &hash_side, NULL, &hf);
	} else if (!list_empty(sexps)) {
		sub = rel2bin_oahash_equi_join(be, rel, refs, jexps, &probed_ids, &probe_sub, &nulls, NULL /* prb_mrk */, NULL /* hsh_mrk */, &probe_side, &hash_side, true /* has_outerselect */);
	} else {
		assert(!list_empty(jexps) && list_length(jexps) == list_length(exps_cmp_prb));
		/* build-phase res: hash-table and hash-payload stmts */
		stmt *stmts_ht = refs_find_rel(refs, rel_hsh);
		assert(stmts_ht);

		/*** PROBE PHASE ***/
		probe_sub = sub = _start_pp(be, rel_prb->l, false, refs, NULL);
		if (!sub) return NULL;

		pp = get_pipeline(be);
		stmt *prb_res = oahash_probe(be, rel, rel->exps, exps_cmp_prb, stmts_ht, sub, pp, anti, false, false, !list_empty(sexps), &nulls, NULL);
		if (prb_res == NULL) return NULL;

		/*** PROJECT RESULT PHASE ***/
		list *lp = oahash_project_prb(be, exps_prj_prb, prb_res, NULL, false, sub, &probed_ids);
		sub = stmt_list(be, lp);
	}
	if (!sub)
		return NULL;
	if (list_empty(jexps) && list_empty(sexps) && rel->op == op_anti) {
		/* outer cross product, ie only return rows if tid is nil (ie were hash side does not exist)*/
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
		stmt *rids = probed_ids;
		if (!list_empty(sexps)) {
			stmt *sel = rel2bin_oahash_select(be, sub, sexps, rel, hf);
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
    if (is_semi(rel->op)) {
        return rel2bin_oahash_semi(be, rel, refs);
    } else if (!list_empty(rel->attr)) {
        return rel2bin_oahash_groupjoin(be, rel, refs);
    } else if (is_outerjoin(rel->op)) {
		if (rel->op == op_full)
			return rel2bin_oahash_fullouterjoin(be, rel, refs);
        return rel2bin_oahash_outerjoin(be, rel, refs);
    } else {
        assert(is_innerjoin(rel->op));
        return rel2bin_oahash_innerjoin(be, rel, refs);
    }
}
