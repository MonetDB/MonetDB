/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_basetable.h"
#include "rel_remote.h"
#include "rel_statistics.h"
#include "rel_rewriter.h"
#include "sql_privileges.h"
#include "sql_storage.h"

#define USED_LEN(nr) ((nr+31)/32)
#define rel_base_set_used(b,nr) b->used[(nr)/32] |= (1U<<((nr)%32))
#define rel_base_is_used(b,nr) ((b->used[(nr)/32]&(1U<<((nr)%32))) != 0)

typedef struct rel_base_t {
	sql_table *mt;
	char *name;
	int disallowed;	/* ie check per column */
	int basenr;
	uint32_t used[];
} rel_base_t;

void
rel_base_disallow(sql_rel *r)
{
	rel_base_t *ba = r->r;
	ba->disallowed = 1;
}

char *
rel_base_name(sql_rel *r)
{
	sql_table *t = r->l;
	rel_base_t *ba = r->r;
	if (ba->name)
		return ba->name;
	return t->base.name;
}

char *
rel_base_rename(sql_rel *r, char *name)
{
	rel_base_t *ba = r->r;
	ba->name = name;
	return name;
}

int
rel_base_idx_nid(sql_rel *r, sql_idx *i)
{
	rel_base_t *ba = r->r;
	sql_table *b = r->l;
	if (i) {
		int j = ba->basenr + ol_length(b->columns) + 1;
		for (node *in = ol_first_node(i->t->idxs); in; in = in->next, j++) {
			if (i == in->data)
				return -(ba->basenr + j);
		}
	}
	return 0;
}

sql_column*
rel_base_find_column(sql_rel *r, int nid)
{
	rel_base_t *ba = r->r;
	sql_table *b = r->l;
	nid = -nid;
	if ((nid - ba->basenr) >= ol_length(b->columns))
		return NULL;
	return ol_fetch(b->columns, nid - ba->basenr);
}

int
rel_base_nid(sql_rel *r, sql_column *c)
{
	rel_base_t *ba = r->r;
	sql_table *b = r->l;
	if (c)
		return -(ba->basenr + c->colnr);
	return -(ba->basenr + ol_length(b->columns));
}

bool
rel_base_has_nid(sql_rel *r, int nid)
{
	rel_base_t *ba = r->r;
	sql_table *b = r->l;

	nid = -nid;
	return (nid >= ba->basenr && nid <= ba->basenr + ol_length(b->columns));
}

int
rel_base_use( mvc *sql, sql_rel *rt, int nr)
{
	assert(is_basetable(rt->op));
	sql_table *t = rt->l;
	rel_base_t *ba = rt->r;

	if (ba->disallowed && nr < ol_length(t->columns)) {
		sql_column *c = ol_fetch(t->columns, nr);
		if (!column_privs(sql, c, PRIV_SELECT))
			return -1;
	}
	rel_base_set_used(ba, nr);
	return 0;
}

void
rel_base_use_tid( mvc *sql, sql_rel *rt)
{
	sql_table *t = rt->l;
	rel_base_use(sql, rt, ol_length(t->columns));
}

void
rel_base_use_all( mvc *sql, sql_rel *rel)
{
	sql_table *t = rel->l;
	rel_base_t *ba = rel->r;

	if (ba->disallowed) {
		int i = 0;
		for (node *cn = ol_first_node(t->columns); cn; cn = cn->next, i++) {
			sql_column *c = cn->data;
			if (!column_privs(sql, c, PRIV_SELECT))
				continue;
			rel_base_set_used(ba, i);
		}
	} else {
		int len = USED_LEN(ol_length(t->columns) + 1 + ol_length(t->idxs));
		for (int i = 0; i < len; i++)
			ba->used[i] = ~0U;
	}
}

sql_rel *
rel_basetable(mvc *sql, sql_table *t, const char *atname)
{
	allocator *sa = sql->sa;
	sql_rel *rel = rel_create(sa);
	int nrcols = ol_length(t->columns), end = nrcols + 1 + ol_length(t->idxs);
	rel_base_t *ba = (rel_base_t*)sa_zalloc(sa, sizeof(rel_base_t) + sizeof(int)*USED_LEN(end));
	sqlstore *store = sql->session->tr->store;

	if(!rel || !ba)
		return NULL;

	ba->basenr = sql->nid;
	sql->nid += end;
	if (isTable(t) && t->s && !isDeclaredTable(t)) /* count active rows only */
		set_count_prop(sql->sa, rel, (BUN)store->storage_api.count_col(sql->session->tr, ol_first_node(t->columns)->data, CNT_ACTIVE));
	assert(atname);
	if (strcmp(atname, t->base.name) != 0)
		ba->name = sa_strdup(sa, atname);
	for(int i = nrcols; i<end; i++)
		rel_base_set_used(ba, i);
	rel->l = t;
	rel->r = ba;
	rel->op = op_basetable;
	rel->card = CARD_MULTI;
	rel->nrcols = nrcols;
	return rel;
}

void
rel_base_copy(mvc *sql, sql_rel *in, sql_rel *out)
{
	allocator *sa = sql->sa;
	sql_table *t = in->l;
	rel_base_t *ba = in->r;

	assert(is_basetable(in->op) && is_basetable(out->op));
	int nrcols = ol_length(t->columns), end = nrcols + 1 + ol_length(t->idxs);
	size_t bsize = sizeof(rel_base_t) + sizeof(uint32_t)*USED_LEN(end);
	rel_base_t *nba = (rel_base_t*)sa_alloc(sa, bsize);

	memcpy(nba, ba, bsize);
	if (ba->name)
		nba->name = sa_strdup(sa, ba->name);

	out->l = t;
	out->r = nba;
}

sql_rel *
rel_base_bind_column_( sql_rel *rel, const char *cname)
{
	sql_table *t = rel->l;
	node *n = ol_find_name(t->columns, cname);
	if (n)
		return rel;
	return NULL;
}

static sql_exp *
bind_col_exp(mvc *sql, rel_base_t *ba, char *name, sql_column *c)
{
	prop *p = NULL;
	sql_exp *e = exp_column(sql->sa, name, c->base.name, &c->type, CARD_MULTI, c->null, is_column_unique(c), 0);

	if (e) {
		e->nid = -(ba->basenr + c->colnr);
		e->alias.label = e->nid;
	}
	if (c->t->pkey && ((sql_kc*)c->t->pkey->k.columns->h->data)->c == c) {
		p = e->p = prop_create(sql->sa, PROP_HASHCOL, e->p);
		p->value.pval = c->t->pkey;
	} else if (c->unique == 2) {
		p = e->p = prop_create(sql->sa, PROP_HASHCOL, e->p);
		p->value.pval = NULL;
	}
	set_basecol(e);
	sql_column_get_statistics(sql, c, e);
	return e;
}

static sql_exp *
bind_col(mvc *sql, sql_rel *rel, char *name, sql_column *c )
{
	if (!c || rel_base_use(sql, rel, c->colnr)) {
		/* error */
		return NULL;
	}
	return bind_col_exp(sql, rel->r, name, c);
}

sql_exp *
rel_base_bind_colnr( mvc *sql, sql_rel *rel, int nr)
{
	sql_table *t = rel->l;
	rel_base_t *ba = rel->r;
	return bind_col(sql, rel, ba->name?ba->name:t->base.name, ol_fetch(t->columns, nr));
}

sql_exp *
rel_base_find_label( mvc *sql, sql_rel *rel, int label)
{
	sql_table *t = rel->l;
	rel_base_t *ba = rel->r;

	label = -label;
	int colnr = label - ba->basenr;
	if (colnr > ol_length(t->columns))
		return NULL;
	return rel_base_bind_colnr(sql, rel, colnr);
}

sql_exp *
rel_base_bind_column( mvc *sql, sql_rel *rel, const char *cname, int no_tname)
{
	sql_table *t = rel->l;
	rel_base_t *ba = rel->r;
	(void)no_tname;
	node *n = t ? ol_find_name(t->columns, cname) : NULL;
	if (!n)
		return NULL;
	return bind_col(sql, rel, ba->name?ba->name:t->base.name, n->data);
}

sql_rel *
rel_base_bind_column2_( sql_rel *rel, const char *tname, const char *cname)
{
	sql_table *t = rel->l;
	rel_base_t *ba = rel->r;

	assert(ba);
	if ((ba->name && strcmp(ba->name, tname) != 0) || (!ba->name && strcmp(t->base.name, tname) != 0))
		return NULL;
	node *n = ol_find_name(t->columns, cname);
	if (!n)
		return NULL;
	return rel;
}

sql_exp *
rel_base_bind_column2( mvc *sql, sql_rel *rel, const char *tname, const char *cname)
{
	sql_table *t = rel->l;
	rel_base_t *ba = rel->r;

	assert(ba);
	if ((ba->name && strcmp(ba->name, tname) != 0) || (!ba->name && strcmp(t->base.name, tname) != 0))
		return NULL;
	node *n = ol_find_name(t->columns, cname);
	if (!n)
		return NULL;
	sql_column *c = n->data;
	return bind_col(sql, rel, ba->name?ba->name:t->base.name, c);
}

sql_exp *
rel_base_bind_column3( mvc *sql, sql_rel *rel, const char *sname, const char *tname, const char *cname)
{
	sql_table *t = rel->l;
	if (!t->s || strcmp(t->s->base.name, sname) != 0)
		return NULL;
	return rel_base_bind_column2(sql, rel, tname, cname);
}

list *
rel_base_projection( mvc *sql, sql_rel *rel, int intern)
{
	int i = 0;
	sql_table *t = rel->l;
	rel_base_t *ba = rel->r;
	char *name = ba->name?ba->name:t->base.name;
	list *exps = new_exp_list(sql->sa);
	prop *p;

	for (node *cn = ol_first_node(t->columns); cn; cn = cn->next, i++) {
		if (rel_base_is_used(ba, i)) {
			sql_exp *e = bind_col_exp(sql, ba, name, cn->data);
			append(exps, e);
		}
	}
	if ((intern && rel_base_is_used(ba, i)) || list_empty(exps)) { /* Add TID column if no column is used */
		sql_exp *e = exp_column(sql->sa, name, TID, sql_fetch_localtype(TYPE_oid), CARD_MULTI, 0, 1, 1);
		e->nid = -(ba->basenr + i);
		e->alias.label = e->nid;
		append(exps, e);
	}
	i++;
	if (intern) {
		int j = i;
		for (node *in = ol_first_node(t->idxs); in; in = in->next, j++) {
			if (rel_base_is_used(ba, j)) {
				sql_idx *i = in->data;
				sql_subtype *t = sql_fetch_localtype(TYPE_lng); /* hash "lng" */
				int has_nils = 0, unique;

				if ((hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
					continue;

				if (i->type == join_idx)
					t = sql_fetch_localtype(TYPE_oid);

				char *iname = sa_strconcat( sql->sa, "%", i->base.name);
				for (node *n = i->columns->h ; n && !has_nils; n = n->next) { /* check for NULL values */
					sql_kc *kc = n->data;

					if (kc->c->null)
						has_nils = 1;
				}
				unique = list_length(i->columns) == 1 && is_column_unique(((sql_kc*)i->columns->h->data)->c);
				sql_exp *e = exp_column(sql->sa, name, iname, t, CARD_MULTI, has_nils, unique, 1);
				e->nid = -(ba->basenr + j);
				e->alias.label = e->nid;
				if (hash_index(i->type)) {
					p = e->p = prop_create(sql->sa, PROP_HASHIDX, e->p);
					p->value.pval = i;
				}
				if (i->type == join_idx) {
					p = e->p = prop_create(sql->sa, PROP_JOINIDX, e->p);
					p->value.pval = i;
				}
				append(exps, e);
			}
		}
	}
	return exps;
}

list *
rel_base_project_all( mvc *sql, sql_rel *rel, char *tname)
{
	sql_table *t = rel->l;
	rel_base_t *ba = rel->r;
	char *name = ba->name?ba->name:t->base.name;
	list *exps = new_exp_list(sql->sa);

	if (!exps || strcmp(name, tname) != 0)
		return NULL;

	for (node *cn = ol_first_node(t->columns); cn; cn = cn->next)
		append(exps, bind_col( sql, rel, name, cn->data));
	return exps;
}

sql_rel *
rel_base_add_columns( mvc *sql, sql_rel *r)
{
	sql_table *t = r->l;
	rel_base_t *ba = r->r;

	r->exps = new_exp_list(sql->sa);
	if(!r->exps) {
		rel_destroy(r);
		return NULL;
	}

	int i = 0;
	prop *p = NULL;
	node *cn;
	const char *tname = t->base.name;
	const char *atname = ba->name?ba->name:tname;

	for (cn = ol_first_node(t->columns); cn; cn = cn->next, i++) {
		sql_column *c = cn->data;
		sql_exp *e = exp_alias(sql, atname, c->base.name, tname, c->base.name, &c->type, CARD_MULTI, c->null, is_column_unique(c), 0);

		if (e == NULL) {
			rel_destroy(r);
			return NULL;
		}
		e->nid = -(ba->basenr + i);
		e->alias.label = e->nid;
		if (c->t->pkey && ((sql_kc*)c->t->pkey->k.columns->h->data)->c == c) {
			p = e->p = prop_create(sql->sa, PROP_HASHCOL, e->p);
			p->value.pval = c->t->pkey;
		} else if (c->unique == 2) {
			p = e->p = prop_create(sql->sa, PROP_HASHCOL, e->p);
			p->value.pval = NULL;
		}
		set_basecol(e);
		sql_column_get_statistics(sql, c, e);
		append(r->exps, e);
	}
	return r;
}

sql_rel *
rewrite_basetable(mvc *sql, sql_rel *rel, bool stats)
{
	if (is_basetable(rel->op) && !rel->exps) {
		allocator *sa = sql->sa;
		sql_table *t = rel->l;
		rel_base_t *ba = rel->r;

		rel->exps = new_exp_list(sa);
		if(!rel->exps) {
			rel_destroy(rel);
			return NULL;
		}

		int i = 0;
		prop *p = NULL;
		node *cn;
		const char *tname = t->base.name;
		const char *atname = ba->name?ba->name:tname;

		if (isRemote(t))
			tname = mapiuri_table(t->query, sql->sa, tname);
		for (cn = ol_first_node(t->columns); cn; cn = cn->next, i++) {
			if (!rel_base_is_used(ba, i))
				continue;

			sql_column *c = cn->data;
			sql_exp *e = exp_alias(sql, atname, c->base.name, tname, c->base.name, &c->type, CARD_MULTI, c->null, is_column_unique(c), 0);

			if (e == NULL) {
				rel_destroy(rel);
				return NULL;
			}
			e->nid = -(ba->basenr + i);
			e->alias.label = e->nid;
			if (c->t->pkey && ((sql_kc*)c->t->pkey->k.columns->h->data)->c == c) {
				p = e->p = prop_create(sa, PROP_HASHCOL, e->p);
				p->value.pval = c->t->pkey;
			} else if (c->unique == 2) {
				p = e->p = prop_create(sa, PROP_HASHCOL, e->p);
				p->value.pval = NULL;
			}
			set_basecol(e);
			if (stats)
				sql_column_get_statistics(sql, c, e);
			append(rel->exps, e);
		}
		if (rel_base_is_used(ba, i) || list_empty(rel->exps)) { /* Add TID column if no column is used */
			sql_exp *e = exp_alias(sql, atname, TID, tname, TID, sql_fetch_localtype(TYPE_oid), CARD_MULTI, 0, 1, 1);
			if (e == NULL) {
				rel_destroy(rel);
				return NULL;
			}
			e->nid = -(ba->basenr + i);
			e->alias.label = e->nid;
			append(rel->exps, e);
		}
		i++;
		int j = i;
		for (cn = ol_first_node(t->idxs); cn; cn = cn->next, j++) {
			if (!rel_base_is_used(ba, j))
				continue;

			sql_exp *e;
			sql_idx *i = cn->data;
			sql_subtype *t;
			char *iname = NULL;
			int has_nils = 0, unique;

			/* do not include empty indices in the plan */
			if ((hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
				continue;

			t = (i->type == join_idx) ? sql_fetch_localtype(TYPE_oid) : sql_fetch_localtype(TYPE_lng);
			iname = sa_strconcat( sa, "%", i->base.name);
			for (node *n = i->columns->h ; n && !has_nils; n = n->next) { /* check for NULL values */
				sql_kc *kc = n->data;

				if (kc->c->null)
					has_nils = 1;
			}
			unique = list_length(i->columns) == 1 && is_column_unique(((sql_kc*)i->columns->h->data)->c);
			e = exp_alias(sql, atname, iname, tname, iname, t, CARD_MULTI, has_nils, unique, 1);
			if (e == NULL) {
				rel_destroy(rel);
				return NULL;
			}
			/* index names are prefixed, to make them independent */
			if (hash_index(i->type)) {
				p = e->p = prop_create(sa, PROP_HASHIDX, e->p);
				p->value.pval = i;
			}
			if (i->type == join_idx) {
				p = e->p = prop_create(sa, PROP_JOINIDX, e->p);
				p->value.pval = i;
			}
			e->nid = -(ba->basenr + j);
			e->alias.label = e->nid;
			append(rel->exps, e);
		}
	}
	return rel;
}

sql_exp *
basetable_get_tid_or_add_it(mvc *sql, sql_rel *rel)
{
	sql_exp *res = NULL;

	if (is_basetable(rel->op)) {
		sql_table *t = rel->l;
		rel_base_t *ba = rel->r;
		const char *tname = t->base.name;
		const char *atname = ba->name?ba->name:tname;

		if (isRemote(t))
			tname = mapiuri_table(t->query, sql->sa, tname);
		if (!rel->exps) { /* no exps yet, just set TID */
			rel_base_use_tid(sql, rel);
			res = exp_alias(sql, atname, TID, tname, TID, sql_fetch_localtype(TYPE_oid), CARD_MULTI, 0, 1, 1);
			res->nid = -(ba->basenr + ol_length(t->columns));
			res->alias.label = res->nid;
		} else if (!rel_base_is_used(ba, ol_length(t->columns)) ||  /* exps set, but no TID, add it */
				   !(res = exps_bind_column2(rel->exps, atname, TID, NULL))) { /* exps set with TID, but maybe rel_dce removed it */
			node *n = NULL;
			rel_base_use_tid(sql, rel);
			res = exp_alias(sql, atname, TID, tname, TID, sql_fetch_localtype(TYPE_oid), CARD_MULTI, 0, 1, 1);
			res->nid = -(ba->basenr + ol_length(t->columns));
			res->alias.label = res->nid;

			/* search for indexes */
			for (node *cn = rel->exps->h; cn && !n; cn = cn->next) {
				sql_exp *e = cn->data;

				if (is_intern(e))
					n = cn;
			}
			if (n) { /* has indexes, insert TID before them */
				list_append_before(rel->exps, n, res);
			} else {
				list_append(rel->exps, res);
			}
		}
	}
	return res;
}

sql_rel *
rel_rename_part(mvc *sql, sql_rel *p, sql_rel *mt_rel, const char *mtalias)
{
	sql_exp *ne = NULL;
	sql_table *mt = rel_base_table(mt_rel), *t = rel_base_table(p);
	rel_base_t *mt_ba = mt_rel->r, *p_ba = p->r;
	p_ba->basenr = mt_ba->basenr;

	assert(!p->exps);
	p->exps = sa_list(sql->sa);
	const char *pname = t->base.name;
	if (isRemote(t))
		pname = mapiuri_table(t->query, sql->sa, pname);
	for (node *n = mt_rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		node *cn = NULL, *ci = NULL;
		const char *nname = e->r;

		if (nname[0] == '%' && strcmp(nname, TID) == 0) {
			list_append(p->exps, ne=exp_alias(sql, mtalias, TID, pname, TID, sql_fetch_localtype(TYPE_oid), CARD_MULTI, 0, 1, 1));
			ne->nid = e->nid;
			ne->alias.label = e->alias.label;
			rel_base_use_tid(sql, p);
		} else if (nname[0] != '%' && (cn = ol_find_name(mt->columns, nname))) {
			sql_column *c = cn->data, *rc = ol_fetch(t->columns, c->colnr);

			/* with name find column in merge table, with colnr find column in member */
			ne = exp_alias(sql, mtalias, exp_name(e), pname, rc->base.name, &rc->type, CARD_MULTI, rc->null, is_column_unique(rc), 0);
			if (rc->t->pkey && ((sql_kc*)rc->t->pkey->k.columns->h->data)->c == rc) {
				prop *p = ne->p = prop_create(sql->sa, PROP_HASHCOL, ne->p);
				p->value.pval = rc->t->pkey;
			} else if (rc->unique == 2) {
				prop *p = ne->p = prop_create(sql->sa, PROP_HASHCOL, ne->p);
				p->value.pval = NULL;
			}
			set_basecol(ne);
			sql_column_get_statistics(sql, c, ne);
			rel_base_use(sql, p, rc->colnr);
			list_append(p->exps, ne);
			ne->nid = e->nid;
			ne->alias.label = e->alias.label;
		} else if (nname[0] == '%' && ol_length(mt->idxs) && (ci = ol_find_name(mt->idxs, nname + 1))) {
			sql_idx *i = ci->data, *ri = NULL;

			/* indexes don't have a number field like 'colnr', so get the index the old way */
			for (node *nn = mt->idxs->l->h, *mm = t->idxs->l->h; nn && mm ; nn = nn->next, mm = mm->next) {
				sql_idx *ii = nn->data;

				if (ii->base.id == i->base.id) {
					ri = mm->data;
					break;
				}
			}

			assert((!hash_index(ri->type) || list_length(ri->columns) > 1) && idx_has_column(ri->type));
			sql_subtype *t = (ri->type == join_idx) ? sql_fetch_localtype(TYPE_oid) : sql_fetch_localtype(TYPE_lng);
			char *iname1 = sa_strconcat(sql->sa, "%", i->base.name), *iname2 = sa_strconcat(sql->sa, "%", ri->base.name);

			ne = exp_alias(sql, mtalias, iname1, pname, iname2, t, CARD_MULTI, has_nil(e), is_unique(e), 1);
			/* index names are prefixed, to make them independent */
			if (hash_index(ri->type)) {
				prop *p = ne->p = prop_create(sql->sa, PROP_HASHIDX, ne->p);
				p->value.pval = ri;
			} else if (ri->type == join_idx) {
				prop *p = ne->p = prop_create(sql->sa, PROP_JOINIDX, ne->p);
				p->value.pval = ri;
			}
			ne->nid = e->nid;
			ne->alias.label = e->alias.label;
			list_append(p->exps, ne);
		}
	}
	rel_base_set_mergetable(p, mt);
	return p;
}

void
rel_base_dump_exps( stream *fout, sql_rel *rel)
{
	int i = 0, comma = 0;
	sql_table *t = rel->l;
	rel_base_t *ba = rel->r;
	assert(ba);
	mnstr_printf(fout, "[ ");
	for (node *cn = ol_first_node(t->columns); cn; cn = cn->next, i++) {
		if (rel_base_is_used(ba, i)) {
			sql_column *c = cn->data;
			mnstr_printf(fout, "%s\"%s\".\"%s\"", comma?", ":"", t->base.name, c->base.name);
			if (ba->name)
				mnstr_printf(fout, " as \"%s\".\"%s\"", ba->name, c->base.name);
			comma = 1;
		}
	}
	if (rel_base_is_used(ba, i)) {
		mnstr_printf(fout, "%s\"%s\".\"%%TID%%\"", comma?", ":"", t->base.name);
		if (ba->name)
			mnstr_printf(fout, " as \"%s\".\"%%TID%%\"", ba->name);
		comma = 1;
	}
	i++;
	for (node *in = ol_first_node(t->idxs); in; in = in->next, i++) {
		if (rel_base_is_used(ba, i)) {
			sql_idx *i = in->data;
			mnstr_printf(fout, "%s\"%s\".\"%s\"", comma?", ":"", t->base.name, i->base.name);
			if (ba->name)
				mnstr_printf(fout, " as \"%s\".\"%s\"", ba->name, i->base.name);
			comma = 1;
		}
	}
	mnstr_printf(fout, " ]");
}

int
rel_base_has_column_privileges(mvc *sql, sql_rel *rel)
{
	sql_table *t = rel->l;
	int has = 0;

	for (node *m = ol_first_node(t->columns); m && !has; m = m->next) {
		sql_column *c = m->data;

		if (column_privs(sql, c, PRIV_SELECT))
			has = 1;
	}
	return has;
}

void
rel_base_set_mergetable(sql_rel *rel, sql_table *mt)
{
	rel_base_t *ba = rel->r;

	if (ba)
		ba->mt = mt;
}

sql_table *
rel_base_get_mergetable(sql_rel *rel)
{
	rel_base_t *ba = rel->r;

	return ba ? ba->mt : NULL;
}
