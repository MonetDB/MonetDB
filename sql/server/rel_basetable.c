/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_basetable.h"
#include "rel_remote.h"
#include "sql_privileges.h"

#define USED_LEN(nr) ((nr+31)/32)
#define rel_base_set_used(b,nr) b->used[(nr)/32] |= (1<<((nr)%32))
#define rel_base_is_used(b,nr) ((b->used[(nr)/32]&(1<<((nr)%32))) != 0)

typedef struct rel_base_t {
	sql_table *mt;
	char *name;
	int disallowed;	/* ie check per column */
	int used[FLEXIBLE_ARRAY_MEMBER];
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
			ba->used[i] = ~0;
	}
}

sql_rel *
rel_basetable(mvc *sql, sql_table *t, const char *atname)
{
	sql_allocator *sa = sql->sa;
	sql_rel *rel = rel_create(sa);
	int nrcols = ol_length(t->columns), end = nrcols + 1 + ol_length(t->idxs);
	rel_base_t *ba = (rel_base_t*)sa_zalloc(sa, sizeof(rel_base_t) + sizeof(int)*USED_LEN(end));

	if(!rel || !ba)
		return NULL;

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
bind_col_exp(mvc *sql, char *name, sql_column *c)
{
	prop *p = NULL;
	sql_exp *e = exp_column(sql->sa, name, c->base.name, &c->type, CARD_MULTI, c->null, 0);

	if (c->t->pkey && ((sql_kc*)c->t->pkey->k.columns->h->data)->c == c) {
		p = e->p = prop_create(sql->sa, PROP_HASHCOL, e->p);
		p->value = c->t->pkey;
	} else if (c->unique == 1) {
		p = e->p = prop_create(sql->sa, PROP_HASHCOL, e->p);
		p->value = NULL;
	}
	set_basecol(e);
	return e;
}

static sql_exp *
bind_col(mvc *sql, sql_rel *rel, char *name, sql_column *c )
{
	if (rel_base_use(sql, rel, c->colnr)) {
		/* error */
		return NULL;
	}
	return bind_col_exp(sql, name, c);
}

sql_exp *
rel_base_bind_colnr( mvc *sql, sql_rel *rel, int nr)
{
	sql_table *t = rel->l;
	rel_base_t *ba = rel->r;
	return bind_col(sql, rel, ba->name?ba->name:t->base.name, ol_fetch(t->columns, nr));
}

sql_exp *
rel_base_bind_column( mvc *sql, sql_rel *rel, const char *cname, int no_tname)
{
	sql_table *t = rel->l;
	rel_base_t *ba = rel->r;
	(void)no_tname;
	node *n = ol_find_name(t->columns, cname);
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
		if (rel_base_is_used(ba, i))
			append(exps, bind_col_exp(sql, name, cn->data));
	}
	if (intern && rel_base_is_used(ba, i))
		append(exps, exp_column(sql->sa, name, TID, sql_bind_localtype("oid"), CARD_MULTI, 0, 1));
	if (intern) {
		for (node *in = ol_first_node(t->idxs); in; in = in->next, i++) {
			if (rel_base_is_used(ba, i)) {
				sql_idx *i = in->data;
				sql_subtype *t = sql_bind_localtype("lng"); /* hash "lng" */
				int has_nils = 0;

				if ((hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
					continue;

				if (i->type == join_idx)
					t = sql_bind_localtype("oid");

				char *iname = sa_strconcat( sql->sa, "%", i->base.name);
				for (node *n = i->columns->h ; n && !has_nils; n = n->next) { /* check for NULL values */
					sql_kc *kc = n->data;

					if (kc->c->null)
						has_nils = 1;
				}
				sql_exp *e = exp_column(sql->sa, name, iname, t, CARD_MULTI, has_nils, 1);
				if (hash_index(i->type)) {
					p = e->p = prop_create(sql->sa, PROP_HASHIDX, e->p);
					p->value = i;
				}
				if (i->type == join_idx) {
					p = e->p = prop_create(sql->sa, PROP_JOINIDX, e->p);
					p->value = i;
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
		sql_exp *e = exp_alias(sql->sa, atname, c->base.name, tname, c->base.name, &c->type, CARD_MULTI, c->null, 0);

		if (e == NULL) {
			rel_destroy(r);
			return NULL;
		}
		if (c->t->pkey && ((sql_kc*)c->t->pkey->k.columns->h->data)->c == c) {
			p = e->p = prop_create(sql->sa, PROP_HASHCOL, e->p);
			p->value = c->t->pkey;
		} else if (c->unique == 1) {
			p = e->p = prop_create(sql->sa, PROP_HASHCOL, e->p);
			p->value = NULL;
		}
		set_basecol(e);
		append(r->exps, e);
	}
	return r;
}

sql_rel *
rewrite_basetable(mvc *sql, sql_rel *rel)
{
	if (is_basetable(rel->op) && !rel->exps) {
		sql_allocator *sa = sql->sa;
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
			sql_exp *e = exp_alias(sa, atname, c->base.name, tname, c->base.name, &c->type, CARD_MULTI, c->null, 0);

			if (e == NULL) {
				rel_destroy(rel);
				return NULL;
			}
			if (c->t->pkey && ((sql_kc*)c->t->pkey->k.columns->h->data)->c == c) {
				p = e->p = prop_create(sa, PROP_HASHCOL, e->p);
				p->value = c->t->pkey;
			} else if (c->unique == 1) {
				p = e->p = prop_create(sa, PROP_HASHCOL, e->p);
				p->value = NULL;
			}
			set_basecol(e);
			append(rel->exps, e);
		}
		append(rel->exps, exp_alias(sa, atname, TID, tname, TID, sql_bind_localtype("oid"), CARD_MULTI, 0, 1));
		i++;

		for (cn = ol_first_node(t->idxs); cn; cn = cn->next, i++) {
			if (!rel_base_is_used(ba, i))
				continue;

			sql_exp *e;
			sql_idx *i = cn->data;
			sql_subtype *t = sql_bind_localtype("lng"); /* hash "lng" */
			char *iname = NULL;
			int has_nils = 0;

			/* do not include empty indices in the plan */
			if ((hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
				continue;

			if (i->type == join_idx)
				t = sql_bind_localtype("oid");

			iname = sa_strconcat( sa, "%", i->base.name);
			for (node *n = i->columns->h ; n && !has_nils; n = n->next) { /* check for NULL values */
				sql_kc *kc = n->data;

				if (kc->c->null)
					has_nils = 1;
			}
			e = exp_alias(sa, atname, iname, tname, iname, t, CARD_MULTI, has_nils, 1);
			/* index names are prefixed, to make them independent */
			if (hash_index(i->type)) {
				p = e->p = prop_create(sa, PROP_HASHIDX, e->p);
				p->value = i;
			}
			if (i->type == join_idx) {
				p = e->p = prop_create(sa, PROP_JOINIDX, e->p);
				p->value = i;
			}
			append(rel->exps, e);
		}
	}
	return rel;
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
		mnstr_printf(fout, "%s\"%s\".\"%%TID\"", comma?", ":"", t->base.name);
		if (ba->name)
			mnstr_printf(fout, " as \"%s\".\"%%TID\"", ba->name);
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
	mnstr_printf(fout, " ]\n");
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

	ba->mt = mt;
}

sql_table *
rel_base_get_mergetable(sql_rel *rel)
{
	rel_base_t *ba = rel->r;

	return ba->mt;
}
