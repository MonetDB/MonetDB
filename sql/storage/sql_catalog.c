/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_catalog.h"
#include "sql_storage.h"

const char *TID = "%TID%";

inline int
base_key( sql_base *b )
{
	return hash_key(b->name);
}

static void *
_list_find_name(list *l, const char *name)
{
	node *n;

	if (l) {
		if (l->ht) {
			int key = hash_key(name);
			sql_hash_e *he = l->ht->buckets[key&(l->ht->size-1)];

			for (; he; he = he->chain) {
				sql_base *b = he->value;

				if (b->name && strcmp(b->name, name) == 0) {
					return b;
				}
			}
			return NULL;
		}
		for (n = l->h; n; n = n->next) {
			sql_base *b = n->data;

			/* check if names match */
			if (name[0] == b->name[0] && strcmp(name, b->name) == 0) {
				return b;
			}
		}
	}
	return NULL;
}

void
trans_add(sql_trans *tr, sql_base *b, void *data, tc_cleanup_fptr cleanup, tc_commit_fptr commit, tc_log_fptr log)
{
	sql_change *change = SA_NEW(tr->sa, sql_change);

	*change = (sql_change) {
		.obj = b,
		.data = data,
		.cleanup = cleanup,
		.commit = commit,
		.log = log,
	};
	MT_lock_set(&tr->lock);
	tr->changes = sa_list_append(tr->sa, tr->changes, change);
	if (log)
		tr->logchanges++;
	MT_lock_unset(&tr->lock);
}

int
tr_version_of_parent(sql_trans *tr, ulng ts)
{
	for( tr = tr->parent; tr; tr = tr->parent)
		if (tr->tid == ts)
			return 1;
	return 0;
}

node *
list_find_name(list *l, const char *name)
{
	node *n;

	if (l)
		for (n = l->h; n; n = n->next) {
			sql_base *b = n->data;

			/* check if names match */
			if (name[0] == b->name[0] && strcmp(name, b->name) == 0) {
				return n;
			}
		}
	return NULL;
}

node *
cs_find_id(changeset * cs, sqlid id)
{
	node *n;
	list *l = cs->set;

	if (l)
		for (n = l->h; n; n = n->next) {
			sql_base *b = n->data;

			/* check if names match */
			if (b->id == id) {
				return n;
			}
		}
	return NULL;
}

node *
list_find_id(list *l, sqlid id)
{
	if (l) {
		node *n;
		for (n = l->h; n; n = n->next) {

			/* check if ids match */
			if (id == *(sqlid *) n->data) {
				return n;
			}
		}
	}
	return NULL;
}

node *
list_find_base_id(list *l, sqlid id)
{
	if (l) {
		node *n;
		for (n = l->h; n; n = n->next) {
			sql_base *b = n->data;

			if (id == b->id)
				return n;
		}
	}
	return NULL;
}

sql_key *
find_sql_key(sql_table *t, const char *kname)
{
	node *n = ol_find_name(t->keys, kname);
	if (n)
		return n->data;
	return NULL;
}

sql_key *
sql_trans_find_key(sql_trans *tr, sqlid id)
{
	struct os_iter oi;
	os_iterator(&oi, tr->cat->schemas, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_schema *s = (sql_schema*)b;
		sql_base *bk = os_find_id(s->keys, tr, id);
		if (bk)
				return (sql_key*)bk;
	}
	return NULL;
}

sql_key *
schema_find_key(sql_trans *tr, sql_schema *s, const char *name)
{
	sql_base *b = os_find_name(s->keys, tr, name);

	if (!b && tr->tmp == s && tr->localtmps.set) { /* for localtmps search tables */
		for(node *n = tr->localtmps.set->h; n; n = n->next) {
			sql_table *t = n->data;
			sql_key *o = find_sql_key(t, name);
			if (o)
				return o;
		}
	}
	return (sql_key*)b;
}

sql_idx *
find_sql_idx(sql_table *t, const char *iname)
{
	node *n = ol_find_name(t->idxs, iname);
	if (n)
		return n->data;
	return NULL;
}

sql_idx *
sql_trans_find_idx(sql_trans *tr, sqlid id)
{
	struct os_iter oi;
	os_iterator(&oi, tr->cat->schemas, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_schema *s = (sql_schema*)b;
		sql_base *bi = os_find_id(s->idxs, tr, id);
		if (bi)
			return (sql_idx*)bi;
	}
	return NULL;
}

sql_idx *
schema_find_idx(sql_trans *tr, sql_schema *s, const char *name)
{
	sql_base *b = os_find_name(s->idxs, tr, name);

	if (!b && tr->tmp == s && tr->localtmps.set) { /* for localtmps search tables */
		for(node *n = tr->localtmps.set->h; n; n = n->next) {
			sql_table *t = n->data;
			sql_idx *o = find_sql_idx(t, name);
			if (o)
				return o;
		}
	}
	return (sql_idx*)b;
}

sql_idx *
schema_find_idx_id(sql_trans *tr, sql_schema *s, sqlid id)
{
	sql_base *b = os_find_id(s->idxs, tr, id);

	if (!b && tr->tmp == s && tr->localtmps.set) { /* for localtmps search tables */
		for(node *n = tr->localtmps.set->h; n; n = n->next) {
			sql_table *t = n->data;
			node *o = ol_find_id(t->idxs, id);
			if (o)
				return (sql_idx*)o->data;
		}
	}
	return (sql_idx*)b;
}


sql_column *
find_sql_column(sql_table *t, const char *cname)
{
	node *n = ol_find_name(t->columns, cname);
	if (n)
		return n->data;
	return NULL;
}

sql_table *
find_sql_table(sql_trans *tr, sql_schema *s, const char *tname)
{
	sql_table *t = (sql_table*)os_find_name(s->tables, tr, tname);
	if (!t && tr->tmp == s)
		t = (sql_table*)_list_find_name(tr->localtmps.set, tname);
	return t;
}

sql_table *
find_sql_table_id(sql_trans *tr, sql_schema *s, sqlid id)
{
	sql_table *t = (sql_table*)os_find_id(s->tables, tr, id);
	if (!t && tr->tmp == s) {
		node *n = cs_find_id(&tr->localtmps, id);
		if (n)
			return (sql_table*)n->data;
	}
	return t;
}

sql_table *
sql_trans_find_table(sql_trans *tr, sqlid id)
{
	struct os_iter oi;
	os_iterator(&oi, tr->cat->schemas, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_schema *s = (sql_schema*)b;
		sql_base *bt = os_find_id(s->tables, tr, id);
		if (bt)
			return (sql_table*)bt;
	}
	return NULL;
}

sql_sequence *
find_sql_sequence(sql_trans *tr, sql_schema *s, const char *sname)
{
	return (sql_sequence*)os_find_name(s->seqs, tr, sname);
}

sql_schema *
find_sql_schema(sql_trans *tr, const char *sname)
{
	if (tr->tmp && strcmp(sname, "tmp")==0)
		return tr->tmp;
	return (sql_schema*)os_find_name(tr->cat->schemas, tr, sname);
}

sql_schema *
find_sql_schema_id(sql_trans *tr, sqlid id)
{
	if (tr->tmp && tr->tmp->base.id == id)
		return tr->tmp;
	return (sql_schema*)os_find_id(tr->cat->schemas, tr, id);
}

sql_type *
find_sql_type(sql_trans *tr, sql_schema *s, const char *tname)
{
	return (sql_type*)os_find_name(s->types, tr, tname);
}

sql_type *
sql_trans_bind_type(sql_trans *tr, sql_schema *c, const char *name)
{
	struct os_iter oi;
	os_iterator(&oi, tr->cat->schemas, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_schema *s = (sql_schema*)b;
		sql_type *t = find_sql_type(tr, s, name);
		if (t)
			return t;
	}
	if (c)
		return find_sql_type(tr, c, name);
	return NULL;
}

sql_type *
sql_trans_find_type(sql_trans *tr, sql_schema *s, sqlid id)
{
	if (s) {
		sql_base *b = os_find_id(s->types, tr, id);
		if (b)
			return (sql_type*)b;
	} else {
		struct os_iter oi;
		os_iterator(&oi, tr->cat->schemas, tr, NULL);
		for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
			sql_schema *s = (sql_schema*)b;
			sql_base *bt = os_find_id(s->types, tr, id);
			if (bt)
				return (sql_type*)bt;
		}
	}
	return NULL;
}

sql_func *
sql_trans_find_func(sql_trans *tr, sqlid id)
{
	struct os_iter oi;
	os_iterator(&oi, tr->cat->schemas, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_schema *s = (sql_schema*)b;
		sql_base *bf = os_find_id(s->funcs, tr, id);
		if (bf)
			return (sql_func*)bf;
	}
	return NULL;
}

static sql_trigger *
find_sql_trigger(sql_table *t, const char *tname)
{
	node *n = ol_find_name(t->triggers, tname);
	if (n)
		return n->data;
	return NULL;
}

sql_trigger *
sql_trans_find_trigger(sql_trans *tr, sqlid id)
{
	struct os_iter oi;
	os_iterator(&oi, tr->cat->schemas, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_schema *s = (sql_schema*)b;
		sql_base *bt = os_find_id(s->triggers, tr, id);
		if (bt)
			return (sql_trigger*)bt;
	}
	return NULL;
}

sql_trigger *
schema_find_trigger(sql_trans *tr, sql_schema *s, const char *name)
{
	sql_base *b = os_find_name(s->triggers, tr, name);

	if (!b && tr->tmp == s && tr->localtmps.set) { /* for localtmps search tables */
		for(node *n = tr->localtmps.set->h; n; n = n->next) {
			sql_table *t = n->data;
			sql_trigger *o = find_sql_trigger(t, name);
			if (o)
				return o;
		}
	}
	return (sql_trigger*)b;
}

void*
sql_values_list_element_validate_and_insert(void *v1, void *v2, void *tpe, int* res)
{
	sql_part_value* pt = (sql_part_value*) v1, *newp = (sql_part_value*) v2;
	sql_subtype *tp = (sql_subtype *) tpe;

	*res = ATOMcmp(tp->type->localtype, newp->value, pt->value);
	return *res == 0 ? pt : NULL;
}

void*
sql_range_part_validate_and_insert(void *v1, void *v2, void *type)
{
	sql_part* pt = (sql_part*) v1, *newp = (sql_part*) v2;
	int res1, res2, tpe = *(int*)type;
	const void *nil = ATOMnilptr(tpe);
	bool pt_down_all = false, pt_upper_all = false, newp_down_all = false, newp_upper_all = false, pt_min_max_same = false, newp_min_max_same = false;

	if (pt == newp) /* same pointer, skip (used in updates) */
		return NULL;

	if (is_bit_nil(pt->with_nills) || is_bit_nil(newp->with_nills)) /* if one partition holds all including nills, then conflicts */
		return pt;
	if (newp->with_nills && pt->with_nills) /* only one partition at most has null values */
		return pt;

	pt_down_all = !ATOMcmp(tpe, nil, pt->part.range.minvalue);
	pt_upper_all = !ATOMcmp(tpe, nil, pt->part.range.maxvalue);
	newp_down_all = !ATOMcmp(tpe, nil, newp->part.range.minvalue);
	newp_upper_all = !ATOMcmp(tpe, nil, newp->part.range.maxvalue);

	/* if one partition just holds NULL values, then there's no conflict */
	if ((newp_down_all && newp_upper_all && newp->with_nills) || (pt_down_all && pt_upper_all && pt->with_nills))
		return NULL;
	 /* holds all range, will always conflict */
	if ((pt_down_all && pt_upper_all && !pt->with_nills) || (newp_down_all && newp_upper_all && !newp->with_nills))
		return pt;

	pt_min_max_same = !ATOMcmp(tpe, pt->part.range.maxvalue, pt->part.range.minvalue);
	newp_min_max_same = !ATOMcmp(tpe, newp->part.range.maxvalue, newp->part.range.minvalue);

	if (pt_down_all) { /* from range min value until a value */
		res1 = ATOMcmp(tpe, pt->part.range.maxvalue, newp->part.range.minvalue);
		if (newp_down_all || (!newp_min_max_same && res1 > 0) || (newp_min_max_same && res1 >= 0))
			return pt;
		return NULL;
	}
	if (pt_upper_all) { /* from value until range max value */
		res1 = ATOMcmp(tpe, newp->part.range.maxvalue, pt->part.range.minvalue);
		if (newp_upper_all || (!newp_min_max_same && res1 > 0) || (newp_min_max_same && res1 >= 0))
			return pt;
		return NULL;
	}
	if (newp_down_all) { /* from range min value until a value */
		res1 = ATOMcmp(tpe, newp->part.range.maxvalue, pt->part.range.minvalue);
		if (pt_down_all || (!newp_min_max_same && res1 > 0) || (newp_min_max_same && res1 >= 0))
			return pt;
		return NULL;
	}
	if (newp_upper_all) { /* from value until range max value */
		res1 = ATOMcmp(tpe, pt->part.range.maxvalue, newp->part.range.minvalue);
		if (pt_upper_all || (!pt_min_max_same && res1 > 0) || (pt_min_max_same && res1 >= 0))
			return pt;
		return NULL;
	}

	/* Fallback into normal cases */
	res1 = ATOMcmp(tpe, newp->part.range.maxvalue, pt->part.range.minvalue);
	res2 = ATOMcmp(tpe, pt->part.range.maxvalue, newp->part.range.minvalue);
	/* overlap: y2 > x1 && x2 > y1 */
	if (((!newp_min_max_same && res1 > 0) || (newp_min_max_same && res1 >= 0)) && ((!pt_min_max_same && res2 > 0) || (pt_min_max_same && res2 >= 0)))
		return pt;
	return NULL;
}

void*
sql_values_part_validate_and_insert(void *v1, void *v2, void *type)
{
	sql_part* pt = (sql_part*) v1, *newp = (sql_part*) v2;
	list* b1 = pt->part.values, *b2 = newp->part.values;
	node *n1 = b1->h, *n2 = b2->h;
	int res, tpe = *(int*)type;

	if (pt == newp) /* same pointer, skip (used in updates) */
		return NULL;

	if (newp->with_nills && pt->with_nills)
		return pt; /* check for nulls first */

	while (n1 && n2) {
		sql_part_value *p1 = (sql_part_value *) n1->data, *p2 = (sql_part_value *) n2->data;
		res = ATOMcmp(tpe, p1->value, p2->value);
		if (!res) { /* overlap -> same value in both partitions */
			return pt;
		} else if (res < 0) {
			n1 = n1->next;
		} else {
			n2 = n2->next;
		}
	}
	return NULL;
}

sql_part *
partition_find_part(sql_trans *tr, sql_table *pt, sql_part *pp)
{
	struct os_iter oi;

	if (!pt->s) /* declared table */
		return NULL;
	os_iterator(&oi, pt->s->parts, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_part *p = (sql_part*)b;

		if (pp) {
			if (p == pp)
				pp = NULL;
			continue;
		}
		if (p->member == pt->base.id)
			return p;
	}
	return NULL;
}

node *
members_find_child_id(list *l, sqlid id)
{
	if (l) {
		node *n;
		for (n = l->h; n; n = n->next) {
			sql_part *p = n->data;

			if (id == p->member)
				return n;
		}
	}
	return NULL;
}

int
nested_mergetable(sql_trans *tr, sql_table *mt, const char *sname, const char *tname)
{
	if (strcmp(mt->s->base.name, sname) == 0 && strcmp(mt->base.name, tname) == 0)
		return 1;
	/* try if this is also a partition */
	for( sql_part *parent = partition_find_part(tr, mt, NULL); parent; parent = partition_find_part(tr, mt, parent)) {
		if (nested_mergetable(tr, parent->t, sname, tname))
			return 1;
	}
	return 0;
}

bool
is_column_unique(sql_column *c)
{
	/* is it a primary key column itself? */
	if (c->t->pkey && list_length(c->t->pkey->k.columns) == 1 && ((sql_kc*)c->t->pkey->k.columns->h->data)->c->base.id == c->base.id)
		return true;
	/* is it a unique key itself */
	return c->unique == 2;
}

ValPtr
SA_VALcopy(sql_allocator *sa, ValPtr d, const ValRecord *s)
{
	if (sa == NULL)
		return VALcopy(d, s);
	if (!ATOMextern(s->vtype)) {
		*d = *s;
	} else if (s->val.pval == 0) {
		const void *p = ATOMnilptr(s->vtype);
		d->vtype = s->vtype;
		d->len = ATOMlen(d->vtype, p);
		d->val.pval = sa_alloc(sa, d->len);
		if (d->val.pval == NULL)
			return NULL;
		memcpy(d->val.pval, p, d->len);
	} else if (s->vtype == TYPE_str) {
		const char *p = s->val.sval;
		d->vtype = TYPE_str;
		d->len = strLen(p);
		d->val.sval = sa_alloc(sa, d->len);
		if (d->val.sval == NULL)
			return NULL;
		memcpy(d->val.sval, p, d->len);
	} else {
		const void *p = s->val.pval;
		d->vtype = s->vtype;
		d->len = ATOMlen(d->vtype, p);
		d->val.pval = sa_alloc(sa, d->len);
		if (d->val.pval == NULL)
			return NULL;
		memcpy(d->val.pval, p, d->len);
	}
	return d;
}

atom *
atom_copy(sql_allocator *sa, atom *a)
{
	atom *r = sa ?SA_NEW(sa, atom):MNEW(atom);
	if (!r)
		return NULL;

	*r = (atom) {
		.isnull = a->isnull,
		.tpe = a->tpe,
		.data = (ValRecord) {.vtype = TYPE_void,},
	};
	if (!a->isnull)
		SA_VALcopy(sa, &r->data, &a->data);
	return r;
}
