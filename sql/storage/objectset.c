/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_catalog.h"
#include "sql_storage.h"


/* TODO
 * implement new double linked list
 * keep hash/map of names -> objectversion
 */

typedef struct objectversion {
	bool deleted;
	ulng ts;
	sql_base *obj;
	struct objectversion *older;
	struct objectversion *newer;
	struct objectset *os;
} objectversion;

typedef struct objectset {
	int refcnt;
	sql_allocator *sa;
	destroy_fptr destroy;
	struct list *objs;
	bool temporary;
	bool unique;	/* names are unique */
} objectset;

static node *
find_id(list *l, sqlid id)
{
	if (l) {
		for (node *n = l->h; n; n = n->next) {
			objectversion *ov = n->data;

			/* check if ids match */
			if (id == ov->obj->id) {
				return n;
			}
		}
	}
	return NULL;
}

static void
objectversion_destroy(sqlstore *store, objectversion *ov, ulng commit_ts, ulng oldest)
{
	objectversion *older = ov->older;
	objectversion *newer = ov->newer;

	if (older && commit_ts) {
		objectversion_destroy(store, older, commit_ts, oldest);
		older = NULL;
	} else if (older) {
		older->newer = newer;
	}
	ov->older = NULL;

	if (newer && commit_ts)
		newer->older = NULL;
	else if (newer && older)
		newer->older = older;

	if (!newer && ov->os) {
		node *on = find_id(ov->os->objs, ov->obj->id);
		assert(on->data == ov);
		if (on)
			list_remove_node(ov->os->objs, on);
		if (older)
			list_append(ov->os->objs, older);
	}
	if (ov && ov->os && ov->os->destroy)
		ov->os->destroy(store, ov->obj);
	/* free ov */
}

static int
tc_gc_objectversion(sql_store Store, sql_change *change, ulng commit_ts, ulng oldest)
{
	objectversion *ov = (objectversion*)change->data;

	if (ov->deleted || !commit_ts) {
		if (ov->ts < oldest || (ov->ts == commit_ts && commit_ts == oldest) || !commit_ts) {
			int ok = LOG_OK;
			objectversion_destroy(Store, ov, commit_ts, oldest);
			if (ok == LOG_OK)
				return 1; /* handled */
			else
				return LOG_ERR;
		}
	}
	return LOG_OK;
}

static int
tc_commit_objectversion(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	objectversion *ov = (objectversion*)change->data;
	assert(ov->ts == tr->tid);
	ov->ts = commit_ts;
	(void)oldest;
	return LOG_OK;
}

objectset *
os_new(sql_allocator *sa, destroy_fptr destroy, bool temporary, bool unique)
{
	objectset *os = SA_NEW(sa, objectset);
	os->refcnt = 1;
	os->sa = sa;
	os->destroy = destroy;
	os->objs = list_new(sa, NULL);
	os->temporary = temporary;
	os->unique = unique;
	return os;
}

objectset *
os_dup(objectset *os)
{
	os->refcnt++;
	return os;
}

void
os_destroy(objectset *os, sql_store store)
{
	if (--os->refcnt > 0)
		return;
	if (os->destroy) {
		for(node *n=os->objs->h; n; n=n->next) {
			os->destroy(store, n->data);
		}
	}
	list_destroy(os->objs);
}

static int
ov_key(objectversion *ov)
{
	return base_key(ov->obj);
}

static node *
find_name(list *l, const char *name)
{
	node *n;

	if (l) {
		MT_lock_set(&l->ht_lock);
		if ((!l->ht || l->ht->size*16 < list_length(l)) && list_length(l) > HASH_MIN_SIZE && l->sa) {
			l->ht = hash_new(l->sa, list_length(l), (fkeyvalue)&ov_key);
			if (l->ht == NULL) {
				MT_lock_unset(&l->ht_lock);
				return NULL;
			}

			for (n = l->h; n; n = n->next ) {
				int key = ov_key(n->data);

				if (hash_add(l->ht, key, n->data) == NULL) {
					MT_lock_unset(&l->ht_lock);
					return NULL;
				}
			}
		}
		if (l->ht) {
			int key = hash_key(name);
			sql_hash_e *he = l->ht->buckets[key&(l->ht->size-1)];

			for (; he; he = he->chain) {
				objectversion *ov = he->value;

				if (ov && ov->obj->name && strcmp(ov->obj->name, name) == 0) {
					MT_lock_unset(&l->ht_lock);
					return list_find(l, ov, NULL);
				}
			}
			MT_lock_unset(&l->ht_lock);
			return NULL;
		}
		MT_lock_unset(&l->ht_lock);
		for (n = l->h; n; n = n->next) {
			objectversion *ov = n->data;

			/* check if names match */
			if (name[0] == ov->obj->name[0] && strcmp(name, ov->obj->name) == 0) {
				return n;
			}
		}
	}
	return NULL;
}

static objectversion*
get_valid_object(sql_trans *tr, objectversion *ov)
{
	while(ov) {
		if (ov->ts == tr->tid || ov->ts < tr->ts)
			return ov;
		else
			ov = ov->older;
	}
	return ov;
}

int /*ok, error (name existed) and conflict (added before) */
os_add(objectset *os, struct sql_trans *tr, const char *name, sql_base *b)
{
	objectversion *ov = SA_NEW(os->sa, objectversion);

	ov->deleted = 0;
	ov->ts = tr->tid;
	ov->obj = b;
	ov->older = ov->newer = NULL;
	ov->os = os;

	node *n = NULL;
	if (os->unique)
		n = find_name(os->objs, name);
	else
		n = find_id(os->objs, b->id);

	if (n) {
		objectversion *co = n->data;
		objectversion *oo = get_valid_object(tr, co);
		if (co != oo) { /* conflict ? */
			return -1;
		}
		ov->older = oo;
		if (oo)
			oo->newer = ov;
		list_update_data(os->objs, n, ov);
		if (!os->temporary)
			trans_add(tr, b, ov, &tc_gc_objectversion, &tc_commit_objectversion);
		return 0;
	} else { /* new */
		list_append(os->objs, ov);
		if (!os->temporary)
			trans_add(tr, b, ov, &tc_gc_objectversion, &tc_commit_objectversion);
		return 0;
	}
}

int
os_del(objectset *os, struct sql_trans *tr, const char *name, sql_base *b)
{
	objectversion *ov = SA_NEW(os->sa, objectversion);

	ov->deleted = 1;
	ov->ts = tr->tid;
	ov->obj = b;
	ov->older = ov->newer = NULL;
	ov->os = os;

	node *n = NULL;
	if (os->unique)
		n = find_name(os->objs, name);
	else
		n = find_id(os->objs, b->id);

	if (n) {
		objectversion *co = n->data;
		objectversion *oo = get_valid_object(tr, co);
		if (co != oo) { /* conflict ? */
			return -1;
		}
		ov->older = oo;
		if (oo)
			oo->newer = ov;
		list_update_data(os->objs, n, ov);
		if (!os->temporary)
			trans_add(tr, b, ov, &tc_gc_objectversion, &tc_commit_objectversion);
		return 0;
	} else {
		/* missing */
		return -1;
	}
}

int
os_size(objectset *os, struct sql_trans *tr)
{
	int cnt = 0;
	if (os->objs) {
		for(node *n = os->objs->h; n; n=n->next) {
			objectversion *ov = n->data;
			if ((ov=get_valid_object(tr, ov)) && !ov->deleted)
				cnt++;
		}
	}
	return cnt;
}

int
os_empty(objectset *os, struct sql_trans *tr)
{
	return os_size(os, tr)==0;
}

int
os_remove(objectset *os, sql_trans *tr, const char *name)
{
	node *n = find_name(os->objs, name);

	if (n)
		objectversion_destroy(tr->store, n->data, 0, 0);
	return LOG_OK;
}

sql_base *
os_find_name(objectset *os, struct sql_trans *tr, const char *name)
{
	if (!os)
		return NULL;
	node *n = find_name(os->objs, name);

	if (n) {
		 objectversion *ov = get_valid_object(tr, n->data);
		 if (ov && !ov->deleted)
			 return ov->obj;
	}
	return NULL;
}

sql_base *
os_find_id(objectset *os, struct sql_trans *tr, sqlid id)
{
	if (!os)
		return NULL;
	node *n = find_id(os->objs, id);

	if (n) {
		 objectversion *ov = get_valid_object(tr, n->data);
		 if (ov && !ov->deleted)
			 return ov->obj;
	}
	return NULL;
}

typedef struct os_iter {
	objectset *os;
	sql_trans *tr;
	const char *name;
	node *n;
} os_iter;

os_iter *
os_iterator(objectset *os, struct sql_trans *tr, const char *name /*optional*/)
{
	os_iter *oi = SA_NEW(os->sa, os_iter);

	oi->os = os;
	oi->tr = tr;
	oi->name = name;
	/*
	if (name)
		oi->n = find_name(os->objs, name);
	else
	*/
		oi->n = os->objs->h;
	return oi;
}

sql_base *
oi_next(os_iter *oi)
{
	node *n = oi->n;
	sql_base *b = NULL;

	while (n && !b) {
		objectversion *ov = n->data;
		n = oi->n = n->next;

		ov = get_valid_object(oi->tr, ov);
		if (ov)
			b = ov->obj;
	}
	return b;
}
