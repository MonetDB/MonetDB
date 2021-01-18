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

#include "gdk_atoms.h"


/* TODO
 * implement new double linked list
 * keep hash/map of names -> objectversion
 */

struct object_node;

typedef struct objectversion {
	bool deleted;
	ulng ts;
	sql_base *obj;
	struct objectversion *older;
	struct objectversion *newer;
	struct object_node *on;
} objectversion;

typedef struct object_node {
    struct object_node* prev;
    struct object_node* next;
    objectversion* data;
	struct objectset* os;
} object_node;

typedef struct objectset {
	int refcnt;
	sql_allocator *sa;
	destroy_fptr destroy;
	MT_Lock ht_lock;	/* latch protecting ht */
	object_node *h;
	object_node *t;
	int cnt;
	struct sql_hash *name_map;
	struct sql_hash *id_map;
	bool temporary;
	bool unique;	/* names are unique */
} objectset;

static int
os_id_key(object_node *n)
{
	return BATatoms[TYPE_int].atomHash(&n->data->obj->id);
}

static object_node *
find_id(objectset *os, sqlid id)
{
	if (os) {
		MT_lock_set(&os->ht_lock);
		if ((!os->id_map || os->id_map->size*16 < os->cnt) && os->cnt > HASH_MIN_SIZE && os->sa) {
			// TODO: This leaks the old map
			os->id_map = hash_new(os->sa, os->cnt, (fkeyvalue)&os_id_key);
			if (os->id_map == NULL) {
				MT_lock_unset(&os->ht_lock);
				return NULL;
			}

			for (object_node *n = os->h; n; n = n->next ) {
				int key = os_id_key(n);

				if (hash_add(os->id_map, key, n) == NULL) {
					MT_lock_unset(&os->ht_lock);
					return NULL;
				}
			}
		}
		if (os->id_map) {
			int key = BATatoms[TYPE_int].atomHash(&id);
			sql_hash_e *he = os->id_map->buckets[key&(os->id_map->size-1)];

			for (; he; he = he->chain) {
				object_node *n = he->value;

				if (n && n->data->obj->id == id) {
					MT_lock_unset(&os->ht_lock);
					return n;
				}
			}
			MT_lock_unset(&os->ht_lock);
			return NULL;
		}
		MT_lock_unset(&os->ht_lock);
		// TODO: can we actually reach this point?
		for (object_node *n = os->h; n; n = n->next) {
			objectversion *ov = n->data;

			/* check if ids match */
			if (id == ov->obj->id) {
				return n;
			}
		}
	}
	return NULL;
}

// TODO copy of static function from sql_list.c. Needs to be made external
static void
hash_delete(sql_hash *h, void *data)
{
	int key = h->key(data);
	sql_hash_e *e, *p = h->buckets[key&(h->size-1)];

	e = p;
	for (;  p && p->value != data ; p = p->chain)
		e = p;
	if (p && p->value == data) {
		if (p == e)
			h->buckets[key&(h->size-1)] = p->chain;
		else
			e->chain = p->chain;
	}
}

static void
node_destroy(objectset *os, object_node *n)
{
	if (n->data && os->destroy) {
		os->destroy(n->data, NULL);
		n->data = NULL;
	}
	if (!os->sa)
		_DELETE(n);
}

static object_node *
os_remove_node(objectset *os, object_node *n)
{
	assert(n);
	object_node *p = os->h;

	if (p != n)
		while (p && p->next != n)
			p = p->next;
	assert(p==n||(p && p->next == n));
	if (p == n) {
		os->h = n->next;
		if (os->h) // i.e. non-empty os
			os->h->prev = NULL;
		p = NULL;
	} else if ( p != NULL)  {
		p->next = n->next;
		if (p->next) // node in the middle
			p->next->prev = p;
	}
	if (n == os->t)
		os->t = p;

	MT_lock_set(&os->ht_lock);
	if (os->id_map && n)
		hash_delete(os->id_map, n);
	if (os->name_map && n)
		hash_delete(os->name_map, n);
	MT_lock_unset(&os->ht_lock);

	os->cnt--;
	assert(os->cnt > 0 || os->h == NULL);

	node_destroy(os, n);
	return p;
}

static object_node *
node_create(sql_allocator *sa, objectversion *ov)
{
	object_node *n = (sa)?SA_NEW(sa, object_node):MNEW(object_node);

	if (n == NULL)
		return NULL;
	*n = (object_node) {
		.data = ov,
	};
	ov->on = n;
	return n;
}

static objectset *
os_append_node(objectset *os, object_node *n)
{
	if (os->cnt) {
		os->t->next = n;
	} else {
		os->h = n;
	}
	n->os = os;
	n->prev = os->t; // aka the double linked list.
	os->t = n;
	os->cnt++;
	if (n->data) {
		MT_lock_set(&os->ht_lock);
		if (os->name_map) {
			int key = os->name_map->key(n);

			if (hash_add(os->name_map, key, n) == NULL) {
				MT_lock_unset(&os->ht_lock);
				return NULL;
			}
		}
		if (os->id_map) {
			int key = os->id_map->key(n);

			if (hash_add(os->id_map, key, n) == NULL) {
				MT_lock_unset(&os->ht_lock);
				return NULL;
			}
		}
		MT_lock_unset(&os->ht_lock);
	}
	return os;
}

static objectset *
os_append(objectset *os, objectversion *ov)
{
	object_node *n = node_create(os->sa, ov);

	if (n == NULL)
		return NULL;
	return os_append_node(os, n);
}

static object_node* find_name(objectset *os, const char *name);

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

	objectset* os = ov->on->os;
	if (!newer) {
		object_node *on = NULL;
		if (os->unique)
			on = find_name(os, ov->obj->name);
		else
			on = find_id(os, ov->obj->id);
		assert(on->data == ov);
		if (on)
			os_remove_node(os, on);
		if (older)
			os_append(os, older);
	}
	if (ov && os && os->destroy)
		os->destroy(store, ov->obj);
	/* free ov */
}

static int
tc_gc_objectversion(sql_store Store, sql_change *change, ulng commit_ts, ulng oldest)
{
	objectversion *ov = (objectversion*)change->data;

	if (ov->deleted || !commit_ts) {
		/* TODO handle savepoints */
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
	*os = (objectset) {
		.refcnt = 1,
		.sa = sa,
		.destroy = destroy,
		.temporary = temporary,
		.unique = unique
	};
	os->destroy = destroy;
	MT_lock_init(&os->ht_lock, "sa_ht_lock");
	return os;
}

objectset *
os_dup(objectset *os)
{
	os->refcnt++;
	return os;
}

// TODO: Look into cohesion between os_destroy, os->destroy and and node_destroy
void
os_destroy(objectset *os, sql_store store)
{
	if (--os->refcnt > 0)
		return;
	if (os->destroy) {
		for(object_node *n=os->h; n; n=n->next) {
			os->destroy(n->data, store);
		}
	}
	object_node *n = os->h;

	MT_lock_destroy(&os->ht_lock);
	os->h = NULL;
	if (os->destroy || os->sa == NULL) {
		while (n) {
			object_node *t = n;

			n = t->next;
			node_destroy(os, t);
		}
	}

	if (os->name_map && !os->name_map->sa)
		hash_destroy(os->name_map);

	if (os->id_map && !os->id_map->sa)
		hash_destroy(os->id_map);

	if (!os->sa)
		_DELETE(os);
}

static int
os_name_key(object_node *n)
{
	return hash_key(n->data->obj->name);
}

static sql_hash*
os_hash_create(objectset *os)
{
	MT_lock_set(&os->ht_lock);
	if ((!os->name_map || os->name_map->size*16 < os->cnt) && os->sa) {
		os->name_map = hash_new(os->sa, os->cnt, (fkeyvalue)&os_name_key);
		if (os->name_map == NULL) {
			MT_lock_unset(&os->ht_lock);
			return NULL;
		}

		for (object_node *n = os->h; n; n = n->next ) {
			int key = os_name_key(n);
			if (hash_add(os->name_map, key, n) == NULL) {
				MT_lock_unset(&os->ht_lock);
				return NULL;
			}
		}
	}
	MT_lock_unset(&os->ht_lock);
	return os->name_map;
}

static sql_hash_e *
find_hash_entry(sql_hash *map, const char *name)
{
	int key = hash_key(name);
	sql_hash_e *he = map->buckets[key&(map->size-1)];

	return he;
}

static object_node *
find_name(objectset *os, const char *name)
{
	if (os) {
		MT_lock_set(&os->ht_lock);
		if ((!os->name_map || os->name_map->size*16 < os->cnt) && os->cnt > HASH_MIN_SIZE && os->sa) {
			// TODO: This leaks the old map
			os->name_map = hash_new(os->sa, os->cnt, (fkeyvalue)&os_name_key);
			if (os->name_map == NULL) {
				MT_lock_unset(&os->ht_lock);
				return NULL;
			}

			for (object_node *n = os->h; n; n = n->next ) {
				int key = os_name_key(n);

				if (hash_add(os->name_map, key, n) == NULL) {
					MT_lock_unset(&os->ht_lock);
					return NULL;
				}
			}
		}
		if (os->name_map) {
			int key = hash_key(name);
			sql_hash_e *he = os->name_map->buckets[key&(os->name_map->size-1)];

			for (; he; he = he->chain) {
				object_node *n = he->value;

				if (n && n->data->obj->name && strcmp(n->data->obj->name, name) == 0) {
					MT_lock_unset(&os->ht_lock);
					return n;
				}
			}
			MT_lock_unset(&os->ht_lock);
			return NULL;
		}
		MT_lock_unset(&os->ht_lock);
		// TODO: can we actually reach this point?
		for (object_node *n = os->h; n; n = n->next) {
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
		if (ov->ts == tr->tid || (tr->parent && tr_version_of_parent(tr, ov->ts)) || ov->ts < tr->ts)
			return ov;
		else
			ov = ov->older;
	}
	return ov;
}

static void
os_update_hash(objectset *os, object_node *n, /*new*/ objectversion *ov)
{
	MT_lock_set(&os->ht_lock);
#if 0
	if (os->name_map) {
		hash_delete(os->name_map, n);
		int nkey = os->name_map->key(ov);
		hash_add(os->name_map, nkey, ov);
	}
#endif

	if (os->id_map) {
		hash_delete(os->id_map, n);
		int nkey = os->id_map->key(ov);
		hash_add(os->id_map, nkey, ov);
	}

//	n->data = ov;

	MT_lock_unset(&os->ht_lock);
}

int /*ok, error (name existed) and conflict (added before) */
os_add(objectset *os, struct sql_trans *tr, const char *name, sql_base *b)
{
	objectversion *ov = SA_NEW(os->sa, objectversion);

	ov->deleted = 0;
	ov->ts = tr->tid;
	ov->obj = b;
	ov->older = ov->newer = NULL;

	object_node *n = NULL;
	if (os->unique)
		n = find_name(os, name);
	else
		n = find_id(os, b->id);

	if (n) {
		objectversion *co = n->data;
		objectversion *oo = get_valid_object(tr, co);
		ov->on = oo->on;
		if (co != oo) { /* conflict ? */
			return -1;
		}
		ov->older = oo;

		MT_lock_set(&os->ht_lock);
		// TODO: double check/refine locking rationale
		if (oo)
			oo->newer = ov;
		n->data = ov;
		MT_lock_unset(&os->ht_lock);
		if (oo && oo->obj->id != ov->obj->id) {
			os_update_hash(os, n, ov);
		}
		if (!os->temporary)
			trans_add(tr, b, ov, &tc_gc_objectversion, &tc_commit_objectversion, NULL);
		return 0;
	} else { /* new */
		os_append(os, ov);
		if (!os->temporary)
			trans_add(tr, b, ov, &tc_gc_objectversion, &tc_commit_objectversion, NULL);
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

	object_node *n = NULL;
	if (os->unique)
		n = find_name(os, name);
	else
		n = find_id(os, b->id);

	if (n) {
		objectversion *co = n->data;
		objectversion *oo = get_valid_object(tr, co);
		ov->on = oo->on;
		if (co != oo) { /* conflict ? */
			return -1;
		}
		ov->older = oo;

		MT_lock_set(&os->ht_lock);
		// TODO: double check/refine locking rationale
		if (oo)
			oo->newer = ov;
		n->data = ov;
		MT_lock_unset(&os->ht_lock);
		if (!os->temporary)
			trans_add(tr, b, ov, &tc_gc_objectversion, &tc_commit_objectversion, NULL);
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
	if (os) {
		for(object_node *n = os->h; n; n=n->next) {
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
	object_node *n = find_name(os, name);

	if (n)
		objectversion_destroy(tr->store, n->data, 0, 0);
	return LOG_OK;
}

sql_base *
os_find_name(objectset *os, struct sql_trans *tr, const char *name)
{
	if (!os)
		return NULL;
	object_node *n = find_name(os, name);

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
	object_node *n = find_id(os, id);

	if (n) {
		 objectversion *ov = get_valid_object(tr, n->data);
		 if (ov && !ov->deleted)
			 return ov->obj;
	}
	return NULL;
}

void
os_iterator(struct os_iter *oi, struct objectset *os, struct sql_trans *tr, const char *name /*optional*/)
{
	*oi = (struct os_iter) {
		.os = os,
		.tr = tr,
		.name = name,
	};
	if (os->cnt && name) {
		if (!os->name_map)
			os->name_map = os_hash_create(os);
		oi->e = find_hash_entry(os->name_map, name);
	} else
		oi->n =	os->h;
}

sql_base *
oi_next(struct os_iter *oi)
{
	sql_base *b = NULL;

	if (oi->name) {
		sql_hash_e *e = oi->e;

		while (e && !b) {
			object_node *n = e->value;

			if (n && n->data->obj->name && strcmp(n->data->obj->name, oi->name) == 0) {
				objectversion *ov = n->data;
				e = oi->e = e->chain;

				ov = get_valid_object(oi->tr, ov);
				if (ov)
					b = ov->obj;
			} else {
				e = e->chain;
			}
		}
	} else {
		object_node *n = oi->n;

		while (n && !b) {
			objectversion *ov = n->data;
			n = oi->n = n->next;

			ov = get_valid_object(oi->tr, ov);
			if (ov)
				b = ov->obj;
		}
	}
	return b;
}

bool
os_obj_intransaction(objectset *os, struct sql_trans *tr, sql_base *b)
{
	object_node *n = find_id(os, b->id);

	if (n) {
		 objectversion *ov = get_valid_object(tr, n->data);
		 if (ov && !ov->deleted && ov->ts == tr->tid)
			 return true;
	}
	return false;
}
