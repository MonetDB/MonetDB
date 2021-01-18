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

struct object_node;// TODO: rename to object_version_chain

typedef struct objectversion {
	bool deleted;
	ulng ts;
	ulng tombstone; // ts of latest active transaction at the time of funeral
	sql_base *obj;
	struct objectversion	*name_based_older;
	struct objectversion	*name_based_newer;
	struct object_node		*name_based_chain;

	struct objectversion	*id_based_older;
	struct objectversion	*id_based_newer;
	struct object_node		*id_based_chain;
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
	object_node *name_based_h;
	object_node *name_based_t;
	object_node *id_based_h;
	object_node *id_based_t;
	object_node * graveyard;
	int name_based_cnt;
	int id_based_cnt;
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
		if ((!os->id_map || os->id_map->size*16 < os->id_based_cnt) && os->id_based_cnt > HASH_MIN_SIZE && os->sa) {
			// TODO: This leaks the old map
			os->id_map = hash_new(os->sa, os->id_based_cnt, (fkeyvalue)&os_id_key);
			if (os->id_map == NULL) {
				MT_lock_unset(&os->ht_lock);
				return NULL;
			}

			for (object_node *n = os->id_based_h; n; n = n->next ) {
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
		for (object_node *n = os->id_based_h; n; n = n->next) {
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
	if (!os->sa)
		_DELETE(n);
}

static object_node *
os_remove_name_based_chain(objectset *os, object_node *n)
{
	assert(n);
	object_node *p = os->name_based_h;

	if (p != n)
		while (p && p->next != n)
			p = p->next;
	assert(p==n||(p && p->next == n));
	if (p == n) {
		os->name_based_h = n->next;
		if (os->name_based_h) // i.e. non-empty os
			os->name_based_h->prev = NULL;
		p = NULL;
	} else if ( p != NULL)  {
		p->next = n->next;
		if (p->next) // node in the middle
			p->next->prev = p;
	}
	if (n == os->name_based_t)
		os->name_based_t = p;

	MT_lock_set(&os->ht_lock);
	if (os->name_map && n)
		hash_delete(os->name_map, n);
	MT_lock_unset(&os->ht_lock);

	node_destroy(os, n);
	return p;
}

static object_node *
os_remove_id_based_chain(objectset *os, object_node *n)
{
	assert(n);
	object_node *p = os->id_based_h;

	if (p != n)
		while (p && p->next != n)
			p = p->next;
	assert(p==n||(p && p->next == n));
	if (p == n) {
		os->id_based_h = n->next;
		if (os->id_based_h) // i.e. non-empty os
			os->id_based_h->prev = NULL;
		p = NULL;
	} else if ( p != NULL)  {
		p->next = n->next;
		if (p->next) // node in the middle
			p->next->prev = p;
	}
	if (n == os->id_based_t)
		os->id_based_t = p;

	MT_lock_set(&os->ht_lock);
	if (os->id_map && n)
		hash_delete(os->id_map, n);
	MT_lock_unset(&os->ht_lock);

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
	return n;
}

static objectset *
os_append_node_name(objectset *os, object_node *n)
{
	if (os->name_based_t) {
		os->name_based_t->next = n;
	} else {
		os->name_based_h = n;
	}
	n->os = os;
	n->prev = os->name_based_t; // aka the double linked list.
	os->name_based_t = n;
	if (n->data) {
		MT_lock_set(&os->ht_lock);
		if (os->name_map) {
			int key = os->name_map->key(n);

			if (hash_add(os->name_map, key, n) == NULL) {
				MT_lock_unset(&os->ht_lock);
				return NULL;
			}
		}
		MT_lock_unset(&os->ht_lock);
	}
	os->name_based_cnt++;
	return os;
}

static objectset *
os_append_name(objectset *os, objectversion *ov)
{
	object_node *n = node_create(os->sa, ov);

	if (n == NULL)
		return NULL;

	ov->name_based_chain = n;
	return os_append_node_name(os, n);
}

static objectset *
os_append_node_id(objectset *os, object_node *n)
{
	if (os->id_based_t) {
		os->id_based_t->next = n;
	} else {
		os->id_based_h = n;
	}
	n->os = os;
	n->prev = os->id_based_t; // aka the double linked list.
	os->id_based_t = n;
	if (n->data) {
		MT_lock_set(&os->ht_lock);
		if (os->id_map) {
			int key = os->id_map->key(n);

			if (hash_add(os->id_map, key, n) == NULL) {
				MT_lock_unset(&os->ht_lock);
				return NULL;
			}
		}
		MT_lock_unset(&os->ht_lock);
	}
	os->id_based_cnt++;
	return os;
}

static objectset *
os_append_id(objectset *os, objectversion *ov)
{
	object_node *n = node_create(os->sa, ov);

	if (n == NULL)
		return NULL;
	ov->id_based_chain = n;
	return os_append_node_id(os, n);
}

static object_node* find_name(objectset *os, const char *name);

static void
objectversion_destroy(sqlstore *store, objectversion *ov, ulng commit_ts, ulng oldest)
{
	// TODO: clean this up once the add and del functions are working
	// TODO: handle name_based_cnt s and id_based_cnt s
	objectversion *name_based_older = ov->name_based_older;
	objectversion *name_based_newer = ov->name_based_newer;

	if (name_based_older && commit_ts) {
		objectversion_destroy(store, name_based_older, commit_ts, oldest);
		name_based_older = NULL;
	} else if (name_based_older) {
		name_based_older->name_based_newer = name_based_newer;
	}
	ov->name_based_older = NULL;

	if (name_based_newer && commit_ts)
		name_based_newer->name_based_older = NULL;
	else if (name_based_newer && name_based_older)
		name_based_newer->name_based_older = name_based_older;

	objectset* os = ov->name_based_chain->os;
	if (!name_based_newer) {
		object_node *on = NULL;
		if (os->unique)
			on = find_name(os, ov->obj->name);
		else
			on = find_id(os, ov->obj->id);
		assert(on->data == ov);
		if (on)
			os_remove_name_based_chain(os, on);
		if (name_based_older)
			os_append_name(os, name_based_older);
	}
	if (ov && os && os->destroy)
		os->destroy(store, ov->obj);
	/* free ov */
}

static int rollback_objectversion(sql_store Store, objectversion *ov)
{
		objectset* os = ov->name_based_chain->os;
		assert(ov->ts > TRANSACTION_ID_BASE);

		if (ov->name_based_older && ov->name_based_older->ts < TRANSACTION_ID_BASE) {
			// ov has a committed parent.
			assert(!ov->id_based_older || ov->name_based_older == ov->id_based_older);

			// TODO: ATOMIC OP
			ov->name_based_older->name_based_older = NULL;
		}

		if (ov->name_based_older == NULL) {
			os_remove_name_based_chain(os, ov->name_based_chain);
		}

		if (ov->id_based_older == NULL) {
			os_remove_id_based_chain(os, ov->name_based_chain);
		}

		os->destroy(Store, ov->obj);
		// destroy objectversion ov.

		return LOG_OK;
}

static int
tc_gc_objectversion(sql_store Store, sql_change *change, ulng commit_ts, ulng oldest)
{
	objectversion *ov = (objectversion*)change->data;

	if (!commit_ts) {
		rollback_objectversion(Store, ov);
	}

	if (ov->deleted) {
		/* TODO handle savepoints */
		if (ov->ts < oldest || (ov->ts == commit_ts && commit_ts == oldest)) {
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

	os->graveyard = SA_ZNEW(sa, object_node);

	return os;
}

objectset *
os_dup(objectset *os)
{
	os->refcnt++;
	return os;
}

// TODO: Look into cohesion between os_destroy, os->destroy and node_destroy
void
os_destroy(objectset *os, sql_store store)
{
	if (--os->refcnt > 0)
		return;
	if (os->destroy) {
		for(object_node *n=os->name_based_h; n; n=n->next) {
			os->destroy(n->data, store);
		}
	}
	object_node *n = os->name_based_h;

	MT_lock_destroy(&os->ht_lock);
	os->name_based_h = NULL;
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

	node_destroy(os, os->graveyard);

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
	if ((!os->name_map || os->name_map->size*16 < os->name_based_cnt) && os->sa) {
		os->name_map = hash_new(os->sa, os->name_based_cnt, (fkeyvalue)&os_name_key);
		if (os->name_map == NULL) {
			MT_lock_unset(&os->ht_lock);
			return NULL;
		}

		for (object_node *n = os->name_based_h; n; n = n->next ) {
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
		if ((!os->name_map || os->name_map->size*16 < os->name_based_cnt) && os->name_based_cnt > HASH_MIN_SIZE && os->sa) {
			// TODO: This leaks the old map
			os->name_map = hash_new(os->sa, os->name_based_cnt, (fkeyvalue)&os_name_key);
			if (os->name_map == NULL) {
				MT_lock_unset(&os->ht_lock);
				return NULL;
			}

			for (object_node *n = os->name_based_h; n; n = n->next ) {
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
		for (object_node *n = os->name_based_h; n; n = n->next) {
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
get_valid_object_name(sql_trans *tr, objectversion *ov)
{
	while(ov) {
		if (ov->ts == tr->tid || (tr->parent && tr_version_of_parent(tr, ov->ts)) || ov->ts < tr->ts)
			return ov;
		else
			ov = ov->name_based_older;
	}
	return ov;
}

static objectversion*
get_valid_object_id(sql_trans *tr, objectversion *ov)
{
	while(ov) {
		if (ov->ts == tr->tid || ov->ts < tr->ts)
			return ov;
		else
			ov = ov->id_based_older;
	}
	return ov;
}

#if 0
static void
os_update_hash(objectset *os, object_node *n, /*new*/ objectversion *ov)
{
	MT_lock_set(&os->ht_lock);
	if (os->name_map) {
		hash_delete(os->name_map, n);
		int nkey = os->name_map->key(ov);
		hash_add(os->name_map, nkey, ov);
	}

	if (os->id_map) {
		hash_delete(os->id_map, n);
		int nkey = os->id_map->key(ov);
		hash_add(os->id_map, nkey, ov);
	}

	n->data = ov;

	MT_lock_unset(&os->ht_lock);
}
#endif

static int
os_add_name_based(objectset *os, struct sql_trans *tr, const char *name, objectversion *ov) {
	object_node *name_based_node = NULL;
	if (ov->id_based_older && strcmp(ov->id_based_older->obj->name, name) == 0)
		name_based_node = ov->id_based_older->name_based_chain;
	else if (os->unique) // Previous name based objectversion is of a different id, so now we do have to perform an extensive look up
		name_based_node = find_name(os, name);
	// else names are not unique and each id based version chain maps to its own name based version chain.

	if (name_based_node) {
		objectversion *co = name_based_node->data;
		objectversion *oo = get_valid_object_name(tr, co);
		if (co != oo) { /* conflict ? */
			return -1;
		}

		assert(ov != oo); // Time loops are not allowed

		MT_lock_set(&os->ht_lock);
		ov->name_based_chain = oo->name_based_chain;
		ov->name_based_older = oo;

		// TODO: double check/refine locking rationale
		if (oo)
			oo->name_based_newer = ov;
		name_based_node->data = ov;
		MT_lock_unset(&os->ht_lock);
		return 0;
	} else { /* new */
		// TODO: can fail i.e. returns NULL
		os_append_name(os, ov);
		return 0;
	}
}

static int
os_add_id_based(objectset *os, struct sql_trans *tr, sqlid id, objectversion *ov) {
	object_node *id_based_node;
	if (ov->name_based_older && ov->name_based_older->obj->id == id)
		id_based_node = ov->name_based_older->id_based_chain;
	else // Previous id based objectversion is of a different name, so now we do have to perform an extensive look up
		id_based_node = find_id(os, id);

	if (id_based_node) {
		objectversion *co = id_based_node->data;
		objectversion *oo = get_valid_object_id(tr, co);
		if (co != oo) { /* conflict ? */
			return -1;
		}

		assert(ov != oo); // Time loops are not allowed

		MT_lock_set(&os->ht_lock);
		ov->id_based_chain = oo->id_based_chain;
		ov->id_based_older = oo;

		// TODO: double check/refine locking rationale
		if (oo)
			oo->id_based_newer = ov;
		id_based_node->data = ov;
		MT_lock_unset(&os->ht_lock);
		return 0;
	} else { /* new */

		// TODO: can fail i.e. returns NULL
		os_append_id(os, ov);
		
		return 0;
	}
}

int /*ok, error (name existed) and conflict (added before) */
os_add(objectset *os, struct sql_trans *tr, const char *name, sql_base *b)
{
	objectversion *ov = SA_ZNEW(os->sa, objectversion);
	ov->ts = tr->tid;
	ov->obj = b;

	if (os_add_id_based(os, tr, b->id, ov)) {
		// TODO clean up ov
		return -1;
	}

	if (os_add_name_based(os, tr, name, ov)) {
		// TODO clean up ov
		return -1;
	}

	if (!os->temporary)
		trans_add(tr, b, ov, &tc_gc_objectversion, &tc_commit_objectversion, NULL);
	return 0;
}

static int
os_del_name_based(objectset *os, struct sql_trans *tr, const char *name, objectversion *ov) {
	object_node *name_based_node;
	if (ov->id_based_older && strcmp(ov->id_based_older->obj->name, name) == 0)
		name_based_node = ov->id_based_older->name_based_chain;
	else if (os->unique) // Previous name based objectversion is of a different id, so now we do have to perform an extensive look up
		name_based_node = find_name(os, name);

	if (name_based_node) {
		objectversion *co = name_based_node->data;
		objectversion *oo = get_valid_object_name(tr, co);
		ov->name_based_chain = oo->name_based_chain;
		if (co != oo) { /* conflict ? */
			return -1;
		}
		ov->name_based_older = oo;

		MT_lock_set(&os->ht_lock);
		// TODO: double check/refine locking rationale
		if (oo)
			oo->name_based_newer = ov;
		name_based_node->data = ov;
		MT_lock_unset(&os->ht_lock);
		return 0;
	} else {
		/* missing */
		return -1;
	}
}

static int
os_del_id_based(objectset *os, struct sql_trans *tr, sqlid id, objectversion *ov) {
	object_node *id_based_node;
	if (ov->name_based_older && ov->name_based_older->obj->id == id)
		id_based_node = ov->name_based_older->id_based_chain;
	else // Previous id based objectversion is of a different name, so now we do have to perform an extensive look up
		id_based_node = find_id(os, id);

	if (id_based_node) {
		objectversion *co = id_based_node->data;
		objectversion *oo = get_valid_object_id(tr, co);
		ov->id_based_chain = oo->id_based_chain;
		if (co != oo) { /* conflict ? */
			return -1;
		}
		ov->id_based_older = oo;

		MT_lock_set(&os->ht_lock);
		// TODO: double check/refine locking rationale
		if (oo)
			oo->id_based_newer = ov;
		id_based_node->data = ov;
		MT_lock_unset(&os->ht_lock);
		return 0;
	} else {
		/* missing */
		return -1;
	}
}

int
os_del(objectset *os, struct sql_trans *tr, const char *name, sql_base *b)
{
	objectversion *ov = SA_ZNEW(os->sa, objectversion);
	ov->deleted = 1;
	ov->ts = tr->tid;
	ov->obj = b;

	if (os_del_id_based(os, tr, b->id, ov)) {
		// TODO clean up ov
		return -1;
	}

	if (os_del_name_based(os, tr, name, ov)) {
		// TODO clean up ov
		return -1;
	}

	if (!os->temporary)
		trans_add(tr, b, ov, &tc_gc_objectversion, &tc_commit_objectversion, NULL);
	return 0;
}

int
os_size(objectset *os, struct sql_trans *tr)
{
	int cnt = 0;
	if (os) {
		for(object_node *n = os->name_based_h; n; n=n->next) {
			objectversion *ov = n->data;
			if ((ov=get_valid_object_name(tr, ov)) && !ov->deleted)
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
		 objectversion *ov = get_valid_object_name(tr, n->data);
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
		 objectversion *ov = get_valid_object_id(tr, n->data);
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
	if (os->name_based_h && name) {
		if (!os->name_map)
			os->name_map = os_hash_create(os);
		oi->e = find_hash_entry(os->name_map, name);
	} else
		oi->n =	os->name_based_h;
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

				ov = get_valid_object_name(oi->tr, ov);
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

			ov = get_valid_object_id(oi->tr, ov);
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
		 objectversion *ov = get_valid_object_id(tr, n->data);
		 if (ov && !ov->deleted && ov->ts == tr->tid)
			 return true;
	}
	return false;
}
