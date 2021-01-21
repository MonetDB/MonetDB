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

#define id_based_rollbacked		(1)
#define name_based_rollbacked	(1<<1)
#define under_destruction		(1<<2)
#define under_construction		(1<<3)

typedef struct objectversion {
	bool deleted;
	ulng ts;
	bte rollbacked;
	bte life_cycle;
	sql_base *obj;
	struct objectset* os;
	struct objectversion	*name_based_older;
	struct objectversion	*name_based_newer; // TODO: must become atomic pointer
	struct object_node		*name_based_chain;

	struct objectversion	*id_based_older;
	struct objectversion	*id_based_newer;
	struct object_node		*id_based_chain;
} objectversion;

typedef struct object_node {
    struct object_node* prev;
    struct object_node* next;
    objectversion* data;
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
node_destroy(objectset *os, sqlstore *store, object_node *n)
{
	if (!os->sa)
		_DELETE(n);
	(void)store; /* todo destroy obj */
}

static object_node *
os_remove_name_based_chain(objectset *os, sqlstore *store, object_node *n)
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

	node_destroy(os, store, n);
	return p;
}

static object_node *
os_remove_id_based_chain(objectset *os, sqlstore *store, object_node *n)
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

	node_destroy(os, store, n);
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
objectversion_destroy(sqlstore *store, objectset* os, objectversion *ov)
{
	if (os->destroy)
		os->destroy(store, ov->obj);

	_DELETE(ov);
}

static void os_rollback_id_based_terminal_decendant(objectversion *ov, sqlstore *store);
static void os_rollback_name_based_terminal_decendant(objectversion *ov, sqlstore *store);

static void
os_rollback_os_id_based_cascading(objectversion *ov, sqlstore *store) {
	assert(ov->rollbacked & id_based_rollbacked);

	if (ov->id_based_older) {
		if (ov->id_based_older->ts < TRANSACTION_ID_BASE) {
			// older is last committed state. Restore object_node pointer to that.
			// TODO START ATOMIC()
			ov->id_based_chain->data = ov->id_based_older;
			// END ATOMIC()
		}
		else {
			os_rollback_name_based_terminal_decendant(ov->name_based_chain->data, store);

			// id based cascaded rollback along the parents
			ov->id_based_older->rollbacked |= id_based_rollbacked;
			os_rollback_os_id_based_cascading(ov->id_based_older, store);
		}
	}
	else {
		// this is a terminal node. i.e. this objectversion does not have id based committed history
		os_remove_id_based_chain(ov->os, store, ov->id_based_chain);
	}
}

static void
os_rollback_os_name_based_cascading(objectversion *ov, sqlstore *store) {
	assert(ov->rollbacked & name_based_rollbacked);

	if (ov->name_based_older) {
		if (ov->name_based_older->ts < TRANSACTION_ID_BASE) {
			// older is last committed state. Restore object_node pointer to that.
			// TODO START ATOMIC()
			ov->name_based_chain->data = ov->name_based_older;
			// END ATOMIC()
		}
		else {
			os_rollback_id_based_terminal_decendant(ov->id_based_chain->data, store);

			// name based cascaded rollback along the parents
			ov->name_based_older->rollbacked |= name_based_rollbacked;
			os_rollback_os_name_based_cascading(ov->name_based_older, store);
		}
	}
	else {
		// this is a terminal node. i.e. this objectversion does not have name based committed history
		os_remove_name_based_chain(ov->os, store, ov->name_based_chain);
	}
}

static void
os_rollback_name_based_terminal_decendant(objectversion *ov, sqlstore *store) {
	if (ov->rollbacked & name_based_rollbacked) {
		return;
	}

	ov->rollbacked |= name_based_rollbacked;

	os_rollback_id_based_terminal_decendant(ov->id_based_chain->data, store);
	os_rollback_os_name_based_cascading(ov, store);

}

static void
os_rollback_id_based_terminal_decendant(objectversion *ov, sqlstore *store) {
	if (ov->rollbacked & id_based_rollbacked) {
		return;
	}

	ov->rollbacked |= id_based_rollbacked;

	os_rollback_name_based_terminal_decendant(ov->name_based_chain->data, store);
	os_rollback_os_id_based_cascading(ov, store);
}

static int
os_rollback(objectversion *ov, sqlstore *store)
{
	os_rollback_name_based_terminal_decendant(ov->name_based_chain->data, store);

	// TODO: label objectversion with a latest timestamp of tid.

	return LOG_OK;
}

static void
put_under_destruction(sqlstore* store, objectversion *ov, ulng oldest)
{
	//TODO ATOMIC CAS
	if (ov->life_cycle == 0) {
		ov->life_cycle = under_destruction;

		if (!ov->name_based_newer) {
			os_remove_name_based_chain(ov->os, store, ov->name_based_chain);
		}
		else {
			ov->name_based_newer->name_based_older = NULL;
		}

		if (!ov->id_based_newer) {
			os_remove_id_based_chain(ov->os, store, ov->id_based_chain);
		}
		else {
			ov->id_based_newer->id_based_older = NULL;
		}

		// TODO ATOMIC GET
		ov->ts = store->timestamp+1;
		// END ATOMIC GET

		if (ov->id_based_older) {
			put_under_destruction(store, ov->id_based_older, oldest);
		}

		if (ov->name_based_older) {
			put_under_destruction(store, ov->name_based_older, oldest);
		}
	}
	//END ATOMIC CAS
}

static int
os_cleanup(sqlstore* store, objectversion *ov, ulng oldest) {

	// TODO ATOMIC GET life_cycle
	if (ov->life_cycle == under_destruction) {
	 	if (ov->ts < oldest) {
			// This one is ready to be freed
			objectversion_destroy(store, ov->os, ov);
			return LOG_ERR;
		}

		// not yet old enough to be safely removed.
		return LOG_OK;
	}

	if (ov->deleted) {
		if (ov->ts < oldest) {
			// the oldest relevant state is deleted so lets try to mark it as destroyed
			put_under_destruction(store, ov, oldest);
		}

		// reinsert it into the queue, either because it is now marked for destruction or
		// we want to retry marking it for destruction later.
		return LOG_OK;
	}

	// TODO ATOMIC GET
	objectversion* newer = ov->name_based_newer;
	// END ATOMIC GET

	if (ov->ts < oldest && newer && (newer->ts < oldest && !newer->rollbacked)) {
		assert(newer == ov->id_based_newer);
		// ov has a committed name based parent

		put_under_destruction(store, ov, oldest);

		// Since this objectversion has two committed oldest parents it is unreachable.
		// So we can directly destroy it.
		objectversion_destroy(store, ov->os, ov);
		return LOG_ERR;
	}

	return LOG_OK;
}

static int
tc_gc_objectversion(sql_store store, sql_change *change, ulng commit_ts, ulng oldest)
{
	(void) store;
	if (commit_ts != oldest) {
		// TODO: for now only oldest is allowed to do clean up
		return LOG_ERR;
	}

	objectversion *ov = (objectversion*)change->data;

	return os_cleanup( (sqlstore*) store, ov, oldest);
}

static int
tc_commit_objectversion(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	objectversion *ov = (objectversion*)change->data;
	if (commit_ts) {
		assert(ov->ts == tr->tid);
		ov->ts = commit_ts;
		(void)oldest;
	}
	else {
		os_rollback(ov, tr->store);
	}

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
			node_destroy(os, store, t);
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

// disabled because we need functions a in order (need some insert - order preserving hash) ..
//#define USE_HASH
#ifdef USE_HASH
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
#endif

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
		if (ov->ts == tr->tid || (tr->parent && tr_version_of_parent(tr, ov->ts)) || (!ov->rollbacked && ov->ts < tr->ts))
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
		if (ov->ts == tr->tid || (tr->parent && tr_version_of_parent(tr, ov->ts))  || (!ov->rollbacked && ov->ts < tr->ts))
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

		if (oo->deleted) {
			// Since our parent oo is comitted deleted objectversion, we might have a conflict with
			// another transaction that tries to clean up oo.
			//TODO ATOMIC CAS
			if (oo->life_cycle == 0) {
				oo->life_cycle = under_construction;
			}
			else {
				return -1; /*conflict with cleaner*/
			}
			//END ATOMIC
		}

		MT_lock_set(&os->ht_lock);
		ov->name_based_chain = oo->name_based_chain;
		ov->name_based_older = oo;

		// TODO: double check/refine locking rationale
		name_based_node->data = ov;
		if (oo) {
			oo->name_based_newer = ov;
			 // TODO ATOMIC SET
			oo->life_cycle = 0;
			// END ATOMIC
		}
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

		if (oo->deleted) {
			// Since our parent oo is comitted deleted objectversion, we might have a conflict with
			// another transaction that tries to clean up oo.
			//TODO ATOMIC CAS
			if (oo->life_cycle == 0) {
				oo->life_cycle = under_construction;
			}
			else {
				return -1; /*conflict with cleaner*/
			}
			//END ATOMIC
		}

		MT_lock_set(&os->ht_lock);
		ov->id_based_chain = oo->id_based_chain;
		ov->id_based_older = oo;

		id_based_node->data = ov;
		if (oo) {
			oo->id_based_newer = ov;
			 // TODO ATOMIC SET
			oo->life_cycle = 0;
			// END ATOMIC
		}
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
	ov->os = os;

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
		if (oo) {
			oo->name_based_newer = ov;
			assert(!oo->deleted);
		}
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
		if (oo) {
			oo->id_based_newer = ov;
			assert(!oo->deleted);
		}
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
	ov->os = os;

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
	(void) os;
	(void) tr;
	(void) name;
	// TODO remove entire object_node corresponding to this name.

	// TODO assert os->unique?s
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
#ifdef USE_HASH
	if (os->name_based_h && name) {
		if (!os->name_map)
			os->name_map = os_hash_create(os);
		oi->e = find_hash_entry(os->name_map, name);
	} else
#endif
		oi->n =	os->name_based_h;
}

sql_base *
oi_next(struct os_iter *oi)
{
	sql_base *b = NULL;

#ifdef USE_HASH
	if (oi->name) {
		sql_hash_e *e = oi->e;

		while (e && !b) {
			object_node *n = e->value;

			if (n && n->data->obj->name && strcmp(n->data->obj->name, oi->name) == 0) {
				objectversion *ov = n->data;
				e = oi->e = e->chain;

				ov = get_valid_object_name(oi->tr, ov);
				if (ov && !ov->deleted)
					b = ov->obj;
			} else {
				e = e->chain;
			}
	 	}
#else
	if (oi->name) {
		object_node *n = oi->n;

		while (n && !b) {

			if (n->data->obj->name && strcmp(n->data->obj->name, oi->name) == 0) {
				objectversion *ov = n->data;

				n = oi->n = n->next;
				ov = get_valid_object_name(oi->tr, ov);
				if (ov && !ov->deleted)
					b = ov->obj;
			} else {
				n = oi->n = n->next;
			}
	 	}
#endif
	} else {
		object_node *n = oi->n;

		while (n && !b) {
			objectversion *ov = n->data;
			n = oi->n = n->next;

			ov = get_valid_object_id(oi->tr, ov);
			if (ov && !ov->deleted)
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
