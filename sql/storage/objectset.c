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

struct versionhead ;

#define active							(0)
#define under_destruction				(1<<1)
#define block_destruction				(1<<2)
#define deleted							(1<<3)
#define rollbacked						(1<<4)

/* This objectversion owns its associated versionhead.
 * When this objectversion gets destroyed,
 * the cleanup procedure should also destroy the associated (name|id) based versionhead.*/
#define name_based_versionhead_owner	(1<<5)
#define id_based_versionhead_owner		(1<<6)

typedef struct objectversion {
	ulng ts;
	ATOMIC_TYPE state;
	sql_base *b; // base of underlying sql object
	struct objectset* os;
	struct objectversion	*name_based_older;
	struct objectversion	*name_based_newer;
	struct versionhead 		*name_based_head;

	struct objectversion	*id_based_older;
	struct objectversion	*id_based_newer;
	struct versionhead		*id_based_head;
} objectversion;

typedef struct versionhead  {
    struct versionhead * prev;
    struct versionhead * next;
    objectversion* ov;
} versionhead ;

typedef struct objectset {
	int refcnt;
	sql_allocator *sa;
	destroy_fptr destroy;
	MT_RWLock rw_lock;	/*readers-writer lock to protect the links (chains) in the objectversion chain.*/
	versionhead  *name_based_h;
	versionhead  *name_based_t;
	versionhead  *id_based_h;
	versionhead  *id_based_t;
	int name_based_cnt;
	int id_based_cnt;
	struct sql_hash *name_map;
	struct sql_hash *id_map;
	bool temporary;
	bool unique;	/* names are unique */
} objectset;

static int
os_id_key(versionhead  *n)
{
	return (int) BATatoms[TYPE_int].atomHash(&n->ov->b->id);
}

static inline void
lock_reader(objectset* os)
{
	MT_rwlock_rdlock(&os->rw_lock);
}

static inline void
unlock_reader(objectset* os)
{
	MT_rwlock_rdunlock(&os->rw_lock);
}

static inline void
lock_writer(objectset* os)
{
	MT_rwlock_wrlock(&os->rw_lock);
}

static inline void
unlock_writer(objectset* os)
{
	MT_rwlock_wrunlock(&os->rw_lock);
}

static bte os_atmc_get_state(objectversion *ov) {
	bte state = (bte) ATOMIC_GET(&ov->state);
	return state;
}

static void os_atmc_set_state(objectversion *ov, bte state) {
	ATOMIC_SET(&ov->state, state);
}

static versionhead  *
find_id(objectset *os, sqlid id)
{
	if (os) {
		lock_reader(os);
		if (os->id_map) {
			int key = (int) BATatoms[TYPE_int].atomHash(&id);
			sql_hash_e *he = os->id_map->buckets[key&(os->id_map->size-1)];

			for (; he; he = he->chain) {
				versionhead  *n = he->value;

				if (n && n->ov->b->id == id) {
					unlock_reader(os);
					return n;
				}
			}
			unlock_reader(os);
			return NULL;
		}

		for (versionhead  *n = os->id_based_h; n; n = n->next) {
			objectversion *ov = n->ov;

			/* check if ids match */
			if (id == ov->b->id) {
				unlock_reader(os);
				return n;
			}
		}
	}

	unlock_reader(os);
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
		if (!h->sa)
			_DELETE(p);
	}
}

static void
node_destroy(objectset *os, sqlstore *store, versionhead  *n)
{
	if (!os->sa)
		_DELETE(n);
	(void)store;
}

static versionhead  *
os_remove_name_based_chain(objectset *os, objectversion* ov)
{
	lock_writer(os);
	versionhead  *n = ov->name_based_head;
	versionhead  *p = os->name_based_h;
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

	if (os->name_map && n)
		hash_delete(os->name_map, n);

	os->name_based_cnt--;
	unlock_writer(os);

	bte state = os_atmc_get_state(ov);
	state |= name_based_versionhead_owner;
	os_atmc_set_state(ov, state);
	return p;
}

static versionhead  *
os_remove_id_based_chain(objectset *os, objectversion* ov)
{
	lock_writer(os);
	versionhead  *n = ov->id_based_head;
	versionhead  *p = os->id_based_h;

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

	if (os->id_map && n)
		hash_delete(os->id_map, n);

	os->name_based_cnt--;
	unlock_writer(os);

	bte state = os_atmc_get_state(ov);
	state |= id_based_versionhead_owner;
	os_atmc_set_state(ov, state);
	return p;
}

static versionhead  *
node_create(sql_allocator *sa, objectversion *ov)
{
	versionhead  *n = SA_NEW(sa, versionhead );

	if (n == NULL)
		return NULL;
	*n = (versionhead ) {
		.ov = ov,
	};
	return n;
}

static inline int
os_name_key(versionhead  *n)
{
	return hash_key(n->ov->b->name);
}

static objectset *
os_append_node_name(objectset *os, versionhead  *n)
{
	lock_writer(os);
	if ((!os->name_map || os->name_map->size*16 < os->name_based_cnt) && os->name_based_cnt > HASH_MIN_SIZE) {
		hash_destroy(os->name_map);
		os->name_map = hash_new(os->sa, os->name_based_cnt, (fkeyvalue)& os_name_key);
		if (os->name_map == NULL) {
			unlock_writer(os);
			return NULL;
		}

		for (versionhead  *n = os->name_based_h; n; n = n->next ) {
			int key = os_name_key(n);

			if (hash_add(os->name_map, key, n) == NULL) {
				unlock_writer(os);
				return NULL;
			}
		}
	}

	if (os->name_map) {
		int key = os->name_map->key(n);

		if (hash_add(os->name_map, key, n) == NULL) {
			unlock_writer(os);
			return NULL;
		}
	}

	if (os->name_based_t) {
		os->name_based_t->next = n;
	} else {
		os->name_based_h = n;
	}
	n->prev = os->name_based_t; // aka the double linked list.
	os->name_based_t = n;
	os->name_based_cnt++;
	unlock_writer(os);
	return os;
}

static objectset *
os_append_name(objectset *os, objectversion *ov)
{
	versionhead  *n = node_create(os->sa, ov);

	if (n == NULL)
		return NULL;

	ov->name_based_head = n;
	if (!(os = os_append_node_name(os, n))){
		_DELETE(n);
		return NULL;
	}

	return os;
}

static objectset *
os_append_node_id(objectset *os, versionhead  *n)
{
	lock_writer(os);
	if ((!os->id_map || (os->id_map->size*16 < os->id_based_cnt && os->id_based_cnt > HASH_MIN_SIZE)) && os->sa) {
		hash_destroy(os->id_map);
		os->id_map = hash_new(os->sa, os->id_based_cnt, (fkeyvalue)&os_id_key);
		if (os->id_map == NULL) {
			unlock_writer(os);
			return NULL;
		}
		for (versionhead  *n = os->id_based_h; n; n = n->next ) {
			int key = os_id_key(n);

			if (hash_add(os->id_map, key, n) == NULL) {
				unlock_writer(os);
				return NULL;
			}
		}
	}

	if (os->id_map) {
		int key = os->id_map->key(n);
		if (hash_add(os->id_map, key, n) == NULL) {
			unlock_writer(os);
			return NULL;
		}
	}

	if (os->id_based_t) {
		os->id_based_t->next = n;
	} else {
		os->id_based_h = n;
	}
	n->prev = os->id_based_t; // aka the double linked list.
	os->id_based_t = n;
	os->id_based_cnt++;
	unlock_writer(os);
	return os;
}

static objectset *
os_append_id(objectset *os, objectversion *ov)
{
	versionhead  *n = node_create(os->sa, ov);

	if (n == NULL)
		return NULL;
	ov->id_based_head = n;
	if (!(os = os_append_node_id(os, n))){
		_DELETE(n);
		return NULL;
	}

	return os;
}

static versionhead * find_name(objectset *os, const char *name);

static void
objectversion_destroy(sqlstore *store, objectset* os, objectversion *ov)
{

	bte state = os_atmc_get_state(ov);

	if (state & name_based_versionhead_owner) {
		node_destroy(ov->os, store, ov->name_based_head);
	}

	if (state & id_based_versionhead_owner) {
		node_destroy(ov->os, store, ov->id_based_head);
	}

	if (os->destroy)
		os->destroy(store, ov->b);

	_DELETE(ov);
}

static void
_os_rollback(objectversion *ov, sqlstore *store)
{
	assert(ov->ts >= TRANSACTION_ID_BASE);

	bte state = os_atmc_get_state(ov);
	if (state & rollbacked) {
		return;
	}

	state |= rollbacked;
	os_atmc_set_state(ov, state);

	bte state_older;

 	/*
	 * We have to use the readers-writer lock here,
	 * since the pointer containing the adress of the older objectversion might be concurrently overwritten if the older itself hass just been put in the under_destruction state .
	 */
	lock_reader(ov->os);
	objectversion* name_based_older = ov->name_based_older;
	unlock_reader(ov->os);

	if (name_based_older && !((state_older= os_atmc_get_state(name_based_older)) & rollbacked)) {
		if (ov->ts != name_based_older->ts) {
			// older is last committed state or belongs to parent transaction.
			// In any case, we restore versionhead pointer to that.

			ATOMIC_BASE_TYPE expected_deleted = deleted;
			if (state_older == active || (state_older == deleted && ATOMIC_CAS(&name_based_older->state, &expected_deleted, block_destruction))) {
				ov->name_based_head->ov = name_based_older;
				name_based_older->name_based_newer=NULL;
				if (state_older != active && expected_deleted == deleted)
					os_atmc_set_state(name_based_older, deleted); //Restore the deleted older back to its deleted state.
			}
		}
		else {
			_os_rollback(name_based_older, store);
		}
	}
	else if (!name_based_older) {
		// this is a terminal node. i.e. this objectversion does not have name based committed history
		if (ov->name_based_head) // The oposite can happen during an early conflict in os_add or os_del.
			os_remove_name_based_chain(ov->os, ov);
	}

 	/*
	 * We have to use the readers-writer lock here,
	 * since the pointer containing the adress of the older objectversion might be concurrently overwritten if the older itself hass just been put in the under_destruction state .
	 */
	lock_reader(ov->os);
	objectversion* id_based_older = ov->id_based_older;
	unlock_reader(ov->os);
	if (id_based_older && !((state_older= os_atmc_get_state(id_based_older)) & rollbacked)) {
		if (ov->ts != id_based_older->ts) {
			// older is last committed state or belongs to parent transaction.
			// In any case, we restore versionhead pointer to that.

			ATOMIC_BASE_TYPE expected_deleted = deleted;
			if (state_older == active || (state_older == deleted && ATOMIC_CAS(&id_based_older->state, &expected_deleted, block_destruction))) {
				ov->id_based_head->ov = id_based_older;
				id_based_older->id_based_newer=NULL;
				if (state_older != active && expected_deleted == deleted)
					os_atmc_set_state(id_based_older, deleted); //Restore the deleted older back to its deleted state.
			}
		}
		else if (id_based_older != name_based_older)
			_os_rollback(id_based_older, store);
	}
	else if (!id_based_older) {
		// this is a terminal node. i.e. this objectversion does not have id based committed history
		os_remove_id_based_chain(ov->os, ov);
	}

	if (ov->name_based_newer && !(os_atmc_get_state(ov->name_based_newer) & rollbacked)) {
		_os_rollback(ov->name_based_newer, store);
	}

	if (ov->id_based_newer && ov->id_based_newer != ov->name_based_newer && !(os_atmc_get_state(ov->id_based_newer) & rollbacked)) {
		_os_rollback(ov->id_based_newer, store);
	}
}

static int
os_rollback(objectversion *ov, sqlstore *store)
{
	_os_rollback(ov, store);

	return LOG_OK;
}

static inline void
try_to_mark_deleted_for_destruction(sqlstore* store, objectversion *ov)
{
	ATOMIC_BASE_TYPE expected_deleted = deleted;
	if (ATOMIC_CAS(&ov->state, &expected_deleted, under_destruction)) {

		if (!ov->name_based_newer || (os_atmc_get_state(ov->name_based_newer) & rollbacked)) {
			os_remove_name_based_chain(ov->os, ov);
		}
		else {
			lock_writer(ov->os);
			ov->name_based_newer->name_based_older = NULL;
			unlock_writer(ov->os);
		}

		if (!ov->id_based_newer || (os_atmc_get_state(ov->id_based_newer) & rollbacked)) {
			os_remove_id_based_chain(ov->os, ov);
		}
		else {
			lock_writer(ov->os);
			ov->id_based_newer->id_based_older = NULL;
			unlock_writer(ov->os);
		}

		ov->ts = store_get_timestamp(store)+1;
	}
}

static void
objectversion_destroy_recursive(sqlstore* store, objectversion *ov)
{
	if (ov->id_based_older && ov->id_based_older == ov->name_based_older) {
			objectversion_destroy_recursive(store, ov->id_based_older);
		}

		objectversion_destroy(store, ov->os, ov);
}

static int
os_cleanup(sqlstore* store, objectversion *ov, ulng oldest)
{
	if (os_atmc_get_state(ov) & under_destruction) {
	 	if (ov->ts < oldest) {
			// This one is ready to be freed
			objectversion_destroy_recursive(store, ov);
			return LOG_ERR;
		}

		// not yet old enough to be safely removed. Try later.
		return LOG_OK;
	}

	if (os_atmc_get_state(ov) & rollbacked) {
	 	if (ov->ts < oldest) {
			// This one is ready to be freed
			if (ov->name_based_older && ov->name_based_older->name_based_newer == ov)
				ov->name_based_older->name_based_newer=NULL;
			if (ov->id_based_older && ov->id_based_older->id_based_newer == ov)
				ov->id_based_older->id_based_newer=NULL;
			objectversion_destroy(store, ov->os, ov);
			return LOG_ERR;
		}

		if (ov->ts > TRANSACTION_ID_BASE) {
			/* We mark it with the latest possible starttime and reinsert it into the cleanup list.
			 * This will cause a safe eventual destruction of this rollbacked ov.
			 */
			ov->ts = store_get_timestamp(store)+1;
		}

		// not yet old enough to be safely removed. Try later.
		return LOG_OK;
	}

	if (os_atmc_get_state(ov) == deleted) {
		if (ov->ts <= oldest) {
			// the oldest relevant state is deleted so lets try to mark it as destroyed
			try_to_mark_deleted_for_destruction(store, ov);
		}

		// Keep it inplace on the cleanup list, either because it is now marked for destruction or
		// we want to retry marking it for destruction later.
		return LOG_OK;
	}

	while (ov->id_based_older && ov->id_based_older == ov->name_based_older && ov->ts >= oldest) {
		ov = ov->id_based_older;
	}

	if (ov->id_based_older && ov->id_based_older == ov->name_based_older) {
		// Destroy everything older then the oldest possibly relevant objectversion.
		objectversion_destroy_recursive(store, ov->id_based_older);
		ov->id_based_older = NULL;
	}

	return LOG_ERR;
}

static int
tc_gc_objectversion(sql_store store, sql_change *change, ulng commit_ts, ulng oldest)
{
	(void) commit_ts;

	assert(!change->handled);
	objectversion *ov = (objectversion*)change->data;

	int res = os_cleanup( (sqlstore*) store, ov, oldest);
	change->handled = (res)?true:false;
	return res;
}

static int
tc_commit_objectversion(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	objectversion *ov = (objectversion*)change->data;
	if (commit_ts) {
		assert(ov->ts == tr->tid);
		ov->ts = commit_ts;
		change->committed = true;
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
	MT_rwlock_init(&os->rw_lock, "sa_readers_lock");

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
	MT_rwlock_destroy(&os->rw_lock);
	versionhead* n=os->id_based_h;
	while(n) {
		objectversion *ov = n->ov;
		while(ov) {
			objectversion *older = ov->id_based_older;
			objectversion_destroy(store, os, ov);
			ov = older;
		}
		versionhead* hn =n->next;
		node_destroy(os, store, n);
		n = hn;
	}

	n=os->name_based_h;
	while(n) {
		versionhead* hn =n->next;
		node_destroy(os, store, n);
		n = hn;
	}

	if (os->id_map && !os->id_map->sa)
		hash_destroy(os->id_map);

	if (os->name_map && !os->name_map->sa)
		hash_destroy(os->name_map);

	if (!os->sa)
		_DELETE(os);
}

static versionhead  *
find_name(objectset *os, const char *name)
{
	lock_reader(os);
	if (os->name_map) {
		int key = hash_key(name);
		sql_hash_e *he = os->name_map->buckets[key&(os->name_map->size-1)];

		for (; he; he = he->chain) {
			versionhead  *n = he->value;

			if (n && n->ov->b->name && strcmp(n->ov->b->name, name) == 0) {
				unlock_reader(os);
				return n;
			}
		}
		unlock_reader(os);
		return NULL;
	}

	for (versionhead  *n = os->name_based_h; n; n = n->next) {
		objectversion *ov = n->ov;

		/* check if names match */
		if (name[0] == ov->b->name[0] && strcmp(name, ov->b->name) == 0) {
			unlock_reader(os);
			return n;
		}
	}

	unlock_reader(os);
	return NULL;
}

static objectversion*
get_valid_object_name(sql_trans *tr, objectversion *ov)
{
	while(ov) {
		if (ov->ts == tr->tid || (tr->parent && tr_version_of_parent(tr, ov->ts)) || ov->ts < tr->ts)
			return ov;
		else {
			lock_reader(ov->os);
			objectversion* name_based_older = ov->name_based_older;
			unlock_reader(ov->os);
			ov = name_based_older;
		}
	}
	return ov;
}

static objectversion*
get_valid_object_id(sql_trans *tr, objectversion *ov)
{
	while(ov) {
		if (ov->ts == tr->tid || (tr->parent && tr_version_of_parent(tr, ov->ts))  || ov->ts < tr->ts)
			return ov;
		else {
			lock_reader(ov->os);
			objectversion* id_based_older = ov->id_based_older;
			unlock_reader(ov->os);
			ov = id_based_older;
		}
	}
	return ov;
}

static int
os_add_name_based(objectset *os, struct sql_trans *tr, const char *name, objectversion *ov) {
	versionhead  *name_based_node = NULL;
	if (ov->id_based_older && strcmp(ov->id_based_older->b->name, name) == 0)
		name_based_node = ov->id_based_older->name_based_head;
	else if (os->unique) // Previous name based objectversion is of a different id, so now we do have to perform an extensive look up
		name_based_node = find_name(os, name);
	// else names are not unique and each id based version head maps to its own name based version head.

	if (name_based_node) {
		objectversion *co = name_based_node->ov;
		objectversion *oo = get_valid_object_name(tr, co);
		if (co != oo) { /* conflict ? */
			return -1;
		}

		assert(ov != oo); // Time loops are not allowed

		bte state = os_atmc_get_state(oo);
		if (state != active) {
			// This can only happen if the parent oo was a comitted deleted at some point.
			assert(state == deleted || state == under_destruction || state == block_destruction);
			/* Since our parent oo is comitted deleted objectversion, we might have a conflict with
			* another transaction that tries to clean up oo or also wants to add a new objectversion.
			*/
			ATOMIC_BASE_TYPE expected_deleted = deleted;
			if (!ATOMIC_CAS(&oo->state, &expected_deleted, block_destruction)) {
				return -1; /*conflict with cleaner or write-write conflict*/
			}
		}

		/* new object with same name within transaction, should have a delete in between */
		assert(!(state == active && oo->ts == ov->ts && !(os_atmc_get_state(ov) & deleted)));

		lock_writer(os);
		ov->name_based_head = oo->name_based_head;
		ov->name_based_older = oo;

		name_based_node->ov = ov;
		if (oo) {
			oo->name_based_newer = ov;
			// if the parent was originally deleted, we restore it to that state.
			os_atmc_set_state(oo, state);
		}
		unlock_writer(os);
		return 0;
	} else { /* new */
		if (os_append_name(os, ov) == NULL)
			return -1; // MALLOC_FAIL
		return 0;
	}
}

static int
os_add_id_based(objectset *os, struct sql_trans *tr, sqlid id, objectversion *ov) {
	versionhead  *id_based_node;

	id_based_node = find_id(os, id);

	if (id_based_node) {
		objectversion *co = id_based_node->ov;
		objectversion *oo = get_valid_object_id(tr, co);
		if (co != oo) { /* conflict ? */
			return -1;
		}

		assert(ov != oo); // Time loops are not allowed

		bte state = os_atmc_get_state(oo);
		if (state != active) {
			// This can only happen if the parent oo was a comitted deleted at some point.
			assert(state == deleted || state == under_destruction || state == block_destruction);
			/* Since our parent oo is comitted deleted objectversion, we might have a conflict with
			* another transaction that tries to clean up oo or also wants to add a new objectversion.
			*/
			ATOMIC_BASE_TYPE expected_deleted = deleted;
			if (!ATOMIC_CAS(&oo->state, &expected_deleted, block_destruction)) {
				return -1; /*conflict with cleaner or write-write conflict*/
			}
		}

		lock_writer(os);
		ov->id_based_head = oo->id_based_head;
		ov->id_based_older = oo;

		id_based_node->ov = ov;
		if (oo) {
			oo->id_based_newer = ov;
			// if the parent was originally deleted, we restore it to that state.
			os_atmc_set_state(oo, state);
		}
		unlock_writer(os);
		return 0;
	} else { /* new */
		if (os_append_id(os, ov) == NULL)
			return -1; // MALLOC_FAIL

		return 0;
	}
}

static int /*ok, error (name existed) and conflict (added before) */
os_add_(objectset *os, struct sql_trans *tr, const char *name, sql_base *b)
{
	objectversion *ov = SA_ZNEW(os->sa, objectversion);
	ov->ts = tr->tid;
	ov->b = b;
	ov->os = os;

	if (os_add_id_based(os, tr, b->id, ov)) {
		_DELETE(ov);
		return -1;
	}

	if (os_add_name_based(os, tr, name, ov)) {
		trans_add(tr, b, ov, &tc_gc_objectversion, &tc_commit_objectversion, NULL);
		return -1;
	}

	if (!os->temporary)
		trans_add(tr, b, ov, &tc_gc_objectversion, &tc_commit_objectversion, NULL);
	return 0;
}

int
os_add(objectset *os, struct sql_trans *tr, const char *name, sql_base *b)
{
	store_lock(tr->store);
	int res = os_add_(os, tr, name, b);
	store_unlock(tr->store);
	return res;
}

static int
os_del_name_based(objectset *os, struct sql_trans *tr, const char *name, objectversion *ov) {
	versionhead  *name_based_node = NULL;
	if (ov->id_based_older && strcmp(ov->id_based_older->b->name, name) == 0)
		name_based_node = ov->id_based_older->name_based_head;
	else if (os->unique) // Previous name based objectversion is of a different id, so now we do have to perform an extensive look up
		name_based_node = find_name(os, name);

	if (name_based_node) {
		objectversion *co = name_based_node->ov;
		objectversion *oo = get_valid_object_name(tr, co);
		ov->name_based_head = oo->name_based_head;
		if (co != oo) { /* conflict ? */
			return -1;
		}
		ov->name_based_older = oo;

		lock_writer(os);
		if (oo) {
			oo->name_based_newer = ov;
			assert(os_atmc_get_state(oo) == active);
		}
		name_based_node->ov = ov;
		unlock_writer(os);
		return 0;
	} else {
		/* missing */
		return -1;
	}
}

static int
os_del_id_based(objectset *os, struct sql_trans *tr, sqlid id, objectversion *ov) {
	versionhead  *id_based_node;
	if (ov->name_based_older && ov->name_based_older->b->id == id)
		id_based_node = ov->name_based_older->id_based_head;
	else // Previous id based objectversion is of a different name, so now we do have to perform an extensive look up
		id_based_node = find_id(os, id);

	if (id_based_node) {
		objectversion *co = id_based_node->ov;
		objectversion *oo = get_valid_object_id(tr, co);
		ov->id_based_head = oo->id_based_head;
		if (co != oo) { /* conflict ? */
			return -1;
		}
		ov->id_based_older = oo;

		lock_writer(os);
		if (oo) {
			oo->id_based_newer = ov;
			assert(os_atmc_get_state(oo) == active);
		}
		id_based_node->ov = ov;
		unlock_writer(os);
		return 0;
	} else {
		/* missing */
		return -1;
	}
}

static int
os_del_(objectset *os, struct sql_trans *tr, const char *name, sql_base *b)
{
	objectversion *ov = SA_ZNEW(os->sa, objectversion);
	os_atmc_set_state(ov, deleted);
	ov->ts = tr->tid;
	ov->b = b;
	ov->os = os;

	if (os_del_id_based(os, tr, b->id, ov)) {
		_DELETE(ov);
		return -1;
	}

	if (os_del_name_based(os, tr, name, ov)) {
		trans_add(tr, b, ov, &tc_gc_objectversion, &tc_commit_objectversion, NULL);
		return -1;
	}

	if (!os->temporary)
		trans_add(tr, b, ov, &tc_gc_objectversion, &tc_commit_objectversion, NULL);
	return 0;
}

int
os_del(objectset *os, struct sql_trans *tr, const char *name, sql_base *b)
{
	store_lock(tr->store);
	int res = os_del_(os, tr, name, b);
	store_unlock(tr->store);
	return res;
}

int
os_size(objectset *os, struct sql_trans *tr)
{
	int cnt = 0;
	if (os) {
		lock_reader(os);
		for(versionhead  *n = os->name_based_h; n; n=n->next) {
			objectversion *ov = n->ov;
			if ((ov=get_valid_object_name(tr, ov)) && os_atmc_get_state(ov) == active)
				cnt++;
		}
		unlock_reader(os);
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
	// TODO remove entire versionhead  corresponding to this name.

	// TODO assert os->unique?s
	return LOG_OK;
}

sql_base *
os_find_name(objectset *os, struct sql_trans *tr, const char *name)
{
	if (!os)
		return NULL;

	assert(os->unique);
	versionhead  *n = find_name(os, name);

	if (n) {
		 objectversion *ov = get_valid_object_name(tr, n->ov);
		 if (ov && os_atmc_get_state(ov) == active)
			 return ov->b;
	}
	return NULL;
}

sql_base *
os_find_id(objectset *os, struct sql_trans *tr, sqlid id)
{
	if (!os)
		return NULL;
	versionhead  *n = find_id(os, id);

	if (n) {
		 objectversion *ov = get_valid_object_id(tr, n->ov);
		 if (ov && os_atmc_get_state(ov) == active)
			 return ov->b;
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

	lock_reader(os);
	oi->n =	os->name_based_h;
	unlock_reader(os);
}

sql_base *
oi_next(struct os_iter *oi)
{
	sql_base *b = NULL;

	if (oi->name) {
		versionhead  *n = oi->n;

		while (n && !b) {

			if (n->ov->b->name && strcmp(n->ov->b->name, oi->name) == 0) {
				objectversion *ov = n->ov;

				n = oi->n = n->next;
				ov = get_valid_object_name(oi->tr, ov);
				if (ov && os_atmc_get_state(ov) == active)
					b = ov->b;
			} else {
				lock_reader(oi->os);
				n = oi->n = n->next;
				unlock_reader(oi->os);
			}
	 	}
	} else {
		versionhead  *n = oi->n;

		while (n && !b) {
			objectversion *ov = n->ov;
			lock_reader(oi->os);
			n = oi->n = n->next;
			unlock_reader(oi->os);

			ov = get_valid_object_id(oi->tr, ov);
			if (ov && os_atmc_get_state(ov) == active)
				b = ov->b;
		}
	}
	return b;
}

bool
os_obj_intransaction(objectset *os, struct sql_trans *tr, sql_base *b)
{
	versionhead  *n = find_id(os, b->id);

	if (n) {
		 objectversion *ov = get_valid_object_id(tr, n->ov);
		 if (ov && os_atmc_get_state(ov) == active && ov->ts == tr->tid)
			 return true;
	}
	return false;
}
