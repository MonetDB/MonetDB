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

static inline int
node_key( node *n )
{
	sql_base *b = n->data;
	return hash_key(b->name);
}

objlist *
ol_new(sql_allocator *sa, destroy_fptr destroy)
{
	objlist *ol = SA_NEW(sa, objlist);
	*ol = (objlist) {
		.l = list_new(sa, (fdestroy)destroy),
		.h = hash_new(sa, 16, (fkeyvalue)&node_key)
	};
	return ol;
}

void
ol_destroy(objlist *ol, sql_store store)
{
	if (!ol->l->sa) {
		hash_destroy(ol->h);
		list_destroy2(ol->l, store);
		_DELETE(ol);
	}
}

int
ol_add(objlist *ol, sql_base *data)
{
	list *l = list_append(ol->l, data);
	if (!l)
		return -1;
	node *n = l->t;
	assert(n->data == data);
	int sz = list_length(ol->l);
	if (ol->h->size <= sz) {
		hash_destroy(ol->h);
		ol->h = hash_new(ol->l->sa, 4*sz, (fkeyvalue)&node_key);
		for (node *n = ol->l->h; n; n = n->next) {
			if (hash_add(ol->h, base_key(n->data), n) == NULL)
				return -1;
		}
	} else {
		if (hash_add(ol->h, base_key(data), n) == NULL)
			return -1;
	}
	return 0;
}

void
ol_del(objlist *ol, sql_store store, node *n)
{
	hash_del(ol->h, node_key(n), n);
	list_remove_node(ol->l, store, n);
}

node *
ol_rehash(objlist *ol, const char *oldname, node *n)
{
	hash_del(ol->h, hash_key(oldname), n);
	if (hash_add(ol->h, node_key(n), n) == NULL)
		return NULL;
	return n;
}

node *
ol_find_name(objlist *ol, const char *name)
{
	int key = hash_key(name);
	sql_hash_e *he = ol->h->buckets[key&(ol->h->size-1)];

	for (; he; he = he->chain) {
		node *n = he->value;
		sql_base *b = n->data;

		if (b->name && strcmp(b->name, name) == 0)
			return n;
	}
	return NULL;
}

node *ol_find_id(objlist *ol, sqlid id)
{
	/* if needed we could add hash on id's as well */
	for (node *n = ol->l->h; n; n = n->next) {
		sql_base *b = n->data;
		if (b->id == id)
			return n;
	}
	return NULL;
}
