/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_mem.h"
#include "sql_hash.h"

static unsigned int
log_base2(unsigned int n)
{
	unsigned int l ;

	for (l = 0; n; l++)
		n >>= 1 ;
	return l ;
}

sql_hash *
hash_new(sql_allocator *sa, int size, fkeyvalue key)
{
	sql_hash *ht = (sa)?SA_NEW(sa, sql_hash):MNEW(sql_hash);

	if (ht == NULL)
		return NULL;
	ht->sa = sa;
	ht->size = (1<<log_base2(size-1));
	ht->key = key;
	ht->buckets = (ht->sa)?SA_NEW_ARRAY(sa, sql_hash_e*, ht->size):NEW_ARRAY(sql_hash_e*, ht->size); // TODO: can fail
	if (ht->buckets == NULL) {
		_DELETE(ht);
		return NULL;
	}
	for (int i = 0; i < ht->size; i++)
		ht->buckets[i] = NULL;
	return ht;
}

void
hash_del(sql_hash *h, int key, void *value)
{
	sql_hash_e *e = h->buckets[key&(h->size-1)], *p = NULL;

	while (e && (e->key != key || e->value != value)) {
		p = e;
		e = e->chain;
	}
	if (e) {
		if (p)
			p->chain = e->chain;
		else
			h->buckets[key&(h->size-1)] = e->chain;
		if(!h->sa)
			_DELETE(e);
	}
}

void
hash_destroy(sql_hash *h) /* this code should be called for hash tables created outside SQL allocators only! */
{
	if (h == NULL || h->sa)
		return;
	for (int i = 0; i < h->size; i++) {
		sql_hash_e *e = h->buckets[i], *c = NULL;

		if (e)
			c = e->chain;
		while (c) {
			sql_hash_e *next = c->chain;

			_DELETE(c);
			c = next;
		}
		_DELETE(e);
	}
	_DELETE(h->buckets);
	_DELETE(h);
}

