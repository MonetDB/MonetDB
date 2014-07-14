/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
	int i;
	sql_hash *ht = SA_ZNEW(sa, sql_hash);

	ht->sa = sa;
	ht->size = (1<<log_base2(size-1));
	ht->key = key;
	ht->buckets = SA_NEW_ARRAY(sa, sql_hash_e*, ht->size);
	for(i = 0; i < ht->size; i++)
		ht->buckets[i] = NULL;
	return ht;
}

sql_hash_e*
hash_add(sql_hash *h, int key, void *value)
{
	sql_hash_e *e = SA_ZNEW(h->sa, sql_hash_e);

	e->chain = h->buckets[key&(h->size-1)];
	h->buckets[key&(h->size-1)] = e;
	e->key = key;
	e->value = value;
	return e;
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
	}
}

unsigned int
hash_key(char *k)
{
	unsigned int h = 0;

	while (*k) {
		h += *k;
		h += (h << 10);
		h ^= (h >> 6);
		k++;
	}
	h += (h << 3);
	h ^= (h >> 11);
	h += (h << 15);
	return h;
}
