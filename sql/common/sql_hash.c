
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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "sql_mem.h"
#include "sql_hash.h"

sql_hash *
hash_new(sql_allocator *sa, int size, fkeyvalue key)
{
	int i;
	sql_hash *ht = SA_ZNEW(sa, sql_hash);

	ht->sa = sa;
	ht->size = (size/2)*2;
	ht->key = key;
	ht->buckets = SA_NEW_ARRAY(sa, sql_hash_e*, size);
	for(i = 0; i < size; i++)
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

int
hash_key(char *k)
{
	char *s = k;
	int h = 1, l;

	while (*k) {
		h <<= 5;
		h += (*k - 'a');
		k++;
	}
	l = (int) (k - s);
	h <<= 4;
	h += l;
	return (h < 0) ? -h : h;
}
