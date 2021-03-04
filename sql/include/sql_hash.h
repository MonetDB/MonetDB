/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef SQL_HASH_H
#define SQL_HASH_H

/* sql_hash implementation
 * used to optimize search in expression and statement lists
 */

#include "sql_mem.h"

#define HASH_MIN_SIZE 4

typedef int (*fkeyvalue) (void *data);

typedef struct sql_hash_e {
	int key;
	void *value;
	struct sql_hash_e *chain;
} sql_hash_e;

typedef struct sql_hash {
	sql_allocator *sa;
	int size; /* power of 2 */
	sql_hash_e **buckets;
	fkeyvalue key;
} sql_hash;

extern sql_hash *hash_new(sql_allocator *sa, int size, fkeyvalue key);
extern void hash_del(sql_hash *ht, int key, void *value);
extern void hash_destroy(sql_hash *h);

static inline sql_hash_e*
hash_add(sql_hash *h, int key, void *value)
{
	sql_hash_e *e = (h->sa)?SA_NEW(h->sa, sql_hash_e):MNEW(sql_hash_e);

	if (e == NULL)
		return NULL;
	e->chain = h->buckets[key&(h->size-1)];
	h->buckets[key&(h->size-1)] = e;
	e->key = key;
	e->value = value;
	return e;
}

static inline unsigned int
hash_key(const char *restrict k)
{
	unsigned int h = 37; /* prime number */
	while (*k) {
		h = (h * 54059) ^ (k[0] * 76963); /* prime numbers */
		k++;
	}
	return h;
}

#endif /* SQL_HASH_H */
