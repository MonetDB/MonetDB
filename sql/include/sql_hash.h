/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
extern sql_hash_e *hash_add(sql_hash *ht, int key, void *value);
extern void hash_del(sql_hash *ht, int key, void *value);

extern unsigned int hash_key(const char *n);

#endif /* SQL_STACK_H */
