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

#ifndef SQL_HASH_H
#define SQL_HASH_H

/* sql_hash implementation 
 * used to optimize search in expression and statement lists 
 */

#include <sql_mem.h>

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

extern unsigned int hash_key(char *n);

#endif /* SQL_STACK_H */
