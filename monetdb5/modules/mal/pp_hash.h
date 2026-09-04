/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _PP_HASH_H_
#define _PP_HASH_H_

#include "matomic.h"
#define GIDBITS 63

//#define HT_MIN_SIZE 1024*64*8
#define HT_MIN_SIZE 1024*8
//#define HT_MIN_SIZE 256
#define HT_MAX_SIZE 1024*1024*1024
#define HP_MIN_SIZE HT_MIN_SIZE
#define HP_MAX_SIZE HT_MAX_SIZE

#define HT_PRE_CLAIM 256
#define ht_preclaim(private) private?1:HT_PRE_CLAIM
//#define HT_PRE_CLAIM 1
//#define ht_preclaim(private) HT_PRE_CLAIM

#define linear_probing k=hv+l
#define quadratic_probing k=hv+(l*l)

#define nextk linear_probing
//#define nextk quadratic_probing

#define hash_rehash(ht, pl, err)		\
	{ 								\
		if (ht_rehash(ht)) {		\
			pl->p->status = 1; 		\
			err = createException(MAL, "oahash.rehash", MAL_MALLOC_FAIL); 	\
			break;					\
		}							\
	}

#define bitHash(x)			bteHash(x)
#define dateHash(x)			intHash(x)
#define daytimeHash(x)		lngHash(x)
#define timestampHash(x)	lngHash(x)
#define gidHash(x)			oidHash(x)

//#define combine(X,Y)  (_hash_lng((X*5671432987))^(ulng)Y)
//#define combine(X,Y)  (_hash_lng((X*(hash_prime_nr[h->bits-5])))^(ulng)Y)
#define combine(X,Y,pr)  (oidHash(&(oid){(X)*(pr)})^(BUN)(Y))
static const int
hash_prime_nr[32] = {
	53,
	97,
	193,
	389,
	769,
	1543,
	3079,
	6151,
	12289,
	24593,
	49157,
	98317,
	196613,
	393241,
	786433,
	1572869,
	3145739,
	6291469,
	12582917,
	25165843,
	50331653,
	100663319,
	201326611,
	402653189,
	805306457,
	1610612741 };


typedef oid		gid;
typedef ATOMIC_TYPE hash_key_t;
#define ATOMIC_GET_GID(a) ((gid)ATOMIC_GET(a))
#define ATOMIC_ADD_GID(a,i) ((gid)ATOMIC_ADD(a,i))

typedef int (*fcmp)(const void *v1, const void *v2);
typedef lng (*fhsh)(const void *v);
typedef size_t (*flen)(const void *v);

typedef struct hash_table {
	struct pipeline_io pl_io;
	int type;
	int width;
	fcmp cmp;
	fhsh hsh;
	flen len;
	bool empty;
	ATOMIC_TYPE has_nil; /* whether the hashed column contain a NULL, defaul: 0, i.e. false */
	int vkey;	/* vheap is unique, ie hashing on offsets */

	void *vals;			/* hash(ed) values */
	hash_key_t *gids;   /* chain of gids (k, ie mark used/-k mark used and value filled) */
	gid *pgids;			/* id of the parent hash */

	struct hash_table *p;	/* parent hash */
	int bits;
	ATOMIC_TYPE last;
	size_t size;
	gid mask;
	Heap **pinned;		/* sharing variable objects means keep reference to varheaps */
	int pinned_nr;
	allocator **allocators;
	int nr_allocators;
	size_t processed;

	MT_RWLock rwlock;	/* needed for save resizing */
} hash_table;

//extern lng str_hsh(str v);
static inline lng
str_hsh( str v )
{
    // Source: https://github.com/aappleby/smhasher/blob/master/src/Hashes.cpp
    ulng h = 2166136261UL;
    const uint8_t* data = (const uint8_t*)v;
    for (int i = 0; data[i]; i++) {
        h ^= data[i];
        h *= 16777619;
    }
    return (lng)h;
}

extern hash_table *ht_create(int type, size_t size, hash_table *p, int vkey);
extern int ht_rehash(hash_table *ht);

extern void ht_activate(hash_table *ht);
extern void ht_deactivate(hash_table *ht);

#endif /*_PP_HASH_H_*/
