/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _PP_HASH_H_
#define _PP_HASH_H_

//#include "pp_mem.h"

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


#define hash_rehash(ht, p, err)		\
	{ 								\
		if (ht_rehash(ht)) {		\
			p->p->status = 1; 		\
			err = createException(MAL, "oahash.rehash", MAL_MALLOC_FAIL); 	\
			break;					\
		}							\
	}

#define _hash_bit(X)  ((unsigned int)X)
#define _hash_bte(X)  ((unsigned int)X)
#define _hash_sht(X)  ((unsigned int)X)
#define _hash_int_(X) ((((unsigned int)X)>>7)^(((unsigned int)X)>>13)^(((unsigned int)X)>>21)^((unsigned int)X))
//#define _hash_int(X)  (_hash_int_((X)*98317))
#define _hash_int(X)  (_hash_int_(((int)(X))*25165843))
//#define _hash_int(X)  (_hash_int_((X)*hash_prime_nr[21]))

#if 0
  static inline ulng _hash_int(unsigned int x) { /* murmur finishing */
      x ^= x >> 16;
      x *= 0x85ebca6bU;
      x ^= x >> 13;
      x *= 0xc2b2ae35U;
      x ^= x >> 16;
      return (ulng)x;
  }
#endif

  static inline ulng _hash_lng(ulng x) { /* splitmix64; see https://nullprogram.com/blog/2018/07/31/ for inversion */
      x ^= x >> 30;
      x *= 0xbf58476d1ce4e5b9ULL;
      x ^= x >> 27;
      x *= 0x94d049bb133111ebULL;
      x ^= x >> 31;
      return x;
  }

#define _hash_date(X) _hash_int(X)
#define _hash_inet4(X) _hash_int((*(int*)&X))
//#define _hash_lng(X)  ((((ulng)X)>>7)^(((ulng)X)>>13)^(((ulng)X)>>21)^(((ulng)X)>>31)^(((ulng)X)>>38)^(((ulng)X)>>46)^(((ulng)X)>>56)^((ulng)X))
#define _hash_oid(X)  _hash_lng((X*98317))
#define _hash_daytime(X) _hash_lng(X)
#define _hash_timestamp(X) _hash_lng(X)

#ifdef HAVE_HGE
#define _hash_uuid(X) _hash_hge(X)

#define _mix_hge(X)      (((hge) (X) >> 7) ^     \
                         ((hge) (X) >> 13) ^    \
                         ((hge) (X) >> 21) ^    \
                         ((hge) (X) >> 31) ^    \
                         ((hge) (X) >> 38) ^    \
                         ((hge) (X) >> 46) ^    \
                         ((hge) (X) >> 56) ^    \
                         ((hge) (X) >> 65) ^    \
                         ((hge) (X) >> 70) ^    \
                         ((hge) (X) >> 78) ^    \
                         ((hge) (X) >> 85) ^    \
                         ((hge) (X) >> 90) ^    \
                         ((hge) (X) >> 98) ^    \
                         ((hge) (X) >> 107) ^   \
                         ((hge) (X) >> 116) ^   \
                         (hge) (X))
#define _hash_hge(X)  (_hash_lng(((lng)X) ^ _hash_lng((lng)(X>>64))))
//#define hash_hge(X)  ((lng)_mix_hge(X))
#endif

#define _hash_flt(X)  (_hash_int(X))
#define _hash_dbl(X)  (_hash_lng(X))
#define _hash_gid(X)  (_hash_lng(X))
#define ROT64(x, y)  ((x << y) | (x >> (64 - y)))
//#define combine(X,Y)  (_hash_lng((X*5671432987))^(ulng)Y)
//#define combine(X,Y)  (_hash_lng((X*(hash_prime_nr[h->bits-5])))^(ulng)Y)
#define combine(X,Y,pr)  (_hash_lng((X*(pr)))^(ulng)Y)
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


typedef lng gid;
typedef ATOMIC_TYPE hash_key_t;

typedef int (*fcmp)(const void *v1, const void *v2);
typedef lng (*fhsh)(const void *v);
typedef size_t (*flen)(const void *v);

typedef struct hash_table {
	Sink s;
	int type;
	int width;
	fcmp cmp;
	fhsh hsh;
	flen len;
	bool empty;

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
    lng h = 2166136261UL;
    const uint8_t* data = (const uint8_t*)v;
    for (int i = 0; data[i]; i++) {
        h ^= data[i];
        h *= 16777619;
    }
    return h;
}

extern hash_table *ht_create(int type, size_t size, hash_table *p);
extern int ht_rehash(hash_table *ht);

extern void ht_activate(hash_table *ht);
extern void ht_deactivate(hash_table *ht);

#endif /*_PP_HASH_H_*/
