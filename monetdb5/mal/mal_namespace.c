/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * (author) M.L. Kersten
 */
#include "monetdb_config.h"
#include "mal_type.h"
#include "mal_namespace.h"
#include "mal_exception.h"
#include "mal_private.h"

#define MAXIDENTIFIERS 4096
#define HASHMASK  4095

/* taken from gdk_atoms */
#define NME_HASH(x,y,K)                \
    do {                        \
        const char *_key = (const char *) (x);  \
        size_t _i;                 \
        for (_i = y = 0; K-- && _key[_i]; _i++) {  \
            y += _key[_i];          \
            y += (y << 10);         \
            y ^= (y >> 6);          \
        }                   \
        y += (y << 3);              \
        y ^= (y >> 11);             \
        y += (y << 15);             \
		y = y & HASHMASK;			\
    } while (0)


typedef struct NAME{
	str nme;
	size_t length;
	struct NAME *next;
} *NamePtr;

static NamePtr *hash, *ehash;

void initNamespace(void) {
	hash= (NamePtr *) GDKzalloc(sizeof(NamePtr) * MAXIDENTIFIERS);
	ehash= (NamePtr *) GDKzalloc(sizeof(NamePtr) * MAXIDENTIFIERS);
	if ( hash == NULL || ehash == NULL){
        /* absolute an error we can not recover from */
        showException(GDKout, MAL,"initNamespace",MAL_MALLOC_FAIL);
        mal_exit();
	}
}

void finishNamespace(void) {
	int i;
	NamePtr n,m;

	/* assume we are at the end of the server session */
	MT_lock_set(&mal_namespaceLock);
	for ( i =0; i < HASHMASK; i++){
		n = hash[i];
		hash[i] = ehash[i] = 0;
		for( ; n; n = m){
			m = n->next;
			if (n->nme)
				GDKfree(n->nme);
			GDKfree(n);
		}
	}
	GDKfree(hash);
	GDKfree(ehash);
	hash = ehash = 0;
	MT_lock_unset(&mal_namespaceLock);
}

/*
 * Before a name is being stored we should check for its occurrence first.
 * The administration is initialized incrementally.
 * Beware, the routine getName relies on datastructure maintenance that
 * is conflict free.
 */
str getName(const char *nme, size_t len)
{
	NamePtr n;
	size_t l = len, key;

	if(len == 0 || nme== NULL || *nme==0) 
		return NULL;
	if(len>=MAXIDENTLEN)
		len = MAXIDENTLEN - 1;
	NME_HASH(nme, key, l);
	if ( ( n = hash[(int)key]) == 0)
		return NULL;

	do {
		if (len == n->length && strncmp(nme,n->nme,len)==0) 
			return n->nme;
		n = n->next;
	} while (n);
	return NULL;
}
/*
 * Name deletion from the namespace is tricky, because there may
 * be multiple threads active on the structure. Moreover, the
 * symbol may be picked up by a concurrent thread and stored
 * somewhere.
 * To avoid all these problems, the namespace should become
 * private to each Client, but this would mean expensive look ups
 * deep into the kernel to access the context.
 */
void delName(const char *nme, size_t len){
	str n;
	n= getName(nme,len);
	if( nme[0]==0 || n == 0) return ;
	/*Namespace garbage collection not available yet */
}

str putName(const char *nme, size_t len)
{
	size_t l,k;
	int key;
	char buf[MAXIDENTLEN];
	str fnd;
	NamePtr n;

	fnd = getName(nme,len);
	if ( fnd )
		return fnd;

	if( nme == NULL || len == 0)
		return NULL;

	/* construct a new entry */
	n = (NamePtr) GDKzalloc(sizeof(*n));
	if ( n == NULL) {
        /* absolute an error we can not recover from */
        showException(GDKout, MAL,"initNamespace",MAL_MALLOC_FAIL);
		mal_exit();
	}
	if(len>=MAXIDENTLEN)
		len = MAXIDENTLEN - 1;
	memcpy(buf, nme, len);
	buf[len]=0;
	n->nme= GDKstrdup(buf);
	if (n->nme == NULL) {
        /* absolute an error we can not recover from */
        showException(GDKout, MAL,"initNamespace",MAL_MALLOC_FAIL);
		mal_exit();
	}
	n->length = len;
	l = len;
	NME_HASH(nme, k, l);
	key = (int) k;

	MT_lock_set(&mal_namespaceLock);
	/* add new elements to the end of the list */
	if ( ehash[key] == 0)
		hash[key] = ehash[key] = n;
	else {
		ehash[key]->next = n;
		ehash[key] = n;
	}
	MT_lock_unset(&mal_namespaceLock);
	return putName(nme, len);	/* just to be sure */
}
