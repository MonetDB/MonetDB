/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
	size_t length;
	struct NAME *next;
	char nme[FLEXIBLE_ARRAY_MEMBER];
} *NamePtr;

static NamePtr *hash= NULL, *ehash = NULL;

void initNamespace(void) {
	if(hash == NULL) hash= (NamePtr *) GDKzalloc(sizeof(NamePtr) * MAXIDENTIFIERS);
	if(ehash == NULL) ehash= (NamePtr *) GDKzalloc(sizeof(NamePtr) * MAXIDENTIFIERS);
	if ( hash == NULL || ehash == NULL){
        /* absolute an error we can not recover from */
        showException(GDKout, MAL,"initNamespace",MAL_MALLOC_FAIL);
        mal_exit();
	}
}

void mal_namespace_reset(void) {
	int i;
	NamePtr n,m;

	/* assume we are at the end of the server session */
	MT_lock_set(&mal_namespaceLock);
	for ( i =0; i < HASHMASK; i++){
		n = hash[i];
		hash[i] = ehash[i] = 0;
		for( ; n; n = m){
			m = n->next;
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
 * Beware, the routine getName relies on data structure maintenance that
 * is conflict free.
 */

str getName(const char *nme) {
	return getNameLen(nme, strlen(nme));
}

str getNameLen(const char *nme, size_t len)
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
	n= getNameLen(nme,len);
	if( nme[0]==0 || n == 0) return ;
	/*Namespace garbage collection not available yet */
}

str putName(const char *nme) {
	return putNameLen(nme, strlen(nme));
}

str putNameLen(const char *nme, size_t len)
{
	size_t l,k;
	int key;
	str fnd;
	NamePtr n;

	fnd = getNameLen(nme,len);
	if ( fnd )
		return fnd;

	if( nme == NULL || len == 0)
		return NULL;

	/* construct a new entry */
	if(len>=MAXIDENTLEN)
		len = MAXIDENTLEN - 1;
	n = GDKmalloc(offsetof(struct NAME, nme) + len + 1);
	if ( n == NULL) {
        /* absolute an error we can not recover from */
        showException(GDKout, MAL,"initNamespace",MAL_MALLOC_FAIL);
		mal_exit();
	}
	memcpy(n->nme, nme, len);
	n->nme[len]=0;
	n->length = len;
	n->next = NULL;
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
	return putNameLen(nme, len);	/* just to be sure */
}
