/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
#define NME_HASH(_key,y,K)								\
    do {												\
        size_t _i;										\
        for (_i = y = 0; _i < K && _key[_i]; _i++) {	\
            y += _key[_i];								\
            y += (y << 10);								\
            y ^= (y >> 6);								\
        }												\
        y += (y << 3);									\
        y ^= (y >> 11);									\
        y += (y << 15);									\
		y = y & HASHMASK;								\
    } while (0)


typedef struct NAME{
	struct NAME *next;
	char nme[IDLENGTH + 1];
	unsigned short length;
} *NamePtr;

static NamePtr hash[MAXIDENTIFIERS];

static struct namespace {
	struct namespace *next;
	int count;
	struct NAME data[4096];
} *namespace;

void initNamespace(void) {
	namespace = NULL;
}

void mal_namespace_reset(void) {
	struct namespace *ns;

	/* assume we are at the end of the server session */
	MT_lock_set(&mal_namespaceLock);
	memset(hash, 0, sizeof(hash));
	while (namespace) {
		ns = namespace->next;
		GDKfree(namespace);
		namespace = ns;
	}
	MT_lock_unset(&mal_namespaceLock);
}

/*
 * Before a name is being stored we should check for its occurrence first.
 * The administration is initialized incrementally.
 * Beware, the routine getName relies on data structure maintenance that
 * is conflict free.
 */

static str findName(const char *nme, size_t len, int allocate)
{
	NamePtr *n, m;
	size_t key;

	assert(len == 0 || nme != NULL);
	if (len == 0 || nme == NULL)
		return NULL;
	if (len > IDLENGTH) {
		len = IDLENGTH;
	}
	NME_HASH(nme, key, len);
	MT_lock_set(&mal_namespaceLock);
	for (n = &hash[key]; *n; n = &(*n)->next) {
#ifdef KEEP_SORTED
		/* keep each list sorted on length, then name */
		if (len < (*n)->length)
			continue;
		if (len == (*n)->length) {
			int c;
			if ((c = strncmp(nme, (*n)->nme, len)) < 0)
				continue;
			if (c == 0) {
				MT_lock_unset(&mal_namespaceLock);
				return (*n)->nme;
			}
			break;
		}
		break;
#else
		/* append entries to list */
		if (len == (*n)->length && strncmp(nme, (*n)->nme, len) == 0) {
			MT_lock_unset(&mal_namespaceLock);
			return (*n)->nme;
		}
#endif
	}
	/* item not found */
	if (!allocate) {
		MT_lock_unset(&mal_namespaceLock);
		return NULL;
	}
	if (namespace == NULL || namespace->count == 4096) {
		struct namespace *ns = GDKmalloc(sizeof(struct namespace));
		if (ns == NULL) {
			/* error we cannot recover from */
			showException(GDKout, MAL, "findName", MAL_MALLOC_FAIL);
			mal_exit();
		}
		ns->next = namespace;
		ns->count = 0;
		namespace = ns;
	}
	m = &namespace->data[namespace->count++];
	strncpy(m->nme, nme, len);
	m->nme[len] = 0;
	m->length = (unsigned short) len;
	m->next = *n;
	*n = m;
	MT_lock_unset(&mal_namespaceLock);
	return (*n)->nme;
}

str getName(const char *nme) {
	return findName(nme, strlen(nme), 0);
}

str getNameLen(const char *nme, size_t len)
{
	return findName(nme, len, 0);
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
	return findName(nme, strlen(nme), 1);
}

str putNameLen(const char *nme, size_t len)
{
	return findName(nme, len, 1);
}
