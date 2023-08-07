/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
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

MT_Lock mal_namespaceLock = MT_LOCK_INITIALIZER(mal_namespaceLock);

/* taken from gdk_atoms */
static inline size_t __attribute__((__pure__))
		nme_hash(const char *key, size_t len)
{
	size_t y = 0;

	for (size_t i = 0; i < len && key[i]; i++) {
		y += key[i];
		y += (y << 10);
		y ^= (y >> 6);
	}
	y += (y << 3);
	y ^= (y >> 11);
	y += (y << 15);
	return y & HASHMASK;
}

typedef struct NAME {
	struct NAME *next;
	char nme[IDLENGTH + 1];
	unsigned short length;
} *NamePtr;

static NamePtr hash[MAXIDENTIFIERS];
const char *optimizerRef;
const char *totalRef;

static struct namespace {
	struct namespace *next;
	int count;
	struct NAME data[4096];
} namespace1, *namespace = &namespace1;

void
initNamespace(void)
{
	optimizerRef = putName("optimizer");
	totalRef = putName("total");
}

void
mal_namespace_reset(void)
{
	struct namespace *ns;

	/* assume we are at the end of the server session */
	MT_lock_set(&mal_namespaceLock);
	memset(hash, 0, sizeof(hash));
	while (namespace) {
		ns = namespace->next;
		if (namespace != &namespace1)
			GDKfree(namespace);
		namespace = ns;
	}
	namespace1.count = 0;
	namespace1.next = NULL;
	namespace = &namespace1;
	MT_lock_unset(&mal_namespaceLock);
}

/*
 * Before a name is being stored we should check for its occurrence first.
 * The administration is initialized incrementally.
 * Beware, the routine getName relies on data structure maintenance that
 * is conflict free.
 */

static const char *
findName(const char *nme, size_t len, bool allocate)
{
	NamePtr *n, m;
	size_t key;

	assert(len == 0 || nme != NULL);
	if (len == 0 || nme == NULL)
		return NULL;
	if (len > IDLENGTH) {
		len = IDLENGTH;
	}
	key = nme_hash(nme, len);
	MT_lock_set(&mal_namespaceLock);
	for (n = &hash[key]; *n; n = &(*n)->next) {
		if (len == (*n)->length && strncmp(nme, (*n)->nme, len) == 0) {
			MT_lock_unset(&mal_namespaceLock);
			return (*n)->nme;
		}
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
			GDKfatal(MAL_MALLOC_FAIL);
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
	return m->nme;
}

const char *
getName(const char *nme)
{
	if (nme != NULL)
		nme = findName(nme, strlen(nme), false);
	return nme;
}

const char *
getNameLen(const char *nme, size_t len)
{
	return findName(nme, len, false);
}

const char *
putName(const char *nme)
{
	if (nme != NULL)
		nme = findName(nme, strlen(nme), true);
	return nme;
}

const char *
putNameLen(const char *nme, size_t len)
{
	return findName(nme, len, true);
}
