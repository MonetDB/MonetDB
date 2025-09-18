/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
__attribute__((__pure__))
static inline size_t
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
} *NamePtr;

static NamePtr hash[MAXIDENTIFIERS];

static struct namespace {
	struct namespace *next;
	int count;
	struct NAME data[4096];
} namespace1, *namespace = &namespace1;

struct fixname {
	struct fixname *next;
	const char *name;
};
static struct fixnamespace {
	int count;
	struct fixname data[1024];
} fixnamespace;
static struct fixname *fixhash[4096];

static void
fixName(const char *name)
{
	size_t key = nme_hash(name, 1024 /* something large */);
	MT_lock_set(&mal_namespaceLock);
	struct fixname **n;
	for (n = &fixhash[key]; *n; n = &(*n)->next) {
		if ((*n)->name == name || strcmp((*n)->name, name) == 0) {
			/* name is already there; this can happen when
			 * reinitializing */
			MT_lock_unset(&mal_namespaceLock);
			return;
		}
	}
	assert(fixnamespace.count < 1024);
	struct fixname *new = &fixnamespace.data[fixnamespace.count++];
	*new = (struct fixname) {
		.name = name,
	};
	*n = new;
	MT_lock_unset(&mal_namespaceLock);
}

#define NAME_DEFINE(NAME) const char NAME##Ref[] = #NAME
FOREACH_NAME(NAME_DEFINE);
const char divRef[] = "/";
const char eqRef[] = "==";
const char minusRef[] = "-";
const char modRef[] = "%";
const char mulRef[] = "*";
const char plusRef[] = "+";

#define NAME_FIX(NAME)	fixName(NAME##Ref)

void
initNamespace(void)
{
	FOREACH_NAME(NAME_FIX);
	fixName(divRef);
	fixName(eqRef);
	fixName(minusRef);
	fixName(modRef);
	fixName(mulRef);
	fixName(plusRef);
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
	namespace1 = (struct namespace) {
		.count = 0,
	};
	namespace = &namespace1;
	MT_lock_unset(&mal_namespaceLock);
}

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
	for (struct fixname *p = fixhash[key]; p; p = p->next) {
		if (p->name == nme || (strncmp(p->name, nme, len) == 0 && p->name[len] == 0)) {
			MT_lock_unset(&mal_namespaceLock);
			return p->name;
		}
	}
	for (n = &hash[key]; *n; n = &(*n)->next) {
		if (strncmp(nme, (*n)->nme, len) == 0 && (*n)->nme[len] == 0) {
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
			MT_lock_unset(&mal_namespaceLock);
			return NULL;
		}
		ns->next = namespace;
		ns->count = 0;
		namespace = ns;
	}
	m = &namespace->data[namespace->count++];
	assert(m->nme != nme);
	strncpy(m->nme, nme, len);
	m->nme[len] = 0;
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
