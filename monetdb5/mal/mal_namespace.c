/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

/*
 * (author) M.L. Kersten
 */
#include "monetdb_config.h"
#include "mal_type.h"
#include "mal_namespace.h"
#include "mal_exception.h"
#include "mal_private.h"

#define HASHSIZE	(1 << 12)		/* power of two */
#define HASHMASK	(HASHSIZE - 1)

MT_Lock mal_namespaceLock = MT_LOCK_INITIALIZER(mal_namespaceLock);

#define nme_hash(key)	((size_t) (strHash(key) & HASHMASK))

typedef struct NAME {
	struct NAME *next;
	char nme[IDLENGTH + 1];
} *NamePtr;

static NamePtr hash[HASHSIZE];

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
static struct fixname *fixhash[HASHSIZE];

static void
fixName(const char *name)
{
	size_t key = nme_hash(name);
	struct fixname **n;
	for (n = &fixhash[key]; *n; n = &(*n)->next) {
		if ((*n)->name == name || strcmp((*n)->name, name) == 0) {
			/* name is already there; this can happen when
			 * reinitializing */
			return;
		}
	}
	assert(fixnamespace.count < 1024);
	struct fixname *new = &fixnamespace.data[fixnamespace.count++];
	*new = (struct fixname) {
		.name = name,
	};
	*n = new;
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
	MT_lock_set(&mal_namespaceLock);
	FOREACH_NAME(NAME_FIX);
	fixName(divRef);
	fixName(eqRef);
	fixName(minusRef);
	fixName(modRef);
	fixName(mulRef);
	fixName(plusRef);
	MT_lock_unset(&mal_namespaceLock);
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
findName(const char *nme, bool allocate)
{
	NamePtr *n, m;
	size_t key;

	if (nme == NULL)
		return NULL;
	key = nme_hash(nme);
	MT_lock_set(&mal_namespaceLock);
	for (struct fixname *p = fixhash[key]; p; p = p->next) {
		if (p->name == nme || (strcmp(p->name, nme) == 0)) {
			MT_lock_unset(&mal_namespaceLock);
			return p->name;
		}
	}
	for (n = &hash[key]; *n; n = &(*n)->next) {
		if (strcmp(nme, (*n)->nme) == 0) {
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
	strcpy_len(m->nme, nme, sizeof(m->nme));
	m->next = *n;
	*n = m;
	MT_lock_unset(&mal_namespaceLock);
	return m->nme;
}

const char *
getName(const char *nme)
{
	if (nme != NULL)
		nme = findName(nme, false);
	return nme;
}

const char *
getNameLen(const char *nme, size_t len)
{
	char name[IDLENGTH + 1];
	if (len > IDLENGTH)
		len = IDLENGTH;
	strcpy_len(name, nme, len + 1);
	return findName(name, false);
}

const char *
putName(const char *nme)
{
	if (nme != NULL)
		nme = findName(nme, true);
	return nme;
}

const char *
putNameLen(const char *nme, size_t len)
{
	char name[IDLENGTH + 1];
	if (len > IDLENGTH)
		len = IDLENGTH;
	strcpy_len(name, nme, len + 1);
	return findName(name, true);
}
