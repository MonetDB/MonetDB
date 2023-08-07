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
 * (authors) M. Kersten, F. Groffen
 * Authorisation adminstration management
 * Authorisation of users is a key concept in protecting the server from
 * malicious and unauthorised users.  This file contains a number of
 * functions that administrate a set of BATs backing the authorisation
 * tables.
 *
 * The implementation is based on three persistent BATs, which keep the
 * usernames, passwords and allowed scenarios for users of the server.
 *
 */
#include "monetdb_config.h"
#include "mal_authorize.h"
#include "mal_exception.h"
#include "mal_private.h"
#include "mcrypt.h"
#include "msabaoth.h"
#include "mal_scenario.h"
#include "mal_interpreter.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

/* yep, the vault key is just stored in memory */
static str vaultKey = NULL;
/* lock to protect the above */
static MT_RWLock rt_lock = MT_RWLOCK_INITIALIZER(rt_lock);

static str AUTHdecypherValueLocked(str *ret, const char *value);

void
AUTHreset(void)
{
	MT_rwlock_wrlock(&rt_lock);
	GDKfree(vaultKey);
	vaultKey = NULL;
	MT_rwlock_wrunlock(&rt_lock);
}

/**
 * Requires the current client to be the admin user thread. If not the case,
 * this function returns an InvalidCredentialsException.
 */
str
AUTHrequireAdmin(Client cntxt)
{
	assert(cntxt);

	if (cntxt->user != MAL_ADMIN)
		throw(MAL, "AUTHrequireAdmin", INVCRED_ACCESS_DENIED);
	return (MAL_SUCCEED);
}

/*=== the vault ===*/

/**
 * Unlocks the vault with the given password.  Since the password is
 * just the decypher key, it is not possible to directly check whether
 * the given password is correct.  If incorrect, however, all decypher
 * operations will probably fail or return an incorrect decyphered
 * value.
 */
str
AUTHunlockVault(const char *password)
{
	if (strNil(password))
		throw(ILLARG, "unlockVault", "password should not be nil");

	/* even though I think this function should be called only once, it
	 * is not of real extra efforts to avoid a mem-leak if it is used
	 * multiple times */
	MT_rwlock_wrlock(&rt_lock);
	GDKfree(vaultKey);

	vaultKey = GDKstrdup(password);
	if (vaultKey == NULL) {
		MT_rwlock_wrunlock(&rt_lock);
		throw(MAL, "unlockVault", SQLSTATE(HY013) MAL_MALLOC_FAIL " vault key");
	}
	MT_rwlock_wrunlock(&rt_lock);
	return (MAL_SUCCEED);
}

/**
 * Decyphers a given value, using the vaultKey.  The returned value
 * might be incorrect if the vaultKey is incorrect or unset.  If the
 * cypher algorithm fails or detects an invalid password, it might throw
 * an exception.  The ret string is GDKmalloced, and should be GDKfreed
 * by the caller.
 */
static str
AUTHdecypherValueLocked(str *ret, const char *value)
{
	/* Cyphering and decyphering can be done using many algorithms.
	 * Future requirements might want a stronger cypher than the XOR
	 * cypher chosen here.  It is left up to the implementor how to do
	 * that once those algoritms become available.  It could be
	 * #ifdef-ed or on if-basis depending on whether the cypher
	 * algorithm is a compile, or runtime option.  When necessary, this
	 * function could be extended with an extra argument that indicates
	 * the cypher algorithm.
	 */

	/* this is the XOR decypher implementation */
	str r, w;
	const char *s = value;
	char t = '\0';
	bool escaped = false;
	/* we default to some garbage key, just to make password unreadable
	 * (a space would only uppercase the password) */
	size_t keylen = 0;

	if (vaultKey == NULL)
		throw(MAL, "decypherValue", "The vault is still locked!");
	w = r = GDKmalloc(sizeof(char) * (strlen(value) + 1));
	if (r == NULL)
		throw(MAL, "decypherValue", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	keylen = strlen(vaultKey);

	/* XOR all characters.  If we encounter a 'one' char after the XOR
	 * operation, it is an escape, so replace it with the next char. */
	for (; (t = *s) != '\0'; s++) {
		if ((t & 0xE0) == 0xC0) {
			assert((t & 0x1E) == 0x02);
			assert((s[1] & 0xC0) == 0x80);
			t = ((t & 0x1F) << 6) | (*++s & 0x3F);
		}
		if (t == '\1' && !escaped) {
			escaped = true;
			continue;
		} else if (escaped) {
			t -= 1;
			escaped = false;
		}
		*w = t ^ vaultKey[(w - r) % keylen];
		w++;
	}
	*w = '\0';

	*ret = r;
	return (MAL_SUCCEED);
}

str
AUTHdecypherValue(str *ret, const char *value)
{
	MT_rwlock_rdlock(&rt_lock);
	str err = AUTHdecypherValueLocked(ret, value);
	MT_rwlock_rdunlock(&rt_lock);
	return err;
}

/**
 * Cyphers the given string using the vaultKey.  If the cypher algorithm
 * fails or detects an invalid password, it might throw an exception.
 * The ret string is GDKmalloced, and should be GDKfreed by the caller.
 */
static str
AUTHcypherValueLocked(str *ret, const char *value)
{
	/* this is the XOR cypher implementation */
	str r, w;
	const char *s = value;
	/* we default to some garbage key, just to make password unreadable
	 * (a space would only uppercase the password) */
	size_t keylen = 0;

	if (vaultKey == NULL)
		throw(MAL, "cypherValue", "The vault is still locked!");
	w = r = GDKmalloc(sizeof(char) * (strlen(value) * 2 + 1));
	if (r == NULL)
		throw(MAL, "cypherValue", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	keylen = strlen(vaultKey);

	/* XOR all characters.  If we encounter a 'zero' char after the XOR
	 * operation, escape it with a 'one' char. */
	for (; *s != '\0'; s++) {
		char c = *s ^ vaultKey[(s - value) % keylen];
		if (c == '\0') {
			*w++ = '\1';
			*w = '\1';
		} else if (c == '\1') {
			*w++ = '\1';
			*w = '\2';
		} else if (c & 0x80) {
			*w++ = 0xC0 | ((c >> 6) & 0x03);
			*w = 0x80 | (c & 0x3F);
		} else {
			*w = c;
		}
		w++;
	}
	*w = '\0';

	*ret = r;
	return (MAL_SUCCEED);
}

str
AUTHcypherValue(str *ret, const char *value)
{
	MT_rwlock_rdlock(&rt_lock);
	str err = AUTHcypherValueLocked(ret, value);
	MT_rwlock_rdunlock(&rt_lock);
	return err;
}

/**
 * Checks if the given string is a (hex represented) hash for the
 * current backend.  This check allows to at least forbid storing
 * trivial plain text passwords by a simple check.
 */
#define concat(x,y)	x##y
#define digestlength(h)	concat(h, _DIGEST_LENGTH)
str
AUTHverifyPassword(const char *passwd)
{
	const char *p = passwd;
	size_t len = strlen(p);

	if (len != digestlength(MONETDB5_PASSWDHASH_TOKEN) * 2) {
		throw(MAL, "verifyPassword",
			  "password is not %d chars long, is it a hex "
			  "representation of a %s password hash?",
			  digestlength(MONETDB5_PASSWDHASH_TOKEN), MONETDB5_PASSWDHASH);
	}
	len++;						// required in case all the checks above are false
	while (*p != '\0') {
		if (!((*p >= 'a' && *p <= 'z') || isdigit((unsigned char) *p)))
			throw(MAL, "verifyPassword",
				  "password does contain invalid characters, is it a"
				  "lowercase hex representation of a hash?");
		p++;
	}

	return (MAL_SUCCEED);
}

str
AUTHGeneratePasswordHash(str *res, const char *value)
{
	return AUTHcypherValue(res, value);
}
