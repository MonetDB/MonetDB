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

static BUN lookupRemoteTableKey(const char *key);

/* Remote table bats */
static BAT *rt_key = NULL;
static BAT *rt_uri = NULL;
static BAT *rt_remoteuser = NULL;
static BAT *rt_hashedpwd = NULL;
static BAT *rt_deleted = NULL;
/* yep, the vault key is just stored in memory */
static str vaultKey = NULL;
static str master_password = NULL;
static AUTHCallbackCntx authCallbackCntx = {
	.get_user_name = NULL,
	.get_user_password = NULL,
	.get_user_oid = NULL
};

void AUTHreset(void)
{
	GDKfree(vaultKey);
	vaultKey = NULL;
}

/**
 * Requires the current client to be the admin user thread. If not the case,
 * this function returns an InvalidCredentialsException.
 */
str
AUTHrequireAdmin(Client cntxt) {
	assert(cntxt);

	if (cntxt->user != MAL_ADMIN)
		throw(MAL, "AUTHrequireAdmin", INVCRED_ACCESS_DENIED);
	return(MAL_SUCCEED);
}

static void
AUTHcommit(void)
{
	bat blist[6];

	blist[0] = 0;

	assert(rt_key);
	blist[1] = rt_key->batCacheid;
	assert(rt_uri);
	blist[2] = rt_uri->batCacheid;
	assert(rt_remoteuser);
	blist[3] = rt_remoteuser->batCacheid;
	assert(rt_hashedpwd);
	blist[4] = rt_hashedpwd->batCacheid;
	assert(rt_deleted);
	blist[5] = rt_deleted->batCacheid;
	TMsubcommit_list(blist, NULL, 6, getBBPlogno(), getBBPtransid());
}

/*
 * Localize the authorization tables in the database.  The authorization
 * tables are a set of aligned BATs that store username, password (hashed)
 * and scenario permissions.
 * If the BATs do not exist, they are created, and the monetdb
 * administrator account is added with the given password (or 'monetdb'
 * if NULL).  Initialising the authorization tables can only be done
 * after the GDK kernel has been initialized.
 */
str
AUTHinitTables(void) {
	bat bid;
	int isNew = 1;
	str msg = MAL_SUCCEED;

	/* skip loading if already loaded */
	if (rt_key != NULL && rt_deleted != NULL)
		return(MAL_SUCCEED);

	/* Remote table authorization table.
	 *
	 * This table holds the remote tabe authorization credentials
	 * (uri, username and hashed password).
	 */
	/* load/create remote table URI BAT */
	bid = BBPindex("M5system_auth_rt_key");
	if (!bid) {
		rt_key = COLnew(0, TYPE_str, 256, PERSISTENT);
		if (rt_key == NULL)
			throw(MAL, "initTables.rt_key", SQLSTATE(HY013) MAL_MALLOC_FAIL " remote table key bat");

		if (BBPrename(rt_key, "M5system_auth_rt_key") != 0 ||
			BATmode(rt_key, false) != GDK_SUCCEED)
			throw(MAL, "initTables.rt_key", GDK_EXCEPTION);
	}
	else {
		int dbg = GDKdebug;
		/* don't check this bat since we'll fix it below */
		GDKdebug &= ~CHECKMASK;
		rt_key = BATdescriptor(bid);
		GDKdebug = dbg;
		if (rt_key == NULL) {
			throw(MAL, "initTables.rt_key", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		isNew = 0;
	}
	assert(rt_key);

	/* load/create remote table URI BAT */
	bid = BBPindex("M5system_auth_rt_uri");
	if (!bid) {
		rt_uri = COLnew(0, TYPE_str, 256, PERSISTENT);
		if (rt_uri == NULL)
			throw(MAL, "initTables.rt_uri", SQLSTATE(HY013) MAL_MALLOC_FAIL " remote table uri bat");

		if (BBPrename(rt_uri, "M5system_auth_rt_uri") != 0 ||
			BATmode(rt_uri, false) != GDK_SUCCEED)
			throw(MAL, "initTables.rt_uri", GDK_EXCEPTION);
	}
	else {
		int dbg = GDKdebug;
		/* don't check this bat since we'll fix it below */
		GDKdebug &= ~CHECKMASK;
		rt_uri = BATdescriptor(bid);
		GDKdebug = dbg;
		if (rt_uri == NULL) {
			throw(MAL, "initTables.rt_uri", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		isNew = 0;
	}
	assert(rt_uri);

	/* load/create remote table remote user name BAT */
	bid = BBPindex("M5system_auth_rt_remoteuser");
	if (!bid) {
		rt_remoteuser = COLnew(0, TYPE_str, 256, PERSISTENT);
		if (rt_remoteuser == NULL)
			throw(MAL, "initTables.rt_remoteuser", SQLSTATE(HY013) MAL_MALLOC_FAIL " remote table local user bat");

		if (BBPrename(rt_remoteuser, "M5system_auth_rt_remoteuser") != 0 ||
			BATmode(rt_remoteuser, false) != GDK_SUCCEED)
			throw(MAL, "initTables.rt_remoteuser", GDK_EXCEPTION);
	}
	else {
		int dbg = GDKdebug;
		/* don't check this bat since we'll fix it below */
		GDKdebug &= ~CHECKMASK;
		rt_remoteuser = BATdescriptor(bid);
		GDKdebug = dbg;
		if (rt_remoteuser == NULL) {
			throw(MAL, "initTables.rt_remoteuser", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		isNew = 0;
	}
	assert(rt_remoteuser);

	/* load/create remote table password BAT */
	bid = BBPindex("M5system_auth_rt_hashedpwd");
	if (!bid) {
		rt_hashedpwd = COLnew(0, TYPE_str, 256, PERSISTENT);
		if (rt_hashedpwd == NULL)
			throw(MAL, "initTables.rt_hashedpwd", SQLSTATE(HY013) MAL_MALLOC_FAIL " remote table local user bat");

		if (BBPrename(rt_hashedpwd, "M5system_auth_rt_hashedpwd") != 0 ||
			BATmode(rt_hashedpwd, false) != GDK_SUCCEED)
			throw(MAL, "initTables.rt_hashedpwd", GDK_EXCEPTION);
	}
	else {
		int dbg = GDKdebug;
		/* don't check this bat since we'll fix it below */
		GDKdebug &= ~CHECKMASK;
		rt_hashedpwd = BATdescriptor(bid);
		GDKdebug = dbg;
		if (rt_hashedpwd == NULL) {
			throw(MAL, "initTables.rt_hashedpwd", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		isNew = 0;
	}
	assert(rt_hashedpwd);

	/* load/create remote table deleted entries BAT */
	bid = BBPindex("M5system_auth_rt_deleted");
	if (!bid) {
		rt_deleted = COLnew(0, TYPE_oid, 256, PERSISTENT);
		if (rt_deleted == NULL)
			throw(MAL, "initTables.rt_deleted", SQLSTATE(HY013) MAL_MALLOC_FAIL " remote table local user bat");

		if (BBPrename(rt_deleted, "M5system_auth_rt_deleted") != 0 ||
			BATmode(rt_deleted, false) != GDK_SUCCEED)
			throw(MAL, "initTables.rt_deleted", GDK_EXCEPTION);
		/* If the database is not new, but we just created this BAT,
		 * write everything to disc. This needs to happen only after
		 * the last BAT of the vault has been created.
		 */
		if (!isNew)
			AUTHcommit();
	}
	else {
		rt_deleted = BATdescriptor(bid);
		if (rt_deleted == NULL) {
			throw(MAL, "initTables.rt_deleted", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		isNew = 0;
	}
	assert(rt_deleted);

	if (!GDKinmemory(0) && !GDKembedded()) {
		free(master_password);
		master_password = NULL;
		msg = msab_pickSecret(&master_password);
		if (msg != NULL) {
			char *nmsg = createException(MAL, "initTables", "%s", msg);
			free(msg);
			return nmsg;
		}
	}

	return(MAL_SUCCEED);
}

/**
 * Checks the credentials supplied and throws an exception if invalid.
 * The user id of the authenticated user is returned upon success.
 */
str
AUTHcheckCredentials(
		oid *uid,
		Client cntxt,
		const char *username,
		const char *passwd,
		const char *challenge,
		const char *algo)
{
	str tmp;
	str pwd = NULL;
	str hash = NULL;
	oid p = oid_nil;
	str passValue = NULL;

	if (strNil(username))
		throw(INVCRED, "checkCredentials", "invalid credentials for unknown user");

	// is this check needed?
	//if (cntxt)
	//	rethrow("checkCredentials", tmp, AUTHrequireAdminOrUser(cntxt, username));

	if (authCallbackCntx.get_user_oid && cntxt) {
		if ((p = authCallbackCntx.get_user_oid(cntxt, username)) == oid_nil) {
			/* DO NOT reveal that the user doesn't exist here! */
			throw(INVCRED, "checkCredentials", INVCRED_INVALID_USER " '%s'", username);
		}
	}

	/* a NULL password is impossible (since we should be dealing with
	 * hashes here) so we can bail out immediately
	 */
	if (strNil(passwd)) {
		/* DO NOT reveal that the password is NULL here! */
		throw(INVCRED, "checkCredentials", INVCRED_INVALID_USER " '%s'", username);
	}

	// load password from users tbl
	if (authCallbackCntx.get_user_password && cntxt)
		passValue = authCallbackCntx.get_user_password(cntxt, username);

	if (strNil(passValue)) {
		throw(INVCRED, "checkCredentials", INVCRED_INVALID_USER " '%s'", username);
	}

	/* find the corresponding password to the user */
	/* decypher the password (we lose the original tmp here) */
	tmp = AUTHdecypherValue(&pwd, passValue);
	GDKfree(passValue);
	if (tmp)
		return tmp;

	/* generate the hash as the client should have done */
	hash = mcrypt_hashPassword(algo, pwd, challenge);
	GDKfree(pwd);
	if(!hash)
		throw(MAL, "checkCredentials", "hash '%s' backend not found", algo);
	/* and now we have it, compare it to what was given to us */
	if (strcmp(passwd, hash) == 0) {
		*uid = p;
		free(hash);
		return(MAL_SUCCEED);
	}
	free(hash);

	/* special case: users whose name starts with '.' can authenticate using
	 * the temporary master password.
	 */
	if (username[0] == '.' && master_password != NULL && master_password[0] != '\0') {
		// first encrypt the master password as if we've just found it
		// in the password store
		str encrypted = mcrypt_BackendSum(master_password, strlen(master_password));
		if (encrypted == NULL)
			throw(MAL, "checkCredentials", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		hash = mcrypt_hashPassword(algo, encrypted, challenge);
		free(encrypted);
		if (hash && strcmp(passwd, hash) == 0) {
			*uid = p;
			free(hash);
			return(MAL_SUCCEED);
		}
		free(hash);
	}

	/* of course we DO NOT print the password here */
	throw(INVCRED, "checkCredentials", INVCRED_INVALID_USER " '%s'", username);
}

/**
 * Returns the username of the given client.
 */
str
AUTHgetUsername(str *username, Client cntxt)
{
	if (cntxt->username) {
		*username = GDKstrdup(cntxt->username);
		return(MAL_SUCCEED);
	}
	if (authCallbackCntx.get_user_name) {
		if ((*username = authCallbackCntx.get_user_name(cntxt)) == NULL) {
			throw(MAL, "getUsername", INVCRED_WRONG_ID);
		}
	}
	return(MAL_SUCCEED);
}

/**
 * Returns the password hash as used by the backend for the given
 * username. Throws an exception if called by a non-superuser.
 */
str
AUTHgetPasswordHash(str *ret, Client cntxt, const char *username)
{
	str tmp;
	str msg;
	str passwd = NULL;

	rethrow("getPasswordHash", tmp, AUTHrequireAdmin(cntxt));

	if (strNil(username))
		throw(ILLARG, "getPasswordHash", "username should not be nil");

	// load password from users tbl
	if (authCallbackCntx.get_user_password && cntxt)
		tmp = authCallbackCntx.get_user_password(cntxt, username);

	if (strNil(tmp)) {
		throw(MAL, "getPasswordHash", "user '%s' does not exist", username);
	}
	/* decypher the password */
	if ((msg = AUTHdecypherValue(&passwd, tmp)) != MAL_SUCCEED) {
		GDKfree(tmp);
		return msg;
	}

	if(tmp)
		GDKfree(tmp);

	*ret = passwd;
	return(MAL_SUCCEED);
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
	GDKfree(vaultKey);

	if ((vaultKey = GDKstrdup(password)) == NULL)
		throw(MAL, "unlockVault", SQLSTATE(HY013) MAL_MALLOC_FAIL " vault key");
	return(MAL_SUCCEED);
}

/**
 * Decyphers a given value, using the vaultKey.  The returned value
 * might be incorrect if the vaultKey is incorrect or unset.  If the
 * cypher algorithm fails or detects an invalid password, it might throw
 * an exception.  The ret string is GDKmalloced, and should be GDKfreed
 * by the caller.
 */
str
AUTHdecypherValue(str *ret, const char *value)
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
	if( r == NULL)
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
	return(MAL_SUCCEED);
}

/**
 * Cyphers the given string using the vaultKey.  If the cypher algorithm
 * fails or detects an invalid password, it might throw an exception.
 * The ret string is GDKmalloced, and should be GDKfreed by the caller.
 */
str
AUTHcypherValue(str *ret, const char *value)
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
	if( r == NULL)
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
	return(MAL_SUCCEED);
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
	len++; // required in case all the checks above are false
	while (*p != '\0') {
		if (!((*p >= 'a' && *p <= 'z') || isdigit((unsigned char) *p)))
			throw(MAL, "verifyPassword",
					"password does contain invalid characters, is it a"
					"lowercase hex representation of a hash?");
		p++;
	}

	return(MAL_SUCCEED);
}

static BUN
lookupRemoteTableKey(const char *key)
{
	BATiter cni = bat_iterator(rt_key);
	BUN p = BUN_NONE;

	assert(rt_key);
	assert(rt_deleted);

	if (BAThash(rt_key) == GDK_SUCCEED) {
		MT_rwlock_rdlock(&cni.b->thashlock);
		HASHloop_str(cni, cni.b->thash, p, key) {
			oid pos = p;
			if (BUNfnd(rt_deleted, &pos) == BUN_NONE) {
				MT_rwlock_rdunlock(&cni.b->thashlock);
				bat_iterator_end(&cni);
				return p;
			}
		}
		MT_rwlock_rdunlock(&cni.b->thashlock);
	}
	bat_iterator_end(&cni);

	return BUN_NONE;

}

str
AUTHgetRemoteTableCredentials(const char *local_table, str *uri, str *username, str *password)
{
	BUN p;
	BATiter i;
	str tmp, pwhash;

	*uri = NULL;
	*username = NULL;
	*password = NULL;

	if (strNil(local_table))
		throw(ILLARG, "getRemoteTableCredentials", "local table should not be nil");

	p = lookupRemoteTableKey(local_table);
	if (p == BUN_NONE) {
		// No credentials for remote table with name local_table.
		return MAL_SUCCEED;
	}

	assert(rt_key);
	assert(rt_uri);
	assert(rt_remoteuser);
	assert(rt_hashedpwd);

	assert(p != BUN_NONE);
	i = bat_iterator(rt_uri);
	*uri = GDKstrdup(BUNtvar(i, p));
	bat_iterator_end(&i);

	i = bat_iterator(rt_remoteuser);
	*username = GDKstrdup(BUNtvar(i, p));
	bat_iterator_end(&i);

	if (!*uri || !*username) {
		GDKfree(*uri);
		GDKfree(*username);
		*uri = NULL;
		*username = NULL;
		throw(MAL, "getRemoteTableCredentials", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	i = bat_iterator(rt_hashedpwd);
	tmp = BUNtvar(i, p);
	tmp = AUTHdecypherValue(&pwhash, tmp);
	bat_iterator_end(&i);
	if (tmp) {
		GDKfree(*uri);
		GDKfree(*username);
		*uri = NULL;
		*username = NULL;
		return tmp;
	}

	*password = pwhash;

	return MAL_SUCCEED;
}

str
AUTHaddRemoteTableCredentials(const char *local_table, const char *local_user, const char *uri, const char *remoteuser, const char *pass, bool pw_encrypted)
{
	char *pwhash = NULL;
	bool free_pw = false;
	str output = MAL_SUCCEED;
	BUN p;
	str msg = MAL_SUCCEED;

	if (strNil(uri))
		throw(ILLARG, "addRemoteTableCredentials", "URI cannot be nil");
	if (strNil(local_user))
		throw(ILLARG, "addRemoteTableCredentials", "local user name cannot be nil");

	assert(rt_key);
	assert(rt_uri);
	assert(rt_remoteuser);
	assert(rt_hashedpwd);

	p = lookupRemoteTableKey(local_table);

	if (p != BUN_NONE) {
	/* An entry with the given key is already in the vault (note: the
	 * key is the string "schema.table_name", which is unique in the
	 * SQL catalog, in the sense that no two tables can have the same
	 * name in the same schema). This can only mean that the entry is
	 * invalid (i.e. it does not correspond to a valid SQL table). To
	 * see this consider the following:
	 *
	 * 1. The function `AUTHaddRemoteTableCredentials` is only called
	 * from `rel_create_table` (also from the upgrade code, but this
	 * is irrelevant for our discussion since, in this case no remote
	 * table will have any credentials), i.e. when we are creating a
	 * (remote) table.
	 *
	 * 2. If a remote table with name "schema.table_name" has been
	 * defined previously (i.e there is already a SQL catalog entry
	 * for it) and we try to define it again,
	 * `AUTHaddRemoteTableCredentials` will *not* be called because we
	 * are trying to define an already existing table, and the SQL
	 * layer will not allow us to continue.
	 *
	 * 3. The only way to add an entry in the vault is calling this
	 * function.
	 *
	 * Accepting (1)-(3) above means that just before
	 * `AUTHaddRemoteTableCredentials` gets called with an argument
	 * "schema.table_name", that table does not exist in the SQL
	 * catalog.
	 *
	 * This means that if we call `AUTHaddRemoteTableCredentials` with
	 * argument "schema.table_name" and find an entry with that key
	 * already in the vault, we can safely overwrite it, because the
	 * table it refers to does not exist in the SQL catalog. We can
	 * also conclude that the previous entry was added in the vault as
	 * part of a CREATE REMOTE TABLE call (conclusion follows from (1)
	 * and (3)), that did not create a corresponding entry in the SQL
	 * catalog (conclusion follows from (2)). The only (valid) way for
	 * this to happen is if the CREATE REMOTE TABLE call was inside a
	 * transaction that did not succeed.
	 *
	 * Implementation note: we first delete the entry and then add a
	 * new entry with the same key.
	 */
		if((output = AUTHdeleteRemoteTableCredentials(local_table)) != MAL_SUCCEED)
			return output;
	}

	assert(pass);
	free_pw = true;
	if (pw_encrypted) {
		if((pwhash = strdup(pass)) == NULL)
			throw(MAL, "addRemoteTableCredentials", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else {
		/* Note: the remote server might have used a different
		 * algorithm to hash the pwhash.
		 */
		if((pwhash = mcrypt_BackendSum(pass, strlen(pass))) == NULL)
			throw(MAL, "addRemoteTableCredentials", SQLSTATE(42000) "Crypt backend hash not found");
	}
	msg = AUTHverifyPassword(pwhash);
	if( msg != MAL_SUCCEED){
		free(pwhash);
		return msg;
	}

	str cypher;
	msg = AUTHcypherValue(&cypher, pwhash);
	if( msg != MAL_SUCCEED){
		free(pwhash);
		return msg;
	}

	/* Add entry */
	bool table_entry = (BUNappend(rt_key, local_table, true) == GDK_SUCCEED &&
						BUNappend(rt_uri, uri, true) == GDK_SUCCEED &&
						BUNappend(rt_remoteuser, remoteuser, true) == GDK_SUCCEED &&
						BUNappend(rt_hashedpwd, cypher, true) == GDK_SUCCEED);

	if (!table_entry) {
		if (free_pw) {
			free(pwhash);
		}
		else {
			GDKfree(pwhash);
		}
		GDKfree(cypher);
		throw(MAL, "addRemoteTableCredentials", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	AUTHcommit();

	if (free_pw) {
		free(pwhash);
	}
	else {
		GDKfree(pwhash);
	}
	GDKfree(cypher);
	return MAL_SUCCEED;
}

str
AUTHdeleteRemoteTableCredentials(const char *local_table)
{
	BUN p;
	oid id;

	assert(rt_key);
	assert(rt_uri);
	assert(rt_remoteuser);
	assert(rt_hashedpwd);

	/* pre-condition check */
	if (strNil(local_table))
		throw(ILLARG, "deleteRemoteTableCredentials", "local table cannot be nil");

	/* ensure that the username exists */
	p = lookupRemoteTableKey(local_table);
	if (p == BUN_NONE)
		throw(MAL, "deleteRemoteTableCredentials", "no such table: '%s'", local_table);
	id = p;

	/* now, we got the oid, start removing the related tuples */
	if (BUNappend(rt_deleted, &id, true) != GDK_SUCCEED)
		throw(MAL, "deleteRemoteTableCredentials", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* make the stuff persistent */
	AUTHcommit();
	return(MAL_SUCCEED);
}


str
AUTHRegisterGetUserNameHandler(get_user_name_handler callback)
{
	authCallbackCntx.get_user_name = callback;
	return MAL_SUCCEED;
}


str
AUTHRegisterGetPasswordHandler(get_user_password_handler callback)
{
	authCallbackCntx.get_user_password = callback;
	return MAL_SUCCEED;
}


str
AUTHRegisterGetUserOIDHandler(get_user_oid_handler callback)
{
	authCallbackCntx.get_user_oid = callback;
	return MAL_SUCCEED;
}

str
AUTHGeneratePasswordHash(str *res, const char *value)
{
	return AUTHcypherValue(res, value);
}
