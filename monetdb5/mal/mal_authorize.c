/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifndef HAVE_EMBEDDED
#ifdef HAVE_OPENSSL
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <openssl/ripemd.h>
#else
#ifdef HAVE_COMMONCRYPTO
#define COMMON_DIGEST_FOR_OPENSSL
#include <CommonCrypto/CommonDigest.h>
#endif
#endif
#endif

static str AUTHdecypherValue(str *ret, str *value);
static str AUTHcypherValue(str *ret, str *value);
static str AUTHverifyPassword(str *passwd);

static BAT *user = NULL;
static BAT *pass = NULL;
static BAT *duser = NULL;
/* yep, the vault key is just stored in memory */
static str vaultKey = NULL;

void AUTHreset(void)
{
	//if( user) BBPunfix(user->batCacheid);
	user = NULL;
	//if( pass) BBPunfix(pass->batCacheid);
	pass = NULL;
	//if( duser) BBPunfix(duser->batCacheid);
	duser = NULL;
	if (vaultKey != NULL)
		GDKfree(vaultKey);
	vaultKey = NULL;
}

static BUN
AUTHfindUser(const char *username)
{
	BATiter cni = bat_iterator(user);
	BUN p;

	if (BAThash(user, 0) == GDK_SUCCEED) {
		HASHloop_str(cni, cni.b->thash, p, username) {
			oid pos = p;
			if (BUNfnd(duser, &pos) == BUN_NONE)
				return p;
		}
	} 
	return BUN_NONE;
}

/**
 * Requires the current client to be the admin user thread.  If not the case,
 * this function returns an InvalidCredentialsException.
 */
static str
AUTHrequireAdmin(Client cntxt) {
	oid id;

	if (cntxt == NULL)
		return(MAL_SUCCEED);
	id = cntxt->user;

	if (id != 0) {
		char u[BUFSIZ] = "";
		str user = u;
		str tmp;

		rethrow("requireAdmin", tmp, AUTHresolveUser(&user, &id));
		throw(INVCRED, "requireAdmin", INVCRED_ACCESS_DENIED " '%s'", user);
	}

	return(MAL_SUCCEED);
}

/**
 * Requires the current client to be the admin user, or the user with
 * the given username.  If not the case, this function returns an
 * InvalidCredentialsException.
 */
static str
AUTHrequireAdminOrUser(Client cntxt, str *username) {
	oid id = cntxt->user;
	char u[BUFSIZ] = "";
	str user = u;
	str tmp = MAL_SUCCEED;

	/* root?  then all is well */
	if (id == 0)
		return(MAL_SUCCEED);

	rethrow("requireAdminOrUser", tmp, AUTHresolveUser(&user, &id));
	if (username == NULL || *username == NULL || strcmp(*username, user) != 0) {
		throw(INVCRED, "requireAdminOrUser", INVCRED_ACCESS_DENIED " '%s'", user);
	}

	return(MAL_SUCCEED);
}

static void
AUTHcommit(void)
{
	bat blist[4];

	blist[0] = 0;

	assert(user);
	blist[1] = user->batCacheid;
	assert(pass);
	blist[2] = pass->batCacheid;
	assert(duser);
	blist[3] = duser->batCacheid;
	TMsubcommit_list(blist, 4);
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
AUTHinitTables(str *passwd) {
	bat bid;
	int isNew = 1;
	str msg = MAL_SUCCEED;

	/* skip loading if already loaded */
	if (user != NULL && pass != NULL)
		return(MAL_SUCCEED);

	/* if one is not NULL here, something is seriously screwed up */
	assert (user == NULL);
	assert (pass == NULL);

	/* load/create users BAT */
	bid = BBPindex("M5system_auth_user");
	if (!bid) {
		user = COLnew(0, TYPE_str, 256, PERSISTENT);
		if (user == NULL)
			throw(MAL, "initTables.user", MAL_MALLOC_FAIL " user table");

		BATkey(user, TRUE);
		BBPrename(BBPcacheid(user), "M5system_auth_user");
		BATmode(user, PERSISTENT);
	} else {
		int dbg = GDKdebug;
		/* don't check this bat since we'll fix it below */
		GDKdebug &= ~CHECKMASK;
		user = BATdescriptor(bid);
		GDKdebug = dbg;
		isNew = 0;
	}
	assert(user);

	/* load/create password BAT */
	bid = BBPindex("M5system_auth_passwd_v2");
	if (!bid) {
		pass = COLnew(0, TYPE_str, 256, PERSISTENT);
		if (pass == NULL)
			throw(MAL, "initTables.passwd", MAL_MALLOC_FAIL " password table");

		BBPrename(BBPcacheid(pass), "M5system_auth_passwd_v2");
		BATmode(pass, PERSISTENT);
	} else {
		int dbg = GDKdebug;
		/* don't check this bat since we'll fix it below */
		GDKdebug &= ~CHECKMASK;
		pass = BATdescriptor(bid);
		GDKdebug = dbg;
		isNew = 0;
	}
	assert(pass);

	/* load/create password BAT */
	bid = BBPindex("M5system_auth_deleted");
	if (!bid) {
		duser = COLnew(0, TYPE_oid, 256, PERSISTENT);
		if (duser == NULL)
			throw(MAL, "initTables.duser", MAL_MALLOC_FAIL " deleted user table");

		BBPrename(BBPcacheid(duser), "M5system_auth_deleted");
		BATmode(duser, PERSISTENT);
		if (!isNew)
			AUTHcommit();
	} else {
		duser = BATdescriptor(bid);
		isNew = 0;
	}
	assert(duser);

	if (isNew == 1) {
		/* insert the monetdb/monetdb administrator account on a
		 * complete fresh and new auth tables system */
		str user = "monetdb";
		str pw = "monetdb";
		oid uid;
		Client c = &mal_clients[0];

		if (passwd != NULL && *passwd != NULL)
			pw = *passwd;
		pw = mcrypt_BackendSum(pw, strlen(pw));
		msg = AUTHaddUser(&uid, c, &user, &pw);
		free(pw);
		if (msg)
			return msg;
		if (uid != 0)
			throw(MAL, "initTables", INTERNAL_AUTHORIZATION " while they were just created!");
		/* normally, we'd commit here, but it's done already in AUTHaddUser */
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
		str *username,
		str *passwd,
		str *challenge,
		str *algo)
{
	str tmp;
	str pwd = NULL;
	str hash = NULL;
	BUN p;
	BATiter passi;

	rethrow("checkCredentials", tmp, AUTHrequireAdminOrUser(cntxt, username));
	assert(user);
	assert(pass);

	if (*username == NULL || strNil(*username))
		throw(INVCRED, "checkCredentials", "invalid credentials for unknown user");

	p = AUTHfindUser(*username);
	if (p == BUN_NONE) {
		/* DO NOT reveal that the user doesn't exist here! */
		throw(INVCRED, "checkCredentials", INVCRED_INVALID_USER " '%s'", *username);
	}

	/* a NULL password is impossible (since we should be dealing with
	 * hashes here) so we can bail out immediately
	 */
	if (*passwd == NULL || strNil(*passwd)) {
		/* DO NOT reveal that the password is NULL here! */
		throw(INVCRED, "checkCredentials", INVCRED_INVALID_USER " '%s'", *username);
	}

	/* find the corresponding password to the user */
	passi = bat_iterator(pass);
	tmp = (str)BUNtail(passi, p);
	assert (tmp != NULL);
	/* decypher the password (we lose the original tmp here) */
	rethrow("checkCredentials", tmp, AUTHdecypherValue(&pwd, &tmp));
	/* generate the hash as the client should have done */
	hash = mcrypt_hashPassword(*algo, pwd, *challenge);
	GDKfree(pwd);
	/* and now we have it, compare it to what was given to us */
	if (strcmp(*passwd, hash) != 0) {
		/* of course we DO NOT print the password here */
		free(hash);
		throw(INVCRED, "checkCredentials", INVCRED_INVALID_USER " '%s'", *username);
	}
	free(hash);

	*uid = p;
	return(MAL_SUCCEED);
}

/**
 * Adds the given user with password to the administration.  The
 * return value of this function is the user id of the added user.
 */
str
AUTHaddUser(oid *uid, Client cntxt, str *username, str *passwd) 
{
	BUN p;
	str tmp;
	str hash = NULL;

	rethrow("addUser", tmp, AUTHrequireAdmin(cntxt));
	assert(user);
	assert(pass);

	/* some pre-condition checks */
	if (*username == NULL || strNil(*username))
		throw(ILLARG, "addUser", "username should not be nil");
	if (*passwd == NULL || strNil(*passwd))
		throw(ILLARG, "addUser", "password should not be nil");
	rethrow("addUser", tmp, AUTHverifyPassword(passwd));

	/* ensure that the username is not already there */
	p = AUTHfindUser(*username);
	if (p != BUN_NONE)
		throw(MAL, "addUser", "user '%s' already exists", *username);

	/* we assume the BATs are still aligned */
	rethrow("addUser", tmp, AUTHcypherValue(&hash, passwd));
	/* needs force, as SQL makes a view over user */
	BUNappend(user, *username, TRUE);
	BUNappend(pass, hash, TRUE);
	GDKfree(hash);
	/* retrieve the oid of the just inserted user */
	p = AUTHfindUser(*username);

	/* make the stuff persistent */
	AUTHcommit();

	*uid = p;
	return(MAL_SUCCEED);
}

/**
 * Removes the given user from the administration.
 */
str
AUTHremoveUser(Client cntxt, str *username) 
{
	BUN p;
	oid id;
	str tmp;

	rethrow("removeUser", tmp, AUTHrequireAdmin(cntxt));
	assert(user);
	assert(pass);

	/* pre-condition check */
	if (*username == NULL || strNil(*username))
		throw(ILLARG, "removeUser", "username should not be nil");

	/* ensure that the username exists */
	p = AUTHfindUser(*username);
	if (p == BUN_NONE)
		throw(MAL, "removeUser", "no such user: '%s'", *username);
	id = p;

	/* find the name of the administrator and see if it equals username */
	if (id == cntxt->user)
		throw(MAL, "removeUser", "cannot remove yourself");

	/* now, we got the oid, start removing the related tuples */
	BUNappend(duser, &id, TRUE);

	/* make the stuff persistent */
	AUTHcommit();
	return(MAL_SUCCEED);
}

/**
 * Changes the username of the user indicated by olduser into newuser.
 * If the username is already in use, an exception is thrown and nothing
 * is modified.
 */
str
AUTHchangeUsername(Client cntxt, str *olduser, str *newuser)
{
	BUN p, q;
	str tmp;

	rethrow("addUser", tmp, AUTHrequireAdminOrUser(cntxt, olduser));

	/* precondition checks */
	if (*olduser == NULL || strNil(*olduser))
		throw(ILLARG, "changeUsername", "old username should not be nil");
	if (*newuser == NULL || strNil(*newuser))
		throw(ILLARG, "changeUsername", "new username should not be nil");

	/* see if the olduser is valid */
	p = AUTHfindUser(*olduser);
	if (p == BUN_NONE)
		throw(MAL, "changeUsername", "user '%s' does not exist", *olduser);
	/* ... and if the newuser is not there yet */
	q = AUTHfindUser(*newuser);
	if (q != BUN_NONE)
		throw(MAL, "changeUsername", "user '%s' already exists", *newuser);

	/* ok, just do it! (with force, because sql makes view over it) */
	BUNinplace(user, p, *newuser, TRUE);
	AUTHcommit();
	return(MAL_SUCCEED);
}

/**
 * Changes the password of the current user to the given password.  The
 * old password must match the one stored before the new password is
 * set.
 */
str
AUTHchangePassword(Client cntxt, str *oldpass, str *passwd) 
{
	BUN p;
	str tmp= NULL;
	str hash= NULL;
	oid id;
	BATiter passi;
	str msg= MAL_SUCCEED;

	/* precondition checks */
	if (*oldpass == NULL || strNil(*oldpass))
		throw(ILLARG, "changePassword", "old password should not be nil");
	if (*passwd == NULL || strNil(*passwd))
		throw(ILLARG, "changePassword", "password should not be nil");
	rethrow("changePassword", tmp, AUTHverifyPassword(passwd));

	/* check the old password */
	id = cntxt->user;
	p = id;
	assert(p != BUN_NONE);
	passi = bat_iterator(pass);
	tmp = BUNtail(passi, p);
	assert (tmp != NULL);
	/* decypher the password */
	msg = AUTHdecypherValue(&hash, &tmp);
	if (msg)
		return msg;
	if (strcmp(hash, *oldpass) != 0){
		GDKfree(hash);
		throw(INVCRED, "changePassword", "Access denied");
	}

	GDKfree(hash);
	/* cypher the password */
	msg = AUTHcypherValue(&hash, passwd);
	if (msg)
		return msg;

	/* ok, just overwrite the password field for this user */
	assert(id == p);
	BUNinplace(pass, p, hash, TRUE);
	GDKfree(hash);
	AUTHcommit();
	return(MAL_SUCCEED);
}

/**
 * Changes the password of the given user to the given password.  This
 * function can be used by the administrator to reset the password for a
 * user.  Note that for the administrator to change its own password, it
 * cannot use this function for obvious reasons.
 */
str
AUTHsetPassword(Client cntxt, str *username, str *passwd) 
{
	BUN p;
	str tmp;
	str hash = NULL;
	oid id;
	BATiter useri;

	rethrow("setPassword", tmp, AUTHrequireAdmin(cntxt));

	/* precondition checks */
	if (*username == NULL || strNil(*username))
		throw(ILLARG, "setPassword", "username should not be nil");
	if (*passwd == NULL || strNil(*passwd))
		throw(ILLARG, "setPassword", "password should not be nil");
	rethrow("setPassword", tmp, AUTHverifyPassword(passwd));

	id = cntxt->user;
	/* find the name of the administrator and see if it equals username */
	p = id;
	assert (p != BUN_NONE);
	useri = bat_iterator(user);
	tmp = BUNtail(useri, p);
	assert (tmp != NULL);
	if (strcmp(tmp, *username) == 0)
		throw(INVCRED, "setPassword", "The administrator cannot set its own password, use changePassword instead");

	/* see if the user is valid */
	p = AUTHfindUser(*username);
	if (p == BUN_NONE)
		throw(MAL, "setPassword", "no such user '%s'", *username);
	id = p;

	/* cypher the password */
	rethrow("setPassword", tmp, AUTHcypherValue(&hash, passwd));
	/* ok, just overwrite the password field for this user */
	assert (p != BUN_NONE);
	assert(id == p);
	BUNinplace(pass, p, hash, TRUE);
	GDKfree(hash);
	AUTHcommit();
	return(MAL_SUCCEED);
}

/**
 * Resolves the given user id and returns the associated username.  If
 * the id is invalid, an exception is thrown.  The given pointer to the
 * username char buffer should be NULL if this function is supposed to
 * allocate memory for it.  If the pointer is pointing to an already
 * allocated buffer, it is supposed to be of size BUFSIZ.
 */
str
AUTHresolveUser(str *username, oid *uid)
{
	BUN p;
	BATiter useri;

	if (uid == NULL || *uid == oid_nil || (p = (BUN) *uid) >= BATcount(user))
		throw(ILLARG, "resolveUser", "userid should not be nil");

	assert (username != NULL);
	useri = bat_iterator(user);
	if (*username == NULL) {
		if ((*username = GDKstrdup((str)(BUNtail(useri, p)))) == NULL)
			throw(MAL, "resolveUser", MAL_MALLOC_FAIL);
	} else {
		snprintf(*username, BUFSIZ, "%s", (str)(BUNtail(useri, p)));
	}
	return(MAL_SUCCEED);
}

/**
 * Returns the username of the given client.
 */
str
AUTHgetUsername(str *username, Client cntxt) 
{
	BUN p;
	BATiter useri;

	p = (BUN) cntxt->user;

	/* If you ask for a username using a client struct, and that user
	 * doesn't exist, you seriously screwed up somehow.  If this
	 * happens, it may be a security breach/attempt, and hence
	 * terminating the entire system seems like the right thing to do to
	 * me. */
	if (p == BUN_NONE || p >= BATcount(user))
		GDKfatal("Internal error: user id that doesn't exist: " OIDFMT, cntxt->user);

	useri = bat_iterator(user);
	if ((*username = GDKstrdup( BUNtail(useri, p))) == NULL)
		throw(MAL, "getUsername", MAL_MALLOC_FAIL);
	return(MAL_SUCCEED);
}

/**
 * Returns a BAT with user names in the tail, and user ids in the head.
 */
str
AUTHgetUsers(BAT **ret1, BAT **ret2, Client cntxt)
{
	BAT *bn;
	str tmp;

	rethrow("getUsers", tmp, AUTHrequireAdmin(cntxt));

	*ret1 = BATdense(user->hseqbase, user->hseqbase, BATcount(user));
	if (BATcount(duser)) {
		bn = BATdiff(*ret1, duser, NULL, NULL, 0, BUN_NONE);
		BBPunfix((*ret1)->batCacheid);
		*ret2 = BATproject(bn, user);
		*ret1 = bn;
	} else {
		*ret2 = COLcopy(user, user->ttype, FALSE, TRANSIENT);
	}
	return(NULL);
}

/**
 * Returns the password hash as used by the backend for the given
 * username.  Throws an exception if called by a non-superuser.
 */
str
AUTHgetPasswordHash(str *ret, Client cntxt, str *username) 
{
	BUN p;
	BATiter i;
	str tmp;
	str passwd = NULL;

	rethrow("getPasswordHash", tmp, AUTHrequireAdmin(cntxt));

	if (*username == NULL || strNil(*username))
		throw(ILLARG, "getPasswordHash", "username should not be nil");

	p = AUTHfindUser(*username);
	if (p == BUN_NONE)
		throw(MAL, "getPasswordHash", "user '%s' does not exist", *username);
	i = bat_iterator(user);
	assert(p != BUN_NONE);
	i = bat_iterator(pass);
	tmp = BUNtail(i, p);
	assert (tmp != NULL);
	/* decypher the password */
	rethrow("changePassword", tmp, AUTHdecypherValue(&passwd, &tmp));

	*ret = passwd;
	return(NULL);
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
AUTHunlockVault(str *password) 
{
	if (password == NULL || strNil(*password))
		throw(ILLARG, "unlockVault", "password should not be nil");

	/* even though I think this function should be called only once, it
	 * is not of real extra efforts to avoid a mem-leak if it is used
	 * multiple times */
	if (vaultKey != NULL)
		GDKfree(vaultKey);

	if ((vaultKey = GDKstrdup(*password)) == NULL)
		throw(MAL, "unlockVault", MAL_MALLOC_FAIL " vault key");
	return(MAL_SUCCEED);
}

/**
 * Decyphers a given value, using the vaultKey.  The returned value
 * might be incorrect if the vaultKey is incorrect or unset.  If the
 * cypher algorithm fails or detects an invalid password, it might throw
 * an exception.  The ret string is GDKmalloced, and should be GDKfreed
 * by the caller.
 */
static str
AUTHdecypherValue(str *ret, str *value) 
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
	str s = *value;
	char t = '\0';
	int escaped = 0;
	/* we default to some garbage key, just to make password unreadable
	 * (a space would only uppercase the password) */
	int keylen = 0;

	if (vaultKey == NULL)
		throw(MAL, "decypherValue", "The vault is still locked!");
	w = r = GDKmalloc(sizeof(char) * (strlen(*value) + 1));
	if( r == NULL)
		throw(MAL, "decypherValue", MAL_MALLOC_FAIL);

	keylen = (int) strlen(vaultKey);

	/* XOR all characters.  If we encounter a 'one' char after the XOR
	 * operation, it is an escape, so replace it with the next char. */
	for (; (t = *s) != '\0'; s++) {
		if (t == '\1' && escaped == 0) {
			escaped = 1;
			continue;
		} else if (escaped != 0) {
			t -= 1;
			escaped = 0;
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
static str
AUTHcypherValue(str *ret, str *value) 
{
	/* this is the XOR cypher implementation */
	str r, w;
	str s = *value;
	/* we default to some garbage key, just to make password unreadable
	 * (a space would only uppercase the password) */
	int keylen = 0;

	if (vaultKey == NULL)
		throw(MAL, "cypherValue", "The vault is still locked!");
	w = r = GDKmalloc(sizeof(char) * (strlen(*value) * 2 + 1));
	if( r == NULL)
		throw(MAL, "cypherValue", MAL_MALLOC_FAIL);

	keylen = (int) strlen(vaultKey);

	/* XOR all characters.  If we encounter a 'zero' char after the XOR
	 * operation, escape it with an 'one' char. */
	for (; *s != '\0'; s++) {
		*w = *s ^ vaultKey[(s - *value) % keylen];
		if (*w == '\0') {
			*w++ = '\1';
			*w = '\1';
		} else if (*w == '\1') {
			*w++ = '\1';
			*w = '\2';
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
static str
AUTHverifyPassword(str *passwd) 
{
#if !defined(HAVE_EMBEDDED) && (defined(HAVE_OPENSSL) || defined(HAVE_COMMONCRYPTO))
	char *p = *passwd;
	size_t len = strlen(p);

	if (len != digestlength(MONETDB5_PASSWDHASH_TOKEN) * 2) {
		throw(MAL, "verifyPassword",
			  "password is not %d chars long, is it a hex "
			  "representation of a %s password hash?",
			  digestlength(MONETDB5_PASSWDHASH_TOKEN), MONETDB5_PASSWDHASH);
	}
	len++; // required in case all the checks above are false
	while (*p != '\0') {
		if (!((*p >= 'a' && *p <= 'z') || (*p >= '0' && *p <= '9')))
			throw(MAL, "verifyPassword",
					"password does contain invalid characters, is it a"
					"lowercase hex representation of a hash?");
		p++;
	}

	return(MAL_SUCCEED);
#else
	(void) passwd;
	throw(MAL, "verifyPassword", "Unknown backend hash algorithm: %s",
		  MONETDB5_PASSWDHASH);
#endif
}
