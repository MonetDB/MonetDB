/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _MAL_AUTHORIZE_H
#define _MAL_AUTHORIZE_H

/* #define _DEBUG_AUTH_*/
#include "mal.h"
#include "mal_instruction.h"
#include "mal_client.h"

#define MAL_ADMIN (oid) 0

mal_export str AUTHcheckCredentials(oid *ret, Client c, const char *user, const char *passwd, const char *challenge, const char *algo);
mal_export str AUTHaddUser(oid *ret, Client c, const char *user, const char *pass);
mal_export str AUTHremoveUser(Client c, const char *username);
mal_export str AUTHchangeUsername(Client c, const char *olduser, const char *newuser);
mal_export str AUTHchangePassword(Client c, const char *oldpass, const char *passwd);
mal_export str AUTHsetPassword(Client c, const char *username, const char *passwd);
mal_export str AUTHresolveUser(str *ret, oid uid);
mal_export str AUTHgetUsername(str *ret, Client c);
mal_export str AUTHgetUsers(BAT **ret1, BAT **ret2, Client c);
mal_export str AUTHgetPasswordHash(str *ret, Client c, const char *username);

mal_export str AUTHinitTables(const char *passwd);

mal_export str AUTHaddRemoteTableCredentials(const char *local_table, const char *localuser, const char *uri, const char *remoteuser, const char *pass, bool pw_encrypted);
mal_export str AUTHgetRemoteTableCredentials(const char *local_table, str *uri, str *username, str *password);
mal_export str AUTHdeleteRemoteTableCredentials(const char *local_table);

/*
 * Authorisation is based on a password.  The passwords are stored hashed
 * in a BAT.  Access to this BAT is ok from the MAL level, and in
 * particular SQL needs it to dump (and later restore) users.
 * The database administrator can unlock the BAT that stores the password
 * (the vault) by supplying the master password which is the key for the
 * cypher algorithm used to store the data.  The BAT will never
 * contain the plain hashes, as they will be decyphered on the fly when
 * needed.  A locked vault means none can log into the system, hence, the
 * vault needs to be unlocked as part of the server startup ritual.
 */
mal_export str AUTHunlockVault(const char *password);
mal_export str AUTHverifyPassword(const char *passwd);
mal_export str AUTHdecypherValue(str *ret, const char *value);
mal_export str AUTHcypherValue(str *ret, const char *value);
mal_export str AUTHrequireAdmin(Client c);

typedef str (*get_user_password_handler)(Client c, const char *user);
typedef oid (*get_user_oid_handler)(Client c, const char *user);

typedef struct AUTHCallbackCntx {
	get_user_password_handler get_user_password;
	get_user_oid_handler get_user_oid;
} AUTHCallbackCntx;

mal_export str AUTHRegisterGetPasswordHandler(get_user_password_handler callback);
mal_export str AUTHRegisterGetUserOIDHandler(get_user_oid_handler callback);
mal_export str AUTHGeneratePasswordHash(str *res, const char *value);

#endif /* _MAL_AUTHORIZE_H */
