/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _MAL_AUTHORIZE_H
#define _MAL_AUTHORIZE_H

/* #define _DEBUG_AUTH_*/
#include "mal.h"
#include "mal_instruction.h"
#include "mal_client.h"

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


/*
 * Authorisation is based on a password.  The passwords are stored hashed
 * in a BAT.  Access to this BAT is ok from the MAL level, and in
 * particular SQL needs it to dump (and later restore) users.
 * The database administrator can unlock the BAT that stores the password
 * (the vault) by supplying the master password which is the key for the
 * cypher algorithm used to store the data.  The BAT will never
 * contain the plain hashes, as they will be decyphered on the fly when
 * needed.  A locked vault means noone can log into the system, hence, the
 * vault needs to be unlocked as part of the server startup ritual.
 */
mal_export str AUTHunlockVault(const char *password);

#endif /* _MAL_AUTHORIZE_H */
