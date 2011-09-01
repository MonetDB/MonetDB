/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _MAL_AUTHORIZE_H
#define _MAL_AUTHORIZE_H

/* #define _DEBUG_AUTH_*/
#include "mal.h"
#include "mal_exception.h"
#include "mal_instruction.h"
#include "mal_client.h"

mal_export str AUTHcheckCredentials(oid *ret, Client *c, str *user, str *passwd, str *challenge, str *algo);
mal_export str AUTHaddUser(oid *ret, Client *c, str *user, str *pass);
mal_export str AUTHremoveUser(Client *c, str *username);
mal_export str AUTHchangeUsername(Client *c, str *olduser, str *newuser);
mal_export str AUTHchangePassword(Client *c, str *oldpass, str *passwd);
mal_export str AUTHsetPassword(Client *c, str *username, str *passwd);
mal_export str AUTHresolveUser(str *ret, oid *uid);
mal_export str AUTHgetUsername(str *ret, Client *c);
mal_export str AUTHgetUsers(BAT **ret, Client *c);
mal_export str AUTHgetPasswordHash(str *ret, Client *c, str *username);

mal_export str AUTHrequireAdmin(Client *c);
mal_export str AUTHrequireAdminOrUser(Client *c, str *username);
mal_export str AUTHinitTables(void);


/*
 * @-
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
mal_export str AUTHunlockVault(str *password);

#endif /* _MAL_AUTHORIZE_H */
