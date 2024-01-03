/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _MAL_AUTHORIZE_H
#define _MAL_AUTHORIZE_H

/* #define _DEBUG_AUTH_*/
#include "mal.h"
#include "mal_instruction.h"
#include "mal_client.h"

#define MAL_ADMIN (oid) 0

mal_export str AUTHunlockVault(const char *password);
mal_export str AUTHverifyPassword(const char *passwd);
mal_export str AUTHdecypherValue(str *ret, const char *value);
mal_export str AUTHcypherValue(str *ret, const char *value);
mal_export str AUTHrequireAdmin(Client c);
mal_export str AUTHGeneratePasswordHash(str *res, const char *value);

#endif /* _MAL_AUTHORIZE_H */
