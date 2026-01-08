/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
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
mal_export str AUTHdecypherValue(allocator *, str *ret, const char *value);
mal_export str AUTHcypherValue(allocator *, str *ret, const char *value);
mal_export str AUTHrequireAdmin(Client c);
mal_export str AUTHGeneratePasswordHash(allocator *, str *res, const char *value);

#endif /* _MAL_AUTHORIZE_H */
