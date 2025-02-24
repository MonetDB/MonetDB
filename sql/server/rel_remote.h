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

#ifndef _REL_REMOTE_H_
#define _REL_REMOTE_H_

#include "sql_relation.h"
#include "msettings.h"

sql_export msettings *sa_msettings_create(allocator *sa);
sql_export char *sa_msettings_to_string(const msettings *mp, allocator *sa, size_t size_hint);

sql_export int mapiuri_valid( const char *uri, allocator *sa);
sql_export const char *mapiuri_uri(const char *uri, allocator *sa);
extern const char *mapiuri_schema(const char *uri, allocator *sa, const char *fallback);
extern const char *mapiuri_table(const char *uri, allocator *sa, const char *fallback);

#endif /*_REL_REMOTE_H_*/
