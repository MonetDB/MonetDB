/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _REL_REMOTE_H_
#define _REL_REMOTE_H_

#include "sql_relation.h"

extern int mapiuri_valid( const char *uri);
extern const char *mapiuri_uri(const char *uri, sql_allocator *sa);
extern const char *mapiuri_database(const char *uri, sql_allocator *sa);
extern const char *mapiuri_schema(const char *uri, sql_allocator *sa, const char *fallback);
extern const char *mapiuri_table(const char *uri, sql_allocator *sa, const char *fallback);

#endif /*_REL_REMOTE_H_*/
