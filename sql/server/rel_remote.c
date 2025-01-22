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

#include "monetdb_config.h"
#include "rel_remote.h"
#include "msettings.h"

static void *
msettings_sa_allocator(void *state, void *old, size_t size)
{
	allocator *sa = state;

	if (size == 0) {
		// This is really a free(), ignore it.
		return NULL;
	} else if (old == NULL) {
		// This is really a malloc()
		return sa_alloc(sa, size);
	} else {
		// We can't handle generic realloc because we don't know how large the
		// previous allocation was, so we don't know how much to copy.
		// Fortunately, msettings doesn't really reallocate so we don't need
		// this for now.
		assert(size == 0 || old == NULL);
		return NULL;
	}
}

msettings *
sa_msettings_create(allocator *sa)
{
	return msettings_create_with(msettings_sa_allocator, sa);
}

#define mapi_prefix "mapi:"
#define monetdb_prefix "monetdb"

int
mapiuri_valid( const char *uri, allocator *sa)
{
	msettings *mp = sa_msettings_create(sa);
	return msettings_parse_url(mp, uri) == NULL;
}

/* assume valid uri's next functions */

/* strip schema- and table name from uri  */
const char *
mapiuri_uri( const char *uri, allocator *sa)
{
	msettings *mp = sa_msettings_create(sa);
	if (!mp || msettings_parse_url(mp, uri) != NULL)
		return NULL;
	msetting_set_string(mp, MP_TABLESCHEMA, "");
	msetting_set_string(mp, MP_TABLE, "");

	size_t buffer_size = strlen(uri) + 1;
	do {
		char *buffer = sa_alloc(sa, buffer_size);
		if (!buffer)
			return NULL;
		size_t needed = msettings_write_url(mp, buffer, buffer_size);
		if (needed + 1 <= buffer_size)
			return buffer;
		// it's unlikely but remotely possible that the url as written by
		// msettings_write_url is longer, for example because it escapes some
		// characters that were not escaped in the original
		buffer_size = needed + 1;
	} while (1);

}

const char *
mapiuri_schema( const char *uri, allocator *sa, const char *fallback)
{
	msettings *mp = sa_msettings_create(sa);
	if (!mp || msettings_parse_url(mp, uri) != NULL)
		return fallback;
	const char *schema = msetting_string(mp, MP_TABLESCHEMA);
	return schema[0] ? schema : fallback;
}

const char *
mapiuri_table( const char *uri, allocator *sa, const char *fallback)
{
	msettings *mp = sa_msettings_create(sa);
	if (!mp || msettings_parse_url(mp, uri) != NULL)
		return fallback;
	const char *schema = msetting_string(mp, MP_TABLE);
	return schema[0] ? schema : fallback;
}
