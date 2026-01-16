/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
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
		return ma_alloc(sa, size);
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

char*
sa_msettings_to_string(const msettings *mp, allocator *sa, size_t size_hint)
{
	size_t buffer_size = size_hint ? size_hint + 1 : 80;
	do {
		char *buffer = ma_alloc(sa, buffer_size);
		if (!buffer)
			return NULL;
		size_t needed = msettings_write_url(mp, buffer, buffer_size);
		if (needed + 1 <= buffer_size)
			return buffer;
		buffer_size = needed + 1;
	} while (1);
}

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
	return sa_msettings_to_string(mp, sa, strlen(uri));
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
