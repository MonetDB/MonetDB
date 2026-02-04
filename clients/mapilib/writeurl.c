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

#include "msettings.h"
#include "msettings_internal.h"

struct outbuf {
	char *buffer;
	char *end;
	char *pos;
	size_t logical_size;
};

static void
ob_append1(struct outbuf *ob, char c)
{
	ob->logical_size += 1;
	if (ob->pos < ob->end)
		*ob->pos++ = c;
}

static void
ob_append(struct outbuf *ob, const char *s)
{
	size_t len = strlen(s);
	ob->logical_size += len;
	size_t avail = ob->end - ob->pos;
	if (avail < len)
		len = avail;
	memcpy(ob->pos, s, len);
	ob->pos += len;
}

static void
ob_append_escaped(struct outbuf *ob, const char *text, bool escape_colon)
{
	const char *hex = "0123456789abcdef";
	for (const char *s = text; *s; s++) {
		int c = *s;
		bool must_escape = false;
		switch (c) {
			case '#':
			case '&':
			case '=':
			case '/':
			case '?':
			case '[':
			case ']':
			case '@':
			case '%':
				must_escape = true;
				break;
			case ':':
				must_escape = escape_colon;
				break;
			default:
				break;
		}
		if (must_escape) {
			int lo = (c & 0x0F);
			int hi = (c & 0xF0) >> 4;
			ob_append1(ob, '%');
			ob_append1(ob, hex[hi]);
			ob_append1(ob, hex[lo]);
		} else {
			ob_append1(ob, c);
		}
	}
}

static void ob_printf(struct outbuf *ob, _In_z_ _Printf_format_string_ const char *fmt, ...)
	__attribute__((__format__(__printf__, 2, 3)));

static void
ob_printf(struct outbuf *ob, const char *fmt, ...)
{
	va_list ap;
	size_t avail = ob->end - ob->pos;

	// vsnprintf wants to write the NUL so we use avail+1.
	// (we left room for that)
	va_start(ap, fmt);
	int n = vsnprintf(ob->pos, avail + 1, fmt, ap);
	va_end(ap);
	assert(n >= 0);
	size_t delta = (size_t)n;
	ob->logical_size += delta;
	ob->pos += (delta <= avail ? delta : avail);
}

static void
format_url(struct outbuf *ob, const msettings *mp)
{
	ob_append(ob, msetting_bool(mp, MP_TLS) ? "monetdbs": "monetdb");
	ob_append(ob, "://");

	const char *host = msetting_string(mp, MP_HOST);
	if (*host == '\0') {
		ob_append(ob, "localhost");
	} else if (strcmp(host, "localhost") == 0) {
		ob_append(ob, "localhost.");
	} else if (strchr(host, ':')) {
		ob_append1(ob, '[');
		ob_append_escaped(ob, host, false);
		ob_append1(ob, ']');
	} else {
		ob_append_escaped(ob, host, true);
	}

	long port = msetting_long(mp, MP_PORT);
	if (port > 0 && port < 65536 && port != 50000) {
		ob_printf(ob, ":%ld", port);
	}

	const char *database = msetting_string(mp, MP_DATABASE);
	const char *table_schema = msetting_string(mp, MP_TABLESCHEMA);
	const char *table_name = msetting_string(mp, MP_TABLE);
	bool include_table_name = (table_name[0] != '\0');
	bool include_table_schema = (table_schema[0] != '\0') || include_table_name;
	bool include_database = (database[0] != '\0') || include_table_schema;
	if (include_database) {
		ob_append1(ob, '/');
		ob_append_escaped(ob, database, true);
	}
	if (include_table_schema) {
		ob_append1(ob, '/');
		ob_append_escaped(ob, table_schema, true);
	}
	if (include_table_name) {
		ob_append1(ob, '/');
		ob_append_escaped(ob, table_name, true);
	}

	char sep = '?';
	char scratch1[40], scratch2[40];
	mparm parm;
	for (int i = 0; (parm = mparm_enumerate(i)) != MP_UNKNOWN; i++) {
		if (parm == MP_IGNORE || mparm_is_core(parm))
			continue;
		const char *value = msetting_as_string(mp, parm, scratch1, sizeof(scratch1));
		const char *default_value = msetting_as_string(msettings_default, parm, scratch2, sizeof(scratch2));
		if (strcmp(value, default_value) == 0)
			continue;
		// render it
		ob_append1(ob, sep);
		sep = '&';
		ob_append(ob, mparm_name(parm));
		ob_append1(ob, '=');
		ob_append_escaped(ob, value, true);
	}
}

size_t
msettings_write_url(const msettings *mp, char *buffer, size_t buffer_size)
{
	char scratch[10];
	if (buffer_size == 0) {
		buffer = scratch;
		buffer_size = sizeof(scratch);
	}

	struct outbuf ob = {
		.buffer = buffer,
		.end = buffer + buffer_size - 1,
		.pos = buffer,
		.logical_size = 0,
	};
	// to ease debugging
	*ob.end = '\0';

	format_url(&ob, mp);

	assert(ob.pos <= ob.end);
	*ob.pos = '\0';
	return ob.logical_size;
}
