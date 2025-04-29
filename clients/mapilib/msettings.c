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

#include "msettings.h"
#include "msettings_internal.h"
#include "mstring.h"

#include <assert.h>
#include <ctype.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif

#define FATAL() do { fprintf(stderr, "\n\n abort in msettings.c: %s\n\n", __func__); abort(); } while (0)

const char MALLOC_FAILED[] = "malloc failed";

bool
msettings_malloc_failed(msettings_error err)
{
	return (err == MALLOC_FAILED);
}


int msetting_parse_bool(const char *text)
{
	static struct { const char *word; bool value; } variants[] = {
		{ "true", true },
		{ "false", false },
		{ "yes", true },
		{ "no", false },
		{ "on", true },
		{ "off", false },
	};
	for (size_t i = 0; i < sizeof(variants) / sizeof(variants[0]); i++)
		if (strcasecmp(text, variants[i].word) == 0)
			return variants[i].value;
	return -1;
}


static const struct { const char *name;  mparm parm; }
by_name[] = {
	{ .name="autocommit", .parm=MP_AUTOCOMMIT },
	{ .name="binary", .parm=MP_BINARY },
	{ .name="cert", .parm=MP_CERT },
	{ .name="certhash", .parm=MP_CERTHASH },
	{ .name="client_application", .parm=MP_CLIENT_APPLICATION },
	{ .name="client_info", .parm=MP_CLIENT_INFO },
	{ .name="client_remark", .parm=MP_CLIENT_REMARK },
	{ .name="clientcert", .parm=MP_CLIENTCERT },
	{ .name="clientkey", .parm=MP_CLIENTKEY },
	{ .name="connect_timeout", .parm=MP_CONNECT_TIMEOUT },
	{ .name="database", .parm=MP_DATABASE },
	{ .name="host", .parm=MP_HOST },
	{ .name="language", .parm=MP_LANGUAGE },
	{ .name="map_to_long_varchar", .parm=MP_MAPTOLONGVARCHAR },
	{ .name="password", .parm=MP_PASSWORD },
	{ .name="port", .parm=MP_PORT },
	{ .name="reply_timeout", .parm=MP_REPLY_TIMEOUT },
	{ .name="replysize", .parm=MP_REPLYSIZE },
	{ .name="fetchsize", .parm=MP_REPLYSIZE },
	{ .name="schema", .parm=MP_SCHEMA },
	{ .name="sock", .parm=MP_SOCK },
	{ .name="sockdir", .parm=MP_SOCKDIR},
	{ .name="table", .parm=MP_TABLE },
	{ .name="tableschema", .parm=MP_TABLESCHEMA },
	{ .name="timezone", .parm=MP_TIMEZONE },
	{ .name="tls", .parm=MP_TLS },
	{ .name="user", .parm=MP_USER },
	//
	{ .name="logfile", .parm=MP_LOGFILE },
	//
	{ .name="hash", .parm=MP_IGNORE },
	{ .name="debug", .parm=MP_IGNORE },
};

mparm
mparm_parse(const char *name)
{
	int n = sizeof(by_name) / sizeof(by_name[0]);
	// could use a binary search but this is not going to be a bottleneck
	for (int i = 0; i < n; i++)
		if (strcmp(by_name[i].name, name) == 0)
			return by_name[i].parm;

	return strchr(name, '_') ? MP_IGNORE : MP_UNKNOWN;
}

mparm
mparm_enumerate(int i)
{
	int n = sizeof(by_name) / sizeof(by_name[0]);
	if (i < 0 || i >= n)
		return MP_UNKNOWN;
	return by_name[i].parm;
}

const char *
mparm_name(mparm parm)
{
	switch (parm) {
		case MP_AUTOCOMMIT: return "autocommit";
		case MP_BINARY: return "binary";
		case MP_CERT: return "cert";
		case MP_CERTHASH: return "certhash";
		case MP_CLIENT_APPLICATION: return "client_application";
		case MP_CLIENT_INFO: return "client_info";
		case MP_CLIENT_REMARK: return "client_remark";
		case MP_CLIENTCERT: return "clientcert";
		case MP_CLIENTKEY: return "clientkey";
		case MP_CONNECT_TIMEOUT: return "connect_timeout";
		case MP_DATABASE: return "database";
		case MP_HOST: return "host";
		case MP_LANGUAGE: return "language";
		case MP_LOGFILE: return "logfile";
		case MP_MAPTOLONGVARCHAR: return "map_to_long_varchar";
		case MP_PASSWORD: return "password";
		case MP_PORT: return "port";
		case MP_REPLY_TIMEOUT: return "reply_timeout";  // underscore present means specific to this client library
		case MP_REPLYSIZE: return "replysize";  // no underscore means mandatory for all client libraries
		case MP_SCHEMA: return "schema";
		case MP_SOCK: return "sock";
		case MP_SOCKDIR: return "sockdir";
		case MP_TABLE: return "table";
		case MP_TABLESCHEMA: return "tableschema";
		case MP_TIMEZONE: return "timezone";
		case MP_TLS: return "tls";
		case MP_USER: return "user";
		default: FATAL();
	}
}

bool
mparm_is_core(mparm parm)
{
	switch (parm) {
		case MP_TLS:
		case MP_HOST:
		case MP_PORT:
		case MP_DATABASE:
		case MP_TABLESCHEMA:
		case MP_TABLE:
			return true;
		default:
			return false;
	}
}

static
const msettings msettings_default_values = {
	.tls = false,
	.autocommit = true,
	.client_info = true,

	.port = -1 ,
	.timezone = 0,
	.replysize = 100,

	.sockdir = { "/tmp", false },
	.binary = { "on", false },

	.lang_is_mal = false,
	.lang_is_sql = true,
	.unix_sock_name_buffer = NULL,

	.validated = false,
};

const msettings *msettings_default = &msettings_default_values;

msettings *msettings_create_with(msettings_allocator alloc, void *allocator_state)
{
	if (alloc == NULL) {
		allocator_state = NULL;
	}

	msettings *mp = realloc_with_fallback(alloc, allocator_state, NULL, sizeof(*mp));
	if (!mp)
		return NULL;

	*mp = msettings_default_values;
	mp->alloc = alloc;
	mp->alloc_state = allocator_state;

	return mp;
}

msettings *msettings_create(void)
{
	return msettings_create_with(NULL, NULL);
}

msettings_allocator
msettings_get_allocator(const msettings *mp, void **put_alloc_state_here)
{
	if (mp->alloc == NULL)
		return NULL;
	if (put_alloc_state_here)
		*put_alloc_state_here = mp->alloc_state;
	return mp->alloc;
}

msettings *msettings_clone_with(msettings_allocator alloc, void *alloc_state, const msettings *orig)
{
	bool free_unix_sock_name_buffer = false;
	struct string *copy_start = NULL;
	struct string *copy_pos = NULL;

	msettings *mp = msettings_create_with(alloc, alloc_state);
	if (!mp)
		return NULL;

	// Before we copy orig over mp, remember mp's allocator as decided by msettings_create_with.
	alloc = mp->alloc;
	alloc_state = mp->alloc_state;

	// Copy the whole struct, including the pointers to data owned by orig.
	// This means we must be really careful when we abort halfway and need to free 'mp'.
	// We will use 'start' and 'pos' to keep track of which strings must be free'd BY US
	*mp = *orig;
	mp->alloc = alloc;
	mp->alloc_state = alloc_state;
	copy_start = &mp->dummy_start_string;
	copy_pos = copy_start;

	if (orig->unix_sock_name_buffer) {
		mp->unix_sock_name_buffer = msettings_strdup(mp, orig->unix_sock_name_buffer);
		if (mp->unix_sock_name_buffer == NULL)
			goto bailout;
		free_unix_sock_name_buffer = true;
	}

	// Duplicate the strings that need to be duplicated
	for (; copy_pos < &mp->dummy_end_string; copy_pos++) {
		if (copy_pos->must_free) {
			copy_pos->str = msettings_strdup(mp, copy_pos->str);
			if (copy_pos->str == NULL)
				goto bailout;
		}
	}

	// Now all references to data allocated by 'orig' has been copied.
	return mp;

bailout:
	if (free_unix_sock_name_buffer)
		msettings_dealloc(mp, mp->unix_sock_name_buffer);
	while (copy_start != copy_pos) {
		msettings_dealloc(mp, copy_start->str);
		copy_start++;
	}
	msettings_dealloc(mp, mp);
	return NULL;
}

msettings *msettings_clone(const msettings *orig)
{
	return msettings_clone_with(NULL, NULL, orig);
}

void
msettings_reset(msettings *mp)
{
	// free modified string settings
	struct string *start = &mp->dummy_start_string;
	struct string *end = &mp->dummy_end_string;
	for (struct string *p = start; p < end; p++) {
		if (p->must_free)
			msettings_dealloc(mp, p->str);
	}

	// free the buffer
	msettings_dealloc(mp, mp->unix_sock_name_buffer);

	// keep the localizer and the allocator
	void *localizer = mp->localizer;
	void *localizer_data = mp->localizer_data;
	msettings_allocator alloc = mp->alloc;
	void *alloc_state = mp->alloc_state;

	// now overwrite the whole thing
	*mp = *msettings_default;
	mp->localizer = localizer;
	mp->localizer_data = localizer_data;
	mp->alloc = alloc;
	mp->alloc_state = alloc_state;
}

msettings *
msettings_destroy(msettings *mp)
{
	if (mp == NULL)
		return NULL;

	for (struct string *p = &mp->dummy_start_string + 1; p < &mp->dummy_end_string; p++) {
		if (p->must_free)
			msettings_dealloc(mp, p->str);
	}
	msettings_dealloc(mp, mp->unix_sock_name_buffer);
	msettings_dealloc(mp, mp);

	return NULL;
}

const char *
format_error(msettings *mp, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(mp->error_message, sizeof(mp->error_message), fmt, ap);
	va_end(ap);

	return mp->error_message;
}

const char *msetting_parm_name(const msettings *mp, mparm parm)
{
	const char *localized = NULL;
	if (mp->localizer)
		localized = (mp->localizer)(mp->localizer_data, parm);
	return localized ? localized : mparm_name(parm);
}

void msettings_set_localizer(msettings *mp, const char* (*localizer)(const void *data, mparm parm), void *data)
{
	mp->localizer = localizer;
	mp->localizer_data = data;
}



const char*
msetting_string(const msettings *mp, mparm parm)
{
	if (mparm_classify(parm) != MPCLASS_STRING) {
		FATAL();
		return "";
	}
	int i = parm - MP__STRING_START;
	struct string const *p = &mp->dummy_start_string + 1 + i;
	if (p >=  &mp->dummy_end_string) {
		FATAL();
		return "";
	}
	char *s = p->str;

	if (s == NULL) {
		if (parm == MP_LANGUAGE)
			return "sql";
		else if (parm == MP_BINARY)
			return "on";
		return "";
	}
	return s;
}


msettings_error
msetting_set_string(msettings *mp, mparm parm, const char* value)
{
	assert(value != NULL);

	if (mparm_classify(parm) != MPCLASS_STRING)
		FATAL();
	int i = parm - MP__STRING_START;
	struct string *p = &mp->dummy_start_string + 1 + i;
	if (p >=  &mp->dummy_end_string)
		FATAL();

	char *v = msettings_strdup(mp, value);
	if (!v)
		return MALLOC_FAILED;
	if (p->must_free)
		msettings_dealloc(mp, p->str);
	p->str = v;
	p->must_free = true;

	switch (parm) {
		case MP_USER:
			mp->user_generation++;
			break;
		case MP_PASSWORD:
			mp->password_generation++;
			break;
		case MP_LANGUAGE:
			mp->lang_is_mal = false;
			mp->lang_is_sql = false;
			// Tricky logic, a mixture of strstr==val and strcmp
			// strstr==val is a clever way to compute 'startswith'
			if (strcmp(value, "mal") == 0 || strcmp(value, "msql") == 0)
				mp->lang_is_mal = true;
			else if (strstr(value, "sql") == value)
				mp->lang_is_sql = true;
			break;
		default:
			break;
	}

	mp->validated = false;
	return NULL;
}


long
msetting_long(const msettings *mp, mparm parm)
{
	if (mparm_classify(parm) != MPCLASS_LONG)
		FATAL();
	int i = parm - MP__LONG_START;
	const long * p = &mp->dummy_start_long + 1 + i;
	if (p >=  &mp->dummy_end_long)
		FATAL();

	return *p;
}


msettings_error
msetting_set_long(msettings *mp, mparm parm, long value)
{
	if (mparm_classify(parm) != MPCLASS_LONG)
		FATAL();
	int i = parm - MP__LONG_START;
	long *p = &mp->dummy_start_long + 1 + i;
	if (p >=  &mp->dummy_end_long)
		FATAL();

	*p = value;

	mp->validated = false;
	return NULL;
}


bool
msetting_bool(const msettings *mp, mparm parm)
{
	if (mparm_classify(parm) != MPCLASS_BOOL)
		FATAL();
	int i = parm - MP__BOOL_START;
	const bool *p = &mp->dummy_start_bool + 1 + i;
	if (p >=  &mp->dummy_end_bool)
		FATAL();
	return *p;
}


msettings_error
msetting_set_bool(msettings *mp, mparm parm, bool value)
{
	if (mparm_classify(parm) != MPCLASS_BOOL)
		FATAL();
	int i = parm - MP__BOOL_START;
	bool *p = &mp->dummy_start_bool + 1 + i;
	if (p >=  &mp->dummy_end_bool)
		FATAL();
	*p = value;

	mp->validated = false;
	return NULL;
}

msettings_error
msetting_parse(msettings *mp, mparm parm, const char *text)
{
	int b; // int not bool because we need to allow for parse errors
	switch (mparm_classify(parm)) {
		case MPCLASS_BOOL:
			b = msetting_parse_bool(text);
			if (b < 0)
				return format_error(mp, "%s: invalid boolean value", msetting_parm_name(mp, parm));
			return msetting_set_bool(mp, parm, b);
		case MPCLASS_LONG:
			if (text[0] == '\0')
				return format_error(mp, "%s: integer parameter cannot be empty string", msetting_parm_name(mp, parm));
			char *end;
			long l = strtol(text, &end, 10);
			if (*end != '\0')
				return format_error(mp, "%s: invalid integer", msetting_parm_name(mp, parm));
			return msetting_set_long(mp, parm, l);
		case MPCLASS_STRING:
			return msetting_set_string(mp, parm, text);
		default:
			assert(0 && "unreachable");
			return "internal error, unclassified parameter type";
	}
}

const char *
msetting_as_string(const msettings *mp, mparm parm, char *scratch, size_t scratch_size)
{
	long l;
	switch (mparm_classify(parm)) {
		case MPCLASS_BOOL:
			return msetting_bool(mp, parm) ? "true" : "false";
		case MPCLASS_LONG:
			l = msetting_long(mp, parm);
			int n = snprintf(scratch, scratch_size, "%ld", l);
			if (n > 0 && scratch_size >= (size_t)n + 1)
				return scratch;
			else
				return NULL;
		case MPCLASS_STRING:
			return msetting_string(mp, parm);
		default:
			assert(0 && "unreachable");
			return NULL;
	}
}

/* store named parameter */
msettings_error
msetting_set_named(msettings *mp, bool allow_core, const char *key, const char *value)
{
	mparm parm = mparm_parse(key);
	if (parm == MP_UNKNOWN)
		return format_error(mp, "%s: unknown parameter", key);

	if (parm == MP_IGNORE)
		return NULL;

	if (!allow_core && mparm_is_core(parm))
		return format_error(mp, "%s: parameter not allowed here", msetting_parm_name(mp, parm));

	return msetting_parse(mp, parm, value);
}


static bool
empty(const msettings *mp, mparm parm)
{
	const char *value = msetting_string(mp, parm);
	assert(value);
	return *value == '\0';
}

static bool
nonempty(const msettings *mp, mparm parm)
{
	return !empty(mp, parm);
}

static msettings_error
validate_certhash(msettings *mp)
{
	mp->certhash_digits_buffer[0] = '\0';

	const char *full_certhash = msetting_string(mp, MP_CERTHASH);
	const char *certhash = full_certhash;
	if (*certhash == '\0')
		return NULL;

	if (strncmp(certhash, "sha256:", 7) == 0) {
		certhash += 7;
	} else {
		return format_error(mp, "%s: expected to start with 'sha256:'", msetting_parm_name(mp, MP_CERTHASH));
	}

	size_t i = 0;
	for (const char *r = certhash; *r != '\0'; r++) {
		if (*r == ':')
			continue;
		if (!isxdigit(*r))
			return format_error(mp, "%s: invalid hex digit", msetting_parm_name(mp, MP_CERTHASH));
		if (i < sizeof(mp->certhash_digits_buffer) - 1)
			mp->certhash_digits_buffer[i++] = tolower(*r);
	}
	mp->certhash_digits_buffer[i] = '\0';
	if (i == 0)
		return format_error(mp, "%s: need at least one digit", msetting_parm_name(mp, MP_CERTHASH));

	return NULL;
}

static bool
validate_identifier(const char *name)
{
	int first = name[0];
	if (first == '\0')
		return true;
	if (first != '_' && !isalpha(first))
		return false;
	for (const char *p = name; *p; p++) {
		bool ok = (isalnum(*p) || *p == '.' || *p == '-' || *p == '_');
		if (!ok)
			return false;
	}
	return true;
}

msettings_error
msettings_validate(msettings *mp)
{
	if (mp->validated)
		return NULL;

	// 1. The parameters have the types listed in the table in [Section
	//    Parameters](#parameters).
	// (this has already been checked)

	// 2. At least one of **sock** and **host** must be empty.
	if (nonempty(mp, MP_SOCK) && nonempty(mp, MP_HOST)) {
		return format_error(mp,
			"With sock='%s', host must be 'localhost', not '%s'",
			msetting_string(mp, MP_SOCK),
			msetting_string(mp, MP_HOST));
	}

	// 3. The string parameter **binary** must either parse as a boolean or as a
	//    non-negative integer.
	// (pretend valid so we can use msettings_connect_binary() to see if it parses)
	mp->validated = true;
	long level = msettings_connect_binary(mp);
	mp->validated = false;
	if (level < 0) {
		return format_error(mp, "invalid value '%s' for parameter 'binary'", msetting_string(mp, MP_BINARY));
	}

	// 4. If **sock** is not empty, **tls** must be 'off'.
	if (nonempty(mp, MP_SOCK) && msetting_bool(mp, MP_TLS)) {
		return format_error(mp, "TLS cannot be used with Unix domain sockets");
	}

	// 5. If **certhash** is not empty, it must be of the form `sha256:hexdigits`
	//    where hexdigits is a non-empty sequence of 0-9, a-f, A-F and colons.
	const char *certhash_msg = validate_certhash(mp);
	if (certhash_msg) {
		return format_error(mp, "%s", certhash_msg);
	}

	// 6. If **tls** is 'off', **cert** and **certhash** must be 'off' as well.
	if (nonempty(mp, MP_CERT) || nonempty(mp, MP_CERTHASH))
		if (!msetting_bool(mp, MP_TLS)) {
			return format_error(mp, "'cert' and 'certhash' can only be used with monetdbs://");
		}

	// 7. Parameters **database**, **tableschema** and **table** must consist only of
	//    upper- and lowercase letters, digits, dashes and underscores. They must not
	//    start with a dash.
	const char *database = msetting_string(mp, MP_DATABASE);
	if (!validate_identifier(database)) {
		return format_error(mp, "invalid database name '%s'", database);
	}
	const char *tableschema = msetting_string(mp, MP_TABLESCHEMA);
	if (!validate_identifier(tableschema)) {
		return format_error(mp, "invalid schema name '%s'", tableschema);
	}
	const char *table = msetting_string(mp, MP_TABLE);
	if (!validate_identifier(table)) {
		return format_error(mp, "invalid table name '%s'", table);
	}

	// 8. Parameter **port** must be -1 or in the range 1-65535.
	long port = msetting_long(mp, MP_PORT);
	bool port_ok = (port == -1 || (port >= 1 && port <= 65535));
	if (!port_ok) {
		return format_error(mp, "invalid port '%ld'", port);
	}

	// 9. If **clientcert** is set, **clientkey** must also be set.
	if (nonempty(mp, MP_CLIENTCERT) && empty(mp, MP_CLIENTKEY)) {
		return format_error(mp, "clientcert can only be set together with clientkey");
	}

	// compute this here so the getter function can take const msettings*
	const char *sockdir = msetting_string(mp, MP_SOCKDIR);
	long effective_port = msettings_connect_port(mp);
	msettings_dealloc(mp, mp->unix_sock_name_buffer);
	mp->unix_sock_name_buffer = msettings_allocprintf(mp, "%s/.s.monetdb.%ld", sockdir, effective_port);
	if (mp->unix_sock_name_buffer == NULL)
		return false;

	mp->validated = true;
	return NULL;
}

bool
msettings_connect_scan(const msettings *mp)
{
	if (empty(mp, MP_DATABASE))
		return false;
	if (nonempty(mp, MP_SOCK))
		return false;
	if (nonempty(mp, MP_HOST))
		return false;
	long port = msetting_long(mp, MP_PORT);
	if (port != -1)
		return false;
	bool tls = msetting_bool(mp, MP_TLS);
	if (tls)
		return false;

	return true;
}

const char *
msettings_connect_unix(const msettings *mp)
{
	assert(mp->validated);
	const char *sock = msetting_string(mp, MP_SOCK);
	const char *host = msetting_string(mp, MP_HOST);
	bool tls = msetting_bool(mp, MP_TLS);

	if (*sock)
		return sock;
	if (tls)
		return "";
	if (*host == '\0') {
		// This was precomputed in msettings_validate(),
		// {sockdir}/.s.monetdb.{port}
		return mp->unix_sock_name_buffer;
	}
	return "";
}


const char *
msettings_connect_tcp(const msettings *mp)
{
	assert(mp->validated);
	const char *sock = msetting_string(mp, MP_SOCK);
	const char *host = msetting_string(mp, MP_HOST);
	// bool tls = msetting_bool(mp, MP_TLS);

	if (*sock)
		return "";
	if (!*host)
		return "localhost";
	return host;
}

long
msettings_connect_port(const msettings *mp)
{
	long port = msetting_long(mp, MP_PORT);
	if (port == -1)
		return 50000;
	else
		return port;
}

enum msetting_tls_verify
msettings_connect_tls_verify(const msettings *mp)
{
	assert(mp->validated);
	bool tls = msetting_bool(mp, MP_TLS);
	const char *cert = msetting_string(mp, MP_CERT);
	const char *certhash = msetting_string(mp, MP_CERTHASH);

	if (!tls)
		return verify_none;
	if (*certhash) // certhash comes before cert
		return verify_hash;
	if (*cert)
		return verify_cert;
	return verify_system;
}

const char*
msettings_connect_clientkey(const msettings *mp)
{
	return msetting_string(mp, MP_CLIENTKEY);
}

const char*
msettings_connect_clientcert(const msettings *mp)
{
	const char *cert = msetting_string(mp, MP_CLIENTCERT);
	if (*cert)
		return cert;
	else
		return msetting_string(mp, MP_CLIENTKEY);
}

const char*
msettings_connect_certhash_digits(const msettings *mp)
{
	return mp->certhash_digits_buffer;
}

// also used as a validator, returns < 0 on invalid
long
msettings_connect_binary(const msettings *mp)
{
	const long sufficiently_large = 65535;
	const char *binary = msetting_string(mp, MP_BINARY);

	// may be bool
	int b = msetting_parse_bool(binary);
	if (b == 0)
		return 0;
	if (b == 1)
		return sufficiently_large;
	assert(b < 0);

	char *end;
	long level = strtol(binary, &end, 10);
	if (end != binary && *end == '\0')
		return level;

	return -1;
}


/* automatically incremented each time the corresponding field is updated */
long
msettings_user_generation(const msettings *mp)
{
	return mp->user_generation;
}

/* automatically incremented each time the corresponding field is updated */
long
msettings_password_generation(const msettings *mp)
{
	return mp->password_generation;
}


bool
msettings_lang_is_mal(const msettings *mp)
{
	return mp->lang_is_mal;
}

bool
msettings_lang_is_sql(const msettings *mp)
{
	return mp->lang_is_sql;
}
