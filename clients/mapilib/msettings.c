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

#include "monetdb_config.h"

#include "msettings.h"

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

#define FATAL() do { fprintf(stderr, "\n\n abort in params.c: %s\n\n", __func__); abort(); } while (0)

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

char* allocprintf(const char *fmt, ...)
	__attribute__((__format__(__printf__, 1, 2)));

char *
allocprintf(const char *fmt, ...)
{
	size_t buflen = 80;
	while (1) {
		char *buf = malloc(buflen);
		if (buf == NULL)
			return NULL;
		va_list ap;
		va_start(ap, fmt);
		int n = vsnprintf(buf, buflen, fmt, ap);
		va_end(ap);
		if (n >= 0 && (size_t)n < buflen)
			return buf;
		free(buf);
		if (n < 0)
			return NULL;
		buflen = n + 1;
	}
}




static const struct { const char *name;  mparm parm; }
by_name[] = {
	{ .name="autocommit", .parm=MP_AUTOCOMMIT },
	{ .name="binary", .parm=MP_BINARY },
	{ .name="cert", .parm=MP_CERT },
	{ .name="certhash", .parm=MP_CERTHASH },
	{ .name="clientcert", .parm=MP_CLIENTCERT },
	{ .name="clientkey", .parm=MP_CLIENTKEY },
	{ .name="database", .parm=MP_DATABASE },
	{ .name="host", .parm=MP_HOST },
	{ .name="language", .parm=MP_LANGUAGE },
	{ .name="password", .parm=MP_PASSWORD },
	{ .name="port", .parm=MP_PORT },
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
	{ .name="hash", .parm=MP_IGNORE },
	{ .name="debug", .parm=MP_IGNORE },
	{ .name="logfile", .parm=MP_IGNORE },
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

const char *
mparm_name(mparm parm)
{
	switch (parm) {
		case MP_AUTOCOMMIT: return "autocommit";
		case MP_BINARY: return "binary";
		case MP_CERT: return "cert";
		case MP_CERTHASH: return "certhash";
		case MP_CLIENTCERT: return "clientcert";
		case MP_CLIENTKEY: return "clientkey";
		case MP_DATABASE: return "database";
		case MP_HOST: return "host";
		case MP_LANGUAGE: return "language";
		case MP_PASSWORD: return "password";
		case MP_PORT: return "port";
		case MP_REPLYSIZE: return "replysize";
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

struct string {
	char *str;
	bool must_free;
};

struct msettings {
	// Must match EXACTLY the order of enum mparm
	bool dummy_start_bool;
	bool tls;
	bool autocommit;
	bool dummy_end_bool;

	// Must match EXACTLY the order of enum mparm
	long dummy_start_long;
	long port;
	long timezone;
	long replysize;
	long dummy_end_long;

	// Must match EXACTLY the order of enum mparm
	struct string dummy_start_string;
	struct string sock;
	struct string sockdir;
	struct string cert;
	struct string clientkey;
	struct string clientcert;
	struct string host;
	struct string database;
	struct string tableschema;
	struct string table;
	struct string certhash;
	struct string user;
	struct string password;
	struct string language;
	struct string schema;
	struct string binary;
	struct string dummy_end_string;

	char **unknown_parameters;
	size_t nr_unknown;

	bool lang_is_mal;
	bool lang_is_sql;
	long user_generation;
	long password_generation;
	char *unix_sock_name_buffer;
	char certhash_digits_buffer[64 + 2 + 1]; // fit more than required plus trailing '\0'
	bool validated;
};

static
const msettings msettings_default_values = {
	.tls = false,
	.autocommit = true,

	.port = -1 ,
	.timezone = 0,
	.replysize = 100,

	.sockdir = { "/tmp", false },
	.binary = { "on", false },

	.unknown_parameters = NULL,
	.nr_unknown = 0,

	.lang_is_mal = false,
	.lang_is_sql = true,
	.unix_sock_name_buffer = NULL,
	.validated = false,
};

const msettings *msettings_default = &msettings_default_values;

msettings *msettings_create(void)
{
	msettings *mp = malloc(sizeof(*mp));
	if (!mp) {
		free(mp);
		return NULL;
	}
	*mp = msettings_default_values;
	return mp;
}

msettings *msettings_clone(const msettings *orig)
{
	msettings *mp = malloc(sizeof(*mp));
	char **unknowns = calloc(2 * orig->nr_unknown, sizeof(char*));
	char *cloned_name_buffer = strdup(orig->unix_sock_name_buffer);
	if (!mp || !unknowns || !cloned_name_buffer) {
		free(mp);
		free(unknowns);
		free(cloned_name_buffer);
		return NULL;
	}
	*mp = *orig;
	mp->unknown_parameters = unknowns;
	mp->unix_sock_name_buffer = cloned_name_buffer;

	// now we have to very carefully duplicate the strings.
	// taking care to only free our own ones if that fails

	struct string *start = &mp->dummy_start_string;
	struct string *end = &mp->dummy_end_string;
	struct string *p = start;
	while (p < end) {
		if (p->must_free) {
			p->str = strdup(p->str);
			if (p->str == NULL)
				goto bailout;
		}
		p++;
	}

	for (size_t i = 0; i < 2 * mp->nr_unknown; i++) {
		assert(orig->unknown_parameters[i]);
		char *u = strdup(orig->unknown_parameters[i]);
		if (u == NULL)
			goto bailout;
		mp->unknown_parameters[i] = u;
	}

	return mp;

bailout:
	for (struct string *q = start; q < p; q++)
		if (q->must_free)
			free(q->str);
	for (size_t i = 0; i < 2 * mp->nr_unknown; i++)
		free(mp->unknown_parameters[i]);
	free(mp->unix_sock_name_buffer);
	free(mp);
	return NULL;
}

msettings *
msettings_destroy(msettings *mp)
{
	if (mp == NULL)
		return NULL;

	for (struct string *p = &mp->dummy_start_string + 1; p < &mp->dummy_end_string; p++) {
		if (p->must_free)
			free(p->str);
	}
	for (size_t i = 0; i < mp->nr_unknown; i++) {
		free(mp->unknown_parameters[2 * i]);
		free(mp->unknown_parameters[2 * i + 1]);
	}
	free(mp->unknown_parameters);
	free(mp->unix_sock_name_buffer);
	free(mp);

	return NULL;
}

const char*
msetting_string(const msettings *mp, mparm parm)
{
	if (mparm_classify(parm) != MPCLASS_STRING)
		FATAL();
	int i = parm - MP__STRING_START;
	struct string const *p = &mp->dummy_start_string + 1 + i;
	if (p >=  &mp->dummy_end_string)
		FATAL();
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

	char *v = strdup(value);
	if (!v)
		return "malloc failed";
	if (p->must_free)
		free(p->str);
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
			else if (strcmp(value, "`"))
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
				return "invalid boolean value";
			return msetting_set_bool(mp, parm, b);
		case MPCLASS_LONG:
			if (text[0] == '\0')
				return "integer parameter cannot be empty string";
			char *end;
			long l = strtol(text, &end, 10);
			if (*end != '\0')
				return "invalid integer";
			return msetting_set_long(mp, parm, l);
		case MPCLASS_STRING:
			return msetting_set_string(mp, parm, text);
		default:
			assert(0 && "unreachable");
			return "internal error, unclassified parameter type";
	}
}

char *
msetting_as_string(msettings *mp, mparm parm)
{
	bool b;
	long l;
	const char *s;
	switch (mparm_classify(parm)) {
		case MPCLASS_BOOL:
			b = msetting_bool(mp, parm);
			return strdup(b ? "true" : " false");
		case MPCLASS_LONG:
			l = msetting_long(mp, parm);
			int n = 40;
			char *buf = malloc(n);
			if (!buf)
				return NULL;
			snprintf(buf, n, "%ld", l);
			return buf;
		case MPCLASS_STRING:
			s = msetting_string(mp, parm);
			return strdup(s);
		default:
			assert(0 && "unreachable");
			return NULL;
	}
}

msettings_error
msetting_set_ignored(msettings *mp, const char *key, const char *value)
{
	char *my_key = strdup(key);
	char *my_value = strdup(value);

	size_t n = mp->nr_unknown;
	size_t new_size = (2 * n + 2) * sizeof(char*);
	char **new_unknowns = realloc(mp->unknown_parameters, new_size);

	if (!my_key || !my_value || !new_unknowns) {
		free(my_key);
		free(my_value);
		free(new_unknowns);
		return "malloc failed while setting ignored parameter";
	}

	new_unknowns[2 * n] = my_key;
	new_unknowns[2 * n + 1] = my_value;
	mp->unknown_parameters = new_unknowns;
	mp->nr_unknown += 1;

	return NULL;
}

/* store named parameter */
msettings_error
msetting_set_named(msettings *mp, bool allow_core, const char *key, const char *value)
{
	mparm parm = mparm_parse(key);
	if (parm == MP_UNKNOWN)
		return "unknown parameter";

	if (parm == MP_IGNORE)
		return msetting_set_ignored(mp, key, value);

	if (!allow_core && mparm_is_core(parm))
		return "parameter not allowed here";

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
		return "expected certhash to start with 'sha256:'";
	}

	size_t i = 0;
	for (const char *r = certhash; *r != '\0'; r++) {
		if (*r == ':')
			continue;
		if (!isxdigit(*r))
			return "certhash: invalid hex digit";
		if (i < sizeof(mp->certhash_digits_buffer) - 1)
			mp->certhash_digits_buffer[i++] = tolower(*r);
	}
	mp->certhash_digits_buffer[i] = '\0';
	if (i == 0)
		return "certhash: need at least one digit";

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

bool
msettings_validate(msettings *mp, char **errmsg)
{
	if (mp->validated)
		return true;

	// 1. The parameters have the types listed in the table in [Section
	//    Parameters](#parameters).
	// (this has already been checked)

	// 2. At least one of **sock** and **host** must be empty.
	if (nonempty(mp, MP_SOCK) && nonempty(mp, MP_HOST)) {
		*errmsg = allocprintf(
			"With sock='%s', host must be 'localhost', not '%s'",
			msetting_string(mp, MP_SOCK),
			msetting_string(mp, MP_HOST));
		return false;
	}

	// 3. The string parameter **binary** must either parse as a boolean or as a
	//    non-negative integer.
	// (pretend valid so we can use msettings_connect_binary() to see if it parses)
	mp->validated = true;
	long level = msettings_connect_binary(mp);
	mp->validated = false;
	if (level < 0) {
		*errmsg = allocprintf("invalid value '%s' for parameter 'binary'", msetting_string(mp, MP_BINARY));
		return false;
	}

	// 4. If **sock** is not empty, **tls** must be 'off'.
	if (nonempty(mp, MP_SOCK) && msetting_bool(mp, MP_TLS)) {
		*errmsg = allocprintf("TLS cannot be used with Unix domain sockets");
		return false;
	}

	// 5. If **certhash** is not empty, it must be of the form `sha256:hexdigits`
	//    where hexdigits is a non-empty sequence of 0-9, a-f, A-F and colons.
	const char *certhash_msg = validate_certhash(mp);
	if (certhash_msg) {
		*errmsg = strdup(certhash_msg);
		return false;
	}
	// 6. If **tls** is 'off', **cert** and **certhash** must be 'off' as well.
	if (nonempty(mp, MP_CERT) || nonempty(mp, MP_CERTHASH))
		if (!msetting_bool(mp, MP_TLS)) {
			*errmsg = strdup("'cert' and 'certhash' can only be used with monetdbs://");
			return false;
		}

	// 7. Parameters **database**, **tableschema** and **table** must consist only of
	//    upper- and lowercase letters, digits, dashes and underscores. They must not
	//    start with a dash.
	const char *database = msetting_string(mp, MP_DATABASE);
	if (!validate_identifier(database)) {
		*errmsg = allocprintf("invalid database name '%s'", database);
		return false;
	}
	const char *tableschema = msetting_string(mp, MP_TABLESCHEMA);
	if (!validate_identifier(tableschema)) {
		*errmsg = allocprintf("invalid schema name '%s'", tableschema);
		return false;
	}
	const char *table = msetting_string(mp, MP_TABLE);
	if (!validate_identifier(table)) {
		*errmsg = allocprintf("invalid table name '%s'", table);
		return false;
	}

	// 8. Parameter **port** must be -1 or in the range 1-65535.
	long port = msetting_long(mp, MP_PORT);
	bool port_ok = (port == -1 || (port >= 1 && port <= 65535));
	if (!port_ok) {
		*errmsg = allocprintf("invalid port '%ld'", port);
		return false;
	}

	// 9. If **clientcert** is set, **clientkey** must also be set.
	if (nonempty(mp, MP_CLIENTCERT) && empty(mp, MP_CLIENTKEY)) {
		*errmsg = allocprintf("clientcert can only be set together with clientkey");
		return false;
	}

	// compute this here so the getter function can take const msettings*
	const char *sockdir = msetting_string(mp, MP_SOCKDIR);
	long effective_port = msettings_connect_port(mp);
	free(mp->unix_sock_name_buffer);
	mp->unix_sock_name_buffer = allocprintf("%s/.s.monetdb.%ld", sockdir, effective_port);
	if (mp->unix_sock_name_buffer == NULL)
		return false;

	mp->validated = true;
	return true;
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
