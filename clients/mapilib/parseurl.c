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
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Scanner state.
// Most scanner-related functions return 'false' on failure, 'true' on success.
// Some return a character pointer, NULL on failure, non-NULL on success.
typedef struct scanner {
	char *buffer;				// owned buffer with the scanned text in it
	char c;						// character we're currently looking at
	char *p;					// pointer to where we found c (may have been updated since)
	char error_message[256];	// error message, or empty string
} scanner;




static bool
initialize(scanner *sc, const char *url)
{
	sc->buffer = strdup(url);
	if (!sc->buffer)
		return false;
	sc->p = &sc->buffer[0];
	sc->c = *sc->p;
	sc->error_message[0] = '\0';
	return true;
}

static void
deinitialize(scanner *sc)
{
	free(sc->buffer);
}

static bool
has_failed(const scanner *sc)
{
	return sc->error_message[0] != '\0';
}

static char
advance(scanner *sc)
{
	assert(!has_failed(sc));
	sc->p++;
	sc->c = *sc->p;
	return sc->c;
}

static bool complain(scanner *sc, const char *fmt, ...)
	__attribute__((__format__(printf, 2, 3)));

static bool
complain(scanner *sc, const char *fmt, ...)
{
	// do not overwrite existing error message,
	// the first one is usually the most informative.
	if (!has_failed(sc)) {
		va_list ap;
		va_start(ap, fmt);
		vsnprintf(sc->error_message, sizeof(sc->error_message), fmt, ap);
		va_end(ap);
		if (!has_failed(sc)) {
			// error message was empty, need non-empty so we know an error has occurred
			strcpy(sc->error_message, "?");
		}
	}

	return false;
}

static bool
unexpected(scanner *sc)
{
	if (sc->c == 0) {
		return complain(sc, "URL ended unexpectedly");
	} else {
		size_t pos = sc->p - sc->buffer;
		return complain(sc, "unexpected character '%c' at position %zu", sc->c, pos);
	}
}

static bool
consume(scanner *sc, const char *text)
{
	for (const char *c = text; *c; c++) {
		if (sc->c == *c) {
			advance(sc);
			continue;
		}
		size_t pos = sc->p - sc->buffer;
		if (sc->c == '\0') {
			return complain(sc, "unexpected end at position %zu, expected '%s'", pos, c);
		}
		return complain(sc, "unexpected character '%c' at position %zu, expected '%s'", sc->c, pos, c);
	}
	return true;
}


static int
percent_decode_digit(char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	// return something so negative that it will still
	// be negative after we combine it with another digit
	return -1000;
}

static bool
percent_decode(scanner *sc, const char *context, char *string)
{
	char *r = string;
	char *w = string;
	while (*r != '\0') {

		if (*r != '%') {
			*w++ = *r++;
			continue;
		}
		char x = r[1];
		if (x == '\0')
			return complain(sc, "percent escape in %s ends after one digit", context);
		char y = r[2];
		int n = 16 * percent_decode_digit(x) + percent_decode_digit(y);
		if (n < 0) {
			return complain(sc, "invalid percent escape in %s", context);
		}
		*w++ = (char)n;
		r += 3;

	}
	*w = '\0';
	return true;
}

enum character_class {
	// regular characters
	not_special,
	// special characters in the sense of RFC 3986 Section 2.2, plus '&' and '='
	generic_special,
	// very special, special even in query parameter values
	very_special,
};

static enum character_class
classify(char c)
{
	switch (c) {
		case '\0':
		case '#':
		case '&':
		case '=':
			return very_special;
		case ':':
		case '/':
		case '?':
		case '[':
		case ']':
		case '@':
			return generic_special;
		case '%':  // % is NOT special!
		default:
			return not_special;
	}
}

static char *
scan(scanner *sc, enum character_class level)
{
	assert(!has_failed(sc));
	char *token = sc->p;

	// scan the token
	while (classify(sc->c) < level)
		advance(sc);

	// the current character is a delimiter.
	// overwrite it with \0 to terminate the scanned string.
	assert(sc->c == *sc->p);
	*sc->p = '\0';

	return token;
}

static char *
find(scanner *sc, const char *delims)
{
	assert(!has_failed(sc));
	char *token = sc->p;

	while (sc->c) {
		for (const char *d = delims; *d; d++)
			if (sc->c == *d) {
				*sc->p = '\0';
				return token;
			}
		advance(sc);
	}
	return token;
}

static bool
store(msettings *mp, scanner *sc, mparm parm, const char *value)
{
	msettings_error msg = msetting_parse(mp, parm, value);
	if (msg)
		return complain(sc, "cannot set %s to '%s': %s",mparm_name(parm), value, msg);
	else
		return true;
}

static bool
scan_query_parameters(scanner *sc, char **key, char **value)
{
		*key = scan(sc, very_special);
		if (strlen(*key) == 0)
			return complain(sc, "parameter name must not be empty");

		if (!consume(sc, "="))
			return false;
		*value = find(sc, "&#");

		return true;
}

static bool
parse_port(msettings *mp, scanner *sc) {
	if (sc->c == ':') {
		advance(sc);
		char *portstr = scan(sc, generic_special);
		char *end;
		long port = strtol(portstr, &end, 10);
		if (portstr[0] == '\0' || *end != '\0' || port < 1 || port > 65535)
			return complain(sc, "invalid port: '%s'", portstr);
		msettings_error msg = msetting_set_long(mp, MP_PORT, port);
		if (msg != NULL)
			return complain(sc, "could not set port: %s\n", msg);
	}
	return true;
}

static bool
parse_path(msettings *mp, scanner *sc, bool percent)
{
	// parse the database name
	if (sc->c != '/')
		return true;
	advance(sc);
	char *database = scan(sc, generic_special);
	if (percent && !percent_decode(sc, "database", database))
		return false;
	if (!store(mp, sc, MP_DATABASE, database))
		return false;

	// parse the schema name
	if (sc->c != '/')
		return true;
	advance(sc);
	char *schema = scan(sc, generic_special);
	if (percent && !percent_decode(sc, "schema", schema))
		return false;
	if (!store(mp, sc, MP_TABLESCHEMA, schema))
		return false;

	// parse the table name
	if (sc->c != '/')
		return true;
	advance(sc);
	char *table = scan(sc, generic_special);
	if (percent && !percent_decode(sc, "table", table))
		return false;
	if (!store(mp, sc, MP_TABLE, table))
		return false;

	return true;
}

static bool
parse_modern(msettings *mp, scanner *sc)
{
	if (!consume(sc, "//"))
		return false;

	// parse the host
	if (sc->c == '[') {
		advance(sc);
		char *host = sc->p;
		while (sc->c == ':' || isxdigit(sc->c))
			advance(sc);
		*sc->p = '\0';
		if (!consume(sc, "]"))
			return false;
		if (!store(mp, sc, MP_HOST, host))
			return false;
	} else {
		char *host = scan(sc, generic_special);
		if (!percent_decode(sc, "host name", host))
			return false;
		if (strcmp(host, "localhost") == 0)
			host = "";
		else if (strcmp(host, "localhost.") == 0)
			host = "localhost";
		else if (sc->c == ':' && strlen(host) == 0) {
			// cannot port number without host, so this is not allowed: monetdb://:50000
			return unexpected(sc);
		}
		if (!store(mp, sc, MP_HOST, host))
			return false;
	}

	if (!parse_port(mp, sc))
		return false;

	if (!parse_path(mp, sc, true))
		return false;

	// parse query parameters
	if (sc->c == '?') {
		do {
			advance(sc);  // skip ? or &

			char *key = NULL;
			char *value = NULL;
			if (!scan_query_parameters(sc, &key, &value))
				return false;
			assert(key && value);

			if (!percent_decode(sc, "parameter name", key))
				return false;
			if (!percent_decode(sc, key, value))
				return false;

			msettings_error msg = msetting_set_named(mp, false, key, value);
			if (msg)
				return complain(sc, "%s: %s", key, msg);
		} while (sc->c == '&');
	}

	// should have consumed everything
	if (sc->c != '\0' && sc-> c != '#')
		return unexpected(sc);

	return true;
}

static bool
parse_classic_query_parameters(msettings *mp, scanner *sc)
{
	assert(sc->c == '?');
	do {
		advance(sc); // skip & or ?

		char *key = NULL;
		char *value = NULL;
		if (!scan_query_parameters(sc, &key, &value))
			return false;
		assert(key && value);
		mparm parm = mparm_parse(key);
		msettings_error msg;
		switch (parm) {
			case MP_DATABASE:
			case MP_LANGUAGE:
				msg = msetting_set_string(mp, parm, value);
				if (msg)
					return complain(sc, "parameter '%s': %s", key, msg);
				break;
			default:
				// ignore
				break;
		}
	} while (sc->c == '&');

	return true;
}

static bool
parse_classic_tcp(msettings *mp, scanner *sc)
{
	assert(sc->c != '/');

	// parse the host
	char *host = find(sc, ":?/");
	if (strchr(host, '@') != NULL)
		return complain(sc, "host@user syntax is not allowed");
	if (!store(mp, sc, MP_HOST, host))
		return false;

	if (!parse_port(mp, sc))
		return false;

	if (!parse_path(mp, sc, false))
		return false;

	if (sc->c == '?') {
		if (!parse_classic_query_parameters(mp, sc))
			return false;
	}

	// should have consumed everything
	if (sc->c != '\0' && sc-> c != '#')
		return unexpected(sc);

	return true;
}

static bool
parse_classic_unix(msettings *mp, scanner *sc)
{
	assert(sc->c == '/');
	char *sock = find(sc, "?");

	if (!store(mp, sc, MP_SOCK, sock))
		return false;

	if (sc->c == '?') {
		if (!parse_classic_query_parameters(mp, sc))
			return false;
	}

	// should have consumed everything
	if (sc->c != '\0' && sc-> c != '#')
		return unexpected(sc);

	return true;
}

static bool
parse_classic_merovingian(msettings *mp, scanner *sc)
{
	if (!consume(sc, "mapi:merovingian://proxy"))
		return false;

	long user_gen = msettings_user_generation(mp);
	long password_gen = msettings_password_generation(mp);

	if (sc->c == '?') {
		if (!parse_classic_query_parameters(mp, sc))
			return false;
	}

	// should have consumed everything
	if (sc->c != '\0' && sc-> c != '#')
		return unexpected(sc);

	long new_user_gen = msettings_user_generation(mp);
	long new_password_gen = msettings_password_generation(mp);
	if (new_user_gen > user_gen || new_password_gen > password_gen)
		return complain(sc, "MAPI redirect is not allowed to set user or password");

	return true;
}

static bool
parse_classic(msettings *mp, scanner *sc)
{
	// we accept mapi:merovingian but we don't want to
	// expose that we do
	if (sc->p[0] == 'm' && sc->p[1] == 'e') {
		if (!consume(sc, "merovingian://proxy"))
			return false;
		return parse_classic_merovingian(mp, sc);
	}

	if (!consume(sc, "monetdb://"))
		return false;

	if (sc->c == '/')
		return parse_classic_unix(mp, sc);
	else
		return parse_classic_tcp(mp, sc);
}

static bool
parse_by_scheme(msettings *mp, scanner *sc)
{
	// process the scheme
	char *scheme = scan(sc, generic_special);
	if (sc->c == ':')
		advance(sc);
	else
		return complain(sc, "expected URL starting with monetdb:, monetdbs: or mapi:monetdb:");
	if (strcmp(scheme, "monetdb") == 0) {
		msetting_set_bool(mp, MP_TLS, false);
		return parse_modern(mp, sc);
	} else if (strcmp(scheme, "monetdbs") == 0) {
		msetting_set_bool(mp, MP_TLS, true);
		return parse_modern(mp, sc);
	} else if (strcmp(scheme, "mapi") == 0) {
		msetting_set_bool(mp, MP_TLS, false);
		return parse_classic(mp, sc);
	} else {
		return complain(sc, "unknown URL scheme '%s'", scheme);
	}
}

static bool
parse(msettings *mp, scanner *sc)
{
	// mapi:merovingian:://proxy is not like other URLs,
	// it designates the existing connection so the core properties
	// must not be cleared and user and password cannot be changed.
	bool is_mero = (strncmp(sc->p, "mapi:merovingian:", 16) == 0);

	if (!is_mero) {
		// clear existing core values
		msetting_set_bool(mp, MP_TLS, false);
		msetting_set_string(mp, MP_HOST, "");
		msetting_set_long(mp, MP_PORT, -1);
		msetting_set_string(mp, MP_DATABASE, "");
	}

	long user_gen = msettings_user_generation(mp);
	long password_gen = msettings_password_generation(mp);

	if (is_mero) {
		if (!parse_classic_merovingian(mp, sc))
			return false;
	} else {
		if (!parse_by_scheme(mp, sc))
			return false;
	}

	bool user_changed = (msettings_user_generation(mp) != user_gen);
	bool password_changed = (msettings_password_generation(mp) != password_gen);

	if (is_mero && (user_changed || password_changed))
		return complain(sc, "MAPI redirect must not change user or password");

	if (user_changed && !password_changed) {
		// clear password
		msettings_error msg = msetting_set_string(mp, MP_PASSWORD, "");
		if (msg) {
			// failed, report
			return complain(sc, "%s", msg);
		}
	}

	return true;
}

/* update the msettings from the URL. set *error_buffer to NULL and return true
 * if success, set *error_buffer to malloc'ed error message and return false on failure.
 * if return value is true but *error_buffer is NULL, malloc failed. */
bool msettings_parse_url(msettings *mp, const char *url, char **error_out)
{
	bool ok;
	scanner sc;

	// This function is all about setting up the scanner and copying
	// error messages out of it.

	if (error_out)
		*error_out = NULL;

	if (!initialize(&sc, url))
		return false;

	ok = parse(mp, &sc);
	if (!ok) {
		assert(sc.error_message[0] != '\0');
		if (error_out)
			*error_out = strdup(sc.error_message);
	}

	deinitialize(&sc);
	return ok;
}
