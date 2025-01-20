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

#include "murltest.h"
#include "msettings.h"
#include "stream.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// love it when a task is so straightforward I can use global variables!
static int start_line = -1;
static int nstarted = 0;
static msettings *mp = NULL;

static
bool verify_roundtrip(const char *location)
{
	const char ch = '*';
	char buffer[1000 + 1];  // + 1 canary byte
	memset(buffer, ch, sizeof(buffer));
	const size_t buffer_size = sizeof(buffer) - 1;

	size_t length = msettings_write_url(mp, buffer, buffer_size);
	if (length == 0) {
		fprintf(stderr, "%s: msettings_write_url returned 0\n", location);
		return false;
	}
	if (length > buffer_size - 1) {
		fprintf(stderr, "%s: Reconstructed the URL unexpectedly large: %zu\n", location, length);
		return false;
	}
	if (memchr(buffer, '\0', buffer_size) == NULL) {
		fprintf(stderr, "%s: msettings_write_url didn't NUL terminate the result\n", location);
		return false;
	}
	if (buffer[buffer_size] != ch) {
		fprintf(stderr, "%s: msettting_write_url wrote beyond the end of the buffer\n", location);
		return false;
	}

	msettings *tmp = msettings_create();
	if (tmp == NULL) {
		fprintf(stderr, "malloc failed\n");
		return false;
	}
	msettings_error err = msettings_parse_url(tmp, buffer);
	if (err) {
		fprintf(stderr, "%s: Reconstructed URL <%s> couldn't be parsed: %s", location, buffer, err);
		msettings_destroy(tmp);
		return false;
	}

	mparm parm;
	bool ok = true;
	for (int i = 0; (parm = mparm_enumerate(i)) != MP_UNKNOWN; i++) {
		if (parm == MP_IGNORE)
			continue;
		char scratch1[100], scratch2[100];
		const char *mp_val = msetting_as_string(mp, parm, scratch1, sizeof(scratch1));
		const char *tmp_val = msetting_as_string(tmp, parm, scratch2, sizeof(scratch2));
		if (strcmp(mp_val, tmp_val) != 0) {
			fprintf(
				stderr,
				"%s: setting %s: reconstructed value <%s> != <%s>\n",
				location, mparm_name(parm), tmp_val, mp_val);
			ok = false;
		}
	}
	msettings_destroy(tmp);
	if (!ok)
		return false;

	// check if rendering to a smaller buffer returns the same length
	// and writes a prefix of the original.

	assert(length > 0); // we checked this above

	char buffer2[sizeof(buffer)];
	for (size_t shorter = length; shorter > 0; shorter--) {
		memset(buffer2, ch, sizeof(buffer));
		size_t n = msettings_write_url(mp, buffer2, shorter);
		if (n != length) {
			fprintf(
				stderr,\
				"%s: writing to buffer of size %zu returns %zu, expected %zu\n",
				location, shorter, n, length);
			return false;
		}
		char *first_nul = memchr(buffer2, '\0', shorter);
		if (first_nul == NULL) {
			fprintf(stderr, "%s: truncated <%zu> msettings_write_url didn't NUL terminate\n", location, shorter);
			return false;
		} else if (strncmp(buffer2, buffer, shorter - 1) != 0) {
			fprintf(stderr,
			"%s: truncated <%zu> msettings_write_url wrote <%s> which isn't a prefix of <%s>",
			location, shorter,
			buffer2, buffer
			);
			return false;
		}
		for (size_t i = shorter + 1; i < sizeof(buffer); i++) {
			if (buffer2[i] != ch) {
				fprintf(
					stderr,
					"%s: truncated <%zu> wsettings_write_url wrote beyond end of buffer (pos %zu)\n",
					location, shorter, i);
				return false;
			}
		}
	}

	return true;
}

static bool
handle_parse_command(const char *location, char *url)
{
	const char *errmsg = msettings_parse_url(mp, url);
	if (errmsg) {
		fprintf(stderr, "%s: %s\n", location, errmsg);
		return false;
	}

	return verify_roundtrip(location);
}

static bool
handle_accept_command(const char *location, char *url)
{
	const char *errmsg = msettings_parse_url(mp, url);
	if (errmsg) {
		fprintf(stderr, "%s: %s\n", location, errmsg);
		return false;
	}

	const char *msg = msettings_validate(mp);
	if (msg != NULL) {
		fprintf(stderr, "%s: URL invalid: %s\n", location, msg);
		return false;
	}
	return verify_roundtrip(location);
}

static bool
handle_reject_command(const char *location, char *url)
{
	const char *errmsg = msettings_parse_url(mp, url);
	if (errmsg)
		return true;

	if (msettings_validate(mp) != NULL) {
		return true;
	}

	fprintf(stderr, "%s: expected URL to be rejected.\n", location);
	return false;
}

static bool
handle_set_command(const char *location, const char *key, const char *value)
{
	msettings_error msg = msetting_set_named(mp, true, key, value);
	if (msg) {
		fprintf(stderr, "%s: %s\n", location, msg);
		return false;
	}
	if (msettings_validate(mp) == NULL)
		return verify_roundtrip(location);
	else
		return true;
}

static bool
ensure_valid(const char *location) {
	const char *msg = msettings_validate(mp);
	if (msg == NULL)
		return true;
	fprintf(stderr, "%s: invalid parameter state: %s\n", location, msg);
	return false;
}

static bool
expect_bool(const char *location, const mparm parm, bool (*extract)(const msettings*), const char *value)
{
	int x = msetting_parse_bool(value);
	if (x < 0) {
		fprintf(stderr, "%s: syntax error: invalid bool '%s'\n", location, value);
	}
	bool b = x > 0;

	bool actual;
	if (extract) {
		if (!ensure_valid(location))
			return false;
		actual = extract(mp);
	} else {
		actual = msetting_bool(mp, parm);
	}

	if (actual == b)
		return true;

	char *b_ = b ? "true" : "false";
	char *actual_ = actual ? "true" : "false";
	fprintf(stderr, "%s: expected %s, found %s\n", location, b_, actual_);
	return false;

}

static bool
expect_long(const char *location, const mparm parm, long (*extract)(const msettings*), const char *value)
{
	if (strlen(value) == 0) {
		fprintf(stderr, "%s: syntax error: integer value cannot be empty string\n", location);
		return false;
	}
	char *end = NULL;
	long n = strtol(value, &end, 10);
	if (*end != '\0') {
		fprintf(stderr, "%s: syntax error: invalid integer '%s'\n", location, value);
		return false;
	}

	long actual;
	if (extract) {
		if (!ensure_valid(location))
			return false;
		actual = extract(mp);
	} else {
		actual = msetting_long(mp, parm);
	}

	if (actual == n)
		return true;

	fprintf(stderr, "%s: expected %ld, found %ld\n", location, n, actual);
	return false;
}

static bool
expect_string(const char *location, const mparm parm, const char *(*extract)(const msettings*), const char *value)
{
	const char *actual;
	if (extract) {
		if (!ensure_valid(location))
			return false;
		actual = extract(mp);
	} else {
		actual = msetting_string(mp, parm);
	}

	if (strcmp(actual, value) == 0)
		return true;

	fprintf(stderr, "%s: expected '%s', found '%s'\n", location, value, actual);
	return false;
}

static const char *
stringify_tls_verify(const msettings *mp)
{
	enum msetting_tls_verify verify = msettings_connect_tls_verify(mp);
	switch (verify) {
		case verify_none:
			return "";
		case verify_system:
			return "system";
		case verify_cert:
			return "cert";
		case verify_hash:
			return "hash";
		default:
			assert(0 && "unknown msetting_tls_verify value");
			return NULL;
	}
	assert(0 && "unreachable");
}

static bool
handle_expect_command(const char *location, char *key, char *value)
{
	if (strcmp("valid", key) == 0) {
		int x = msetting_parse_bool(value);
		if (x < 0) {
			fprintf(stderr, "%s: invalid boolean value: %s\n", location, value);
			return false;
		}
		bool expected_valid = x > 0;

		bool actually_valid = msettings_validate(mp) == NULL;
		if (actually_valid != expected_valid) {
			fprintf(stderr, "%s: expected '%s', found '%s'\n",
				location,
				expected_valid ? "true" : "false",
				actually_valid ? "true" : "false"
			);
			return false;
		}
		return true;
	}

	if (strcmp("connect_scan", key) == 0)
		return expect_bool(location, MP_UNKNOWN, msettings_connect_scan, value);
	if (strcmp("connect_unix", key) == 0)
		return expect_string(location, MP_UNKNOWN, msettings_connect_unix, value);
	if (strcmp("connect_tcp", key) == 0)
		return expect_string(location, MP_UNKNOWN, msettings_connect_tcp, value);
	if (strcmp("connect_port", key) == 0)
		return expect_long(location, MP_UNKNOWN, msettings_connect_port, value);
	if (strcmp("connect_tls_verify", key) == 0)
		return expect_string(location, MP_UNKNOWN, stringify_tls_verify, value);
	if (strcmp("connect_certhash_digits", key) == 0)
		return expect_string(location, MP_UNKNOWN, msettings_connect_certhash_digits, value);
	if (strcmp("connect_binary", key) == 0)
		return expect_long(location, MP_UNKNOWN, msettings_connect_binary, value);
	if (strcmp("connect_clientkey", key) == 0)
		return expect_string(location, MP_UNKNOWN, msettings_connect_clientkey, value);
	if (strcmp("connect_clientcert", key) == 0)
		return expect_string(location, MP_UNKNOWN, msettings_connect_clientcert, value);

	const mparm parm = mparm_parse(key);
	if (parm == MP_UNKNOWN) {
		fprintf(stderr, "%s: unknown parameter '%s'\n:", location, key);
		return false;
	}
	if (parm == MP_IGNORE) {
		if (strncmp(key, "connect_", 8) == 0)
			fprintf(stderr, "%s: unknown virtual parameter '%s'\n", location, key);
		else
			fprintf(stderr, "%s: EXPECTing ignored parameters is not supported yet\n", location);
		return false;
	}

	switch (mparm_classify(parm)) {
		case MPCLASS_BOOL:
			return expect_bool(location, parm, NULL, value);
		case MPCLASS_LONG:
			return expect_long(location, parm, NULL, value);
		case MPCLASS_STRING:
			return expect_string(location, parm, NULL, value);
		default:
			fprintf(stderr, "%s: internal error: unclassified parameter %d\n", location, (int)parm);
			return false;
	}
}




static bool
handle_line(int lineno, const char *location, char *line, int verbose)
{
	// first trim trailing whitespace
	size_t n = strlen(line);
	while (n > 0 && isspace(line[n - 1]))
		n--;
	line[n] = '\0';

	if (mp == NULL) {
		// not in a code block
		if (strcmp(line, "```test") == 0) {
			// block starts here
			nstarted++;
			start_line = lineno;
			mp = msettings_create();
			if (mp == NULL) {
				fprintf(stderr, "%s: malloc failed\n", location);
				return false;
			}
			if (verbose >= 2)
				fprintf(stderr, "· %s\n", location);
			return true;
		} else {
			// ignore
			return true;
		}
	}

	// we're in a code block, does it end here?
	if (strlen(line) > 0 && line[0] == '`') {
		if (strcmp(line, "```") == 0) {
			// lone backticks, block ends here
			msettings_destroy(mp);
			mp = NULL;
			if (verbose >= 3)
				fprintf(stderr, "\n");
			return true;
		} else {
			fprintf(stderr, "%s: unexpected backtick\n", location);
			return false;
		}
	}

	// this is line from a code block
	if (verbose >= 3)
		fprintf(stderr, "%s\n", line);
	const char *whitespace = " \t";
	char *command = strtok(line, whitespace);
	if (command == NULL) {
		// empty line
		return true;
	} else if (strcasecmp(command, "ONLY") == 0) {
		char *impl = strtok(NULL, " \n");
		if (impl) {
			if (strcmp(impl, "libmapi") != 0) {
				// ONLY command is not about us. End the block here
				msettings_destroy(mp);
				mp = NULL;
			}
			return true;
		}
		// if !impl we print an error below
	} else if (strcasecmp(command, "NOT") == 0) {
		char *impl = strtok(NULL, " \n");
		if (impl) {
			if (strcmp(impl, "libmapi") == 0) {
				// NOT command is about us. End the block here.
				msettings_destroy(mp);
				mp = NULL;
			}
			return true;
		}
		// if !impl we print an error below
	} else if (strcasecmp(command, "PARSE") == 0) {
		char *url = strtok(NULL, "\n");
		if (url)
			return handle_parse_command(location, url);
	} else if (strcasecmp(command, "ACCEPT") == 0) {
		char *url = strtok(NULL, "\n");
		if (url)
			return handle_accept_command(location, url);
	} else if (strcasecmp(command, "REJECT") == 0) {
		char *url = strtok(NULL, "\n");
		if (url)
			return handle_reject_command(location, url);
	} else if (strcasecmp(command, "SET") == 0) {
		char *key = strtok(NULL, "=");
		char *value = strtok(NULL, "\n");
		if (key)
			return handle_set_command(location, key, value ? value : "");
	} else if (strcasecmp(command, "EXPECT") == 0) {
		char *key = strtok(NULL, "=");
		char *value = strtok(NULL, "\n");
		if (key)
			return handle_expect_command(location, key, value ? value : "");
	} else {
		fprintf(stderr, "%s: unknown command: %s\n", location, command);
		return false;
	}

	// if we get here, url or key was not present
	fprintf(stderr, "%s: syntax error\n", location);
	return false;
}

static bool
run_tests_inner(stream *s, int verbose)
{
	int orig_nstarted = nstarted;
	const char *filename = mnstr_name(s);
	char *location = malloc(strlen(filename) + 100);
	strcpy(location, filename);
	char *location_lineno = &location[strlen(filename)];
	*location_lineno++ = ':';
	*location_lineno = '\0';
	char line_buffer[1024];

	errno = 0;

	int lineno = 0;

	while (true) {
		lineno++;
		sprintf(location_lineno, "%d", lineno);
		ssize_t nread = mnstr_readline(s, line_buffer, sizeof(line_buffer));
		if (nread == 0)
			break;
		if (nread < 0) {
			if (errno) {
				fprintf(stderr, "%s: %s\n", location, strerror(errno));
				free(location);
				return false;
			} else {
				break;
			}
		} else if (nread >= (ssize_t)sizeof(line_buffer) - 2) {
			fprintf(stderr, "%s: line too long\n", location);

		}
		if (!handle_line(lineno, location, line_buffer, verbose)) {
			free(location);
			return false;
		}
	}

	if (mp) {
		fprintf(stderr, "%s:%d: unterminated code block starts here\n", filename, start_line);
		free(location);
		return false;
	}

	if (verbose >= 1) {
		fprintf(stderr, "ran %d successful tests from %s\n", nstarted - orig_nstarted, filename);
	}

	free(location);
	return true;
}

bool
run_tests(stream *s, int verbose)
{
	assert(mp == NULL);
	bool ok = run_tests_inner(s, verbose);
	if (mp) {
		msettings_destroy(mp);
		mp = NULL;
	}
	return ok;
}
