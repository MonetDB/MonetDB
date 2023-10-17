#define _POSIX_C_SOURCE 200809L

#include "murltest.h"
#include "msettings.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

// love it when a task is so straightforward I can use global variables!
static int start_line = -1;
static int nstarted = 0;
static msettings *mp = NULL;

static bool
handle_parse_command(const char *location, char *url)
{
	char *errmsg = NULL;
	bool ok = msettings_parse_url(mp, url, &errmsg);
	if (!ok) {
		assert(errmsg);
		fprintf(stderr, "%s: %s\n", location, errmsg);
		free(errmsg);
		return false;
	}
	return true;
}

static bool
handle_accept_command(const char *location, char *url)
{
	char *errmsg = NULL;
	bool ok = msettings_parse_url(mp, url, &errmsg);
	if (!ok) {
		assert(errmsg);
		fprintf(stderr, "%s: %s\n", location, errmsg);
		free(errmsg);
		return false;
	}

	char *msg = NULL;
	if (!msettings_validate(mp, &msg)) {
		fprintf(stderr, "%s: URL invalid: %s\n", location, msg);
		free(msg);
		return false;
	}
	return true;
}

static bool
handle_reject_command(const char *location, char *url)
{
	bool ok = msettings_parse_url(mp, url, NULL);
	if (!ok)
		return true;

	char *msg = NULL;
	if (!msettings_validate(mp, &msg)) {
		free(msg);
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
		fprintf(stderr, "%s: cannot set '%s': %s\n", location, key, msg);
		return false;
	}
	return true;
}

static bool
ensure_valid(const char *location) {
	char *msg = NULL;
	if (msettings_validate(mp, &msg))
		return true;
	fprintf(stderr, "%s: invalid parameter state: %s\n", location, msg);
	free(msg);
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

		char * msg = NULL;
		bool actually_valid = msettings_validate(mp, &msg);
		free(msg);
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
run_tests_inner(const char *filename, FILE *f, int verbose)
{
	int orig_nstarted = nstarted;
	char *location = malloc(strlen(filename) + 100);
	strcpy(location, filename);
	char *location_lineno = &location[strlen(filename)];
	*location_lineno++ = ':';
	*location_lineno = '\0';

	errno = 0;

	int lineno = 0;
	char *line_buffer = NULL;
	size_t line_buffer_size = 0;

	while (true) {
		lineno++;
		sprintf(location_lineno, "%d", lineno);
		ssize_t nread = getline(&line_buffer, &line_buffer_size, f);
		if (nread < 0) {
			if (errno) {
				fprintf(stderr, "%s: %s\n", location, strerror(errno));
				free(line_buffer);
				free(location);
				return false;
			} else {
				break;
			}
		}
		if (!handle_line(lineno, location, line_buffer, verbose)) {
			free(line_buffer);
			free(location);
			return false;
		}
	}

	if (mp) {
		fprintf(stderr, "%s:%d: unterminated code block starts here\n", filename, start_line);
		free(line_buffer);
		free(location);
		return false;
	}

	if (verbose >= 1) {
		fprintf(stderr, "ran %d succesful tests from %s\n", nstarted - orig_nstarted, filename);
	}

	free(line_buffer);
	free(location);
	return true;
}

bool
run_tests(const char *filename, FILE *f, int verbose)
{
	assert(mp == NULL);
	bool ok = run_tests_inner(filename, f, verbose);
	if (mp) {
		msettings_destroy(mp);
		mp = NULL;
	}
	return ok;
}
