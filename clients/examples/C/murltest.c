/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#define _POSIX_C_SOURCE 200809L

#include "murltest.h"

#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

char *USAGE = "Usage: murltest TESTFILES..";

static bool
run_file(const char *filename, int verbose)
{
	stream *s;
	if (strcmp(filename, "-") == 0) {
		s = stdin_rastream();
	} else {
		s = open_rastream(filename);
		if (!s || mnstr_errnr(s) != MNSTR_NO__ERROR) {
			fprintf(stderr, "Could not open %s: %s\n", filename, mnstr_peek_error(s));
			return false;
		}
	}

	bool ok = run_tests(s, verbose);

	close_stream(s);
	return ok;
}

static bool run_files(char **files, int verbose)
{
	while (*files) {
		if (!run_file(*files, verbose))
			return false;
		files++;
	}
	return true;
}

int
main(int argc, char **argv)
{
	int verbose = 0;

	if (mnstr_init() != 0) {
		fprintf(stderr, "could not initialize libstream\n");
		return 1;
	}

	char **files = calloc(argc + 1, sizeof(char*));
	if (!files)
		return 3;
	char **next_slot = &files[0];
	for (int i = 1; i < argc; i++) {
		char *arg = argv[i];
		if (arg[0] != '-') {
			*next_slot++ = arg;
			continue;
		}
		if (arg[1] == 'v') {
			char *p = &arg[1];
			while (*p == 'v') {
				p++;
				verbose++;
			}
			if (*p == '\0')
				continue;
			fprintf(stderr, "invalid letter %c in flag %s\n", *p, arg);
			free(files);
			return 1;
		} else {
			fprintf(stderr, "invalid flag %s\n", arg);
			free(files);
			return 1;
		}
	}

	bool ok = run_files(files, verbose);

	free(files);
	return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
