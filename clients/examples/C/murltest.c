#define _POSIX_C_SOURCE 200809L

#include "murltest.h"

#include <errno.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

char *USAGE = "Usage: murltest TESTFILES..";

static bool
run_file(const char *filename, int verbose)
{
	FILE *to_close, *f;
	if (strcmp(filename, "-") == 0) {
		f = stdin;
		to_close = NULL;
	} else {
		f = fopen(filename, "r");
		if (!f) {
			fprintf(stderr, "Could not open %s: %s\n", filename, strerror(errno));
			return false;
		}
		to_close = f;
	}

	bool ok = run_tests(filename, f, verbose);

	if (to_close)
		fclose(to_close);
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
