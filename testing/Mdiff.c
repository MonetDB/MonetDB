/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"

#include <stdlib.h>
#include <stdio.h>
#include "difflib.h"
#ifdef HAVE_IO_H
# include <io.h>
#endif
#include <string.h>

#ifdef HAVE_GETOPT
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif
#else
#include "testing_getopt.c"
#endif

static void
showUsage(char *name)
{
	printf("Usage:  %s [-I<exp>] [-F<exp>] [-C<num>] [-A<num>] [-t<text>] [-r<rev>] <oldfile> <newfile> [<outfile>]\n", name);
	printf("\n");
	printf(" -I<exp>   : ignore lines matching <exp> during diff (optional, default: -I'^#')\n");
	printf(" -F<exp>   : show the most recent line matching <exp>\n");
	printf(" -C<num>   : use <num> lines of context during diff (optional, default: -C1)\n");
	printf(" -A<num>   : accuracy for diff: 0=lines, 1=words, 2=chars (optional, default: -A1)\n");
	printf(" -d        : change the algorithm to perhaps find a smaller set of changes;\n");
	printf("             this makes diff slower (sometimes much slower)\n");
	printf(" -t<text>  : text for caption (optional, default: empty)\n");
	printf(" -r<rev>   : revision of old file (optional, default: empty)\n");
	printf(" -q        : be less verbose\n");
	printf(" <oldfile> : first file for diff\n");
	printf(" <newfile> : second file for diff\n");
	printf(" <outfile> : file for HTML output (optional, default: stdout)\n");
}

int
main(int argc, char **argv)
{
	char EMPTY[] = "";
#ifdef NATIVE_WIN32
	char DEFAULT[] = "\"-I^#\"";
#else
	char DEFAULT[] = "-I'^#'";
#endif
	char ignoreWHITE[] = " -b -B";
	char *old_fn, *new_fn, *html_fn, *caption = EMPTY, *revision = EMPTY, *ignoreEXP = DEFAULT, *ignore = NULL, *function = EMPTY;
	int LWC = 1, context = 1, option, mindiff = 0, quiet = 0;

	while ((option = getopt(argc, argv, "hdqA:C:I:F:t:r:")) != EOF)
		switch (option) {
		case 'd':
			mindiff = 1;
			break;
		case 'A':
			LWC = atoi(optarg);
			break;
		case 'C':
			context = atoi(optarg);

			break;
		case 'I':
			ignoreEXP = (char *) malloc(strlen(optarg) + 6);
#ifdef NATIVE_WIN32
			strcpy(ignoreEXP, "\"-I");
#else
			strcpy(ignoreEXP, "'-I");
#endif
			strcat(ignoreEXP, optarg);
#ifdef NATIVE_WIN32
			strcat(ignoreEXP, "\"");
#else
			strcat(ignoreEXP, "'");
#endif
			break;
		case 'F':
			function = malloc(strlen(optarg) + 6);
#ifdef NATIVE_WIN32
			strcpy(function, "\"-F");
#else
			strcpy(function, "'-F");
#endif
			strcat(function, optarg);
#ifdef NATIVE_WIN32
			strcat(function, "\"");
#else
			strcat(function, "'");
#endif
			break;
		case 't':
			caption = optarg;
			break;
		case 'r':
			revision = optarg;
			break;
		case 'q':
			quiet = 1;
			break;
		case 'h':
		default:
			showUsage(argv[0]);
			if (ignoreEXP != DEFAULT)
				free(ignoreEXP);
			if (function != EMPTY)
				free(function);
			exit(1);
		}

	ignore = (char *) malloc(strlen(ignoreEXP) + strlen(ignoreWHITE) + 2);
	strcpy(ignore, ignoreEXP);
	strcat(ignore, ignoreWHITE);

	optind--;
	old_fn = ((argc > (++optind)) ? argv[optind] : "-");
	new_fn = ((argc > (++optind)) ? argv[optind] : "-");
	html_fn = ((argc > (++optind)) ? argv[optind] : "-");

	TRACE(fprintf(STDERR, "%s %s -A %i -C %i %s %s -t %s -r %s  %s %s %s\n", argv[0], mindiff ? "-d" : "", LWC, context, ignore, function, caption, revision, old_fn, new_fn, html_fn));

	switch (oldnew2html(mindiff, LWC, context, ignore, function, old_fn, new_fn, html_fn, caption, revision)) {
	case 0:
		if (quiet == 0)
			fprintf(STDERR, "%s and %s are equal.\n", old_fn, new_fn);
		break;
	case 1:
		if (quiet != 0) {
			fprintf(STDERR, "\n+ (%s) slightly\n", new_fn);
		} else {
			fprintf(STDERR, "%s and %s differ slightly.\n", old_fn, new_fn);
		}
		break;
	case 2:
		if (quiet != 0) {
			fprintf(STDERR, "\n* (%s) significantly\n", new_fn);
		} else {
			fprintf(STDERR, "%s and %s differ SIGNIFICANTLY!\n", old_fn, new_fn);
		}
		break;
	}
	free(ignore);
	if (ignoreEXP != DEFAULT)
		free(ignoreEXP);
	if (function != EMPTY)
		free(function);

	TRACE(fprintf(STDERR, "done.\n"));
	return 0;
}
