/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "helpers.h"
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#ifndef DIR_SEP
# define DIR_SEP '/'
#endif

void
ErrXit(char *text1, char *text2, int num)
{
	fprintf(stderr, "ERROR: %s%s\n", text1, text2);
	exit(num);
}

/* ErrXit */


FILE *
Rfopen(char *name)
{
	FILE *fp;

	if (!strcmp(name, "-"))
		fp = stdin;
	else if (!(fp = fopen(name, "r")))
		ErrXit("could not read file ", name, 1);
	return fp;
}

/* Rfopen */


FILE *
Wfopen(char *name)
{
	FILE *fp;

	if (!strcmp(name, "-"))
		fp = stdout;
	else if (!(fp = fopen(name, "w")))
		ErrXit("could not write file ", name, 1);
	return fp;
}

/* Wfopen */


FILE *
Afopen(char *name)
{
	FILE *fp;

	if (!strcmp(name, "-"))
		fp = stdout;
	else if (!(fp = fopen(name, "a")))
		ErrXit("could not append file ", name, 1);
	return fp;
}

/* Afopen */


char *
filename(char *path)
{
	char *fn = strrchr(path, (int) DIR_SEP);

	if (fn)
		return (fn + 1);
	else
		return path;
}

#if DIR_SEP == '/'
char *default_tmpdir = "/tmp";

#define TMPDIR_ENV "TMPDIR"
#else
char *default_tmpdir = "C:\\Temp";

#define TMPDIR_ENV "TEMP"
#endif

char *
tmpdir(void)
{
	char *rtrn = getenv(TMPDIR_ENV);

	if (!rtrn)
		rtrn = default_tmpdir;
	return rtrn;
}
