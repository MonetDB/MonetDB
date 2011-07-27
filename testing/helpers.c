/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
