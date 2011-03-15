/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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

#include <monetdb_config.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <limits.h>
#include "Mx.h"
#include "disclaimer.h"

int disclaimer = 0;
char *disclaimerfile;

static const char defaultfile[] = "license.txt";

static FILE *
openDisclaimerFile(const char *filename)
{
	FILE *fp = NULL;

	if (!filename || strlen(filename) < 1)
		filename = defaultfile;

	if (*filename == DIR_SEP) {
		fp = fopen(filename, "r");
	} else {
		char buf[8096];
		size_t len;
		buf[0] = '\0';
		strncat(buf, inputdir, 8096 - 1);
		len = strlen(buf);
		if (len > 0 && len < 8095 && buf[len - 1] != DIR_SEP
#ifdef WIN32
		    && buf[len - 1] != '/'
#endif
			) {
			buf[len++] = DIR_SEP;
			buf[len] = 0;
		}
		/* search backwards, such that we can find the license.txt
		 * file in the root of each module */
		strncat(buf, filename, 8095 - len);
		while (len > 0 && (fp = fopen(buf, "r")) == 0) {
			/* remove last path */
			len--; /* the trailing slash */
			while (len > 0) {
				if (buf[--len] == '/') {
					buf[++len] = '\0';
					break;
				}
			}
			strncat(buf, filename, 8096 - len - 1);
		}
	}

	if (fp == NULL)
		fprintf(stderr, "Mx: can't open licence file '%s' (skipping).\n", filename);
	return fp;
}

static void
writeDisclaimer(FILE *fp, const char *comment_start, const char *prefix, const char *comment_end, const char *filename)
{

	FILE *dfile;
	char line[DISC_WIDTH + 2];
	size_t prefixLength = strlen(prefix), i, ret;

	dfile = openDisclaimerFile(filename);

	if (!dfile)
		return;

	if (strlen(comment_start) > 0) {
		if (!fwrite(comment_start, strlen(comment_start), 1, fp))
			return;
		if (!fwrite("\n", 1, 1, fp))
			return;
	}

	memcpy(line, prefix, prefixLength);

	while (!feof(dfile)) {
		i = prefixLength;
		do {
			ret = fread(&line[i++], 1, 1, dfile);
		}
		while (i < DISC_WIDTH && line[i - 1] != '\n' && ret);
		if (!ret)
			break;

		if (line[i - 1] != '\n')
			line[i++] = '\n';
		line[i] = '\0';
		if (!fwrite(line, strlen(line), 1, fp))
			return;
	}
	if (strlen(comment_end) > 0) {
		if (!fwrite(comment_end, strlen(comment_end), 1, fp))
			return;
		if (!fwrite("\n", 1, 1, fp))
			return;
	}

	if (!fwrite("\n", 1, 1, fp))
		return;
	fclose(dfile);

}

/* format <suffix> <comment_begin> <comment_prefix> <comment_end> */
static struct suffixes {
	const char *suffix;
	const char *comment_begin;
	const char *comment_prefix;
	const char *comment_end;
} suffixes[] = {
	{"c", "/*", " * ", " */",},
	{"h", "/*", " * ", " */",},
	{"html", "<!--", "", " -->",},
	{"tex", "", "% ", "",},
	{"mal", "", "# ", "",},
	{"mx", "", "@' ", ""},
	{0, 0, 0, 0},		/* sentinel */
};

#define DISC_SUFFIXES  (sizeof(suffixes)/sizeof(siffixes[0]))

void
insertDisclaimer(FILE *fp, char *rfilename)
{
	struct suffixes *s;
	char *suffix;

	if ((suffix = strrchr(rfilename, '.')) == 0)
		return;		/* no suffix found => no disclaimer */
	suffix++;		/* position after "." */
	for (s = suffixes; s->suffix; s++)
		if (strcmp(s->suffix, suffix) == 0) {
			writeDisclaimer(fp, s->comment_begin, s->comment_prefix, s->comment_end, disclaimerfile);
			break;
		}
}
