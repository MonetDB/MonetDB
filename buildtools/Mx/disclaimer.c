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
 * Portions created by CWI are Copyright (C) 1997-2007 CWI.
 * All Rights Reserved.
 */

#include <mx_config.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <limits.h>
#include "disclaimer.h"

int disclaimer = 0;
char *disclaimerfile;

static const char defaultfile[] = "COPYRIGHT";

static FILE *
openDisclaimerFile(const char *filename)
{
	FILE *fp;

	if (!filename || strlen(filename) < 1)
		filename = defaultfile;

	if ((fp = fopen(filename, "r")) == 0)
		fprintf(stderr, "Mx: can't open disclaimer file '%s' (skipping).\n", filename);
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
		fwrite(comment_start, strlen(comment_start), 1, fp);
		fwrite("\n", 1, 1, fp);
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
		fwrite(line, strlen(line), 1, fp);
	}
	if (strlen(comment_end) > 0) {
		fwrite(comment_end, strlen(comment_end), 1, fp);
		fwrite("\n", 1, 1, fp);
	}

	fwrite("\n", 1, 1, fp);
	fclose(dfile);

}

/* format <suffix> <comment_begin> <comment_prefix> <comment_end> */
static struct suffixes {
	const char *suffix;
	const char *comment_begin;
	const char *comment_prefix;
	const char *comment_end;
} suffixes[] = {
	{
	"c", "/*", " * ", " */",}, {
	"h", "/*", " * ", " */",}, {
	MX_CXX_SUFFIX, "/*", " * ", " */",}, {
	"html", "<!--", " + ", " -->",}, {
	"tex", "", "% ", "",}, {
	"mil", "", "# ", "",}, {
	"m", "", "# ", "",}, {
	"mx", "", "@' ", ""}, {
	0, 0, 0, 0},		/* sentinel */
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
