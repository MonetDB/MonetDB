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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "dotmonetdb.h"
#include <stdio.h>
#ifdef HAVE_STRING_H
#include <string.h>
#endif
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif

void
parse_dotmonetdb(char **user, char **passwd, char **language, int *save_history, char **output, int *pagewidth)
{
	char *cfile;
	FILE *config = NULL;
	char buf[1024];

	if ((cfile = getenv("DOTMONETDBFILE")) == NULL) {
		/* no environment variable: use a default */
		if ((config = fopen(".monetdb", "r")) == NULL) {
			if ((cfile = getenv("HOME")) != NULL) {
				snprintf(buf, sizeof(buf), "%s%c.monetdb", cfile, DIR_SEP);
				config = fopen(buf, "r");
				if (config)
					cfile = strdup(buf);
				else
					cfile = NULL;
			}
		} else {
			cfile = strdup(".monetdb");
		}
	} else if (*cfile == 0) {
		/* empty environment variable: skip the file */
		cfile = NULL;
	} else if ((config = fopen(cfile, "r")) == NULL) {
		cfile = NULL;
		fprintf(stderr, "failed to open file '%s': %s\n",
			cfile, strerror(errno));
	} else {
		cfile = strdup(cfile);
	}

	if (user)
		*user = NULL;
	if (passwd)
		*passwd = NULL;
	if (language)
		*language = NULL;
	if (output)
		*output = NULL;
	if (save_history)
		*save_history = 0;
	if (pagewidth)
		*pagewidth = 0;

	if (config) {
		int line = 0;
		char *q;
		while (fgets(buf, sizeof(buf), config) != NULL) {
			line++;
			q = strchr(buf, '\n');
			if (q)
				*q = 0;
			if (buf[0] == '\0' || buf[0] == '#')
				continue;
			if ((q = strchr(buf, '=')) == NULL) {
				fprintf(stderr, "%s:%d: syntax error: %s\n",
					cfile, line, buf);
				continue;
			}
			*q++ = '\0';
			/* this basically sucks big time, as I can't easily set
			 * a default, hence I only do things I think are useful
			 * for now, needs a better solution */
			if (strcmp(buf, "user") == 0) {
				if (user)
					*user = strdup(q);
				q = NULL;
			} else if (strcmp(buf, "password") == 0) {
				if (passwd)
					*passwd = strdup(q);
				q = NULL;
			} else if (strcmp(buf, "language") == 0) {
				/* make sure we don't set garbage */
				if (strcmp(q, "sql") != 0 &&
				    strcmp(q, "mal") != 0 &&
				    strcmp(q, "jaql") != 0) {
					fprintf(stderr, "%s:%d: unsupported "
						"language: %s\n",
						cfile, line, q);
				} else if (language)
					*language = strdup(q);
				q = NULL;
			} else if (strcmp(buf, "save_history") == 0) {
				if (strcmp(q, "true") == 0 ||
				    strcmp(q, "on") == 0) {
					if (save_history)
						*save_history = 1;
					q = NULL;
				} else if (strcmp(q, "false") == 0 ||
					   strcmp(q, "off") == 0) {
					if (save_history)
						*save_history = 0;
					q = NULL;
				}
			} else if (strcmp(buf, "format") == 0) {
				if (output)
					*output = strdup(q);
				q = NULL;
			} else if (strcmp(buf, "width") == 0) {
				if (pagewidth)
					*pagewidth = atoi(q);
				q = NULL;
			}
			if (q != NULL)
				fprintf(stderr, "%s:%d: unknown property: %s\n",
					cfile, line, buf);
		}
	}
	if (cfile)
		free(cfile);
	if (config)
		fclose(config);
}
