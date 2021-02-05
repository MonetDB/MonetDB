/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "dotmonetdb.h"
#define LIBMUTILS 1
#include "mutils.h"
#include <string.h>

void
parse_dotmonetdb(DotMonetdb *dotfile)
{
	char *cfile;
	FILE *config = NULL;
	char buf[FILENAME_MAX];

	if (dotfile == NULL)
		return;
	/* if environment variable DOTMONETDBFILE is set, use it */
	/* 1. use $DOTMONETDBFILE (if set but empty do not read config file);
	 * 2. use .monetdb;
	 * 3. use ${XDG_CONFIG_HOME-$HOME/.config}/monetdb;
	 * 4. use $HOME/.monetdb
	 * (3 is standard shell syntax: use XDG_CONFIG_HOME if set, else use
	 * $HOME/.config in its place)
	 */
	if ((cfile = getenv("DOTMONETDBFILE")) == NULL) {
		/* no environment variable: use a default */
		cfile = ".monetdb";
		if ((config = MT_fopen(cfile, "r")) == NULL) {
			const char *xdg = getenv("XDG_CONFIG_HOME");
			const char *home = getenv("HOME");
			int len = -1;
			cfile = buf;
			if (xdg != NULL)
				len = snprintf(buf, sizeof(buf), "%s%cmonetdb", xdg, DIR_SEP);
			else if (home != NULL)
				len = snprintf(buf, sizeof(buf), "%s%c.config%cmonetdb", home, DIR_SEP, DIR_SEP);
			if (len == -1 || len >= FILENAME_MAX || (config = MT_fopen(buf, "r")) == NULL) {
				if (home) {
					len = snprintf(buf, sizeof(buf), "%s%c.monetdb", home, DIR_SEP);
					if (len >= 0 && len < FILENAME_MAX)
						config = MT_fopen(buf, "r");
				}
			}
		}
	} else if (*cfile != 0 && (config = MT_fopen(cfile, "r")) == NULL) {
		fprintf(stderr, "failed to open file '%s': %s\n",
			cfile, strerror(errno));
	}

	*dotfile = (DotMonetdb) {0};

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
				dotfile->user = strdup(q);
				q = NULL;
			} else if (strcmp(buf, "password") == 0) {
				dotfile->passwd = strdup(q);
				q = NULL;
			} else if (strcmp(buf, "database") == 0) {
				dotfile->dbname = strdup(q);
				q = NULL;
                        } else if (strcmp(buf, "host") == 0) {
				dotfile->host = strdup(q);
				q = NULL;
			} else if (strcmp(buf, "language") == 0) {
				/* make sure we don't set garbage */
				if (strcmp(q, "sql") != 0 &&
				    strcmp(q, "mal") != 0) {
					fprintf(stderr, "%s:%d: unsupported "
						"language: %s\n",
						cfile, line, q);
				}
				dotfile->language = strdup(q);
				q = NULL;
			} else if (strcmp(buf, "save_history") == 0) {
				if (strcmp(q, "true") == 0 ||
				    strcmp(q, "on") == 0) {
					dotfile->save_history = true;
					q = NULL;
				} else if (strcmp(q, "false") == 0 ||
					   strcmp(q, "off") == 0) {
					dotfile->save_history = false;
					q = NULL;
				}
			} else if (strcmp(buf, "format") == 0) {
				dotfile->output = strdup(q);
				q = NULL;
			} else if (strcmp(buf, "width") == 0) {
				dotfile->pagewidth = atoi(q);
				q = NULL;
                        } else if (strcmp(buf, "port") == 0) {
				dotfile->port = atoi(q);
				q = NULL;
			}
			if (q != NULL)
				fprintf(stderr, "%s:%d: unknown property: %s\n",
					cfile, line, buf);
		}
		fclose(config);
	}
}
