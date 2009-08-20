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
 * Copyright August 2008-2009 MonetDB B.V.
 * All Rights Reserved.
 */

#include "sql_config.h"
#include "mal_sabaoth.h"
#include <stdio.h> /* fprintf, rename */
#include <unistd.h> /* stat, rmdir, unlink, ioctl */
#include <errno.h>

static char seedChars[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
	'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
	'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
	'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
	'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};

char* db_create(char *dbfarm, char* dbname, char maintenance) {
	sabdb *stats;
	char* e;
	size_t c;
	char buf[8096];
	char path[8096];
	FILE *f;
	unsigned int size;

	/* check if dbname matches [A-Za-z0-9-_]+ */
	if (dbname[0] == '\0')
		return(strdup("database name should not be an empty string"));
	for (c = 0; dbname[c] != '\0'; c++) {
		if (
				!(dbname[c] >= 'A' && dbname[c] <= 'Z') &&
				!(dbname[c] >= 'a' && dbname[c] <= 'z') &&
				!(dbname[c] >= '0' && dbname[c] <= '9') &&
				!(dbname[c] == '-') &&
				!(dbname[c] == '_')
		   )
		{
			snprintf(buf, sizeof(buf), "create: invalid character "
					"'%c' at " SZFMT " in database name '%s'",
					dbname[c], c, dbname);
			return(strdup(buf));
		}
	}

	/* the argument is the database to create, see what Sabaoth can
	 * tell us about it */
	if ((e = SABAOTHgetStatus(&stats, dbname)) != MAL_SUCCEED) {
		snprintf(buf, sizeof(buf), "internal error: %s", e);
		GDKfree(e);
		return(strdup(buf));
	}

	/* if sabaoth doesn't know, then it's green light for us! */
	if (stats != NULL) {
		SABAOTHfreeStatus(&stats);
		snprintf(buf, sizeof(buf), "database '%s' already exists", dbname);
		return(strdup(buf));
	}

	/* create the directory */
	c = snprintf(path, sizeof(path), "%s/%s", dbfarm, dbname);
	if (c >= sizeof(path))
		return(strdup("path/dbname combination too long, "
				"path would get truncated"));
	if (mkdir(path, 0755) == -1) {
		snprintf(buf, sizeof(buf), "unable to create %s: %s",
				dbname, strerror(errno));
		return(strdup(buf));
	}

	/* perform another length check, with the .maintenance file,
	 * which happens to be the longest */
	c = snprintf(path, sizeof(path), "%s/%s/.maintenance",
			dbfarm, dbname);
	if (c >= sizeof(path)) {
		/* try to cleanup */
		snprintf(path, sizeof(path), "%s/%s", dbfarm, dbname);
		rmdir(path);
		return(strdup("path/dbname combination too long, "
				"filenames inside would get truncated"));
	}

	/* if we should put this database in maintenance, make sure
	 * no race condition ever can happen, by putting it into
	 * maintenance before it even exists for Merovingian */
	if (maintenance == 1)
		fclose(fopen(path, "w"));

	/* avoid GDK making fugly complaints */
	snprintf(path, sizeof(path), "%s/%s/.gdk_lock", dbfarm, dbname);
	f = fopen(path, "w");
	/* to all insanity, .gdk_lock is "valid" if it contains a
	 * ':', which it does by pure coincidence of time having a
	 * ':' in there twice... */
	if (fwrite("bla:", 1, 4, f) < 4) {
		snprintf(buf, sizeof(buf), "cannot write lock file: %s",
				strerror(errno));
		return(strdup(buf));
	}
	fclose(f);
	/* generate a vault key */
	size = (unsigned int)rand();
	size = (size % (36 - 20)) + 20;
	for (c = 0; c < size; c++)
		buf[c] = seedChars[rand() % 62];
	for ( ; c < 48; c++)
		buf[c] = '\0';
	snprintf(path, sizeof(path), "%s/%s/.vaultkey", dbfarm, dbname);
	f = fopen(path, "w");
	if (fwrite(buf, 1, 48, f) < 48) {
		snprintf(buf, sizeof(buf), "cannot write vaultkey: %s",
				strerror(errno));
		return(strdup(buf));
	}
	fclose(f);

	/* without an .uplog file, Merovingian won't work, this
	 * needs to be last to avoid race conditions */
	snprintf(path, sizeof(path), "%s/%s/.uplog", dbfarm, dbname);
	fclose(fopen(path, "w"));

	return(NULL);
}
