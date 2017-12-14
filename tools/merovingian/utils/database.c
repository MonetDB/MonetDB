/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/* NOTE: for this file to work correctly, msab_init must be called. */

#include "monetdb_config.h"
#include "msabaoth.h"
#include <unistd.h> /* stat, rmdir, unlink, ioctl */
#include <dirent.h> /* readdir */
#include <sys/stat.h> /* mkdir, stat, umask */
#include <sys/types.h> /* mkdir, readdir */
#include <string.h>
#include "utils.h"
#include "mutils.h"
#include "database.h"

/* check if dbname matches [A-Za-z0-9-_]+ */
char* db_validname(char *dbname) {
	size_t c;
	char buf[8096];

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
			snprintf(buf, sizeof(buf), "invalid character "
					"'%c' at " SZFMT " in database name '%s'",
					dbname[c], c, dbname);
			return(strdup(buf));
		}
	}

	return(NULL);
}

char* db_create(char* dbname) {
	sabdb *stats;
	size_t c;
	char* e;
	char* dbfarm;
	char buf[8096];
	char path[8096];
	FILE *f;

	if ((e = db_validname(dbname)) != NULL)
		return(e);

	/* the argument is the database to create, see what Sabaoth can
	 * tell us about it */
	if ((e = msab_getStatus(&stats, dbname)) != NULL) {
		snprintf(buf, sizeof(buf), "internal error: %s", e);
		free(e);
		return(strdup(buf));
	}

	/* if sabaoth doesn't know, then it's green light for us! */
	if (stats != NULL) {
		msab_freeStatus(&stats);
		snprintf(buf, sizeof(buf), "database '%s' already exists", dbname);
		return(strdup(buf));
	}

	if ((e = msab_getDBfarm(&dbfarm)) != NULL) {
		snprintf(buf, sizeof(buf), "internal error: %s", e);
		free(e);
		return(strdup(buf));
	}

	/* create the directory */
	c = snprintf(path, sizeof(path), "%s/%s", dbfarm, dbname);
	if (c >= sizeof(path)) {
		free(dbfarm);
		return(strdup("path/dbname combination too long, "
				"path would get truncated"));
	}
	if (mkdir(path, 0755) == -1) {
		snprintf(buf, sizeof(buf), "unable to create %s: %s",
				dbname, strerror(errno));
		free(dbfarm);
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
		free(dbfarm);
		return(strdup("path/dbname combination too long, "
				"filenames inside would get truncated"));
	}

	/* put this database under maintenance, make sure no race condition
	 * ever can happen, by putting it under maintenance before it even
	 * exists for Merovingian */
	if ((f = fopen(path, "w")) != NULL)
		fclose(f); /* if this fails, below probably fails too */

	/* avoid GDK making fugly complaints */
	snprintf(path, sizeof(path), "%s/%s/.gdk_lock", dbfarm, dbname);
	if ((f = fopen(path, "w")) == NULL) {
		snprintf(buf, sizeof(buf), "cannot write lock file: %s",
				strerror(errno));
		free(dbfarm);
		return(strdup(buf));
	}
	fclose(f);

	/* generate a vault key */
	snprintf(path, sizeof(path), "%s/%s/.vaultkey", dbfarm, dbname);
	if ((e = generatePassphraseFile(path)) != NULL) {
		free(dbfarm);
		return(e);
	}

	/* without an .uplog file, Merovingian won't work, this
	 * needs to be last to avoid race conditions */
	snprintf(path, sizeof(path), "%s/%s/.uplog", dbfarm, dbname);
	fclose(fopen(path, "w"));

	free(dbfarm);
	return(NULL);
}

/* recursive helper function to delete a directory */
static char* deletedir(const char *dir) {
	DIR *d;
	struct dirent *e;
	char buf[8192];
	char path[4096];

	d = opendir(dir);
	if (d == NULL) {
		/* silently return if we cannot find the directory; it's
		 * probably already deleted */
		if (errno == ENOENT)
			return(NULL);
		if (errno == ENOTDIR) {
			if (remove(dir) != 0 && errno != ENOENT) {
				snprintf(buf, sizeof(buf),
					 "unable to remove file %s: %s",
					 dir, strerror(errno));
				return(strdup(buf));
			}
			return NULL;
		}
		snprintf(buf, sizeof(buf), "unable to open dir %s: %s",
				dir, strerror(errno));
		return(strdup(buf));
	}
	while ((e = readdir(d)) != NULL) {
		/* ignore . and .. */
		if (strcmp(e->d_name, ".") != 0 &&
		    strcmp(e->d_name, "..") != 0) {
			char* er;
			snprintf(path, sizeof(path), "%s/%s", dir, e->d_name);
			if ((er = deletedir(path)) != NULL) {
				closedir(d);
				return(er);
			}
		}
	}
	closedir(d);
	if (rmdir(dir) == -1 && errno != ENOENT) {
		snprintf(buf, sizeof(buf), "unable to remove directory %s: %s",
				dir, strerror(errno));
		return(strdup(buf));
	}

	return(NULL);
}

char* db_destroy(char* dbname) {
	sabdb* stats;
	char* e;
	char buf[8096];

	if (dbname[0] == '\0')
		return(strdup("database name should not be an empty string"));

	/* the argument is the database to destroy, see what Sabaoth can
	 * tell us about it */
	if ((e = msab_getStatus(&stats, dbname)) != NULL) {
		snprintf(buf, sizeof(buf), "internal error: %s", e);
		free(e);
		return(strdup(buf));
	}

	if (stats == NULL) {
		snprintf(buf, sizeof(buf), "no such database: %s", dbname);
		return(strdup(buf));
	}

	if (stats->state == SABdbRunning) {
		snprintf(buf, sizeof(buf), "database '%s' is still running, "
				"please stop database first", dbname);
		msab_freeStatus(&stats);
		return(strdup(buf));
	}

	/* annoyingly we have to delete file by file, and
	 * directories recursively... */
	if ((e = deletedir(stats->path)) != NULL) {
		snprintf(buf, sizeof(buf), "failed to destroy '%s': %s",
				dbname, e);
		free(e);
		msab_freeStatus(&stats);
		return(strdup(buf));
	}
	msab_freeStatus(&stats);

	return(NULL);
}

char* db_rename(char *olddb, char *newdb) {
	char new[1024];
	char buf[8096];
	char *p;
	sabdb* stats;

	if (olddb[0] == '\0' || newdb[0] == '\0')
		return(strdup("database name should not be an empty string"));

	/* check if dbname matches [A-Za-z0-9-_]+ */
	if ((p = db_validname(newdb)) != NULL)
		return(p);

	if ((p = msab_getStatus(&stats, newdb)) != NULL) {
		snprintf(buf, sizeof(buf), "internal error: %s", p);
		free(p);
		return(strdup(buf));
	}
	if (stats != NULL) {
		msab_freeStatus(&stats);
		snprintf(buf, sizeof(buf), "a database with the same name "
				"already exists: %s", newdb);
		return(strdup(buf));
	}

	if ((p = msab_getStatus(&stats, olddb)) != NULL) {
		snprintf(buf, sizeof(buf), "internal error: %s", p);
		free(p);
		return(strdup(buf));
	}

	if (stats == NULL) {
		snprintf(buf, sizeof(buf), "no such database: %s", olddb);
		return(strdup(buf));
	}

	if (stats->state == SABdbRunning) {
		snprintf(buf, sizeof(buf), "database '%s' is still running, "
				"please stop database first", olddb);
		msab_freeStatus(&stats);
		return(strdup(buf));
	}

	/* construct path to new database */
	snprintf(new, sizeof(new), "%s", stats->path);
	p = strrchr(new, '/');
	if (p == NULL) {
		snprintf(buf, sizeof(buf), "non-absolute database path? '%s'",
				stats->path);
		msab_freeStatus(&stats);
		return(strdup(buf));
	}
	snprintf(p + 1, sizeof(new) - (p + 1 - new), "%s", newdb);

	/* Renaming is as simple as changing the directory name.  Since the
	 * logdir is below it, we don't need to bother about that either. */
	if (rename(stats->path, new) != 0) {
		snprintf(buf, sizeof(buf), "failed to rename database from "
				"'%s' to '%s': %s\n", stats->path, new, strerror(errno));
		msab_freeStatus(&stats);
		return(strdup(buf));
	}

	msab_freeStatus(&stats);
	return(NULL);
}

char* db_lock(char *dbname) {
	char *e;
	sabdb *stats;
	char path[8096];
	char buf[8096];
	FILE *f;

	/* the argument is the database to take under maintenance, see
	 * what Sabaoth can tell us about it */
	if ((e = msab_getStatus(&stats, dbname)) != NULL) {
		snprintf(buf, sizeof(buf), "internal error: %s", e);
		free(e);
		return(strdup(buf));
	}

	if (stats == NULL) {
		snprintf(buf, sizeof(buf), "no such database: %s", dbname);
		return(strdup(buf));
	}

	if (stats->locked == 1) {
		msab_freeStatus(&stats);
		snprintf(buf, sizeof(buf), "database '%s' already is "
				"under maintenance", dbname);
		return(strdup(buf));
	}

	/* put this database in maintenance mode */
	snprintf(path, sizeof(path), "%s/.maintenance", stats->path);
	msab_freeStatus(&stats);
	if ((f = fopen(path, "w")) == NULL) {
		snprintf(buf, sizeof(buf), "could not create '%s' for '%s': %s",
				path, dbname, strerror(errno));
		return(strdup(buf));
	}
	fclose(f); /* no biggie if it fails, file is already there */

	return(NULL);
}

char *db_release(char *dbname) {
	char *e;
	sabdb *stats;
	char path[8096];
	char buf[8096];

	/* the argument is the database to take under maintenance, see
	 * what Sabaoth can tell us about it */
	if ((e = msab_getStatus(&stats, dbname)) != NULL) {
		snprintf(buf, sizeof(buf), "internal error: %s", e);
		free(e);
		return(strdup(buf));
	}

	if (stats == NULL) {
		snprintf(buf, sizeof(buf), "no such database: %s", dbname);
		return(strdup(buf));
	}

	if (stats->locked != 1) {
		msab_freeStatus(&stats);
		snprintf(buf, sizeof(buf), "database '%s' is not "
				"under maintenance", dbname);
		return(strdup(buf));
	}

	/* get this database out of maintenance mode */
	snprintf(path, sizeof(path), "%s/.maintenance", stats->path);
	msab_freeStatus(&stats);
	if (remove(path) != 0) {
		snprintf(buf, sizeof(buf), "could not remove '%s' for '%s': %s",
				path, dbname, strerror(errno));
		return(strdup(buf));
	}

	return(NULL);
}
