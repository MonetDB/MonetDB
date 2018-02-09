/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/**
 * Sabaoth
 * Fabian Groffen
 * Cluster support
 *
 * The cluster facilitation currently only deals with (de-)registering
 * of services offered by the local server to other servers.  This
 * module allows programs to be aware of mservers in a dbfarm on a local
 * machine.
 */

#include "monetdb_config.h"
#include <unistd.h>	/* unlink and friends */
#include <sys/types.h>
#ifdef HAVE_DIRENT_H
#include <dirent.h> /* readdir, DIR */
#endif
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <string.h> /* for getting error messages */
#include <stddef.h>

#include "msabaoth.h"
#include "mutils.h"
#include "muuid.h"

#if defined(_MSC_VER) && _MSC_VER >= 1400
#define close _close
#define unlink _unlink
#define fdopen _fdopen
#endif

/** the directory where the databases are (aka dbfarm) */
char *_sabaoth_internal_dbfarm = NULL;
/** the database which is "active" */
char *_sabaoth_internal_dbname = NULL;
/** identifier of the current process */
char *_sabaoth_internal_uuid = NULL;

/**
 * Retrieves the dbfarm path plus an optional extra component added
 */
static char *
getFarmPath(char *pathbuf, size_t size, const char *extra)
{
	if (_sabaoth_internal_dbfarm == NULL)
		return(strdup("sabaoth not initialized"));

	if (extra == NULL) {
		snprintf(pathbuf, size, "%s", _sabaoth_internal_dbfarm);
	} else {
		snprintf(pathbuf, size, "%s%c%s",
				_sabaoth_internal_dbfarm, DIR_SEP, extra);
	}

	return(NULL);
}

/**
 * Retrieves the path to the database directory with an optional extra
 * component added
 */
static char *
getDBPath(char *pathbuf, size_t size, const char *extra)
{
	if (_sabaoth_internal_dbfarm == NULL)
		return(strdup("sabaoth not initialized"));
	if (_sabaoth_internal_dbname == NULL)
		return(strdup("sabaoth was not initialized as active database"));

	if (extra == NULL) {
		snprintf(pathbuf, size, "%s%c%s",
				_sabaoth_internal_dbfarm, DIR_SEP, _sabaoth_internal_dbname);
	} else {
		snprintf(pathbuf, size, "%s%c%s%c%s",
				_sabaoth_internal_dbfarm, DIR_SEP,
				_sabaoth_internal_dbname, DIR_SEP, extra);
	}

	return(NULL);
}

static inline int
msab_isuuid(const char *restrict s)
{
	int hyphens = 0;

	/* correct length */
	if (strlen(s) != 36)
		return 0;

	/* hyphens at correct locations */
	if (s[8] != '-' ||
		s[13] != '-' ||
		s[18] != '-' ||
		s[23] != '-')
		return 0;
	/* only hexadecimals and hypens */
	while (*s) {
		if (!('a' <= *s && *s <= 'f') && !('0' <= *s && *s <= '9')) {
			if (*s == '-')
				hyphens++;
			else
				return 0;
		}
		s++;
	}
	/* correct number of hyphens */
	return hyphens == 4;
}

/**
 * Initialises this Sabaoth instance to use the given dbfarm and dbname.
 * dbname may be NULL to indicate that there is no active database.  The
 * arguments are copied for internal use.
 */
static void
msab_init(const char *dbfarm, const char *dbname)
{
	size_t len;
	DIR *d;
	char *tmp;

	assert(dbfarm != NULL);

	if (_sabaoth_internal_dbfarm != NULL)
		free(_sabaoth_internal_dbfarm);
	if (_sabaoth_internal_dbname != NULL)
		free(_sabaoth_internal_dbname);

	/* this UUID is supposed to be unique per-process, we use it lateron
	 * to determine if a database is (started by) the current process,
	 * since locking always succeeds for the same process */
	if (_sabaoth_internal_uuid == NULL)
		_sabaoth_internal_uuid = generateUUID();

	len = strlen(dbfarm);
	_sabaoth_internal_dbfarm = strdup(dbfarm);
	/* remove trailing slashes, newlines and spaces */
	len--;
	while (len > 0 && (
				_sabaoth_internal_dbfarm[len] == '/' ||
				_sabaoth_internal_dbfarm[len] == '\n' ||
				_sabaoth_internal_dbfarm[len] == ' '))
	{
		_sabaoth_internal_dbfarm[len] = '\0';
		len--;
	}

	if (dbname == NULL) {
		_sabaoth_internal_dbname = NULL;
	} else {
		_sabaoth_internal_dbname = strdup(dbname);
	}

	/* clean out old UUID files in case the database crashed in a
	 * previous incarnation */
	if (_sabaoth_internal_dbname != NULL &&
		(tmp = malloc(strlen(_sabaoth_internal_dbfarm) + strlen(_sabaoth_internal_dbname) + 2)) != NULL) {
		sprintf(tmp, "%s%c%s", _sabaoth_internal_dbfarm, DIR_SEP, _sabaoth_internal_dbname);
		if ((d = opendir(tmp)) != NULL) {
			struct dbe {
				struct dbe *next;
				char path[FLEXIBLE_ARRAY_MEMBER];
			} *dbe = NULL, *db;
			struct dirent *e;
			len = offsetof(struct dbe, path) + strlen(tmp) + 2;
			while ((e = readdir(d)) != NULL) {
				if (msab_isuuid(e->d_name) &&
					(db = malloc(strlen(e->d_name) + len)) != NULL) {
					db->next = dbe;
					dbe = db;
					sprintf(db->path, "%s%c%s", tmp, DIR_SEP, e->d_name);
				}
			}
			closedir(d);
			/* remove in a separate loop after reading the directory,
			 * so as to not have any interference */
			while (dbe != NULL) {
				(void) remove(dbe->path);
				db = dbe;
				dbe = dbe->next;
				free(db);
			}
		}
		free(tmp);
	}
}
void
msab_dbpathinit(const char *dbpath)
{
	char dbfarm[FILENAME_MAX];
	const char *p;

	p = strrchr(dbpath, DIR_SEP);
	assert(p != NULL);
	strncpy(dbfarm, dbpath, p - dbpath);
	dbfarm[p - dbpath] = 0;
	msab_init(dbfarm, p + 1);
}
void
msab_dbfarminit(const char *dbfarm)
{
	msab_init(dbfarm, NULL);
}

/**
 * Returns the dbfarm as received during msab_init.  Returns an
 * exception if not initialised.
 */
char *
msab_getDBfarm(char **ret)
{
	if (_sabaoth_internal_dbfarm == NULL)
		return(strdup("sabaoth not initialized"));
	*ret = strdup(_sabaoth_internal_dbfarm);
	return(NULL);
}

/**
 * Returns the dbname as received during msab_init.  Throws an
 * exception if not initialised or dbname was set to NULL.
 */
char *
msab_getDBname(char **ret)
{
	if (_sabaoth_internal_dbfarm == NULL)
		return(strdup("sabaoth not initialized"));
	if (_sabaoth_internal_dbname == NULL)
		return(strdup("sabaoth was not initialized as active database"));
	*ret = strdup(_sabaoth_internal_dbname);
	return(NULL);
}

#define SCENARIOFILE ".scen"

/**
 * Writes the given language to the scenarios file.  If the file doesn't
 * exist, it is created.  Multiple invocations of this function for the
 * same language are ignored.
 */
char *
msab_marchScenario(const char *lang)
{
	FILE *f;
	char buf[256];	/* should be enough for now */
	size_t len;
	char pathbuf[FILENAME_MAX];
	char *tmp;

	if ((tmp = getDBPath(pathbuf, sizeof(pathbuf), SCENARIOFILE)) != NULL)
		return(tmp);

	if ((f = fopen(pathbuf, "a+")) != NULL) {
		if ((len = fread(buf, 1, 255, f)) > 0) {
			char *p;

			buf[len] = '\0';
			tmp = buf;
			/* find newlines and evaluate string */
			while ((p = strchr(tmp, '\n')) != NULL) {
				*p = '\0';
				if (strcmp(tmp, lang) == 0) {
					(void)fclose(f);
					return(NULL);
				}
				tmp = p;
			}
		}
		/* append to the file */
		fprintf(f, "%s\n", lang);
		(void)fflush(f);
		(void)fclose(f);
		return(NULL);
	}
	snprintf(buf, sizeof(buf), "failed to open file: %s (%s)",
			strerror(errno), pathbuf);
	return(strdup(buf));
}

/**
 * Removes the given language from the scenarios file.  If the scenarios
 * file is empty (before or) after removing the language, the file is
 * removed.
 */
char *
msab_retreatScenario(const char *lang)
{
	FILE *f;
	char buf[256];	/* should be enough to hold the entire file */
	size_t len;
	char pathbuf[FILENAME_MAX];
	char *tmp;

	if ((tmp = getDBPath(pathbuf, sizeof(pathbuf), SCENARIOFILE)) != NULL)
		return(tmp);

	if ((f = fopen(pathbuf, "a+")) != NULL) {
		if ((len = fread(buf, 1, 255, f)) > 0) {
			char *p;
			char written = 0;

			buf[len] = '\0';
			tmp = buf;
			/* find newlines and evaluate string */
			while ((p = strchr(tmp, '\n')) != NULL) {
				*p = '\0';
				if (strcmp(tmp, lang) == 0) {
					memmove(tmp, p + 1, strlen(p + 1) + 1);
					written = 1;
				} else {
					*p = '\n';
					tmp = p+1;
				}
			}
			if (written != 0) {
				rewind(f);
				len = strlen(buf) + 1;
				if (fwrite(buf, 1, len, f) < len) {
					(void)fclose(f);
					snprintf(buf, sizeof(buf), "failed to write: %s (%s)",
							strerror(errno), pathbuf);
					return(strdup(buf));
				}
				fflush(f);
				fclose(f);
				return(NULL);
			}
			(void)fclose(f);
			(void) remove(pathbuf);
			return(NULL);
		} else {
			if (ferror(f)) {
				/* some error */
				snprintf(buf, sizeof(buf), "failed to write: %s (%s)",
					 strerror(errno), pathbuf);
				(void)fclose(f);
				return strdup(buf);
			}
			(void)fclose(f);
			(void) remove(pathbuf);  /* empty file? try to remove */
			return(NULL);
		}
	}
	snprintf(buf, sizeof(buf), "failed to open file: %s (%s)",
			strerror(errno), pathbuf);
	return(strdup(buf));
}

#define CONNECTIONFILE ".conn"
/**
 * Writes an URI to the connection file based on the given arguments.
 * If the file doesn't exist, it is created.  Multiple invocations of
 * this function for the same arguments are NOT ignored.  If port is set
 * to <= 0, this function treats the host argument as UNIX domain
 * socket, in which case host must start with a '/'.
 */
char *
msab_marchConnection(const char *host, const int port)
{
	FILE *f;
	char pathbuf[FILENAME_MAX];
	char *tmp;

	if ((tmp = getDBPath(pathbuf, sizeof(pathbuf), CONNECTIONFILE)) != NULL)
		return(tmp);

	if (port <= 0 && host[0] != '/')
		return(strdup("UNIX domain connections should be given as "
					"absolute path"));

	if ((f = fopen(pathbuf, "a")) != NULL) {
		/* append to the file */
		if (port > 0) {
			fprintf(f, "mapi:monetdb://%s:%i/\n", host, port);
		} else {
			fprintf(f, "mapi:monetdb://%s\n", host);
		}
		(void)fflush(f);
		(void)fclose(f);
		return(NULL);
	} else {
		char buf[FILENAME_MAX + 1024];
		snprintf(buf, sizeof(buf), "failed to open file: %s (%s)",
				strerror(errno), pathbuf);
		return(strdup(buf));
	}
}

#define STARTEDFILE ".started"
/**
 * Removes all known publications of available services.  The function
 * name is a nostalgic phrase from "Defender of the Crown" from the
 * Commodore Amiga age.
 */
char *
msab_wildRetreat(void)
{
	char pathbuf[FILENAME_MAX];
	char *tmp;

	if ((tmp = getDBPath(pathbuf, sizeof(pathbuf), SCENARIOFILE)) != NULL)
		return(tmp);
	(void) remove(pathbuf);

	if ((tmp = getDBPath(pathbuf, sizeof(pathbuf), CONNECTIONFILE)) != NULL)
		return(tmp);
	(void) remove(pathbuf);

	if ((tmp = getDBPath(pathbuf, sizeof(pathbuf), STARTEDFILE)) != NULL)
		return(tmp);
	(void) remove(pathbuf);

	if ((tmp = getDBPath(pathbuf, sizeof(pathbuf), _sabaoth_internal_uuid)) != NULL)
		return(tmp);
	(void) remove(pathbuf);

	return(NULL);
}

#define UPLOGFILE ".uplog"
/**
 * Writes a start attempt to the sabaoth start/stop log.  Examination of
 * the log at a later stage reveals crashes of the server.  In addition
 * to updating the uplog file, it also leaves the unique signature of
 * the current process behind.
 */
char *
msab_registerStarting(void)
{
	/* The sabaoth uplog is in fact a simple two column table that
	 * contains a start time and a stop time.  Start times are followed
	 * by a tab character, while stop times are followed by a newline.
	 * This allows to detect crashes, while sabaoth only appends to the
	 * uplog. */

	FILE *f;
	char pathbuf[FILENAME_MAX];
	char *tmp;

	if ((tmp = getDBPath(pathbuf, sizeof(pathbuf), UPLOGFILE)) != NULL)
		return(tmp);

	if ((f = fopen(pathbuf, "a")) != NULL) {
		/* append to the file */
		fprintf(f, "%" PRId64 "\t", (int64_t)time(NULL));
		(void)fflush(f);
		(void)fclose(f);
	} else {
		char buf[FILENAME_MAX];
		snprintf(buf, sizeof(buf), "failed to open file: %s (%s)",
				strerror(errno), pathbuf);
		return(strdup(buf));
	}

	/* we treat errors here (albeit being quite unlikely) as non-fatal,
	 * since they will cause wrong state information in the worst case
	 * later on */
	if ((tmp = getDBPath(pathbuf, sizeof(pathbuf), _sabaoth_internal_uuid)) != NULL) {
		free(tmp);
		return(NULL);
	}
	f = fopen(pathbuf, "w");
	if (f)
		fclose(f);

	/* remove any stray file that would suggest we've finished starting up */
	if ((tmp = getDBPath(pathbuf, sizeof(pathbuf), STARTEDFILE)) != NULL)
		return(tmp);
	(void) remove(pathbuf);


	return(NULL);
}

/**
 * Removes the starting state, and turns this into a fully started
 * engine.  The caller is responsible for calling registerStarting()
 * first.
 */
char *
msab_registerStarted(void)
{
	char pathbuf[FILENAME_MAX];
	char *tmp;
	FILE *fp;

	/* flag this database as started up */
	if ((tmp = getDBPath(pathbuf, sizeof(pathbuf), STARTEDFILE)) != NULL)
		return(tmp);
	fp = fopen(pathbuf, "w");
	if (fp)
		fclose(fp);
	else
		return strdup("sabaoth cannot create " STARTEDFILE);

	return(NULL);
}

/**
 * Writes a start attempt to the sabaoth start/stop log.  Examination of
 * the log at a later stage reveals crashes of the server.
 */
char *
msab_registerStop(void)
{
	FILE *f;
	char pathbuf[FILENAME_MAX];
	char *tmp;

	if ((tmp = getDBPath(pathbuf, sizeof(pathbuf), UPLOGFILE)) != NULL)
		return(tmp);

	if ((f = fopen(pathbuf, "a")) != NULL) {
		/* append to the file */
		fprintf(f, "%" PRId64 "\n", (int64_t)time(NULL));
		(void)fflush(f);
		(void)fclose(f);
	} else {
		char buf[FILENAME_MAX];
		snprintf(buf, sizeof(buf), "failed to open file: %s (%s)",
				strerror(errno), pathbuf);
		return(strdup(buf));
	}

	/* remove server signature, it's no problem when it's left behind,
	 * but for the sake of keeping things clean ... */
	if ((tmp = getDBPath(pathbuf, sizeof(pathbuf), _sabaoth_internal_uuid)) != NULL)
		return(tmp);
	(void) remove(pathbuf);
	return(NULL);
}

/**
 * Returns the status as NULL terminated sabdb struct list for the
 * current database.  Since the current database should always exist,
 * this function never returns NULL.
 */
char *
msab_getMyStatus(sabdb** ret)
{
	char *err;
	if (_sabaoth_internal_dbname == NULL)
		return(strdup("sabaoth was not initialized as active database"));
	err = msab_getStatus(ret, _sabaoth_internal_dbname);
	if (err != NULL)
		return(err);
	if (*ret == NULL)
		return(strdup("could not find my own database?!?"));
	return(NULL);
}

#define MAINTENANCEFILE ".maintenance"

static sabdb *
msab_getSingleStatus(const char *pathbuf, const char *dbname, sabdb *next)
{
	char buf[FILENAME_MAX];
	char data[8096];
	char log[FILENAME_MAX];
	FILE *f;
	int fd;
	struct stat statbuf;

	sabdb *sdb;
	sdb = NULL;

	snprintf(buf, sizeof(buf), "%s/%s/%s", pathbuf, dbname, UPLOGFILE);
	if (stat(buf, &statbuf) == -1)
		return next;

	sdb = malloc(sizeof(sabdb));
	sdb->uplog = NULL;
	sdb->uri = NULL;
	sdb->next = next;

	/* store the database name */
	snprintf(buf, sizeof(buf), "%s/%s", pathbuf, dbname);
	sdb->path = strdup(buf);
	sdb->dbname = sdb->path + strlen(sdb->path) - strlen(dbname);


	/* check the state of the server by looking at its gdk lock:
	 * - if we can lock it, the server has crashed or isn't running
	 * - if we can't open it because it's locked, the server is
	 *   running
	 * - to distinguish between a crash and proper shutdown, consult
	 *   the uplog
	 * - one exception to all above; if this is the same process, we
	 *   cannot lock (it always succeeds), hence, if we have the
	 *   same signature, we assume running if the uplog states so.
	 */
	snprintf(buf, sizeof(buf), "%s/%s/%s", pathbuf, dbname,
			 _sabaoth_internal_uuid);
	if (stat(buf, &statbuf) != -1) {
		/* database has the same process signature as ours, which
		 * means, it must be us, rely on the uplog state */
		snprintf(log, sizeof(log), "%s/%s/%s", pathbuf, dbname, UPLOGFILE);
		if ((f = fopen(log, "r")) != NULL) {
			(void)fseek(f, -1, SEEK_END);
			if (fread(data, 1, 1, f) != 1) {
				/* the log is empty, assume no crash */
				sdb->state = SABdbInactive;
			} else if (data[0] == '\t') {
				/* see if the database has finished starting */
				snprintf(buf, sizeof(buf), "%s/%s/%s",
						 pathbuf, dbname, STARTEDFILE);
				if (stat(buf, &statbuf) == -1) {
					sdb->state = SABdbStarting;
				} else {
					sdb->state = SABdbRunning;
				}
			} else { /* should be \n */
				sdb->state = SABdbInactive;
			}
			(void)fclose(f);
		}
	} else if ((snprintf(buf, sizeof(buf), "%s/%s/%s", pathbuf, dbname, ".gdk_lock") > 0) & /* no typo */
			   ((fd = MT_lockf(buf, F_TEST, 4, 1)) == -2)) {
		/* Locking failed; this can be because the lockfile couldn't
		 * be created.  Probably there is no Mserver running for
		 * that case also.
		 */
		sdb->state = SABdbInactive;
	} else if (fd == -1) {
		/* file is locked, so mserver is running, see if the database
		 * has finished starting */
		snprintf(buf, sizeof(buf), "%s/%s/%s",
				 pathbuf, dbname, STARTEDFILE);
		if (stat(buf, &statbuf) == -1) {
			sdb->state = SABdbStarting;
		} else {
			sdb->state = SABdbRunning;
		}
	} else {
		/* file is not locked, check for a crash in the uplog */
		snprintf(log, sizeof(log), "%s/%s/%s", pathbuf, dbname, UPLOGFILE);
		if ((f = fopen(log, "r")) != NULL) {
			(void)fseek(f, -1, SEEK_END);
			if (fread(data, 1, 1, f) != 1) {
				/* the log is empty, assume no crash */
				sdb->state = SABdbInactive;
			} else if (data[0] == '\n') {
				sdb->state = SABdbInactive;
			} else { /* should be \t */
				sdb->state = SABdbCrashed;
			}
			(void)fclose(f);
		} else {
			/* no uplog, so presumably never started */
			sdb->state = SABdbInactive;
		}
	}
	snprintf(buf, sizeof(buf), "%s/%s/%s", pathbuf, dbname, MAINTENANCEFILE);
	if (stat(buf, &statbuf) == -1) {
		sdb->locked = 0;
	} else {
		sdb->locked = 1;
	}

	/* add scenarios that are supported */
	sdb->scens = NULL;
	snprintf(buf, sizeof(buf), "%s/%s/%s", pathbuf, dbname, SCENARIOFILE);
	if ((f = fopen(buf, "r")) != NULL) {
		sablist* np = NULL;
		while (fgets(data, 8095, f) != NULL) {
			if (*data != '\0' && data[strlen(data) - 1] == '\n')
				data[strlen(data) - 1] = '\0';
			if (sdb->scens == NULL) {
				sdb->scens = malloc(sizeof(sablist));
				sdb->scens->val = strdup(data);
				sdb->scens->next = NULL;
				np = sdb->scens;
			} else {
				np = np->next = malloc(sizeof(sablist));
				np->val = strdup(data);
				np->next = NULL;
			}
		}
		(void)fclose(f);
	}

	/* add how this server can be reached */
	sdb->conns = NULL;
	snprintf(buf, sizeof(buf), "%s/%s/%s", pathbuf, dbname, CONNECTIONFILE);
	if ((f = fopen(buf, "r")) != NULL) {
		sablist* np = NULL;
		while (fgets(data, 8095, f) != NULL) {
			if (*data != '\0' && data[strlen(data) - 1] == '\n')
				data[strlen(data) - 1] = '\0';
			if (sdb->conns == NULL) {
				sdb->conns = malloc(sizeof(sablist));
				sdb->conns->val = strdup(data);
				sdb->conns->next = NULL;
				np = sdb->conns;
			} else {
				np = np->next = malloc(sizeof(sablist));
				np->val = strdup(data);
				np->next = NULL;
			}
		}
		(void)fclose(f);
	}

	return sdb;
}

/**
 * Returns a list of populated sabdb structs.  If dbname == NULL, the
 * list contains sabdb structs for all found databases in the dbfarm.
 * Otherwise, at most one sabdb struct is returned for the database from
 * the dbfarm that matches dbname.
 * If no database could be found, an empty list is returned.  Each list
 * is terminated by a NULL entry.
 */
char *
msab_getStatus(sabdb** ret, char *dbname)
{
	DIR *d;
	struct dirent *e;
	char data[8096];
	char pathbuf[FILENAME_MAX];
	char *p;

	/* Caching strategies (might be nice) should create a new struct with
	 * the last modified time_t of the files involved, such that a stat is
	 * sufficient to see if reparsing is necessary.  The gdk_lock always has
	 * to be checked to detect crashes. */

	sabdb *sdb;
	sdb = *ret = NULL;

	/* scan the parent for directories */
	if ((p = getFarmPath(pathbuf, sizeof(pathbuf), NULL)) != NULL)
		return(p);
	if (dbname) {
		*ret = msab_getSingleStatus(pathbuf, dbname, NULL);
		return NULL;
	}

	d = opendir(pathbuf);
	if (d == NULL) {
		snprintf(data, sizeof(data), "failed to open directory %s: %s",
				pathbuf, strerror(errno));
		return(strdup(data));
	}
	while ((e = readdir(d)) != NULL) {
		if (strcmp(e->d_name, "..") == 0 || strcmp(e->d_name, ".") == 0)
			continue;

		sdb = msab_getSingleStatus(pathbuf, e->d_name, sdb);
	}
	(void)closedir(d);

	*ret = sdb;
	return(NULL);
}

/**
 * Frees up the sabdb structure returned by getStatus.
 */
char *
msab_freeStatus(sabdb** ret)
{
	sabdb *p, *q;
	sablist *r, *s;

	p = *ret;
	while (p != NULL) {
		if (p->path != NULL)
			free(p->path);
		if (p->uri != NULL)
			free(p->uri);
		if (p->uplog != NULL)
			free(p->uplog);
		r = p->scens;
		while (r != NULL) {
			if (r->val != NULL)
				free(r->val);
			s = r->next;
			free(r);
			r = s;
		}
		r = p->conns;
		while (r != NULL) {
			if (r->val != NULL)
				free(r->val);
			s = r->next;
			free(r);
			r = s;
		}
		q = p->next;
		free(p);
		p = q;
	}

	return(NULL);
}

/**
 * Parses the .uplog file for the given database, and fills ret with the
 * parsed information.
 */
char *
msab_getUplogInfo(sabuplog *ret, const sabdb *db)
{
	char log[FILENAME_MAX];
	char data[24];
	char *p;
	FILE *f;
	time_t start, stop, up;
	int avg10[10];
	int avg30[30];
	int i = 0;

	/* early bailout if cached */
	if (db->uplog != NULL) {
		*ret = *db->uplog;
		return(NULL);
	}
		
	memset(avg10, 0, sizeof(int) * 10);
	memset(avg30, 0, sizeof(int) * 30);

	/* clear the struct */
	memset(ret, 0, sizeof(sabuplog));
	ret->minuptime = -1;
	ret->lastcrash = -1;
	ret->laststop = -1;

	snprintf(log, sizeof(log), "%s/%s", db->path, UPLOGFILE);
	if ((f = fopen(log, "r")) != NULL) {
		int c;
		start = stop = up = 0;
		p = data;
		while ((c = getc(f)) != EOF) {
			*p = (char)c;
			switch (*p) {
				case '\t':
					/* start attempt */
					ret->startcntr++;
					if (start != 0)
						ret->lastcrash = start;
					memmove(&avg10[0], &avg10[1], sizeof(int) * 9);
					memmove(&avg30[0], &avg30[1], sizeof(int) * 29);
					avg10[9] = avg30[29] = ret->crashavg1 = 
						(start != 0);
					*p = '\0';
					ret->laststart = start = (time_t)atol(data);
					p = data;
				break;
				case '\n':
					/* successful stop */
					ret->stopcntr++;
					*p = '\0';
					ret->laststop = stop = (time_t)atol(data);
					p = data;
					i = (int) (stop - start);
					if (i > ret->maxuptime)
						ret->maxuptime = i;
					if (ret->minuptime == -1 || ret->minuptime > stop - start)
						ret->minuptime = stop - start;
					up += i;
					start = 0;
				break;
				default:
					/* timestamp */
					p++;
				break;
			}
		}
		if (start != 0 && db->state != SABdbRunning)
			ret->lastcrash = start;
		memmove(&avg10[0], &avg10[1], sizeof(int) * 9);
		memmove(&avg30[0], &avg30[1], sizeof(int) * 29);
		avg10[9] = avg30[29] = ret->crashavg1 =
			(start != 0 ? (db->state != SABdbRunning) : 0);
		ret->crashcntr =
			ret->startcntr - (db->state == SABdbRunning) -
			ret->stopcntr;
		for (i = 0; i < 10; i++)
			ret->crashavg10 += avg10[i];
		ret->crashavg10 = ret->crashavg10 / 10.0;
		for (i = 0; i < 30; i++)
			ret->crashavg30 += avg30[i];
		ret->crashavg30 = ret->crashavg30 / 30.0;

		if (ret->stopcntr > 0) {
			ret->avguptime = (time_t)(((double)up / (double)ret->stopcntr) + 0.5);
		} else {
			ret->avguptime = 0;
			ret->minuptime = 0;
			ret->maxuptime = 0;
		}
		(void)fclose(f);
	} else {
		char buf[FILENAME_MAX];
		snprintf(buf, sizeof(buf), "could not open file %s: %s",
				log, strerror(errno));
		return(strdup(buf));
	}

	/* Don't store the sabuplog in the sabdb as there is no good reason
	 * to retrieve the sabuplog struct more than once for a database
	 * (without refetching the sabdb struct).  Currently, only a
	 * serialisation/deserialisation of a sabdb struct will prefill the
	 * sabuplog struct. */
	return(NULL);
}

/* used in the serialisation to be able to change it in the future */
#define SABDBVER "2"

/**
 * Produces a string representation suitable for storage/sending.
 */
char *
msab_serialise(char **ret, const sabdb *db)
{
	char buf[8096];
	char scens[64];
	sablist *l;
	sabuplog dbu;
	char *p;
	size_t avail;
	size_t len;

	scens[0] = '\0';
	p = scens;
	avail = sizeof(scens) - 1;
	for (l = db->scens; l != NULL; l = l->next) {
		len = strlen(l->val);
		if (len > avail)
			break;
		memcpy(p, l->val, len);
		p += len + 1;
		avail -= len + 1;
		memcpy(p - 1, "'", 2);
	}
	if (p != scens)
		p[-1] = '\0';

	if ((p = msab_getUplogInfo(&dbu, db)) != NULL)
		return(p);

	/* sabdb + sabuplog structs in one */
	snprintf(buf, sizeof(buf), "sabdb:" SABDBVER ":"
			 "%s,%s,%d,%d,%s,"
			 "%d,%d,%d,"
			 "%" PRId64 ",%" PRId64 ",%" PRId64 ","
			 "%" PRId64 ",%" PRId64 ",%" PRId64 ","
			 "%d,%f,%f",
			 db->dbname, db->uri ? db->uri : "", db->locked,
			 (int) db->state, scens,
			 dbu.startcntr, dbu.stopcntr, dbu.crashcntr,
			 (int64_t) dbu.avguptime, (int64_t) dbu.maxuptime,
			 (int64_t) dbu.minuptime, (int64_t) dbu.lastcrash,
			 (int64_t) dbu.laststart, (int64_t) dbu.laststop,
			 dbu.crashavg1, dbu.crashavg10, dbu.crashavg30);

	*ret = strdup(buf);
	return(NULL);
}

/**
 * Produces a sabdb struct out of a serialised string.
 */
char *
msab_deserialise(sabdb **ret, char *sdb)
{
	char *dbname;
	char *uri;
	int locked;
	int state;
	char *scens = "";
	sabdb *s;
	sabuplog *u;
	sablist *l;
	char *p;
	char *lasts;
	char buf[FILENAME_MAX];
	char protover = 0;

	lasts = sdb;
	if ((p = strchr(lasts, ':')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain a magic: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	if (strcmp(lasts, "sabdb") != 0) {
		snprintf(buf, sizeof(buf), 
				"string is not a sabdb struct: %s", lasts);
		return(strdup(buf));
	}
	lasts = p;
	if ((p = strchr(p, ':')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain a version number: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	if (strcmp(lasts, "1") == 0) {
		/* Protocol 1 was used uptil Oct2012.  Since Jul2012 a new state
		 * SABdbStarting was introduced, but not exposed to the client
		 * in serialise.  In Feb2013, the path component was removed
		 * and replaced by an URI field.  This meant dbname could no
		 * longer be deduced from path, and hence sent separately.
		 * Since the conns property became useless in the light of the
		 * added uri, it was dropped.  On top of this, a laststop
		 * property was added to the uplog struct.
		 * These four changes were effectuated in protocol 2.  When
		 * reading protocol 1, we use the path field to set dbname, but
		 * ignore the path information (and set uri to "<unknown>".  The
		 * SABdbStarting state never occurs. */
	} else if (strcmp(lasts, SABDBVER) != 0) {
		snprintf(buf, sizeof(buf), 
				"string has unsupported version: %s", lasts);
		return(strdup(buf));
	}
	protover = lasts[0];
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain %s: %s",
				protover == '1' ? "path" : "dbname", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	dbname = lasts;
	if (protover == '1') {
		uri = "<unknown>";
	} else {
		lasts = p;
		if ((p = strchr(p, ',')) == NULL) {
			snprintf(buf, sizeof(buf), 
					"string does not contain uri: %s", lasts);
			return(strdup(buf));
		}
		*p++ = '\0';
		uri = lasts;
	}
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain locked state: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	locked = atoi(lasts);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain state: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	state = atoi(lasts);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain scenarios: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	scens = lasts;
	lasts = p;
	if (protover == '1') {
		if ((p = strchr(p, ',')) == NULL) {
			snprintf(buf, sizeof(buf), 
					"string does not contain connections: %s", lasts);
			return(strdup(buf));
		}
		*p++ = '\0';
		lasts = p;
	}

	/* start parsing sabuplog struct */
	u = malloc(sizeof(sabuplog));

	if ((p = strchr(p, ',')) == NULL) {
		free(u);
		snprintf(buf, sizeof(buf), 
				"string does not contain startcounter: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->startcntr = atoi(lasts);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		free(u);
		snprintf(buf, sizeof(buf), 
				"string does not contain stopcounter: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->stopcntr = atoi(lasts);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		free(u);
		snprintf(buf, sizeof(buf), 
				"string does not contain crashcounter: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->crashcntr = atoi(lasts);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		free(u);
		snprintf(buf, sizeof(buf), 
				"string does not contain avguptime: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->avguptime = (time_t)strtoll(lasts, (char **)NULL, 10);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		free(u);
		snprintf(buf, sizeof(buf), 
				"string does not contain maxuptime: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->maxuptime = (time_t)strtoll(lasts, (char **)NULL, 10);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		free(u);
		snprintf(buf, sizeof(buf), 
				"string does not contain minuptime: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->minuptime = (time_t)strtoll(lasts, (char **)NULL, 10);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		free(u);
		snprintf(buf, sizeof(buf), 
				"string does not contain lastcrash: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->lastcrash = (time_t)strtoll(lasts, (char **)NULL, 10);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		free(u);
		snprintf(buf, sizeof(buf), 
				"string does not contain laststart: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->laststart = (time_t)strtoll(lasts, (char **)NULL, 10);
	lasts = p;
	if (protover != '1') {
		if ((p = strchr(p, ',')) == NULL) {
			free(u);
			snprintf(buf, sizeof(buf), 
					"string does not contain laststop: %s", lasts);
			return(strdup(buf));
		}
		*p++ = '\0';
		u->laststop = (time_t)strtoll(lasts, (char **)NULL, 10);
		lasts = p;
	} else {
		u->laststop = -1;
	}
	if ((p = strchr(p, ',')) == NULL) {
		free(u);
		snprintf(buf, sizeof(buf), 
				"string does not contain crashavg1: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->crashavg1 = atoi(lasts);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		free(u);
		snprintf(buf, sizeof(buf), 
				"string does not contain crashavg10: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->crashavg10 = atof(lasts);
	lasts = p;
	if ((p = strchr(p, ',')) != NULL) {
		free(u);
		snprintf(buf, sizeof(buf), 
				"string contains additional garbage after crashavg30: %s",
				p);
		return(strdup(buf));
	}
	u->crashavg30 = atof(lasts);

	/* fill/create sabdb struct */

	if (protover == '1') {
		if ((p = strrchr(dbname, '/')) == NULL) {
			free(u);
			snprintf(buf, sizeof(buf), "invalid path: %s", dbname);
			return(strdup(buf));
		}
		dbname = p + 1;
	}

	s = malloc(sizeof(sabdb));

	/* msab_freeStatus() actually relies on this trick */
	s->path = s->dbname = strdup(dbname);
	s->uri = strdup(uri);
	s->locked = locked;
	s->state = (SABdbState)state;
	if (strlen(scens) == 0) {
		s->scens = NULL;
	} else {
		l = s->scens = malloc(sizeof(sablist));
		p = strtok_r(scens, "'", &lasts);
		if (p == NULL) {
			l->val = strdup(scens);
			l->next = NULL;
		} else {
			l->val = strdup(p);
			l->next = NULL;
			while ((p = strtok_r(NULL, "'", &lasts)) != NULL) {
				l = l->next = malloc(sizeof(sablist));
				l->val = strdup(p);
				l->next = NULL;
			}
		}
	}
	s->conns = NULL;
	s->uplog = u;
	s->next = NULL;

	*ret = s;
	return(NULL);
}
