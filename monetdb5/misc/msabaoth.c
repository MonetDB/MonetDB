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
#include <stdio.h> /* fseek, rewind */
#include <unistd.h>	/* unlink and friends */
#include <sys/types.h>
#ifdef HAVE_DIRENT_H
#include <dirent.h> /* readdir, DIR */
#endif
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <string.h> /* for getting error messages */
#include <assert.h>

#include "msabaoth.h"
#include "mutils.h"

#if defined(_MSC_VER) && _MSC_VER >= 1400
#define close _close
#define unlink _unlink
#endif

/** the directory where the databases are (aka dbfarm) */
char *_sabaoth_internal_dbfarm = NULL;
/** the database which is "active" */
char *_sabaoth_internal_dbname = NULL;

/**
 * Retrieves the dbfarm path plus an optional extra component added
 */
static char *
getFarmPath(char **ret, size_t size, const char *extra)
{
	if (_sabaoth_internal_dbfarm == NULL)
		return(strdup("sabaoth not initialized"));

	if (extra == NULL) {
		snprintf(*ret, size, "%s", _sabaoth_internal_dbfarm);
	} else {
		snprintf(*ret, size, "%s%c%s",
				_sabaoth_internal_dbfarm, DIR_SEP, extra);
	}

	return(NULL);
}

/**
 * Retrieves the path to the database directory with an optional extra
 * component added
 */
static char *
getDBPath(char **ret, size_t size, const char *extra)
{
	if (_sabaoth_internal_dbfarm == NULL)
		return(strdup("sabaoth not initialized"));
	if (_sabaoth_internal_dbname == NULL)
		return(strdup("sabaoth was not initialized as active database"));

	if (extra == NULL) {
		snprintf(*ret, size, "%s%c%s",
				_sabaoth_internal_dbfarm, DIR_SEP, _sabaoth_internal_dbname);
	} else {
		snprintf(*ret, size, "%s%c%s%c%s",
				_sabaoth_internal_dbfarm, DIR_SEP,
				_sabaoth_internal_dbname, DIR_SEP, extra);
	}

	return(NULL);
}

/**
 * Initialises this Sabaoth instance to use the given dbfarm and dbname.
 * dbname may be NULL to indicate that there is no active database.  The
 * arguments are copied for internal use.
 */
void
msab_init(char *dbfarm, char *dbname)
{
	size_t len;

	assert(dbfarm != NULL);

	if (_sabaoth_internal_dbfarm != NULL)
		free(_sabaoth_internal_dbfarm);
	if (_sabaoth_internal_dbname != NULL)
		free(_sabaoth_internal_dbname);

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

#define PATHLENGTH 4096

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
	char *buf = alloca(sizeof(char) * 256);	/* should be enough for now */
	size_t len;
	char *path = alloca(sizeof(char) * (PATHLENGTH));
	char *tmp;

	if ((tmp = getDBPath(&path, PATHLENGTH, SCENARIOFILE)) != NULL)
		return(tmp);

	if ((f = fopen(path, "a+")) != NULL) {
		if ((len = fread(buf, 1, 255, f)) > 0) {
			char *p;

			buf[len] = '\0';
			/* find newlines and evaluate string */
			while ((p = strchr(buf, '\n')) != NULL) {
				*p = '\0';
				if (strcmp(buf, lang) == 0) {
					(void)fclose(f);
					return(NULL);
				}
				buf = p;
			}
		}
		/* append to the file */
		fprintf(f, "%s\n", lang);
		(void)fflush(f);
		(void)fclose(f);
		return(NULL);
	}
	snprintf(buf, sizeof(buf), "failed to open file: %s (%s)",
			strerror(errno), path);
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
	char *buf = alloca(sizeof(char) * 256);	/* should be enough for now */
	size_t len;
	char *path = alloca(sizeof(char) * (PATHLENGTH));
	char *tmp;

	if ((tmp = getDBPath(&path, PATHLENGTH, SCENARIOFILE)) != NULL)
		return(tmp);

	if ((f = fopen(path, "a+")) != NULL) {
		if ((len = fread(buf, 1, 255, f)) > 0) {
			char *p;
			FILE *tmp = tmpfile();
			int written = 0;

			buf[len] = '\0';
			/* find newlines and evaluate string */
			while ((p = strchr(buf, '\n')) != NULL) {
				*p = '\0';
				if (strcmp(buf, lang) != 0) {
					fprintf(tmp, "%s\n", buf);
					written = 1;
				}
				buf = p;
			}
			if (written != 0) {
				/* no idea how to "move" a file by it's fd (sounds
				 * impossible anyway) and tmpnam is so much "DO NOT USE"
				 * that I decided to just copy over the file again... */
				rewind(f);
				fflush(tmp);
				rewind(tmp);
				len = fread(buf, 1, 256, tmp);
				if (fwrite(buf, 1, len, f) < len) {
					(void)fclose(tmp);
					(void)fclose(f);
					snprintf(buf, sizeof(buf), "failed to write: %s (%s)",
							strerror(errno), path);
					return(strdup(buf));
				}
				fflush(f);
				fclose(f);
				fclose(tmp); /* this should remove it automagically */
				return(NULL);
			} else {
				(void)fclose(f);
				unlink(path);
				return(NULL);
			}
		} else if (len == 0) {
			(void)fclose(f);
			unlink(path);
			return(NULL);
		} else { /* some error */
			(void)fclose(f);
			snprintf(buf, sizeof(buf), "failed to write: %s (%s)",
					strerror(errno), path);
			return(strdup(buf));
		}
	}
	snprintf(buf, sizeof(buf), "failed to open file: %s (%s)",
			strerror(errno), path);
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
	char *path = alloca(sizeof(char) * (PATHLENGTH));
	char *tmp;

	if ((tmp = getDBPath(&path, PATHLENGTH, CONNECTIONFILE)) != NULL)
		return(tmp);

	if (port <= 0 && host[0] != '/')
		return(strdup("UNIX domain connections should be given as "
					"absolute path"));

	if ((f = fopen(path, "a")) != NULL) {
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
		char buf[PATHLENGTH];
		snprintf(buf, sizeof(buf), "failed to open file: %s (%s)",
				strerror(errno), path);
		return(strdup(buf));
	}
}

/**
 * Removes all known publications of available services.  The function
 * name is a nostalgic phrase from "Defender of the Crown" from the
 * Commodore Amiga age.
 */
char *
msab_wildRetreat(void)
{
	char *path = alloca(sizeof(char) * (PATHLENGTH));
	char *tmp;

	if ((tmp = getDBPath(&path, PATHLENGTH, SCENARIOFILE)) != NULL)
		return(tmp);
	unlink(path);

	if ((tmp = getDBPath(&path, PATHLENGTH, CONNECTIONFILE)) != NULL)
		return(tmp);
	unlink(path);

	return(NULL);
}

#define UPLOGFILE ".uplog"
/**
 * Writes a start attempt to the sabaoth start/stop log.  Examination of
 * the log at a later stage reveals crashes of the server.
 */
char *
msab_registerStart(void)
{
	/* The sabaoth uplog is in fact a simple two column table that
	 * contains a start time and a stop time.  Start times are followed
	 * by a tab character, while stop times are followed by a newline.
	 * This allows to detect crashes, while sabaoth only appends to the
	 * uplog. */

	FILE *f;
	char *path = alloca(sizeof(char) * (PATHLENGTH));
	char *tmp;

	if ((tmp = getDBPath(&path, PATHLENGTH, UPLOGFILE)) != NULL)
		return(tmp);

	if ((f = fopen(path, "a")) != NULL) {
		/* append to the file */
		fprintf(f, LLFMT "\t", (lng)time(NULL));
		(void)fflush(f);
		(void)fclose(f);
		return(NULL);
	} else {
		char buf[PATHLENGTH];
		snprintf(buf, sizeof(buf), "failed to open file: %s (%s)",
				strerror(errno), path);
		return(strdup(buf));
	}
}

/**
 * Writes a start attempt to the sabaoth start/stop log.  Examination of
 * the log at a later stage reveals crashes of the server.
 */
char *
msab_registerStop(void)
{
	FILE *f;
	char *path = alloca(sizeof(char) * (PATHLENGTH));
	char *tmp;

	if ((tmp = getDBPath(&path, PATHLENGTH, UPLOGFILE)) != NULL)
		return(tmp);

	if ((f = fopen(path, "a")) != NULL) {
		/* append to the file */
		fprintf(f, LLFMT "\n", (lng)time(NULL));
		(void)fflush(f);
		(void)fclose(f);
		return(NULL);
	} else {
		char buf[PATHLENGTH];
		snprintf(buf, sizeof(buf), "failed to open file: %s (%s)",
				strerror(errno), path);
		return(strdup(buf));
	}
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
	char *buf = alloca(sizeof(char) * (PATHLENGTH));
	char *data = alloca(sizeof(char) * 8096);
	char *path = alloca(sizeof(char) * (PATHLENGTH));
	char *log = alloca(sizeof(char) * (PATHLENGTH));
	char *p;
	FILE *f;
	int fd;
	struct stat statbuf;

	/* Caching strategies (might be nice) should create a new struct with
	 * the last modified time_t of the files involved, such that a stat is
	 * sufficient to see if reparsing is necessary.  The gdk_lock always has
	 * to be checked to detect crashes. */

	sabdb *sdb, *top;
	sdb = top = *ret = NULL;

	buf[PATHLENGTH - 1] = '\0';
	/* scan the parent for directories */
	if ((p = getFarmPath(&path, PATHLENGTH, NULL)) != NULL)
		return(p);
	d = opendir(path);
	if (d == NULL) {
		snprintf(data, sizeof(data), "failed to open directory %s: %s",
				path, strerror(errno));
		return(strdup(data));
	}
	while ((e = readdir(d)) != NULL) {
		if (dbname != NULL && strcmp(e->d_name, dbname) != 0)
			continue;
		if (strcmp(e->d_name, "..") == 0 || strcmp(e->d_name, ".") == 0)
			continue;

		snprintf(buf, PATHLENGTH, "%s/%s/%s", path, e->d_name, UPLOGFILE);
		if (stat(buf, &statbuf) == -1)
			continue;

		if (sdb == NULL) {
			top = sdb = malloc(sizeof(sabdb));
		} else {
			sdb = sdb->next = malloc(sizeof(sabdb));
		}
		sdb->uplog = NULL;
		sdb->next = NULL;

		/* store the database name */
		snprintf(buf, PATHLENGTH, "%s/%s", path, e->d_name);
		sdb->path = strdup(buf);
		sdb->dbname = sdb->path + strlen(sdb->path) - strlen(e->d_name);

		/* add scenarios that are supported */
		sdb->scens = NULL;
		snprintf(buf, PATHLENGTH, "%s/%s/%s", path, e->d_name, SCENARIOFILE);
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
		snprintf(buf, PATHLENGTH, "%s/%s/%s", path, e->d_name, CONNECTIONFILE);
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

		/* check the state of the server by looking at its gdk lock:
		 * - if we can lock it, the server has crashed or isn't running
		 * - if we can't open it because it's locked, the server is
		 *   running
		 * - to distinguish between a crash and proper shutdown, consult
		 *   the uplog
		 */
		snprintf(buf, PATHLENGTH, "%s/%s/%s", path, e->d_name, ".gdk_lock");
		if (_sabaoth_internal_dbname != NULL &&
				strcmp(_sabaoth_internal_dbname, e->d_name) == 0)
		{
			/* if we are the mserver that is running this database,
			 * don't touch the lock! */
			sdb->state = SABdbRunning;
		} else if ((fd = MT_lockf(buf, F_TLOCK, 4, 1)) == -2) {
			/* Locking failed; this can be because the lockfile couldn't
			 * be created.  Probably there is no Mserver running for
			 * that case also.
			 */
			sdb->state = SABdbInactive;
		} else if (fd == -1) {
			/* lock denied, so Mserver is running */
			sdb->state = SABdbRunning;
		} else {
			/* locking succeed, check for a crash in the uplog */
			snprintf(log, PATHLENGTH, "%s/%s/%s", path, e->d_name, UPLOGFILE);
			if ((f = fopen(log, "r")) != NULL) {
				(void)fseek(f, -1, SEEK_END);
				if (fread(data, 1, 1, f) != 1) {
					/* the log is corrupt/wrong, assume no crash */
					sdb->state = SABdbInactive;
				} else if (data[0] == '\n') {
					sdb->state = SABdbInactive;
				} else { /* should be \t */
					sdb->state = SABdbCrashed;
				}
				(void)fclose(f);
			} else {
				/* no uplog file? assume no crash */
				sdb->state = SABdbInactive;
			}

			/* release the lock */
			close(fd);
		}
		snprintf(buf, PATHLENGTH, "%s/%s/%s", path, e->d_name, MAINTENANCEFILE);
		f = fopen(buf, "r");
		if (f != NULL) {
			(void)fclose(f);
			sdb->locked = 1;
		} else {
			sdb->locked = 0;
		}
	}
	(void)closedir(d);

	*ret = top;
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
	char log[PATHLENGTH];
	char data[24];
	char *p;
	FILE *f;
	time_t start, stop, up;
	int avg10[10];
	int avg30[30];
	int i = 0;

	/* early bailout if cached */
	if (db->uplog != NULL) {
		memcpy(ret, db->uplog, sizeof(sabuplog));
		return(NULL);
	}
		
	memset(avg10, 0, sizeof(int) * 10);
	memset(avg30, 0, sizeof(int) * 30);

	/* clear the struct */
	memset(ret, 0, sizeof(sabuplog));
	ret->minuptime = -1;
	ret->lastcrash = -1;

	snprintf(log, sizeof(log), "%s/%s", db->path, UPLOGFILE);
	if ((f = fopen(log, "r")) != NULL) {
		int c;
		start = stop = up = 0;
		p = data;
		while ((c = (char)fgetc(f)) != EOF) {
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
						(start != 0 ? 1 : 0);
					*p = '\0';
					ret->laststart = start = (time_t)atol(data);
					p = data;
				break;
				case '\n':
					/* successful stop */
					ret->stopcntr++;
					*p = '\0';
					stop = (time_t)atol(data);
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
			(start != 0 ? (db->state != SABdbRunning ? 1 : 0) : 0);
		ret->crashcntr =
			ret->startcntr - (db->state == SABdbRunning ? 1 : 0) -
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
		char buf[PATHLENGTH];
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
#define SABDBVER "1"

/**
 * Produces a string representation suitable for storage/sending.
 */
char *
msab_serialise(char **ret, const sabdb *db)
{
	char buf[8096];
	char scens[64];
	char conns[1024];
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

	conns[0] = '\0';
	p = conns;
	avail = sizeof(conns) - 1;
	for (l = db->conns; l != NULL; l = l->next) {
		len = strlen(l->val);
		if (len > avail)
			break;
		memcpy(p, l->val, len);
		p += len + 1;
		avail -= len + 1;
		memcpy(p - 1, "'", 2);
	}
	if (p != conns)
		p[-1] = '\0';

	if ((p = msab_getUplogInfo(&dbu, db)) != NULL)
		return(p);

	/* sabdb + sabuplog structs in one */
	snprintf(buf, sizeof(buf), "sabdb:" SABDBVER ":"
			"%s,%d,%d,%s,%s" ","
			"%d,%d,%d,"
			"%lld,%lld,%lld,"
			"%lld,%lld,"
			"%d,%f,%f",
			db->path, db->locked, (int)(db->state), scens, conns,
			dbu.startcntr, dbu.stopcntr, dbu.crashcntr,
			(lng)dbu.avguptime, (lng)dbu.maxuptime, (lng)dbu.minuptime,
			(lng)dbu.lastcrash, (lng)dbu.laststart,
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
	char *path;
	int locked;
	int state;
	char *scens = "";
	char *conns = "";
	sabdb *s;
	sabuplog *u;
	sablist *l;
	char *p;
	char *lasts;
	char buf[PATHLENGTH];

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
	if (strcmp(lasts, SABDBVER) != 0) {
		snprintf(buf, sizeof(buf), 
				"string has unsupported version: %s", lasts);
		return(strdup(buf));
	}
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain path: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	path = lasts;
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
	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain connections: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	conns = lasts;
	lasts = p;

	/* start parsing sabuplog struct */
	u = malloc(sizeof(sabuplog));

	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain startcounter: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->startcntr = atoi(lasts);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain stopcounter: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->stopcntr = atoi(lasts);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain crashcounter: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->crashcntr = atoi(lasts);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain avguptime: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->avguptime = (time_t)strtoll(lasts, (char **)NULL, 10);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain maxuptime: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->maxuptime = (time_t)strtoll(lasts, (char **)NULL, 10);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain minuptime: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->minuptime = (time_t)strtoll(lasts, (char **)NULL, 10);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain lastcrash: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->lastcrash = (time_t)strtoll(lasts, (char **)NULL, 10);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain laststart: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->laststart = (time_t)strtoll(lasts, (char **)NULL, 10);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain crashavg1: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->crashavg1 = atoi(lasts);
	lasts = p;
	if ((p = strchr(p, ',')) == NULL) {
		snprintf(buf, sizeof(buf), 
				"string does not contain crashavg10: %s", lasts);
		return(strdup(buf));
	}
	*p++ = '\0';
	u->crashavg10 = atof(lasts);
	lasts = p;
	if ((p = strchr(p, ',')) != NULL) {
		snprintf(buf, sizeof(buf), 
				"string does contain additional garbage after crashavg30: %s",
				lasts);
		return(strdup(buf));
	}
	u->crashavg30 = atof(lasts);

	/* fill/create sabdb struct */

	if (strrchr(path, '/') == NULL) {
		snprintf(buf, sizeof(buf), "invalid path: %s", path);
		return(strdup(buf));
	}

	s = malloc(sizeof(sabdb));

	s->path = strdup(path);
	/* msab_freeStatus() actually relies on this trick */
	s->dbname = strrchr(s->path, '/') + 1;
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
	if (strlen(conns) == 0) {
		s->conns = NULL;
	} else {
		l = s->conns = malloc(sizeof(sablist));
		p = strtok_r(conns, "'", &lasts);
		if (p == NULL) {
			l->val = strdup(conns);
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
	s->uplog = u;
	s->next = NULL;

	*ret = s;
	return(NULL);
}
