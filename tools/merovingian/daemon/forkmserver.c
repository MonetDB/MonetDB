/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <sys/types.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <signal.h>
#include <unistd.h>
#include <string.h> /* char ** */
#include <time.h> /* localtime */

#include "msabaoth.h"
#include "utils/utils.h"
#include "utils/glob.h"
#include "utils/properties.h"
#include "mutils.h"

#include "merovingian.h"
#include "discoveryrunner.h" /* remotedb */
#include "multiplex-funnel.h" /* multiplexInit */
#include "forkmserver.h"

#ifndef S_ISDIR
#define S_ISDIR(mode)	(((mode) & _S_IFMT) == _S_IFDIR)
#endif

/**
 * The terminateProcess function tries to let the given mserver process
 * shut down gracefully within a given time-out.  If that fails, it
 * sends the deadly SIGKILL signal to the mserver process and returns.
 */
bool
terminateProcess(char *dbname, pid_t pid, mtype type)
{
	sabdb *stats;
	char *er;
	int i;
	confkeyval *kv;

	er = msab_getStatus(&stats, dbname);
	if (er != NULL) {
		Mfprintf(stderr, "cannot terminate process %lld: %s\n",
				 (long long int)pid, er);
		free(er);
		return false;
	}

	if (stats == NULL) {
		Mfprintf(stderr, "strange, process %lld serves database '%s' "
				 "which does not exist\n", (long long int)pid, dbname);
		return false;
	}

	if (pid == -1) {
		/* it's already dead */
		msab_freeStatus(&stats);
		return true;
	}

	if (stats->pid != pid) {
		Mfprintf(stderr,
			"strange, trying to kill process %lld to stop database '%s' "
			"which seems to be served by process %lld instead\n",
			(long long int)stats->pid,
			dbname,
			(long long int)pid
		);
		if (stats->pid >= 1 && pid < 1) {
			/* assume the server was started by a previous merovingian */
			pid = stats->pid;
		} else {
			msab_freeStatus(&stats);
			return false;
		}
	}
	assert(stats->pid == pid);

	switch (stats->state) {
	case SABdbRunning:
		/* ok, what we expect */
		break;
	case SABdbCrashed:
		Mfprintf(stderr, "cannot shut down database '%s', mserver "
				 "(pid %lld) has crashed\n",
				 dbname, (long long int)pid);
		msab_freeStatus(&stats);
		return false;
	case SABdbInactive:
		Mfprintf(stdout, "database '%s' appears to have shut down already\n",
				 dbname);
		fflush(stdout);
		msab_freeStatus(&stats);
		return false;
	case SABdbStarting:
		Mfprintf(stderr, "database '%s' appears to be starting up\n",
				 dbname);
		/* starting up, so we'll go to the shut down phase */
		break;
	default:
		Mfprintf(stderr, "unknown state: %d\n", (int)stats->state);
		msab_freeStatus(&stats);
		return false;
	}

	if (type == MEROFUN) {
		multiplexDestroy(dbname);
		msab_freeStatus(&stats);
		return true;
	} else if (type != MERODB) {
		/* barf */
		Mfprintf(stderr, "cannot stop merovingian process role: %s\n",
				 dbname);
		msab_freeStatus(&stats);
		return false;
	}

	/* ok, once we get here, we'll be shutting down the server */
	Mfprintf(stdout, "sending process %lld (database '%s') the "
			 "TERM signal\n", (long long int)pid, dbname);
	if (kill(pid, SIGTERM) < 0) {
		/* barf */
		Mfprintf(stderr, "cannot send TERM signal to process (database '%s'): %s\n",
				 dbname, strerror(errno));
		msab_freeStatus(&stats);
		return false;
	}
	kv = findConfKey(_mero_props, "exittimeout");
	int exittimeout = atoi(kv->val);
	for (i = 0; exittimeout < 0 || i < exittimeout * 2; i++) {
		if (stats != NULL)
			msab_freeStatus(&stats);
		sleep_ms(500);
		er = msab_getStatus(&stats, dbname);
		if (er != NULL) {
			Mfprintf(stderr, "unexpected problem: %s\n", er);
			free(er);
			/* don't die, just continue, so we KILL in the end */
		} else if (stats == NULL) {
			Mfprintf(stderr, "hmmmm, database '%s' suddenly doesn't exist "
					 "any more\n", dbname);
		} else {
			switch (stats->state) {
			case SABdbRunning:
			case SABdbStarting:
				/* ok, try again */
				break;
			case SABdbCrashed:
				Mfprintf (stderr, "database '%s' crashed after SIGTERM\n",
						  dbname);
				msab_freeStatus(&stats);
				return true;
			case SABdbInactive:
				Mfprintf(stdout, "database '%s' has shut down\n", dbname);
				fflush(stdout);
				msab_freeStatus(&stats);
				return true;
			default:
				Mfprintf(stderr, "unknown state: %d\n", (int)stats->state);
				break;
			}
		}
	}
	Mfprintf(stderr, "timeout of %s seconds expired, sending process %lld"
			 " (database '%s') the KILL signal\n",
			 kv->val, (long long int)pid, dbname);
	kill(pid, SIGKILL);
	msab_freeStatus(&stats);
	return true;
}

/**
 * Fork an mserver and detach.  Before forking off, Sabaoth is consulted
 * to see if forking makes sense, or whether it is necessary at all, or
 * forbidden by restart policy, e.g. when in maintenance.
 */

#define MAX_NR_ARGS 511

err
forkMserver(const char *database, sabdb** stats, bool force)
{
	pid_t pid;
	char *er;
	sabuplog info;
	struct tm *t;
	char tstr[20];
	int pfdo[2];
	int pfde[2];
	dpair dp;
	char vaultkey[512];
	struct stat statbuf;
	char upmin[8];
	char upavg[8];
	char upmax[8];
	confkeyval *ckv, *kv, *list;
	SABdbState state;
	char *sabdbfarm;
	char dbpath[1024];
	char dbextra_path[1024];
	char dbtrace_path[1024];
	char listenaddr[512];
	char muri[512]; /* possibly undersized */
	char usock[512];
	bool mydoproxy;
	char nthreads[32];
	char nclients[32];
	char pipeline[512];
	char memmaxsize[64];
	char vmmaxsize[64];
	char *readonly = NULL;
	char *embeddedr = NULL;
	char *embeddedpy = NULL;
	char *embeddedc = NULL;
	char *raw_strings = NULL;
	bool ipv6 = false;
	char *dbextra = NULL;
	char *dbtrace = NULL;
	char *mserver5_extra = NULL;
	char *mserver5_extra_token = NULL;
	char *argv[MAX_NR_ARGS+1];	/* for the exec arguments */
	char property_other[1024];
	int c = 0;
	int freec = 0;				/* from where to free entries in argv */
	unsigned int mport;
	char *set = "--set";

	/* Find or allocate a dpair entry for this database */
	pthread_mutex_lock(&_mero_topdp_lock);
	dp = _mero_topdp->next;
	while (strcmp(dp->dbname, database) != 0) {
		if (dp->next == NULL) {
			dp = dp->next = malloc(sizeof(struct _dpair));
			*dp = (struct _dpair) {
				.dbname = strdup(database),
				.fork_lock = PTHREAD_MUTEX_INITIALIZER,
			};
			break;
		}
		dp = dp->next;
	}
	pthread_mutex_unlock(&_mero_topdp_lock);

	/* Make sure we only start one mserver5 at the same time. */
	pthread_mutex_lock(&dp->fork_lock);

	er = msab_getStatus(stats, database);
	if (er != NULL) {
		pthread_mutex_unlock(&dp->fork_lock);
		err e = newErr("%s", er);
		free(er);
		return(e);
	}

	/* NOTE: remotes also include locals through self announcement */
	if (*stats == NULL) {
		*stats = getRemoteDB(database);
		pthread_mutex_unlock(&dp->fork_lock);

		if (*stats != NULL)
			return(NO_ERR);

		return(newErr("no such database: %s", database));
	}

	/* Since we ask for a specific database, it should be either there
	 * or not there.  Since we checked the latter case above, it should
	 * just be there, and be the right one.  There also shouldn't be
	 * more than one entry in the list, so we assume we have the right
	 * one here. */

	if ((*stats)->state == SABdbRunning) {
		/* return before doing expensive stuff, when this db just seems
		 * to be running */
		pthread_mutex_unlock(&dp->fork_lock);
		return(NO_ERR);
	}

	ckv = getDefaultProps();
	readAllProps(ckv, (*stats)->path);
	kv = findConfKey(ckv, "type");
	if (kv->val == NULL)
		kv = findConfKey(_mero_db_props, "type");

	if ((*stats)->locked) {
		if (!force) {
			Mfprintf(stdout, "%s '%s' is under maintenance\n",
					 kv->val, database);
			freeConfFile(ckv);
			free(ckv);
			pthread_mutex_unlock(&dp->fork_lock);
			return(NO_ERR);
		} else {
			Mfprintf(stdout, "startup of %s under maintenance "
					 "'%s' forced\n", kv->val, database);
		}
	}

	/* retrieve uplog information to print a short conclusion */
	er = msab_getUplogInfo(&info, *stats);
	if (er != NULL) {
		err e = newErr("could not retrieve uplog information: %s", er);
		free(er);
		msab_freeStatus(stats);
		freeConfFile(ckv);
		free(ckv);
		pthread_mutex_unlock(&dp->fork_lock);
		return(e);
	}

	switch ((*stats)->state) {
	case SABdbRunning:
		freeConfFile(ckv);
		free(ckv);
		pthread_mutex_unlock(&dp->fork_lock);
		return(NO_ERR);
	case SABdbCrashed:
		t = localtime(&info.lastcrash);
		strftime(tstr, sizeof(tstr), "%Y-%m-%d %H:%M:%S", t);
		secondsToString(upmin, info.minuptime, 1);
		secondsToString(upavg, info.avguptime, 1);
		secondsToString(upmax, info.maxuptime, 1);
		Mfprintf(stdout, "%s '%s' has crashed after start on %s, "
				 "attempting restart, "
				 "up min/avg/max: %s/%s/%s, "
				 "crash average: %d.00 %.2f %.2f (%d-%d=%d)\n",
				 kv->val, database, tstr,
				 upmin, upavg, upmax,
				 info.crashavg1, info.crashavg10, info.crashavg30,
				 info.startcntr, info.stopcntr, info.crashcntr);
		break;
	case SABdbInactive:
		secondsToString(upmin, info.minuptime, 1);
		secondsToString(upavg, info.avguptime, 1);
		secondsToString(upmax, info.maxuptime, 1);
		Mfprintf(stdout, "starting %s '%s', "
				 "up min/avg/max: %s/%s/%s, "
				 "crash average: %d.00 %.2f %.2f (%d-%d=%d)\n",
				 kv->val, database,
				 upmin, upavg, upmax,
				 info.crashavg1, info.crashavg10, info.crashavg30,
				 info.startcntr, info.stopcntr, info.crashcntr);
		break;
	default:
		/* this also includes SABdbStarting, which we shouldn't ever
		 * see due to the global starting lock
		 *
		 * if SABdbStarting: a process (presumably mserver5) has locked
		 * the database (i.e. the .gdk_lock file), but the server is not
		 * ready to accept connections (i.e. there is no .started
		 * file) */
		state = (*stats)->state;
		msab_freeStatus(stats);
		freeConfFile(ckv);
		free(ckv);
		pthread_mutex_unlock(&dp->fork_lock);
		if (state == SABdbStarting)
			return(newErr("unexpected state: database is locked but not yet started"));
		else
			return(newErr("unknown or impossible state: %d",
						  (int)state));
	}

	/* create the pipes (filedescriptors) now, such that we and the
	 * child have the same descriptor set */
	if (pipe(pfdo) == -1) {
		int e = errno;
		msab_freeStatus(stats);
		freeConfFile(ckv);
		free(ckv);
		pthread_mutex_unlock(&dp->fork_lock);
		return(newErr("unable to create pipe: %s", strerror(e)));
	}
	if (pipe(pfde) == -1) {
		int e = errno;
		close(pfdo[0]);
		close(pfdo[1]);
		msab_freeStatus(stats);
		freeConfFile(ckv);
		free(ckv);
		pthread_mutex_unlock(&dp->fork_lock);
		return(newErr("unable to create pipe: %s", strerror(e)));
	}

	/* a multiplex-funnel means starting a separate thread */
	if (strcmp(kv->val, "mfunnel") == 0) {
		FILE *f1, *f2;
		/* fill in the rest of the dpair entry */
		pthread_mutex_lock(&_mero_topdp_lock);

		dp->input[0].fd = pfdo[0];
		dp->input[1].fd = pfde[0];
		dp->type = MEROFUN;
		dp->pid = getpid();
		dp->flag = 0;

		pthread_mutex_unlock(&_mero_topdp_lock);

		kv = findConfKey(ckv, "mfunnel");
		if(!(f1 = fdopen(pfdo[1], "a"))) {
			msab_freeStatus(stats);
			freeConfFile(ckv);
			free(ckv);
			pthread_mutex_unlock(&dp->fork_lock);
			close(pfdo[1]);
			close(pfde[1]);
			return newErr("Failed to open file descriptor\n");
		}
		if(!(f2 = fdopen(pfde[1], "a"))) {
			fclose(f1);
			close(pfde[1]);
			msab_freeStatus(stats);
			freeConfFile(ckv);
			free(ckv);
			pthread_mutex_unlock(&dp->fork_lock);
			return newErr("Failed to open file descriptor\n");
		}
		if ((er = multiplexInit(database, kv->val, f1, f2)) != NO_ERR) {
			Mfprintf(stderr, "failed to create multiplex-funnel: %s\n",
					 getErrMsg(er));
			msab_freeStatus(stats);
			freeConfFile(ckv);
			free(ckv);
			pthread_mutex_unlock(&dp->fork_lock);
			return(er);
		}
		freeConfFile(ckv);
		free(ckv);

		/* refresh stats, now we will have a connection registered */
		msab_freeStatus(stats);
		er = msab_getStatus(stats, database);
		pthread_mutex_unlock(&dp->fork_lock);
		if (er != NULL) {
			/* since the client mserver lives its own life anyway,
			 * it's not really a problem we exit here */
			err e = newErr("%s", er);
			free(er);
			return(e);
		}
		return(NO_ERR);
	}

	/* check if the vaultkey is there, otherwise abort early (value
	 * later on reused when server is started) */
	snprintf(vaultkey, sizeof(vaultkey), "%s/.vaultkey", (*stats)->path);
	if (stat(vaultkey, &statbuf) == -1) {
		msab_freeStatus(stats);
		freeConfFile(ckv);
		free(ckv);
		pthread_mutex_unlock(&dp->fork_lock);
		close(pfdo[0]);
		close(pfdo[1]);
		close(pfde[0]);
		close(pfde[1]);
		return(newErr("cannot start database '%s': no .vaultkey found "
					  "(did you create the database with `monetdb create %s`?)",
					  database, database));
	}

	er = msab_getDBfarm(&sabdbfarm);
	if (er != NULL) {
		msab_freeStatus(stats);
		freeConfFile(ckv);
		free(ckv);
		pthread_mutex_unlock(&dp->fork_lock);
		close(pfdo[0]);
		close(pfdo[1]);
		close(pfde[0]);
		close(pfde[1]);
		return(er);
	}

	mydoproxy = strcmp(getConfVal(_mero_props, "forward"), "proxy") == 0;

	kv = findConfKey(ckv, "nthreads");
	if (kv->val == NULL)
		kv = findConfKey(_mero_db_props, "nthreads");
	if (kv->val != NULL) {
		snprintf(nthreads, sizeof(nthreads), "gdk_nr_threads=%s", kv->val);
	} else {
		nthreads[0] = '\0';
	}

	kv = findConfKey(ckv, "nclients");
	if (kv->val == NULL)
		kv = findConfKey(_mero_db_props, "nclients");
	if (kv->val != NULL) {
		snprintf(nclients, sizeof(nclients), "max_clients=%s", kv->val);
	} else {
		nclients[0] = '\0';
	}

	kv = findConfKey(ckv, "optpipe");
	if (kv->val == NULL)
		kv = findConfKey(_mero_db_props, "optpipe");
	if (kv->val != NULL) {
		snprintf(pipeline, sizeof(pipeline), "sql_optimizer=%s", kv->val);
	} else {
		pipeline[0] = '\0';
	}

	kv = findConfKey(ckv, "memmaxsize");
	if (kv->val != NULL) {
		snprintf(memmaxsize, sizeof(memmaxsize), "gdk_mem_maxsize=%s", kv->val);
	} else {
		memmaxsize[0] = '\0';
	}

	kv = findConfKey(ckv, "vmmaxsize");
	if (kv->val != NULL) {
		snprintf(vmmaxsize, sizeof(vmmaxsize), "gdk_vm_maxsize=%s", kv->val);
	} else {
		vmmaxsize[0] = '\0';
	}

	kv = findConfKey(ckv, "readonly");
	if (kv->val != NULL && strcmp(kv->val, "no") != 0)
		readonly = "--readonly";

	kv = findConfKey(ckv, "embedr");
	if (kv->val != NULL && strcmp(kv->val, "no") != 0)
		embeddedr = "embedded_r=true";

	kv = findConfKey(ckv, "embedpy3");
	if (kv->val != NULL && strcmp(kv->val, "no") != 0) {
		if (embeddedpy) {
			// only one python version can be active at a time
			msab_freeStatus(stats);
			freeConfFile(ckv);
			free(ckv);
			pthread_mutex_unlock(&dp->fork_lock);
			free(sabdbfarm);
			return newErr("attempting to start mserver with both embedded python2 and embedded python3; only one python version can be active at a time\n");
		}
		embeddedpy = "embedded_py=3";
	}
	kv = findConfKey(ckv, "embedc");
	if (kv->val != NULL && strcmp(kv->val, "no") != 0)
		embeddedc = "embedded_c=true";
	kv = findConfKey(ckv, "dbextra");
	if (kv != NULL && kv->val != NULL) {
		dbextra = kv->val;
	}

	kv = findConfKey(ckv, "dbtrace");
	if (kv != NULL && kv->val != NULL)
		dbtrace = kv->val;

	kv = findConfKey(ckv, "listenaddr");
	if (kv->val != NULL) {
		if (mydoproxy) {
			// listenaddr is only available on forwarding method
			msab_freeStatus(stats);
			freeConfFile(ckv);
			free(ckv);
			pthread_mutex_unlock(&dp->fork_lock);
			free(sabdbfarm);
			return newErr("attempting to start mserver with listening address while being proxied by monetdbd; this option is only possible on forward method\n");
		}
		snprintf(listenaddr, sizeof(listenaddr), "mapi_listenaddr=%s", kv->val);
	} else {
		listenaddr[0] = '\0';
	}

	kv = findConfKey(ckv, "raw_strings");
	if (kv->val != NULL && strcmp(kv->val, "no") != 0) {
		raw_strings="raw_strings=true";
	}
	mport = (unsigned int)getConfNum(_mero_props, "port");

	/* ok, now exec that mserver we want */
	snprintf(dbpath, sizeof(dbpath),
			 "--dbpath=%s/%s", sabdbfarm, database);
	free(sabdbfarm);
	snprintf(vaultkey, sizeof(vaultkey),
			 "monet_vault_key=%s/.vaultkey", (*stats)->path);
	snprintf(muri, sizeof(muri),
			 "merovingian_uri=mapi:monetdb://%s:%u/%s",
			 _mero_hostname, mport, database);
	argv[c++] = _mero_mserver;
	argv[c++] = dbpath;
	argv[c++] = set; argv[c++] = muri;
	if (dbextra != NULL) {
		snprintf(dbextra_path, sizeof(dbextra_path),
				 "--dbextra=%s", dbextra);
		argv[c++] = dbextra_path;
	}

	if (dbtrace != NULL) {
		snprintf(dbtrace_path, sizeof(dbtrace_path),
				 "--dbtrace=%s", dbtrace);
		argv[c++] = dbtrace_path;
	}

	if (strncmp(listenaddr, "127.0.0.1", strlen("127.0.0.1")) == 0 ||
		strncmp(listenaddr, "0.0.0.0", strlen("0.0.0.0")) == 0) {
		ipv6 = false;
	} else {
		ipv6 = true;
	}

	if (mydoproxy) {
		/* we "proxy", so we can just solely use UNIX domain sockets
		 * internally.  Before we hit our head, check if we can
		 * actually use a UNIX socket (due to pathlength) */
		if (strlen((*stats)->path) + 11 < sizeof(((struct sockaddr_un *) 0)->sun_path)) {
			argv[c++] = set; argv[c++] = "mapi_listenaddr=none";
			snprintf(usock, sizeof(usock), "mapi_usock=%s/.mapi.sock",
					 (*stats)->path);
			argv[c++] = set; argv[c++] = usock;
		} else {
			/* for logic here, see comment below */
			argv[c++] = set; argv[c++] = "mapi_port=0";
			argv[c++] = set; argv[c++] = ipv6 ? "mapi_listenaddr=localhost" : "mapi_listenaddr=127.0.0.1";
			snprintf(usock, sizeof(usock), "mapi_usock=");
			argv[c++] = set; argv[c++] = usock;
		}
	} else {
		if (listenaddr[0] != '\0') {
			argv[c++] = set; argv[c++] = listenaddr;
		} else {
			argv[c++] = set; argv[c++] = ipv6 ? "mapi_listenaddr=all" : "mapi_listenaddr=0.0.0.0";
		}
		argv[c++] = set; argv[c++] = "mapi_port=0";
		/* avoid this mserver binding to the same port as merovingian
		 * but on another interface, (INADDR_ANY ... sigh) causing
		 * endless redirects since 0.0.0.0 is not a valid address to
		 * connect to, and hence the hostname is advertised instead */
		snprintf(usock, sizeof(usock), "mapi_usock=");
		argv[c++] = set; argv[c++] = usock;
	}
	argv[c++] = set; argv[c++] = vaultkey;
	if (nthreads[0] != '\0') {
		argv[c++] = set; argv[c++] = nthreads;
	}
	if (nclients[0] != '\0') {
		argv[c++] = set; argv[c++] = nclients;
	}
	if (pipeline[0] != '\0') {
		argv[c++] = set; argv[c++] = pipeline;
	}
	if (memmaxsize[0] != '\0') {
		argv[c++] = set; argv[c++] = memmaxsize;
	}
	if (vmmaxsize[0] != '\0') {
		argv[c++] = set; argv[c++] = vmmaxsize;
	}
	if (embeddedr != NULL) {
		argv[c++] = set; argv[c++] = embeddedr;
	}
	if (embeddedpy != NULL) {
		argv[c++] = set; argv[c++] = embeddedpy;
	}
	if (embeddedc != NULL) {
		argv[c++] = set; argv[c++] = embeddedc;
	}
	if (readonly != NULL) {
		argv[c++] = readonly;
	}
	if (raw_strings != NULL) {
		argv[c++] = set; argv[c++] = raw_strings;
	}
	/* get the rest (non-default) mserver props set in the conf file */
	list = ckv;
	freec = c;					/* following entries to be freed if != set */
	while (list->key != NULL) {
		if (list->val != NULL && !defaultProperty(list->key)) {
			if (strcmp(list->key, "gdk_debug") == 0) {
				snprintf(property_other, sizeof(property_other), "-d%s", list->val);
			} else {
				argv[c++] = set;
				snprintf(property_other, sizeof(property_other), "%s=%s", list->key, list->val);
			}
			argv[c++] = strdup(property_other);
		}
		list++;
	}

	/* Let's get extra mserver5 args from the environment */
	mserver5_extra = getenv("MSERVER5_EXTRA_ARGS");
	if (mserver5_extra != NULL) {
		/* work on copy of the environment value since strtok_r changes it */
		mserver5_extra = strdup(mserver5_extra);
		if (mserver5_extra != NULL) {
			char *sp = NULL;
			mserver5_extra_token = strtok_r(mserver5_extra, " ", &sp);
			while (c < MAX_NR_ARGS && mserver5_extra_token != NULL) {
				argv[c++] = strdup(mserver5_extra_token);
				mserver5_extra_token = strtok_r(NULL, " ", &sp);
			}
			free(mserver5_extra);
		}
	}

	argv[c++] = NULL;

	freeConfFile(ckv);
	free(ckv); /* can make ckv static and reuse it all the time */

	/* make sure we're not inside childhandler() fiddling with our
	 * dpair instance */
	pthread_mutex_lock(&_mero_topdp_lock);

	pid = fork();
	if (pid == 0) {
		/* redirect stdout and stderr to a new pair of fds for
		 * logging help */
		ssize_t write_error;	/* to avoid compiler warning */
		int dup_err;
		close(pfdo[0]);
		dup_err = dup2(pfdo[1], 1);
		if(dup_err == -1)
			perror("dup2");
		close(pfdo[1]);

		close(pfde[0]);
		dup_err = dup2(pfde[1], 2);
		if(dup_err == -1)
			perror("dup2");
		close(pfde[1]);

		write_error = write(1, "arguments:", 10);
		for (c = 0; argv[c] != NULL; c++) {
			/* very stupid heuristic to make copy/paste easier from
			 * merovingian's log */
			int q = strchr(argv[c], ' ') != NULL;
			write_error |= write(1, " \"", 1 + q);
			write_error |= write(1, argv[c], strlen(argv[c]));
			if (q)
				write_error |= write(1, "\"", 1);
		}
		write_error |= write(1, "\n", 1);
		if (write_error < 0)
			perror("write");

		execv(_mero_mserver, argv);
		/* if the exec returns, it is because of a failure */
		perror("executing failed");
		exit(1);
	} else if (pid > 0) {
		/* parent: fine, let's add the pipes for this child */
		dp->input[0].fd = pfdo[0];
		close(pfdo[1]);
		dp->input[1].fd = pfde[0];
		close(pfde[1]);
		dp->type = MERODB;
		dp->pid = pid;
		dp->flag = 0;
		pthread_mutex_unlock(&_mero_topdp_lock);

		while (argv[freec] != NULL) {
			if (argv[freec] != set)
				free(argv[freec]);
			freec++;
		}

		/* wait for the child to finish starting, at some point we
		 * decided that we should wait indefinitely here because if the
		 * mserver needs time to start up, we shouldn't interrupt it,
		 * and if it hangs, we're just doomed, with the drawback that we
		 * completely kill the functionality of monetdbd too */
		do {
			/* give the database a break */
			sleep_ms(500);

			/* stats cannot be NULL, as we don't allow starting non
			 * existing databases, note that we need to run this loop at
			 * least once not to leak */
			msab_freeStatus(stats);
			er = msab_getStatus(stats, database);
			if (er != NULL) {
				/* since the client mserver lives its own life anyway,
				 * it's not really a problem we exit here */
				err e = newErr("%s", er);
				free(er);
				pthread_mutex_unlock(&dp->fork_lock);
				return(e);
			}

			/* in the meanwhile, if the server has stopped (and been
			 * waited for), the pid will have been set to -1, so check
			 * that. */
			pthread_mutex_lock(&_mero_topdp_lock);
			if (dp->pid == -1) {
				pthread_mutex_unlock(&_mero_topdp_lock);
				break;
			}
			pthread_mutex_unlock(&_mero_topdp_lock);
		} while ((*stats)->state != SABdbRunning);

		/* check if the SQL scenario was loaded */
		if (dp->pid != -1 && (*stats)->state == SABdbRunning &&
			(*stats)->conns != NULL &&
			(*stats)->conns->val != NULL &&
			(*stats)->scens != NULL &&
			(*stats)->scens->val != NULL) {
			sablist *scen = (*stats)->scens;
			do {
				if (scen->val != NULL && strcmp(scen->val, "sql") == 0)
					break;
			} while ((scen = scen->next) != NULL);
			if (scen == NULL) {
				/* we don't know what it's doing, but we don't like it
				 * any case, so kill it */
				(void) terminateProcess(dp->dbname, dp->pid, MERODB);
				msab_freeStatus(stats);
				pthread_mutex_unlock(&dp->fork_lock);
				return(newErr("database '%s' did not initialise the sql "
							  "scenario", database));
			}
		} else if (dp->pid != -1) {
			(void) terminateProcess(dp->dbname, dp->pid, MERODB);
			msab_freeStatus(stats);
			pthread_mutex_unlock(&dp->fork_lock);
			return(newErr(
					   "database '%s' started up, but failed to open up "
					   "a communication channel", database));
		}

		/* try to be clear on why starting the database failed */
		if (dp->pid == -1) {
			state = (*stats)->state;

			pthread_mutex_unlock(&dp->fork_lock);

			/* starting failed */
			msab_freeStatus(stats);

			switch ((int)state) {
			case SABdbRunning:
				/* right, it's not there, but it's running */
				return(newErr(
						   "database '%s' has inconsistent state "
						   "(sabaoth administration reports running, "
						   "but process seems gone), "
						   "review monetdbd's "
						   "logfile (%s) for any peculiarities", database,
						   getConfVal(_mero_props, "logfile")));
			case SABdbCrashed:
				return(newErr(
						   "database '%s' has crashed after starting, "
						   "manual intervention needed, "
						   "check monetdbd's logfile (%s) for details",
						   database, getConfVal(_mero_props, "logfile")));
			case SABdbInactive:
				return(newErr(
						   "database '%s' appears to shut "
						   "itself down after starting, "
						   "check monetdbd's logfile (%s) for possible "
						   "hints", database,
						   getConfVal(_mero_props, "logfile")));
			case SABdbStarting:
				return(newErr(
						   "database '%s' has inconsistent state "
						   "(sabaoth administration reports starting up, "
						   "but process seems gone), "
						   "review monetdbd's "
						   "logfile (%s) for any peculiarities", database,
						   getConfVal(_mero_props, "logfile")));
			default:
				return(newErr("unknown state: %d", (int)state));
			}
		}

		pthread_mutex_unlock(&dp->fork_lock);

		if ((*stats)->locked) {
			Mfprintf(stdout, "database '%s' has been put into maintenance "
					 "mode during startup\n", database);
		}

		return(NO_ERR);
	}
	/* pid < 0: fork failed */
	int e = errno;

	/* forking failed somehow, cleanup the pipes */
	close(pfdo[0]);
	close(pfdo[1]);
	close(pfde[0]);
	close(pfde[1]);
	msab_freeStatus(stats);
	pthread_mutex_unlock(&dp->fork_lock);
	pthread_mutex_unlock(&_mero_topdp_lock);
	return(newErr("%s", strerror(e)));
}
