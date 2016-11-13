/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h> /* char ** */
#include <time.h> /* localtime */
#include <errno.h>

#include <msabaoth.h>
#include <utils/utils.h>
#include <utils/glob.h>
#include <utils/properties.h>

#include "merovingian.h"
#include "discoveryrunner.h" /* remotedb */
#include "multiplex-funnel.h" /* multiplexInit */
#include "forkmserver.h"


static pthread_mutex_t fork_lock = PTHREAD_MUTEX_INITIALIZER;

/**
 * The terminateProcess function tries to let the given mserver process
 * shut down gracefully within a given time-out.  If that fails, it
 * sends the deadly SIGKILL signal to the mserver process and returns.
 */
void
terminateProcess(pid_t pid, char *dbname, mtype type, int lock)
{
	sabdb *stats;
	char *er;
	int i;
	confkeyval *kv;

	if (lock)
		pthread_mutex_lock(&fork_lock);

	er = msab_getStatus(&stats, dbname);
	if (er != NULL) {
		if (lock)
			pthread_mutex_unlock(&fork_lock);
		Mfprintf(stderr, "cannot terminate process " LLFMT ": %s\n",
				(long long int)pid, er);
		free(er);
		free(dbname);
		return;
	}

	if (stats == NULL) {
		if (lock)
			pthread_mutex_unlock(&fork_lock);
		Mfprintf(stderr, "strange, process " LLFMT " serves database '%s' "
				"which does not exist\n", (long long int)pid, dbname);
		free(dbname);
		return;
	}

	switch (stats->state) {
		case SABdbRunning:
			/* ok, what we expect */
		break;
		case SABdbCrashed:
			if (lock)
				pthread_mutex_unlock(&fork_lock);
			Mfprintf(stderr, "cannot shut down database '%s', mserver "
					"(pid " LLFMT ") has crashed\n",
					dbname, (long long int)pid);
			msab_freeStatus(&stats);
			free(dbname);
			return;
		case SABdbInactive:
			if (lock)
				pthread_mutex_unlock(&fork_lock);
			Mfprintf(stdout, "database '%s' appears to have shut down already\n",
					dbname);
			fflush(stdout);
			msab_freeStatus(&stats);
			free(dbname);
			return;
		case SABdbStarting:
			if (lock)
				pthread_mutex_unlock(&fork_lock);
			Mfprintf(stderr, "database '%s' appears to be starting up\n",
					 dbname);
			/* starting up, so we'll go to the shut down phase */
			break;
		default:
			if (lock)
				pthread_mutex_unlock(&fork_lock);
			Mfprintf(stderr, "unknown state: %d\n", (int)stats->state);
			msab_freeStatus(&stats);
			free(dbname);
			return;
	}

	if (type == MEROFUN) {
		if (lock)
			pthread_mutex_unlock(&fork_lock);
		multiplexDestroy(dbname);
		msab_freeStatus(&stats);
		free(dbname);
		return;
	} else if (type != MERODB) {
		/* barf */
		if (lock)
			pthread_mutex_unlock(&fork_lock);
		Mfprintf(stderr, "cannot stop merovingian process role: %s\n", dbname);
		msab_freeStatus(&stats);
		free(dbname);
		return;
	}

	/* ok, once we get here, we'll be shutting down the server */
	Mfprintf(stdout, "sending process " LLFMT " (database '%s') the "
			"TERM signal\n", (long long int)pid, dbname);
	kill(pid, SIGTERM);
	kv = findConfKey(_mero_props, "exittimeout");
	for (i = 0; i < atoi(kv->val) * 2; i++) {
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
					if (lock)
						pthread_mutex_unlock(&fork_lock);
					Mfprintf (stderr, "database '%s' crashed after SIGTERM\n",
							dbname);
					msab_freeStatus(&stats);
					free(dbname);
					return;
				case SABdbInactive:
					if (lock)
						pthread_mutex_unlock(&fork_lock);
					Mfprintf(stdout, "database '%s' has shut down\n", dbname);
					fflush(stdout);
					msab_freeStatus(&stats);
					free(dbname);
					return;
				default:
					Mfprintf(stderr, "unknown state: %d\n", (int)stats->state);
				break;
			}
		}
	}
	Mfprintf(stderr, "timeout of %s seconds expired, sending process " LLFMT
			" (database '%s') the KILL signal\n",
			kv->val, (long long int)pid, dbname);
	kill(pid, SIGKILL);
	msab_freeStatus(&stats);
	free(dbname);
	if (lock)
		pthread_mutex_unlock(&fork_lock);
}

/**
 * Fork an mserver and detach.  Before forking off, Sabaoth is consulted
 * to see if forking makes sense, or whether it is necessary at all, or
 * forbidden by restart policy, e.g. when in maintenance.
 */
err
forkMserver(char *database, sabdb** stats, int force)
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

	er = msab_getStatus(stats, database);
	if (er != NULL) {
		err e = newErr("%s", er);
		free(er);
		return(e);
	}

	/* NOTE: remotes also include locals through self announcement */
	if (*stats == NULL) {
		*stats = getRemoteDB(database);

		if (*stats != NULL)
			return(NO_ERR);

		return(newErr("no such database: %s", database));
	}

	/* Since we ask for a specific database, it should be either there
	 * or not there.  Since we checked the latter case above, it should
	 * just be there, and be the right one.  There also shouldn't be
	 * more than one entry in the list, so we assume we have the right
	 * one here. */

	if ((*stats)->state == SABdbRunning)
		/* return before doing expensive stuff, when this db just seems
		 * to be running */
		return(NO_ERR);

	/* Make sure we only start one mserver5 at the same time, this is a
	 * horsedrug for preventing race-conditions where two or more
	 * clients start the same database at the same time, because they
	 * were all identified as being SABdbInactive.  If this "global"
	 * lock ever becomes a problem, we can reduce it to a per-database
	 * lock instead. */
	pthread_mutex_lock(&fork_lock);

	/* refetch the status, as it may have changed */
	msab_freeStatus(stats);
	er = msab_getStatus(stats, database);
	if (er != NULL) {
		err e = newErr("%s", er);
		free(er);
		pthread_mutex_unlock(&fork_lock);
		return(e);
	}

	ckv = getDefaultProps();
	readAllProps(ckv, (*stats)->path);
	kv = findConfKey(ckv, "type");
	if (kv->val == NULL)
		kv = findConfKey(_mero_db_props, "type");

	if ((*stats)->locked == 1) {
		if (force == 0) {
			Mfprintf(stdout, "%s '%s' is under maintenance\n",
					kv->val, database);
			freeConfFile(ckv);
			free(ckv);
			pthread_mutex_unlock(&fork_lock);
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
		pthread_mutex_unlock(&fork_lock);
		return(e);
	}

	switch ((*stats)->state) {
		case SABdbRunning:
			freeConfFile(ckv);
			free(ckv);
			pthread_mutex_unlock(&fork_lock);
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
			 * see due to the global starting lock */
			state = (*stats)->state;
			msab_freeStatus(stats);
			freeConfFile(ckv);
			free(ckv);
			pthread_mutex_unlock(&fork_lock);
			return(newErr("unknown or impossible state: %d",
						(int)state));
	}

	/* create the pipes (filedescriptors) now, such that we and the
	 * child have the same descriptor set */
	if (pipe(pfdo) == -1) {
		msab_freeStatus(stats);
		freeConfFile(ckv);
		free(ckv);
		pthread_mutex_unlock(&fork_lock);
		return(newErr("unable to create pipe: %s", strerror(errno)));
	}
	if (pipe(pfde) == -1) {
		close(pfdo[0]);
		close(pfdo[1]);
		msab_freeStatus(stats);
		freeConfFile(ckv);
		free(ckv);
		pthread_mutex_unlock(&fork_lock);
		return(newErr("unable to create pipe: %s", strerror(errno)));
	}

	/* a multiplex-funnel means starting a separate thread */
	if (strcmp(kv->val, "mfunnel") == 0) {
		/* create a dpair entry */
		pthread_mutex_lock(&_mero_topdp_lock);

		dp = _mero_topdp;
		while (dp->next != NULL)
			dp = dp->next;
		dp = dp->next = malloc(sizeof(struct _dpair));
		dp->out = pfdo[0];
		dp->err = pfde[0];
		dp->next = NULL;
		dp->type = MEROFUN;
		dp->pid = getpid();
		dp->dbname = strdup(database);
		dp->flag = 0;

		pthread_mutex_unlock(&_mero_topdp_lock);

		kv = findConfKey(ckv, "mfunnel");
		if ((er = multiplexInit(database, kv->val,
						fdopen(pfdo[1], "a"), fdopen(pfde[1], "a"))) != NO_ERR)
		{
			Mfprintf(stderr, "failed to create multiplex-funnel: %s\n",
					getErrMsg(er));
			freeConfFile(ckv);
			free(ckv);
			pthread_mutex_unlock(&fork_lock);
			return(er);
		}
		freeConfFile(ckv);
		free(ckv);

		/* refresh stats, now we will have a connection registered */
		msab_freeStatus(stats);
		er = msab_getStatus(stats, database);
		if (er != NULL) {
			/* since the client mserver lives its own life anyway,
			 * it's not really a problem we exit here */
			err e = newErr("%s", er);
			free(er);
			pthread_mutex_unlock(&fork_lock);
			return(e);
		}
		pthread_mutex_unlock(&fork_lock);
		return(NO_ERR);
	}

	/* check if the vaultkey is there, otherwise abort early (value
	 * lateron reused when server is started) */
	snprintf(vaultkey, sizeof(vaultkey), "%s/.vaultkey", (*stats)->path);
	if (stat(vaultkey, &statbuf) == -1) {
		msab_freeStatus(stats);
		freeConfFile(ckv);
		free(ckv);
		pthread_mutex_unlock(&fork_lock);
		return(newErr("cannot start database '%s': no .vaultkey found "
					"(did you create the database with `monetdb create %s`?)",
					database, database));
	}

	pid = fork();
	if (pid == 0) {
		char *sabdbfarm;
		char dbpath[1024];
		char port[24];
		char muri[512]; /* possibly undersized */
		char usock[512];
		char mydoproxy;
		char nthreads[24];
		char nclients[24];
		char pipeline[512];
		char *readonly = NULL;
		char *embeddedr = NULL;
		char *embeddedpy = NULL;
		char *argv[512];	/* for the exec arguments */
		char property_other[1024];
		int c = 0;
		unsigned int mport;

		er = msab_getDBfarm(&sabdbfarm);
		if (er != NULL) {
			Mfprintf(stderr, "unexpected error: %s\n", er);
			exit(1);
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

		kv = findConfKey(ckv, "readonly");
		if (kv->val != NULL && strcmp(kv->val, "no") != 0)
			readonly = "--readonly";

		kv = findConfKey(ckv, "embedr");
		if (kv->val != NULL && strcmp(kv->val, "no") != 0)
			embeddedr = "embedded_r=true";

		kv = findConfKey(ckv, "embedpy");
		if (kv->val != NULL && strcmp(kv->val, "no") != 0)
			embeddedpy = "embedded_py=true";


		/* redirect stdout and stderr to a new pair of fds for
		 * logging help */
		close(pfdo[0]);
		dup2(pfdo[1], 1);
		close(pfdo[1]);

		close(pfde[0]);
		dup2(pfde[1], 2);
		close(pfde[1]);

		mport = (unsigned int)getConfNum(_mero_props, "port");

		/* ok, now exec that mserver we want */
		snprintf(dbpath, sizeof(dbpath),
				"--dbpath=%s/%s", sabdbfarm, database);
		snprintf(vaultkey, sizeof(vaultkey),
				"monet_vault_key=%s/.vaultkey", (*stats)->path);
		snprintf(muri, sizeof(muri),
				"merovingian_uri=mapi:monetdb://%s:%u/%s",
				_mero_hostname, mport, database);
		argv[c++] = _mero_mserver;
		argv[c++] = dbpath;
		argv[c++] = "--set"; argv[c++] = muri;
		if (mydoproxy == 1) {
			struct sockaddr_un s; /* only for sizeof(s.sun_path) :( */
			argv[c++] = "--set"; argv[c++] = "mapi_open=false";
			/* we "proxy", so we can just solely use UNIX domain sockets
			 * internally.  Before we hit our head, check if we can
			 * actually use a UNIX socket (due to pathlength) */
			if (strlen((*stats)->path) + 11 < sizeof(s.sun_path)) {
				snprintf(port, sizeof(port), "mapi_port=0");
				snprintf(usock, sizeof(usock), "mapi_usock=%s/.mapi.sock",
						(*stats)->path);
			} else {
				argv[c++] = "--set"; argv[c++] = "mapi_autosense=true";
				/* for logic here, see comment below */
				snprintf(port, sizeof(port), "mapi_port=%u", mport + 1);
				snprintf(usock, sizeof(usock), "mapi_usock=");
			}
		} else {
			argv[c++] = "--set"; argv[c++] = "mapi_open=true";
			argv[c++] = "--set"; argv[c++] = "mapi_autosense=true";
			/* avoid this mserver binding to the same port as merovingian
			 * but on another interface, (INADDR_ANY ... sigh) causing
			 * endless redirects since 0.0.0.0 is not a valid address to
			 * connect to, and hence the hostname is advertised instead */
			snprintf(port, sizeof(port), "mapi_port=%u", mport + 1);
			snprintf(usock, sizeof(usock), "mapi_usock=");
		}
		argv[c++] = "--set"; argv[c++] = port;
		argv[c++] = "--set"; argv[c++] = usock;
		argv[c++] = "--set"; argv[c++] = vaultkey;
		if (nthreads[0] != '\0') {
			argv[c++] = "--set"; argv[c++] = nthreads;
		}
		if (nclients[0] != '\0') {
			argv[c++] = "--set"; argv[c++] = nclients;
		}
		if (pipeline[0] != '\0') {
			argv[c++] = "--set"; argv[c++] = pipeline;
		}
		if (embeddedr != NULL) {
			argv[c++] = "--set"; argv[c++] = embeddedr;
		}
		if (embeddedpy != NULL) {
			argv[c++] = "--set"; argv[c++] = embeddedpy;
		}
		if (readonly != NULL) {
			argv[c++] = readonly;
		}
		/* get the rest (non-default) mserver props set in the conf file */
		list = ckv;
		while (list->key != NULL) {
			if (list->val != NULL && !defaultProperty(list->key)) {
				argv[c++] = "--set";
				snprintf(property_other, sizeof(property_other), "%s=%s", list->key, list->val);
				argv[c++] = property_other;
			}
			list++;
		}

		/* keep this one last for easy copy/paste with gdb */
		argv[c++] = "--set"; argv[c++] = "monet_daemon=yes";

		argv[c++] = NULL;

		freeConfFile(ckv);
		free(ckv); /* can make ckv static and reuse it all the time */

		fprintf(stdout, "arguments:");
		for (c = 0; argv[c] != NULL; c++) {
			/* very stupid heuristic to make copy/paste easier from
			 * merovingian's log */
			if (strchr(argv[c], ' ') != NULL) {
				fprintf(stdout, " \"%s\"", argv[c]);
			} else {
				fprintf(stdout, " %s", argv[c]);
			}
		}
		Mfprintf(stdout, "\n");

		execv(_mero_mserver, argv);
		/* if the exec returns, it is because of a failure */
		Mfprintf(stderr, "executing failed: %s\n", strerror(errno));
		exit(1);
	} else {
		/* don't need this, child did */
		freeConfFile(ckv);
		free(ckv);

		if (pid > 0) {
			/* make sure no entries are shot while adding and that we
			 * deliver a consistent state */
			pthread_mutex_lock(&_mero_topdp_lock);

			/* parent: fine, let's add the pipes for this child */
			dp = _mero_topdp;
			while (dp->next != NULL)
				dp = dp->next;
			dp = dp->next = malloc(sizeof(struct _dpair));
			dp->out = pfdo[0];
			close(pfdo[1]);
			dp->err = pfde[0];
			close(pfde[1]);
			dp->next = NULL;
			dp->type = MERODB;
			dp->pid = pid;
			dp->dbname = strdup(database);
			dp->flag = 0;

			pthread_mutex_unlock(&_mero_topdp_lock);

			/* wait for the child to finish starting, at some point we
			 * decided that we should wait indefinitely here because if the
			 * mserver needs time to start up, we shouldn't interrupt it,
			 * and if it hangs, we're just doomed, with the drawback that we
			 * completely kill the functionality of monetdbd too */
			do {
				/* give the database a break */
				sleep_ms(500);

				/* in the meanwhile, if the server has stopped, it will
				 * have been removed from the dpair list, so check if
				 * it's still there. */
				pthread_mutex_lock(&_mero_topdp_lock);
				dp = _mero_topdp;
				while (dp != NULL && dp->pid != pid)
					dp = dp->next;
				pthread_mutex_unlock(&_mero_topdp_lock);

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
					pthread_mutex_unlock(&fork_lock);
					return(e);
				}

				/* server doesn't run, no need to wait any longer */
				if (dp == NULL)
					break;
			} while ((*stats)->state != SABdbRunning);

			/* check if the SQL scenario was loaded */
			if (dp != NULL && (*stats)->state == SABdbRunning &&
				(*stats)->conns != NULL &&
				(*stats)->conns->val != NULL &&
				(*stats)->scens != NULL &&
				(*stats)->scens->val != NULL)
			{
				sablist *scen = (*stats)->scens;
				do {
					if (scen->val != NULL && strcmp(scen->val, "sql") == 0)
						break;
				} while ((scen = scen->next) != NULL);
				if (scen == NULL) {
					/* we don't know what it's doing, but we don't like it
					 * any case, so kill it */
					terminateProcess(pid, strdup(database), MERODB, 0);
					msab_freeStatus(stats);
					pthread_mutex_unlock(&fork_lock);
					return(newErr("database '%s' did not initialise the sql "
								  "scenario", database));
				}
			} else if (dp != NULL) {
				terminateProcess(pid, strdup(database), MERODB, 0);
				msab_freeStatus(stats);
				pthread_mutex_unlock(&fork_lock);
				return(newErr(
						   "database '%s' started up, but failed to open up "
						   "a communication channel", database));
			}

			pthread_mutex_unlock(&fork_lock);

			/* try to be clear on why starting the database failed */
			if (dp == NULL) {
				state = (*stats)->state;

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
							   "logfile for any peculiarities", database));
				case SABdbCrashed:
					return(newErr(
							   "database '%s' has crashed after starting, "
							   "manual intervention needed, "
							   "check monetdbd's logfile for details",
							   database));
				case SABdbInactive:
					return(newErr(
							   "database '%s' appears to shut "
							   "itself down after starting, "
							   "check monetdbd's logfile for possible "
							   "hints", database));
				case SABdbStarting:
					return(newErr(
							   "database '%s' has inconsistent state "
							   "(sabaoth administration reports starting up, "
							   "but process seems gone), "
							   "review monetdbd's "
							   "logfile for any peculiarities", database));
				default:
					return(newErr("unknown state: %d", (int)state));
				}
			}

			if ((*stats)->locked == 1) {
				Mfprintf(stdout, "database '%s' has been put into maintenance "
						 "mode during startup\n", database);
			}

			return(NO_ERR);
		}
	}
	/* forking failed somehow, cleanup the pipes */
	close(pfdo[0]);
	close(pfdo[1]);
	close(pfde[0]);
	close(pfde[1]);
	pthread_mutex_unlock(&fork_lock);
	return(newErr("%s", strerror(errno)));
}

/* vim:set ts=4 sw=4 noexpandtab: */
