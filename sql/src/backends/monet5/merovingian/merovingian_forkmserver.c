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
 * Copyright August 2008-2010 MonetDB B.V.
 * All Rights Reserved.
 */

/**
 * Fork an Mserver and detach.  Before forking off, Sabaoth is consulted
 * to see if forking makes sense, or whether it is necessary at all, or
 * forbidden by restart policy, e.g. when in maintenance.
 */
static err
forkMserver(str database, sabdb** stats, int force)
{
	pid_t pid;
	str er;
	sabuplog info;
	struct tm *t;
	char tstr[20];
	int pfdo[2];
	int pfde[2];
	dpair dp;
	str vaultkey = NULL;
	struct stat statbuf;
	char upmin[8];
	char upavg[8];
	char upmax[8];

	er = SABAOTHgetStatus(stats, database);
	if (er != MAL_SUCCEED) {
		err e = newErr("%s", er);
		GDKfree(er);
		return(e);
	}

	/* NOTE: remotes also include locals through self announcement */
	if (*stats == NULL) {
		struct _remotedb dummy = { NULL, NULL, NULL, NULL, 0, NULL };
		remotedb rdb = NULL;
		remotedb pdb = NULL;
		remotedb down = NULL;
		sabdb *walk = *stats;
		size_t dbsize = strlen(database);
		char *mdatabase = GDKmalloc(sizeof(char) * (dbsize + 2 + 1));
		char mfullname[8096];  /* should be enough for everyone... */

		/* each request has an implicit /'* (without ') added to match
		 * all sub-levels to the request, such that a request for e.g. X
		 * will return X/level1/level2/... */
		memcpy(mdatabase, database, dbsize + 1);
		if (dbsize <= 2 ||
				mdatabase[dbsize - 2] != '/' ||
				mdatabase[dbsize - 1] != '*')
		{
			mdatabase[dbsize++] = '/';
			mdatabase[dbsize++] = '*';
			mdatabase[dbsize++] = '\0';
		}

		/* check the remote databases, in private */
		pthread_mutex_lock(&_mero_remotedb_lock);

		dummy.next = _mero_remotedbs;
		rdb = dummy.next;
		pdb = &dummy;
		while (rdb != NULL) {
			snprintf(mfullname, sizeof(mfullname), "%s/", rdb->fullname);
			if (glob(mdatabase, mfullname) == 1) {
				/* create a fake sabdb struct, chain where necessary */
				if (walk != NULL) {
					walk = walk->next = GDKmalloc(sizeof(sabdb));
				} else {
					walk = *stats = GDKmalloc(sizeof(sabdb));
				}
				walk->dbname = GDKstrdup(rdb->dbname);
				walk->path = walk->dbname; /* only freed by sabaoth */
				walk->locked = 0;
				walk->state = SABdbRunning;
				walk->scens = GDKmalloc(sizeof(sablist));
				walk->scens->val = GDKstrdup("sql");
				walk->scens->next = NULL;
				walk->conns = GDKmalloc(sizeof(sablist));
				walk->conns->val = GDKstrdup(rdb->conn);
				walk->conns->next = NULL;
				walk->next = NULL;
				walk->uplog = NULL;

				/* cut out first returned entry, put it down the list
				 * later, as to implement a round-robin DNS-like
				 * algorithm */
				if (down == NULL) {
					down = rdb;
					if (pdb->next == _mero_remotedbs) {
						_mero_remotedbs = pdb->next = rdb->next;
					} else {
						pdb->next = rdb->next;
					}
					rdb->next = NULL;
					rdb = pdb;
				}
			}
			pdb = rdb;
			rdb = rdb->next;
		}

		if (down != NULL)
			pdb->next = down;

		pthread_mutex_unlock(&_mero_remotedb_lock);

		GDKfree(mdatabase);

		if (*stats != NULL)
			return(NO_ERR);

		return(newErr("no such database: %s", database));
	}

	/* Since we ask for a specific database, it should be either there
	 * or not there.  Since we checked the latter case above, it should
	 * just be there, and be the right one.  There also shouldn't be
	 * more than one entries in the list, so we assume we have the right
	 * one here. */

	/* retrieve uplog information to print a short conclusion */
	er = SABAOTHgetUplogInfo(&info, *stats);
	if (er != MAL_SUCCEED) {
		err e = newErr("could not retrieve uplog information: %s", er);
		GDKfree(er);
		SABAOTHfreeStatus(stats);
		return(e);
	}

	if ((*stats)->locked == 1) {
		Mfprintf(stdout, "database '%s' is under maintenance\n", database);
		if (force == 0)
			return(NO_ERR);
	}

	switch ((*stats)->state) {
		case SABdbRunning:
			return(NO_ERR);
		break;
		case SABdbCrashed:
			t = localtime(&info.lastcrash);
			strftime(tstr, sizeof(tstr), "%Y-%m-%d %H:%M:%S", t);
			secondsToString(upmin, info.minuptime, 1);
			secondsToString(upavg, info.avguptime, 1);
			secondsToString(upmax, info.maxuptime, 1);
			Mfprintf(stdout, "database '%s' has crashed after start on %s, "
					"attempting restart, "
					"up min/avg/max: %s/%s/%s, "
					"crash average: %d.00 %.2f %.2f (%d-%d=%d)\n",
					database, tstr,
					upmin, upavg, upmax,
					info.crashavg1, info.crashavg10, info.crashavg30,
					info.startcntr, info.stopcntr, info.crashcntr);
		break;
		case SABdbInactive:
			secondsToString(upmin, info.minuptime, 1);
			secondsToString(upavg, info.avguptime, 1);
			secondsToString(upmax, info.maxuptime, 1);
			Mfprintf(stdout, "starting database '%s', "
					"up min/avg/max: %s/%s/%s, "
					"crash average: %d.00 %.2f %.2f (%d-%d=%d)\n",
					database,
					upmin, upavg, upmax,
					info.crashavg1, info.crashavg10, info.crashavg30,
					info.startcntr, info.stopcntr, info.crashcntr);
		break;
		default:
			SABAOTHfreeStatus(stats);
			return(newErr("unknown state: %d", (int)(*stats)->state));
	}

	if ((*stats)->locked == 1 && force == 1)
		Mfprintf(stdout, "startup of database under maintenance "
				"'%s' forced\n", database);

	/* check if the vaultkey is there, otherwise abort early (value
	 * lateron reused when server is started) */
	vaultkey = alloca(sizeof(char) * 512);
	snprintf(vaultkey, 511, "%s/.vaultkey", (*stats)->path);
	if (stat(vaultkey, &statbuf) == -1) {
		SABAOTHfreeStatus(stats);
		return(newErr("cannot start database '%s': no .vaultkey found "
					"(did you create the database with `monetdb create %s`?)",
					database, database));
	}

	/* create the pipes (filedescriptors) now, such that we and the
	 * child have the same descriptor set */
	if (pipe(pfdo) == -1) {
		SABAOTHfreeStatus(stats);
		return(newErr("unable to create pipe: %s", strerror(errno)));
	}
	if (pipe(pfde) == -1) {
		close(pfdo[0]);
		close(pfdo[1]);
		SABAOTHfreeStatus(stats);
		return(newErr("unable to create pipe: %s", strerror(errno)));
	}

	pid = fork();
	if (pid == 0) {
		str conffile = alloca(sizeof(char) * 512);
		str dbname = alloca(sizeof(char) * 512);
		str port = alloca(sizeof(char) * 24);
		str muri = alloca(sizeof(char) * 512); /* possibly undersized */
		char mydoproxy;
		str nthreads = NULL;
		str master = NULL;
		str slave = NULL;
		str pipeline = NULL;
		str argv[25];	/* for the exec arguments */
		confkeyval *ckv, *kv;
		int c = 0;

		ckv = getDefaultProps();
		readProps(ckv, (*stats)->path);

		kv = findConfKey(ckv, "forward");
		if (kv->val == NULL)
			kv = findConfKey(_mero_props, "forward");
		mydoproxy = strcmp(kv->val, "proxy") == 0;

		kv = findConfKey(ckv, "nthreads");
		if (kv->val == NULL)
			kv = findConfKey(_mero_props, "nthreads");
		if (kv->val != NULL) {
			nthreads = alloca(sizeof(char) * 24);
			snprintf(nthreads, 24, "gdk_nr_threads=%s", kv->val);
		}

		kv = findConfKey(ckv, "optpipe");
		if (kv->val == NULL)
			kv = findConfKey(_mero_props, "optpipe");
		if (kv->val != NULL) {
			pipeline = alloca(sizeof(char) * 512);
			snprintf(pipeline, 512, "sql_optimizer=%s", kv->val);
		}

		kv = findConfKey(ckv, "master");
		/* can't have master configured by default */
		if (kv->val != NULL && strcmp(kv->val, "no") != 0) {
			size_t len = 24 + strlen(kv->val);
			master = alloca(sizeof(char) * len);
			snprintf(master, len, "replication_master=%s", kv->val);
		}

		kv = findConfKey(ckv, "slave");
		/* can't have slave configured by default */
		if (kv->val != NULL) {
			size_t len = 24 + strlen(kv->val);
			slave = alloca(sizeof(char) * len);
			snprintf(slave, len, "replication_slave=%s", kv->val);
		}

		freeConfFile(ckv);
		GDKfree(ckv); /* can make ckv static and reuse it all the time */

		/* redirect stdout and stderr to a new pair of fds for
		 * logging help */
		close(pfdo[0]);
		dup2(pfdo[1], 1);
		close(pfdo[1]);

		close(pfde[0]);
		dup2(pfde[1], 2);
		close(pfde[1]);

		/* ok, now exec that mserver we want */
		snprintf(conffile, 512, "--config=%s", _mero_conffile);
		snprintf(dbname, 512, "--dbname=%s", database);
		snprintf(vaultkey, 512, "monet_vault_key=%s/.vaultkey", (*stats)->path);
		snprintf(muri, 512, "merovingian_uri=mapi:monetdb://%s:%d/%s",
				_mero_hostname, _mero_port, database);
		/* avoid this mserver binding to the same port as merovingian
		 * but on another interface, (INADDR_ANY ... sigh) causing
		 * endless redirects since 0.0.0.0 is not a valid address to
		 * connect to, and hence the hostname is advertised instead */
		snprintf(port, 24, "mapi_port=%d", _mero_port + 1);
		argv[c++] = _mero_mserver;
		argv[c++] = conffile;
		argv[c++] = dbname;
		argv[c++] = "--dbinit=include sql;"; /* yep, no quotes needed! */
		argv[c++] = "--set"; argv[c++] = muri;
		argv[c++] = "--set"; argv[c++] = "monet_daemon=yes";
		if (mydoproxy == 1) {
			argv[c++] = "--set"; argv[c++] = "mapi_open=false";
		} else {
			argv[c++] = "--set"; argv[c++] = "mapi_open=true";
		}
		argv[c++] = "--set"; argv[c++] = "mapi_autosense=true";
		argv[c++] = "--set"; argv[c++] = port;
		argv[c++] = "--set"; argv[c++] = vaultkey;
		if (nthreads != NULL) {
			argv[c++] = "--set"; argv[c++] = nthreads;
		}
		if (pipeline != NULL) {
			argv[c++] = "--set"; argv[c++] = pipeline;
		}
		if (master != NULL) {
			argv[c++] = "--set"; argv[c++] = master;
		}
		if (slave != NULL) {
			argv[c++] = "--set"; argv[c++] = slave;
		}
		argv[c++] = NULL;

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
	} else if (pid > 0) {
		int i;

		/* make sure no entries are shot while adding and that we
		 * deliver a consistent state */
		pthread_mutex_lock(&_mero_topdp_lock);

		/* parent: fine, let's add the pipes for this child */
		dp = _mero_topdp;
		while (dp->next != NULL)
			dp = dp->next;
		dp = dp->next = GDKmalloc(sizeof(struct _dpair));
		dp->out = pfdo[0];
		close(pfdo[1]);
		dp->err = pfde[0];
		close(pfde[1]);
		dp->next = NULL;
		dp->pid = pid;
		dp->dbname = GDKstrdup(database);

		pthread_mutex_unlock(&_mero_topdp_lock);

		/* wait for the child to open up a communication channel */
		for (i = 0; i < 20; i++) {	/* wait up to 10 seconds */
			/* give the database a break */
			MT_sleep_ms(500);
			/* stats cannot be NULL, as we don't allow starting non
			 * existing databases, note that we need to run this loop at
			 * least once not to leak */
			SABAOTHfreeStatus(stats);
			er = SABAOTHgetStatus(stats, database);
			if (er != MAL_SUCCEED) {
				/* since the client mserver lives its own life anyway,
				 * it's not really a problem we exit here */
				err e = newErr("%s", er);
				GDKfree(er);
				return(e);
			}
			if ((*stats)->state == SABdbRunning &&
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
				if (scen != NULL)
					break;
			}
		}
		/* if we've never found a connection, try to figure out why */
		if (i >= 20) {
			int state = (*stats)->state;
			dpair pdp;

			/* starting failed */
			SABAOTHfreeStatus(stats);

			/* in the meanwhile the list may have changed so refetch the
			 * parent and self */
			pthread_mutex_lock(&_mero_topdp_lock);
			dp = NULL;
			pdp = _mero_topdp;
			while (pdp != NULL && pdp->next != NULL && pdp->next->pid != pid)
				pdp = pdp->next;

			/* pdp is NULL when the database terminated somehow while
			 * starting */
			if (pdp != NULL) {
				pthread_mutex_unlock(&_mero_topdp_lock);
				switch (state) {
					case SABdbRunning:
						/* right, it's not there, but it's running */
						return(newErr(
									"database '%s' has inconsistent state "
									"(running but dead), review merovingian's "
									"logfile for any peculiarities", database));
					case SABdbCrashed:
						return(newErr(
									"database '%s' has crashed after starting, "
									"manual intervention needed, "
									"check merovingian's logfile for details",
									database));
					case SABdbInactive:
						return(newErr(
									"database '%s' appears to shut "
									"itself down after starting, "
									"check merovingian's logfile for possible "
									"hints", database));
					default:
						return(newErr("unknown state: %d", (int)(*stats)->state));
				}
			}

			/* in this case something seems still to be running, which
			 * we don't want */
			dp = pdp->next;
			terminateProcess(dp);
			pthread_mutex_unlock(&_mero_topdp_lock);

			switch (state) {
				case SABdbRunning:
					return(newErr(
								"timeout when waiting for database '%s' to "
								"open up a communication channel or to "
								"initialise the sql scenario", database));
				case SABdbCrashed:
					return(newErr(
								"database '%s' has crashed after starting, "
								"manual intervention needed, "
								"check merovingian's logfile for details",
								database));
				case SABdbInactive:
					/* due to GDK only locking once it has loaded all
					 * its stuff, Sabaoth cannot "see" if a database is
					 * starting up, or just shut down, this means that
					 * in this case GDK may still be trying to start up,
					 * or that it indeed cleanly shut itself down after
					 * starting... kill it in any case. */
					return(newErr(
								"database '%s' either needs a longer timeout "
								"to start up, or appears to shut "
								"itself down after starting, "
								"review merovingian's logfile for any "
								"peculiarities", database));
				default:
					return(newErr("unknown state: %d", (int)(*stats)->state));
			}
		}
		if ((*stats)->locked == 1) {
			Mfprintf(stdout, "database '%s' has been put into maintenance "
					"mode during startup\n", database);
		}

		return(NO_ERR);
	}
	/* forking failed somehow, cleanup the pipes */
	close(pfdo[0]);
	close(pfdo[1]);
	close(pfde[0]);
	close(pfde[1]);
	return(newErr(strerror(errno)));
}
