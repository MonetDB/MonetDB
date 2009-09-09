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

static void
discoveryRunner(void *d)
{
	int sock = *(int *)d;
	int s = -1;
	struct sockaddr_storage peer_addr;
	socklen_t peer_addr_len;
	fd_set fds;
	struct timeval tv;
	int c;
	/* avoid first announce, the HELO will cause an announce when it's
	 * received by ourself */
	time_t deadline = 1;
	time_t now = 0;
	int forceannc = 0;
	sabdb *orig;
	sabdb *stats;
	confkeyval *ckv;
	confkeyval *kv;
	err e;
	remotedb rdb;
	remotedb prv;
	char *val;

	ssize_t nread;
	char buf[512]; /* our packages should be pretty small */
	char host[128];
	char service[8];

	/* seed random number generation for random delay in HELO response */
	srand(time(NULL));

	/* start shouting around that we're here ;) request others to tell
	 * what databases they have */
	snprintf(buf, 512, "HELO %s", _mero_hostname);
	broadcast(buf);

	ckv = getDefaultProps();

	/* main loop */
	while (_mero_keep_listening == 1) {
		now = time(NULL);
		/* do a round of announcements, we're ahead of the ttl because
		 * when we announce, we add 60 seconds to avoid a "gap" */
		if (forceannc == 1 || deadline <= now) {
			/* set new deadline */
			deadline = now + _mero_discoveryttl;
			forceannc = 0;

			/* list all known databases */
			if ((e = SABAOTHgetStatus(&stats, NULL)) != MAL_SUCCEED) {
				Mfprintf(_mero_discerr, "SABAOTHgetStatus error: %s, "
						"discovery services disabled\n", e);
				GDKfree(e);
				return;
			}

			for (orig = stats; stats != NULL; stats = stats->next) {
				readProps(ckv, stats->path);
				kv = findConfKey(ckv, "shared");
				val = kv->val == NULL ? "" : kv->val;
				/* skip databases under maintenance */
				if (strcmp(val, "no") != 0 && stats->locked != 1) {
					/* craft ANNC message for this db */
					if (strcmp(val, "yes") == 0)
						val = "";
					snprintf(buf, 512, "ANNC %s%s%s mapi:monetdb://%s:%hu/ %d",
							stats->dbname, val[0] == '\0' ? "" : "/", val,
							_mero_hostname, _mero_port,
							_mero_discoveryttl + 60);
					broadcast(buf);
				}
				freeConfFile(ckv);
			}

			if (orig != NULL)
				SABAOTHfreeStatus(&orig);
		}

		/* do a round to see if we have to cleanup anything (expired
		 * ttl) */
		pthread_mutex_lock(&_mero_remotedb_lock);

		prv = NULL;
		rdb = _mero_remotedbs;
		while (rdb != NULL) {
			if (rdb->ttl <= now) {
				/* expired, let's remove */
				if (prv == NULL) {
					_mero_remotedbs = rdb->next;
				} else {
					prv->next = rdb->next;
				}
				Mfprintf(_mero_discout, "neighbour database %s%s "
						"has expired\n", rdb->conn, rdb->fullname);
				free(rdb->dbname);
				free(rdb->conn);
				free(rdb->tag);
				free(rdb->fullname);
				free(rdb);
				break;
			}
			prv = rdb;
			rdb = rdb->next;
		}

		pthread_mutex_unlock(&_mero_remotedb_lock);

		peer_addr_len = sizeof(struct sockaddr_storage);
		FD_ZERO(&fds);
		FD_SET(sock, &fds);
		/* Wait up to 5 seconds. */
		tv.tv_sec = 5;
		tv.tv_usec = 0;
		nread = select(sock + 1, &fds, NULL, NULL, &tv);
		if (nread == 0) {
			/* nothing interesting has happened */
			buf[0] = '\0';
			continue;
		}
		nread = recvfrom(sock, buf, 512, 0,
				(struct sockaddr *)&peer_addr, &peer_addr_len);
		if (nread == -1) {
			buf[0] = '\0';
			continue; /* ignore failed request */
		}

		s = getnameinfo((struct sockaddr *)&peer_addr,
				peer_addr_len, host, 128,
				service, 8, NI_NUMERICSERV);
		if (s != 0) {
			Mfprintf(_mero_discerr, "cannot retrieve name info: %s\n",
					gai_strerror(s));
			continue; /* skip this message */
		}

		/* ignore messages from broadcast interface */
		if (strcmp(host, "0.0.0.0") == 0)
			continue;

		if (strncmp(buf, "HELO ", 5) == 0) {
			/* HELLO message, respond with current databases */
			Mfprintf(_mero_discout, "new neighbour %s\n", host);
			/* sleep a random amount of time to avoid an avalanche of
			 * ANNC messages flooding the network */
			c = 1 + (int)(2500.0 * (rand() / (RAND_MAX + 1.0)));
			MT_sleep_ms(c);
			/* force an announcement round by dropping the deadline */
			forceannc = 1;
			continue;
		} else if (strncmp(buf, "LEAV ", 5) == 0) {
			/* LEAVE message, unregister database */
			char *sp = NULL;
			char *dbname;
			char *conn;
			char hadmatch = 0;

			strtok_r(buf, " ", &sp); /* discard the msg type */
			dbname = strtok_r(NULL, " ", &sp);
			conn = strtok_r(NULL, " ", &sp);

			if (dbname == NULL || conn == NULL)
				continue;

			/* look for the database, and verify that its "conn"
			 * (merovingian) is the same */

			/* technically, we could use Diffie-Hellman (without Debian
			 * modifications) to negotiate a shared secret key, such
			 * that only the original registrant can unregister a
			 * database, however... do we really care that much? */

			pthread_mutex_lock(&_mero_remotedb_lock);

			prv = NULL;
			rdb = _mero_remotedbs;
			while (rdb != NULL) {
				if (strcmp(dbname, rdb->dbname) == 0 &&
						strcmp(conn, rdb->conn) == 0)
				{
					/* found, let's remove */
					if (prv == NULL) {
						_mero_remotedbs = rdb->next;
					} else {
						prv->next = rdb->next;
					}
					Mfprintf(_mero_discout,
							"removed neighbour database %s%s\n",
							conn, rdb->fullname);
					free(rdb->dbname);
					free(rdb->conn);
					free(rdb->tag);
					free(rdb->fullname);
					free(rdb);
					hadmatch = 1;
					/* there may be more, keep looking */
				}
				prv = rdb;
				rdb = rdb->next;
			}
			if (hadmatch == 0)
				Mfprintf(_mero_discout,
						"received leave request for unknown database "
						"%s%s from %s\n", conn, dbname, host);

			pthread_mutex_unlock(&_mero_remotedb_lock);
		} else if (strncmp(buf, "ANNC ", 5) == 0) {
			/* ANNOUNCE message, register database */
			char *sp = NULL;
			char *dbname;
			char *tag = NULL;
			char *conn;
			char *ttl;

			strtok_r(buf, " ", &sp); /* discard the msg type */
			dbname = strtok_r(NULL, " ", &sp);
			conn = strtok_r(NULL, " ", &sp);
			ttl = strtok_r(NULL, " ", &sp);

			if (dbname == NULL || conn == NULL || ttl == NULL)
				continue;

			pthread_mutex_lock(&_mero_remotedb_lock);

			if (_mero_remotedbs == NULL) {
				rdb = _mero_remotedbs = malloc(sizeof(struct _remotedb));
			} else {
				prv = NULL;
				rdb = _mero_remotedbs;
				while (rdb != NULL) {
					if (strcmp(dbname, rdb->fullname) == 0 &&
							strcmp(conn, rdb->conn) == 0)
					{
						/* refresh ttl */
						rdb->ttl = time(NULL) + atoi(ttl);
						rdb = prv;
						break;
					}
					prv = rdb;
					rdb = rdb->next;
				}
				if (rdb == prv) {
					pthread_mutex_unlock(&_mero_remotedb_lock);
					continue;
				}
				rdb = prv->next = malloc(sizeof(struct _remotedb));
			}
			rdb->fullname = strdup(dbname);
			if ((tag = strchr(dbname, '/')) != NULL)
				*tag++ = '\0';
			rdb->dbname = strdup(dbname);
			rdb->tag = tag != NULL ? strdup(tag) : NULL;
			rdb->conn = strdup(conn);
			rdb->ttl = time(NULL) + atoi(ttl);
			rdb->next = NULL;

			pthread_mutex_unlock(&_mero_remotedb_lock);

			Mfprintf(_mero_discout, "new database "
					"%s%s (ttl=%ss)\n",
					conn, rdb->fullname, ttl);
		} else {
			Mfprintf(_mero_discout, "ignoring unknown message from "
					"%s:%s: '%s'\n", host, service, buf);
		}
	}

	/* now notify of our soon to be absence ;) */

	/* list all known databases */
	if ((e = SABAOTHgetStatus(&stats, NULL)) != MAL_SUCCEED) {
		Mfprintf(_mero_discerr, "SABAOTHgetStatus error: %s, "
				"discovery services disabled\n", e);
		GDKfree(e);
		return;
	}

	/* craft LEAV messages for each db */
	orig = stats;
	while (stats != NULL) {
		readProps(ckv, stats->path);
		kv = findConfKey(ckv, "shared");
		if (kv->val != NULL && strcmp(kv->val, "no") != 0) {
			snprintf(buf, 512, "LEAV %s mapi:monetdb://%s:%hu/",
					stats->dbname, _mero_hostname, _mero_port);
			broadcast(buf);
		}
		freeConfFile(ckv);
		stats = stats->next;
	}

	if (orig != NULL)
		SABAOTHfreeStatus(&orig);

	GDKfree(ckv);
}

