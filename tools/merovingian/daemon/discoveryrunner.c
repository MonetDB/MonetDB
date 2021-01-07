/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <string.h> /* str* */
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#include <fcntl.h>
#include <time.h>

#include "msabaoth.h"
#include "utils/glob.h"
#include "utils/utils.h"
#include "utils/properties.h"

#include "merovingian.h"
#include "multiplex-funnel.h"
#include "discoveryrunner.h"


/* list of remote databases as discovered */
remotedb _mero_remotedbs = NULL;
/* lock to _mero_remotedbs */
pthread_mutex_t _mero_remotedb_lock = PTHREAD_MUTEX_INITIALIZER;

void
broadcast(char *msg)
{
	int len = strlen(msg) + 1;
	if (_mero_broadcastsock < 0)
		return;
	if (sendto(_mero_broadcastsock, msg, len, 0,
				(struct sockaddr *)&_mero_broadcastaddr,
				sizeof(_mero_broadcastaddr)) != len)
		Mfprintf(_mero_discerr, "error while sending broadcast "
				"message: %s\n", strerror(errno));
}

static bool
removeRemoteDB(const char *dbname, const char *conn)
{
	remotedb rdb;
	remotedb prv;
	bool hadmatch = false;

	pthread_mutex_lock(&_mero_remotedb_lock);

	prv = NULL;
	rdb = _mero_remotedbs;
	while (rdb != NULL) {
		/* look for the database, and verify that its "conn"
		 * (merovingian) is the same */
		if (strcmp(dbname, rdb->dbname) == 0 &&
				strcmp(conn, rdb->conn) == 0)
		{
			/* found, let's remove */
			if (prv == NULL) {
				_mero_remotedbs = rdb->next;
			} else {
				prv->next = rdb->next;
			}

			/* inform multiplex-funnels about this removal */
			multiplexNotifyRemovedDB(rdb->fullname);

			Mfprintf(_mero_discout,
					"removed neighbour database %s%s\n",
					conn, rdb->fullname);
			free(rdb->dbname);
			free(rdb->conn);
			free(rdb->fullname);
			free(rdb);
			rdb = prv;
			hadmatch = true;
			/* in the future, there may be more, so keep looking */
		}
		prv = rdb;
		if (rdb == NULL) {
			rdb = _mero_remotedbs;
		} else {
			rdb = rdb->next;
		}
	}

	pthread_mutex_unlock(&_mero_remotedb_lock);

	return(hadmatch);
}

static int
addRemoteDB(const char *dbname, const char *conn, const int ttl) {
	remotedb rdb;
	remotedb prv;
	char *tag;

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
				rdb->ttl = time(NULL) + ttl;
				rdb = prv;
				break;
			}
			prv = rdb;
			rdb = rdb->next;
		}
		if (rdb == prv) {
			pthread_mutex_unlock(&_mero_remotedb_lock);
			return(0);
		}
		rdb = prv->next = malloc(sizeof(struct _remotedb));
	}
	rdb->fullname = strdup(dbname);
	rdb->dbname = strdup(dbname);
	if ((tag = strchr(rdb->dbname, '/')) != NULL)
		*tag++ = '\0';
	rdb->tag = tag;
	rdb->conn = strdup(conn);
	rdb->ttl = time(NULL) + ttl;
	rdb->next = NULL;

	pthread_mutex_unlock(&_mero_remotedb_lock);

	/* inform multiplex-funnels about this addition */
	multiplexNotifyAddedDB(rdb->fullname);

	return(1);
}

sabdb *
getRemoteDB(const char *database)
{
	struct _remotedb dummy = { NULL, NULL, NULL, NULL, 0, NULL };
	remotedb rdb = NULL;
	remotedb pdb = NULL;
	remotedb down = NULL;
	sabdb *walk = NULL;
	sabdb *stats = NULL;
	size_t dbsize = strlen(database);
	char *mdatabase = malloc(sizeof(char) * (dbsize + 2 + 1));
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
		if (db_glob(mdatabase, mfullname) == 1) {
			/* create a fake sabdb struct, chain where necessary */
			if (walk != NULL) {
				walk = walk->next = malloc(sizeof(sabdb));
			} else {
				walk = stats = malloc(sizeof(sabdb));
			}
			walk->dbname = strdup(rdb->dbname);
			walk->path = walk->dbname; /* only freed by sabaoth */
			walk->locked = false;
			walk->state = SABdbRunning;
			walk->pid = 0;
			walk->scens = malloc(sizeof(sablist));
			walk->scens->val = strdup("sql");
			walk->scens->next = NULL;
			walk->conns = malloc(sizeof(sablist));
			walk->conns->val = strdup(rdb->conn);
			walk->conns->next = NULL;
			walk->uri = NULL;
			walk->secret = NULL;
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

	free(mdatabase);

	return(stats);
}

typedef struct _disc_message_tap {
	int fd;
	struct _disc_message_tap *next;
} *disc_message_tap;

/* list of hooks for incoming messages */
static disc_message_tap _mero_disc_msg_taps = NULL;

void
registerMessageTap(int fd)
{
	disc_message_tap h;
	/* make sure we never block in the main loop below because we can't
	 * write to the pipe */
	(void) fcntl(fd, F_SETFD, O_NONBLOCK);
	pthread_mutex_lock(&_mero_remotedb_lock);
	h = _mero_disc_msg_taps;
	if (h == NULL) {
		h = malloc(sizeof(struct _disc_message_tap));
		_mero_disc_msg_taps = h;
	} else {
		for (; h->next != NULL; h = h->next)
			;
		h = h->next = malloc(sizeof(struct _disc_message_tap));
	}
	h->next = NULL;
	h->fd = fd;
	pthread_mutex_unlock(&_mero_remotedb_lock);
}

void
unregisterMessageTap(int fd)
{
	disc_message_tap h, lasth;
	pthread_mutex_lock(&_mero_remotedb_lock);
	h = _mero_disc_msg_taps;
	for (lasth = NULL; h != NULL; lasth = h, h = h->next) {
		if (h->fd == fd) {
			if (lasth == NULL) {
				_mero_disc_msg_taps = h->next;
			} else {
				lasth->next = h->next;
			}
			free(h);
			break;
		}
	}
	pthread_mutex_unlock(&_mero_remotedb_lock);
}

void *
discoveryRunner(void *d)
{
	int socks[2];
	int sock;
	int s = -1;
	struct sockaddr_storage peer_addr;
	socklen_t peer_addr_len;
#ifdef HAVE_POLL
	struct pollfd pfd[2];
#else
	fd_set fds;
	struct timeval tv;
#endif
	/* avoid first announce, the HELO will cause an announce when it's
	 * received by ourself */
	time_t deadline = 1;
	time_t now = 0;
	int forceannc = 0;
	sabdb *orig;
	sabdb *stats;
	confkeyval *ckv;
	confkeyval *kv;
	confkeyval *discttl;
	err e;
	remotedb rdb;
	remotedb prv;
	char *val;

	ssize_t nread;
	char buf[512]; /* our packages should be pretty small */
	char host[128];
	char service[8];

	socks[0] = ((int *) d)[0];
	socks[1] = ((int *) d)[1];

	/* start shouting around that we're here ;) request others to tell
	 * what databases they have */
	snprintf(buf, 512, "HELO %s", _mero_hostname);
	broadcast(buf);

	ckv = getDefaultProps();
	discttl = findConfKey(_mero_props, "discoveryttl");

	/* main loop */
	while (_mero_keep_listening == 1) {
		now = time(NULL);
		/* do a round of announcements, we're ahead of the ttl because
		 * when we announce, we add 60 seconds to avoid a "gap" */
		if (forceannc == 1 || deadline <= now) {
			forceannc = 0;
			/* set new deadline */
			deadline = now + discttl->ival;

			/* list all known databases */
			if ((e = msab_getStatus(&stats, NULL)) != NULL) {
				Mfprintf(_mero_discerr, "msab_getStatus error: %s, "
						"discovery services disabled\n", e);
				free(e);
				free(ckv);
				if (socks[0] >= 0)
					closesocket(socks[0]);
				if (socks[1] >= 0)
					closesocket(socks[1]);
				return NULL;
			}

			for (orig = stats; stats != NULL; stats = stats->next) {
				readProps(ckv, stats->path);
				kv = findConfKey(ckv, "shared");
				val = kv->val == NULL ? "" : kv->val;
				/* skip databases under maintenance */
				if (strcmp(val, "no") != 0 && !stats->locked) {
					/* craft ANNC message for this db */
					if (strcmp(val, "yes") == 0)
						val = "";
					snprintf(buf, 512, "ANNC %s%s%s mapi:monetdb://%s:%u/ %d",
							stats->dbname, val[0] == '\0' ? "" : "/", val,
							_mero_hostname, (unsigned int)getConfNum(_mero_props, "port"),
							discttl->ival + 60);
					broadcast(buf);
				}
				freeConfFile(ckv);
			}

			if (orig != NULL)
				msab_freeStatus(&orig);

			if (getConfNum(_mero_props, "control") != 0) {
				/* announce control port */
				snprintf(buf, 512, "ANNC * %s:%u %d",
						_mero_hostname, (unsigned int)getConfNum(_mero_props, "port"),
						discttl->ival + 60);
				/* coverity[string_null] */
				broadcast(buf);
			}
		}

		/* do a round to see if we have to cleanup anything (expired
		 * ttl) */
		pthread_mutex_lock(&_mero_remotedb_lock);

		prv = NULL;
		rdb = _mero_remotedbs;
		while (rdb != NULL) {
			if (rdb->ttl > 0 && rdb->ttl <= now) {
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
				free(rdb->fullname);
				free(rdb);
				break;
			}
			prv = rdb;
			rdb = rdb->next;
		}

		pthread_mutex_unlock(&_mero_remotedb_lock);

		peer_addr_len = sizeof(struct sockaddr_storage);
		/* Wait up to 5 seconds. */
		sock = -1;
		for (s = 0; s < 5; s++) {
#ifdef HAVE_POLL
			int npoll = 0;
			if (socks[0] >= 0)
				pfd[npoll++] = (struct pollfd) {.fd = socks[0],
												.events = POLLIN};
			if (socks[1] >= 0)
				pfd[npoll++] = (struct pollfd) {.fd = socks[1],
												.events = POLLIN};
			nread = poll(pfd, npoll, 1000);
			if (nread > 0) {
				for (int i = 0; i < npoll; i++) {
					if (pfd[i].revents & POLLIN) {
						sock = pfd[i].fd;
						break;
					}
				}
				break;
			}
#else
			FD_ZERO(&fds);
			if (socks[0] >= 0)
				FD_SET(socks[0], &fds);
			if (socks[1] >= 0)
				FD_SET(socks[1], &fds);
			tv = (struct timeval) {.tv_sec = 1};
			nread = select((socks[0] > socks[1] ? socks[0] : socks[1]) + 1,
						   &fds, NULL, NULL, &tv);
			if (nread > 0) {
				sock = socks[0] >= 0 && FD_ISSET(socks[0], &fds) ? socks[0] : socks[1];
				break;
			}
#endif
			if (nread != 0)
				break;
			if (!_mero_keep_listening)
				goto breakout;
		}
		if (nread <= 0 || sock < 0) {  /* assume only failure is EINTR */
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
		/* forward messages not coming from ourself to all routes that
		 * are active */
		if (strcmp(host, _mero_hostname) != 0) {
			disc_message_tap h = _mero_disc_msg_taps;
			for (; h != NULL; h = h->next) {
				if (write(h->fd, buf, nread) == -1) {
					/* really nothing to be done here, since this is
					 * best effort stuff, keep the condition to keep
					 * fortification warnings off */
				}
			}
		}

		if (strncmp(buf, "HELO ", 5) == 0) {
			/* HELLO message, respond with current databases */
			Mfprintf(_mero_discout, "new neighbour %s (%s)\n", buf + 5, host);
			/* sleep a random amount of time to avoid an avalanche of
			 * ANNC messages flooding the network */
#ifndef __COVERITY__			/* hide rand() from Coverity */
			sleep_ms(1 + (int)(2500.0 * (rand() / (RAND_MAX + 1.0))));
#endif
			/* force an announcement round by dropping the deadline */
			forceannc = 1;
			continue;
		} else if (strncmp(buf, "LEAV ", 5) == 0) {
			/* LEAVE message, unregister database */
			char *sp = NULL;
			char *dbname;
			char *conn;

			strtok_r(buf, " ", &sp); /* discard the msg type */
			dbname = strtok_r(NULL, " ", &sp);
			conn = strtok_r(NULL, " ", &sp);

			if (dbname == NULL || conn == NULL)
				continue;

			if (!removeRemoteDB(dbname, conn))
				Mfprintf(_mero_discout,
						"received leave request for unknown database "
						"%s%s from %s\n", conn, dbname, host);
		} else if (strncmp(buf, "ANNC ", 5) == 0) {
			/* ANNOUNCE message, register database */
			char *sp = NULL;
			char *dbname;
			char *conn;
			char *ttl;

			strtok_r(buf, " ", &sp); /* discard the msg type */
			dbname = strtok_r(NULL, " ", &sp);
			conn = strtok_r(NULL, " ", &sp);
			ttl = strtok_r(NULL, " ", &sp);

			if (dbname == NULL || conn == NULL || ttl == NULL)
				continue;

			if (addRemoteDB(dbname, conn, atoi(ttl)) == 1) {
				if (strcmp(dbname, "*") == 0) {
					Mfprintf(_mero_discout, "registered neighbour %s\n",
							conn);
				} else {
					Mfprintf(_mero_discout, "new database "
							"%s%s (ttl=%ss)\n",
							conn, dbname, ttl);
				}
			}
		} else {
			Mfprintf(_mero_discout, "ignoring unknown message from "
					"%s:%s: '%s'\n", host, service, buf);
		}
	}
  breakout:

	if (socks[0] >= 0) {
		shutdown(socks[0], SHUT_WR);
		closesocket(socks[0]);
	}
	if (socks[1] >= 0) {
		shutdown(socks[1], SHUT_WR);
		closesocket(socks[1]);
	}

	/* now notify of imminent absence ;) */

	/* list all known databases */
	if ((e = msab_getStatus(&stats, NULL)) != NULL) {
		Mfprintf(_mero_discerr, "msab_getStatus error: %s, "
				"discovery services disabled\n", e);
		free(e);
		free(ckv);
		return NULL;
	}

	/* craft LEAV messages for each db */
	orig = stats;
	while (stats != NULL) {
		readProps(ckv, stats->path);
		kv = findConfKey(ckv, "shared");
		if (kv->val != NULL && strcmp(kv->val, "no") != 0) {
			snprintf(buf, 512, "LEAV %s mapi:monetdb://%s:%u/",
					stats->dbname, _mero_hostname,
					(unsigned int)getConfNum(_mero_props, "port"));
			broadcast(buf);
		}
		freeConfFile(ckv);
		stats = stats->next;
	}

	if (orig != NULL)
		msab_freeStatus(&orig);

	/* deregister this merovingian, so it doesn't remain a stale entry */
	if (getConfNum(_mero_props, "control") != 0) {
		snprintf(buf, 512, "LEAV * %s:%u",
				_mero_hostname, (unsigned int)getConfNum(_mero_props, "port"));
		broadcast(buf);
	}

	free(ckv);
	return NULL;
}

/* vim:set ts=4 sw=4 noexpandtab: */
