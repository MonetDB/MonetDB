/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <unistd.h>
#include <string.h>
#include <sys/types.h>

#include "mapi.h"
#include "mutils.h" /* MT_lockf */
#include <fcntl.h>

#include "utils/glob.h"

#include "merovingian.h"
#include "discoveryrunner.h"
#include "multiplex-funnel.h"

#ifndef HAVE_PIPE2
#define pipe2(pipefd, flags)	pipe(pipefd)
#endif

#ifndef O_CLOEXEC
#define O_CLOEXEC	0
#endif

typedef struct _multiplexlist {
	multiplex *m;
	struct _multiplexlist *next;
} multiplexlist;

static multiplexlist *multiplexes = NULL;
static pthread_mutex_t mpl_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_t mfmanager = 0;
static int mfpipe[2];

static void *multiplexThread(void *d);


/**
 * Connections from all multiplex funnels are maintained by a single
 * thread that resolves and creates connections upon updates on the
 * discovery space.  Connections aren't made/checked/updated upon their
 * usage, because this introduces delays for the clients.  This is in
 * particular an issue when a target is updated to point to another
 * database.  To maintain a stable query performance, the connection
 * creation must happen in the background and set life once established.
 */
static void *
MFconnectionManager(void *d)
{
	int i;
	multiplex *m;
	multiplexlist *w;
	char buf[1024];
	size_t len;
	char *msg;
	struct timeval tv;
	fd_set fds;

	(void)d;

	while (_mero_keep_listening == 1) {
		FD_ZERO(&fds);
		FD_SET(mfpipe[0], &fds);

		/* wait up to 5 seconds */
		tv.tv_sec = 5;
		tv.tv_usec = 0;
		i = select(mfpipe[0] + 1, &fds, NULL, NULL, &tv);
		if (i == 0)
			continue;
		if (i == -1 && errno != EINTR) {
			Mfprintf(stderr, "failed to select on mfpipe: %s\n",
					strerror(errno));
			break;
		}
		/* coverity[string_null_argument] */
		if (read(mfpipe[0], &msg, sizeof(msg)) < 0) {
			Mfprintf(stderr, "failed reading from notification pipe: %s\n",
					strerror(errno));
			break;
		}
		/* we just received a POINTER to a string! */

		/* intended behaviour:
		 * - additions don't change any connection targets, they only
		 *   fill in gaps (conn == NULL)
		 * - removals of targets in use, cause a re-lookup of the
		 *   original pattern, on failure, conn is left NULL
		 */
		pthread_mutex_lock(&mpl_lock);
		if (msg[0] == '+') { /* addition */
			for (w = multiplexes; w != NULL; w = w->next) {
				m = w->m;
				for (i = 0; i < m->dbcc; i++) {
					if (m->dbcv[i]->conn == NULL) {
						len = snprintf(buf, sizeof(buf), "%s/*",
								m->dbcv[i]->database);
						if (len >= sizeof(buf)) {
							Mfprintf(stderr, "buffer buf too small, "
									"increase size in %s:%d\n",
									__FILE__, __LINE__);
							continue;
						}
						/* avoid double /'*'/'* (no ') */
						if (len >= 4 &&
								buf[len - 3] == '*' && buf[len - 4] == '/')
							buf[len - 2] = '\0';
						if (db_glob(buf, msg + 1) == 1) {
							sabdb *stats;
							Mapi tm = NULL;
							/* match! eat away trailing / (for matching) */
							msg[strlen(msg) - 1] = '\0';
							stats = getRemoteDB(msg + 1);
							if (stats == NULL) {
								Mfprintf(stderr, "target %s cannot be resolved "
										"despite being just discovered as %s\n",
										m->dbcv[i]->database, msg + 1);
								continue;
							}
							snprintf(buf, sizeof(buf), "%s%s",
									stats->conns->val, stats->dbname);
							msab_freeStatus(&stats);
							Mfprintf(stdout, "setting up multiplexer "
									"target %s->%s\n",
									m->dbcv[i]->database, buf);
							tm = mapi_mapiuri(buf,
									m->dbcv[i]->user, m->dbcv[i]->pass, "sql");
							if (mapi_reconnect(tm) == MOK) {
								m->dbcv[i]->conn = tm;
								mapi_cache_limit(tm, -1); /* don't page */
							} else {
								Mfprintf(stdout, "failed to connect to %s: %s\n",
										buf, mapi_error_str(tm));
								mapi_destroy(tm);
							}
						}
					}
				}
			}
		} else { /* removal */
			for (w = multiplexes; w != NULL; w = w->next) {
				m = w->m;
				for (i = 0; i < m->dbcc; i++) {
					if (m->dbcv[i]->conn != NULL) {
						len = snprintf(buf, sizeof(buf), "%s/*",
								m->dbcv[i]->database);
						if (len >= sizeof(buf)) {
							Mfprintf(stderr, "buffer buf too small, "
									"increase size in %s:%d\n",
									__FILE__, __LINE__);
							continue;
						}
						/* avoid double /'*'/'* (no ') */
						if (len >= 4 &&
								buf[len - 3] == '*' && buf[len - 4] == '/')
							buf[len - 2] = '\0';
						if (db_glob(buf, msg + 1) == 1) {
							/* reevaluate, to see if connection is still
							 * available */
							sabdb *walk;
							sabdb *stats = getRemoteDB(m->dbcv[i]->database);
							Mapi tm = m->dbcv[i]->conn;
							char *uri = mapi_get_uri(tm);
							if (stats == NULL) {
								Mfprintf(stderr, "target %s can no longer "
										"be resolved\n",
										m->dbcv[i]->database);
								/* schedule to drop connection */
								m->dbcv[i]->newconn = NULL;
								m->dbcv[i]->connupdate = 1;
								continue;
							}
							/* walk all connections, in an attempt to
							 * see if the original connection is still
							 * available, despite the removal of the
							 * server we got a message for */
							for (walk = stats; walk != NULL; walk = walk->next) {
								snprintf(buf, sizeof(buf), "%s%s",
										walk->conns->val, walk->dbname);
								if (strcmp(uri, buf) == 0)
									break;
							}
							if (walk == NULL) {
								snprintf(buf, sizeof(buf), "%s%s",
										stats->conns->val, stats->dbname);
								Mfprintf(stdout, "changing multiplexer target %s: %s->%s\n",
										m->dbcv[i]->database, uri, buf);
								tm = mapi_mapiuri(buf,
										m->dbcv[i]->user, m->dbcv[i]->pass,
										"sql");
								if (mapi_reconnect(tm) != MOK) {
									Mfprintf(stderr, "mapi_reconnect for %s "
											"failed: %s\n",
											m->dbcv[i]->database,
											mapi_error_str(tm));
									mapi_destroy(tm);
									/* schedule connection for removal */
									m->dbcv[i]->newconn = NULL;
									m->dbcv[i]->connupdate = 1;
									msab_freeStatus(&stats);
									continue;
								}
								mapi_cache_limit(tm, -1); /* don't page */

								/* let the new connection go live */
								m->dbcv[i]->newconn = tm;
								m->dbcv[i]->connupdate = 1;
							}
							msab_freeStatus(&stats);
						}
					}
				}
			}
		}
		pthread_mutex_unlock(&mpl_lock);

		free(msg); /* alloced by multiplexNotify* */
	}
	return NULL;
}

void
multiplexNotifyAddedDB(const char *database)
{
	char dbslash[256];
	char *p;

	if (mfmanager == 0)
		return;

	snprintf(dbslash, sizeof(dbslash), "+%s/", database);
	p = strdup(dbslash);
	if (write(mfpipe[1], &p, sizeof(p)) != sizeof(p)) {
		Mfprintf(stderr, "failed to write notify added message to mfpipe\n");
		free(p);
	}
	/* p is freed by MFconnectionManager */
	/* coverity[leaked_storage] */
}

void
multiplexNotifyRemovedDB(const char *database)
{
	char dbslash[256];
	char *p;

	if (mfmanager == 0)
		return;

	snprintf(dbslash, sizeof(dbslash), "-%s/", database);
	p = strdup(dbslash);
	if (write(mfpipe[1], &p, sizeof(p)) != sizeof(p)) {
		Mfprintf(stderr, "failed to write notify removed message to mfpipe\n");
		free(p);
	}
	/* p is freed by MFconnectionManager */
	/* coverity[leaked_storage] */
}

/* ultra ugly, we peek inside Sabaoth's internals to update the uplog
 * file */
extern char *_sabaoth_internal_dbname;

err
multiplexInit(char *name, char *pattern, FILE *sout, FILE *serr)
{
	multiplex *m = malloc(sizeof(multiplex));
	multiplexlist *mpl;
	char buf[256];
	char *p, *q;
	int i;

	/* the multiplex targets are given separated by commas, split them
	 * out in multiplex_database entries */
	/* user+pass@pattern,user+pass@pattern,... */

	m->tid = 0;
	m->gdklock = -1;
	m->shutdown = 0;
	m->name = strdup(name);
	m->pool = strdup(pattern);
	m->sout = sout;
	m->serr = serr;
	m->dbcc = 1;
	p = m->pool;
	while ((p = strchr(p, ',')) != NULL) {
		m->dbcc++;
		p++;
	}
	m->dbcv = malloc(sizeof(multiplex_database *) * m->dbcc);
	p = m->pool;
	i = 0;
	while ((q = strchr(p, ',')) != NULL) {
		m->dbcv[i] = malloc(sizeof(multiplex_database));
		m->dbcv[i]->user = malloc(sizeof(char) * (q - p + 1));
		memcpy(m->dbcv[i]->user, p, q - p);
		m->dbcv[i]->user[q - p] = '\0';
		if ((p = strchr(m->dbcv[i]->user, '+')) == NULL) {
			err e = newErr("illegal target %s: missing '+'", m->dbcv[i]->user);
			for (; i >= 0; i--) {
				free(m->dbcv[i]->user);
				free(m->dbcv[i]);
			}
			free(m->dbcv);
			free(m->pool);
			free(m);
			return(e);
		}
		*p = '\0';
		m->dbcv[i]->pass = p + 1;
		if ((p = strchr(m->dbcv[i]->pass, '@')) == NULL) {
			err e = newErr("illegal target %s+%s: missing '@'",
					m->dbcv[i]->user, m->dbcv[i]->pass);
			for (; i >= 0; i--) {
				free(m->dbcv[i]->user);
				free(m->dbcv[i]);
			}
			free(m->dbcv);
			free(m->pool);
			free(m);
			return(e);
		}
		*p = '\0';
		m->dbcv[i]->database = p + 1;
		m->dbcv[i]->conn = NULL;
		m->dbcv[i]->newconn = NULL;
		m->dbcv[i]->connupdate = 0;

		i++;
		p = q + 1;
	}
	m->dbcv[i] = malloc(sizeof(multiplex_database));
	m->dbcv[i]->user = strdup(p);
	if ((p = strchr(m->dbcv[i]->user, '+')) == NULL) {
		err e = newErr("illegal target %s: missing '+'", m->dbcv[i]->user);
		for (; i >= 0; i--) {
			free(m->dbcv[i]->user);
			free(m->dbcv[i]);
		}
		free(m->dbcv);
		free(m->pool);
		free(m);
		return(e);
	} else {
		*p = '\0';
		m->dbcv[i]->pass = p + 1;
		if ((p = strchr(m->dbcv[i]->pass, '@')) == NULL) {
			err e = newErr("illegal target %s+%s: missing '@'",
					m->dbcv[i]->user, m->dbcv[i]->pass);
			for (; i >= 0; i--) {
				free(m->dbcv[i]->user);
				free(m->dbcv[i]);
			}
			free(m->dbcv);
			free(m->pool);
			free(m);
			return(e);
		} else {
			*p = '\0';
			m->dbcv[i]->database = p + 1;
			m->dbcv[i]->conn = NULL;
			m->dbcv[i]->newconn = NULL;
			m->dbcv[i]->connupdate = 0;
		}
	}

	m->clients = NULL; /* initially noone is connected */

	if (mfmanager == 0) {
		pthread_attr_t detach;
		pthread_attr_init(&detach);
		pthread_attr_setdetachstate(&detach, PTHREAD_CREATE_DETACHED);

		/* create communication channel */
		if (pipe2(mfpipe, O_CLOEXEC) != 0)
			Mfprintf(stderr, "failed to create mfpipe: %s\n", strerror(errno));
		else {
#if !defined(HAVE_PIPE2) || O_CLOEXEC == 0
			(void) fcntl(mfpipe[0], F_SETFD, FD_CLOEXEC);
			(void) fcntl(mfpipe[1], F_SETFD, FD_CLOEXEC);
#endif
			Mfprintf(stdout, "starting multiplex-funnel connection manager\n");
			if ((i = pthread_create(&mfmanager, &detach,
									MFconnectionManager, NULL)) != 0) {
				Mfprintf(stderr, "failed to start MFconnectionManager: %s\n",
						 strerror(i));
				mfmanager = 0;
			}
		}
	}

	for (i = 0; i < m->dbcc; i++) {
		sabdb *stats = getRemoteDB(m->dbcv[i]->database);
		if (stats == NULL) {
			Mfprintf(serr, "mfunnel: target %s cannot be resolved\n",
					m->dbcv[i]->database);
			continue;
		}
		snprintf(buf, sizeof(buf), "%s%s", stats->conns->val, stats->dbname);
		Mfprintf(sout, "mfunnel: setting up multiplexer target %s->%s\n",
				m->dbcv[i]->database, buf);
		m->dbcv[i]->conn = mapi_mapiuri(buf,
				m->dbcv[i]->user, m->dbcv[i]->pass, "sql");
		msab_freeStatus(&stats);
	}

	pthread_mutex_lock(&mpl_lock);
	mpl = malloc(sizeof(multiplexlist));
	mpl->next = multiplexes;
	mpl->m = m;
	multiplexes = mpl;
	pthread_mutex_unlock(&mpl_lock);

	if ((i = pthread_create(&m->tid, NULL,
					multiplexThread, (void *)m)) != 0)
	{
		/* FIXME: we don't cleanup here */
		return(newErr("starting thread for multiplex-funnel %s failed: %s",
					name, strerror(i)));
	}

	/* fake lock such that sabaoth believes we are (still) running, we
	 * rely on merovingian moving to dbfarm here */
	snprintf(buf, sizeof(buf), "%s/.gdk_lock", name);
	if ((m->gdklock = MT_lockf(buf, F_TLOCK, 4, 1)) == -1) {
		/* locking failed, FIXME: cleanup here */
		Mfprintf(serr, "mfunnel: another instance is already running?\n");
		return(newErr("cannot lock for %s, already locked", name));
	} else if (m->gdklock == -2) {
		/* directory or something doesn't exist, FIXME: cleanup */
		Mfprintf(serr, "mfunnel: unable to create %s file: %s\n",
				buf, strerror(errno));
		return(newErr("cannot create lock for %s", name));
	}

	/* hack alert: set sabaoth uplog status by cheating with its
	 * internals -- we know dbname should be NULL, and hack it for the
	 * purpose of this moment, see also extern declaration before this
	 * function */
	_sabaoth_internal_dbname = name;
	if ((p = msab_registerStarting()) != NULL ||
			(p = msab_registerStarted()) != NULL ||
			(p = msab_marchScenario("mfunnel")) != NULL)
	{
		err em;

		_sabaoth_internal_dbname = NULL;

		Mfprintf(serr, "mfunnel: unable to startup %s: %s\n",
				name, p);
		em = newErr("cannot create funnel %s due to sabaoth: %s", name, p);
		free(p);

		return(em);
	}
	_sabaoth_internal_dbname = NULL;

	return(NO_ERR);
}

void
multiplexDestroy(char *mp)
{
	multiplexlist *ml, *mlp;
	multiplex *m = NULL;
	char *msg;

	/* lock and remove */
	pthread_mutex_lock(&mpl_lock);
	mlp = NULL;
	for (ml = multiplexes; ml != NULL; ml = ml->next) {
		if (strcmp(ml->m->name, mp) == 0) {
			m = ml->m;
			if (mlp == NULL) {
				multiplexes = ml->next;
			} else {
				mlp->next = ml->next;
			}
			break;
		}
		mlp = ml;
	}
	pthread_mutex_unlock(&mpl_lock);

	if (m == NULL) {
		Mfprintf(stderr, "request to remove non-existing "
				"multiplex-funnel: %s\n", mp);
		return;
	}

	/* deregister from sabaoth, same hack alert as at Init */
	_sabaoth_internal_dbname = m->name;
	if ((msg = msab_registerStop()) != NULL ||
		(msg = msab_wildRetreat()) != NULL) {
		Mfprintf(stderr, "mfunnel: %s\n", msg);
		free(msg);
	}
	_sabaoth_internal_dbname = NULL;

	/* signal the thread to stop and cleanup */
	m->shutdown = 1;
	pthread_join(m->tid, NULL);
}

static void
multiplexQuery(multiplex *m, char *buf, stream *fout)
{
	int i;
	const char *t;
	MapiHdl h;
	int64_t rlen;
	int fcnt;
	int qtype;

	/* first send the query to all, such that we don't waste time
	 * waiting for each server to produce an answer, but wait for all of
	 * them concurrently */
	for (i = 0; i < m->dbcc; i++) {
		if (m->dbcv[i]->conn == NULL) {
			mnstr_printf(fout, "!connection for %s is currently unresolved\n",
					m->dbcv[i]->database);
			mnstr_flush(fout);
			Mfprintf(m->serr, "failed to find a provider for %s\n",
					m->dbcv[i]->database);
			return;
		}
		if (!mapi_is_connected(m->dbcv[i]->conn)) {
			if (mapi_reconnect(m->dbcv[i]->conn) != MOK) {
				mnstr_printf(fout, "!failed to establish connection "
						"for %s: %s\n", m->dbcv[i]->database,
						mapi_error_str(m->dbcv[i]->conn));
				mnstr_flush(fout);
				Mfprintf(m->serr, "mapi_reconnect for %s failed: %s\n",
						m->dbcv[i]->database,
						mapi_error_str(m->dbcv[i]->conn));
				return;
			}
			mapi_cache_limit(m->dbcv[i]->conn, -1); /* don't page */
		}

		m->dbcv[i]->hdl = mapi_send(m->dbcv[i]->conn, buf);
	}
	/* fail as soon as one of the servers fails */
	t = NULL;
	rlen = 0;
	fcnt = -1;
	qtype = -1;
	for (i = 0; i < m->dbcc; i++) {
		h = m->dbcv[i]->hdl;
		/* check for responses */
		if (mapi_read_response(h) != MOK) {
			t = mapi_result_error(h);
			mnstr_printf(fout, "!node %s failed: %s\n",
					m->dbcv[i]->database, t ? t : "no response");
			Mfprintf(m->serr, "mapi_read_response for %s failed: %s\n",
					m->dbcv[i]->database, t ? t : "(no error)");
			break;
		}
		/* check for errors */
		if ((t = mapi_result_error(h)) != NULL) {
			mnstr_printf(fout, "!node %s failed: %s\n",
					m->dbcv[i]->database, t);
			Mfprintf(m->serr, "mapi_result_error for %s: %s\n",
					m->dbcv[i]->database, t);
			break;
		}
		/* check for result type consistency */
		if (qtype == -1) {
			qtype = mapi_get_querytype(h);
		} else if (qtype != mapi_get_querytype(h)) {
			t = "err"; /* for cleanup code below */
			mnstr_printf(fout, "!node %s returned a different type of result "
					"than the previous node\n", m->dbcv[i]->database);
			Mfprintf(m->serr, "encountered mix of result types, "
					"got %d, expected %d\n", mapi_get_querytype(h), qtype);
			break;
		}
		
		/* determine correctness based on headers */
		switch (qtype) {
			case Q_PARSE:
				/* mapi returns Q_PARSE for empty results */
				continue;
			case Q_TABLE:
				/* prepare easily appending of all results */
				rlen += mapi_get_row_count(h);
				if (fcnt == -1) {
					fcnt = mapi_get_field_count(h);
				} else if (mapi_get_field_count(h) != fcnt) {
					t = "err"; /* for cleanup code below */
					mnstr_printf(fout, "!node %s has mismatch in result fields\n",
							m->dbcv[i]->database);
					Mfprintf(m->serr, "mapi_get_field_count inconsistent for %s: "
							"got %d, expected %d\n",
							m->dbcv[i]->database,
							mapi_get_field_count(h), fcnt);
				}
				break;
			case Q_UPDATE:
				/* just pile up the update counts */
				rlen += mapi_rows_affected(h);
				break;
			case Q_SCHEMA:
				/* accept, just write ok lateron */
				break;
			case Q_TRANS:
				/* just check all servers end up in the same state */
				if (fcnt == -1) {
					fcnt = mapi_get_autocommit(m->dbcv[i]->conn);
				} else if (fcnt != mapi_get_autocommit(m->dbcv[i]->conn)) {
					t = "err"; /* for cleanup code below */
					mnstr_printf(fout, "!node %s has mismatch in transaction state\n",
							m->dbcv[i]->database);
					Mfprintf(m->serr, "mapi_get_autocommit inconsistent for %s: "
							"got %d, expected %d\n",
							m->dbcv[i]->database,
							mapi_get_autocommit(m->dbcv[i]->conn), fcnt);
				}
				break;
			default:
				t = "err"; /* for cleanup code below */
				mnstr_printf(fout, "!node %s returned unhandled result type\n",
						m->dbcv[i]->database);
				Mfprintf(m->serr, "unhandled querytype for %s: %d\n",
						m->dbcv[i]->database, mapi_get_querytype(h));
				break;
		}
		if (t != NULL)
			break;
	}

	/* error or empty result, just end here */
	if (t != NULL || qtype == Q_PARSE) {
		mnstr_flush(fout);
		for (i = 0; i < m->dbcc; i++)
			mapi_close_handle(m->dbcv[i]->hdl);
		return;
	}

	/* write output to client */
	switch (qtype) {
		case Q_TABLE:
			/* Compose the header.  For the table id, we just send 0,
			 * such that we never get a close request.  Steal headers
			 * from the first node. */
			mnstr_printf(fout, "&%d 0 %" PRId64 " %d %" PRId64 "\n",
					Q_TABLE, rlen, fcnt, rlen);
			/* now read the answers, and write them directly to the client */
			for (i = 0; i < m->dbcc; i++) {
				h = m->dbcv[i]->hdl;
				while ((t = mapi_fetch_line(h)) != NULL)
					if (i == 0 || *t != '%') /* skip other server's headers */
						mnstr_printf(fout, "%s\n", t);
			}
			break;
		case Q_UPDATE:
			/* Write a single header for all update counts, to sort of
			 * complement the transparency created for Q_TABLE results,
			 * but forget about last id data (wouldn't make sense if
			 * we'd emit multiple update counts either) */
			mnstr_printf(fout, "&%d %" PRId64 " -1\n", Q_UPDATE, rlen);
			break;
		case Q_SCHEMA:
			mnstr_printf(fout, "&%d\n", Q_SCHEMA);
			break;
		case Q_TRANS:
			mnstr_printf(fout, "&%d %c\n", Q_TRANS, fcnt ? 't' : 'f');
			break;
	}
	mnstr_flush(fout);
	/* finish up */
	for (i = 0; i < m->dbcc; i++)
		mapi_close_handle(m->dbcv[i]->hdl);
}

static void *
multiplexThread(void *d)
{
	multiplex *m = (multiplex *)d;
	struct timeval tv;
	fd_set fds;
	multiplex_client *c;
	int msock = -1;
	char buf[10 * BLOCK + 1];
	ssize_t len;
	int r, i;
	dpair p, q;

	/* select on upstream clients, on new data, read query, forward,
	 * union all results, send back, and restart cycle. */
	
	while (m->shutdown == 0) {
		FD_ZERO(&fds);
		for (c = m->clients; c != NULL; c = c->next) {
			FD_SET(c->sock, &fds);
			if (c->sock > msock)
				msock = c->sock;
		}

		/* wait up to 1 second. */
		tv.tv_sec = 1;
		tv.tv_usec = 0;
		r = select(msock + 1, &fds, NULL, NULL, &tv);

		/* evaluate if connections have to be switched */
		for (i = 0; i < m->dbcc; i++) {
			if (m->dbcv[i]->connupdate) {
				if (m->dbcv[i]->newconn != NULL) {
					/* put new connection live */
					Mfprintf(m->sout, "performing deferred connection cycle "
							"for %s from %s to %s\n",
							m->dbcv[i]->database,
							m->dbcv[i]->conn != NULL ?
								mapi_get_uri(m->dbcv[i]->conn) :
								"<unconnected>",
							mapi_get_uri(m->dbcv[i]->newconn));
					mapi_disconnect(m->dbcv[i]->conn);
					mapi_destroy(m->dbcv[i]->conn);
					m->dbcv[i]->conn = m->dbcv[i]->newconn;
					m->dbcv[i]->newconn = NULL;
					m->dbcv[i]->connupdate = 0;
				} else {
					/* put new connection live */
					Mfprintf(m->sout, "performing deferred connection drop "
							"for %s from %s\n",
							m->dbcv[i]->database,
							m->dbcv[i]->conn != NULL ?
								mapi_get_uri(m->dbcv[i]->conn) :
								"<unconnected>");
					mapi_disconnect(m->dbcv[i]->conn);
					mapi_destroy(m->dbcv[i]->conn);
					m->dbcv[i]->conn = NULL;
					m->dbcv[i]->connupdate = 0;
				}
			}
		}

		/* nothing interesting has happened */
		if (r <= 0)
			continue;
		for (c = m->clients; c != NULL; c = c->next) {
			if (!FD_ISSET(c->sock, &fds))
				continue;
			if ((len = mnstr_read(c->fdin, buf, 1, 10 * BLOCK)) < 0) {
				/* error, or some garbage */
				multiplexRemoveClient(m, c);
				/* don't crash on now stale c */
				break;
			} else if (len == 0) {
				/* flush from client, ignore */
				continue;
			}

			buf[len] = '\0';
			switch (*buf) {
				case 's':
				case 'S':
					/* accepted, just SQL queries */
					break;
				case 'X':
					/* ignored, some clients just really insist on using
					 * these */
					mnstr_flush(c->fout);
					continue;
				default:
					mnstr_printf(c->fout, "!modifier %c not supported by "
							"multiplex-funnel\n", *buf);
					mnstr_flush(c->fout);
					Mfprintf(m->serr, "client attempted to perform %c "
							"type query: %s\n", *buf, buf);
					continue;
			}
			/* we assume (and require) the query to fit in one block,
			 * that is, we only forward the first block, without having
			 * any idea what it is */
			multiplexQuery(m, buf + 1, c->fout);
		}
	}

	Mfprintf(stdout, "stopping mfunnel '%s'\n", m->name);

	/* free, cleanup, etc. */
	while (m->clients != NULL) {
		c = m->clients;
		close_stream(c->fdin);
		close_stream(c->fout);
		free(c->name);
		m->clients = m->clients->next;
		free(c);
	}
	for (i = 0; i < m->dbcc; i++) {
		if (m->dbcv[i]->connupdate && m->dbcv[i]->newconn != NULL)
			mapi_destroy(m->dbcv[i]->newconn);
		if (m->dbcv[i]->conn != NULL)
			mapi_destroy(m->dbcv[i]->conn);
		free(m->dbcv[i]->user);
		/* pass and database belong to the same malloced block from user */
	}
	fflush(m->sout);
	fclose(m->sout);
	fflush(m->serr);
	fclose(m->serr);
	close(m->gdklock);
	free(m->pool);

	/* last bit, remove from logger structure */
	pthread_mutex_lock(&_mero_topdp_lock);

	q = _mero_topdp->next; /* skip console */
	p = q->next;
	while (p != NULL) {
		if (p->type == MEROFUN && strcmp(p->dbname, m->name) == 0) {
			/* log everything that's still in the pipes */
			logFD(p->out, "MSG", p->dbname, (long long int)p->pid, _mero_logfile, 1);
			/* remove from the list */
			q->next = p->next;
			/* close the descriptors */
			close(p->out);
			close(p->err);
			Mfprintf(stdout, "mfunnel '%s' has stopped\n", p->dbname);
			free(p->dbname);
			free(p);
			break;
		}
		q = p;
		p = q->next;
	}

	pthread_mutex_unlock(&_mero_topdp_lock);

	free(m->name);
	free(m);
	return NULL;
}

void
multiplexAddClient(char *mp, int sock, stream *fout, stream *fdin, char *name)
{
	multiplex_client *w;
	multiplex_client *n = malloc(sizeof(multiplex_client));
	multiplexlist *ml;
	multiplex *m;

	n->sock = sock;
	n->fdin = fdin;
	n->fout = fout;
	n->name = strdup(name);
	n->next = NULL;

	pthread_mutex_lock(&mpl_lock);
	for (ml = multiplexes; ml != NULL; ml = ml->next) {
		if (strcmp(ml->m->name, mp) == 0)
			break;
	}
	if (ml == NULL) {
		pthread_mutex_unlock(&mpl_lock);
		Mfprintf(stderr, "failed to find multiplex-funnel '%s' for client %s\n",
				mp, name);
		mnstr_printf(fout, "!monetdbd: internal error: could not find multiplex-funnel '%s'\n", mp);
		mnstr_flush(fout);
		close_stream(fdin);
		close_stream(fout);
		free(n->name);
		free(n);
		return;
	}
	m = ml->m;

	if (m->clients == NULL) {
		m->clients = n;
	} else {
		for (w = m->clients; w->next != NULL; w = w->next)
			;
		w->next = n;
	}
	pthread_mutex_unlock(&mpl_lock);

	Mfprintf(m->sout, "mfunnel: added new client %s\n", n->name);

	/* send client a prompt */
	mnstr_flush(fout);
}

void
multiplexRemoveClient(multiplex *m, multiplex_client *c)
{
	multiplex_client *w;
	multiplex_client *p = NULL;

	Mfprintf(m->sout, "mfunnel: removing client %s\n", c->name);

	for (w = m->clients; w != NULL; w = w->next) {
		if (w == c) {
			if (w == m->clients) {
				m->clients = w->next;
			} else {
				p->next = w->next;
			}
			c->next = NULL;
			close_stream(c->fdin);
			close_stream(c->fout);
			free(c->name);
			free(c);
			break;
		}
		p = w;
	}
}

/* vim:set ts=4 sw=4 noexpandtab: */
