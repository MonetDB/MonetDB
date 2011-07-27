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

#include "monetdb_config.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sys/types.h>

#include <mapi.h>

#include "utils/glob.h"

#include "merovingian.h"
#include "discoveryrunner.h"
#include "multiplex-funnel.h"

typedef struct _multiplexlist {
	multiplex *m;
	struct _multiplexlist *next;
} multiplexlist;

static multiplexlist *multiplexes = NULL;
static pthread_t mfmanager = 0;
static int mfpipe[2];
/**
 * Connections from all multiplex funnels are maintained by a single
 * thread that resolves and creates connections upon updates on the
 * discovery space.  Connections aren't made/checked/updated upon their
 * usage, because this introduces delays for the clients.  This is in
 * particular an issue when a target is updated to point to another
 * database.  To maintain a stable query performance, the connection
 * creation must happen in the background and set life once established.
 */
void
MFconnectionManager(void *d)
{
	int i;
	multiplex *m;
	multiplexlist *w;
	char buf[1024];
	size_t len;
	void *p;
	char *msg;

	(void)d;

	while (_mero_keep_listening) {
		/* FIXME: use select for timeout */
		if (read(mfpipe[0], &p, sizeof(void *)) < 0) {
			Mfprintf(stderr, "failed reading from notification pipe: %s\n",
					strerror(errno));
			break;
		}
		msg = (char *)p;

		/* intended behaviour:
		 * - additions don't change any connection targets, they only
		 *   fill in gaps (conn == NULL)
		 * - removals of targets in use, cause a re-lookup of the
		 *   original pattern, on failure, conn is left NULL
		 */
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
						if (glob(buf, msg + 1) == 1) {
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
						if (glob(buf, msg + 1) == 1) {
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
	}
}

void
multiplexNotifyAddedDB(const char *database)
{
	char dbslash[256];
	void *p;

	if (mfmanager == 0)
		return;

	snprintf(dbslash, sizeof(dbslash), "+%s/", database);
	p = strdup(dbslash);
	if (write(mfpipe[1], &p, sizeof(void *)) != sizeof(void *))
		Mfprintf(stderr, "failed to write notify added message to mfpipe\n");
}

void
multiplexNotifyRemovedDB(const char *database)
{
	char dbslash[256];
	void *p;

	if (mfmanager == 0)
		return;

	snprintf(dbslash, sizeof(dbslash), "-%s/", database);
	p = strdup(dbslash);
	if (write(mfpipe[1], &p, sizeof(void *)) != sizeof(void *))
		Mfprintf(stderr, "failed to write notify removed message to mfpipe\n");
}

err
multiplexInit(multiplex **ret, char *database)
{
	multiplex *m = malloc(sizeof(multiplex));
	multiplexlist *mpl;
	char buf[256];
	char *p, *q;
	int i;

	/* the multiplex targets are given separated by commas, split them
	 * out in multiplex_database entries */
	/* user+pass@pattern,user+pass@pattern,... */

	Mfprintf(stdout, "building multiplexer for %s\n", database);

	m->tid = 0;
	m->pool = strdup(database);
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
		if (pipe(mfpipe) != 0)
			Mfprintf(stderr, "failed to create mfpipe: %s\n", strerror(errno));

		if ((i = pthread_create(&mfmanager, &detach,
				(void *(*)(void *))MFconnectionManager, (void *)NULL)) != 0)
		{
			Mfprintf(stderr, "failed to start MFconnectionManager: %s\n",
					strerror(i));
			mfmanager = 0;
		}
	}

	for (i = 0; i < m->dbcc; i++) {
		sabdb *stats = getRemoteDB(m->dbcv[i]->database);
		if (stats == NULL) {
			Mfprintf(stderr, "target %s cannot be resolved\n",
					m->dbcv[i]->database);
			continue;
		}
		snprintf(buf, sizeof(buf), "%s%s", stats->conns->val, stats->dbname);
		Mfprintf(stdout, "setting up multiplexer target %s->%s\n",
				m->dbcv[i]->database, buf);
		m->dbcv[i]->conn = mapi_mapiuri(buf,
				m->dbcv[i]->user, m->dbcv[i]->pass, "sql");
		msab_freeStatus(&stats);
	}

	mpl = malloc(sizeof(multiplexlist));
	mpl->next = multiplexes;
	mpl->m = m;
	multiplexes = mpl;

	*ret = m;
	return(NO_ERR);
}

void
multiplexQuery(multiplex *m, char *buf, stream *fout)
{
	int i;
	MapiHdl hdl[m->dbcc];
	char *t;
	mapi_int64 rlen;
	int fcnt;
	char emptyres, isempty;

	/* first send the query to all, such that we don't waste time
	 * waiting for each server to produce an answer, but wait for all of
	 * them concurrently */
	for (i = 0; i < m->dbcc; i++) {
		if (m->dbcv[i]->conn == NULL) {
			mnstr_printf(fout, "!connection for %s is currently unresolved\n",
					m->dbcv[i]->database);
			mnstr_flush(fout);
			Mfprintf(stderr, "failed to find a provider for %s\n",
					m->dbcv[i]->database);
			return;
		}
		if (!mapi_is_connected(m->dbcv[i]->conn)) {
			if (mapi_reconnect(m->dbcv[i]->conn) != MOK) {
				mnstr_printf(fout, "!failed to establish connection "
						"for %s: %s\n", m->dbcv[i]->database,
						mapi_error_str(m->dbcv[i]->conn));
				mnstr_flush(fout);
				Mfprintf(stderr, "mapi_reconnect for %s failed: %s\n",
						m->dbcv[i]->database,
						mapi_error_str(m->dbcv[i]->conn));
				return;
			}
			mapi_cache_limit(m->dbcv[i]->conn, -1); /* don't page */
		}

		hdl[i] = mapi_send(m->dbcv[i]->conn, buf);
	}
	/* fail as soon as one of the servers fails */
	t = NULL;
	rlen = 0;
	fcnt = -1;
	emptyres = 0;
	for (i = 0; i < m->dbcc; i++) {
		if (mapi_read_response(hdl[i]) != MOK) {
			t = mapi_result_error(hdl[i]);
			mnstr_printf(fout, "!node %s failed: %s\n",
					m->dbcv[i]->database, t ? t : "no response");
			Mfprintf(stderr, "mapi_read_response for %s failed: %s\n",
					m->dbcv[i]->database, t ? t : "(no error)");
			break;
		}
		if ((t = mapi_result_error(hdl[i])) != NULL) {
			mnstr_printf(fout, "!node %s failed: %s\n",
					m->dbcv[i]->database, t);
			Mfprintf(stderr, "mapi_result_error for %s: %s\n",
					m->dbcv[i]->database, t);
			break;
		}
		isempty = 0;
		/* mapi return Q_PARSE for empty results */
		if (mapi_get_querytype(hdl[i]) == Q_PARSE)
			emptyres = isempty = 1;
		if (emptyres && !isempty) {
			t = "err"; /* for cleanup code below */
			mnstr_printf(fout, "!node %s returned a result while previous "
					"did not\n", m->dbcv[i]->database);
			Mfprintf(stderr, "encountered mix of empty and non-empty "
					"results\n");
			break;
		}
		if (isempty)
			continue;
		/* only support Q_TABLE, because appending is easy */
		if (mapi_get_querytype(hdl[i]) != Q_TABLE) {
			t = "err"; /* for cleanup code below */
			mnstr_printf(fout, "!node %s returned a non-table result\n",
					m->dbcv[i]->database);
			Mfprintf(stderr, "querytype != Q_TABLE for %s: %d\n",
					m->dbcv[i]->database, mapi_get_querytype(hdl[i]));
			break;
		}
		rlen += mapi_get_row_count(hdl[i]);
		if (fcnt == -1) {
			fcnt = mapi_get_field_count(hdl[i]);
		} else {
			if (mapi_get_field_count(hdl[i]) != fcnt) {
				t = "err"; /* for cleanup code below */
				mnstr_printf(fout, "!node %s has mismatch in result fields\n",
						m->dbcv[i]->database);
				Mfprintf(stderr, "mapi_get_field_count inconsistent for %s: "
						"got %d, expected %d\n",
						m->dbcv[i]->database,
						mapi_get_field_count(hdl[i]), fcnt);
				break;
			}
		}
	}
	if (t != NULL || emptyres) {
		mnstr_flush(fout);
		for (i = 0; i < m->dbcc; i++)
			mapi_close_handle(hdl[i]);
		return;
	}
	/* Compose the header.  For the table id, we just send 0, such that
	 * we never get a close request.  Steal headers from the first node. */
	mnstr_printf(fout, "&%d 0 " LLFMT " %d " LLFMT "\n", Q_TABLE, rlen, fcnt, rlen);
	/* now read the answers, and write them directly to the client */
	for (i = 0; i < m->dbcc; i++) {
		while ((t = mapi_fetch_line(hdl[i])) != NULL)
			if (i == 0 || *t != '%') /* skip other server's headers */
				mnstr_printf(fout, "%s\n", t);
	}
	mnstr_flush(fout);
	/* finish up */
	for (i = 0; i < m->dbcc; i++)
		mapi_close_handle(hdl[i]);
}

void
multiplexThread(void *d)
{
	multiplex *m = (multiplex *)d;
	struct timeval tv;
	fd_set fds;
	multiplex_client *c;
	int msock = -1;
	char buf[BLOCK + 1];
	ssize_t len;
	int r, i;

	/* select on upstream clients, on new data, read query, forward,
	 * union all results, send back, and restart cycle. */
	
	while (_mero_keep_listening == 1) {
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
					Mfprintf(stdout, "performing deferred connection cycle "
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
					Mfprintf(stdout, "performing deferred connection drop "
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
			if ((len = mnstr_read(c->fdin, buf, 1, BLOCK)) < 0) {
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
					Mfprintf(stderr, "client attempted to perform %c "
							"type query: %s\n", *buf, buf);
					continue;
			}
			/* we assume (and require) the query to fit in one block,
			 * that is, we only forward the first block, without having
			 * any idea what it is */
			multiplexQuery(m, buf + 1, c->fout);
		}
	}
}

void
multiplexAddClient(multiplex *m, int sock, stream *fout, stream *fdin, char *name)
{
	multiplex_client *w;
	multiplex_client *n = malloc(sizeof(multiplex_client));

	n->sock = sock;
	n->fdin = fdin;
	n->fout = fout;
	n->name = strdup(name);
	n->next = NULL;

	if (m->clients == NULL) {
		m->clients = n;
	} else {
		for (w = m->clients; w->next != NULL; w = w->next);
		w->next = n;
	}

	Mfprintf(stdout, "added new client %s for multiplexer %s\n",
			n->name, m->pool);

	/* send client a prompt */
	mnstr_flush(fout);
}

void
multiplexRemoveClient(multiplex *m, multiplex_client *c)
{
	multiplex_client *w;
	multiplex_client *p = NULL;

	Mfprintf(stdout, "removing client %s for multiplexer %s\n",
			c->name, m->pool);

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
			close(c->sock);
			free(c->name);
			free(c);
			break;
		}
		p = w;
	}
}

/* vim:set ts=4 sw=4 noexpandtab: */
