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
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/in.h>
#include <time.h>
#include <string.h>  /* strerror */
#include <unistd.h>  /* select */
#include <signal.h>

#include <errno.h>
#include <pthread.h>

#include <msabaoth.h>
#include <utils/utils.h>
#include <utils/properties.h>
#include <utils/database.h>
#include <utils/control.h>

#include "merovingian.h"
#include "discoveryrunner.h" /* broadcast, remotedb */
#include "peering.h"
#include "forkmserver.h"
#include "controlrunner.h"


static void
leavedb(char *name)
{
	char buf[128];
	snprintf(buf, sizeof(buf),
			"LEAV %s mapi:monetdb://%s:%u/",
			name, _mero_hostname,
			(unsigned int)getConfNum(_mero_props, "port"));
	broadcast(buf);
}

static void
leavedbS(sabdb *stats)
{
	confkeyval *props = getDefaultProps();
	char *shared;
	readProps(props, stats->path);
	shared = getConfVal(props, "shared");
	if (stats->locked != 1 && (shared == NULL || strcmp(shared, "no") != 0))
		leavedb(stats->dbname);
	freeConfFile(props);
	free(props);
}

static void
anncdbS(sabdb *stats)
{
	char buf[128];
	confkeyval *props = getDefaultProps();
	char *shared;
	readProps(props, stats->path);
	shared = getConfVal(props, "shared");
	if (stats->locked != 1 && (shared == NULL || strcmp(shared, "no") != 0)) {
		snprintf(buf, sizeof(buf),
				"ANNC %s%s%s mapi:monetdb://%s:%u/ %d",
				stats->dbname,
				shared == NULL ? "" : "/",
				shared == NULL ? "" : shared,
				_mero_hostname,
				(unsigned int)getConfNum(_mero_props, "port"),
				getConfNum(_mero_props, "discoveryttl") + 60);
		broadcast(buf);
	}
	freeConfFile(props);
	free(props);
}

inline static int
recvWithTimeout(int msgsock, char *buf, size_t buflen)
{
	fd_set fds;
	struct timeval tv;
	int retval;

	FD_ZERO(&fds);
	FD_SET(msgsock, &fds);

	/* Wait up to 1 second.  If a client doesn't make this, it's too slow */
	tv.tv_sec = 1;
	tv.tv_usec = 0;
	retval = select(msgsock + 1, &fds, NULL, NULL, &tv);
	if (retval <= 0) {
		/* nothing interesting has happened */
		return(-2);
	}

	return(recv(msgsock, buf, buflen, 0));
}

void
controlRunner(void *d)
{
	int *socks = (int *)d;
	int usock = socks[0];
	int tsock = socks[1];
	int sock = -1;
	char buf[256];
	char buf2[8096];
	char *p, *q;
	sabdb *stats;
	int pos = 0;
	int retval;
	fd_set fds;
	struct timeval tv;
	int msgsock;
	size_t len;
	err e;
	int maxsock;
	char origin[128];

	if (usock > tsock) {
		maxsock = usock + 1;
	} else {
		maxsock = tsock + 1;
	}

	do {
		FD_ZERO(&fds);
		FD_SET(usock, &fds);
		if (tsock > 0)
			FD_SET(tsock, &fds);

		/* Wait up to 5 seconds. */
		tv.tv_sec = 5;
		tv.tv_usec = 0;
		retval = select(maxsock, &fds, NULL, NULL, &tv);
		if (retval == 0) {
			/* nothing interesting has happened */
			continue;
		}
		if (retval < 0) {
			if (_mero_keep_listening == 0)
				break;
			if (errno != EINTR) {
				e = newErr("control runner: error during select: %s",
						strerror(errno));
			}
			continue;
		}

		if (FD_ISSET(usock, &fds)) {
			sock = usock;
		} else if (tsock > 0 && FD_ISSET(tsock, &fds)) {
			sock = tsock;
		} else {
			continue;
		}

		if ((msgsock = accept(sock, (SOCKPTR) 0, (socklen_t *) 0)) < 0) {
			if (_mero_keep_listening == 0)
				break;
			if (errno != EINTR) {
				e = newErr("control runner: error during accept: %s",
						strerror(errno));
			}
			continue;
		}

		snprintf(origin, sizeof(origin), "(local)");

		/* do password ritual for TCP connections */
		if (sock == tsock) {
			struct sockaddr_in saddr;
			socklen_t saddrlen = sizeof(struct sockaddr_in);

			/* below routine is eligable for a function (reuse in
			 * merovingian_client.c) */
			if (getpeername(msgsock,
						(struct sockaddr *)&saddr, &saddrlen) == -1)
			{
				Mfprintf(_mero_ctlerr, "couldn't get peername of client: %s\n",
						strerror(errno));
				snprintf(origin, sizeof(origin), "(unknown)");
			} else {
				/* avoid doing this, it requires some includes that probably
				 * give trouble on windowz
				 host = inet_ntoa(saddr.sin_addr);
				 */
				struct hostent *hoste = 
					gethostbyaddr(&saddr.sin_addr.s_addr, 4, saddr.sin_family);
				if (hoste == NULL) {
					snprintf(origin, sizeof(origin), "%u.%u.%u.%u:%u",
							(unsigned)((ntohl(saddr.sin_addr.s_addr) >> 24) & 0xff),
							(unsigned)((ntohl(saddr.sin_addr.s_addr) >> 16) & 0xff),
							(unsigned)((ntohl(saddr.sin_addr.s_addr) >> 8) & 0xff),
							(unsigned)(ntohl(saddr.sin_addr.s_addr) & 0xff),
							(unsigned)(ntohs(saddr.sin_port)));
				} else {
					snprintf(origin, sizeof(origin), "%s:%u",
							hoste->h_name, (unsigned)(ntohs(saddr.sin_port)));
				}
			}

			/* send challenge */
			p = buf;
			generateSalt(p, 32);
			len = snprintf(buf2, sizeof(buf2),
					"merovingian:2:%s:\n", p);
			send(msgsock, buf2, len, 0);
			if ((pos = recvWithTimeout(msgsock, buf2, sizeof(buf2))) == 0) {
				close(msgsock);
				continue;
			} else if (pos == -1) {
				Mfprintf(_mero_ctlerr, "%s: error reading from control "
						"channel: %s\n", origin, strerror(errno));
				close(msgsock);
				continue;
			} else if (pos == -2) {
				Mfprintf(_mero_ctlerr, "%s: time-out reading from control "
						"channel, disconnecting client\n", origin);
				close(msgsock);
				continue;
			}
			buf2[pos - 1] = '\0';
			pos = 0;

			q = strchr(buf2, ':');
			if (q == NULL) {
				Mfprintf(_mero_ctlout, "%s: invalid response "
						"(missing mode)\n", origin);
				len = snprintf(buf2, sizeof(buf2),
						"invalid response\n");
				send(msgsock, buf2, len, 0);
				close(msgsock);
				continue;
			}
			*q++ = '\0';

			p = control_hash(getConfVal(_mero_props, "passphrase"), p);
			if (strcmp(buf2, p) != 0) {
				Mfprintf(_mero_ctlout, "%s: permission denied "
						"(bad passphrase)\n", origin);
				len = snprintf(buf2, sizeof(buf2),
						"access denied\n");
				send(msgsock, buf2, len, 0);
				close(msgsock);
				continue;
			}

			if (strcmp(q, "control") == 0) {
				len = snprintf(buf2, sizeof(buf2), "OK\n");
				send(msgsock, buf2, len, 0);
			} else if (strcmp(q, "peer") == 0) {
				pthread_t ptid; /* FIXME: register global */
				int ret;
				len = snprintf(buf2, sizeof(buf2), "OK\n");
				send(msgsock, buf2, len, 0);
				/* start a separate thread to handle the peering */
				if ((ret = pthread_create(&ptid, NULL,
							(void *(*)(void *))peeringServerThread,
							(void *)&msgsock)) != 0)
				{
					Mfprintf(stderr, "failed to create peeringServerThread: %s\n",
							strerror(ret));
				}
			} else {
				Mfprintf(_mero_ctlout, "%s: invalid mode "
						"(%s)\n", origin, q);
				len = snprintf(buf2, sizeof(buf2),
						"invalid mode: %s\n", q);
				send(msgsock, buf2, len, 0);
				close(msgsock);
				continue;
			}
		}

		while (_mero_keep_listening) {
			if (pos == 0) {
				if ((pos = recvWithTimeout(msgsock, buf, sizeof(buf))) == 0) {
					/* EOF */
					break;
				} else if (pos == -1) {
					/* we got interrupted ... so what? */
					if (errno == EINTR) {
						pos = 0;
						continue;
					}
					/* hmmm error ... give up */
					Mfprintf(_mero_ctlerr, "%s: error reading from control "
							"channel: %s\n", origin, strerror(errno));
					break;
				} else if (pos == -2) {
					Mfprintf(_mero_ctlerr, "%s: time-out reading from "
							"control channel, disconnecting client\n", origin);
					close(msgsock);
					break;
				} else {
					buf[pos] = '\0';
					pos = 0;
				}
			}
			q = buf + pos;
			p = strchr(q, '\n');
			if (p == NULL) {
				/* skip, must be garbage */
				Mfprintf(_mero_ctlerr, "%s: skipping garbage on control "
						"channel: %s\n", origin, buf);
				pos = 0;
				continue;
			}
			*p++ = '\0';
			if (*p == '\0') {
				pos = 0;
			} else {
				pos = p - buf;
			}

			/* format is simple: database<space>command */
			if ((p = strchr(q, ' ')) == NULL) {
				Mfprintf(_mero_ctlerr, "%s: malformed control signal: %s\n",
						origin, q);
			} else {
				*p++ = '\0';
				if (strcmp(p, "ping") == 0) {
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send(msgsock, buf2, len, 0);
				} else if (strcmp(p, "start") == 0) {
					err e;
					if ((e = msab_getStatus(&stats, q)) != NULL) {
						len = snprintf(buf2, sizeof(buf2),
								"internal error, please review the logs\n");
						send(msgsock, buf2, len, 0);
						Mfprintf(_mero_ctlerr, "%s: start: msab_getStatus: "
								"%s\n", origin, e);
						freeErr(e);
						continue;
					} else {
						if (stats == NULL) {
							Mfprintf(_mero_ctlerr, "%s: received start signal "
									"for database not under merovingian "
									"control: %s\n", origin, q);
							len = snprintf(buf2, sizeof(buf2),
									"no such database: %s\n", q);
							send(msgsock, buf2, len, 0);
							continue;
						}

						if (stats->state == SABdbRunning) {
							Mfprintf(_mero_ctlerr, "%s: received start signal "
									"for already running database: %s\n",
									origin, q);
							len = snprintf(buf2, sizeof(buf2),
									"database is already running: %s\n", q);
							send(msgsock, buf2, len, 0);
							msab_freeStatus(&stats);
							continue;
						}

						msab_freeStatus(&stats);
					}
					if ((e = forkMserver(q, &stats, 1)) != NO_ERR) {
						Mfprintf(_mero_ctlerr, "%s: failed to fork mserver: "
								"%s\n", origin, getErrMsg(e));
						len = snprintf(buf2, sizeof(buf2),
								"starting '%s' failed: %s\n",
								q, getErrMsg(e));
						send(msgsock, buf2, len, 0);
						freeErr(e);
						stats = NULL;
					} else {
						len = snprintf(buf2, sizeof(buf2), "OK\n");
						send(msgsock, buf2, len, 0);
						Mfprintf(_mero_ctlout, "%s: started database '%s'\n",
								origin, q);
					}

					if (stats != NULL)
						msab_freeStatus(&stats);
				} else if (strcmp(p, "stop") == 0 ||
						strcmp(p, "kill") == 0)
				{
					dpair dp;
					/* we need to find the right dpair, that is we
					 * sort of assume the control signal is right */
					pthread_mutex_lock(&_mero_topdp_lock);
					dp = _mero_topdp->next; /* don't need the console/log */
					while (dp != NULL) {
						if (strcmp(dp->dbname, q) == 0) {
							if (strcmp(p, "stop") == 0) {
								terminateProcess(dp);
								Mfprintf(_mero_ctlout, "%s: stopped "
										"database '%s'\n", origin, q);
							} else {
								kill(dp->pid, SIGKILL);
								Mfprintf(_mero_ctlout, "%s: killed "
										"database '%s'\n", origin, q);
							}
							len = snprintf(buf2, sizeof(buf2), "OK\n");
							send(msgsock, buf2, len, 0);
							break;
						}
						dp = dp->next;
					}
					if (dp == NULL) {
						Mfprintf(_mero_ctlerr, "%s: received stop signal for "
								"non running database: %s\n", origin, q);
						len = snprintf(buf2, sizeof(buf2),
								"database is not running: %s\n", q);
						send(msgsock, buf2, len, 0);
					}
					pthread_mutex_unlock(&_mero_topdp_lock);
				} else if (strcmp(p, "create") == 0) {
					err e = db_create(q);
					if (e != NO_ERR) {
						Mfprintf(_mero_ctlerr, "%s: failed to create "
								"database '%s': %s\n", origin, q, getErrMsg(e));
						len = snprintf(buf2, sizeof(buf2),
								"%s\n", getErrMsg(e));
						send(msgsock, buf2, len, 0);
						free(e);
					} else {
						Mfprintf(_mero_ctlout, "%s: created database '%s'\n",
								origin, q);
						len = snprintf(buf2, sizeof(buf2), "OK\n");
						send(msgsock, buf2, len, 0);
					}
				} else if (strcmp(p, "destroy") == 0) {
					err e = db_destroy(q);
					if (e != NO_ERR) {
						Mfprintf(_mero_ctlerr, "%s: failed to destroy "
								"database '%s': %s\n", origin, q, getErrMsg(e));
						len = snprintf(buf2, sizeof(buf2),
								"%s\n", getErrMsg(e));
						send(msgsock, buf2, len, 0);
						free(e);
					} else {
						/* we can leave without tag, will remove all,
						 * generates an "leave request for unknown
						 * database" if not shared (e.g. when under
						 * maintenance) */
						leavedb(q);
						Mfprintf(_mero_ctlout, "%s: destroyed database '%s'\n",
								origin, q);
						len = snprintf(buf2, sizeof(buf2), "OK\n");
						send(msgsock, buf2, len, 0);
					}
				} else if (strcmp(p, "lock") == 0) {
					char *e = db_lock(q);
					if (e != NULL) {
						Mfprintf(_mero_ctlerr, "%s: failed to lock "
								"database '%s': %s\n", origin, q, getErrMsg(e));
						len = snprintf(buf2, sizeof(buf2),
								"%s\n", getErrMsg(e));
						send(msgsock, buf2, len, 0);
						free(e);
					} else {
						/* we go under maintenance, unshare it, take
						 * spam if database happened to be unshared "for
						 * love" */
						leavedb(q);
						Mfprintf(_mero_ctlout, "%s: locked database '%s'\n",
								origin, q);
						len = snprintf(buf2, sizeof(buf2), "OK\n");
						send(msgsock, buf2, len, 0);
					}
				} else if (strcmp(p, "release") == 0) {
					char *e = db_release(q);
					if (e != NULL) {
						Mfprintf(_mero_ctlerr, "%s: failed to release "
								"database '%s': %s\n", origin, q, getErrMsg(e));
						len = snprintf(buf2, sizeof(buf2),
								"%s\n", getErrMsg(e));
						send(msgsock, buf2, len, 0);
						free(e);
					} else {
						/* announce database, but need to do it the
						 * right way so we don't accidentially announce
						 * an unshared database */
						if ((e = msab_getStatus(&stats, q)) != NULL) {
							len = snprintf(buf2, sizeof(buf2),
									"internal error, please review the logs\n");
							send(msgsock, buf2, len, 0);
							Mfprintf(_mero_ctlerr, "%s: release: "
									"msab_getStatus: %s\n", origin, e);
							freeErr(e);
							/* we need to OK regardless, as releasing
							 * succeed */
						} else {
							anncdbS(stats);
							msab_freeStatus(&stats);
						}
						Mfprintf(_mero_ctlout, "%s: released database '%s'\n",
								origin, q);
						len = snprintf(buf2, sizeof(buf2), "OK\n");
						send(msgsock, buf2, len, 0);
					}
				} else if (strncmp(p, "name=", strlen("name=")) == 0) {
					char *e;

					p += strlen("name=");
					e = db_rename(q, p);
					if (e != NULL) {
						Mfprintf(_mero_ctlerr, "%s: %s\n", origin, e);
						len = snprintf(buf2, sizeof(buf2), "%s\n", e);
						send(msgsock, buf2, len, 0);
						free(e);
					} else {
						if ((e = msab_getStatus(&stats, p)) != NULL) {
							Mfprintf(_mero_ctlerr, "%s: name: msab_getStatus:"
									" %s\n", origin, e);
							freeErr(e);
							/* should not fail, since the rename was
							 * already successful */
						} else {
							leavedb(q); /* could be spam, but shouldn't harm */
							anncdbS(stats);
							msab_freeStatus(&stats);
						}
						Mfprintf(_mero_ctlout, "%s: renamed database '%s' "
								"to '%s'\n", origin, q, p);
						len = snprintf(buf2, sizeof(buf2), "OK\n");
						send(msgsock, buf2, len, 0);
					}
				} else if (strchr(p, '=') != NULL) {
					char *val;
					char doshare = 0;

					if ((e = msab_getStatus(&stats, q)) != NULL) {
						len = snprintf(buf2, sizeof(buf2),
								"internal error, please review the logs\n");
						send(msgsock, buf2, len, 0);
						Mfprintf(_mero_ctlerr, "%s: set: msab_getStatus: "
								"%s\n", origin, e);
						freeErr(e);
						continue;
					}
					if (stats == NULL) {
						Mfprintf(_mero_ctlerr, "%s: received property signal "
								"for unknown database: %s\n", origin, q);
						len = snprintf(buf2, sizeof(buf2),
								"unknown database: %s\n", q);
						send(msgsock, buf2, len, 0);
						continue;
					}

					val = strchr(p, '=');
					*val++ = '\0';
					if (*val == '\0')
						val = NULL;

					if ((doshare = !strcmp(p, "shared"))) {
						confkeyval *kv;

						/* bail out if we don't do discovery at all */
						kv = findConfKey(_mero_props, "discovery");
						if (kv->ival == 0) {
							/* can't do much */
							len = snprintf(buf2, sizeof(buf2),
									"discovery service is globally disabled, "
									"enable it first\n");
							send(msgsock, buf2, len, 0);
							Mfprintf(_mero_ctlerr, "%s: set: cannot perform "
									"client share request: discovery service "
									"is globally disabled\n", origin);
							continue;
						}

						/* we're going to change the way it is shared,
						 * so remove it now in its old form */
						leavedbS(stats);
					} else if (stats->state == SABdbRunning) {
						Mfprintf(_mero_ctlerr, "%s: cannot set property '%s' "
								"on running database\n", origin, p);
						len = snprintf(buf2, sizeof(buf2),
								"cannot set property '%s' on running "
								"database\n", p);
						send(msgsock, buf2, len, 0);
						msab_freeStatus(&stats);
						continue;
					}

					if ((e = setProp(stats->path, p, val)) != NULL) {
						if (doshare)
							/* reannounce again, there was an error */
							anncdbS(stats);
						Mfprintf(_mero_ctlerr, "%s: setting property failed: "
								"%s\n", origin, e);
						len = snprintf(buf2, sizeof(buf2),
								"%s\n", e);
						send(msgsock, buf2, len, 0);
						free(e);
						msab_freeStatus(&stats);
						continue;
					} else if (doshare) {
						/* announce in new personality */
						anncdbS(stats);
					}

					msab_freeStatus(&stats);

					if (val != NULL) {
						Mfprintf(_mero_ctlout, "%s: set property '%s' for "
								"database '%s' to '%s'\n", origin, p, q, val);
					} else {
						Mfprintf(_mero_ctlout, "%s: inherited property '%s' "
								"for database '%s'\n", origin, p, q);
					}
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send(msgsock, buf2, len, 0);

	/* comands below this point are multi line and hence you can't
	 * combine them, so they disconnect the client afterwards */
				} else if (strcmp(p, "get") == 0) {
					confkeyval *props = getDefaultProps();
					char *pbuf;

					if (strcmp(q, "#defaults") == 0) {
						/* send defaults to client */
						len = snprintf(buf2, sizeof(buf2), "OK\n");
						send(msgsock, buf2, len, 0);
						writePropsBuf(_mero_db_props, &pbuf);
						send(msgsock, pbuf, strlen(pbuf), 0);
						free(props);

						Mfprintf(_mero_ctlout, "%s: served default property "
								"list\n", origin);
						break;
					}

					if ((e = msab_getStatus(&stats, q)) != NULL) {
						len = snprintf(buf2, sizeof(buf2),
								"internal error, please review the logs\n");
						send(msgsock, buf2, len, 0);
						Mfprintf(_mero_ctlerr, "%s: get: msab_getStatus: "
								"%s\n", origin, e);
						freeErr(e);
						break;
					}
					if (stats == NULL) {
						Mfprintf(_mero_ctlerr, "%s: received get signal for "
								"unknown database: %s\n", origin, q);
						len = snprintf(buf2, sizeof(buf2),
								"unknown database: %s\n", q);
						send(msgsock, buf2, len, 0);
						break;
					}

					/* from here we'll always succeed, even if we don't
					 * send anything */
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send(msgsock, buf2, len, 0);
				
					readProps(props, stats->path);
					writePropsBuf(props, &pbuf);
					send(msgsock, pbuf, strlen(pbuf), 0);
					freeConfFile(props);
					free(props);
					msab_freeStatus(&stats);

					Mfprintf(_mero_ctlout, "%s: served property list for "
							"database '%s'\n", origin, q);
					break;
				} else if (strcmp(p, "status") == 0) {
					sabdb *stats;
					sabdb *topdb;
					char *sdb;

					if (strcmp(q, "#all") == 0)
						/* list all */
						q = NULL;

					/* return a list of sabdb structs for our local
					 * databases */
					if ((e = msab_getStatus(&stats, q)) != NULL) {
						len = snprintf(buf2, sizeof(buf2),
								"internal error, please review the logs\n");
						send(msgsock, buf2, len, 0);
						Mfprintf(_mero_ctlerr, "%s: status: msab_getStatus: "
								"%s\n", origin, e);
						freeErr(e);
						break;
					}

					if (stats == NULL && q != NULL) {
						Mfprintf(_mero_ctlerr, "%s: received status signal for "
								"unknown database: %s\n", origin, q);
						len = snprintf(buf2, sizeof(buf2),
								"unknown database: %s\n", q);
						len = snprintf(buf2, sizeof(buf2), "no such database '%s'\n", q);
						send(msgsock, buf2, len, 0);
						break;
					}

					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send(msgsock, buf2, len, 0);

					for (topdb = stats; stats != NULL; stats = stats->next) {
						/* currently never fails (just crashes) */
						msab_serialise(&sdb, stats);
						len = snprintf(buf2, sizeof(buf2),
								"%s\n", sdb);
						send(msgsock, buf2, len, 0);
						free(sdb);
					}

					if (q == NULL) {
						Mfprintf(_mero_ctlout, "%s: served status list\n",
								origin);
					} else {
						Mfprintf(_mero_ctlout, "%s: returned status for "
								"'%s'\n", origin, q);
					}

					msab_freeStatus(&topdb);
					break;
				} else if (strcmp(q, "anelosimus") == 0 &&
						strcmp(p, "eximius") == 0)
				{
					/* return a list of remote databases from our Aranita */
					remotedb rdb;

					pthread_mutex_lock(&_mero_remotedb_lock);

					/* this never fails */
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send(msgsock, buf2, len, 0);

					rdb = _mero_remotedbs;
					while (rdb != NULL) {
						len = snprintf(buf2, sizeof(buf2), "%s\t%s\n",
								rdb->fullname,
								rdb->conn);
						send(msgsock, buf2, len, 0);
						rdb = rdb->next;
					}

					pthread_mutex_unlock(&_mero_remotedb_lock);

					Mfprintf(_mero_ctlout, "%s: served neighbour list\n",
							origin);
					break;
				} else {
					Mfprintf(_mero_ctlerr, "%s: unknown control command: %s\n",
							origin, p);
					len = snprintf(buf2, sizeof(buf2),
							"unknown command: %s\n", p);
					send(msgsock, buf2, len, 0);
				}
			}
		}
		close(msgsock);
	} while (_mero_keep_listening);
	shutdown(usock, SHUT_RDWR);
	close(usock);
	if (tsock > 0) {
		shutdown(tsock, SHUT_RDWR);
		close(tsock);
	}
	Mfprintf(_mero_ctlout, "control channel closed\n");
}

/* vim:set ts=4 sw=4 noexpandtab: */
