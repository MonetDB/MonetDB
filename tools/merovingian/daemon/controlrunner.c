/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#include <inttypes.h>
#include <time.h>
#include <string.h>  /* strerror */
#include <unistd.h>  /* select */
#include <signal.h>
#include <fcntl.h>

#include "monet_options.h"
#include "stream.h"
#include "stream_socket.h"
#include "msabaoth.h"
#include "mcrypt.h"
#include "utils/utils.h"
#include "utils/properties.h"
#include "utils/database.h"
#include "utils/control.h"

#include "merovingian.h"
#include "discoveryrunner.h" /* broadcast, remotedb */
#include "forkmserver.h"
#include "controlrunner.h"
#include "snapshot.h"
#include "multiplex-funnel.h"

#if !defined(HAVE_ACCEPT4) || !defined(SOCK_CLOEXEC)
#define accept4(sockfd, addr, addrlen, flags)	accept(sockfd, addr, addrlen)
#endif

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
	if (!stats->locked && (shared == NULL || strcmp(shared, "no") != 0))
		leavedb(stats->dbname);
	freeConfFile(props);
	free(props);
}

static char _internal_uri_buf[256];
static void
setURI(sabdb *stats)
{
	confkeyval *props = getDefaultProps();
	char *shared;
	readProps(props, stats->path);
	shared = getConfVal(props, "shared");
	if (!stats->locked && (shared == NULL || strcmp(shared, "no") != 0)) {
		snprintf(_internal_uri_buf, sizeof(_internal_uri_buf),
				"mapi:monetdb://%s:%u/%s%s%s",
				_mero_hostname,
				(unsigned int)getConfNum(_mero_props, "port"),
				stats->dbname,
				shared == NULL ? "" : "/",
				shared == NULL ? "" : shared);
		stats->uri = _internal_uri_buf;
	}
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
	if (!stats->locked && (shared == NULL || strcmp(shared, "no") != 0)) {
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
recvWithTimeout(int msgsock, stream *fdin, char *buf, size_t buflen)
{
	int retval;
#ifdef HAVE_POLL
	struct pollfd pfd = (struct pollfd) {.fd = msgsock, .events = POLLIN};

	/* Wait up to 1 second.  If a client doesn't make this, it's too slow */
	retval = poll(&pfd, 1, 1000);
#else
	fd_set fds;
	struct timeval tv;

	FD_ZERO(&fds);
	FD_SET(msgsock, &fds);

	/* Wait up to 1 second.  If a client doesn't make this, it's too slow */
	tv = struct timeval) {.tv_sec = 1};
	retval = select(msgsock + 1, &fds, NULL, NULL, &tv);
#endif
	if (retval <= 0) {
		/* nothing interesting has happened */
		return(-2);
	}

	if (fdin != NULL) {
		ssize_t ret;
		/* stream.h is sooo broken :( */
		memset(buf, '\0', buflen);
		ret = mnstr_read_block(fdin, buf, buflen - 1, 1);
		return(ret >= 0 ? (int)strlen(buf) : mnstr_errnr(fdin) < 0 ? -1 : 0);
	} else {
		return(recv(msgsock, buf, buflen, 0));
	}
}

char
control_authorise(
		const char *host,
		const char *chal,
		const char *algo,
		const char *passwd,
		stream *fout)
{
	char *pwd;

	if (getConfNum(_mero_props, "control") == 0 ||
			getConfVal(_mero_props, "passphrase") == NULL)
	{
		Mfprintf(_mero_ctlout, "%s: remote control disabled\n", host);
		mnstr_printf(fout, "!access denied\n");
		mnstr_flush(fout, MNSTR_FLUSH_DATA);
		return 0;
	}

	pwd = mcrypt_hashPassword(algo,
			getConfVal(_mero_props, "passphrase"), chal);
	if (!pwd) {
		Mfprintf(_mero_ctlout, "%s: Allocation failure during authentication\n", host);
		mnstr_printf(fout, "!allocation failure\n");
		mnstr_flush(fout, MNSTR_FLUSH_DATA);
		return 0;
	}
	if (strcmp(pwd, passwd) != 0) {
		free(pwd);
		Mfprintf(_mero_ctlout, "%s: permission denied "
				"(bad passphrase)\n", host);
		mnstr_printf(fout, "!access denied\n");
		mnstr_flush(fout, MNSTR_FLUSH_DATA);
		return 0;
	}
	free(pwd);

	mnstr_printf(fout, "=OK\n");
	mnstr_flush(fout, MNSTR_FLUSH_DATA);

	return 1;
}

#define send_client(P)								\
	do {											\
		if (fout != NULL) {							\
			mnstr_printf(fout, P "%s", buf2);		\
			mnstr_flush(fout, MNSTR_FLUSH_DATA);						\
		} else {									\
			if (send(msgsock, buf2, len, 0) < 0)	\
				senderror = errno;					\
		}											\
	} while (0)

#define send_list()											\
	do {													\
		len = snprintf(buf2, sizeof(buf2), "OK\n");			\
		if (fout == NULL) {									\
			if (send(msgsock, buf2, strlen(buf2), 0) < 0 ||	\
				send(msgsock, pbuf, strlen(pbuf), 0) < 0)	\
				senderror = errno;							\
		} else {											\
			char *p, *q = pbuf;								\
			mnstr_printf(fout, "=OK\n");					\
			while ((p = strchr(q, '\n')) != NULL) {			\
				*p++ = '\0';								\
				mnstr_printf(fout, "=%s\n", q);				\
				q = p;										\
			}												\
			if (*q != '\0')									\
				mnstr_printf(fout, "=%s\n", q);				\
			mnstr_flush(fout, MNSTR_FLUSH_DATA);								\
		}													\
	} while (0)

static void ctl_handle_client(
		const char *origin,
		int msgsock,
		stream *fdin,
		stream *fout)
{
	/* TODO: this function may actually stall the entire client
	 * handler, so we should probably at some point implement a take
	 * over of the socket such that a separate (controlrunner) thread is
	 * going to handle the traffic and negotiations, instead of the
	 * client thread that just goes inside this program here. */
	char buf[8096];
	char buf2[8096];
	char *p, *q;
	sabdb *stats;
	int pos = 0;
	size_t len;
	err e;
	int senderror = 0;

	while (_mero_keep_listening && !senderror) {
		if (pos == 0) {
			if ((pos = recvWithTimeout(msgsock, fdin, buf, sizeof(buf))) == 0) {
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
				send_client("=");
			} else if (strcmp(p, "start") == 0) {
				err e;
				if ((e = msab_getStatus(&stats, q)) != NULL) {
					len = snprintf(buf2, sizeof(buf2),
							"internal error, please review the logs\n");
					send_client("!");
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
						send_client("!");
						continue;
					}

					if (stats->state == SABdbRunning) {
						Mfprintf(_mero_ctlerr, "%s: received start signal "
								"for already running database: %s\n",
								origin, q);
						len = snprintf(buf2, sizeof(buf2),
								"database is already running: %s\n", q);
						send_client("!");
						msab_freeStatus(&stats);
						continue;
					}

					msab_freeStatus(&stats);
				}
				if ((e = forkMserver(q, &stats, true)) != NO_ERR) {
					Mfprintf(_mero_ctlerr, "%s: failed to fork mserver: "
							"%s\n", origin, getErrMsg(e));
					len = snprintf(buf2, sizeof(buf2),
							"starting '%s' failed: %s\n",
							q, getErrMsg(e));
					send_client("!");
					freeErr(e);
					stats = NULL;
				} else {
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send_client("=");
					Mfprintf(_mero_ctlout, "%s: started '%s'\n",
							origin, q);
				}

				if (stats != NULL)
					msab_freeStatus(&stats);
			} else if (strcmp(p, "stop") == 0 ||
					strcmp(p, "kill") == 0)
			{
				mtype mtype = 0;
				pid_t pid = 0;
				bool terminated = false;

				// First look for something started by ourself.
				pthread_mutex_lock(&_mero_topdp_lock);
				dpair dp = _mero_topdp->next;  /* don't need the console/log */
				for (; dp != NULL; dp = dp->next)
					if (strcmp(dp->dbname, q) == 0) {
						mtype = dp->type;
						pid = dp->pid;
						break;
					}
				pthread_mutex_unlock(&_mero_topdp_lock);
				// after releasing the lock we can no longer access *dp but
				// checking dp's nullity is fine.
				if (dp != NULL && mtype == MEROFUN) {
					multiplexDestroy(q);
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send_client("=");
					break;
				}

				// it wasn't started by us but maybe it was already there
				if (dp == NULL) {
					if ((e = msab_getStatus(&stats, q)) != NULL) {
						len = snprintf(buf2, sizeof(buf2),
								"internal error, please review the logs\n");
						send_client("!");
						Mfprintf(_mero_ctlerr, "%s: start: msab_getStatus: "
								"%s\n", origin, e);
						freeErr(e);
						continue;
					}
					if (stats != NULL) {
						pid = stats->pid;
						mtype = MERODB;
						msab_freeStatus(&stats);
					}
				}
				// At this point pid may have been set from a dpair or from msab_getStatus()
				if (pid <= 0) {
					Mfprintf(_mero_ctlerr, "%s: received stop signal for "
							"non running database: %s\n", origin, q);
					len = snprintf(buf2, sizeof(buf2),
							"database is not running: %s\n", q);
					send_client("!");
					break;
				}

				// Kill it appropriately
				if (strcmp(p, "stop") == 0) {
					/* make an attempt to shut down the profiler first. */
					if ((e = shutdown_profiler(q, &stats)) != NULL) {
						free(e);
					} else if (stats != NULL)
						msab_freeStatus(&stats);
					/* then kill it */
					if (dp)
						pthread_mutex_lock(&dp->fork_lock);
					terminated = terminateProcess(q, pid, mtype);
					if (dp)
						pthread_mutex_unlock(&dp->fork_lock);
					Mfprintf(_mero_ctlout, "%s: stopped "
							"database '%s'\n", origin, q);
				} else {
					terminated = kill(pid, SIGKILL) == 0;
					Mfprintf(_mero_ctlout, "%s: killed "
							"database '%s'\n", origin, q);
				}
				if (terminated) {
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send_client("=");
				} else {
					Mfprintf(_mero_ctlerr, "%s: received stop signal for "
							"non running database: %s\n", origin, q);
					len = snprintf(buf2, sizeof(buf2),
							"database is not running: %s\n", q);
					send_client("!");
					break;
				}
			} else if (strcmp(p, "create") == 0 ||
					strncmp(p, "create password=", strlen("create password=")) == 0) {
				err e;

				p += strlen("create");
				if (*p == ' ')
					p += strlen(" password=");

				e = db_create(q);
				if (e != NO_ERR) {
					Mfprintf(_mero_ctlerr, "%s: failed to create "
							"database '%s': %s\n", origin, q, getErrMsg(e));
					len = snprintf(buf2, sizeof(buf2),
							"%s\n", getErrMsg(e));
					send_client("!");
					free(e);
				} else {
					if (*p != '\0') {
						pid_t child;
						int pipes[2];
						if (pipe(pipes) == -1) {
							Mfprintf(_mero_ctlerr, "%s: creating pipe failed\n",
									 origin);
						} else if ((child = fork()) == 0) {
							/* this is the child process; exit non-zero
							 * on failure */
							char *err;
							char *sadbfarm;
							char buf3[8092];

							close(pipes[1]);
							dup2(pipes[0], 0);
							close(pipes[0]);

							if ((err = msab_getDBfarm(&sadbfarm)) != NULL) {
								Mfprintf(_mero_ctlerr,
										 "%s: internal error: %s\n",
										 origin, err);
								exit(EXIT_FAILURE);
							}
							snprintf(buf2, sizeof(buf2),
									 "monet_vault_key=%s/%s/.vaultkey",
									 sadbfarm, q);
							snprintf(buf3, sizeof(buf3), "--dbpath=%s/%s",
									 sadbfarm, q);
							free(sadbfarm);
							execl(_mero_mserver,
								  _mero_mserver,
								  "--set",
								  buf2,
								  buf3,
								  "--read-password-initialize-and-exit",
								  NULL);
							Mfprintf(_mero_ctlerr,
									 "%s: cannot start mserver5\n", origin);
							exit(EXIT_FAILURE);
						} else if (child > 0) {
							/* this is the parent process */
							close(pipes[0]);
							bool error = write(pipes[1], p, strlen(p)) < 0 || write(pipes[1], "\n", 1) < 0;
							close(pipes[1]);
							/* wait for the child to finish */
							int status;
							waitpid(child, &status, 0);
							if (error || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
								Mfprintf(_mero_ctlerr,
										 "%s: initialization of database '%s' failed\n",
										 origin, q);
								len = snprintf(buf2, sizeof(buf2),
											   "initialization of database '%s' failed\n", q);
								send_client("!");
								continue;
							}
							e = db_release(q);
							if (e != NO_ERR) {
								Mfprintf(_mero_ctlerr,
										 "%s: could not release database '%s': %sd\n",
										 origin, q, e);
								free(e);
							}
						} else {
							close(pipes[0]);
							close(pipes[1]);
							Mfprintf(_mero_ctlerr, "%s: forking failed\n",
									 origin);
						}
					}

					Mfprintf(_mero_ctlout, "%s: created database '%s'\n",
							origin, q);
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send_client("=");
				}
			} else if (strncmp(p, "create mfunnel=", strlen("create mfunnel=")) == 0) {
				err e = NO_ERR;
				char *r;

				/* check mfunnel definition for correctness first */
				p += strlen("create mfunnel=");
				/* user+pass@pattern,user+pass@pattern,... */
				r = p;
				do {
					for (; *r != '\0' && *r != '+'; r++)
						;
					if (*r == '\0' || *(r - 1) == ',' || (*(r - 1) == '=')) {
						e = "missing user";
						break;
					}
					for (r++; *r != '\0' && *r != '@'; r++)
						;
					if (*r == '\0' || *(r - 1) == '+') {
						e = "missing password";
						break;
					}
					for (r++; *r != '\0' && *r != ','; r++)
						;
					if (*(r - 1) == '@') {
						e = "missing pattern";
						break;
					}
					if (*r == '\0')
						break;
					r++;
				} while(1);
				if (e != NO_ERR) {
					Mfprintf(_mero_ctlerr, "%s: invalid multiplex-funnel "
							"specification '%s': %s at char %zu\n",
							origin, p, getErrMsg(e), (size_t)(r - p));
					len = snprintf(buf2, sizeof(buf2),
							"invalid pattern: %s\n", getErrMsg(e));
					send_client("!");
				} else if ((e = db_create(q)) != NO_ERR) {
					Mfprintf(_mero_ctlerr, "%s: failed to create "
							"multiplex-funnel '%s': %s\n",
							origin, q, getErrMsg(e));
					len = snprintf(buf2, sizeof(buf2),
							"%s\n", getErrMsg(e));
					send_client("!");
					free(e);
				} else {
					confkeyval *props = getDefaultProps();
					confkeyval *kv;
					char *dbfarm;
					/* write the funnel config */
					kv = findConfKey(props, "type");
					setConfVal(kv, "mfunnel");
					kv = findConfKey(props, "mfunnel");
					setConfVal(kv, p);
					if ((e = msab_getDBfarm(&dbfarm)) != NULL) {
						Mfprintf(_mero_ctlerr, "%s: failed to retrieve "
								"dbfarm: %s\n", origin, e);
						free(e);
						/* try, hopefully this succeeds */
						if ((e = db_destroy(q)) != NO_ERR) {
							Mfprintf(_mero_ctlerr, "%s: could not destroy: "
									"%s\n", origin, getErrMsg(e));
							free(e);
						}
						len = snprintf(buf2, sizeof(buf2),
								"failed to prepare multiplex-funnel\n");
						send_client("!");
					} else {
						snprintf(buf2, sizeof(buf2), "%s/%s", dbfarm, q);
						free(dbfarm);
						writeProps(props, buf2);
						Mfprintf(_mero_ctlout,
								"%s: created multiplex-funnel '%s'\n",
								origin, q);
						len = snprintf(buf2, sizeof(buf2), "OK\n");
						send_client("!");
					}
					freeConfFile(props);
					free(props);
				}
			} else if (strcmp(p, "destroy") == 0) {
				err e = db_destroy(q);
				if (e != NO_ERR) {
					Mfprintf(_mero_ctlerr, "%s: failed to destroy "
							"database '%s': %s\n", origin, q, getErrMsg(e));
					len = snprintf(buf2, sizeof(buf2),
							"%s\n", getErrMsg(e));
					send_client("!");
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
					send_client("=");
				}
			} else if (strcmp(p, "lock") == 0) {
				char *e = db_lock(q);
				if (e != NULL) {
					Mfprintf(_mero_ctlerr, "%s: failed to lock "
							"database '%s': %s\n", origin, q, getErrMsg(e));
					len = snprintf(buf2, sizeof(buf2),
							"%s\n", getErrMsg(e));
					send_client("!");
					free(e);
				} else {
					/* we go under maintenance, unshare it, take
					 * spam if database happened to be unshared "for
					 * love" */
					leavedb(q);
					Mfprintf(_mero_ctlout, "%s: locked database '%s'\n",
							origin, q);
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send_client("=");
				}
			} else if (strcmp(p, "release") == 0) {
				char *e = db_release(q);
				if (e != NULL) {
					Mfprintf(_mero_ctlerr, "%s: failed to release "
							"database '%s': %s\n", origin, q, getErrMsg(e));
					len = snprintf(buf2, sizeof(buf2),
							"%s\n", getErrMsg(e));
					send_client("!");
					free(e);
				} else {
					/* announce database, but need to do it the
					 * right way so we don't accidentially announce
					 * an unshared database */
					if ((e = msab_getStatus(&stats, q)) != NULL) {
						len = snprintf(buf2, sizeof(buf2),
								"internal error, please review the logs\n");
						send_client("!");
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
					send_client("=");
				}
			} else if (strncmp(p, "profilerstart", strlen("profilerstart")) == 0) {
				char *log_path = NULL;
				char *e = fork_profiler(q, &stats, &log_path);
				if (e != NULL) {
					Mfprintf(_mero_ctlerr, "%s: failed to start the profiler "
							 "database '%s': %s\n", origin, q, getErrMsg(e));
					len = snprintf(buf2, sizeof(buf2),
								   "%s\n", getErrMsg(e));
					send_client("!");
					freeErr(e);
				} else {
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send_client("=");
					Mfprintf(_mero_ctlout, "%s: started profiler for '%s'\n",
							 origin, q);
					Mfprintf(_mero_ctlout, "%s: logs at: %s\n",
							 origin, log_path);
				}
				msab_freeStatus(&stats);
				if (log_path)
					free(log_path);
			}  else if (strncmp(p, "profilerstop", strlen("profilerstop")) == 0) {
				char *e = shutdown_profiler(q, &stats);
				if (e != NULL) {
					Mfprintf(_mero_ctlerr, "%s: failed to shutdown the profiler "
							 "database '%s': %s\n", origin, q, getErrMsg(e));
					len = snprintf(buf2, sizeof(buf2),
								   "%s\n", getErrMsg(e));
					send_client("!");
					freeErr(e);
				} else {
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send_client("=");
					Mfprintf(_mero_ctlout, "%s: profiler shut down for '%s'\n",
							 origin, q);
				}
				msab_freeStatus(&stats);
			} else if (strncmp(p, "snapshot create adhoc ", strlen("snapshot create adhoc ")) == 0) {
				char *dest = p + strlen("snapshot create adhoc ");
				Mfprintf(_mero_ctlout, "Start snapshot of database '%s' to file '%s'\n", q, dest);
				char *e = snapshot_database_to(q, dest);
				if (e != NULL) {
					Mfprintf(_mero_ctlerr, "%s: snapshot database '%s' to %s failed: %s",
						origin, q, dest, getErrMsg(e));
					len = snprintf(buf2, sizeof(buf2), "%s\n", getErrMsg(e));
					send_client("!");
					freeErr(e);
				} else {
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send_client("=");
					Mfprintf(_mero_ctlout, "%s: completed snapshot of database '%s' to '%s'\n",
						origin, q, dest);
				}
			} else if (strcmp(p, "snapshot create automatic") == 0) {
				char *dest = NULL;
				char *e = snapshot_default_filename(&dest, q);
				if (e != NULL) {
					Mfprintf(_mero_ctlerr, "%s: snapshot database '%s': %s",
						origin, q, getErrMsg(e));
					len = snprintf(buf2, sizeof(buf2), "%s\n", getErrMsg(e));
					send_client("!");
					freeErr(e);
				} else {
					Mfprintf(_mero_ctlout, "Start snapshot of database '%s' to file '%s'\n", q, dest);
					e = snapshot_database_to(q, dest);
					if (e != NULL) {
						Mfprintf(_mero_ctlerr, "%s: snapshot database '%s' to %s failed: %s",
							origin, q, dest, getErrMsg(e));
						len = snprintf(buf2, sizeof(buf2), "%s\n", getErrMsg(e));
						send_client("!");
						freeErr(e);
					} else {
						len = snprintf(buf2, sizeof(buf2), "OK\n");
						send_client("=");
						Mfprintf(_mero_ctlout, "%s: completed snapshot of database '%s' to '%s'\n",
							origin, q, dest);
					}
					free(dest);
				}
			} else if (strcmp(p, "snapshot stream") == 0) {

				Mfprintf(_mero_ctlout, "Start streaming snapshot of database '%s'\n", q);

				stream *wrapper = NULL;
				stream *bs = NULL;
				stream *s = NULL; // aliases either bs or fout
				do {
					if (fout) {
						if (!isa_block_stream(fout)) {
							e = newErr("internal error: expected fout to be a block stream");
							break;
						}
						s = fout;
					} else {
						wrapper = socket_wstream(msgsock, "sockwrapper");
						if (!wrapper) {
							e = newErr("internal error: could not create sock wrapper");
							break;
						}
						bs = block_stream(wrapper);
						if (!bs) {
							e = newErr("internal error: could not wrap block_stream");
							break;
						}
						wrapper = NULL; // will be cleanup through bs
						s = bs;
						//
					}
					e = snapshot_database_stream(q, s);
					mnstr_flush(s, MNSTR_FLUSH_DATA);
				} while (0);
				if (bs)
					mnstr_destroy(bs);
				if (wrapper)
					mnstr_destroy(wrapper);
				if (e != NULL) {
					Mfprintf(_mero_ctlerr, "%s: streaming snapshot database '%s' failed: %s",
						origin, q, getErrMsg(e));
					len = snprintf(buf2, sizeof(buf2), "%s\n", getErrMsg(e));
					send_client("!");
					freeErr(e);
				} else {
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send_client("=");
					Mfprintf(_mero_ctlout, "%s: completed streaming snapshot of database '%s'\n",
						origin, q);
				}
				break;
			} else if (strncmp(p, "snapshot restore adhoc ", strlen("snapshot restore adhoc ")) == 0) {
				char *source = p + strlen("snapshot restore adhoc ");
				Mfprintf(_mero_ctlout, "Start restore snapshot of database '%s' from file '%s'\n", q, source);
				char *e = snapshot_restore_from(q, source);
				if (e != NULL) {
					Mfprintf(_mero_ctlerr, "%s: restore  database '%s' from snapshot %s failed: %s",
						origin, q, source, getErrMsg(e));
					len = snprintf(buf2, sizeof(buf2), "%s\n", getErrMsg(e));
					send_client("!");
					freeErr(e);
				} else {
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send_client("=");
					Mfprintf(_mero_ctlout, "%s: restored database '%s' from snapshot '%s'\n",
						origin, q, source);
				}
			} else if (strncmp(p, "snapshot destroy ", strlen("snapshot destroy ")) == 0) {
				char *path = p + strlen("snapshot destroy ");
				Mfprintf(_mero_ctlout, "%s: drop snapshot '%s'\n", origin, path);
				char *e = snapshot_destroy_file(path);
				if (e != NULL) {
					Mfprintf(_mero_ctlerr, "%s: drop snapshot '%s' failed: %s\n", origin, path, e);
					len = snprintf(buf2, sizeof(buf2), "%s\n", e);
					send_client("!");
					freeErr(e);
				} else {
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send_client("=");
				}
			} else if (strcmp(p, "snapshot list") == 0) {
				Mfprintf(_mero_ctlout, "Start snapshot list\n");
				int nsnaps = 0;
				struct snapshot *snaps = NULL;
				char *e = snapshot_list(&nsnaps, &snaps);
				if (e != NULL) {
					Mfprintf(_mero_ctlerr, "%s: snapshot list failed: %s", origin, getErrMsg(e));
					len = snprintf(buf2, sizeof(buf2), "%s\n", getErrMsg(e));
					send_client("!");
					freeErr(e);
					break; // <================== DISCONNECT!!!!
				}
				len = snprintf(buf2, sizeof(buf2), "OK1\n");
				send_client("=");
				for (int i = 0; i < nsnaps; i++) {
					struct tm tm = { 0 };
					char datebuf[100];
					struct snapshot *snap = &snaps[i];
					gmtime_r(&snap->time, &tm);
					strftime(datebuf, sizeof(datebuf), "%Y%m%dT%H%M%S", &tm);
					len = snprintf(buf2, sizeof(buf2), "%s %" PRIu64 " %s %s\n",
						datebuf,
						(uint64_t)snap->size,
						snap->dbname,
						snap->path != NULL ? snap->path : "");
					send_client("=");
				}
				free_snapshots(snaps, nsnaps);
				Mfprintf(_mero_ctlout, "Returned %d snapshots\n", nsnaps);
				break; // <==================== DISCONNECT!!!!
			} else if (strncmp(p, "name=", strlen("name=")) == 0) {
				char *e;

				p += strlen("name=");
				e = db_rename(q, p);
				if (e != NULL) {
					Mfprintf(_mero_ctlerr, "%s: %s\n", origin, e);
					len = snprintf(buf2, sizeof(buf2), "%s\n", e);
					send_client("!");
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
					send_client("=");
				}
			} else if (strchr(p, '=') != NULL) { /* set */
				char *val;
				bool doshare = false;

				if ((e = msab_getStatus(&stats, q)) != NULL) {
					len = snprintf(buf2, sizeof(buf2),
							"internal error, please review the logs\n");
					send_client("!");
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
					send_client("!");
					continue;
				}

				val = strchr(p, '=');
				assert(val != NULL); /* see above */
				*val++ = '\0';
				if (*val == '\0')
					val = NULL;

				if ((doshare = strcmp(p, "shared") == 0)) {
					/* bail out if we don't do discovery at all */
					if (getConfNum(_mero_props, "discovery") == 0) {
						len = snprintf(buf2, sizeof(buf2),
								"discovery service is globally disabled, "
								"enable it first\n");
						send_client("!");
						Mfprintf(_mero_ctlerr, "%s: set: cannot perform "
								"client share request: discovery service "
								"is globally disabled\n", origin);
						msab_freeStatus(&stats);
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
					send_client("!");
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
					send_client("!");
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
				send_client("=");

	/* comands below this point are multi line and hence you can't
	 * combine them, so they disconnect the client afterwards */

			} else if (strcmp(p, "version") == 0) {
				len = snprintf(buf2, sizeof(buf2), "OK\n");
				send_client("=");
				len = snprintf(buf2, sizeof(buf2), "%s (%s)\n",
							   MONETDB_VERSION,
#ifdef MONETDB_RELEASE
							   MONETDB_RELEASE
#else
							   "unreleased"
#endif
					);
				send_client("=");
				break;
			} else if (strcmp(p, "mserver") == 0) {
				len = snprintf(buf2, sizeof(buf2), "OK\n");
				send_client("=");
				len = snprintf(buf2, sizeof(buf2), "%s\n", _mero_mserver);
				send_client("=");
				break;
			} else if (strcmp(p, "get") == 0) {
				confkeyval *props = getDefaultProps();
				char *pbuf;

				if (strcmp(q, "#defaults") == 0) {
					/* send defaults to client */
					writePropsBuf(_mero_db_props, &pbuf);
					send_list();

					Mfprintf(_mero_ctlout, "%s: served default property "
							"list\n", origin);
					freeConfFile(props);
					free(props);
					free(pbuf);
					break;
				}

				if ((e = msab_getStatus(&stats, q)) != NULL) {
					len = snprintf(buf2, sizeof(buf2),
							"internal error, please review the logs\n");
					send_client("!");
					Mfprintf(_mero_ctlerr, "%s: get: msab_getStatus: "
							"%s\n", origin, e);
					freeErr(e);
					freeConfFile(props);
					free(props);
					break;
				}
				if (stats == NULL) {
					Mfprintf(_mero_ctlerr, "%s: received get signal for "
							"unknown database: %s\n", origin, q);
					len = snprintf(buf2, sizeof(buf2),
							"unknown database: %s\n", q);
					send_client("!");
					freeConfFile(props);
					free(props);
					break;
				}

				/* from here we'll always succeed, even if we don't
				 * send anything */
				readProps(props, stats->path);
				writePropsBuf(props, &pbuf);
				send_list();
				freeConfFile(props);
				free(props);
				free(pbuf);
				msab_freeStatus(&stats);

				Mfprintf(_mero_ctlout, "%s: served property list for "
						"database '%s'\n", origin, q);
				break;
			} else if (strcmp(p, "status") == 0) {
				sabdb *stats;
				sabdb *topdb;
				char *sdb = NULL;

				if (strcmp(q, "#all") == 0)
					/* list all */
					q = NULL;

				/* return a list of sabdb structs for our local
				 * databases */
				if ((e = msab_getStatus(&stats, q)) != NULL) {
					len = snprintf(buf2, sizeof(buf2),
							"internal error, please review the logs\n");
					send_client("!");
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
					send_client("!");
					break;
				}

				len = snprintf(buf2, sizeof(buf2), "OK\n");
				if (fout == NULL) {
					if (send(msgsock, buf2, len, 0) < 0)
						senderror = errno;
				} else {
					mnstr_printf(fout, "=%s", buf2);
				}

				for (topdb = stats; stats != NULL; stats = stats->next) {
					/* set uri */
					setURI(stats);
					/* currently never fails (just crashes) */
					if ((e = msab_serialise(&sdb, stats)) != NULL)
						break;
					stats->uri = NULL;
					len = snprintf(buf2, sizeof(buf2), "%s\n", sdb);
					if (fout == NULL) {
						if (send(msgsock, buf2, len, 0) < 0)
							senderror = errno;
					} else {
						mnstr_printf(fout, "=%s", buf2);
					}
					free(sdb);
				}
				if (e != NULL) {
					len = snprintf(buf2, sizeof(buf2),
							"internal error, please review the logs\n");
					send_client("!");
					Mfprintf(_mero_ctlerr, "%s: status: msab_getStatus: "
							"%s\n", origin, e);
					msab_freeStatus(&topdb);
					freeErr(e);
					break;
				}

				if (fout != NULL)
					mnstr_flush(fout, MNSTR_FLUSH_DATA);

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
				if (fout == NULL) {
					if (send(msgsock, buf2, len, 0) < 0)
						senderror = errno;
				} else {
					mnstr_printf(fout, "=%s", buf2);
				}

				rdb = _mero_remotedbs;
				while (rdb != NULL && !senderror) {
					len = snprintf(buf2, sizeof(buf2), "%s\t%s\n",
							rdb->fullname,
							rdb->conn);
					if (fout == NULL) {
						if (send(msgsock, buf2, len, 0) < 0)
							senderror = errno;
					} else {
						mnstr_printf(fout, "=%s", buf2);
					}
					rdb = rdb->next;
				}

				if (fout != NULL)
					mnstr_flush(fout, MNSTR_FLUSH_DATA);

				pthread_mutex_unlock(&_mero_remotedb_lock);

				Mfprintf(_mero_ctlout, "%s: served neighbour list\n",
						origin);
				break;
			} else {
				Mfprintf(_mero_ctlerr, "%s: unknown control command: %s\n",
						origin, p);
				len = snprintf(buf2, sizeof(buf2),
						"unknown command: %s\n", p);
				send_client("!");
				break;
			}
		}
	}
	if (senderror)
		Mfprintf(_mero_ctlerr, "%s: error sending to control "
				 "channel: %s\n", origin, strerror(senderror));
}

void
control_handleclient(const char *host, int sock, stream *fdin, stream *fout)
{
	ctl_handle_client(host, sock, fdin, fout);
}

static void *
handle_client(void *p)
{
	int msgsock = * (int *) p;

	free(p);
	ctl_handle_client("(local)", msgsock, NULL, NULL);
	shutdown(msgsock, SHUT_RDWR);
	closesocket(msgsock);
	return NULL;
}

void *
controlRunner(void *d)
{
	int usock = *(int *)d;
	int retval;
#ifdef HAVE_POLL
	struct pollfd pfd;
#else
	fd_set fds;
	struct timeval tv;
#endif
	int msgsock;
	pthread_t tid;
	int *p;

	do {
		if ((p = malloc(sizeof(int))) == NULL) {
			Mfprintf(_mero_ctlerr, "malloc failed");
			break;
		}
		/* limit waiting time in order to check whether we need to exit */
#ifdef HAVE_POLL
		pfd = (struct pollfd) {.fd = usock, .events = POLLIN};
		retval = poll(&pfd, 1, 1000);
#else
		FD_ZERO(&fds);
		FD_SET(usock, &fds);

		tv = (struct timeval) {.tv_sec = 1};
		retval = select(usock + 1, &fds, NULL, NULL, &tv);
#endif
		if (retval == 0) {
			/* nothing interesting has happened */
			free(p);
			continue;
		}
		if (retval == -1) {
			free(p);
			continue;
		}

#ifdef HAVE_POLL
		if ((pfd.revents & POLLIN) == 0) {
			free(p);
			continue;
		}
#else
		if (!FD_ISSET(usock, &fds)) {
			free(p);
			continue;
		}
#endif

		if ((msgsock = accept4(usock, NULL, NULL, SOCK_CLOEXEC)) == -1) {
			free(p);
			if (_mero_keep_listening == 0)
				break;
			if (errno != EINTR) {
				Mfprintf(_mero_ctlerr, "error during accept: %s",
						strerror(errno));
			}
			continue;
		}
#if defined(HAVE_FCNTL) && (!defined(SOCK_CLOEXEC) || !defined(HAVE_ACCEPT4))
		(void) fcntl(msgsock, F_SETFD, FD_CLOEXEC);
#endif

		*p = msgsock;
		if (pthread_create(&tid, NULL, handle_client, p) != 0) {
			closesocket(msgsock);
			free(p);
		} else
			pthread_detach(tid);
	} while (_mero_keep_listening);
	shutdown(usock, SHUT_RDWR);
	closesocket(usock);
	Mfprintf(_mero_ctlout, "control channel closed\n");
	return NULL;
}

/* vim:set ts=4 sw=4 noexpandtab: */
