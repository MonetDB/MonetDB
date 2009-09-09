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
controlRunner(void *d)
{
	int sock = *(int *)d;
	char buf[256];
	char buf2[256];
	char *p, *q;
	sabdb *stats;
	int pos = 0;
	int retval;
	fd_set fds;
	struct timeval tv;
	int msgsock;
	size_t len;
	err e;

	do {
		FD_ZERO(&fds);
		FD_SET(sock, &fds);

		/* Wait up to 5 seconds. */
		tv.tv_sec = 5;
		tv.tv_usec = 0;
		retval = select(sock + 1, &fds, NULL, NULL, &tv);
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
				goto error;
			}
			continue;
		}
		if (FD_ISSET(sock, &fds)) {
			if ((msgsock = accept(sock, (SOCKPTR) 0, (socklen_t *) 0)) < 0) {
				if (_mero_keep_listening == 0)
					break;
				if (errno != EINTR) {
					e = newErr("control runner: error during accept: %s",
							strerror(errno));
					goto error;
				}
				continue;
			}
		} else
			continue;

		while (_mero_keep_listening) {
			if (pos == 0) {
				if ((pos = recv(msgsock, buf, sizeof(buf), 0)) == 0) {
					/* EOF */
					break;
				} else if (pos == -1) {
					/* we got interrupted ... so what? */
					if (errno == EINTR) {
						pos = 0;
						continue;
					}
					/* hmmm error ... give up */
					Mfprintf(_mero_ctlerr, "error reading from control "
							"channel: %s\n", strerror(errno));
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
				Mfprintf(_mero_ctlerr, "skipping garbage on control "
						"channel: %s\n", buf);
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
				Mfprintf(_mero_ctlerr, "malformed control signal: %s\n", q);
			} else {
				*p++ = '\0';
				if (strcmp(p, "start") == 0) {
					err e;
					Mfprintf(_mero_ctlout, "starting database '%s'\n", q);
					if ((e = forkMserver(q, &stats, 1)) != NO_ERR) {
						Mfprintf(_mero_ctlerr, "failed to fork mserver: %s\n",
								getErrMsg(e));
						len = snprintf(buf2, sizeof(buf2),
								"starting '%s' failed: %s\n",
								q, getErrMsg(e));
						send(msgsock, buf2, len, 0);
						freeErr(e);
						stats = NULL;
					} else {
						len = snprintf(buf2, sizeof(buf2), "OK\n");
						send(msgsock, buf2, len, 0);
					}

					if (stats != NULL)
						SABAOTHfreeStatus(&stats);
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
								Mfprintf(_mero_ctlout, "stopping "
										"database '%s'\n", q);
								terminateProcess(dp);
							} else {
								Mfprintf(_mero_ctlout, "killing "
										"database '%s'\n", q);
								kill(dp->pid, SIGKILL);
							}
							len = snprintf(buf2, sizeof(buf2), "OK\n");
							send(msgsock, buf2, len, 0);
							break;
						}
						dp = dp->next;
					}
					if (dp == NULL) {
						Mfprintf(_mero_ctlerr, "received stop signal for "
								"database not under merovingian control: %s\n",
								q);
						len = snprintf(buf2, sizeof(buf2),
								"'%s' is not controlled by merovingian\n", q);
						send(msgsock, buf2, len, 0);
					}
					pthread_mutex_unlock(&_mero_topdp_lock);
				} else if (strcmp(p, "create") == 0) {
					err e = db_create(q);
					if (e != NO_ERR) {
						Mfprintf(_mero_ctlerr, "failed to create "
								"database '%s': %s\n", q, getErrMsg(e));
						len = snprintf(buf2, sizeof(buf2),
								"%s\n", getErrMsg(e));
						send(msgsock, buf2, len, 0);
						free(e);
					} else {
						Mfprintf(_mero_ctlout, "created database '%s'\n", q);
						len = snprintf(buf2, sizeof(buf2), "OK\n");
						send(msgsock, buf2, len, 0);
					}
				} else if (strcmp(p, "destroy") == 0) {
					err e = db_destroy(q);
					if (e != NO_ERR) {
						Mfprintf(_mero_ctlerr, "failed to destroy "
								"database '%s': %s\n", q, getErrMsg(e));
						len = snprintf(buf2, sizeof(buf2),
								"%s\n", getErrMsg(e));
						send(msgsock, buf2, len, 0);
						free(e);
					} else {
						/* we can leave without tag, will remove all,
						 * generates an "leave request for unknown
						 * database" if not shared (e.g. when under
						 * maintenance) */
						snprintf(buf2, sizeof(buf2),
								"LEAV %s mapi:monetdb://%s:%hu/",
								q, _mero_hostname, _mero_port);
						broadcast(buf2);
						Mfprintf(_mero_ctlout, "destroyed database '%s'\n", q);
						len = snprintf(buf2, sizeof(buf2), "OK\n");
						send(msgsock, buf2, len, 0);
					}
				} else if (strncmp(p, "share=", strlen("share=")) == 0) {
					sabdb *stats;
					sabdb *topdb;
					err e;
					confkeyval *kv, *props = getDefaultProps();
					char *value;

					kv = findConfKey(_mero_props, "shared");
					if (strcmp(kv->val, "no") == 0) {
						/* can't do much */
						len = snprintf(buf2, sizeof(buf2),
								"discovery service is globally disabled, "
								"enable it first\n");
						send(msgsock, buf2, len, 0);
						Mfprintf(_mero_ctlerr, "share: cannot perform client "
								"share request: discovery service is globally "
								"disabled in %s\n", _mero_conffile);
						continue;
					}

					if ((e = SABAOTHgetStatus(&stats, NULL)) != MAL_SUCCEED) {
						len = snprintf(buf2, sizeof(buf2),
								"internal error, please review the logs\n");
						send(msgsock, buf2, len, 0);
						Mfprintf(_mero_ctlerr, "share: SABAOTHgetStatus: "
								"%s\n", e);
						freeErr(e);
						continue;
					}

					/* check if tag matches [A-Za-z0-9./]+ */
					p += strlen("share=");
					value = p;
					while (*value != '\0') {
						if (!(
									(*value >= 'A' && *value <= 'Z') ||
									(*value >= 'a' && *value <= 'z') ||
									(*value >= '0' && *value <= '9') ||
									(*value == '.' || *value == '/')
							 ))
						{
							len = snprintf(buf2, sizeof(buf2),
									"invalid character '%c' at %d "
									"in tag name '%s'\n",
									*value, (int)(value - p), p);
							send(msgsock, buf2, len, 0);
							buf2[len] = '\0';
							Mfprintf(_mero_ctlerr, "set: %s", buf2);
							value = NULL;
							break;
						}
						value++;
					}
					if (value == NULL)
						continue;

					topdb = stats;
					while (stats != NULL) {
						if (strcmp(q, stats->dbname) == 0) {
							readProps(props, stats->path);
							kv = findConfKey(props, "shared");
							if (kv->val == NULL || strcmp(kv->val, "no") != 0) {
								/* we can leave without tag, will remove all */
								snprintf(buf2, sizeof(buf2),
										"LEAV %s mapi:monetdb://%s:%hu/",
										stats->dbname, _mero_hostname,
										_mero_port);
								broadcast(buf2);
							}
							if (kv->val != NULL) {
								GDKfree(kv->val);
								kv->val = NULL;
							}
							/* the prophecy:
							 * <empty> inherit (bit useless)
							 * yes     share without tag
							 * no      don't share
							 * *       share with * as tag
							 */
							if (*p == '\0') {
								/* empty, inherit (e.g. remove local opt),
								 * but keep enabled, otherwise we never
								 * came here */
							} else if (strcmp(p, "yes") == 0) {
								/* make this share an empty tag */
								*p = '\0';
								kv->val = GDKstrdup("yes");
							} else if (strcmp(p, "no") == 0) {
								/* do not share (any more) */
								kv->val = GDKstrdup("no");
								writeProps(props, stats->path);
								break;
							} else {
								/* tag, include '/' for easy life
								 * afterwards */
								*--p = '/';
								kv->val = GDKstrdup(p + 1);
							}

							snprintf(buf2, sizeof(buf2),
									"ANNC %s%s mapi:monetdb://%s:%hu/ %d",
									stats->dbname, p, _mero_hostname,
									_mero_port, _mero_discoveryttl + 60);
							broadcast(buf2);
							writeProps(props, stats->path);
							Mfprintf(_mero_ctlout, "shared database '%s' "
									"as '%s%s'\n", stats->dbname,
									stats->dbname, p);
							break;
						}
						stats = stats->next;
					}
					if (stats == NULL) {
						Mfprintf(_mero_ctlerr, "received share signal for "
								"database not under merovingian control: %s\n",
								q);
						len = snprintf(buf2, sizeof(buf2),
								"'%s' is not controlled by merovingian\n", q);
						send(msgsock, buf2, len, 0);
					}
					len = snprintf(buf2, sizeof(buf2), "OK\n");
					send(msgsock, buf2, len, 0);
					SABAOTHfreeStatus(&topdb);
					freeConfFile(props);
					GDKfree(props);
				} else if (strcmp(q, "anelosimus") == 0 &&
						strcmp(p, "eximius") == 0)
				{
					/* return a list of remote databases from our Aranita */
					remotedb rdb;

					pthread_mutex_lock(&_mero_remotedb_lock);

					rdb = _mero_remotedbs;
					while (rdb != NULL) {
						len = snprintf(buf2, sizeof(buf2), "%s%s%s\t%s\n",
								rdb->dbname,
								rdb->tag == NULL ? "" : "/",
								rdb->tag == NULL ? "" : rdb->tag,
								rdb->conn);
						send(msgsock, buf2, len, 0);
						rdb = rdb->next;
					}

					pthread_mutex_unlock(&_mero_remotedb_lock);

					Mfprintf(_mero_ctlout, "served neighbour list\n");

					/* because this command is multi line, you can't
					 * combine it, disconnect the client */
					break;
				} else {
					Mfprintf(_mero_ctlerr, "unknown control command: %s\n", p);
					len = snprintf(buf2, sizeof(buf2),
							"unknown command: %s\n", p);
					send(msgsock, buf2, len, 0);
				}
			}
		}
		close(msgsock);
		continue;

error:
		Mfprintf(stderr, "%s\n", e);
	} while (_mero_keep_listening);
	shutdown(sock, SHUT_RDWR);
	close(sock);
	Mfprintf(stdout, "control channel closed\n");
}

