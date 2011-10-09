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
#include <string.h>  /* strerror, strchr, strcmp */
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_SYS_UIO_H
# include <sys/uio.h>
#endif

#include <msabaoth.h>
#include <mcrypt.h>
#include <stream.h>
#include <stream_socket.h>
#include <utils/utils.h> /* freeConfFile */
#include <utils/properties.h> /* readProps */

#include "merovingian.h"
#include "forkmserver.h"
#include "proxy.h"
#include "multiplex-funnel.h"
#include "controlrunner.h"
#include "client.h"

static err
handleClient(int sock, char isusock)
{
	stream *fdin, *fout;
	char buf[8096];
	char chal[32];
	char *user = NULL, *algo = NULL, *passwd = NULL, *lang = NULL;
	char *database = NULL, *s;
	char dbmod[64];
	char host[128];
	sabdb *top = NULL;
	sabdb *stat = NULL;
	struct sockaddr_in saddr;
	socklen_t saddrlen = sizeof(struct sockaddr_in);
	err e;
	confkeyval *ckv, *kv;
	char mydoproxy;
	sabdb redirs[24];  /* do we need more? */
	int r = 0;
	char *algos;

	fdin = socket_rastream(sock, "merovingian<-client (read)");
	if (fdin == 0)
		return(newErr("merovingian-client inputstream problems"));
	fdin = block_stream(fdin);

	fout = socket_wastream(sock, "merovingian->client (write)");
	if (fout == 0) {
		close_stream(fdin);
		return(newErr("merovingian-client outputstream problems"));
	}
	fout = block_stream(fout);

	if (isusock) {
		snprintf(host, sizeof(host), "(local)");
	} else if (getpeername(sock, (struct sockaddr *)&saddr, &saddrlen) == -1) {
		Mfprintf(stderr, "couldn't get peername of client: %s\n",
				strerror(errno));
		snprintf(host, sizeof(host), "(unknown)");
	} else {
		struct hostent *hoste = 
			gethostbyaddr(&saddr.sin_addr.s_addr, 4, saddr.sin_family);
		if (hoste == NULL) {
			snprintf(host, sizeof(host), "%u.%u.%u.%u:%u",
					(unsigned) ((ntohl(saddr.sin_addr.s_addr) >> 24) & 0xff),
					(unsigned) ((ntohl(saddr.sin_addr.s_addr) >> 16) & 0xff),
					(unsigned) ((ntohl(saddr.sin_addr.s_addr) >> 8) & 0xff),
					(unsigned) (ntohl(saddr.sin_addr.s_addr) & 0xff),
					(unsigned) (ntohs(saddr.sin_port)));
		} else {
			snprintf(host, sizeof(host), "%s:%u",
					hoste->h_name, (unsigned) (ntohs(saddr.sin_port)));
		}
	}

	/* note: since Jan2012 we speak proto 9 for control connections */
	chal[31] = '\0';
	generateSalt(chal, 31);
	algos = mcrypt_getHashAlgorithms();
	mnstr_printf(fout, "%s:merovingian:9:%s:%s:%s:",
			chal,
			algos,
#ifdef WORDS_BIGENDIAN
			"BIG",
#else
			"LIT",
#endif
			MONETDB5_PASSWDHASH
			);
	mnstr_flush(fout);
	free(algos);

	/* get response */
	buf[0] = '\0';
	if (mnstr_read_block(fdin, buf, sizeof(buf) - 1, 1) < 0) {
		/* we didn't get a terminated block :/ */
		e = newErr("client %s sent challenge in incomplete block: %s",
				host, buf);
		mnstr_printf(fout, "!monetdbd: client sent something this "
				"server could not understand, sorry\n");
		mnstr_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(e);
	}
	buf[sizeof(buf) - 1] = '\0';

	/* decode BIG/LIT:user:{cypher}passwordchal:lang:database: line */

	user = buf;
	/* byte order */
	s = strchr(user, ':');
	if (s) {
		*s = 0;
		/* we don't use this in merovingian */
		/* mnstr_set_byteorder(fin->s, strcmp(user, "BIG") == 0); */
		user = s + 1;
	} else {
		e = newErr("client %s challenge error: %s", host, buf);
		mnstr_printf(fout, "!monetdbd: incomplete challenge '%s'\n", buf);
		mnstr_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(e);
	}

	/* passwd */
	s = strchr(user, ':');
	if (s) {
		*s = 0;
		passwd = s + 1;
		/* decode algorithm, i.e. {plain}mypasswordchallenge */
		if (*passwd != '{') {
			e = newErr("client %s challenge error: %s", host, buf);
			mnstr_printf(fout, "!monetdbd: invalid password entry\n");
			mnstr_flush(fout);
			close_stream(fout);
			close_stream(fdin);
			return(e);
		}
		algo = passwd + 1;
		s = strchr(algo, '}');
		if (!s) {
			e = newErr("client %s challenge error: %s", host, buf);
			mnstr_printf(fout, "!monetdbd: invalid password entry\n");
			mnstr_flush(fout);
			close_stream(fout);
			close_stream(fdin);
			return(e);
		}
		*s = 0;
		passwd = s + 1;
	} else {
		e = newErr("client %s challenge error: %s", host, buf);
		mnstr_printf(fout, "!monetdbd: incomplete challenge, missing password after '%s'\n", user);
		mnstr_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(e);
	}

	/* lang */
	s = strchr(passwd, ':');
	if (s) {
		*s = 0;
		lang = s + 1;
	} else {
		e = newErr("client %s challenge error: %s", host, buf);
		mnstr_printf(fout, "!monetdbd: incomplete challenge, missing language after '%s'\n", passwd);
		mnstr_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(e);
	}

	/* database */
	s = strchr(lang, ':');
	if (s) {
		*s = 0;
		database = s + 1;
		/* since we don't know where the string ends, we need to look
		 * for another : */
		s = strchr(database, ':');
		if (s == NULL) {
			e = newErr("client %s challenge error: %s", host, buf);
			mnstr_printf(fout, "!monetdbd: incomplete challenge, missing trailing colon\n");
			mnstr_flush(fout);
			close_stream(fout);
			close_stream(fdin);
			return(e);
		} else {
			*s = '\0';
		}
	}

	if (*database == '\0') {
		/* we need to have a database, if we haven't gotten one,
		 * complain */
		mnstr_printf(fout, "!monetdbd: please specify a database\n");
		mnstr_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(newErr("client %s specified no database", host));
	}

	if (strcmp(lang, "control") == 0) {
		/* handle control client */
		if (control_authorise(host, chal, algo, passwd, sock))
			control_handleclient(sock, host);
		close_stream(fout);
		close_stream(fdin);
		return(NO_ERR);
	}

	if (strcmp(lang, "resolve") == 0) {
		/* ensure the pattern ends with '/\*' such that we force a
		 * remote entry, including those for local databases, this
		 * way we will get a redirect back to merovingian for such
		 * database if it is proxied and hence not remotely
		 * available */
		size_t len = strlen(database);
		if (len > 2 &&
				database[len - 2] != '/' &&
				database[len - 1] != '*')
		{
			snprintf(dbmod, sizeof(dbmod), "%s/*", database);
			database = dbmod;
		}
	}

	if ((e = forkMserver(database, &top, 0)) != NO_ERR) {
		if (top == NULL) {
			mnstr_printf(fout, "!monetdbd: no such database '%s', please create it first\n", database);
		} else {
			mnstr_printf(fout, "!monetdbd: internal error while starting mserver, please refer to the logs\n");
		}
		mnstr_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		return(e);
	}
	stat = top;

	/* a multiplex-funnel is a database which has no connections, but a
	 * scenario "mfunnel" */
	if ((top->conns == NULL || top->conns->val == NULL) &&
			top->scens != NULL && strcmp(top->scens->val, "mfunnel") == 0)
	{
		multiplexAddClient(top->dbname, sock, fout, fdin, host);
		msab_freeStatus(&top);
		return(NO_ERR);
	}

	/* collect possible redirects */
	for (stat = top; stat != NULL; stat = stat->next) {
		if (stat->conns == NULL || stat->conns->val == NULL) {
			Mfprintf(stdout, "dropping database without available "
					"connections: '%s'\n", stat->dbname);
		} else if (r == 24) {
			Mfprintf(stdout, "dropping database connection because of "
					"too many already: %s\n", stat->conns->val);
		} else {
			redirs[r++] = *stat;
		}
	}

	/* if we can't redirect, our mission ends here */
	if (r == 0) {
		if (top->locked) {
			e = newErr("database '%s' is under maintenance", top->dbname);
		} else {
			e = newErr("there are no available connections for '%s'", database);
		}
		mnstr_printf(fout, "!monetdbd: %s\n", e);
		mnstr_flush(fout);
		close_stream(fout);
		close_stream(fdin);
		msab_freeStatus(&top);
		return(e);
	}

	/* need to send a response, either we are going to proxy, or we send
	 * a redirect, if we have multiple options, a redirect is our only
	 * option, but if the redir is a single remote we need to stick to
	 * our default, there is a special case when the client indicates it
	 * is only resolving a pattern, in which we always need to send
	 * redirects, even if it's one */
	mydoproxy = 0;
	if (r == 1 && strcmp(lang, "resolve") != 0) {
		if (redirs[0].dbname != redirs[0].path) {
			/* this is a real local database (not a remote) */
			ckv = getDefaultProps();
			readProps(ckv, redirs[0].path);
			kv = findConfKey(ckv, "forward");
		} else {
			ckv = NULL;
			kv = NULL;
		}
		if (kv == NULL || kv->val == NULL)
			kv = findConfKey(_mero_props, "forward");
		mydoproxy = strcmp(kv->val, "proxy") == 0;
		if (ckv != NULL) {
			freeConfFile(ckv);
			free(ckv);
		}
	}

	if (mydoproxy == 0) {
		fprintf(stdout, "redirecting client %s for database '%s' to",
				host, database);
		/* client is in control, send all redirects */
		while (--r >= 0) {
			fprintf(stdout, " %s%s",
					redirs[r].conns->val, redirs[r].dbname);
			mnstr_printf(fout, "^%s%s\n",
					redirs[r].conns->val, redirs[r].dbname);
		}
		/* flush redirect */
		fprintf(stdout, "\n");
		fflush(stdout);
		mnstr_flush(fout);
	} else {
		Mfprintf(stdout, "proxying client %s for database '%s' to "
				"%s?database=%s\n",
				host, database, redirs[0].conns->val, redirs[0].dbname);
		/* merovingian is in control, only consider the first redirect */
		mnstr_printf(fout, "^mapi:merovingian://proxy?database=%s\n",
				redirs[0].dbname);
		/* flush redirect */
		mnstr_flush(fout);

		/* wait for input, or disconnect in a proxy runner */
		if ((e = startProxy(sock, fdin, fout,
						redirs[0].conns->val, host)) != NO_ERR)
		{
			/* we need to let the client login in order not to violate
			 * the protocol */
			mnstr_printf(fout, "void:merovingian:8:plain:BIG");
			mnstr_flush(fout);
			mnstr_read_block(fdin, buf, 8095, 1); /* eat away client response */
			mnstr_printf(fout, "!monetdbd: an internal error has occurred, refer to the logs for details, please try again later\n");
			mnstr_flush(fout);
			close_stream(fout);
			close_stream(fdin);
			Mfprintf(stdout, "starting a proxy failed: %s\n", e);
			msab_freeStatus(&top);
			return(e);
		};
	}

	msab_freeStatus(&top);
	return(NO_ERR);
}

char *
acceptConnections(int sock, int usock)
{
	char *msg;
	int retval;
	fd_set fds;
	int msgsock;
	err e;
	struct timeval tv;

	do {
		/* handle socket connections */
		FD_ZERO(&fds);
		FD_SET(sock, &fds);
		FD_SET(usock, &fds);

		/* Wait up to 5 seconds */
		tv.tv_sec = 5;
		tv.tv_usec = 0;
		retval = select((sock > usock ? sock : usock) + 1,
				&fds, NULL, NULL, &tv);
		if (retval == 0) {
			/* nothing interesting has happened */
			continue;
		}
		if (retval < 0) {
			if (_mero_keep_listening == 0)
				break;
			if (errno != EINTR) {
				msg = strerror(errno);
				goto error;
			}
			continue;
		}
		if (FD_ISSET(sock, &fds)) {
			if ((msgsock = accept(sock, (SOCKPTR)0, (socklen_t *) 0)) < 0) {
				if (_mero_keep_listening == 0)
					break;
				if (errno != EINTR) {
					msg = strerror(errno);
					goto error;
				}
				continue;
			}
		} else if (FD_ISSET(usock, &fds)) {
			struct msghdr msgh;
			struct iovec iov;
			char buf[1];
			int rv;
			char ccmsg[CMSG_SPACE(sizeof(int))];

			if ((msgsock = accept(usock, (SOCKPTR)0, (socklen_t *)0)) < 0) {
				if (_mero_keep_listening == 0)
					break;
				if (errno != EINTR) {
					msg = strerror(errno);
					goto error;
				}
				continue;
			}

			/* BEWARE: unix domain sockets have a slightly different
			 * behaviour initialy than normal sockets, because we can
			 * send filedescriptors or credentials with them.  To do so,
			 * we need to use sendmsg/recvmsg, which operates on a bare
			 * socket.  Unfortunately we *have* to send something, so it
			 * is one byte that can optionally carry the ancillary data.
			 * This byte is at this moment defined to contain a character:
			 *  '0' - there is no ancillary data
			 *  '1' - ancillary data for passing a file descriptor
			 * The future may introduce a state for passing credentials.
			 * Any unknown character must be interpreted as some unknown
			 * action, and hence not supported by the server.
			 * Since there is no reason why one would like to pass
			 * descriptors to Merovingian, this is not implemented here. */

			iov.iov_base = buf;
			iov.iov_len = 1;

			msgh.msg_name = 0;
			msgh.msg_namelen = 0;
			msgh.msg_iov = &iov;
			msgh.msg_iovlen = 1;
			msgh.msg_control = ccmsg;
			msgh.msg_controllen = sizeof(ccmsg);

			rv = recvmsg(msgsock, &msgh, 0);
			if (rv == -1) {
				close(msgsock);
				continue;
			}

			switch (*buf) {
				case '0':
					/* nothing special, nothing to do */
				break;
				case '1':
					/* filedescriptor, no way */
					close(msgsock);
					Mfprintf(stderr, "client error: fd passing not supported\n");
				continue;
				default:
					/* some unknown state */
					close(msgsock);
					Mfprintf(stderr, "client error: unknown initial byte\n");
				continue;
			}	
		} else
			continue;
		e = handleClient(msgsock, FD_ISSET(usock, &fds));
		if (e != NO_ERR) {
			Mfprintf(stderr, "client error: %s\n", getErrMsg(e));
			freeErr(e);
		}
	} while (_mero_keep_listening);
	shutdown(sock, SHUT_RDWR);
	close(sock);
	return(NO_ERR);

error:
	_mero_keep_listening = 0;
	shutdown(sock, SHUT_RDWR);
	close(sock);
	return(newErr("accept connection: %s", msg));
}

/* vim:set ts=4 sw=4 noexpandtab: */
