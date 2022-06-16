/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"

#include <string.h>  /* strerror, strchr, strcmp */
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_UIO_H
# include <sys/uio.h>
#endif
#include <fcntl.h>

#include "msabaoth.h"
#include "mcrypt.h"
#include "stream.h"
#include "stream_socket.h"
#include "utils/utils.h" /* freeConfFile */
#include "utils/properties.h" /* readProps */

#include "merovingian.h"
#include "forkmserver.h"
#include "proxy.h"
#include "multiplex-funnel.h"
#include "controlrunner.h"
#include "client.h"
#include "handlers.h"

#if !defined(HAVE_ACCEPT4) || !defined(SOCK_CLOEXEC)
#define accept4(sockfd, addr, addrlen, flags)	accept(sockfd, addr, addrlen)
#endif

struct threads {
	struct threads *next;
	pthread_t tid;
	volatile bool dead;
};
struct clientdata {
	int sock;
	bool isusock;
	struct threads *self;
	char challenge[32];
};

static void *
handleClient(void *data)

{
	stream *fdin, *fout;
	char buf[8096];
	char chal[32];
	char *user = NULL, *algo = NULL, *passwd = NULL, *lang = NULL;
	char *database = NULL, *s;
	char dbmod[64];
	char host[512];
	char port[16];
	sabdb *top = NULL;
	sabdb *stat = NULL;
	struct sockaddr_storage saddr;
	socklen_t saddrlen = (socklen_t) sizeof(saddr);
	err e;
	confkeyval *ckv, *kv;
	char mydoproxy;
	sabdb redirs[24];  /* do we need more? */
	int r = 0;
	int sock;
	bool isusock;
	struct threads *self;

	sock = ((struct clientdata *) data)->sock;
	isusock = ((struct clientdata *) data)->isusock;
	self = ((struct clientdata *) data)->self;
	memcpy(chal, ((struct clientdata *) data)->challenge, sizeof(chal));
	free(data);
	fdin = socket_rstream(sock, "merovingian<-client (read)");
	if (fdin == NULL) {
		self->dead = true;
		return(newErr("merovingian-client inputstream problems: %s", mnstr_peek_error(NULL)));
	}
	fdin = block_stream(fdin);

	fout = socket_wstream(sock, "merovingian->client (write)");
	if (fout == 0) {
		close_stream(fdin);
		self->dead = true;
		return(newErr("merovingian-client outputstream problems: %s", mnstr_peek_error(NULL)));
	}
	fout = block_stream(fout);

	if (isusock) {
		snprintf(host, sizeof(host), "(local)");
	} else if (getpeername(sock, (struct sockaddr *) &saddr, &saddrlen) == -1) {
		Mfprintf(stderr, "couldn't get peername of client: %s\n", strerror(errno));
		snprintf(host, sizeof(host), "(unknown)");
	} else {
		char ghost[512];
		if (getnameinfo((struct sockaddr *) &saddr, saddrlen, ghost, sizeof(ghost), port, sizeof(port),
			NI_NUMERICSERV | NI_NUMERICHOST) == 0) {
			snprintf(host, sizeof(host), "%s:%s", ghost, port);
		} else {
			snprintf(host, sizeof(host), "(unknown):%s", port);
		}
	}

	/* note: since Jan2012 we speak proto 9 for control connections */
	mnstr_printf(fout, "%s:merovingian:9:%s:%s:%s:",
			chal,
			mcrypt_getHashAlgorithms(),
#ifdef WORDS_BIGENDIAN
			"BIG",
#else
			"LIT",
#endif
			MONETDB5_PASSWDHASH
			);
	mnstr_flush(fout, MNSTR_FLUSH_DATA);

	/* get response */
	buf[0] = '\0';
	if (mnstr_read_block(fdin, buf, sizeof(buf) - 1, 1) < 0) {
		/* we didn't get a terminated block :/ */
		e = newErr("client %s sent challenge in incomplete block: %s",
				host, buf);
		mnstr_printf(fout, "!monetdbd: client sent something this "
				"server could not understand, sorry\n");
		mnstr_flush(fout, MNSTR_FLUSH_DATA);
		close_stream(fout);
		close_stream(fdin);
		self->dead = true;
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
		mnstr_flush(fout, MNSTR_FLUSH_DATA);
		close_stream(fout);
		close_stream(fdin);
		self->dead = true;
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
			mnstr_flush(fout, MNSTR_FLUSH_DATA);
			close_stream(fout);
			close_stream(fdin);
			self->dead = true;
			return(e);
		}
		algo = passwd + 1;
		s = strchr(algo, '}');
		if (!s) {
			e = newErr("client %s challenge error: %s", host, buf);
			mnstr_printf(fout, "!monetdbd: invalid password entry\n");
			mnstr_flush(fout, MNSTR_FLUSH_DATA);
			close_stream(fout);
			close_stream(fdin);
			self->dead = true;
			return(e);
		}
		*s = 0;
		passwd = s + 1;
	} else {
		e = newErr("client %s challenge error: %s", host, buf);
		mnstr_printf(fout, "!monetdbd: incomplete challenge, missing password after '%s'\n", user);
		mnstr_flush(fout, MNSTR_FLUSH_DATA);
		close_stream(fout);
		close_stream(fdin);
		self->dead = true;
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
		mnstr_flush(fout, MNSTR_FLUSH_DATA);
		close_stream(fout);
		close_stream(fdin);
		self->dead = true;
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
			mnstr_flush(fout, MNSTR_FLUSH_DATA);
			close_stream(fout);
			close_stream(fdin);
			self->dead = true;
			return(e);
		} else {
			*s = '\0';
		}
	}

	if (database == NULL || *database == '\0') {
		/* we need to have a database, if we haven't gotten one,
		 * complain */
		mnstr_printf(fout, "!monetdbd: please specify a database\n");
		mnstr_flush(fout, MNSTR_FLUSH_DATA);
		close_stream(fout);
		close_stream(fdin);
		self->dead = true;
		return(newErr("client %s specified no database", host));
	}

	if (strcmp(lang, "control") == 0) {
		/* handle control client */
		if (control_authorise(host, chal, algo, passwd, fout))
			control_handleclient(host, sock, fdin, fout);
		close_stream(fout);
		close_stream(fdin);
		self->dead = true;
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

	if ((e = forkMserver(database, &top, false)) != NO_ERR) {
		if (top == NULL) {
			mnstr_printf(fout, "!monetdbd: no such database '%s', please create it first\n", database);
		} else {
			mnstr_printf(fout, "!monetdbd: internal error while starting mserver '%s'%s\n", e, strstr(e, "logfile")?"":", please refer to the logs");
			Mfprintf(_mero_ctlerr, "!monetdbd: an internal error has occurred '%s'\n",e);
		}
		mnstr_flush(fout, MNSTR_FLUSH_DATA);
		close_stream(fout);
		close_stream(fdin);
		self->dead = true;
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
		self->dead = true;
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
		mnstr_flush(fout, MNSTR_FLUSH_DATA);
		close_stream(fout);
		close_stream(fdin);
		msab_freeStatus(&top);
		self->dead = true;
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
		mnstr_flush(fout, MNSTR_FLUSH_DATA);
		close_stream(fout);
		close_stream(fdin);
	} else {
		/* Jan2022: disabled logging of next info message to reduce merovingian.log size:
		Mfprintf(stdout, "proxying client %s for database '%s' to "
				"%s?database=%s\n",
				host, database, redirs[0].conns->val, redirs[0].dbname);
		*/
		/* merovingian is in control, only consider the first redirect */
		mnstr_printf(fout, "^mapi:merovingian://proxy?database=%s\n",
				redirs[0].dbname);
		/* flush redirect */
		mnstr_flush(fout, MNSTR_FLUSH_DATA);

		/* wait for input, or disconnect in a proxy runner */
		if ((e = startProxy(sock, fdin, fout,
						redirs[0].conns->val, host)) != NO_ERR)
		{
			/* we need to let the client login in order not to violate
			 * the protocol */
			mnstr_printf(fout, "void:merovingian:9:%s:BIG:%s:",
					mcrypt_getHashAlgorithms(), MONETDB5_PASSWDHASH);
			mnstr_flush(fout, MNSTR_FLUSH_DATA);
			mnstr_read_block(fdin, buf, 8095, 1); /* eat away client response */
			mnstr_printf(fout, "!monetdbd: an internal error has occurred '%s', refer to the logs for details, please try again later\n",e);
			mnstr_flush(fout, MNSTR_FLUSH_DATA);
			Mfprintf(_mero_ctlerr, "!monetdbd: an internal error has occurred '%s'\n",e);
			close_stream(fout);
			close_stream(fdin);
			Mfprintf(stdout, "starting a proxy failed: %s\n", e);
			msab_freeStatus(&top);
			self->dead = true;
			return(e);
		}
	}

	msab_freeStatus(&top);
	self->dead = true;
	return(NO_ERR);
}

char *
acceptConnections(int socks[3])
{
	char *msg;
	int retval;
	void *e;
	struct clientdata *data;
	struct threads *threads = NULL, **threadp, *p;
	int errnr;					/* saved errno */

	do {
		/* handle socket connections */
		bool isusock = false;
		int i;
		int sock = -1;

#ifdef HAVE_POLL
		struct pollfd pfd[3];
		int npoll = 0;
		for (i = 0; i < 3; i++) {
			if (socks[i] >= 0)
				pfd[npoll++] = (struct pollfd) {.fd = socks[i],
												.events = POLLIN};
		}

		/* Wait up to 5 seconds */
		retval = poll(pfd, npoll, 5000);
#else
		fd_set fds;
		FD_ZERO(&fds);

		for (i = 0; i < 3; i++) {
			if (socks[i] >= 0) {
				if (socks[i] > sock)
					sock = socks[i];
				FD_SET(socks[i], &fds);
			}
		}

		/* Wait up to 5 seconds */
		struct timeval tv = (struct timeval) {.tv_sec = 5};
		retval = select(sock + 1, &fds, NULL, NULL, &tv);
		sock = -1;
#endif
		errnr = errno;
		/* join any handleClient threads that we started and that may
		 * have finished by now */
		for (threadp = &threads; *threadp; threadp = &(*threadp)->next) {
			if ((*threadp)->dead &&
				pthread_join((*threadp)->tid, &e) == 0) {
				p = *threadp;
				*threadp = p->next;
				free(p);
				if (e != NO_ERR) {
					Mfprintf(stderr, "client error: %s\n",
							 getErrMsg((char *) e));
					freeErr(e);
				}
				if (*threadp == NULL)
					break;
			}
		}
		childhandler();
		reinitialize();
		if (retval == 0) {
			/* nothing interesting has happened */
			continue;
		}
		if (retval == -1) {
			if (_mero_keep_listening == 0)
				break;
			switch (errnr) {
			case EINTR:
				/* interrupted */
				break;
			case EMFILE:
			case ENFILE:
			case ENOBUFS:
			case ENOMEM:
				/* transient failures */
				break;
			default:
				msg = strerror(errnr);
				goto error;
			}
			continue;
		}
#ifdef HAVE_POLL
		for (i = 0; i < npoll; i++) {
			if (pfd[i].revents & POLLIN) {
				sock = pfd[i].fd;
				isusock = sock == socks[2];
				break;
			}
		}
#else
		for (i = 0; i < 3; i++) {
			if (socks[i] >= 0 && FD_ISSET(socks[i], &fds)) {
				sock = socks[i];
				isusock = i == 2;
				break;
			}
		}
#endif
		if (sock < 0)
			continue;

		if ((sock = accept4(sock, NULL, NULL, SOCK_CLOEXEC)) == -1) {
			if (_mero_keep_listening == 0)
				break;
			switch (errno) {
			case EINTR:
				/* interrupted */
				break;
			case EMFILE:
			case ENFILE:
			case ENOBUFS:
			case ENOMEM:
				/* transient failures */
				sleep_ms(500);	/* wait a little, maybe it goes away */
				break;
			case ECONNABORTED:
				/* connection aborted before we began */
				break;
			default:
				msg = strerror(errno);
				goto error;
			}
			continue;
		}
#if defined(HAVE_FCNTL) && (!defined(SOCK_CLOEXEC) || !defined(HAVE_ACCEPT4))
		(void) fcntl(sock, F_SETFD, FD_CLOEXEC);
#endif

		if (isusock) {
			struct msghdr msgh;
			struct iovec iov;
			char buf[1];
			int rv;
			char ccmsg[CMSG_SPACE(sizeof(int))];

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

			msgh = (struct msghdr) {
				.msg_name = 0,
				.msg_namelen = 0,
				.msg_iov = &iov,
				.msg_iovlen = 1,
				.msg_control = ccmsg,
				.msg_controllen = sizeof(ccmsg),
			};

			rv = recvmsg(sock, &msgh, 0);
			if (rv == -1) {
				closesocket(sock);
				continue;
			}

			switch (buf[0]) {
			case '0':
				/* nothing special, nothing to do */
				break;
			case '1':
				/* filedescriptor, no way */
				closesocket(sock);
				Mfprintf(stderr, "client error: fd passing not supported\n");
				continue;
			default:
				/* some unknown state */
				closesocket(sock);
				Mfprintf(stderr, "client error: unknown initial byte\n");
				continue;
			}
		}

		/* start handleClient as a thread so that we're not blocked by
		 * a slow client */
		data = malloc(sizeof(*data)); /* freed by handleClient */
		p = malloc(sizeof(*p));
		if (data == NULL || p == NULL) {
			if (data)
				free(data);
			if (p)
				free(p);
			closesocket(sock);
			Mfprintf(stderr, "cannot allocate memory\n");
			continue;
		}
		data->sock = sock;
		data->isusock = isusock;
		p->dead = false;
		data->self = p;
		data->challenge[31] = '\0';
		generateSalt(data->challenge, 31);
		if (pthread_create(&p->tid, NULL, handleClient, data) == 0) {
			p->next = threads;
			threads = p;
		} else {
			closesocket(sock);
			free(data);
			free(p);
		}
	} while (_mero_keep_listening);
	if (socks[0] >= 0) {
		shutdown(socks[0], SHUT_RDWR);
		closesocket(socks[0]);
	}
	if (socks[1] >= 0) {
		shutdown(socks[1], SHUT_RDWR);
		closesocket(socks[1]);
	}
	return(NO_ERR);

error:
	_mero_keep_listening = 0;
	if (socks[0] >= 0) {
		shutdown(socks[0], SHUT_RDWR);
		closesocket(socks[0]);
	}
	if (socks[1] >= 0) {
		shutdown(socks[1], SHUT_RDWR);
		closesocket(socks[1]);
	}
	return(newErr("accept connection: %s", msg));
}
