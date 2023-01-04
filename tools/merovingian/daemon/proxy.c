/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <sys/types.h>
#include <sys/stat.h> /* stat */
#include <sys/wait.h> /* wait */
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <string.h> /* strerror */
#ifdef HAVE_SYS_UIO_H
# include <sys/uio.h>
#endif

#include "mstring.h"
#include "stream.h"
#include "stream_socket.h"

#include "merovingian.h"
#include "proxy.h"

typedef struct _merovingian_proxy {
	stream *in;      /* the input to read from and to dispatch to out */
	stream *out;     /* where to write the read input to */
	stream *co_in;   /* the input stream of the co-thread,
	                    don't read from this stream!  close only */
	stream *co_out;  /* the output stream of the co-thread,
	                    don't write to this stream!  close only */
	char *name;      /* a description to log when this thread ends */
	pthread_t co_thr;/* the other proxyThread */
} merovingian_proxy;

static void *
proxyThread(void *d)
{
	merovingian_proxy *p = (merovingian_proxy *)d;
	int len;
	char data[8 * 1024];

	/* pass everything from in to out, until either reading from in,
	 * or writing to out fails, then close the other proxyThread's in
	 * and out streams so that it stops as well (it will, or already
	 * has, closed the streams we read from/write to) */
	while ((len = mnstr_read(p->in, data, 1, sizeof(data))) >= 0) {
		if (len > 0 && mnstr_write(p->out, data, len, 1) != 1)
			break;
		if (len == 0 &&	mnstr_flush(p->out, MNSTR_FLUSH_DATA) == -1)
			break;
	}

	mnstr_close(p->co_out);  /* out towards target B */
	mnstr_close(p->co_in);   /* related in from target A */

	if (p->name != NULL) {
		/* name is only set on the client-to-server thread */
		if (len <= 0) {
			Mlevelfprintf(DEBUG, stdout, "client %s has disconnected from proxy\n",
					p->name);
		} else {
			Mlevelfprintf(WARNING, stdout, "server has terminated proxy connection, "
					"disconnecting client %s\n", p->name);
		}
		free(p->name);

		/* wait for the other thread to finish, after which we can
		 * finally destroy the streams (all four, since we're the only
		 * one doing it) */
		pthread_join(p->co_thr, NULL);
		mnstr_destroy(p->co_out);
		mnstr_destroy(p->in);
		mnstr_destroy(p->out);
		mnstr_destroy(p->co_in);
	}

	free(p);
	return NULL;
}

err
startProxy(int psock, stream *cfdin, stream *cfout, char *url, char *client)
{
	int ssock = -1;
	char *port, *t, *conn, *endipv6;
	struct stat statbuf;
	stream *sfdin, *sfout;
	merovingian_proxy *pctos, *pstoc;
	pthread_t ptid;
	pthread_attr_t detachattr;
	int thret;

	/* quick 'n' dirty parsing */
	if (strncmp(url, "mapi:monetdb://", sizeof("mapi:monetdb://") - 1) == 0) {
		conn = strdup(url + sizeof("mapi:monetdb://") - 1);

		if (*conn == '[') { /* check for an IPv6 address */
			if ((endipv6 = strchr(conn, ']')) != NULL) {
				if ((port = strchr(endipv6, ':')) != NULL) {
					*port = '\0';
					port++;
					if ((t = strchr(port, '/')) != NULL)
						*t = '\0';
				} else {
					free(conn);
					return(newErr("can't find a port in redirect: %s", url));
				}
			} else {
				free(conn);
				return(newErr("invalid IPv6 address in redirect: %s", url));
			}
		} else if ((port = strchr(conn, ':')) != NULL) { /* drop anything off after the hostname */
			*port = '\0';
			port++;
			if ((t = strchr(port, '/')) != NULL)
				*t = '\0';
		} else if (stat(conn, &statbuf) != -1) {
			ssock = 0;
		} else {
			free(conn);
			return(newErr("can't find a port in redirect, "
						"or is not a UNIX socket file: %s", url));
		}
	} else {
		return(newErr("unsupported protocol/scheme in redirect: %s", url));
	}

	if (ssock != -1) {
		/* UNIX socket connect, don't proxy, but pass socket fd */
		struct sockaddr_un server;
		struct msghdr msg;
		char ccmsg[CMSG_SPACE(sizeof(ssock))];
		struct cmsghdr *cmsg;
		struct iovec vec;
		char buf[1];
		int *c_d;

		server = (struct sockaddr_un) {
			.sun_family = AF_UNIX,
		};
		strcpy_len(server.sun_path, conn, sizeof(server.sun_path));
		free(conn);
		if ((ssock = socket(PF_UNIX, SOCK_STREAM
#ifdef SOCK_CLOEXEC
							| SOCK_CLOEXEC
#endif
							, 0)) == -1) {
			return(newErr("cannot open socket: %s", strerror(errno)));
		}
#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
		(void) fcntl(ssock, F_SETFD, FD_CLOEXEC);
#endif
		if (connect(ssock, (struct sockaddr *) &server, sizeof(struct sockaddr_un)) == -1) {
			closesocket(ssock);
			return(newErr("cannot connect: %s", strerror(errno)));
		}

		/* send first byte, nothing special to happen */
		msg.msg_name = NULL;
		msg.msg_namelen = 0;
		*buf = '1'; /* pass fd */
		vec.iov_base = buf;
		vec.iov_len = 1;
		msg.msg_iov = &vec;
		msg.msg_iovlen = 1;
		msg.msg_control = ccmsg;
		msg.msg_controllen = sizeof(ccmsg);
		cmsg = CMSG_FIRSTHDR(&msg);
		cmsg->cmsg_level = SOL_SOCKET;
		cmsg->cmsg_type = SCM_RIGHTS;
		cmsg->cmsg_len = CMSG_LEN(sizeof(psock));
		/* HACK to avoid
		 * "dereferencing type-punned pointer will break strict-aliasing rules"
		 * (with gcc 4.5.1 on Fedora 14)
		 */
		c_d = (int *)CMSG_DATA(cmsg);
		*c_d = psock;
		msg.msg_controllen = cmsg->cmsg_len;
		msg.msg_flags = 0;

		Mlevelfprintf(DEBUG, stdout, "target connection is on local UNIX domain socket, "
				"passing on filedescriptor instead of proxying\n");
		if (sendmsg(ssock, &msg, 0) < 0) {
			closesocket(ssock);
			return(newErr("could not send initial byte: %s", strerror(errno)));
		}
		/* block until the server acknowledges that it has psock
		 * connected with itself */
		if (recv(ssock, buf, 1, 0) == -1) {
			closesocket(ssock);
			return(newErr("could not receive initial byte: %s", strerror(errno)));
		}
		shutdown(ssock, SHUT_RDWR);
		closesocket(ssock);
		/* psock is the underlying socket of cfdin/cfout which we
		 * passed on to the client; we need to close the socket, but
		 * not call shutdown() on it, which would happen if we called
		 * close_stream(), so we call closesocket to close the socket
		 * and mnstr_destroy to free memory */
		closesocket(psock);
		mnstr_destroy(cfdin);
		mnstr_destroy(cfout);
		return(NO_ERR);
	} else {
		int check;
		struct addrinfo *results, *rp, hints = (struct addrinfo) {
			.ai_family = AF_UNSPEC,
			.ai_socktype = SOCK_STREAM,
			.ai_protocol = IPPROTO_TCP,
		};

		if ((check = getaddrinfo(conn, port, &hints, &results)) != 0) {
			err x = newErr("cannot get address for hostname '%s': %s", conn, gai_strerror(check));
			free(conn);
			return(x);
		}
		free(conn);

		for (rp = results; rp; rp = rp->ai_next) {
			ssock = socket(rp->ai_family, rp->ai_socktype
#ifdef SOCK_CLOEXEC
							| SOCK_CLOEXEC
#endif
						   , rp->ai_protocol);
			if (ssock == -1)
				continue;
			if (connect(ssock, rp->ai_addr, rp->ai_addrlen) == -1) {
				closesocket(ssock);
				continue;
			} else {
#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
				(void) fcntl(ssock, F_SETFD, FD_CLOEXEC);
#endif
				break;
			}
		}
		if (results)
			freeaddrinfo(results);
		if (rp == NULL)
			return(newErr("cannot open socket: %s", strerror(errno)));
	}

	sfdin = block_stream(socket_rstream(ssock, "merovingian<-server (proxy read)"));
	if (sfdin == 0) {
		return(newErr("merovingian-server inputstream or outputstream problems: %s", mnstr_peek_error(NULL)));
	}

	sfout = block_stream(socket_wstream(ssock, "merovingian->server (proxy write)"));
	if (sfout == 0) {
		close_stream(sfdin);
		return(newErr("merovingian-server inputstream or outputstream problems: %s", mnstr_peek_error(NULL)));
	}

	/* our proxy schematically looks like this:
	 *
	 *                  A___>___B
	 *        out     in |     | out     in
	 * client  --------- |  M  | ---------  server
	 *        in     out |_____| in     out
	 *                  C   <   D
	 *
	 * the thread that does A -> B is called ctos, C -> D stoc
	 * the merovingian_proxy structs are filled like:
	 * ctos: in = A, out = B, co_in = D, co_out = C
	 * stoc: in = D, out = C, co_in = A, co_out = B
	 */

	pstoc = malloc(sizeof(merovingian_proxy));
	pstoc->in     = sfdin;
	pstoc->out    = cfout;
	pstoc->co_in  = cfdin;
	pstoc->co_out = sfout;
	pstoc->name   = NULL;  /* we want only one log-message on disconnect */
	pstoc->co_thr = 0;

	if ((thret = pthread_create(&ptid, NULL,
				proxyThread, (void *)pstoc)) != 0)
	{
		close_stream(sfout);
		close_stream(sfdin);
		return(newErr("failed to create proxy thread: %s", strerror(thret)));
	}

	pctos = malloc(sizeof(merovingian_proxy));
	pctos->in     = cfdin;
	pctos->out    = sfout;
	pctos->co_in  = sfdin;
	pctos->co_out = cfout;
	pctos->name   = strdup(client);
	pctos->co_thr = ptid;

	pthread_attr_init(&detachattr);
	pthread_attr_setdetachstate(&detachattr, PTHREAD_CREATE_DETACHED);
	if ((thret = pthread_create(&ptid, &detachattr,
				proxyThread, (void *)pctos)) != 0)
	{
		close_stream(sfout);
		close_stream(sfdin);
		return(newErr("failed to create proxy thread: %s", strerror(thret)));
	}

	return(NO_ERR);
}
