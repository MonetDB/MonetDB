/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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

#include "mstring.h"
#include "stream.h"
#include "stream_socket.h"

#include "merovingian.h"
#include "connections.h"

err
openConnectionIP(int *socks, bool udp, const char *bindaddr, unsigned short port, FILE *log)
{
	struct addrinfo *result = NULL, *rp = NULL;
	int sock = -1, check = 0;
	int nsock = 0;
	int on = 1;
	char sport[16];
	char host[512] = "";
	int e = 0;
	const char *msghost = bindaddr ? bindaddr : "any"; /* for messages */
	int ipv6_vs6only = -1;

	struct addrinfo hints = (struct addrinfo) {
		.ai_family = AF_INET6,
		.ai_socktype = udp ? SOCK_DGRAM : SOCK_STREAM,
		.ai_flags = AI_PASSIVE | AI_NUMERICSERV,
		.ai_protocol = udp ? 0 : IPPROTO_TCP,
	};
	snprintf(sport, sizeof(sport), "%hu", port);

	socks[0] = socks[1] = -1;

	if (bindaddr == NULL || strcmp(bindaddr, "localhost") == 0) {
		hints.ai_family = AF_INET6;
		hints.ai_flags |= AI_NUMERICHOST;
		ipv6_vs6only = 0;
		bindaddr = "::1";
		strcpy_len(host, "localhost", sizeof(host));
	} else if (strcmp(bindaddr, "all") == 0) {
		hints.ai_family = AF_INET6;
		ipv6_vs6only = 0;
		bindaddr = NULL;
	} else if (strcmp(bindaddr, "::") == 0) {
		hints.ai_family = AF_INET6;
		ipv6_vs6only = 1;
		bindaddr = NULL;
	} else if (strcmp(bindaddr, "0.0.0.0") == 0) {
		hints.ai_family = AF_INET;
		hints.ai_flags |= AI_NUMERICHOST;
		bindaddr = NULL;
	} else if (strcmp(bindaddr, "::1") == 0) {
		hints.ai_family = AF_INET6;
		hints.ai_flags |= AI_NUMERICHOST;
		ipv6_vs6only = 1;
		strcpy_len(host, "localhost", sizeof(host));
	} else if (strcmp(bindaddr, "127.0.0.1") == 0) {
		hints.ai_family = AF_INET;
		hints.ai_flags |= AI_NUMERICHOST;
		strcpy_len(host, "localhost", sizeof(host));
	} else {
		hints.ai_family = AF_INET6;
		ipv6_vs6only = 0;
	}

	for (;;) {					/* max twice */
		check = getaddrinfo(bindaddr, sport, &hints, &result);
		if (check != 0)
			return newErr("cannot find interface %s with error: %s", msghost, gai_strerror(check));

		for (rp = result; rp != NULL; rp = rp->ai_next) {
			sock = socket(rp->ai_family, rp->ai_socktype
#ifdef SOCK_CLOEXEC
						  | SOCK_CLOEXEC
#endif
						  , rp->ai_protocol);
			if (sock == -1) {
				e = errno;
				continue;
			}
#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
			if (fcntl(sock, F_SETFD, FD_CLOEXEC) < 0)
					Mfprintf(log, "fcntl FD_CLOEXEC: %s", strerror(e));
#endif

			if (rp->ai_family == AF_INET6 &&
				setsockopt(sock, IPPROTO_IPV6, IPV6_V6ONLY,
						   (const char *) &(int){0}, sizeof(int)) == -1)
				Mfprintf(log, "setsockopt IPV6_V6ONLY: %s", strerror(e));

			if (!udp) {
				if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR,
							   (const char *) &on, sizeof on) < 0) {
					e = errno;
					closesocket(sock);
					continue;
				}
#ifdef SO_EXCLUSIVEADDRUSE
				if (setsockopt(sock, SOL_SOCKET, SO_EXCLUSIVEADDRUSE,
							   (const char *) &on, sizeof on) < 0)
					Mfprintf(log, "setsockopt SO_EXCLUSIVEADDRUSE: %s", strerror(e));
#endif
#ifdef SO_EXCLBIND
				if (setsockopt(sock, SOL_SOCKET, SO_EXCLBIND,
							   (const char *) &on, sizeof on) < 0)
					Mfprintf(log, "setsockopt SO_EXCLBIND: %s", strerror(e));
#endif
			}

			if (bind(sock, rp->ai_addr, rp->ai_addrlen) == -1) {
				e = errno;
				closesocket(sock);
				continue;
			}
			if (!udp && listen(sock, 5) == -1) {
				e = errno;
				closesocket(sock);
				continue;
			}
			struct sockaddr_storage addr;
			socklen_t addrlen = (socklen_t) sizeof(addr);
			if (getsockname(sock, (struct sockaddr *) &addr, &addrlen) == -1) {
				e = errno;
				closesocket(sock);
				continue;
			}
			if (getnameinfo((struct sockaddr *) &addr, addrlen,
							host, sizeof(host),
							sport, sizeof(sport),
							NI_NUMERICSERV | (udp ? NI_DGRAM : 0)) != 0) {
				host[0] = 0;
				snprintf(sport, sizeof(sport), "%hu", port);
			}
			if (udp)
				Mfprintf(log, "listening for UDP messages on %s:%s\n", host, sport);
			else
				Mfprintf(log, "accepting connections on TCP socket %s:%s\n", host, sport);
			socks[nsock++] = sock;
			break;					/* working */
		}
		freeaddrinfo(result);
		if (ipv6_vs6only == 0) {
			ipv6_vs6only = -1;
			hints.ai_family = AF_INET;
			if (bindaddr && strcmp(bindaddr, "::1") == 0)
				bindaddr = "127.0.0.1";
		} else
			break;
	}

	if (nsock == 0) {
		if (e != 0) {			/* results found, error occurred */
			return newErr("binding to %s socket port %hu failed: %s",
						  udp ? "datagram" : "stream", port, strerror(e));
		} else { /* no results found, could not translate address */
			return newErr("cannot translate host %s", msghost);
		}
	}

	return(NO_ERR);
}

err
openConnectionUNIX(int *ret, const char *path, int mode, FILE *log)
{
	struct sockaddr_un server;
	int sock;
	int omask;

	if (strlen(path) >= sizeof(server.sun_path))
		return newErr("pathname for UNIX stream socket too long");

	sock = socket(AF_UNIX, SOCK_STREAM
#ifdef SOCK_CLOEXEC
				  | SOCK_CLOEXEC
#endif
				  , 0);
	if (sock == -1)
		return(newErr("creation of UNIX stream socket failed: %s",
					strerror(errno)));
#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
	(void) fcntl(sock, F_SETFD, FD_CLOEXEC);
#endif

	server = (struct sockaddr_un) {
		.sun_family = AF_UNIX,
	};
	strcpy_len(server.sun_path, path, sizeof(server.sun_path));

	/* have to use umask to restrict permissions to avoid a race
	 * condition */
	omask = umask(mode);
	if (bind(sock, (struct sockaddr *) &server, sizeof(struct sockaddr_un)) == -1) {
		umask(omask);
		closesocket(sock);
		return(newErr("binding to UNIX stream socket at %s failed: %s",
				path, strerror(errno)));
	}
	umask(omask);

	/* keep queue of 5 */
	if(listen(sock, 5) == -1) {
		closesocket(sock);
		return(newErr("setting UNIX stream socket at %s to listen failed: %s",
					  path, strerror(errno)));
	}

	Mfprintf(log, "accepting connections on UNIX domain socket %s\n", path);

	*ret = sock;
	return(NO_ERR);
}

/* vim:set ts=4 sw=4 noexpandtab: */
