/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
openConnectionIP(int *ret, bool udp, bool bind_ipv6, const char *bindaddr, unsigned short port, FILE *log)
{
	struct addrinfo *result = NULL, *rp = NULL;
	int sock = -1, check = 0;
	int on = 1;
	char sport[16];
	char host[512] = "";
	int e = 0;

	struct addrinfo hints = (struct addrinfo) {
		.ai_family = bind_ipv6 ? AF_INET6 : AF_INET,
		.ai_socktype = udp ? SOCK_DGRAM : SOCK_STREAM,
		.ai_flags = AI_PASSIVE,
		.ai_protocol = udp ? 0 : IPPROTO_TCP,
	};
	snprintf(sport, sizeof(sport), "%hu", port);

	check = getaddrinfo(bindaddr, sport, &hints, &result);
	if (bindaddr == NULL)
		bindaddr = "any";		/* provide something for messages */
	if (check != 0)
		return newErr("cannot find host %s with error: %s", bindaddr, gai_strerror(check));

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
		(void) fcntl(sock, F_SETFD, FD_CLOEXEC);
#endif

		if (rp->ai_family == AF_INET6)
			(void) setsockopt(sock, IPPROTO_IPV6, IPV6_V6ONLY,
							  (const char *) &(int){0}, sizeof(int));

		if (!udp) {
			if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR,
						   (const char *) &on, sizeof on) < 0) {
				e = errno;
				closesocket(sock);
				sock = -1;
				continue;
			}
#ifdef SO_EXCLUSIVEADDRUSE
			(void) setsockopt(sock, SOL_SOCKET, SO_EXCLUSIVEADDRUSE,
							  (const char *) &on, sizeof on);
#endif
#ifdef SO_EXCLBIND
			(void) setsockopt(sock, SOL_SOCKET, SO_EXCLBIND,
							  (const char *) &on, sizeof on);
#endif
		}

		if (bind(sock, rp->ai_addr, rp->ai_addrlen) == -1) {
			e = errno;
			closesocket(sock);
			sock = -1;
			if (e == EADDRNOTAVAIL && bind_ipv6) {
				freeaddrinfo(result);
				return openConnectionIP(ret, udp, false, bindaddr, port, log);
			}
			continue;
		}
		if (getnameinfo(rp->ai_addr, rp->ai_addrlen,
						host, sizeof(host),
						sport, sizeof(sport),
						NI_NUMERICSERV | (udp ? NI_DGRAM : 0)) != 0) {
			host[0] = 0;
			snprintf(sport, sizeof(sport), "%hu", port);
		}
		break;					/* working */
	}
	freeaddrinfo(result);
	if (sock == -1) {
		if (e != 0) {			/* results found, error occurred */
			return newErr("binding to %s socket port %hu failed: %s",
						  udp ? "datagram" : "stream", port, strerror(e));
		} else { /* no results found, could not translate address */
			return newErr("cannot translate host %s", bindaddr);
		}
	}

	if (udp) {
		Mfprintf(log, "listening for UDP messages on %s:%s\n", host, sport);
	} else {
		/* keep queue of 5 */
		if (listen(sock, 5) == -1) {
			e = errno;
			closesocket(sock);
			return(newErr("failed setting socket to listen: %s", strerror(e)));
		}

		Mfprintf(log, "accepting connections on TCP socket %s:%s\n", host, sport);
	}

	*ret = sock;
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
