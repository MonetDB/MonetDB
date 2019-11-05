/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
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
openConnectionTCP(int *ret, bool bind_ipv6, const char *bindaddr, unsigned short port, FILE *log)
{
	struct sockaddr *server;
	struct sockaddr_in server_ipv4;
	struct sockaddr_in6 server_ipv6;
	struct addrinfo *result = NULL, *rp = NULL;
	int sock = -1, check = 0;
	socklen_t length = 0;
	int on = 1;
	int i = 0;
	char sport[16];
	char host[512];

	snprintf(sport, 16, "%hu", port);
	if (bindaddr) {
		struct addrinfo hints = (struct addrinfo) {
			.ai_family = bind_ipv6 ? AF_INET6 : AF_INET,
			.ai_socktype = SOCK_STREAM,
			.ai_flags = AI_PASSIVE,
			.ai_protocol = IPPROTO_TCP,
		};

		check = getaddrinfo(bindaddr, sport, &hints, &result);
		if (check != 0)
			return newErr("cannot find host %s with error: %s", bindaddr, gai_strerror(check));

		for (rp = result; rp != NULL; rp = rp->ai_next) {
			sock = socket(rp->ai_family, rp->ai_socktype
#ifdef SOCK_CLOEXEC
						 | SOCK_CLOEXEC
#endif
					, rp->ai_protocol);
			if (sock == -1)
				continue;
#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
			(void) fcntl(sock, F_SETFD, FD_CLOEXEC);
#endif

			if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (char *) &on, sizeof on) < 0) {
				closesocket(sock);
				continue;
			}

			if (bind(sock, rp->ai_addr, rp->ai_addrlen) != -1)
				break; /* working */
		}
		if (rp == NULL) {
			int e = errno;
			if (result)
				freeaddrinfo(result);
			if (sock != -1)
				closesocket(sock);
			if (result) { /* results found, tried socket, setsockopt and bind calls */
				errno = e;
				return newErr("binding to stream socket port %hu failed: %s", port, strerror(errno));
			} else { /* no results found, could not translate address */
				return newErr("cannot translate host %s", bindaddr);
			}
		}
		server = rp->ai_addr;
		length = rp->ai_addrlen;
	} else {
		sock = socket(bind_ipv6 ? AF_INET6 : AF_INET, SOCK_STREAM
#ifdef SOCK_CLOEXEC
					| SOCK_CLOEXEC
#endif
				, 0);
		if (sock == -1)
			return(newErr("creation of stream socket failed: %s", strerror(errno)));

		if (bind_ipv6) {
			memset(&server_ipv6, 0, sizeof(server_ipv6));
			server_ipv6.sin6_family = AF_INET6;
			length = (socklen_t) sizeof(server_ipv6);
			server_ipv6.sin6_port = htons((unsigned short) ((port) & 0xFFFF));
			server_ipv6.sin6_addr = ipv6_any_addr;
		} else {
			server_ipv4.sin_family = AF_INET;
			for (i = 0; i < 8; i++)
				server_ipv4.sin_zero[i] = 0;
			length = (socklen_t) sizeof(server_ipv4);
			server_ipv4.sin_port = htons((unsigned short) ((port) & 0xFFFF));
			server_ipv4.sin_addr.s_addr = htonl(INADDR_ANY);
		}
		server = bind_ipv6 ? (struct sockaddr*) &server_ipv6 : (struct sockaddr*) &server_ipv4;

#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
		(void) fcntl(sock, F_SETFD, FD_CLOEXEC);
#endif
		if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (char *) &on, sizeof on) < 0) {
			int e = errno;
			closesocket(sock);
			errno = e;
			return newErr("setsockopt unexpectedly failed: %s", strerror(errno));
		}

		if (bind(sock, server, length) == -1) {
			int e = errno;
			closesocket(sock);
			errno = e;
			return(newErr("binding to stream socket port %hu failed: %s", port, strerror(errno)));
		}

		if (getsockname(sock, server, &length) == -1) {
			int e = errno;
			closesocket(sock);
			errno = e;
			return(newErr("failed getting socket name: %s", strerror(errno)));
		}
	}

	check = getnameinfo(server, length, host, sizeof(host), sport, sizeof(sport), NI_NUMERICSERV);
	if (result)
		freeaddrinfo(result);
	if (check != 0) {
		closesocket(sock);
		return(newErr("failed getting socket name: %s", gai_strerror(check)));
	}

	/* keep queue of 5 */
	if (listen(sock, 5) == -1) {
		int e = errno;
		closesocket(sock);
		errno = e;
		return(newErr("failed setting socket to listen: %s", strerror(errno)));
	}

	Mfprintf(log, "accepting connections on TCP socket %s:%s\n", host, sport);

	*ret = sock;
	return(NO_ERR);
}

err
openConnectionUDP(int *ret, bool bind_ipv6, const char *bindaddr, unsigned short port)
{
	struct addrinfo hints;
	struct addrinfo *result, *rp;
	int sock = -1;

	char sport[10];
	char host[512];

	hints = (struct addrinfo) {
		.ai_family = bind_ipv6 ? AF_INET6 : AF_INET,
		.ai_socktype = SOCK_DGRAM, /* Datagram socket */
		.ai_flags = AI_PASSIVE,    /* For wildcard IP address */
		.ai_protocol = 0,          /* Any protocol */
		.ai_canonname = NULL,
		.ai_addr = NULL,
		.ai_next = NULL,
	};

	snprintf(sport, 10, "%hu", port);
	sock = getaddrinfo(bindaddr, sport, &hints, &result);
	if (sock != 0)
		return(newErr("failed getting address info: %s", gai_strerror(sock)));

	for (rp = result; rp != NULL; rp = rp->ai_next) {
		sock = socket(rp->ai_family, rp->ai_socktype
#ifdef SOCK_CLOEXEC
					  | SOCK_CLOEXEC
#endif
					  , rp->ai_protocol);
		if (sock == -1)
			continue;
#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
		(void) fcntl(sock, F_SETFD, FD_CLOEXEC);
#endif

		if (bind(sock, rp->ai_addr, rp->ai_addrlen) != -1)
			break; /* working */

		closesocket(sock);
	}

	if (rp == NULL) {
		freeaddrinfo(result);
		return(newErr("binding to datagram socket port %hu failed: "
					"no available address", port));
	}

	/* retrieve information from the socket */
	if(getnameinfo(rp->ai_addr, rp->ai_addrlen,
			host, sizeof(host),
			sport, sizeof(sport),
			NI_NUMERICSERV | NI_DGRAM) == 0) {
		Mfprintf(_mero_discout, "listening for UDP messages on %s:%s\n", host, sport);
	} else {
		Mfprintf(_mero_discout, "listening for UDP messages\n");
	}

	freeaddrinfo(result);

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
	if (bind(sock, (SOCKPTR) &server, sizeof(struct sockaddr_un)) == -1) {
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
