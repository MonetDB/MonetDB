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

#include "stream.h"
#include "stream_socket.h"

#include "merovingian.h"
#include "connections.h"

static struct in6_addr ipv6_any_addr = IN6ADDR_ANY_INIT;

err
openConnectionTCP(int *ret, bool bind_ipv6, const char *bindaddr, unsigned short port, FILE *log)
{
	struct sockaddr_in server_ipv4;
	struct sockaddr_in6 server_ipv6;
	struct addrinfo *rp = NULL;
	int sock = -1, check = 0;
	socklen_t length = 0;
	int on = 1;
	int i = 0;
	struct hostent *hoste;
	char *host = NULL;
	char sport[16];
	char hostip[24];
	char ghost[512];

	snprintf(sport, 16, "%hu", port);
	if (bindaddr) {
		struct addrinfo *result;
		struct addrinfo hints = (struct addrinfo) {
			.ai_family = bind_ipv6 ? AF_INET6 : AF_INET,
			.ai_socktype = SOCK_STREAM,
			.ai_flags = AI_PASSIVE,
			.ai_protocol = IPPROTO_TCP,
			.ai_canonname = NULL,
			.ai_addr = NULL,
			.ai_next = NULL,
		};

		check = getaddrinfo(bindaddr, sport, &hints, &result);
		if (check != 0)
			return newErr("cannot find host %s with error: %s", bindaddr, strerror(errno));

		for (rp = result; rp != NULL; rp = rp->ai_next) {
			sock = socket(rp->ai_family, rp->ai_socktype
#ifdef SOCK_CLOEXEC
						 | SOCK_CLOEXEC
#endif
					, rp->ai_protocol);
			if (sock == -1)
				continue;
#ifndef SOCK_CLOEXEC
			(void) fcntl(sock, F_SETFD, FD_CLOEXEC);
#endif

			if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (char *) &on, sizeof on) < 0) {
				closesocket(sock);
				return newErr("setsockopt unexpectedly failed: %s", strerror(errno));
			}

			if (bind(sock, rp->ai_addr, rp->ai_addrlen) != -1)
				break; /* working */
		}
		if (rp == NULL) {
			if (sock != -1)
				closesocket(sock);
			return newErr("cannot bind to host %s", bindaddr);
		}
	} else {
		sock = socket(bind_ipv6 ? AF_INET6 : AF_INET, SOCK_STREAM
#ifdef SOCK_CLOEXEC
					| SOCK_CLOEXEC
#endif
				, 0);
		if (sock == -1)
			return(newErr("creation of stream socket failed: %s", strerror(errno)));

		if (bind_ipv6) {
			server_ipv6.sin6_family = AF_INET6;
			server_ipv6.sin6_flowinfo = 0;
			server_ipv6.sin6_scope_id = 0;
			length = (socklen_t) sizeof(server_ipv6);
			server_ipv6.sin6_port = htons((unsigned short) ((port) & 0xFFFF));
			memcpy(server_ipv6.sin6_addr.s6_addr, &ipv6_any_addr, sizeof(struct in6_addr));
		} else {
			server_ipv4.sin_family = AF_INET;
			for (i = 0; i < 8; i++)
				server_ipv4.sin_zero[i] = 0;
			length = (socklen_t) sizeof(server_ipv4);
			server_ipv4.sin_port = htons((unsigned short) ((port) & 0xFFFF));
			server_ipv4.sin_addr.s_addr = htonl(INADDR_ANY);
		}

#ifndef SOCK_CLOEXEC
		(void) fcntl(sock, F_SETFD, FD_CLOEXEC);
#endif
		if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (char *) &on, sizeof on) < 0) {
			closesocket(sock);
			return newErr("setsockopt unexpectedly failed: %s", strerror(errno));
		}

		if (bind(sock, bind_ipv6 ? (SOCKPTR) &server_ipv6 : (SOCKPTR) &server_ipv4, length) == -1) {
			closesocket(sock);
			return(newErr("binding to stream socket port %hu failed: %s", port, strerror(errno)));
		}

		if (getsockname(sock, bind_ipv6 ? (SOCKPTR) &server_ipv6 : (SOCKPTR) &server_ipv4, &length) == -1) {
			closesocket(sock);
			return(newErr("failed getting socket name: %s", strerror(errno)));
		}
	}

	if (bindaddr) {
		if (getnameinfo(rp->ai_addr, rp->ai_addrlen, ghost, sizeof(ghost), sport, sizeof(sport), NI_NUMERICSERV) != 0) {
			closesocket(sock);
			return(newErr("failed getting socket name: %s", strerror(errno)));
		}
		host = ghost;
	} else {
		if (bind_ipv6)
			hoste = gethostbyaddr(&server_ipv6.sin6_addr.s6_addr, sizeof(server_ipv6.sin6_addr.s6_addr),
								  server_ipv6.sin6_family);
		else
			hoste = gethostbyaddr(&server_ipv4.sin_addr.s_addr, sizeof(server_ipv4.sin_addr.s_addr),
								  server_ipv4.sin_family);
		if (hoste == NULL) {
			if (bind_ipv6) {
				snprintf(hostip, sizeof(hostip),
						"%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x",
						 (int)server_ipv6.sin6_addr.s6_addr[0],  (int)server_ipv6.sin6_addr.s6_addr[1],
						 (int)server_ipv6.sin6_addr.s6_addr[2],  (int)server_ipv6.sin6_addr.s6_addr[3],
						 (int)server_ipv6.sin6_addr.s6_addr[4],  (int)server_ipv6.sin6_addr.s6_addr[5],
						 (int)server_ipv6.sin6_addr.s6_addr[6],  (int)server_ipv6.sin6_addr.s6_addr[7],
						 (int)server_ipv6.sin6_addr.s6_addr[8],  (int)server_ipv6.sin6_addr.s6_addr[9],
						 (int)server_ipv6.sin6_addr.s6_addr[10], (int)server_ipv6.sin6_addr.s6_addr[11],
						 (int)server_ipv6.sin6_addr.s6_addr[12], (int)server_ipv6.sin6_addr.s6_addr[13],
						 (int)server_ipv6.sin6_addr.s6_addr[14], (int)server_ipv6.sin6_addr.s6_addr[15]);
			} else {
				snprintf(hostip, sizeof(hostip), "%u.%u.%u.%u",
						 (unsigned) ((ntohl(server_ipv4.sin_addr.s_addr) >> 24) & 0xff),
						 (unsigned) ((ntohl(server_ipv4.sin_addr.s_addr) >> 16) & 0xff),
						 (unsigned) ((ntohl(server_ipv4.sin_addr.s_addr) >> 8) & 0xff),
						 (unsigned) (ntohl(server_ipv4.sin_addr.s_addr) & 0xff));
			}
			host = hostip;
		} else {
			host = hoste->h_name;
		}
	}

	/* keep queue of 5 */
	if(listen(sock, 5) == -1) {
		closesocket(sock);
		return(newErr("failed setting socket to listen: %s", strerror(errno)));
	}

	Mfprintf(log, "accepting connections on TCP socket %s:%s\n", host, sport);

	*ret = sock;
	return(NO_ERR);
}

err
openConnectionUDP(int *ret, const char *bindaddr, unsigned short port)
{
	struct addrinfo hints;
	struct addrinfo *result, *rp;
	int sock = -1;

	char sport[10];
	char host[512];

	hints = (struct addrinfo) {
		.ai_family = AF_INET,      /* Allow IPv4 only (broadcasting) */
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
#ifndef SOCK_CLOEXEC
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
#ifndef SOCK_CLOEXEC
	(void) fcntl(sock, F_SETFD, FD_CLOEXEC);
#endif

	server = (struct sockaddr_un) {
		.sun_family = AF_UNIX,
	};
	strncpy(server.sun_path, path, sizeof(server.sun_path) - 1);

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
