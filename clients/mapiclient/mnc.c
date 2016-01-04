/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/**
 * mnc
 * Fabian Groffen
 *
 * MCL netcat
 * Simple utility meant to measure the protocol overhead incurred by our
 * stream library compared to "plain" netcat (nc).
 */

#include "monetdb_config.h"
#include "monet_options.h"
#include <mapi.h>
#include <stream.h>
#include <stream_socket.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#ifdef HAVE_PTHREAD_H
#include <pthread.h>
#endif
#include <sys/types.h>
#ifdef HAVE_SYS_SOCKET_H
# include <sys/socket.h>
#endif
#ifdef NATIVE_WIN32
# include <winsock.h>
#endif
#ifdef HAVE_NETDB_H
# include <netdb.h>
#endif
#ifdef HAVE_NETINET_IN_H
# include <netinet/in.h>
#endif

#ifndef HAVE_GETOPT_LONG
# include "monet_getopt.h"
#else
# ifdef HAVE_GETOPT_H
#  include "getopt.h"
# endif
#endif

#define SOCKPTR struct sockaddr *
#ifdef HAVE_SOCKLEN_T
#define SOCKLEN socklen_t
#else
#define SOCKLEN int
#endif


static void
usage(void)
{
	fprintf(stderr, "mnc [options] destination port\n");
	fprintf(stderr, "  -l | --listen   listen for connection instead\n");
	/* TODO
	fprintf(stderr, "  -u | --udp      use UDP instead of TCP\n");
	fprintf(stderr, "  -g | --gzip     use gzip stream wrapper\n");
	fprintf(stderr, "  -j | --bzip2    use bzip2 stream wrapper\n");
	fprintf(stderr, "  -b | --buffer   use buffered stream\n");
	*/
	fprintf(stderr, "  -B | --block    use block stream\n");
}

int
main(int argc, char **argv)
{
	int a = 1;
	char *host = NULL;
	int port = 0;
	char clisten = 0;
	/* char udp = 0; */
	/* char zip = 0; */
	/* char buffer = 0; */
	char block = 0;
	SOCKET s = INVALID_SOCKET;
	stream *in = NULL;
	stream *out = NULL;
	char buf[8096];
	ssize_t len;
	fd_set fds;
	char seeneof = 0;
	char seenflush = 0;

	static struct option long_options[8] = {
		{ "listen", 0, 0, 'l' },
		{ "udp", 0, 0, 'u' },
		{ "gzip", 0, 0, 'g' },
		{ "bzip2", 0, 0, 'j' },
		{ "buffer", 0, 0, 'b' },
		{ "block", 0, 0, 'B' },
		{ "help", 0, 0, '?' },
		{ 0, 0, 0, 0 }
	};
	while (1) {
		int option_index = 0;
		int c = getopt_long(argc, argv, "lugjbB?h",
			long_options, &option_index);
		if (c == -1)
			break;
		switch (c) {
			case 'l':
				clisten = 1;
				break;
			case 'u':
				/* udp = 1; */
				break;
			case 'g':
				/* zip = 1; */
				break;
			case 'j':
				/* zip = 2; */
				break;
			case 'b':
				/* buffer = 1; */
				break;
			case 'B':
				block = 1;
				break;
			default:
				usage();
				exit(0);
		}
	}

	a = optind;
	if (argc - a != 2) {
		fprintf(stderr, "%s: need destination and port arguments\n", argv[0]);
		usage();
		exit(-1);
	}

	host = argv[a++];
	port = atoi(argv[a]);
	
	/* call the stream wrappers based on the user's choice, stream
	 * everything from/to stdin/stdout */

	if (!clisten) {
#ifdef HAVE_GETADDRINFO
		struct addrinfo hints, *res, *rp;
		char sport[32];
		int ret;

		snprintf(sport, sizeof(sport), "%d", port & 0xFFFF);

		memset(&hints, 0, sizeof(hints));
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_protocol = IPPROTO_TCP;
		ret = getaddrinfo(host, sport, &hints, &res);
		if (ret) {
			fprintf(stderr, "getaddrinfo failed: %s\n", gai_strerror(ret));
			exit(1);
		}
		for (rp = res; rp; rp = rp->ai_next) {
			s = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
			if (s == INVALID_SOCKET)
				continue;
			if (connect(s, rp->ai_addr, (socklen_t) rp->ai_addrlen) != SOCKET_ERROR)
				break;  /* success */
			closesocket(s);
		}
		freeaddrinfo(res);
		if (rp == NULL) {
			fprintf(stderr, "could not connect to %s:%s: %s\n",
					host, sport, strerror(errno));
			exit(1);
		}
#else
		struct sockaddr_in server;
		struct hostent *hp;
		struct sockaddr *serv = (struct sockaddr *) &server;

		if ((hp = gethostbyname(host)) == NULL) {
			fprintf(stderr, "gethostbyname failed: %s\n", errno ? strerror(errno) : hstrerror(h_errno));
			exit(1);
		}
		memset(&server, 0, sizeof(server));
		memcpy(&server.sin_addr, hp->h_addr_list[0], hp->h_length);
		server.sin_family = hp->h_addrtype;
		server.sin_port = htons((unsigned short) (port & 0xFFFF));
		s = socket(server.sin_family, SOCK_STREAM, IPPROTO_TCP);

		if (s == INVALID_SOCKET) {
			fprintf(stderr, "opening socket failed: %s\n", strerror(errno));
			exit(1);
		}

		if (connect(s, serv, sizeof(server)) == SOCKET_ERROR) {
			fprintf(stderr,
				 "initiating connection on socket failed: %s\n",
				 strerror(errno));
			exit(1);
		}
#endif
	} else {
		struct sockaddr_in server;
		SOCKLEN length = 0;
		SOCKET sock = INVALID_SOCKET;
		int on = 1;
		int i = 0;

		if (port <= 0 || port > 65535) {
			fprintf(stderr, "invalid port: %d\n", port);
			exit(1);
		}

		if ((sock = socket(AF_INET, SOCK_STREAM, 0)) == INVALID_SOCKET) {
			fprintf(stderr, "failed to create socket: %s\n", strerror(errno));
			exit(1);
		}

		setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (char *) &on, sizeof on);

		server.sin_family = AF_INET;
		server.sin_addr.s_addr = htonl(INADDR_ANY);
		for (i = 0; i < 8; i++)
			server.sin_zero[i] = 0;
		length = (SOCKLEN) sizeof(server);

		server.sin_port = htons((unsigned short) ((port) & 0xFFFF));
		if (bind(sock, (SOCKPTR) &server, length) == SOCKET_ERROR) {
			fprintf(stderr, "bind to port %d failed: %s\n",
					port, strerror(errno));
			exit(1);
		}

		listen(sock, 1);
		if ((s = accept(sock, (SOCKPTR)0, (socklen_t *)0)) == INVALID_SOCKET) {
			fprintf(stderr, "failed to accept connection: %s\n",
					strerror(errno));
			exit(1);
		}
	}

	out = socket_wastream(s, "ascii write stream");
	in = socket_rastream(s, "ascii read stream");

	if (block) {
		out = block_stream(out);
		in = block_stream(in);
	}

	while (1) {
		FD_ZERO(&fds);
		FD_SET(s, &fds);
		FD_SET(0, &fds);

		select((int)s + 1, &fds, NULL, NULL, NULL);
		if (FD_ISSET(s, &fds)) {
			if ((len = mnstr_read(in, buf, 1, sizeof(buf))) > 0) {
				/* on Windows: unsigned int,
				 * elsewhere: size_t, but then
				 * unsigned int shouldn't harm */
				if (!write(1, buf, (unsigned int) len))
					exit(2);
				seenflush = 0;
			} else {
				/* flush or error */
				if (!seenflush) {
					seenflush = 1;
				} else {
					break;
				}
			}
		}
		if (FD_ISSET(0, &fds)) {
			if ((len = read(0, buf, sizeof(buf))) > 0) {
				mnstr_write(out, buf, (size_t) len, 1);
				seeneof = 0;
			} else if (len == 0) {
				/* EOF */
				if (!seeneof) {
					mnstr_flush(out);
					seeneof = 1;
				} else {
					break;
				}
			} else {
				/* error */
				break;
			}
		}
	}

	mnstr_flush(out);
	mnstr_destroy(in);
	mnstr_destroy(out);

	return 0;
}
