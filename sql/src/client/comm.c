/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at
 * http://monetdb.cwi.nl/Legal/MonetDBPL-1.0.html
 *
 * Software distributed under the License is distributed on an
 * "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 *
 * The Original Code is the Monet Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.
 * All Rights Reserved.
 *
 * Contributor(s):
 * 		Martin Kersten  <Martin.Kersten@cwi.nl>
 * 		Peter Boncz  <Peter.Boncz@cwi.nl>
 * 		Niels Nes  <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

#include <mem.h>
#include "comm.h"

#include <stdlib.h>
#include <sys/types.h>
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_NETDB_H
#include <netinet/in.h>
#include <netdb.h>
#endif
#include <stdio.h>
#include <signal.h>
#include <errno.h>
#if TIME_WITH_SYS_TIME
# include <sys/time.h>
# include <time.h>
#else
# if HAVE_SYS_TIME_H
#  include <sys/time.h>
# else
#  include <time.h>
# endif
#endif
#include <unistd.h>
#include <string.h>
#include <stream.h>

/* returns a socket descriptor (>= 0) or an error code (< 0) */
int client(char *host, int port ){
    struct sockaddr_in server;
    struct hostent * hp;
    int sock;
    int res;

    if (!(hp = gethostbyname (host))) {
      fprintf (stderr, "client: unknown host %s\n", host);
      return -3;
    }

    if ((sock = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
      fprintf (stderr, "client: could not open socket\n");
      return -2;
    }

    memset(&server, 0, sizeof(server));
    memcpy(&server.sin_addr, hp->h_addr, hp->h_length);
    server.sin_family = hp -> h_addrtype;
    server.sin_port   = htons((unsigned short)(port&0xFFFF));

    if ((res=connect (sock, (struct sockaddr *) &server, sizeof server)) < 0) {
      fprintf (stderr, "client: could not connect to server %d\n", res);
      (void) close(sock);
      return -4;
    }

    return sock;
}

char *readblock( stream *s ){
	int len = 0;
	int size = BLOCK + 1;
	char *buf = NEW_ARRAY(char, size ), *start = buf;

	while ((len = stream_read(s, start, 1, BLOCK)) == BLOCK){
		size += BLOCK;
		buf = RENEW_ARRAY(char, buf, size); 
		start = buf + size - BLOCK - 1;
		*start = '\0';
	}
	start += len;
	*start = '\0';
	return buf;
}

