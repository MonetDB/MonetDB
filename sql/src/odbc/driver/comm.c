/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
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
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <signal.h>
#include <errno.h>
#include <sys/time.h>
#include <unistd.h>
#include <stream.h>

#include <mem.h>
#include "comm.h"

/* returns a socket descriptor (>= 0) or an error code (< 0) */
int client(char *host, int port ){
    struct sockaddr_in server;
    struct hostent * host_info;
    int sock;

    if ((sock = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
      fprintf (stderr, "client: could not open socket");
      return -2;
    }

    if (!(host_info = gethostbyname (host))) {
      fprintf (stderr, "client: unknown host %s", host);
      return -3;
    }

    server.sin_family = host_info -> h_addrtype;
    memcpy ((char *) &server.sin_addr,
            host_info -> h_addr,
            host_info -> h_length);
    server.sin_port = htons (port);

    if (connect (sock, (struct sockaddr *) &server, sizeof server) < 0) {
      fprintf (stderr, "client: could not connect to server");
      return -4;
    }

    return sock;
}

char *readblock( stream *s ){
	int len = 0;
	int size = BLOCK + 1;
	char *buf = NEW_ARRAY(char, size ), *start = buf;

	while ((len = s->read(s, start, 1, BLOCK)) == BLOCK){
		size += BLOCK;
		buf = RENEW_ARRAY(char, buf, size); 
		start = buf + size - BLOCK - 1;
		*start = '\0';
	}
	start += len;
	*start = '\0';
	return buf;
}

