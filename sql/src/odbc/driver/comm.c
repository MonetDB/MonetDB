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

#include <mem.h>
#include "comm.h"
#include "ODBCStmt.h"

/* returns a socket descriptor (>= 0) or an error code (< 0) */
int client(char *host, int port ){
    struct sockaddr_in server;
    struct hostent * hp;
    int sock;
    int res;

    if ((sock = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
      fprintf (stderr, "client: could not open socket\n");
      return -2;
    }

    if (!(hp = gethostbyname (host))) {
      fprintf (stderr, "client: unknown host %s\n", host);
      return -3;
    }

    memset(&server, 0, sizeof(server));
    memcpy(&server.sin_addr, hp->h_addr, hp->h_length);
    server.sin_family = hp -> h_addrtype;
    server.sin_port   = htons((unsigned short)(port&0xFFFF));

    if ((res=connect (sock, (struct sockaddr *) &server, sizeof server)) < 0) {
      fprintf (stderr, "client: could not connect to server %d\n", res);
      return -4;
    }

    return sock;
}

char *readblock( stream *in ){
	int len = 0;
	bstream *s = bstream_create(in, BLOCK);
	int size = s->size, eof = 0;
	char *buf = NEW_ARRAY(char, size+1 ), *start = buf;

	while (!eof){
		if (bstream_read(s, s->size - (s->len - s->pos)) == 0)
			eof = 1;
		if ( (size-len) < s->size - (s->len - s->pos)){
			size += s->size;
			buf = RENEW_ARRAY(char, buf, size+1); 
		}
		memcpy(buf+len, s->buf+s->pos, (s->len-s->pos));
		len += (s->len-s->pos);
		buf[len] = '\0';
		s->pos = s->len;
	}
	bstream_destroy(s);
	return buf;
}

int simple_receive( stream *in, stream *out, int debug ){
	int type = 0, res = 0;
	int nRows;

	if ((res = stream_readInt(in, &type)) && type != Q_END){
		char buf[BLOCK+1], *n = buf;
		int last = 0;
		int status;

		stream_readInt(in, &status);
		if (status < 0){ /* output error */
			/* skip rest */
			int nr = bs_read_next(in,buf,&last);
			while(!last){
				int nr = bs_read_next(in,buf,&last);
			}
			return status;
		}
		nRows = status;
		if (type == Q_TABLE && nRows > 0){
			/* skip rest */
			int nr = bs_read_next(in,buf,&last);
	
			while(!last){
				fwrite( buf, nr, 1, stdout );
			}
		}
	} else if (type != Q_END){
		return -type;
	}
	return nRows;
}
