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
#include "ODBCStmt.h"

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
	int flag = 0, res = 0;
	int nRows;

	if ((res = stream_readInt(in, &flag)) && flag != COMM_DONE){
		char buf[BLOCK+1], *n = buf;
		int last = 0;
		int type;
		int status;

		stream_readInt(in, &type);
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
		if (type == QTABLE && nRows > 0){
			/* skip rest */
			int nr = bs_read_next(in,buf,&last);
	
			while(!last){
				fwrite( buf, nr, 1, stdout );
			}
		}
	} else if (flag != COMM_DONE){
		return -flag;
	}
	return nRows;
}
