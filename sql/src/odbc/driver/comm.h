/*
 * This code was created by Peter Harvey (mostly during Christmas 98/99).
 * This code is LGPL. Please ensure that this message remains in future
 * distributions and uses of this code (thats about all I get out of it).
 * - Peter Harvey pharvey@codebydesign.com
 * 
 * This file has been modified for the MonetDB project.  See the file
 * Copyright in this directory for more information.
 */

#ifndef _COMM_H_
#define _COMM_H_

#include <stream.h>

int client(char *host, int port);
char *readblock(stream *s);
int simple_receive(stream *rs, stream *out, int debug);

#endif /*_COMM_H_*/
