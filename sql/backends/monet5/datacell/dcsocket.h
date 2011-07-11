/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _DCSOCKET_
#define _DCSOCKET_
#include "monetdb_config.h"
#ifdef WIN32
#include <winsock.h>
#else
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#endif
#include "mal.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include "stream_socket.h"

/* #define _DEBUG_SOCKET_  */
#define DCout GDKout

#define MYBUFSIZ 64*1024

#ifdef WIN32
#ifndef LIBDCSOCKET
#define dcsocket_export extern __declspec(dllimport)
#else
#define dcsocket_export extern __declspec(dllexport)
#endif
#else
#define dcsocket_export extern
#endif

dcsocket_export str socket_server_connect(SOCKET *sfd, int port);
dcsocket_export str socket_server_listen(SOCKET sockfd, SOCKET *newsfd);
dcsocket_export str socket_client_connect(SOCKET * sfd, char * host, int port);
dcsocket_export str socket_close(SOCKET sockfd);
#endif

#ifndef SHUT_RD
#define SHUT_RD		0
#define SHUT_WR		1
#define SHUT_RDWR	2
#endif

#ifndef ECONNRESET
#define ECONNRESET 64
#endif

