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
 * Portions created by CWI are Copyright (C) 1997-2007 CWI.
 * All Rights Reserved.
 */

/* This file contains the prototype declarations of the stream
 * functions that need special include files (sockets and SSL) */

#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef NATIVE_WIN32
# include <winsock.h>
#endif

#ifndef INVALID_SOCKET
#define INVALID_SOCKET (-1)
#endif

stream_export stream *socket_rstream(SOCKET socket, const char *name);
stream_export stream *socket_wstream(SOCKET socket, const char *name);
stream_export stream *socket_rastream(SOCKET socket, const char *name);
stream_export stream *socket_wastream(SOCKET socket, const char *name);

#ifdef HAVE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>

stream_export stream *ssl_rstream(SSL * ssl, const char *name);
stream_export stream *ssl_wstream(SSL * ssl, const char *name);
stream_export stream *ssl_rastream(SSL * ssl, const char *name);
stream_export stream *ssl_wastream(SSL * ssl, const char *name);
#endif
