/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/* This file contains the prototype declarations of the stream
 * functions that need special include files (sockets) */

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
