/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/* This file contains the prototype declarations of the stream
 * functions that need special include files (sockets) */

#ifndef _STREAM_SOCKET_H_
#define _STREAM_SOCKET_H_

#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_WINSOCK_H
# include <winsock.h>
#endif

#ifndef INVALID_SOCKET
#define INVALID_SOCKET (-1)
#endif
#ifndef SOCKET_ERROR
#define SOCKET_ERROR (-1)
#endif

stream_export stream *socket_rstream(SOCKET socket, const char *name);
stream_export stream *socket_wstream(SOCKET socket, const char *name);
stream_export stream *socket_rastream(SOCKET socket, const char *name);
stream_export stream *socket_wastream(SOCKET socket, const char *name);

#endif	/* _STREAM_SOCKET_H_ */
