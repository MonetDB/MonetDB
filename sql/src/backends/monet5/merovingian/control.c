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
 * Copyright August 2008-2009 MonetDB B.V.
 * All Rights Reserved.
 */

#include "sql_config.h"
#include <stdio.h>
#include <unistd.h> /* close */
#include <string.h> /* strerror */
#include <sys/socket.h> /* socket */
#ifdef HAVE_SYS_UN_H
#include <sys/un.h> /* sockaddr_un */
#endif
#include <errno.h>

#define SOCKPTR struct sockaddr *

/* Sends command for database to merovingian listening at host and port.
 * If host is a path, and port is 0, a UNIX socket connection for host
 * is opened.  The response of merovingian is returned as a malloced
 * string.
 * TODO: implement TCP connect
 */
char* control_send(
		char** ret,
		char* host,
		int port,
		char* database,
		char* command)
{
	char buf[8096];
	int sock = -1;
	struct sockaddr_un server;
	size_t len;

	(void)port;
	/* UNIX socket connect */
	if ((sock = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
		snprintf(buf, sizeof(buf), "cannot open connection: %s\n",
				strerror(errno));
		return(strdup(buf));
	}
	memset(&server, 0, sizeof(struct sockaddr_un));
	server.sun_family = AF_UNIX;
	strncpy(server.sun_path, host, sizeof(server.sun_path) - 1);
	if (connect(sock, (SOCKPTR) &server, sizeof(struct sockaddr_un))) {
		snprintf(buf, sizeof(buf), "cannot connect: %s\n", strerror(errno));
		return(strdup(buf));
	}

	len = snprintf(buf, sizeof(buf), "%s %s\n", database, command);
	send(sock, buf, len, 0);
	if ((len = recv(sock, buf, sizeof(buf), 0)) <= 0)
		return(strdup("no response from merovingian\n"));
	buf[len == 0 ? 0 : len - 1] = '\0';

	close(sock);

	*ret = strdup(buf);
	return(NULL);
}
