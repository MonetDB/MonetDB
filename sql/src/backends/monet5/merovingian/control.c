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
#include <stdlib.h> /* malloc, realloc */
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
 * string.  If wait is set to a non-zero value, this function will only
 * return after it has seen an EOF from the server.  This is useful with
 * multi-line responses, but can lock up for single line responses where
 * the server allows pipelining (and hence doesn't close the
 * connection).
 * TODO: implement TCP connect
 */
char* control_send(
		char** ret,
		char* host,
		int port,
		char* database,
		char* command,
		char wait)
{
	char sbuf[8096];
	char *buf;
	int sock = -1;
	struct sockaddr_un server;
	size_t len;

	(void)port;
	/* UNIX socket connect */
	if ((sock = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
		snprintf(sbuf, sizeof(sbuf), "cannot open connection: %s",
				strerror(errno));
		return(strdup(sbuf));
	}
	memset(&server, 0, sizeof(struct sockaddr_un));
	server.sun_family = AF_UNIX;
	strncpy(server.sun_path, host, sizeof(server.sun_path) - 1);
	if (connect(sock, (SOCKPTR) &server, sizeof(struct sockaddr_un))) {
		snprintf(sbuf, sizeof(sbuf), "cannot connect: %s", strerror(errno));
		return(strdup(sbuf));
	}

	len = snprintf(sbuf, sizeof(sbuf), "%s %s\n", database, command);
	send(sock, sbuf, len, 0);
	if (wait != 0) {
		size_t buflen = sizeof(sbuf);
		size_t bufpos = 0;
		char *bufp;
		bufp = buf = malloc(sizeof(char) * buflen);
		if (buf == NULL)
			return(strdup("failed to allocate memory"));
		while ((len = recv(sock, buf + bufpos, buflen - bufpos, 0)) > 0) {
			if (len == buflen - bufpos) {
				bufp = realloc(buf, sizeof(char) * buflen * 2);
				if (bufp == NULL) {
					free(buf);
					return(strdup("failed to allocate more memory"));
				}
				buf = bufp;
			}
			bufpos += len;
		}
		if (bufpos == 0)
			return(strdup("no response from merovingian"));
		buf[bufpos - 1] = '\0';
		*ret = buf;
	} else {
		if ((len = recv(sock, sbuf, sizeof(sbuf), 0)) <= 0)
			return(strdup("no response from merovingian"));
		sbuf[len - 1] = '\0';
		*ret = strdup(sbuf);
	}

	close(sock);

	return(NULL);
}
