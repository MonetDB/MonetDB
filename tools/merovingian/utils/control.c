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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "control.h"
#include <stdio.h>
#include <stdlib.h> /* malloc, realloc */
#include <unistd.h> /* close */
#include <string.h> /* strerror */
#include <sys/socket.h> /* socket */
#ifdef HAVE_SYS_UN_H
#include <sys/un.h> /* sockaddr_un */
#endif
#include <netdb.h>
#include <netinet/in.h>
#include <errno.h>

#define SOCKPTR struct sockaddr *

/* Sends command for database to merovingian listening at host and port.
 * If host is a path, and port is -1, a UNIX socket connection for host
 * is opened.  The response of merovingian is returned as a malloced
 * string.  If wait is set to a non-zero value, this function will only
 * return after it has seen an EOF from the server.  This is useful with
 * multi-line responses, but can lock up for single line responses where
 * the server allows pipelining (and hence doesn't close the
 * connection).
 */
char* control_send(
		char** ret,
		char* host,
		int port,
		char* database,
		char* command,
		char wait,
		char* pass)
{
	char sbuf[8096];
	char *buf;
	int sock = -1;
	size_t len;

	if (port == -1) {
		struct sockaddr_un server;
		/* UNIX socket connect */
		if ((sock = socket(PF_UNIX, SOCK_STREAM, 0)) < 0) {
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
	} else {
		struct sockaddr_in server;
		struct hostent *hp;
		char ver = '\0';

		/* TCP socket connect */
		if ((sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
			snprintf(sbuf, sizeof(sbuf), "cannot open connection: %s",
					strerror(errno));
			return(strdup(sbuf));
		}
		hp = gethostbyname(host);
		if (hp == NULL) {
			snprintf(sbuf, sizeof(sbuf), "cannot lookup hostname: %s",
					hstrerror(h_errno));
			return(strdup(sbuf));
		}
		memset(&server, 0, sizeof(struct sockaddr_in));
		server.sin_family = hp->h_addrtype;
		memcpy(&server.sin_addr, hp->h_addr_list[0], hp->h_length);
		server.sin_port = htons((unsigned short) (port & 0xFFFF));
		if (connect(sock, (SOCKPTR) &server, sizeof(struct sockaddr_in)) < 0) {
			snprintf(sbuf, sizeof(sbuf), "cannot connect: %s", strerror(errno));
			return(strdup(sbuf));
		}
		
		/* perform login ritual */
		if ((len = recv(sock, sbuf, sizeof(sbuf), 0)) <= 0) {
			snprintf(sbuf, sizeof(sbuf), "no response from merovingian");
			return(strdup(sbuf));
		}
		/* we only understand merovingian:1 and :2 */
		if (strncmp(sbuf, "merovingian:1:", strlen("merovingian:1:")) != 0 &&
				strncmp(sbuf, "merovingian:2:", strlen("merovingian:2:")) != 0)
		{
			if (len > 2 &&
					(strstr(sbuf + 2, ":BIG:") != NULL ||
					 strstr(sbuf + 2, ":LIT:") != NULL))
			{
				snprintf(sbuf, sizeof(sbuf), "cannot connect: "
						"server looks like a mapi server, "
						"are you using monetdbd's mapi port (default 50000) "
						"instead of its control port (default 50001)?");
			} else {
				snprintf(sbuf, sizeof(sbuf), "cannot connect: "
						"unsupported merovingian server");
			}
			return(strdup(sbuf));
		}
		buf = strchr(sbuf + strlen("merovingian:1:"), ':');
		if (buf != NULL)
			*buf = '\0';
		buf = sbuf + strlen("merovingian:1:");
		ver = buf[-2]; /* store the version for later */

		buf = control_hash(pass, buf);

		switch (ver) {
			case '1':
				len = snprintf(sbuf, sizeof(sbuf), "%s\n", buf);
			break;
			case '2':
				/* in the library we only support control mode for now */
				len = snprintf(sbuf, sizeof(sbuf), "%s:control\n", buf);
			break;
		}
		free(buf);
		send(sock, sbuf, len, 0);

		if ((len = recv(sock, sbuf, sizeof(sbuf), 0)) <= 0)
			return(strdup("no response from merovingian"));
		sbuf[len - 1] = '\0';
		if (strcmp(sbuf, "OK") != 0)
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
				buflen *= 2;
				bufp = realloc(buf, sizeof(char) * buflen);
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

/**
 * Returns a hash for pass and salt, to use when logging in on a remote
 * merovingian.  The result is a malloced string.
 */
char *
control_hash(char *pass, char *salt) {
	unsigned int ho;
	unsigned int h = 0;
	char buf[32];

	/* use a very simple hash function designed for a single int val
	 * (hash buckets), we can make this more interesting if necessary in
	 * the future.
	 * http://www.cs.hmc.edu/~geoff/classes/hmc.cs070.200101/homework10/hashfuncs.html */

	while (*pass != '\0') {
		ho = h & 0xf8000000;
		h <<= 5;
		h ^= ho >> 27;
		h ^= (unsigned int)(*pass);
		pass++;
	}

	while (*salt != '\0') {
		ho = h & 0xf8000000;
		h <<= 5;
		h ^= ho >> 27;
		h ^= (unsigned int)(*salt);
		salt++;
	}

	snprintf(buf, sizeof(buf), "%u", h);
	return(strdup(buf));
}

/**
 * Returns if the merovingian server at host, port using pass is alive
 * or not.  If alive, 0 is returned.  The same rules for host, port and
 * pass hold as for control_send regarding the use of a UNIX vs TCP
 * socket.
 */
char
control_ping(char *host, int port, char *pass) {
	char *res;
	char *err;
	if ((err = control_send(&res, host, port, "", "ping", 0, pass)) == NULL) {
		if (res != NULL)
			free(res);
		return(0);
	}
	free(err);
	return(1);
}
