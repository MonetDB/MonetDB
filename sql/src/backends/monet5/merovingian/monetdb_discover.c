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

static void
command_discover(int argc, char *argv[])
{
	char path[8096];
	int sock = -1;
	struct sockaddr_un server;
	char buf[256];
	char *p, *q;
	int len;
	int pos;
	size_t twidth = TERMWIDTH;
	char location[twidth + 1];
	char *match = NULL;
	size_t numlocs = 50;
	size_t posloc = 0;
	size_t loclen = 0;
	char **locations = malloc(sizeof(char*) * numlocs);

	/* if Merovingian isn't running, there's not much we can do */
	if (mero_running == 0) {
		fprintf(stderr, "discover: cannot perform: MonetDB Database Server "
				"(merovingian) is not running\n");
		exit(1);
	}

	if (argc == 0) {
		exit(2);
	} else if (argc > 2) {
		/* print help message for this command */
		command_help(2, &argv[-1]);
		exit(1);
	} else if (argc == 2) {
		match = argv[1];
	}

	snprintf(path, 8095, "%s/.merovingian_control", dbfarm);
	path[8095] = '\0';

	if ((sock = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
		fprintf(stderr, "discover: cannot open connection: %s\n",
				strerror(errno));
		exit(2);
	}
	memset(&server, 0, sizeof(struct sockaddr_un));
	server.sun_family = AF_UNIX;
	strncpy(server.sun_path, path, sizeof(server.sun_path) - 1);
	if (connect(sock, (SOCKPTR) &server, sizeof(struct sockaddr_un))) {
		fprintf(stderr, "discover: cannot connect: %s\n", strerror(errno));
		exit(2);
	}

 	/* Send the pass phrase to unlock the information available in
	 * merovingian.  Anelosimus eximius is a social species of spiders,
	 * which help each other, just like merovingians do among each
	 * other. */
	len = snprintf(buf, sizeof(buf), "anelosimus eximius\n");
	send(sock, buf, len, 0);
	pos = 0;
	len = 0;
	buf[len] = '\0';
	do {
		if ((p = strchr(buf, '\n')) == NULL) {
			if (len == sizeof(buf) - 1) {
				/* no newline in this block, too large, warn and discard */
				printf("discover: WARNING: discarding (too long?) line: %s\n",
						buf);
				pos = 0;
			} else {
				pos = len;
			}
			len = recv(sock, buf + pos, sizeof(buf) - 1 - pos, 0);
			/* if there is no more... */
			if (len == 0) {
				break;
			} else if (len < 0) {
				fprintf(stderr, "discover: error while reading "
						"from merovingian\n");
				exit(2);
			}
			len += pos;
			buf[len] = '\0';
			continue;
		}
		*p++ = '\0';

		if ((q = strchr(buf, '\t')) == NULL) {
			/* doesn't look correct */
			printf("discover: WARNING: discarding incorrect line: %s\n", buf);
			len -= p - buf;
			memmove(buf, p, len + 1 /* include \0 */);
			continue;
		}
		*q++ = '\0';

		snprintf(path, sizeof(path), "%s%s", q, buf);

		if (match == NULL || glob(match, path)) {
			/* cut too long location name */
			abbreviateString(location, path, twidth);
			/* store what we found */
			if (posloc == numlocs)
				locations = realloc(locations,
						sizeof(char) * (numlocs = numlocs * 2));
			locations[posloc++] = strdup(location);
			if (strlen(location) > loclen)
				loclen = strlen(location);
		}

		/* move it away */
		len -= p - buf;
		memmove(buf, p, len + 1 /* include \0 */);
	} while (1);

	close(sock);

	if (posloc > 0) {
		printf("%*slocation\n",
				(int)(loclen - 8 /* "location" */ - ((loclen - 8) / 2)), "");
		/* could qsort the array here but we don't :P */
		for (loclen = 0; loclen < posloc; loclen++) {
			printf("%s\n", locations[loclen]);
			free(locations[loclen]);
		}
	}

	free(locations);
}

