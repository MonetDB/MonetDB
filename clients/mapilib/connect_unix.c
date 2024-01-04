/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "stream.h"		/* include before mapi.h */
#include "stream_socket.h"
#include "mapi.h"
#include "mapi_prompt.h"
#include "mcrypt.h"
#include "matomic.h"
#include "mstring.h"
#include "mutils.h"

#include "mapi_intern.h"

#define MAX_SCAN (24)

MapiMsg
scan_unix_sockets(Mapi mid)
{
	struct {
		int port;
		int priority;
	} candidates[MAX_SCAN];
	int ncandidates = 0;
	DIR *dir = NULL;
	struct dirent *entry;

	const char *sockdir = msetting_string(mid->settings, MP_SOCKDIR);
	size_t len = strlen(sockdir);
	char *namebuf = malloc(len + 50);
	if (namebuf == NULL)
		return mapi_setError(mid, "malloc failed", __func__, MERROR);
	strcpy(namebuf, sockdir);
	strcpy(namebuf + len, "/.s.monetdb.PORTXXXXX");
	char *put_port_here = strrchr(namebuf, 'P');

	msettings *original = mid->settings;
	mid->settings = NULL;  // invalid state, will fix it before use and on return

	mapi_log_record(mid, "CONN", "Scanning %s for Unix domain sockets", sockdir);

	// Make a list of Unix domain sockets in /tmp
	uid_t me = getuid();
	dir = opendir(sockdir);
	if (dir) {
		while (ncandidates < MAX_SCAN && (entry = readdir(dir)) != NULL) {
			const char *basename = entry->d_name;
			if (strncmp(basename, ".s.monetdb.", 11) != 0 || basename[11] == '\0' || strlen(basename) > 20)
				continue;

			char *end;
			long port = strtol(basename + 11, &end, 10);
			if (port < 1 || port > 65535 || *end)
				continue;

			sprintf(put_port_here, "%ld", port);
			struct stat st;
			if (stat(namebuf, &st) < 0 || !S_ISSOCK(st.st_mode))
				continue;

			candidates[ncandidates].port = port;
			candidates[ncandidates++].priority = st.st_uid == me ? 0 : 1;
		}
		closedir(dir);
	}

	mapi_log_record(mid, "CONN", "Found %d Unix domain sockets", ncandidates);

	// Try those owned by us first, then all others
	for (int round = 0; round < 2; round++) {
		for (int i = 0; i < ncandidates; i++) {
			if (candidates[i].priority != round)
				continue;

			assert(!mid->connected);
			assert(mid->settings == NULL);
			mid->settings = msettings_clone(original);
			if (!mid->settings) {
				mid->settings = original;
				free(namebuf);
				return mapi_setError(mid, "malloc failed", __func__, MERROR);
			}
			msettings_error errmsg = msetting_set_long(mid->settings, MP_PORT, candidates[i].port);
			char *allocated_errmsg = NULL;
			if (!errmsg && !msettings_validate(mid->settings, &allocated_errmsg)) {
				errmsg = allocated_errmsg;
			}
			if (errmsg) {
				mapi_setError(mid, errmsg, __func__, MERROR);
				free(allocated_errmsg);
				free(namebuf);
				msettings_destroy(mid->settings);
				mid->settings = original;
				return MERROR;
			}
			MapiMsg msg = establish_connection(mid);
			if (msg == MOK) {
				// do not restore original
				msettings_destroy(original);
				free(namebuf);
				return MOK;
			} else {
				msettings_destroy(mid->settings);
				mid->settings = NULL;
				// now we're ready to try another one
			}
		}
	}

	free(namebuf);
	assert(mid->settings == NULL);
	mid->settings = original;
	mapi_log_record(mid, "CONN", "All %d Unix domain sockets failed. Falling back to TCP", ncandidates);
	return MERROR;
}


MapiMsg
connect_socket_unix(Mapi mid)
{
	const char *sockname = msettings_connect_unix(mid->settings);
	assert (*sockname != '\0');

	mapi_log_record(mid, "CONN", "Connecting to Unix domain socket %s", sockname);

	struct sockaddr_un userver;
	if (strlen(sockname) >= sizeof(userver.sun_path)) {
		return mapi_printError(mid, __func__, MERROR, "path name '%s' too long", sockname);
	}

	// Create the socket, taking care of CLOEXEC

#ifdef SOCK_CLOEXEC
	int s = socket(PF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
#else
	int s = socket(PF_UNIX, SOCK_STREAM, 0);
#endif
	if (s == INVALID_SOCKET) {
		return mapi_printError(
			mid, __func__, MERROR,
			"could not create Unix domain socket '%s': %s", sockname, strerror(errno));
	}
#if !defined(SOCK_CLOEXEC) && defined(HAVE_FCNTL)
	(void) fcntl(s, F_SETFD, FD_CLOEXEC);
#endif

	// Attempt to connect

	userver = (struct sockaddr_un) {
		.sun_family = AF_UNIX,
	};
	strcpy_len(userver.sun_path, sockname, sizeof(userver.sun_path));

	if (connect(s, (struct sockaddr *) &userver, sizeof(struct sockaddr_un)) == SOCKET_ERROR) {
		closesocket(s);
		return mapi_printError(
			mid, __func__, MERROR,
			"connect to Unix domain socket '%s' failed: %s", sockname, strerror(errno));
	}

	// Send an initial zero (not NUL) to let the server know we're not passing a file
	// descriptor.

	ssize_t n = send(s, "0", 1, 0);
	if (n < 1) {
		// used to be if n < 0 but this makes more sense
		closesocket(s);
		return mapi_printError(
			mid, __func__, MERROR,
			"could not send initial '0' on Unix domain socket: %s", strerror(errno));
	}

	return wrap_socket(mid, s);
}
