/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _SEEN_CONTROL_H
#define _SEEN_CONTROL_H 1

char* control_send(
		char** ret,
		const char *host,
		int port,
		const char *database,
		const char *command,
		char wait,
		const char *pass);
char* control_send_callback(
		char** ret,
		const char *host,
		int port,
		const char *database,
		const char *command,
		void (*callback)(const void *data, size_t size, void *cb_private),
		void *cb_private,
		const char *pass);
char* control_hash(const char *pass, const char *salt);
char *control_ping(const char *host, int port, const char *pass);

#endif
