/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _SEEN_CONTROL_H
#define _SEEN_CONTROL_H 1

char* control_send(
		char** ret,
		char* host,
		int port,
		char* database,
		char* command,
		char wait,
		char* pass);
char* control_hash(char *pass, char *salt);
char control_ping(char *host, int port, char *pass);

#endif
