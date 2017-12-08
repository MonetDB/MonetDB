/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _DISCOVERYRUNNER_H
#define _DISCOVERYRUNNER_H 1

#include "msabaoth.h"

void broadcast(char *msg);
void registerMessageTap(int fd);
void unregisterMessageTap(int fd);
void *discoveryRunner(void *d);

typedef struct _remotedb {
	char *dbname;       /* remote database name */
	char *tag;          /* database tag, if any, default = "" */
	char *fullname;     /* dbname + tag */
	char *conn;         /* remote connection, use in redirect */
	int ttl;            /* time-to-live in seconds */
	struct _remotedb* next;
}* remotedb;

sabdb *getRemoteDB(char *database);

extern remotedb _mero_remotedbs;
extern pthread_mutex_t _mero_remotedb_lock;

#endif

/* vim:set ts=4 sw=4 noexpandtab: */
