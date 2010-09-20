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
 * Copyright August 2008-2010 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _DISCOVERYRUNNER_H
#define _DISCOVERYRUNNER_H 1

void broadcast(char *msg);
void registerMessageTap(int fd);
void unregisterMessageTap(int fd);
void discoveryRunner(void *d);

typedef struct _remotedb {
	str dbname;       /* remote database name */
	str tag;          /* database tag, if any, default = "" */
	str fullname;     /* dbname + tag */
	str conn;         /* remote connection, use in redirect */
	int ttl;          /* time-to-live in seconds */
	struct _remotedb* next;
}* remotedb;

#endif

/* vim:set ts=4 sw=4 noexpandtab: */
