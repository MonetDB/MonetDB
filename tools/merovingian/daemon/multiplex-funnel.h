/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _MULTIPLEX_H
#define _MULTIPLEX_H 1

#include <stdio.h> /* FILE */

#include "mapi.h"
#include "stream.h"

#include "merovingian.h"

typedef struct _multiplex_database {
	Mapi conn;              /* current connection in use */
	Mapi newconn;           /* new connection we should set live (or NULL) */
	char connupdate;        /* if set, we have to cycle newconn into conn */
	char *user;
	char *pass;
	char *database;
	MapiHdl hdl;
} multiplex_database;

typedef struct _multiplex_client {
	char                     *name;
	int                       sock;
	stream                   *fdin;
	stream                   *fout;
	struct _multiplex_client *next;
} multiplex_client;

typedef struct _multiplex {
	pthread_t            tid;
	int                  gdklock;
	char                 shutdown;
	char                *name;
	char                *pool;
	FILE                *sout;
	FILE                *serr;
	int                  dbcc;
	multiplex_database **dbcv;
	multiplex_client    *clients;
} multiplex;

err multiplexInit(char *name, char *pattern, FILE *sout, FILE *serr);
void multiplexDestroy(char *mp);
void multiplexAddClient(char *mp, int sock, stream *fout, stream *fdin, char *name);
void multiplexRemoveClient(multiplex *m, multiplex_client *c);
void multiplexNotifyAddedDB(const char *database);
void multiplexNotifyRemovedDB(const char *database);

#endif

/* vim:set ts=4 sw=4 noexpandtab: */
