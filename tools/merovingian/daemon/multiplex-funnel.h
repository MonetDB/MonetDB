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

#ifndef _MULTIPLEX_H
#define _MULTIPLEX_H 1

#include <pthread.h>
#include <mapi.h>
#include <stream.h>

#include "merovingian.h"

typedef struct _multiplex_database {
	Mapi conn;              /* current connection in use */
	Mapi newconn;           /* new connection we should set live (or NULL) */
	char connupdate;        /* if set, we have to cycle newconn into conn */
	char *user;
	char *pass;
	char *database;
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
	char                *pool;
	int                  dbcc;
	multiplex_database **dbcv;
	multiplex_client    *clients;
} multiplex;

err multiplexInit(multiplex **ret, char *database);
void multiplexThread(void *d);
void multiplexAddClient(multiplex *m, int sock, stream *fout, stream *fdin, char *name);
void multiplexRemoveClient(multiplex *m, multiplex_client *c);
void multiplexNotifyAddedDB(const char *database);
void multiplexNotifyRemovedDB(const char *database);

#endif

/* vim:set ts=4 sw=4 noexpandtab: */
