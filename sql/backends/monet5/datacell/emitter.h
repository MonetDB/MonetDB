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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _EMITTER_
#define _EMITTER_
#include "mal_interpreter.h"
#include "tablet.h"
#include "mtime.h"
#include "basket.h"

/* #define _DEBUG_EMITTER_ */
#define EMout GDKout

typedef struct EMITTER {
	str name;
	str host;
	int port;
	int mode;   	/* active/passive */
	int protocol;   /* event protocol UDP,TCP,CSV */
	int bskt;   	/* connected to a basket */
	int status;
	int delay; 		/* control the delay between attempts to connect */
	int lck;
	SOCKET sockfd;
	SOCKET newsockfd;
	stream *emitter;
	str error;
	MT_Id pid;
	/* statistics */
	timestamp lastseen;
	int cycles;		/* how often emptied */
	int pending;		/* pending events */
	int sent;
	Tablet table;
	struct EMITTER *nxt, *prv;
} EMrecord, *Emitter;

#ifdef WIN32
#ifndef LIBDATACELL
#define adapters_export extern __declspec(dllimport)
#else
#define adapters_export extern __declspec(dllexport)
#endif
#else
#define adapters_export extern
#endif

adapters_export str EMemitterStart(int *ret, str *tbl, str *host, int *port);
adapters_export str EMemitterPause(int *ret, str *nme);
adapters_export str EMemitterResume(int *ret, str *nme);
adapters_export str EMemitterStop(int *ret, str *nme);
adapters_export Emitter EMfind(str nme);
adapters_export str EMpause(int *ret);
adapters_export str EMresume(int *ret);
adapters_export str EMstop(int *ret);
adapters_export str EMdump(void);
adapters_export str EMtable(int *nameId, int *hostId, int *portId, int *protocolId, int *mode, int *statusId, int *seenId, int *cyclesId, int *sentId, int *pendingId);

#endif

