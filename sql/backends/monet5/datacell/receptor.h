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

/*
 * @-
 * @+ Implementation
 * The implementation is inspired by the tablet module.
 */
#ifndef _RECEPTOR_
#define _RECEPTOR_
#include "mal_interpreter.h"
#include "tablet.h"
#include "mtime.h"
#include "basket.h"

/* #define _DEBUG_RECEPTOR_*/
#define RCout GDKout

typedef struct RECEPTOR {
	str name;
	str host;
	int port;
	int mode;   	/* active/passive */
	int protocol;   /* event protocol UDP,TCP,CSV */
	int bskt;   	/* connected to a basket */
	int status;
	int delay;  	/* control the delay between attempts to connect */
	int lck;
	str scenario;   /* use a scenario file */
	int sequence;   /* repetition count */
	str modnme, fcnnme; /* generic receptor generators */
	SOCKET sockfd;
	SOCKET newsockfd;
	str error;  /* what went wrong */
	MT_Id pid;
	/* statistics */
	timestamp lastseen;
	int cycles;		/* how often emptied */
	int pending;		/* pending events */
	int received;
	Tablet table;   /* tuple input structure */
	struct RECEPTOR *nxt, *prv;
} RCrecord, *Receptor;

#ifdef WIN32
#ifndef LIBDATACELL
#define adapters_export extern __declspec(dllimport)
#else
#define adapters_export extern __declspec(dllexport)
#endif
#else
#define adapters_export extern
#endif

adapters_export str RCreceptorStart(int *ret, str *tbl, str *host, int *port);
adapters_export str RCreceptorPause(int *ret, str *nme);
adapters_export str RCreceptorResume(int *ret, str *nme);
adapters_export str RCreceptorStop(int *ret, str *nme);
adapters_export Receptor RCfind(str nme);
adapters_export str RCpause(int *ret);
adapters_export str RCresume(int *ret);
adapters_export str RCstop(int *ret);
adapters_export str RCscenario(int *ret, str *nme, str *fnme, int *seq);
adapters_export str RCgenerator(int *ret, str *nme, str *modnme, str *fcnnme);
adapters_export str RCdump(void) ;
adapters_export str RCtable(int *nameId, int *hostId, int *portId, int *protocolId, int *mode, int *statusId, int *seenId, int *cyclesId, int *receivedId, int *pendingId);
#endif

