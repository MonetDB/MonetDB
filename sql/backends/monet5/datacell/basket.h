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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _BASKETS_
#define _BASKETS_

#include "monetdb_config.h"
#include "mal.h"
#include "mal_interpreter.h"
#include "sql.h"

#ifdef WIN32
#ifndef LIBCONTAINERS
#define datacell_export extern __declspec(dllimport)
#else
#define datacell_export extern __declspec(dllexport)
#endif
#else
#define datacell_export extern
#endif

/* #define _DEBUG_DATACELL     debug this module */
#define BSKTout GDKout
#define MAXCOL 128
#define MAXBSK 64

typedef struct{
	MT_Lock lock;
	str name;	/* table that represents the basket */
	int threshold ; /* bound to determine scheduling eligibility */
	int winsize, winstride; /* sliding window operations */
	lng timeslice, timestride; /* temporal sliding window, determined by first temporal component */
	lng beat;	/* milliseconds delay */
	int colcount;
	int port;	/* port claimed */
	str *cols;
	BAT **primary;
	/* statistics */
	timestamp seen;
	int events; /* total number of events grabbed */
	int grabs; /* number of grabs */
	/* collected errors */
	BAT *errors;
} *BSKTbasket, BSKTbasketRec;

datacell_export str schema_default;
datacell_export str BSKTregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str BSKTdrop(int *ret, str *tbl);
datacell_export str BSKTreset(int *ret);
datacell_export str BSKTinventory(int *ret);
datacell_export int BSKTmemberCount(str tbl);
datacell_export int BSKTlocate(str tbl);
datacell_export str BSKTdump(int *ret);
datacell_export str BSKTgrab(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str BSKTupdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str BSKTthreshold(int *ret, str *tbl, int *sz);
datacell_export str BSKTbeat(int *ret, str *tbl, int *sz);
datacell_export str BSKTwindow(int *ret, str *tbl, int *sz, int *slide);
datacell_export str BSKTtimewindow(int *ret, str *tbl, int *sz, int *slide);
datacell_export str BSKTtable(int *ret);
datacell_export str BSKTtableerrors(int *ret);

datacell_export str BSKTlock(int *ret, str *tbl, int *delay);
datacell_export str BSKTunlock(int *ret, str *tbl);
datacell_export str BSKTlock2(int *ret, str *tbl);

datacell_export str BSKTnewbasket(sql_schema *s, sql_table *t, sql_trans *tr);
datacell_export void BSKTelements(str nme, str buf, str *schema, str *tbl);
datacell_export InstrPtr BSKTgrabInstruction(MalBlkPtr mb, str tbl);
datacell_export InstrPtr BSKTupdateInstruction(MalBlkPtr mb, str tbl);
datacell_export void BSKTtolower(char *src);

datacell_export BSKTbasketRec *baskets;
datacell_export int bsktTop, bsktLimit;
datacell_export lng usec(void);
#endif
