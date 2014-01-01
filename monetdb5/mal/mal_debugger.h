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

#ifndef _MAL_DEBUGGER_H
#define _MAL_DEBUGGER_H

#include "mal_scenario.h"
#include "mal_client.h"

#define MAL_DEBUGGER		/* debugger is active */

#define MAXBREAKS 32

mal_export int MDBdelay;	/* do not immediately react */
typedef struct {
	MalBlkPtr brkBlock[MAXBREAKS];
	int		brkPc[MAXBREAKS];
	int		brkVar[MAXBREAKS];
	str		brkMod[MAXBREAKS];
	str		brkFcn[MAXBREAKS];
	char	brkCmd[MAXBREAKS];
	str		brkRequest[MAXBREAKS];
	int		brkTop;
} mdbStateRecord, *mdbState;

typedef struct MDBSTATE{
	MalBlkPtr mb;
	MalStkPtr stk;
	InstrPtr p;
	int pc;
} MdbState;

mal_export void mdbSetBreakRequest(Client cntxt, MalBlkPtr mb, str request, char cmd);
mal_export int mdbSetTrap(Client cntxt, str modnme, str fcnnme, int flag);
mal_export str mdbGrab(Client cntxt, MalBlkPtr mb1, MalStkPtr stk1, InstrPtr pc1);
mal_export str mdbTrapClient(Client cntxt, MalBlkPtr mb1, MalStkPtr stk1, InstrPtr pc1);
mal_export str mdbTrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int pc);
mal_export int mdbSession(void);
mal_export void mdbDump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export void printStack(stream *f, MalBlkPtr mb, MalStkPtr s);

mal_export str runMALDebugger(Client cntxt, Symbol s);

mal_export str debugOptimizers(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export void debugLifespan(Client cntxt, MalBlkPtr mb, Lifespan span);
#endif /* _MAL_DEBUGGER_h */
