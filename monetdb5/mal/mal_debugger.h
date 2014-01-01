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

mal_export void mdbInit(void);
mal_export void mdbSetBreakpoint(Client cntxt, MalBlkPtr mb, int pc, char cmd);
mal_export void mdbSetBreakRequest(Client cntxt, MalBlkPtr mb, str request, char cmd);
mal_export void mdbClrBreakRequest(Client cntxt, str name);
mal_export void mdbShowBreakpoints(Client cntxt);
mal_export void mdbCommand(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int pc);
mal_export int mdbSetTrap(Client cntxt, str modnme, str fcnnme, int flag);
mal_export str mdbGrab(Client cntxt, MalBlkPtr mb1, MalStkPtr stk1, InstrPtr pc1);
mal_export str mdbTrapClient(Client cntxt, MalBlkPtr mb1, MalStkPtr stk1, InstrPtr pc1);
mal_export str mdbTrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int pc);
mal_export int mdbSession(void);
mal_export void mdbStep(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int pc);
mal_export void mdbDump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export void mdbHelp(stream *f);
mal_export void printBATelm(stream *f, int i, BUN cnt, BUN first);
mal_export void printStack(stream *f, MalBlkPtr mb, MalStkPtr s);
mal_export void printBatDetails(stream *f, int bid);
mal_export void printBatInfo(stream *f, VarPtr n, ValPtr v);
mal_export void printBatProperties(stream *f, VarPtr n, ValPtr v, str props);
mal_export void printTraceCall(stream *out, MalBlkPtr mb, MalStkPtr stk, int pc, int flags);
mal_export char BBPTraceCall(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int pc);

mal_export str runMALDebugger(Client cntxt, Symbol s);
mal_export void printBBPinfo(stream *out);

mal_export void optimizerDebug(Client cntxt, MalBlkPtr mb, str name, int actions, lng usec);
mal_export str debugOptimizers(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export void debugLifespan(Client cntxt, MalBlkPtr mb, Lifespan span);
#endif /* _MAL_DEBUGGER_h */
