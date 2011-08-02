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

#ifndef _MAL_FCN_H
#define _MAL_FCN_H

#include "mal_module.h"
#include "mal_resolve.h"

typedef struct lifespan {
	int beginLifespan, endLifespan, lastUpdate;
} *Lifespan, LifespanRecord;

#define getLastUpdate(L,I)	(L[I].lastUpdate)
#define getEndLifespan(L,I)	(L[I].endLifespan)
#define getBeginLifespan(L,I)	(L[I].beginLifespan)

/* #define DEBUG_MAL_FCN */
/* #define DEBUG_CLONE */

mal_export Symbol   newFunction(str mod, str nme,int kind);
mal_export int      getPC(MalBlkPtr mb, InstrPtr p);

mal_export InstrPtr newCall(Module scope, str fcnname, int kind);
mal_export Symbol cloneFunction(Module scope, Symbol proc, MalBlkPtr mb, InstrPtr p);

mal_export Symbol   getFunctionSymbol(Module scope, InstrPtr p);
mal_export void chkFlow(MalBlkPtr mb);
mal_export void chkDeclarations(MalBlkPtr mb);
mal_export void clrDeclarations(MalBlkPtr mb);
mal_export int getBarrierEnvelop(MalBlkPtr mb);
mal_export int isLoopBarrier(MalBlkPtr mb, int pc);
mal_export int getBlockExit(MalBlkPtr mb,int pc);
mal_export int getBlockBegin(MalBlkPtr mb,int pc);

#define newLifespan(M) (Lifespan)GDKzalloc(sizeof(LifespanRecord)*(M)->vsize)
mal_export Lifespan setLifespan(MalBlkPtr mb);
mal_export void malGarbageCollector(MalBlkPtr mb);

mal_export void printFunction(stream *fd, MalBlkPtr mb, MalStkPtr stk, int flg);
mal_export void listFunction(stream *fd, MalBlkPtr mb, MalStkPtr stk, int flg, int first, int step);
mal_export void showFlowGraph(MalBlkPtr mb, MalStkPtr stk, str fname);

#include "mal_exception.h"

#define MAXDEPTH 32
#endif /*  _MAL_FCN_H*/
