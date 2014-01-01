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

#ifndef _MAL_RUNTIME_H
#define _MAL_RUNTIME_H

#include "mal.h"
#include "mal_client.h"
#include "mal_instruction.h"

/* During MAL interpretation we collect performance event data.
 * Their management is orchestrated from here.
*/
typedef struct{
	lng newclk;
	int ppc;
	lng tcs;
	lng oublock, inblock;
	struct Mallinfo memory;
} *RuntimeProfile, RuntimeProfileRecord;

void runtimeProfileInit(MalBlkPtr mb, RuntimeProfile prof, int initmemory);
void runtimeProfileBegin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int stkpc, RuntimeProfile prof, int start);
void runtimeProfileExit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, RuntimeProfile prof);
void runtimeTiming(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int tid, MT_Lock *lock, RuntimeProfile prof);
lng getVolume(MalStkPtr stk, InstrPtr pci, int rd);
void displayVolume(Client cntxt, lng vol);
lng getFootPrint(MalBlkPtr mb, MalStkPtr stk);
#endif
