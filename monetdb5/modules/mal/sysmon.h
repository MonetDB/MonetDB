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
#ifndef _SYSMON_H
#define _SYSMON_H
#include "mal.h"
#include "mal_interpreter.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define sysmon_export extern __declspec(dllimport)
#else
#define sysmon_export extern __declspec(dllexport)
#endif
#else
#define sysmon_export extern
#endif

sysmon_export str SYSMONpause(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sysmon_export str SYSMONresume(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sysmon_export str SYSMONstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sysmon_export str SYSMONqueue(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _SYSMON_H */
