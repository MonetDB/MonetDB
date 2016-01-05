/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
