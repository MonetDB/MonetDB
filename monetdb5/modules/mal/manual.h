/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _MANUAL_H
#define _MANUAL_H
#include "gdk.h"
#include <stdarg.h>
#include <time.h>
#include "mal_resolve.h"
#include "mal_client.h"
#include "mal_exception.h"
#include "mal_debugger.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define manual_export extern __declspec(dllimport)
#else
#define manual_export extern __declspec(dllexport)
#endif
#else
#define manual_export extern
#endif

manual_export str MANUALhelp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manual_export str MANUALcreateOverview(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _MANUAL_H */
