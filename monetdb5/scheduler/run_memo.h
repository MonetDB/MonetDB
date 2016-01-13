/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _RUN_MEMORUN
#define _RUN_MEMORUN
#include "mal.h"
#include "mal_interpreter.h"
#include "mal_linker.h"
#include "mal_client.h"

/* #define DEBUG_RUN_MEMORUN*/

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define run_memo_export extern __declspec(dllimport)
#else
#define run_memo_export extern __declspec(dllexport)
#endif
#else
#define run_memo_export extern
#endif

run_memo_export str RUNchoice(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
run_memo_export str RUNvolumeCost(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
run_memo_export str RUNcostPrediction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
run_memo_export str RUNpickResult(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
#endif /* MAL_RUN_MEMORUN */

