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

mal_export str RUNchoice(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str RUNvolumeCost(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str RUNcostPrediction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str RUNpickResult(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
#endif /* MAL_RUN_MEMORUN */

