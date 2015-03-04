/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef _OPT_ACCUMULATORS_
#define _OPT_ACCUMULATORS_
#include "opt_prelude.h"
#include "opt_support.h"
#include "mal_interpreter.h"

opt_export int OPTaccumulatorsImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#define OPTDEBUGaccumulators  if ( optDebug & ((lng)1 <<DEBUG_OPT_ACCUMULATORS) )

#endif
