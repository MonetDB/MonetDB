/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _MAL_EMPTYBIND_
#define _MAL_EMPTYBIND_
#include "opt_prelude.h"
#include "opt_support.h"

mal_export str OPTemptybindImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

#define OPTDEBUGemptybind  if ( optDebug & ((lng) 1 <<DEBUG_OPT_EMPTYBIND) )
//#define OPTDEBUGemptybind  if (1)

#endif
