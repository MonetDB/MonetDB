/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _OPT_FACTORIZE_
#define _OPT_FACTORIZE_
#include "opt_prelude.h"
#include "opt_support.h"

mal_export int OPTfactorizeImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#define OPTDEBUGfactorize  if ( optDebug & ((lng) 1 <<DEBUG_OPT_FACTORIZE) )

#endif
