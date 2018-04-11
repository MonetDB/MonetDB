/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _OPT_CX_
#define _OPT_CX_
#include "mal_stack.h"
#include "mal_interpreter.h"	/* for showErrors() */
#include "opt_prelude.h"
#include "opt_support.h"

mal_export str OPTevaluateImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif
