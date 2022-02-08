/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _MAL_MINIMAL_PIPE_
#define _MAL_MINIMAL_PIPE_
#include "opt_prelude.h"
#include "opt_support.h"
#include "mal_instruction.h"

extern str OPTminimalfastImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
extern str OPTdefaultfastImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

#endif
