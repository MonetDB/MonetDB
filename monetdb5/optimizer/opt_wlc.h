/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _OPT_WLC_H
#define _OPT_WLC_H

#include "mal_interpreter.h"
#include "mal_scenario.h"
#include "opt_support.h"
#include "opt_prelude.h"

extern str OPTwlcImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _OPT_WLC_H */
