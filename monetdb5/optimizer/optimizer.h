/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _OPTIMIZER_H
#define _OPTIMIZER_H

#include "mal_interpreter.h"
#include "mal_scenario.h"
#include "mal_namespace.h"
#include "opt_support.h"
#include "opt_prelude.h"

extern str optimizer_prelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
extern str optimizer_epilogue(void *ret);
extern str QOToptimize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _OPTIMIZER_H */
