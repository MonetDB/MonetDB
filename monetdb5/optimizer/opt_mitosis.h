/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _OPT_MITOSIS_
#define _OPT_MITOSIS_
#include "opt_prelude.h"
#include "opt_support.h"

#define MAXSLICES 1024		/* to be refined */

extern str OPTmitosisImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

#endif
