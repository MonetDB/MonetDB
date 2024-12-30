/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _OPT_PROJECTIONPATH_
#define _OPT_PROJECTIONPATH_
#include "opt_support.h"
#include "mal_interpreter.h"

extern str OPTprojectionpathImplementation(Client cntxt, MalBlkPtr mb,
										   MalStkPtr stk, InstrPtr p);

#endif
