/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _MAL_MERGETABLE_
#define _MAL_MERGETABLE_
#include "opt_support.h"
#include "mal_interpreter.h"
#include "mal_builder.h"

extern str OPTmergetableImplementation(Client cntxt, MalBlkPtr mb,
									   MalStkPtr stk, InstrPtr p);

#endif
