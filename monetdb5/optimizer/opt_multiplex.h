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

#ifndef _OPT_MULTIPLEX_H_
#define _OPT_MULTIPLEX_H_
#include "mal.h"
#include "mal_builder.h"
#include "opt_support.h"

extern str OPTmultiplexImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
									  InstrPtr pci);
extern str OPTmultiplexSimple(Client cntxt, MalBlkPtr mb);

#endif
