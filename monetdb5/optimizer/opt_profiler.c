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

/*
 * Collect properties for beautified variable rendering
 * All variables are tagged with the schema.table.column name if possible.
 */

#include "monetdb_config.h"
#include "mal_instruction.h"
#include "mal_profiler.h"
#include "opt_profiler.h"

str
OPTprofilerImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) stk;
	(void) cntxt;

	(void) pushInt(mb, pci, 0);
	return MAL_SUCCEED;
}
