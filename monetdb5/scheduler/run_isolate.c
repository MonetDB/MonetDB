/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * @f run_isolate
 * @a M. Kersten
 * @+ Run isolation
 * Run isolation involves making available a private copy of the MAL
 * block being executed for further massaging, e.g. code replacements
 * or flow-of-control adjustments.
 * These changes should be confined to a single execution. The next time around
 * there may be a different situation to take care off. This is achieved by
 * replacing the current program with a private copy.
 *
 * The easiest way is to duplicate the MAL program and assign the old
 * version to its history. This way any reference to individual instructions
 * remain valid and the result of the schedule action can be inspected
 * with the debugger.
 * Its lifetime then is identical to that of the main program call.
 *
 * @end example
 * This function with its history remain available as long as f()
 * is defined.
 */
/*
 * @+ Isolation implementation
 */
#include "monetdb_config.h"
#include "mal.h"
#include "mal_instruction.h"
#include "mal_client.h"
#include "mal_exception.h"

static str
RUNisolation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) cntxt;
	(void) stk;
	addtoMalBlkHistory(mb);
	removeInstruction(mb, p);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func run_isolate_init_funcs[] = {
 pattern("run_isolate", "isolation", RUNisolation, false, "Run a private copy of the MAL program", args(1,1, arg("",void))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_run_isolate_mal)
{ mal_module("run_isolate", NULL, run_isolate_init_funcs); }
