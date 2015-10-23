/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
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
#include "run_isolate.h"

str
RUNisolation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) cntxt;
	(void) stk;
	addtoMalBlkHistory(mb,"isolation");
	removeInstruction(mb, p);
	return MAL_SUCCEED;
}
