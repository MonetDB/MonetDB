/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
 * The first example create a private copy, leaving out the scheduler call.
 * @example
 * @code{
 * function f();
 *     i@{runonce, rows>4@}:=1;	# just properties
 *     mdb.list();
 *     io.print("start running\n");
 *     scheduler.isolation();
 *     io.print("done\n");
 *     mdb.list();
 * end f;
 * f(); #shows self-modification in action
 * }
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
