/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * @a Lefteris Sidirourgos
 * @d 30/08/2011
 * @+ The sampling facilities
 */

#ifndef _SAMPLE_H_
#define _SAMPLE_H_

/* #define _DEBUG_SAMPLE_ */


#include "mal_interpreter.h"
 
mal_export str
SAMPLEuniform(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif
