/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * M. Kersten
 * Default multiplex operator implementation
 */
#ifndef _MANIFOLD_LIB_
#define _MANIFOLD_LIB_
#include <string.h>

#include "mal.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

/* #define _DEBUG_MANIFOLD_*/

mal_export MALfcn MANIFOLDtypecheck(Client cntxt, MalBlkPtr mb, InstrPtr pci, int checkprops);
mal_export str MANIFOLDevaluate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str MANIFOLDremapMultiplex(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

#endif /* _MANIFOLD_LIB_ */
