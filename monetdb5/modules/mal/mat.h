/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _MAT_H
#define _MAT_H

#include <stdarg.h>
#include "mal_resolve.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

mal_export str MATpack(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str MATpackIncrement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str MATpackValues(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

#endif /* _MAT_H */
