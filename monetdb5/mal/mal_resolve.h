/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _MAL_RESOLVE_H
#define _MAL_RESOLVE_H

#include "mal_exception.h"
#include "mal_function.h"
#include "mal_listing.h"
#include "mal_exception.h"

#define MAXTYPEVAR  10

mal_export str chkProgram(Module s, MalBlkPtr mb);
mal_export int chkInstruction(Module s, MalBlkPtr mb, InstrPtr p);
mal_export str chkTypes(Module s, MalBlkPtr mb, int silent);
mal_export void typeChecker(Module scope, MalBlkPtr mb, InstrPtr p, int p_idx, int silent);

/*
 * @- Type resolution algorithm.
 * Every actual argument of a function call should be type compatible
 * with the formal argument, and the function result type should be
 * compatible with the destination variable.
 * In both cases the 'receiving' variable may not be fully qualified,
 * i.e. of type 'any'. The type resolution algorithm creates the concrete
 * type for subsequent use.
 */
mal_export int resolveType(int dsttype, int srctype);

#endif /*  _MAL_RESOLVE_H*/
