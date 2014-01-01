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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _MAL_RESOLVE_H
#define _MAL_RESOLVE_H

#include "mal_exception.h"
#include "mal_function.h"
#include "mal_listing.h"
#include "mal_exception.h"

/*
#define DEBUG_MAL_RESOLVE 1
*/
#define MAXTYPEVAR  10

mal_export void chkProgram(stream *out, Module s, MalBlkPtr mb);
mal_export void chkInstruction(stream *out, Module s, MalBlkPtr mb, InstrPtr p);
mal_export void chkTypes(stream *out, Module s, MalBlkPtr mb, int silent);
mal_export void typeChecker(stream *out,  Module scope, MalBlkPtr mb, InstrPtr p, int silent);
mal_export int fcnBinder(stream *out, Module scope, MalBlkPtr mb, InstrPtr p);

extern str traceFcnName;
mal_export void expandMacro(MalBlkPtr mb, InstrPtr p, MalBlkPtr mc);

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
