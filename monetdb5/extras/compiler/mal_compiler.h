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

#ifndef _MAL_COMPILER_H_
#define _MAL_COMPILER_H_
#include "mal.h"
#include "mal_client.h"
#include "mal_linker.h"

#define DEBUG_MAL_COMPILER

#ifdef WIN32
#ifndef LIBMAL_COMPILER
#define mal_compiler_export extern __declspec(dllimport)
#else
#define mal_compiler_export extern __declspec(dllexport)
#endif
#else
#define mal_compiler_export extern
#endif

mal_compiler_export str MCdynamicCompiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_compiler_export str MCloadFunction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_compiler_export str MCmcc(int *ret, str *fname);

#endif /* _MAL_COMPILER_H_ */
