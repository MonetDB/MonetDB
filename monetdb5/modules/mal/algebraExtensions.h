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
 * @include prelude.mx
 * @+ Implementation section
 * In most cases we pass a BAT identifier, which should be unified
 * with a BAT descriptor. Upon failure we can simply abort the function.
 *
 */
#ifndef _ALGEBRA_EXTENSIONS_H
#define _ALGEBRA_EXTENSIONS_H
#include "mal_box.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "bat5.h"
#include "algebra.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define ae_export extern __declspec(dllimport)
#else
#define ae_export extern __declspec(dllexport)
#endif
#else
#define ae_export extern
#endif

ae_export str ALGprojectCst(Client cntxt,MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
ae_export str ALGprojectCstBody(bat *result, int *bid, ptr *p, int tt);
#endif /* _ALGEBRA_EXTENSIONS_H*/

