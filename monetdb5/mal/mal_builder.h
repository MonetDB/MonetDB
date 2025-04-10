/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _MAL_BUILDER_
#define _MAL_BUILDER_

#include "mal.h"
#include "mal_instruction.h"

mal_export InstrPtr newStmt(MalBlkPtr mb, const char *module, const char *name);
mal_export InstrPtr newStmtArgs(MalBlkPtr mb, const char *module,
								const char *name, int args);
mal_export InstrPtr newAssignment(MalBlkPtr mb);
mal_export InstrPtr newAssignmentArgs(MalBlkPtr mb, int args);
mal_export InstrPtr newComment(MalBlkPtr mb, const char *val);
mal_export InstrPtr newCatchStmt(MalBlkPtr mb, const char *nme);
mal_export InstrPtr newRaiseStmt(MalBlkPtr mb, const char *nme);
mal_export InstrPtr newExitStmt(MalBlkPtr mb, const char *nme);
mal_export InstrPtr newReturnStmt(MalBlkPtr mb);
mal_export InstrPtr newFcnCall(MalBlkPtr mb, const char *mod, const char *fcn);
mal_export InstrPtr newFcnCallArgs(MalBlkPtr mb, const char *mod,
								   const char *fcn, int args);
mal_export InstrPtr pushEndInstruction(MalBlkPtr mb);

/* all the below push* functions (and also pushArgument) return NULL
 * _only_ if q is NULL, else they return q or a valid new instruction
 * (and the old q was then freed); in case of error, mb->errors is set
 * to a non-NULL value */
mal_export InstrPtr pushSht(MalBlkPtr mb, InstrPtr q, sht val);
mal_export InstrPtr pushInt(MalBlkPtr mb, InstrPtr q, int val);
mal_export InstrPtr pushLng(MalBlkPtr mb, InstrPtr q, lng val);
#ifdef HAVE_HGE
mal_export InstrPtr pushHge(MalBlkPtr mb, InstrPtr q, hge val);
#endif
mal_export InstrPtr pushBte(MalBlkPtr mb, InstrPtr q, bte val);
mal_export InstrPtr pushOid(MalBlkPtr mb, InstrPtr q, oid val);
mal_export InstrPtr pushVoid(MalBlkPtr mb, InstrPtr q);
mal_export InstrPtr pushDbl(MalBlkPtr mb, InstrPtr q, dbl val);
mal_export InstrPtr pushFlt(MalBlkPtr mb, InstrPtr q, flt val);
mal_export InstrPtr pushStr(MalBlkPtr mb, InstrPtr q, const char *val);
mal_export InstrPtr pushBit(MalBlkPtr mb, InstrPtr q, bit val);
mal_export InstrPtr pushNil(MalBlkPtr mb, InstrPtr q, int tpe);
mal_export InstrPtr pushNilBat(MalBlkPtr mb, InstrPtr q);
mal_export InstrPtr pushType(MalBlkPtr mb, InstrPtr q, int tpe);
mal_export InstrPtr pushNilType(MalBlkPtr mb, InstrPtr q, char *tpe);
mal_export InstrPtr pushZero(MalBlkPtr mb, InstrPtr q, int tpe);
mal_export InstrPtr pushValue(MalBlkPtr mb, InstrPtr q, const ValRecord *cst);

mal_export int getIntConstant(MalBlkPtr mb, int val);
mal_export int getLngConstant(MalBlkPtr mb, lng val);
mal_export int getShtConstant(MalBlkPtr mb, sht val);
mal_export int getBteConstant(MalBlkPtr mb, bte val);
mal_export int getOidConstant(MalBlkPtr mb, oid val);
mal_export int getDblConstant(MalBlkPtr mb, dbl val);
mal_export int getFltConstant(MalBlkPtr mb, flt val);
mal_export int getStrConstant(MalBlkPtr mb, str val);
mal_export int getBitConstant(MalBlkPtr mb, bit val);
#ifdef HAVE_HGE
mal_export int getHgeConstant(MalBlkPtr mb, hge val);
#endif
#endif /* _MAL_BUILDER_ */
