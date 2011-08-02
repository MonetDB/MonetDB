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

#ifndef _MAL_BUILDER_
#define _MAL_BUILDER_

#include "mal.h"
#include "mal_function.h"
#include "mal_namespace.h"

mal_export InstrPtr newStmt(MalBlkPtr mb, char *module, char *name);
mal_export InstrPtr newStmt1(MalBlkPtr mb, str module, char *name);
mal_export InstrPtr newStmt2(MalBlkPtr mb, str module, char *name);
mal_export InstrPtr newStmtId(MalBlkPtr mb, char *id, char *module, char *name);
mal_export InstrPtr newAssignment(MalBlkPtr mb);
mal_export InstrPtr newAssignmentId(MalBlkPtr mb, str nme);
mal_export InstrPtr newComment(MalBlkPtr mb, const char *val);
mal_export InstrPtr newCatchStmt(MalBlkPtr mb, str nme);
mal_export InstrPtr newRaiseStmt(MalBlkPtr mb, str nme);
mal_export InstrPtr newExitStmt(MalBlkPtr mb, str nme);
mal_export InstrPtr newFcnCall(MalBlkPtr mb, char *mod, char *fcn);
mal_export InstrPtr pushInt(MalBlkPtr mb, InstrPtr q, int val);
mal_export InstrPtr pushWrd(MalBlkPtr mb, InstrPtr q, wrd val);
mal_export InstrPtr pushBte(MalBlkPtr mb, InstrPtr q, bte val);
mal_export InstrPtr pushChr(MalBlkPtr mb, InstrPtr q, chr val);
mal_export InstrPtr pushOid(MalBlkPtr mb, InstrPtr q, oid val);
mal_export InstrPtr pushVoid(MalBlkPtr mb, InstrPtr q);
mal_export InstrPtr pushLng(MalBlkPtr mb, InstrPtr q, lng val);
mal_export InstrPtr pushDbl(MalBlkPtr mb, InstrPtr q, dbl val);
mal_export InstrPtr pushFlt(MalBlkPtr mb, InstrPtr q, flt val);
mal_export InstrPtr pushStr(MalBlkPtr mb, InstrPtr q, const char *val);
mal_export InstrPtr pushBit(MalBlkPtr mb, InstrPtr q, bit val);
mal_export InstrPtr pushNil(MalBlkPtr mb, InstrPtr q, int tpe);
mal_export InstrPtr pushType(MalBlkPtr mb, InstrPtr q, int tpe);
mal_export InstrPtr pushNilType(MalBlkPtr mb, InstrPtr q, char *tpe);
mal_export InstrPtr pushZero(MalBlkPtr mb, InstrPtr q, int tpe);
mal_export InstrPtr pushEmptyBAT(MalBlkPtr mb, InstrPtr q, int tpe);
mal_export InstrPtr pushValue(MalBlkPtr mb, InstrPtr q, ValPtr cst);

#endif /* _MAL_BUILDER_ */

