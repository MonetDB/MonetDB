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

#ifndef _MAL_LISTING_H
#define _MAL_LISTING_H

#include "mal_type.h"
#include "mal_stack.h"
#include "mal_instruction.h"
#include "mal_properties.h"

#define DEBUG_MAL_LIST

mal_export str fcnDefinition(MalBlkPtr mb, InstrPtr p, str s, int flg, str base, size_t len);
mal_export void printInstruction(stream *fd, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int flg);
mal_export str instructionCall(MalBlkPtr mb, InstrPtr p, str s, str base, size_t len);
mal_export void promptInstruction(stream *fd, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int flg);
mal_export str instruction2str(MalBlkPtr mb, MalStkPtr stl, InstrPtr p, int hidden);
mal_export str mal2str(MalBlkPtr mb, int flg, int first, int last);
mal_export str function2str(MalBlkPtr mb, int flg);
mal_export void showMalBlkHistory(stream *out, MalBlkPtr mb);

#endif /*  _MAL_LIST_H */
