/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _MAL_LISTING_H
#define _MAL_LISTING_H

#include "mal_type.h"
#include "mal_stack.h"
#include "mal_instruction.h"

#define DEBUG_MAL_LIST

mal_export str fcnDefinition(MalBlkPtr mb, InstrPtr p, str s, int flg, str base, size_t len);
mal_export void printInstruction(stream *fd, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int flg);
mal_export void fprintInstruction(FILE *fd, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int flg);
mal_export str instructionCall(MalBlkPtr mb, InstrPtr p, str s, str base, size_t len);
mal_export void promptInstruction(stream *fd, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int flg);
mal_export str instruction2str(MalBlkPtr mb, MalStkPtr stl, InstrPtr p, int hidden);
mal_export str shortStmtRendering(MalBlkPtr mb, MalStkPtr stl, InstrPtr p);
mal_export str mal2str(MalBlkPtr mb, int first, int last);
mal_export void showMalBlkHistory(stream *out, MalBlkPtr mb);

#endif /*  _MAL_LIST_H */
