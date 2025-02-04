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

#ifndef _MAL_LISTING_H
#define _MAL_LISTING_H

#include "mal_type.h"
#include "mal_stack.h"
#include "mal_instruction.h"

mal_export str fcnDefinition(MalBlkPtr mb, InstrPtr p, str t, int flg, str base, size_t len);
mal_export str cfcnDefinition(Symbol s, str base, size_t len);
mal_export void printInstruction(stream *fd, MalBlkPtr mb, MalStkPtr stk,
								 InstrPtr p, int flg);
mal_export void traceInstruction(component_t comp, MalBlkPtr mb, MalStkPtr stk,
								 InstrPtr p, int flg);
mal_export str instruction2str(MalBlkPtr mb, MalStkPtr stl, InstrPtr p,
							   int hidden);
mal_export str mal2str(MalBlkPtr mb, int first, int last);

#endif /*  _MAL_LIST_H */
