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

#ifndef _OPT_SUPPORT_H
#define _OPT_SUPPORT_H

#include "mal.h"
#include "mal_function.h"
#include "mal_client.h"
#include "mal_builder.h"

/*
 * The optimizers should all be equipped with debugging code
 * to easily trace their performance.
 */
extern int isSimpleSQL(MalBlkPtr mb);
extern int optimizerIsApplied(MalBlkPtr mb, const char *opt);
extern int isUnsafeInstruction(InstrPtr q);
extern int isUnsafeFunction(InstrPtr q);
extern int safetyBarrier(InstrPtr p, InstrPtr q);
extern int hasSameSignature(MalBlkPtr mb, InstrPtr p, InstrPtr q);
extern int hasSameArguments(MalBlkPtr mb, InstrPtr p, InstrPtr q);
extern int hasCommonResults(InstrPtr p, InstrPtr q);
extern int isUpdateInstruction(InstrPtr p);
mal_export int hasSideEffects(MalBlkPtr mb, InstrPtr p, int strict);
extern int mayhaveSideEffects(Client cntxt, MalBlkPtr mb, InstrPtr p,
							  int strict);
extern int isSideEffectFree(MalBlkPtr mb);
extern int isBlocking(InstrPtr p);
extern int isFragmentGroup(InstrPtr q);
extern int isFragmentGroup2(InstrPtr q);
extern int isDelta(InstrPtr q);
extern int isMatJoinOp(InstrPtr q);
extern int isMatLeftJoinOp(InstrPtr q);
extern int isMapOp(InstrPtr q);
extern int isMap2Op(InstrPtr q);
extern int isLikeOp(InstrPtr q);
extern int isTopn(InstrPtr q);
extern int isSlice(InstrPtr q);
extern int isSample(InstrPtr q);
extern int isOrderby(InstrPtr q);
extern int isSelect(InstrPtr q);
extern int isSubJoin(InstrPtr q);
extern int isMultiplex(InstrPtr q);
extern int isUnion(InstrPtr q);
extern int isOptimizerEnabled(MalBlkPtr mb, const char *opt);
extern bool isOptimizerUsed(MalBlkPtr mb, InstrPtr p, const char *opt);

#endif /* _OPT_SUPPORT_H */
