/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _OPT_SUPPORT_H
#define _OPT_SUPPORT_H

#include "mal.h"
#include "mal_function.h"
#include "mal_scenario.h"
#include "mal_builder.h"

/*
 * The optimizers should all be equiped with debugging code
 * to easily trace their performance.
 */
mal_export str MALoptimizer(Client c);
mal_export str optimizeMALBlock(Client cntxt, MalBlkPtr mb);
mal_export void showOptimizerStep(str fnme,int i, int flg);
mal_export void showOptimizerHistory(void);

mal_export int optimizerIsApplied(MalBlkPtr mb, str name);
mal_export int isUnsafeInstruction(InstrPtr q);
mal_export int isUnsafeFunction(InstrPtr q);
mal_export int isSealedFunction(InstrPtr q);
mal_export int safetyBarrier(InstrPtr p, InstrPtr q);
mal_export int hasSameSignature(MalBlkPtr mb, InstrPtr p, InstrPtr q, int stop);
mal_export int hasSameArguments(MalBlkPtr mb, InstrPtr p, InstrPtr q);
mal_export int hasCommonResults(InstrPtr p, InstrPtr q);
mal_export int isUpdateInstruction(InstrPtr p);
mal_export int hasSideEffects(MalBlkPtr mb, InstrPtr p, int strict);
mal_export int mayhaveSideEffects(Client cntxt, MalBlkPtr mb, InstrPtr p, int strict);
mal_export int isSideEffectFree(MalBlkPtr mb);
mal_export int isBlocking(InstrPtr p);
mal_export int isFragmentGroup(InstrPtr q);
mal_export int isFragmentGroup2(InstrPtr q);
mal_export int isDelta(InstrPtr q);
mal_export int isMatJoinOp(InstrPtr q);
mal_export int isMatLeftJoinOp(InstrPtr q);
mal_export int isMapOp(InstrPtr q);
mal_export int isLikeOp(InstrPtr q);
mal_export int isTopn(InstrPtr q);
mal_export int isSlice(InstrPtr q);
mal_export int isSample(InstrPtr q);
mal_export int isOrderby(InstrPtr q);
mal_export int isSelect(InstrPtr q);
mal_export int isSubJoin(InstrPtr q);
mal_export int isMultiplex(InstrPtr q);
mal_export int isOptimizerEnabled(MalBlkPtr mb, str opt);

#endif /* _OPT_SUPPORT_H */

