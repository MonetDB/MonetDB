/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
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
extern int isSimpleSQL(MalBlkPtr mb)
	ATTR_VISIBILITY_HIDDEN;
extern int optimizerIsApplied(MalBlkPtr mb, const char *opt)
	ATTR_VISIBILITY_HIDDEN;
extern int isUnsafeInstruction(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isUnsafeFunction(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int safetyBarrier(InstrPtr p, InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int hasSameSignature(MalBlkPtr mb, InstrPtr p, InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int hasSameArguments(MalBlkPtr mb, InstrPtr p, InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int hasCommonResults(InstrPtr p, InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isUpdateInstruction(InstrPtr p)
	ATTR_VISIBILITY_HIDDEN;
mal_export int hasSideEffects(MalBlkPtr mb, InstrPtr p, int strict);
extern int mayhaveSideEffects(Client cntxt, MalBlkPtr mb, InstrPtr p,
							  int strict)
	ATTR_VISIBILITY_HIDDEN;
extern int isSideEffectFree(MalBlkPtr mb)
	ATTR_VISIBILITY_HIDDEN;
extern int isFragmentGroup(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isFragmentGroup2(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isDelta(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isMatJoinOp(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isMatLeftJoinOp(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isMapOp(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isMap2Op(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isLikeOp(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isTopn(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isSlice(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isSample(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isOrderby(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isSelect(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isSubJoin(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isMultiplex(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isUnion(InstrPtr q)
	ATTR_VISIBILITY_HIDDEN;
extern int isOptimizerEnabled(MalBlkPtr mb, const char *opt)
	ATTR_VISIBILITY_HIDDEN;

#define MB_LARGE(mb) (mb->vtop > 2048000)
#endif /* _OPT_SUPPORT_H */
