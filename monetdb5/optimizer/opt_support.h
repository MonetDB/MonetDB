/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
	__attribute__((__visibility__("hidden")));
extern int optimizerIsApplied(MalBlkPtr mb, const char *opt)
	__attribute__((__visibility__("hidden")));
extern int isUnsafeInstruction(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isUnsafeFunction(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int safetyBarrier(InstrPtr p, InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int hasSameSignature(MalBlkPtr mb, InstrPtr p, InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int hasSameArguments(MalBlkPtr mb, InstrPtr p, InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int hasCommonResults(InstrPtr p, InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isUpdateInstruction(InstrPtr p)
	__attribute__((__visibility__("hidden")));
mal_export int hasSideEffects(MalBlkPtr mb, InstrPtr p, int strict);
extern int mayhaveSideEffects(Client cntxt, MalBlkPtr mb, InstrPtr p,
							  int strict)
	__attribute__((__visibility__("hidden")));
extern int isSideEffectFree(MalBlkPtr mb)
	__attribute__((__visibility__("hidden")));
extern int isBlocking(InstrPtr p)
	__attribute__((__visibility__("hidden")));
extern int isFragmentGroup(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isFragmentGroup2(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isDelta(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isMatJoinOp(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isMatLeftJoinOp(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isMapOp(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isMap2Op(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isLikeOp(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isTopn(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isSlice(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isSample(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isOrderby(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isSelect(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isSubJoin(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isMultiplex(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isUnion(InstrPtr q)
	__attribute__((__visibility__("hidden")));
extern int isOptimizerEnabled(MalBlkPtr mb, const char *opt)
	__attribute__((__visibility__("hidden")));

#define MB_LARGE(mb) (mb->vtop > 204800)
#endif /* _OPT_SUPPORT_H */
