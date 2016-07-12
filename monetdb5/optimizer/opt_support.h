/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _OPT_SUPPORT_H
#define _OPT_SUPPORT_H

#include "mal.h"
#include "mal_function.h"
#include "mal_scenario.h"
#include "mal_builder.h"

/*
 * @-
 * The optimizers should all be equiped with debugging code
 * to easily trace their performance.
 */
#define DEBUG_OPT_OPTIMIZER			2
#define DEBUG_OPT_ALIASES			3
#define DEBUG_OPT_REDUCE			4
#define DEBUG_OPT_COMMONTERMS		5
#define DEBUG_OPT_CANDIDATES		6
#define DEBUG_OPT_CONSTANTS			7
#define DEBUG_OPT_COSTMODEL			8
#define DEBUG_OPT_CRACK				9
#define DEBUG_OPT_DATACYCLOTRON		11
#define DEBUG_OPT_DATAFLOW			12
#define DEBUG_OPT_DEADCODE     		13
#define DEBUG_OPT_DICTIONARY		15
#define DEBUG_OPT_EVALUATE			17
#define DEBUG_OPT_FACTORIZE			18
#define DEBUG_OPT_GARBAGE			19
#define DEBUG_OPT_GENERATOR			56
#define DEBUG_OPT_INLINE			20
#define DEBUG_OPT_PROJECTIONPATH	21
#define DEBUG_OPT_MACRO				23
#define DEBUG_OPT_MATPACK			53
#define DEBUG_OPT_MERGETABLE		24
#define DEBUG_OPT_ORIGIN			52
#define DEBUG_OPT_PARTITIONS		26
#define DEBUG_OPT_PEEPHOLE			27
#define DEBUG_OPT_PREJOIN      		28
#define DEBUG_OPT_QEP				30
#define DEBUG_OPT_RECYCLE			31
#define DEBUG_OPT_REMAP       		32
#define DEBUG_OPT_REMOTE			33
#define DEBUG_OPT_REORDER			34
#define DEBUG_OPT_REORDER_DETAILS	35
#define DEBUG_OPT_REPLICATION      	36
#define DEBUG_OPT_STRENGTHREDUCTION	38
#define DEBUG_OPT_COERCION			39
#define DEBUG_OPT_HISTORY			40
#define DEBUG_OPT_MITOSIS			41
#define DEBUG_OPT_MULTIPLEX			42
#define DEBUG_OPT_SELCRACK			46
#define DEBUG_OPT_SIDCRACK			47
#define DEBUG_OPT_TRACE				48
#define DEBUG_OPT_HEURISTIC			49
#define DEBUG_OPT_PUSHSELECT		51
#define DEBUG_OPT_JSON				54
#define DEBUG_OPT_GEOSPATIAL			55
#define DEBUG_OPT_VOLCANO			10

#define DEBUG_OPT(X) ((lng) 1 << (X))
mal_export lng optDebug;

mal_export str MALoptimizer(Client c);
mal_export str optimizerCheck(Client cntxt, MalBlkPtr mb, str name, int actions, lng usec);
mal_export str optimizeMALBlock(Client cntxt, MalBlkPtr mb);
mal_export void showOptimizerStep(str fnme,int i, int flg);
mal_export void showOptimizerHistory(void);

mal_export int isUnsafeInstruction(InstrPtr q);
mal_export int isUnsafeFunction(InstrPtr q);
mal_export int isInvariant(MalBlkPtr mb, int pcf, int pcl, int varid);
mal_export int isDependent(InstrPtr p, InstrPtr q);
mal_export int safetyBarrier(InstrPtr p, InstrPtr q);
mal_export int hasSameSignature(MalBlkPtr mb, InstrPtr p, InstrPtr q, int stop);
mal_export int hasSameArguments(MalBlkPtr mb, InstrPtr p, InstrPtr q);
mal_export int hasCommonResults(InstrPtr p, InstrPtr q);
mal_export int isProcedure(MalBlkPtr mb, InstrPtr p);
mal_export int isUpdateInstruction(InstrPtr p);
mal_export int hasSideEffects(InstrPtr p, int strict);
mal_export int mayhaveSideEffects(Client cntxt, MalBlkPtr mb, InstrPtr p, int strict);
mal_export int isSideEffectFree(MalBlkPtr mb);
mal_export int isBlocking(InstrPtr p);
mal_export int isAllScalar(MalBlkPtr mb, InstrPtr p);
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
mal_export int isSubSelect(InstrPtr q);
mal_export int isSubJoin(InstrPtr q);
mal_export int isMultiplex(InstrPtr q);
mal_export int isOptimizerEnabled(MalBlkPtr mb, str opt);
mal_export str OPTsetDebugStr(void *ret, str *nme);

#endif /* _OPT_SUPPORT_H */

