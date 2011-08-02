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

#ifndef _OPT_SUPPORT_H
#define _OPT_SUPPORT_H

#include "mal.h"
#include "mal_function.h"
#include "mal_scenario.h"
#include "mal_builder.h"
#include "mal_properties.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define opt_export extern __declspec(dllimport)
#else
#define opt_export extern __declspec(dllexport)
#endif
#else
#define opt_export extern
#endif

/*
 * @-
 * The optimizers should all be equiped with debugging code
 * to easily trace their performance.
 */
#define DEBUG_OPT_ACCESSMODE		1
#define DEBUG_OPT_OPTIMIZER			2
#define DEBUG_OPT_ALIASES			3
#define DEBUG_OPT_REDUCE			4
#define DEBUG_OPT_COMMONTERMS		5
#define DEBUG_OPT_COMPRESS			6
#define DEBUG_OPT_CONSTANTS			7
#define DEBUG_OPT_COSTMODEL			8
#define DEBUG_OPT_CRACK				9
#define DEBUG_OPT_DATACELL			10
#define DEBUG_OPT_DATACYCLOTRON		11
#define DEBUG_OPT_DATAFLOW			12
#define DEBUG_OPT_DEADCODE     		13
#define DEBUG_OPT_DERIVEPATH		14
#define DEBUG_OPT_DICTIONARY		15
#define DEBUG_OPT_EMPTYSET			16
#define DEBUG_OPT_EVALUATE			17
#define DEBUG_OPT_FACTORIZE			18
#define DEBUG_OPT_GARBAGE			19
#define DEBUG_OPT_INLINE			20
#define DEBUG_OPT_JOINPATH			21
#define DEBUG_OPT_JOINSELECT		22
#define DEBUG_OPT_MACRO				23
#define DEBUG_OPT_MERGETABLE		24
#define DEBUG_OPT_OCTOPUS			25
#define DEBUG_OPT_ORIGIN			52
#define DEBUG_OPT_PARTITIONS		26
#define DEBUG_OPT_PEEPHOLE			27
#define DEBUG_OPT_PREJOIN      		28
#define DEBUG_OPT_PUSHRANGES		29
#define DEBUG_OPT_QEP				30
#define DEBUG_OPT_RECYCLE			31
#define DEBUG_OPT_REMAP       		32
#define DEBUG_OPT_REMOTE			33
#define DEBUG_OPT_REORDER			34
#define DEBUG_OPT_REORDER_DETAILS	35
#define DEBUG_OPT_REPLICATION      	36
#define DEBUG_OPT_SINGLETON			37
#define DEBUG_OPT_STRENGTHREDUCTION	38
#define DEBUG_OPT_COERCION			39
#define DEBUG_OPT_HISTORY			40
#define DEBUG_OPT_MITOSIS			41
#define DEBUG_OPT_MULTIPLEX			42
#define DEBUG_OPT_ACCUMULATORS		43
#define DEBUG_OPT_CLUSTER			44
#define DEBUG_OPT_MAPREDUCE			45
#define DEBUG_OPT_SELCRACK			46
#define DEBUG_OPT_SIDCRACK			47
#define DEBUG_OPT_TRACE				48
#define DEBUG_OPT_HEURISTIC			49

#define DEBUG_OPT(X) ((lng) 1 << (X))
opt_export lng optDebug;

opt_export str MALoptimizer(Client c);
opt_export str optimizerCheck(Client cntxt, MalBlkPtr mb, str name, int actions, lng usec, int flag);
opt_export str optimizeMALBlock(Client cntxt, MalBlkPtr mb);
opt_export void showOptimizerStep(str fnme,int i, int flg);
opt_export void showOptimizerHistory(void);

opt_export int isUnsafeInstruction(InstrPtr q);
opt_export int isUnsafeFunction(InstrPtr q);
opt_export int isInvariant(MalBlkPtr mb, int pcf, int pcl, int varid);
opt_export int isDependent(InstrPtr p, InstrPtr q);
opt_export int safetyBarrier(InstrPtr p, InstrPtr q);
opt_export int hasSameSignature(MalBlkPtr mb, InstrPtr p, InstrPtr q, int stop);
opt_export int hasSameArguments(MalBlkPtr mb, InstrPtr p, InstrPtr q);
opt_export int isUpdated(MalBlkPtr mb, int pc);
opt_export int hasCommonResults(InstrPtr p, InstrPtr q);
opt_export int isProcedure(MalBlkPtr mb, InstrPtr p);
opt_export int isUpdateInstruction(InstrPtr p);
opt_export int hasSideEffects(InstrPtr p, int strict);
opt_export int isSideEffectFree(MalBlkPtr mb);
opt_export int isBlocking(InstrPtr p);
opt_export int isAllScalar(MalBlkPtr mb, InstrPtr p);
opt_export int isFragmentGroup(InstrPtr q);
opt_export int isSelect(InstrPtr p);
opt_export int isProjection(InstrPtr p);
opt_export int isMatJoinOp(InstrPtr q);
opt_export int isMapOp(InstrPtr q);
opt_export int isTopn(InstrPtr q);
opt_export int isSlice(InstrPtr q);
opt_export int isOrderby(InstrPtr q);
opt_export int isDiffOp(InstrPtr q);
opt_export int allTargetsVisible(MalBlkPtr mb, Lifespan span, int pc,int qc);
opt_export int isOptimizerEnabled(MalBlkPtr mb, str opt);
opt_export str OPTsetDebugStr(int *ret, str *nme);
/*
 * @-
 * The optimizerCheck is defensive. In some cases, e.g. coercion,
 * a light check would be sufficient. Such a refinement is left
 * for the future using an additional flag.
 */
#define OPT_CHECK_FLOW	1
#define OPT_CHECK_TYPES 2
#define OPT_CHECK_DECL 4

#define OPT_CHECK_ALL (OPT_CHECK_FLOW | OPT_CHECK_TYPES | OPT_CHECK_DECL )

opt_export wrd getVarRows(MalBlkPtr mb, int v);

#endif /* _OPT_SUPPORT_H */

