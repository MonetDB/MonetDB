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

#ifndef _MAL_RECYCLE_
#define _MAL_RECYCLE_

#include "mal.h"
#include "mal_exception.h"
#include "mal_instruction.h"
#include "mal_client.h"

/*
#define _DEBUG_RECYCLE_
#define _DEBUG_RECYCLE_REUSE
#define _DEBUG_CACHE_
#define _DEBUG_RESET_
*/

/*
 * @-
 * We need some hard limits to not run out of datastructure
 * spaces.
 */
#define RU 1024 /* recycle unit in bytes */
#define GIGA (lng)(1024*1024*1024)

#define HARDLIMIT_VAR 100000		/* maximum variables to watch */
#define HARDLIMIT_STMT 20000		/* roughly 5/line needed */
#define HARDLIMIT_MEM 8 * (GIGA/RU)     /* avoid memory overflow */

mal_export int admissionPolicy;
#define ADM_NONE	0
#define ADM_ALL 	1
#define ADM_CAT	2
#define ADM_INTEREST	3
#define ADM_ADAPT	4

#define NO_RECYCLING -1
#define REC_NO_INTEREST 0
#define REC_MAX_INTEREST 3
#define REC_MIN_INTEREST 1

mal_export lng recycleTime;
mal_export lng recycleSearchTime;
mal_export lng msFindTime;
mal_export lng msComputeTime;
/*mal_export lng recycleVolume; */

mal_export int reusePolicy;
#define REUSE_NONE	0
#define REUSE_COVER	1
#define REUSE_EXACT	2
#define REUSE_MULTI	3

mal_export int rcachePolicy;
#define RCACHE_ALL		0
#define RCACHE_LRU		1
#define RCACHE_BENEFIT 	2
#define RCACHE_PROFIT 	3

mal_export int recycleCacheLimit;
mal_export lng recycleMemory;	/* Units of memory permitted */
mal_export lng recyclerUsedMemory;
mal_export MalBlkPtr recycleBlk;
mal_export double recycleAlpha;
mal_export int recycleMaxInterest;
mal_export int monitorRecycler;

/*
 * @- Statistics about query patterns
 */
typedef struct QRYSTAT {
	lng recid;	/* unique id given by the recycle optimizer */
	int calls; 	/* number of calls */
	int greuse; /* number of global reuse */
	int lreuse; /* number of local reuse in current execution only */
	lng dtreuse;/* data transfer amount in RU that query reuses from others */
	int *crd;   /* instructions credits */
	int stop;
	int wl;		/* waterline of globally reused instructions*/
	bte *gl;	/* mask for globally reused instructions */
} QryStat, *QryStatPtr;

typedef struct QRYPATTERN {
	int cnt; /* number of query patterns */
	int sz;  /* storage capacity */
	QryStatPtr *ptrn; /* patterns */
} QryPat, *QryPatPtr;


typedef str (*aggrFun) (ptr, int *);


mal_export QryPatPtr recycleQPat;
mal_export aggrFun minAggr;
mal_export aggrFun maxAggr;

mal_export void RECYCLEinit(void);
mal_export int RECYCLEentry(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export void RECYCLEexit(Client cntxt,MalBlkPtr mb, MalStkPtr stk, InstrPtr p, lng ticks);
mal_export void RECYCLEreset(Client cntxt,MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export void RECYCLEshutdown(Client cntxt);
mal_export int RECYCLEinterest(InstrPtr p);
mal_export int RECYCLEnewQryStat(MalBlkPtr mb);
mal_export void RECYCLEinitQPat(int sz);
mal_export bte RECYCLEgetQryCat(int qidx);
mal_export bit isBindInstr(InstrPtr p);
#endif
