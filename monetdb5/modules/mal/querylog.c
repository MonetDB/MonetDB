/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 *  Martin Kersten
 * Language Extensions
 * Iterators over scalar ranges are often needed, also at the MAL level.
 * The barrier and control primitives are sufficient to mimic them directly.
 *
 * The modules located in the kernel directory should not
 * rely on the MAL datastructures. That's why we have to deal with
 * some bat operations here and delegate the signature to the
 * proper module upon loading.
 *
 * Running a script is typically used to initialize a context.
 * Therefore we need access to the runtime context.
 * For the call variants we have
 * to determine an easy way to exchange the parameter/return values.
 */

#include "monetdb_config.h"
#include "querylog.h"
#include "mtime.h"

/* (c) M.L. Kersten
 * The query logger facility is hardwired to avoid interference 
 * with the SQL transaction manager.
 *
 * The events being captured are stored in separate BATs.
 * They are made persistent to accumulate information over
 * multiple sessions. This means it has to be explicitly reset
 * to avoid disc overflow using querylog.reset().
create table querylog.catalog(
    id oid,
    "user" string,      -- owner of the query
    defined timestamp,  -- when entered into the cache
    query string,
    pipe string,   		-- optimizer pipe-line deployed
    optimize bigint     -- time in usec
);
create table querylog.calls(
    id oid,
    "start" timestamp,  -- time the statement was started
    "stop" timestamp,   -- time the statement was completely finished
    arguments string,
    tuples lng,         -- number of tuples in the result set
    exec bigint,        -- time spent (in usec)  until the result export
    result bigint,      -- time spent (in usec)  to ship the result set
    cpuload int,        -- average cpu load percentage during execution
    iowait int         -- time waiting for IO to finish in usec
);
*/

static int QLOGtrace = 0;
static int QLOG_init = 0;
static int QLOGthreshold = 0;

static BAT *QLOG_cat_id = 0;
static BAT *QLOG_cat_user = 0;
static BAT *QLOG_cat_defined = 0;
static BAT *QLOG_cat_query = 0;
static BAT *QLOG_cat_pipe = 0;
static BAT *QLOG_cat_plan = 0;
static BAT *QLOG_cat_mal = 0;
static BAT *QLOG_cat_optimize = 0;

static BAT *QLOG_calls_id = 0;
static BAT *QLOG_calls_start = 0;
static BAT *QLOG_calls_stop = 0;
static BAT *QLOG_calls_arguments = 0;
static BAT *QLOG_calls_tuples = 0;
static BAT *QLOG_calls_exec = 0;
static BAT *QLOG_calls_result = 0;
static BAT *QLOG_calls_cpuload = 0;
static BAT *QLOG_calls_iowait = 0;

void
QLOGcatalog(BAT **r)
{
	int i;
	for ( i=0;i < 8; i++)
		r[i]=0;
	if (initQlog())
		return ;
	MT_lock_set(&mal_profileLock);
	r[0] = COLcopy(QLOG_cat_id, QLOG_cat_id->ttype, 0, TRANSIENT);
	r[1] = COLcopy(QLOG_cat_user, QLOG_cat_user->ttype,0, TRANSIENT);
	r[2] = COLcopy(QLOG_cat_defined, QLOG_cat_defined->ttype,0, TRANSIENT);
	r[3] = COLcopy(QLOG_cat_query, QLOG_cat_query->ttype,0, TRANSIENT);
	r[4] = COLcopy(QLOG_cat_pipe, QLOG_cat_pipe->ttype,0, TRANSIENT);
	r[5] = COLcopy(QLOG_cat_plan, QLOG_cat_plan->ttype,0, TRANSIENT);
	r[6] = COLcopy(QLOG_cat_mal, QLOG_cat_mal->ttype,0, TRANSIENT);
	r[7] = COLcopy(QLOG_cat_optimize, QLOG_cat_optimize->ttype,0, TRANSIENT);
	MT_lock_unset(&mal_profileLock);
}

void
QLOGcalls(BAT **r)
{
	int i;
	for ( i=0;i < 10; i++)
		r[i]=0;
	if (initQlog())
		return ;
	MT_lock_set(&mal_profileLock);
	r[0] = COLcopy(QLOG_calls_id, QLOG_calls_id->ttype, 0, TRANSIENT);
	r[1] = COLcopy(QLOG_calls_start, QLOG_calls_start->ttype,0, TRANSIENT);
	r[2] = COLcopy(QLOG_calls_stop, QLOG_calls_stop->ttype,0, TRANSIENT);
	r[3] = COLcopy(QLOG_calls_arguments, QLOG_calls_arguments->ttype,0, TRANSIENT);
	r[4] = COLcopy(QLOG_calls_tuples, QLOG_calls_tuples->ttype,0, TRANSIENT);
	r[5] = COLcopy(QLOG_calls_exec, QLOG_calls_exec->ttype,0, TRANSIENT);
	r[6] = COLcopy(QLOG_calls_result, QLOG_calls_result->ttype,0, TRANSIENT);
	r[7] = COLcopy(QLOG_calls_cpuload, QLOG_calls_cpuload->ttype,0, TRANSIENT);
	r[8] = COLcopy(QLOG_calls_iowait, QLOG_calls_iowait->ttype,0, TRANSIENT);
	MT_lock_unset(&mal_profileLock);
}

static bat commitlist[32];
static int committop=1;

static BAT *
QLOGcreate(str hnme, str tnme, int tt)
{
	BAT *b;
	char buf[128];

	snprintf(buf, 128, "querylog_%s_%s", hnme, tnme);
	b = BATdescriptor(BBPindex(buf));
	if (b) 
		return b;

	b = COLnew(0, tt, 1 << 16, PERSISTENT);
	if (b == NULL)
		return NULL;

	BATmode(b, PERSISTENT);
	BBPrename(b->batCacheid, buf);
	commitlist[committop++]= b->batCacheid;
	assert(committop < 32);
	return b;
}

#define cleanup(X)  if (X) { (X)->batPersistence = TRANSIENT; BBPrename((X)->batCacheid,"_"); BBPunfix((X)->batCacheid); } (X) = NULL;

static void
_QLOGcleanup(void)
{
	cleanup(QLOG_cat_id);
	cleanup(QLOG_cat_user);
	cleanup(QLOG_cat_defined);
	cleanup(QLOG_cat_query);
	cleanup(QLOG_cat_pipe);
	cleanup(QLOG_cat_plan);
	cleanup(QLOG_cat_mal);
	cleanup(QLOG_cat_optimize);
	
	cleanup(QLOG_calls_id);
	cleanup(QLOG_calls_start);
	cleanup(QLOG_calls_stop);
	cleanup(QLOG_calls_arguments);
	cleanup(QLOG_calls_tuples);
	cleanup(QLOG_calls_exec);
	cleanup(QLOG_calls_result);
	cleanup(QLOG_calls_cpuload);
	cleanup(QLOG_calls_iowait);
}

static void
_initQlog(void)
{
	QLOG_cat_id = QLOGcreate("cat","id",TYPE_oid);
	QLOG_cat_user = QLOGcreate("cat","user",TYPE_str);
	QLOG_cat_defined = QLOGcreate("cat","defined",TYPE_timestamp);
	QLOG_cat_query = QLOGcreate("cat","query",TYPE_str);
	QLOG_cat_pipe = QLOGcreate("cat","pipe",TYPE_str);
	QLOG_cat_plan = QLOGcreate("cat","size",TYPE_str);
	QLOG_cat_mal = QLOGcreate("cat","mal",TYPE_int);
	QLOG_cat_optimize = QLOGcreate("cat","optimize",TYPE_lng);
	
	QLOG_calls_id = QLOGcreate("calls","id",TYPE_oid);
	QLOG_calls_start = QLOGcreate("calls","start",TYPE_timestamp);
	QLOG_calls_stop = QLOGcreate("calls","stop",TYPE_timestamp);
	QLOG_calls_arguments = QLOGcreate("calls","arguments",TYPE_str);
	QLOG_calls_tuples = QLOGcreate("calls","tuples",TYPE_lng);
	QLOG_calls_exec = QLOGcreate("calls","exec",TYPE_lng);
	QLOG_calls_result = QLOGcreate("calls","result",TYPE_lng);
	QLOG_calls_cpuload = QLOGcreate("calls","cpuload",TYPE_int);
	QLOG_calls_iowait = QLOGcreate("calls","iowait",TYPE_int);

	if( QLOG_cat_id == NULL || QLOG_cat_user == NULL || QLOG_cat_defined == NULL ||
		QLOG_cat_query == NULL || QLOG_cat_pipe == NULL || QLOG_cat_plan == NULL ||
		QLOG_cat_mal == NULL || QLOG_cat_optimize == NULL || QLOG_calls_id == NULL ||
		QLOG_calls_start == NULL || QLOG_calls_stop == NULL || QLOG_calls_arguments == NULL ||
		QLOG_calls_tuples == NULL || QLOG_calls_exec == NULL || QLOG_calls_result == NULL ||
		QLOG_calls_cpuload == NULL || QLOG_calls_iowait == NULL){
			_QLOGcleanup();
			return;
	}

	if (QLOG_cat_id == NULL )
		_QLOGcleanup();
	else
		QLOG_init = 1;
	TMsubcommit_list(commitlist, committop);
}

int
initQlog(void)
{
	if (QLOG_init)
		return 0;	   /* already initialized */
	MT_lock_set(&mal_profileLock);
	_initQlog();
	MT_lock_unset(&mal_profileLock);
	return QLOG_init ? 0 : -1;
}

str
QLOGenable(void *ret)
{
	(void) ret;
	QLOGtrace = TRUE;
	return MAL_SUCCEED;
}

str
QLOGenableThreshold(void *ret, int *threshold)
{
	(void) ret;
	QLOGthreshold = *threshold;
	return MAL_SUCCEED;
}

str
QLOGdisable(void *ret)
{
	(void) ret;
	QLOGtrace = FALSE;
	return MAL_SUCCEED;
}

int
QLOGisset(void)
{
	return QLOGtrace;
}

str
QLOGissetFcn(int *ret)
{
	*ret = QLOGtrace;
	return MAL_SUCCEED;
}

str
QLOGempty(void *ret)
{
	(void) ret;
	initQlog();
	MT_lock_set(&mal_profileLock);
	/* drop all querylog tables */

	BATclear(QLOG_cat_id,TRUE);
	BATclear(QLOG_cat_user,TRUE);
	BATclear(QLOG_cat_defined,TRUE);
	BATclear(QLOG_cat_query,TRUE);
	BATclear(QLOG_cat_pipe,TRUE);
	BATclear(QLOG_cat_plan,TRUE);
	BATclear(QLOG_cat_mal,TRUE);
	BATclear(QLOG_cat_optimize,TRUE);
	
	BATclear(QLOG_calls_id,TRUE);
	BATclear(QLOG_calls_start,TRUE);
	BATclear(QLOG_calls_stop,TRUE);
	BATclear(QLOG_calls_arguments,TRUE);
	BATclear(QLOG_calls_tuples,TRUE);
	BATclear(QLOG_calls_exec,TRUE);
	BATclear(QLOG_calls_result,TRUE);
	BATclear(QLOG_calls_cpuload,TRUE);
	BATclear(QLOG_calls_iowait,TRUE);

	TMsubcommit_list(commitlist, committop);
	MT_lock_unset(&mal_profileLock);
	return MAL_SUCCEED;
}

str
QLOGappend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	oid *ret = getArgReference_oid(stk,pci,0);
	str *q = getArgReference_str(stk,pci,1);
	str *pipe = getArgReference_str(stk,pci,2);
	str  *usr = getArgReference_str(stk,pci,3);
	timestamp *tick = getArgReference_TYPE(stk,pci,4,timestamp);
	oid o;
	InstrPtr sig = getInstrPtr(mb,0);
	char buf[128], *nme= buf;

	(void) cntxt;
	initQlog();
	snprintf(buf,128,"%s.%s", getModuleId(sig), getFunctionId(sig));
	MT_lock_set(&mal_profileLock);
	o = BUNfnd(QLOG_cat_id, &mb->tag);
	if ( o == BUN_NONE){
		*ret = mb->tag;
		BUNappend(QLOG_cat_id,&mb->tag,FALSE);
		BUNappend(QLOG_cat_query,*q,FALSE);
		BUNappend(QLOG_cat_pipe,*pipe,FALSE);
		BUNappend(QLOG_cat_plan,nme,FALSE);
		BUNappend(QLOG_cat_mal,&mb->stop,FALSE);
		BUNappend(QLOG_cat_optimize,&mb->optimize,FALSE);
		BUNappend(QLOG_cat_user,*usr,FALSE);
		BUNappend(QLOG_cat_defined,tick,FALSE);
	}
	MT_lock_unset(&mal_profileLock);
	TMsubcommit_list(commitlist, committop);
	return MAL_SUCCEED;
}

str
QLOGdefineNaive(void *ret, str *qry, str *opt, int *nr)
{
	// Nothing else to be done.
	(void) ret;
	(void) qry;
	(void) opt;
	(void) nr;
	return MAL_SUCCEED;
}

str
QLOGcall(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	timestamp *tick1  = getArgReference_TYPE(stk,pci,1,timestamp);
	timestamp *tick2  = getArgReference_TYPE(stk,pci,2,timestamp);
	str *arg	= getArgReference_str(stk,pci,3);
	lng *tuples = getArgReference_lng(stk,pci,4);
	lng *xtime  = getArgReference_lng(stk,pci,5);
	lng *rtime  = getArgReference_lng(stk,pci,6);
	int *cpu	= getArgReference_int(stk,pci,7);
	int *iowait = getArgReference_int(stk,pci,8);
	(void) cntxt;

	initQlog();
	if ( *xtime + *rtime < QLOGthreshold)
		return MAL_SUCCEED;
	MT_lock_set(&mal_profileLock);
	BUNappend(QLOG_calls_id,&mb->tag,FALSE);
	BUNappend(QLOG_calls_start,tick1,FALSE);
	BUNappend(QLOG_calls_stop,tick2,FALSE);
	BUNappend(QLOG_calls_arguments,*arg,FALSE);
	BUNappend(QLOG_calls_tuples,tuples,FALSE);
	BUNappend(QLOG_calls_exec,xtime,FALSE);
	BUNappend(QLOG_calls_result,rtime,FALSE);
	BUNappend(QLOG_calls_cpuload,cpu,FALSE);
	BUNappend(QLOG_calls_iowait,iowait,FALSE);
	MT_lock_unset(&mal_profileLock);
	TMsubcommit_list(commitlist, committop);
	return MAL_SUCCEED;
}
