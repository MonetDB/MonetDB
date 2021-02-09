/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "gdk_time.h"

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

static bool QLOGtrace = false;
static bool QLOG_init = false;
static lng QLOGthreshold = 0;

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

static MT_Lock QLOGlock = MT_LOCK_INITIALIZER(QLOGlock);

static str initQlog(void);

str
QLOGcatalog(BAT **r)
{
	int i,cnt = 0;
	str msg;

	for ( i=0;i < 8; i++)
		r[i]=0;
	msg = initQlog();
	if( msg)
		return msg;
	MT_lock_set(&QLOGlock);
	r[0] = COLcopy(QLOG_cat_id, QLOG_cat_id->ttype, false, TRANSIENT);
	r[1] = COLcopy(QLOG_cat_user, QLOG_cat_user->ttype,false, TRANSIENT);
	r[2] = COLcopy(QLOG_cat_defined, QLOG_cat_defined->ttype,false, TRANSIENT);
	r[3] = COLcopy(QLOG_cat_query, QLOG_cat_query->ttype,false, TRANSIENT);
	r[4] = COLcopy(QLOG_cat_pipe, QLOG_cat_pipe->ttype,false, TRANSIENT);
	r[5] = COLcopy(QLOG_cat_plan, QLOG_cat_plan->ttype,false, TRANSIENT);
	r[6] = COLcopy(QLOG_cat_mal, QLOG_cat_mal->ttype,false, TRANSIENT);
	r[7] = COLcopy(QLOG_cat_optimize, QLOG_cat_optimize->ttype,false, TRANSIENT);
	MT_lock_unset(&QLOGlock);
	for ( i = 0; i< 8; i++)
		cnt += r[i] != 0;
	if( cnt != 8){
		for ( i = 0; i< 8; i++)
		if( r[i]){
			BBPunfix(r[i]->batCacheid);
			r[i]=0;
		}
	}
	if( r[0])
		return MAL_SUCCEED;
	throw(MAL,"catalog_queries", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

str
QLOGcalls(BAT **r)
{
	int i, cnt = 0;
	str msg;

	for ( i=0;i < 10; i++)
		r[i]=0;
	msg = initQlog();
	if( msg)
		return msg;
	MT_lock_set(&QLOGlock);
	r[0] = COLcopy(QLOG_calls_id, QLOG_calls_id->ttype, false, TRANSIENT);
	r[1] = COLcopy(QLOG_calls_start, QLOG_calls_start->ttype,false, TRANSIENT);
	r[2] = COLcopy(QLOG_calls_stop, QLOG_calls_stop->ttype,false, TRANSIENT);
	r[3] = COLcopy(QLOG_calls_arguments, QLOG_calls_arguments->ttype,false, TRANSIENT);
	r[4] = COLcopy(QLOG_calls_tuples, QLOG_calls_tuples->ttype,false, TRANSIENT);
	r[5] = COLcopy(QLOG_calls_exec, QLOG_calls_exec->ttype,false, TRANSIENT);
	r[6] = COLcopy(QLOG_calls_result, QLOG_calls_result->ttype,false, TRANSIENT);
	r[7] = COLcopy(QLOG_calls_cpuload, QLOG_calls_cpuload->ttype,false, TRANSIENT);
	r[8] = COLcopy(QLOG_calls_iowait, QLOG_calls_iowait->ttype,false, TRANSIENT);
	MT_lock_unset(&QLOGlock);
	for ( i = 0; i< 9; i++)
		cnt += r[i] != 0;
	if( cnt != 9){
		for ( i = 0; i< 9; i++)
		if( r[i]){
			BBPunfix(r[i]->batCacheid);
			r[i]=0;
		}
	}
	if( r[0])
		return MAL_SUCCEED;
	throw(MAL,"catalog_calls", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

#define MAXCOMMITLIST 32
static bat commitlist[MAXCOMMITLIST];
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

	if (BBPrename(b->batCacheid, buf) != 0 ||
		BATmode(b, false) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		return NULL;
	}
	commitlist[committop++]= b->batCacheid;
	assert(committop < MAXCOMMITLIST);
	return b;
}

#define cleanup(X)  if (X) { (X)->batTransient = true; BBPrename((X)->batCacheid,"_"); BBPunfix((X)->batCacheid); } (X) = NULL;

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

static str
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
			throw(MAL,"querylog.init", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	QLOG_init = true;
	TMsubcommit_list(commitlist, NULL, committop, getBBPlogno(), getBBPtransid());
	return MAL_SUCCEED;
}

static str
initQlog(void)
{
	str msg;

	if (QLOG_init)
		return MAL_SUCCEED;	   /* already initialized */
	MT_lock_set(&QLOGlock);
	msg = _initQlog();
	MT_lock_unset(&QLOGlock);
	return msg;
}

str
QLOGenable(void *ret)
{
	(void) ret;
	QLOGtrace = true;
	QLOGthreshold = 0;
	return MAL_SUCCEED;
}

str
QLOGenableThreshold(void *ret, int *threshold)
{
	(void) ret;
	QLOGtrace = true;
	QLOGthreshold = *threshold * LL_CONSTANT(1000);
	return MAL_SUCCEED;
}

str
QLOGdisable(void *ret)
{
	(void) ret;
	QLOGtrace = false;
	return MAL_SUCCEED;
}

int
QLOGisset(void)
{
	return QLOGtrace;
}

static str
QLOGissetFcn(int *ret)
{
	*ret = QLOGtrace;
	return MAL_SUCCEED;
}

str
QLOGempty(void *ret)
{
	str msg;

	(void) ret;
	msg = initQlog();
	if( msg)
		return msg;
	MT_lock_set(&QLOGlock);
	/* drop all querylog tables */

	BATclear(QLOG_cat_id,true);
	BATclear(QLOG_cat_user,true);
	BATclear(QLOG_cat_defined,true);
	BATclear(QLOG_cat_query,true);
	BATclear(QLOG_cat_pipe,true);
	BATclear(QLOG_cat_plan,true);
	BATclear(QLOG_cat_mal,true);
	BATclear(QLOG_cat_optimize,true);

	BATclear(QLOG_calls_id,true);
	BATclear(QLOG_calls_start,true);
	BATclear(QLOG_calls_stop,true);
	BATclear(QLOG_calls_arguments,true);
	BATclear(QLOG_calls_tuples,true);
	BATclear(QLOG_calls_exec,true);
	BATclear(QLOG_calls_result,true);
	BATclear(QLOG_calls_cpuload,true);
	BATclear(QLOG_calls_iowait,true);

	TMsubcommit_list(commitlist, NULL, committop, getBBPlogno(), getBBPtransid());
	MT_lock_unset(&QLOGlock);
	return MAL_SUCCEED;
}

static str
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
	str msg;

	(void) cntxt;
	msg = initQlog();
	if( msg)
		return msg;
	snprintf(buf,128,"%s.%s", getModuleId(sig), getFunctionId(sig));
	MT_lock_set(&QLOGlock);
	o = BUNfnd(QLOG_cat_id, &mb->tag);
	if ( o == BUN_NONE){
		*ret = mb->tag;
		if (BUNappend(QLOG_cat_id,&mb->tag,false) != GDK_SUCCEED ||
			BUNappend(QLOG_cat_query,*q,false) != GDK_SUCCEED ||
			BUNappend(QLOG_cat_pipe,*pipe,false) != GDK_SUCCEED ||
			BUNappend(QLOG_cat_plan,nme,false) != GDK_SUCCEED ||
			BUNappend(QLOG_cat_mal,&mb->stop,false) != GDK_SUCCEED ||
			BUNappend(QLOG_cat_optimize,&mb->optimize,false) != GDK_SUCCEED ||
			BUNappend(QLOG_cat_user,*usr,false) != GDK_SUCCEED ||
			BUNappend(QLOG_cat_defined,tick,false) != GDK_SUCCEED) {
			MT_lock_unset(&QLOGlock);
			throw(MAL, "querylog.append", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	TMsubcommit_list(commitlist, NULL, committop, getBBPlogno(), getBBPtransid());
	MT_lock_unset(&QLOGlock);
	return MAL_SUCCEED;
}

static str
QLOGdefineNaive(void *ret, str *qry, str *opt, int *nr)
{
	// Nothing else to be done.
	(void) ret;
	(void) qry;
	(void) opt;
	(void) nr;
	return MAL_SUCCEED;
}

static str
QLOGcontextNaive(void *ret, str *release, str *version, str *revision, str *uri)
{
	// Nothing else to be done.
	(void) ret;
	(void) release;
	(void) version;
	(void) revision;
	(void) uri;
	return MAL_SUCCEED;
}

static str
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
	str msg;

	(void) cntxt;

	msg = initQlog();
	if( msg)
		return msg;
	if ( *xtime + *rtime < QLOGthreshold)
		return MAL_SUCCEED;
	MT_lock_set(&QLOGlock);
	if (BUNappend(QLOG_calls_id,&mb->tag,false) != GDK_SUCCEED ||
		BUNappend(QLOG_calls_start,tick1,false) != GDK_SUCCEED ||
		BUNappend(QLOG_calls_stop,tick2,false) != GDK_SUCCEED ||
		BUNappend(QLOG_calls_arguments,*arg,false) != GDK_SUCCEED ||
		BUNappend(QLOG_calls_tuples,tuples,false) != GDK_SUCCEED ||
		BUNappend(QLOG_calls_exec,xtime,false) != GDK_SUCCEED ||
		BUNappend(QLOG_calls_result,rtime,false) != GDK_SUCCEED ||
		BUNappend(QLOG_calls_cpuload,cpu,false) != GDK_SUCCEED ||
		BUNappend(QLOG_calls_iowait,iowait,false) != GDK_SUCCEED) {
		MT_lock_unset(&QLOGlock);
		throw(MAL, "querylog.call", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	TMsubcommit_list(commitlist, NULL, committop, getBBPlogno(), getBBPtransid());
	MT_lock_unset(&QLOGlock);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func querylog_init_funcs[] = {
 command("querylog", "enable", QLOGenableThreshold, false, "Turn on the query logger", args(0,1, arg("threshold",int))),
 command("querylog", "enable", QLOGenable, false, "Turn on the query logger", noargs),
 command("querylog", "disable", QLOGdisable, false, "Turn off the query logger", noargs),
 command("querylog", "isset", QLOGissetFcn, false, "Return status of query logger", args(1,1, arg("",int))),
 command("querylog", "empty", QLOGempty, false, "Clear the query log tables", noargs),
 pattern("querylog", "append", QLOGappend, false, "Add a new query call to the query log", args(0,4, arg("q",str),arg("pipe",str),arg("usr",str),arg("tick",timestamp))),
 command("querylog", "define", QLOGdefineNaive, false, "Noop operation, just marking the query", args(0,3, arg("q",str),arg("pipe",str),arg("size",int))),
 command("querylog", "context", QLOGcontextNaive, false, "Noop operation, just marking the query", args(0,4, arg("release",str),arg("version",str),arg("revision",str),arg("uri",str))),
 pattern("querylog", "call", QLOGcall, false, "Add a new query call to the query log", args(0,8, arg("tick1",timestamp),arg("tick2",timestamp),arg("arg",str),arg("tuples",lng),arg("xtime",lng),arg("rtime",lng),arg("cpu",int),arg("iowait",int))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_querylog_mal)
{ mal_module("querylog", NULL, querylog_init_funcs); }
