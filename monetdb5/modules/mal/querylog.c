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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
    tuples wrd,         -- number of tuples in the result set
    exec bigint,        -- time spent (in usec)  until the result export
    result bigint,      -- time spent (in usec)  to ship the result set
    cpuload int,        -- average cpu load percentage during execution
    iowait int,         -- time waiting for IO to finish in usec
    space bigint        -- total storage size of intermediates created (in MB)
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
static BAT *QLOG_calls_space = 0;

void
QLOGcatalog(BAT **r)
{
	int i;
	for ( i=0;i < 7; i++)
		r[i]=0;
    if (initQlog())
        return ;
    MT_lock_set(&mal_profileLock, "querylogLock");
    r[0] = BATcopy(QLOG_cat_id, TYPE_oid, QLOG_cat_id->ttype, 0);
	r[1] = BATcopy(QLOG_cat_user, TYPE_oid, QLOG_cat_user->ttype,0);
	r[2] = BATcopy(QLOG_cat_defined, TYPE_oid, QLOG_cat_defined->ttype,0);
	r[3] = BATcopy(QLOG_cat_query, TYPE_oid, QLOG_cat_query->ttype,0);
	r[4] = BATcopy(QLOG_cat_pipe, TYPE_oid, QLOG_cat_pipe->ttype,0);
	r[5] = BATcopy(QLOG_cat_mal, TYPE_oid, QLOG_cat_mal->ttype,0);
	r[6] = BATcopy(QLOG_cat_optimize, TYPE_oid, QLOG_cat_optimize->ttype,0);
    MT_lock_unset(&mal_profileLock, "querylogLock");
}

void
QLOGcalls(BAT **r)
{
	int i;
	for ( i=0;i < 10; i++)
		r[i]=0;
    if (initQlog())
        return ;
    MT_lock_set(&mal_profileLock, "querylogLock");
    r[0] = BATcopy(QLOG_calls_id, TYPE_oid, QLOG_calls_id->ttype, 0);
	r[1] = BATcopy(QLOG_calls_start, TYPE_oid, QLOG_calls_start->ttype,0);
	r[2] = BATcopy(QLOG_calls_stop, TYPE_oid, QLOG_calls_stop->ttype,0);
	r[3] = BATcopy(QLOG_calls_arguments, TYPE_oid, QLOG_calls_arguments->ttype,0);
	r[4] = BATcopy(QLOG_calls_tuples, TYPE_oid, QLOG_calls_tuples->ttype,0);
	r[5] = BATcopy(QLOG_calls_exec, TYPE_oid, QLOG_calls_exec->ttype,0);
	r[6] = BATcopy(QLOG_calls_result, TYPE_oid, QLOG_calls_result->ttype,0);
	r[7] = BATcopy(QLOG_calls_cpuload, TYPE_oid, QLOG_calls_cpuload->ttype,0);
	r[8] = BATcopy(QLOG_calls_iowait, TYPE_oid, QLOG_calls_iowait->ttype,0);
	r[9] = BATcopy(QLOG_calls_space, TYPE_oid, QLOG_calls_space->ttype,0);
    MT_lock_unset(&mal_profileLock, "querylogLock");
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

    b = BATnew(TYPE_void, tt, 1 << 16);
    if (b == NULL)
        return NULL;

    BATmode(b, PERSISTENT);
    BATseqbase(b, 0);
    BATkey(b, TRUE);
    BBPrename(b->batCacheid, buf);
	commitlist[committop++]= ABS(b->batCacheid);
	assert(committop < 32);
    return b;
}

#define cleanup(X)  if (X) { (X)->batPersistence = TRANSIENT; BBPrename((X)->batCacheid,"_"); BBPreleaseref((X)->batCacheid); } (X) = NULL;

static void
_QLOGcleanup(void)
{
	cleanup(QLOG_cat_id);
	cleanup(QLOG_cat_user);
	cleanup(QLOG_cat_defined);
	cleanup(QLOG_cat_query);
	cleanup(QLOG_cat_pipe);
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
	cleanup(QLOG_calls_space);
}

static void
_initQlog(void)
{
	QLOG_cat_id = QLOGcreate("cat","id",TYPE_oid);
	QLOG_cat_user = QLOGcreate("cat","user",TYPE_str);
	QLOG_cat_defined = QLOGcreate("cat","defined",TYPE_lng);
	QLOG_cat_query = QLOGcreate("cat","query",TYPE_str);
	QLOG_cat_pipe = QLOGcreate("cat","pipe",TYPE_str);
	QLOG_cat_mal = QLOGcreate("cat","mal",TYPE_int);
	QLOG_cat_optimize = QLOGcreate("cat","optimize",TYPE_lng);
	
	QLOG_calls_id = QLOGcreate("calls","id",TYPE_oid);
	QLOG_calls_start = QLOGcreate("calls","start",TYPE_lng);
	QLOG_calls_stop = QLOGcreate("calls","stop",TYPE_lng);
	QLOG_calls_arguments = QLOGcreate("calls","arguments",TYPE_str);
	QLOG_calls_tuples = QLOGcreate("calls","tuples",TYPE_wrd);
	QLOG_calls_exec = QLOGcreate("calls","exec",TYPE_lng);
	QLOG_calls_result = QLOGcreate("calls","result",TYPE_lng);
	QLOG_calls_cpuload = QLOGcreate("calls","cpuload",TYPE_int);
	QLOG_calls_iowait = QLOGcreate("calls","iowait",TYPE_int);
	QLOG_calls_space = QLOGcreate("calls","space",TYPE_lng);
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
        return 0;       /* already initialized */
    MT_lock_set(&mal_profileLock, "querylogLock");
    _initQlog();
    MT_lock_unset(&mal_profileLock, "querylogLock");
    return QLOG_init ? 0 : -1;
}

str
QLOGenable(int *ret)
{
	(void) ret;
	QLOGtrace = TRUE;
	return MAL_SUCCEED;
}

str
QLOGenableThreshold(int *ret, int *threshold)
{
	(void) ret;
	QLOGthreshold = *threshold;
	return MAL_SUCCEED;
}

str
QLOGdisable(int *ret)
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
QLOGempty(int *ret)
{
	(void) ret;
	initQlog();
    MT_lock_set(&mal_profileLock, "querylog.reset");
    /* drop all querylog tables */

	BATclear(QLOG_cat_id,TRUE);
	BATclear(QLOG_cat_user,TRUE);
	BATclear(QLOG_cat_defined,TRUE);
	BATclear(QLOG_cat_query,TRUE);
	BATclear(QLOG_cat_pipe,TRUE);
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
	BATclear(QLOG_calls_space,TRUE);

	TMsubcommit_list(commitlist, committop);
    MT_lock_unset(&mal_profileLock, "querylog.reset");
	return MAL_SUCCEED;
}

str
QLOGdefine(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	oid *ret = (oid*) getArgReference(stk,pci,0);
	str *q = (str*) getArgReference(stk,pci,1);
	str *pipe = (str*) getArgReference(stk,pci,2);
	str  *usr = (str*) getArgReference(stk,pci,3);
	lng *tick = (lng*) getArgReference(stk,pci,4);
	oid o;

	(void) cntxt;
	initQlog();
    MT_lock_set(&mal_profileLock, "querylog.define");
	o = BUNfnd( BATmirror(QLOG_cat_id), &mb->tag);
	if ( o == BUN_NONE){
		*ret = mb->tag;
		QLOG_cat_id = BUNappend(QLOG_cat_id,&mb->tag,FALSE);
		QLOG_cat_query = BUNappend(QLOG_cat_query,*q,FALSE);
		QLOG_cat_pipe = BUNappend(QLOG_cat_pipe,*pipe,FALSE);
		QLOG_cat_mal = BUNappend(QLOG_cat_mal,&mb->stop,FALSE);
		QLOG_cat_optimize = BUNappend(QLOG_cat_optimize,&mb->optimize,FALSE);
		QLOG_cat_user = BUNappend(QLOG_cat_user,*usr,FALSE);
		QLOG_cat_defined = BUNappend(QLOG_cat_defined,tick,FALSE);
	}
    MT_lock_unset(&mal_profileLock, "querylog.define");
	TMsubcommit_list(commitlist, committop);
	return MAL_SUCCEED;
}

str
QLOGcall(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng *tick1  = (lng*) getArgReference(stk,pci,1);
	lng *tick2  = (lng*) getArgReference(stk,pci,2);
	str *arg    = (str*) getArgReference(stk,pci,3);
	wrd *tuples = (wrd*) getArgReference(stk,pci,4);
	lng *xtime  = (lng*) getArgReference(stk,pci,5);
	lng *rtime  = (lng*) getArgReference(stk,pci,6);
	int *cpu    = (int*) getArgReference(stk,pci,7);
	int *iowait = (int*) getArgReference(stk,pci,8);
	lng *space  = (lng*) getArgReference(stk,pci,9);
	(void) cntxt;

	initQlog();
	if ( *xtime + *rtime < QLOGthreshold)
		return MAL_SUCCEED;
    MT_lock_set(&mal_profileLock, "querylog.call");
	QLOG_calls_id = BUNappend(QLOG_calls_id,&mb->tag,FALSE);
	QLOG_calls_start = BUNappend(QLOG_calls_start,tick1,FALSE);
	QLOG_calls_stop = BUNappend(QLOG_calls_stop,tick2,FALSE);
	QLOG_calls_arguments = BUNappend(QLOG_calls_arguments,*arg,FALSE);
	QLOG_calls_tuples = BUNappend(QLOG_calls_tuples,tuples,FALSE);
	QLOG_calls_exec = BUNappend(QLOG_calls_exec,xtime,FALSE);
	QLOG_calls_result = BUNappend(QLOG_calls_result,rtime,FALSE);
	QLOG_calls_cpuload = BUNappend(QLOG_calls_cpuload,cpu,FALSE);
	QLOG_calls_iowait = BUNappend(QLOG_calls_iowait,iowait,FALSE);
	QLOG_calls_space = BUNappend(QLOG_calls_space,space,FALSE);
    MT_lock_unset(&mal_profileLock, "querylog.call");
	TMsubcommit_list(commitlist, committop);
	return MAL_SUCCEED;
}
