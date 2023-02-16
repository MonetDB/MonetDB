/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_interpreter.h"
#include "mal_authorize.h"
#include "mal_client.h"
#include "mal_runtime.h"
#include "gdk_time.h"
#include "mal_exception.h"
#include "mal_internal.h"

/* (c) M.L. Kersten
 * The queries currently in execution are returned to the front-end for managing expensive ones.
 */

static str
SYSMONstatistics(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;

	/* Temporary hack not allowing MAL clients (mclient -lmal)
	   to use this function */
	if (cntxt->sqlcontext == NULL)
		throw(MAL, "SYSMONstatistics", SQLSTATE(42000) "Calling from a mclient -lmal.");

	BAT *user, *querycount, *totalticks, *started, *finished, *maxquery, *maxticks;
	bat *u = getArgReference_bat(stk,pci,0);
	bat *c = getArgReference_bat(stk,pci,1);
	bat *t = getArgReference_bat(stk,pci,2);
	bat *s = getArgReference_bat(stk,pci,3);
	bat *f = getArgReference_bat(stk,pci,4);
	bat *m = getArgReference_bat(stk,pci,5);
	bat *q = getArgReference_bat(stk,pci,6);
	size_t i;
	timestamp tsn = timestamp_nil;
	str msg = MAL_SUCCEED;

	user = COLnew(0, TYPE_str, usrstatscnt, TRANSIENT);
	querycount = COLnew(0, TYPE_lng, usrstatscnt, TRANSIENT);
	totalticks = COLnew(0, TYPE_lng, usrstatscnt, TRANSIENT);
	started = COLnew(0, TYPE_timestamp, usrstatscnt, TRANSIENT);
	finished = COLnew(0, TYPE_timestamp, usrstatscnt, TRANSIENT);
	maxticks = COLnew(0, TYPE_lng, usrstatscnt, TRANSIENT);
	maxquery = COLnew(0, TYPE_str, usrstatscnt, TRANSIENT);
	if (user == NULL || querycount == NULL || totalticks == NULL || started == NULL || finished == NULL || maxquery == NULL || maxticks == NULL){
		BBPreclaim(user);
		BBPreclaim(started);
		BBPreclaim(querycount);
		BBPreclaim(totalticks);
		BBPreclaim(finished);
		BBPreclaim(maxticks);
		BBPreclaim(maxquery);
		throw(MAL, "SYSMONstatistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	MT_lock_set(&mal_delayLock);
	// FIXME: what if there are multiple users with ADMIN privilege?
	for (i = 0; i < usrstatscnt; i++) {
		/* We can stop at the first empty entry */
		if (USRstats[i].username == NULL) break;

		if (BUNappend(user, USRstats[i].username, false) != GDK_SUCCEED) {
			msg = createException(MAL, "SYSMONstatistics", "Failed to append 'user'");
			goto bailout;
		}
		if (BUNappend(querycount, &USRstats[i].querycount, false) != GDK_SUCCEED){
			msg = createException(MAL, "SYSMONstatistics", "Failed to append 'querycount'");
			goto bailout;
		}
		if (BUNappend(totalticks, &USRstats[i].totalticks, false) != GDK_SUCCEED){
			msg = createException(MAL, "SYSMONstatistics", "Failed to append 'totalticks'");
			goto bailout;
		}
		/* convert number of seconds into a timestamp */
		if (USRstats[i].maxquery != 0){
			tsn = timestamp_fromtime(USRstats[i].started);
			if (is_timestamp_nil(tsn)) {
				msg = createException(MAL, "SYSMONstatistics", SQLSTATE(22003) "failed to convert start time");
				goto bailout;
			}
			if (BUNappend(started, &tsn, false) != GDK_SUCCEED){
				msg = createException(MAL, "SYSMONstatistics", "Failed to append 'started'");
				goto bailout;
			}

			if (USRstats[i].finished == 0) {
				tsn = timestamp_nil;
			} else {
				tsn = timestamp_fromtime(USRstats[i].finished);
				if (is_timestamp_nil(tsn)) {
					msg = createException(MAL, "SYSMONstatistics", SQLSTATE(22003) "failed to convert finish time");
					goto bailout;
				}
			}
			if (BUNappend(finished, &tsn, false) != GDK_SUCCEED){
				msg = createException(MAL, "SYSMONstatistics", "Failed to append 'finished'");
				goto bailout;
			}
		} else {
			tsn = timestamp_nil;
			if (BUNappend(started, &tsn, false) != GDK_SUCCEED){
				msg = createException(MAL, "SYSMONstatistics", "Failed to append 'started'");
				goto bailout;
			}
			if (BUNappend(finished, &tsn, false) != GDK_SUCCEED){
				msg = createException(MAL, "SYSMONstatistics", "Failed to append 'finished'");
				goto bailout;
			}
		}

		if (BUNappend(maxticks, &USRstats[i].maxticks, false) != GDK_SUCCEED){
			msg = createException(MAL, "SYSMONstatistics", "Failed to append 'maxticks'");
			goto bailout;
		}
		if( USRstats[i].maxquery == 0){
			if (BUNappend(maxquery, "none", false) != GDK_SUCCEED ){
				msg = createException(MAL, "SYSMONstatistics", "Failed to append 'maxquery' 1");
				goto bailout;
			}
		}else {
			if (BUNappend(maxquery, USRstats[i].maxquery, false) != GDK_SUCCEED ){
				msg = createException(MAL, "SYSMONstatistics", "Failed to append 'maxquery' 2");
				goto bailout;
			}
		}
	}
	MT_lock_unset(&mal_delayLock);
	*u = user->batCacheid;
	BBPkeepref(user);
	*c = querycount->batCacheid;
	BBPkeepref(querycount);
	*t = totalticks->batCacheid;
	BBPkeepref(totalticks);
	*s = started->batCacheid;
	BBPkeepref(started);
	*f = finished->batCacheid;
	BBPkeepref(finished);
	*m = maxticks->batCacheid;
	BBPkeepref(maxticks);
	*q = maxquery->batCacheid;
	BBPkeepref(maxquery);
	return MAL_SUCCEED;

 bailout:
	MT_lock_unset(&mal_delayLock);
	BBPunfix(user->batCacheid);
	BBPunfix(querycount->batCacheid);
	BBPunfix(totalticks->batCacheid);
	BBPunfix(started->batCacheid);
	BBPunfix(finished->batCacheid);
	BBPunfix(maxticks->batCacheid);
	BBPunfix(maxquery->batCacheid);
	return msg;
}

static str
SYSMONqueue(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* Temporary hack not allowing MAL clients (mclient -lmal)
	   to use this function */
	if (cntxt->sqlcontext == NULL)
		throw(MAL, "SYSMONqueue", SQLSTATE(42000) "Calling from a mclient -lmal.");

	bat *t = getArgReference_bat(stk,pci,0),
		*s = getArgReference_bat(stk,pci,1),
		*u = getArgReference_bat(stk,pci,2),
		*sd = getArgReference_bat(stk,pci,3),
		*ss = getArgReference_bat(stk,pci,4),
		*q = getArgReference_bat(stk,pci,5),
		*f = getArgReference_bat(stk,pci,6),
		*w = getArgReference_bat(stk,pci,7),
		*m = getArgReference_bat(stk,pci,8);

	BUN sz = (BUN)qsize;
	BAT *tag = COLnew(0, TYPE_lng, sz, TRANSIENT),
		*sessionid = COLnew(0, TYPE_int, sz, TRANSIENT),
		*user = COLnew(0, TYPE_str, sz, TRANSIENT),
		*started = COLnew(0, TYPE_timestamp, sz, TRANSIENT),
		*status = COLnew(0, TYPE_str, sz, TRANSIENT),
		*query = COLnew(0, TYPE_str, sz, TRANSIENT),
		*finished = COLnew(0, TYPE_timestamp, sz, TRANSIENT),
		*workers = COLnew(0, TYPE_int, sz, TRANSIENT),
		*memory = COLnew(0, TYPE_int, sz, TRANSIENT);

	lng qtag;
	int wrk, mem;
	timestamp tsn;
	str userqueue = NULL, msg = MAL_SUCCEED;

	/* If pci->argc == 10, arg 9 type is a string */
	bool getall = false, admin = pci->argc == 10 ? true : false;
	if (admin) {
		assert(getArgType(mb, pci, 9) == TYPE_str);
		userqueue = *getArgReference_str(stk, pci, 9);
		if (strcmp("ALL", userqueue) == 0)
			getall = true;
	}

	if (tag == NULL || sessionid == NULL || user == NULL ||
		query == NULL || started == NULL || finished == NULL ||
		workers == NULL || memory == NULL){
		BBPreclaim(tag);
		BBPreclaim(sessionid);
		BBPreclaim(user);
		BBPreclaim(started);
		BBPreclaim(status);
		BBPreclaim(query);
		BBPreclaim(finished);
		BBPreclaim(workers);
		BBPreclaim(memory);
		throw(MAL, "SYSMONqueue", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	MT_lock_set(&mal_delayLock);
	for (size_t i = 0; i < qsize; i++) {
		/* Filtering the queries according to how SYSMONqueue was called.
		   Either:
		   SYSADMIN calls sys.queue("ALL") or SYSADMIN calls sys.queue(USER)
		   or any user calls sys.queue() to retrieve its own queue. */
		if (QRYqueue[i].query &&
			((admin && getall) ||
			 (admin && strcmp(QRYqueue[i].username, userqueue) == 0) ||
			 ((admin == false) && strcmp(QRYqueue[i].username, cntxt->username) == 0))) {
			qtag = (lng) QRYqueue[i].tag;
			if (BUNappend(tag, &qtag, false) != GDK_SUCCEED ||
				BUNappend(user, QRYqueue[i].username, false) != GDK_SUCCEED ||
				BUNappend(sessionid, &(QRYqueue[i].idx),false) != GDK_SUCCEED ||
				BUNappend(query, QRYqueue[i].query, false) != GDK_SUCCEED ||
				BUNappend(status, QRYqueue[i].status, false) != GDK_SUCCEED)
				goto bailout;
			/* convert number of seconds into a timestamp */
			tsn = timestamp_fromtime(QRYqueue[i].start);
			if (is_timestamp_nil(tsn)) {
				msg = createException(MAL, "SYSMONqueue", SQLSTATE(22003) "Cannot convert time.");
				goto bailout;
			}
			if (BUNappend(started, &tsn, false) != GDK_SUCCEED)
				goto bailout;
			if (QRYqueue[i].finished == 0)
				tsn = timestamp_nil;
			else {
				tsn = timestamp_fromtime(QRYqueue[i].finished);
				if (is_timestamp_nil(tsn)) {
					msg = createException(MAL, "SYSMONqueue", SQLSTATE(22003) "Cannot convert time.");
					goto bailout;
				}
			}
			if (BUNappend(finished, &tsn, false) != GDK_SUCCEED)
				goto bailout;
			if (QRYqueue[i].mb)
				wrk = (int) ATOMIC_GET(&QRYqueue[i].mb->workers);
			else
				wrk = QRYqueue[i].workers;
			if (QRYqueue[i].mb)
				mem = (int)(1 + QRYqueue[i].mb->memory / LL_CONSTANT(1048576));
			else
				mem = QRYqueue[i].memory;
			if (BUNappend(workers, &wrk, false) != GDK_SUCCEED ||
				BUNappend(memory, &mem, false) != GDK_SUCCEED)
				goto bailout;
		}
	}
	MT_lock_unset(&mal_delayLock);
	*t = tag->batCacheid;
	BBPkeepref(tag);
	*s = sessionid->batCacheid;
	BBPkeepref(sessionid);
	*u = user->batCacheid;
	BBPkeepref(user);
	*sd = started->batCacheid;
	BBPkeepref(started);
	*ss = status->batCacheid;
	BBPkeepref(status);
	*q = query->batCacheid;
	BBPkeepref(query);
	*f = finished->batCacheid;
	BBPkeepref(finished);
	*w = workers->batCacheid;
	BBPkeepref(workers);
	*m = memory->batCacheid;
	BBPkeepref(memory);
	return MAL_SUCCEED;

 bailout:
	MT_lock_unset(&mal_delayLock);
	BBPunfix(tag->batCacheid);
	BBPunfix(sessionid->batCacheid);
	BBPunfix(user->batCacheid);
	BBPunfix(started->batCacheid);
	BBPunfix(status->batCacheid);
	BBPunfix(query->batCacheid);
	BBPunfix(finished->batCacheid);
	BBPunfix(workers->batCacheid);
	BBPunfix(memory->batCacheid);
	return msg ? msg : createException(MAL, "SYSMONqueue", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
SYSMONpause(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* Temporary hack not allowing MAL clients (mclient -lmal)
	   to use this function */
	if (cntxt->sqlcontext == NULL)
		throw(MAL, "SYSMONpause", SQLSTATE(42000) "Calling from a mclient -lmal.");

	oid tag = 0;
	size_t i = 0;
	bool paused = false;
	bool admin = pci->argc == 3 ? true : false;
	int owner = -1;

	assert(getArgType(mb, pci, 1) == TYPE_lng);

	if ((tag = (oid)*getArgReference_lng(stk, pci, 1)) < 1 )
		throw(MAL, "SYSMONpause", SQLSTATE(22003) "Tag must be positive.");
	if (tag == cntxt->curprg->def->tag)
		throw(MAL, "SYSMONpause", SQLSTATE(HY009) "SYSMONpause cannot pause itself.");

	MT_lock_set(&mal_delayLock);
	for (i = 0; i < qsize; i++) {
		if (QRYqueue[i].tag == tag) {
			if (QRYqueue[i].stk) {
				if (admin || (owner = strcmp(QRYqueue[i].username, cntxt->username)) == 0 ) {
					QRYqueue[i].stk->status = 'p';
					QRYqueue[i].status = "paused";
					paused = true;
				}
				/* tag found, but either not admin or user cannot
				   pause that query with OID ctag */
				break;
			}
			/* tag found, but query could have already finished...
			   stack is 0 by this time.. potential problem?
			   using MAL fcn alarm.sleep exposes the above */
			break;
		}
	}
	MT_lock_unset(&mal_delayLock);

	return paused ? MAL_SUCCEED :
		i == qsize ? createException(MAL, "SYSMONpause", SQLSTATE(42S12) "Tag "LLFMT" unknown.", tag) :
		createException(MAL, "SYSMONpause", SQLSTATE(HY009) "Tag "LLFMT" unknown to the user.", tag);
}

static str
SYSMONresume(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* Temporary hack not allowing MAL clients (mclient -lmal)
	   to use this function */
	if (cntxt->sqlcontext == NULL)
		throw(MAL, "SYSMONresume", SQLSTATE(42000) "Calling from a mclient -lmal.");

	oid tag = 0;
	size_t i = 0;
	bool paused = false;
	bool admin = pci->argc == 3 ? true : false;
	int owner = -1;

	assert(getArgType(mb, pci, 1) == TYPE_lng);

	if ((tag = (oid)*getArgReference_lng(stk, pci, 1)) < 1 )
		throw(MAL, "SYSMONresume", SQLSTATE(22003) "Tag must be positive.");
	if (tag == cntxt->curprg->def->tag)
		throw(MAL, "SYSMONresume", SQLSTATE(HY009) "SYSMONresume cannot pause itself.");

	MT_lock_set(&mal_delayLock);
	for (i = 0; i < qsize; i++) {
		if (QRYqueue[i].tag == tag) {
			if (QRYqueue[i].stk) {
				if (admin || (owner = strcmp(QRYqueue[i].username, cntxt->username)) == 0 ) {
					QRYqueue[i].stk->status = 0;
					QRYqueue[i].status = "running";
					paused = true;
				}
				/* tag found, but either not admin or user cannot
				   pause that query with OID ctag */
				break;
			}
			/* tag found, but query could have already finished...
			   stack is 0 by this time.. potential problem?
			   using MAL fcn alarm.sleep exposes the above */
			break;
		}
	}
	MT_lock_unset(&mal_delayLock);

	return paused ? MAL_SUCCEED :
		i == qsize ? createException(MAL, "SYSMONresume", SQLSTATE(42S12) "Tag "LLFMT" unknown.", tag) :
		createException(MAL, "SYSMONresume", SQLSTATE(HY009) "Tag "LLFMT" unknown to the user.", tag);
}

static str
SYSMONstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* Temporary hack not allowing MAL clients (mclient -lmal)
	   to use this function */
	if (cntxt->sqlcontext == NULL)
		throw(MAL, "SYSMONstop", SQLSTATE(42000) "Calling from a mclient -lmal.");

	oid tag = 0;
	size_t i = 0;
	bool paused = false;
	bool admin = pci->argc == 3 ? true : false;
	int owner = -1;

	assert(getArgType(mb, pci, 1) == TYPE_lng);

	if ((tag = (oid)*getArgReference_lng(stk, pci, 1)) < 1 )
		throw(MAL, "SYSMONstop", SQLSTATE(22003) "Tag must be positive.");
	if (tag == cntxt->curprg->def->tag)
		throw(MAL, "SYSMONstop", SQLSTATE(HY009) "SYSMONstop cannot pause itself.");

	MT_lock_set(&mal_delayLock);
	for (i = 0; i < qsize; i++) {
		if (QRYqueue[i].tag == tag) {
			if (QRYqueue[i].stk) {
				if (admin || (owner = strcmp(QRYqueue[i].username, cntxt->username)) == 0 ) {
					QRYqueue[i].stk->status = 'q';
					QRYqueue[i].status = "stopping";
					paused = true;
				}
				/* tag found, but either not admin or user cannot
				   pause that query with OID ctag */
				break;
			}
			/* tag found, but query could have already finished...
			   stack is 0 by this time.. potential problem?
			   using MAL fcn alarm.sleep exposes the above */
			break;
		}
	}
	MT_lock_unset(&mal_delayLock);

	return paused ? MAL_SUCCEED :
		i == qsize ? createException(MAL, "SYSMONstop", SQLSTATE(42S12) "Tag "LLFMT" unknown.", tag) :
		createException(MAL, "SYSMONstop", SQLSTATE(HY009) "Tag "LLFMT" unknown to the user.", tag);
}

#include "mel.h"
mel_func sysmon_init_funcs[] = {
	pattern("sysmon", "pause", SYSMONpause, true, "Suspend query execution with OID id", args(0, 1, arg("id", lng))),
	pattern("sysmon", "pause", SYSMONpause, true, "Sysadmin call, suspend query execution with OID id belonging to user", args(0, 2, arg("id", lng), arg("user", str))),
	pattern("sysmon", "resume", SYSMONresume, true, "Resume query execution with OID id", args(0, 1, arg("id", lng))),
	pattern("sysmon", "resume", SYSMONresume, true, "Sysadmin call, resume query execution with OID id belonging to user", args(0, 2, arg("id", lng), arg("user", str))),
	pattern("sysmon", "stop", SYSMONstop, true, "Stop query execution with OID id", args(0, 1, arg("id", lng))),
	pattern("sysmon", "stop", SYSMONstop, true, "Sysadmin call, stop query execution with OID id belonging to user", args(0, 2, arg("id", lng), arg("user", str))),
	pattern("sysmon", "queue", SYSMONqueue, false, "A queue of queries that are currently being executed or recently finished", args(9, 9, batarg("tag", lng), batarg("sessionid", int), batarg("user", str), batarg("started", timestamp), batarg("status", str), batarg("query", str), batarg("finished", timestamp), batarg("workers", int), batarg("memory", int))),
	pattern("sysmon", "queue", SYSMONqueue, false, "Sysadmin call, to see either the global queue or user queue of queries that are currently being executed or recently finished", args(9, 10, batarg("tag", lng), batarg("sessionid", int), batarg("user", str), batarg("started", timestamp), batarg("status", str), batarg("query", str), batarg("finished", timestamp), batarg("workers", int), batarg("memory", int), arg("user", str))),
	pattern("sysmon", "user_statistics", SYSMONstatistics, false, "", args(7, 7, batarg("user", str), batarg("querycount", lng), batarg("totalticks", lng), batarg("started", timestamp), batarg("finished", timestamp), batarg("maxticks", lng), batarg("maxquery", str))),
	{ .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_sysmon_mal)
{ mal_module("sysmon", NULL, sysmon_init_funcs); }
