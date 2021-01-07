/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_interpreter.h"
#include "mal_authorize.h"
#include "mal_client.h"
#include "mal_runtime.h"
#include "gdk_time.h"

/* (c) M.L. Kersten
 * The queries currently in execution are returned to the front-end for managing expensive ones.
*/

static str
SYSMONstatistics(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
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

	(void) mb;
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
	for (i = 0 && cntxt->user == MAL_ADMIN; i < usrstatscnt; i++) {
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
	BBPkeepref(*u = user->batCacheid);
	BBPkeepref(*c = querycount->batCacheid);
	BBPkeepref(*t = totalticks->batCacheid);
	BBPkeepref(*s = started->batCacheid);
	BBPkeepref(*f = finished->batCacheid);
	BBPkeepref(*m = maxticks->batCacheid);
	BBPkeepref(*q = maxquery->batCacheid);
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
	BAT *tag, *sessionid, *user, *started, *status, *query, *finished, *workers, *memory;
	bat *t = getArgReference_bat(stk,pci,0);
	bat *s = getArgReference_bat(stk,pci,1);
	bat *u = getArgReference_bat(stk,pci,2);
	bat *sd = getArgReference_bat(stk,pci,3);
	bat *ss = getArgReference_bat(stk,pci,4);
	bat *q = getArgReference_bat(stk,pci,5);
	bat *f = getArgReference_bat(stk,pci,6);
	bat *w = getArgReference_bat(stk,pci,7);
	bat *m = getArgReference_bat(stk,pci,8);
	lng qtag;
	int wrk, mem;
	BUN sz;
	timestamp tsn;
	str msg = MAL_SUCCEED;

	(void) mb;
	sz = (BUN) qsize;	// reserve space for all tuples in QRYqueue
	tag = COLnew(0, TYPE_lng, sz, TRANSIENT);
	sessionid = COLnew(0, TYPE_int, sz, TRANSIENT);
	user = COLnew(0, TYPE_str, sz, TRANSIENT);
	started = COLnew(0, TYPE_timestamp, sz, TRANSIENT);
	status = COLnew(0, TYPE_str, sz, TRANSIENT);
	query = COLnew(0, TYPE_str, sz, TRANSIENT);
	finished = COLnew(0, TYPE_timestamp, sz, TRANSIENT);
	workers = COLnew(0, TYPE_int, sz, TRANSIENT);
	memory = COLnew(0, TYPE_int, sz, TRANSIENT);
	if ( tag == NULL || sessionid == NULL || user == NULL || query == NULL || started == NULL || finished == NULL || workers == NULL || memory == NULL){
		BBPreclaim(tag);
		BBPreclaim(sessionid);
		BBPreclaim(user);
		BBPreclaim(started);
		BBPreclaim(status);
		BBPreclaim(query);
		BBPreclaim(finished);
		BBPreclaim(workers);
		BBPreclaim(memory);
		throw(MAL, "SYSMONqueue", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	MT_lock_set(&mal_delayLock);
	for (size_t i = qtail; i != qhead; i++){
		if ( i == qsize){
			i = 0;
			if( i == qhead)
				break;
		}
		if( QRYqueue[i].query && (cntxt->user == MAL_ADMIN || 
					strcmp(cntxt->username, QRYqueue[i].username) == 0) ){
			qtag = (lng) QRYqueue[i].tag;
			if (BUNappend(tag, &qtag, false) != GDK_SUCCEED)
				goto bailout;

			if (BUNappend(user, QRYqueue[i].username, false) != GDK_SUCCEED) {
				goto bailout;
			}

			if (BUNappend(sessionid, &(QRYqueue[i].idx), false) != GDK_SUCCEED) {
				goto bailout;
			}

			if (BUNappend(query, QRYqueue[i].query, false) != GDK_SUCCEED ||
				BUNappend(status, QRYqueue[i].status, false) != GDK_SUCCEED)
				goto bailout;

			/* convert number of seconds into a timestamp */
			tsn = timestamp_fromtime(QRYqueue[i].start);
			if (is_timestamp_nil(tsn)) {
				msg = createException(MAL, "SYSMONqueue", SQLSTATE(22003) "cannot convert time");
				goto bailout;
			}
			if (BUNappend(started, &tsn, false) != GDK_SUCCEED)
				goto bailout;

			if (QRYqueue[i].finished == 0) {
				tsn = timestamp_nil;
			} else {
				tsn = timestamp_fromtime(QRYqueue[i].finished);
				if (is_timestamp_nil(tsn)) {
					msg = createException(MAL, "SYSMONqueue", SQLSTATE(22003) "cannot convert time");
					goto bailout;
				}
			}
			if (BUNappend(finished, &tsn, false) != GDK_SUCCEED)
				goto bailout;

			wrk = QRYqueue[i].workers;
			mem = QRYqueue[i].memory;
			if ( BUNappend(workers, &wrk, false) != GDK_SUCCEED ||
				 BUNappend(memory, &mem, false) != GDK_SUCCEED)
				goto bailout;
		}
	}
	MT_lock_unset(&mal_delayLock);
	BBPkeepref( *t =tag->batCacheid);
	BBPkeepref( *s =sessionid->batCacheid);
	BBPkeepref( *u =user->batCacheid);
	BBPkeepref( *sd =started->batCacheid);
	BBPkeepref( *ss =status->batCacheid);
	BBPkeepref( *q =query->batCacheid);
	BBPkeepref( *f =finished->batCacheid);
	BBPkeepref( *w =workers->batCacheid);
	BBPkeepref( *m =memory->batCacheid);
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
	bool set = false;
	lng tag = 0;

	switch(getArgType(mb,pci,1)){
	case TYPE_bte: tag = *getArgReference_bte(stk,pci,1); break;
	case TYPE_sht: tag = *getArgReference_sht(stk,pci,1); break;
	case TYPE_int: tag = *getArgReference_int(stk,pci,1); break;
	case TYPE_lng: tag = *getArgReference_lng(stk,pci,1); break;
	default:
		throw(MAL, "SYSMONpause", SQLSTATE(42000) "SYSMONpause requires a 64-bit integer");
	}
	if (tag < 1)
		throw(MAL, "SYSMONpause", SQLSTATE(42000) "Tag must be positive");
	MT_lock_set(&mal_delayLock);
	for (size_t i = qtail; i != qhead; i++){
		if( i == qsize){
			i = 0;
			if( i == qhead)
				break;
		}
		if( (lng) QRYqueue[i].tag == tag && cntxt->user == MAL_ADMIN && QRYqueue[i].stk){
			QRYqueue[i].stk->status = 'p';
			QRYqueue[i].status = "paused";
			set = true;
		}
	}
	MT_lock_unset(&mal_delayLock);
	return set ? MAL_SUCCEED : createException(MAL, "SYSMONpause", SQLSTATE(42000) "Tag " LLFMT " unknown", tag);
}

static str
SYSMONresume(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bool set = false;
	lng tag = 0;

	switch(getArgType(mb,pci,1)){
	case TYPE_bte: tag = *getArgReference_bte(stk,pci,1); break;
	case TYPE_sht: tag = *getArgReference_sht(stk,pci,1); break;
	case TYPE_int: tag = *getArgReference_int(stk,pci,1); break;
	case TYPE_lng: tag = *getArgReference_lng(stk,pci,1); break;
	default:
		throw(MAL, "SYSMONresume", SQLSTATE(42000) "SYSMONresume requires a 64-bit integer");
	}
	if (tag < 1)
		throw(MAL, "SYSMONresume", SQLSTATE(42000) "Tag must be positive");
	MT_lock_set(&mal_delayLock);
	for (size_t i = qtail; i == qhead; i++){
		if( i == qsize){
			i = 0;
			if ( i== qhead)
				break;
		}
		if( (lng)QRYqueue[i].tag == tag && cntxt->user == MAL_ADMIN && QRYqueue[i].stk){
			QRYqueue[i].stk->status = 0;
			QRYqueue[i].status = "running";
			set = true;
		}
	}
	MT_lock_unset(&mal_delayLock);
	return set ? MAL_SUCCEED : createException(MAL, "SYSMONresume", SQLSTATE(42000) "Tag " LLFMT " unknown", tag);
}

static str
SYSMONstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bool set = false;
	lng tag = 0;

	switch(getArgType(mb,pci,1)){
	case TYPE_bte: tag = *getArgReference_bte(stk,pci,1); break;
	case TYPE_sht: tag = *getArgReference_sht(stk,pci,1); break;
	case TYPE_int: tag = *getArgReference_int(stk,pci,1); break;
	case TYPE_lng: tag = *getArgReference_lng(stk,pci,1); break;
	default:
		throw(MAL, "SYSMONstop", SQLSTATE(42000) "SYSMONstop requires a 64-bit integer");
	}
	if (tag < 1)
		throw(MAL, "SYSMONstop", SQLSTATE(42000) "Tag must be positive");
	MT_lock_set(&mal_delayLock);
	for (size_t i = qtail; i != qhead; i++){
		if( i == qsize){
			i = 0;
			if( i == qhead)
				break;
		}
		if( (lng) QRYqueue[i].tag == tag && cntxt->user == MAL_ADMIN && QRYqueue[i].stk){
			QRYqueue[i].stk->status = 'q';
			QRYqueue[i].status = "stopping";
			set = true;
		}
	}
	MT_lock_unset(&mal_delayLock);
	return set ? MAL_SUCCEED : createException(MAL, "SYSMONstop", SQLSTATE(42000) "Tag " LLFMT " unknown", tag);
}

#include "mel.h"
mel_func sysmon_init_funcs[] = {
 pattern("sysmon", "pause", SYSMONpause, false, "Suspend a running query", args(0,1, arg("id",sht))),
 pattern("sysmon", "pause", SYSMONpause, false, "Suspend a running query", args(0,1, arg("id",int))),
 pattern("sysmon", "pause", SYSMONpause, false, "Suspend a running query", args(0,1, arg("id",lng))),
 pattern("sysmon", "resume", SYSMONresume, false, "Resume processing of a query ", args(0,1, arg("id",sht))),
 pattern("sysmon", "resume", SYSMONresume, false, "Resume processing of a query ", args(0,1, arg("id",int))),
 pattern("sysmon", "resume", SYSMONresume, false, "Resume processing of a query ", args(0,1, arg("id",lng))),
 pattern("sysmon", "stop", SYSMONstop, false, "Stop a single query a.s.a.p.", args(0,1, arg("id",sht))),
 pattern("sysmon", "stop", SYSMONstop, false, "Stop a single query a.s.a.p.", args(0,1, arg("id",int))),
 pattern("sysmon", "stop", SYSMONstop, false, "Stop a single query a.s.a.p.", args(0,1, arg("id",lng))),
 pattern("sysmon", "queue", SYSMONqueue, false, "A queue of queries that are currently being executed or recently finished", args(9,9, batarg("tag",lng),batarg("sessionid",int),batarg("user",str),batarg("started",timestamp),batarg("status",str),batarg("query",str),batarg("finished",timestamp),batarg("workers",int),batarg("memory",int))),
 pattern("sysmon", "user_statistics", SYSMONstatistics, false, "", args(7,7, batarg("user",str),batarg("querycount",lng),batarg("totalticks",lng),batarg("started",timestamp),batarg("finished",timestamp),batarg("maxticks",lng),batarg("maxquery",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_sysmon_mal)
{ mal_module("sysmon", NULL, sysmon_init_funcs); }
