/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sysmon.h"
#include "mal_authorize.h"
#include "mal_runtime.h"
#include "mtime.h"

/* (c) M.L. Kersten
 * The queries currently in execution are returned to the front-end for managing expensive ones.
*/

str
SYSMONqueue(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *tag, *sessionid, *user, *query, *estimate, *started, *progress, *activity, *oids;
	bat *t = getArgReference_bat(stk,pci,0);
	bat *d = getArgReference_bat(stk,pci,1);
	bat *u = getArgReference_bat(stk,pci,2);
	bat *s = getArgReference_bat(stk,pci,3);
	bat *e = getArgReference_bat(stk,pci,4);
	bat *p = getArgReference_bat(stk,pci,5);
	bat *a = getArgReference_bat(stk,pci,6);
	bat *o = getArgReference_bat(stk,pci,7);
	bat *q = getArgReference_bat(stk,pci,8);
	time_t now;
	lng i;
	int prog;
	str usr;
	timestamp tsn;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) mb;
	tag = COLnew(0, TYPE_lng, 256, TRANSIENT);
	sessionid = COLnew(0, TYPE_int, 256, TRANSIENT);
	user = COLnew(0, TYPE_str, 256, TRANSIENT);
	started = COLnew(0, TYPE_timestamp, 256, TRANSIENT);
	estimate = COLnew(0, TYPE_timestamp, 256, TRANSIENT);
	progress = COLnew(0, TYPE_int, 256, TRANSIENT);
	activity = COLnew(0, TYPE_str, 256, TRANSIENT);
	oids = COLnew(0, TYPE_oid, 256, TRANSIENT);
	query = COLnew(0, TYPE_str, 256, TRANSIENT);
	if ( tag == NULL || sessionid == NULL || user == NULL || query == NULL || started == NULL || estimate == NULL || progress == NULL || activity == NULL || oids == NULL){
		BBPreclaim(tag);
		BBPreclaim(sessionid);
		BBPreclaim(user);
		BBPreclaim(query);
		BBPreclaim(activity);
		BBPreclaim(started);
		BBPreclaim(estimate);
		BBPreclaim(progress);
		BBPreclaim(oids);
		throw(MAL, "SYSMONqueue", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	MT_lock_set(&mal_delayLock);
	for ( i = 0; i< qtop; i++)
	if( QRYqueue[i].query && (QRYqueue[i].cntxt->user == MAL_ADMIN || QRYqueue[i].cntxt->user == cntxt->user)) {
		now= time(0);
		if ( (now-QRYqueue[i].start) > QRYqueue[i].runtime)
			prog =QRYqueue[i].runtime > 0 ? 100: int_nil;
		else
			// calculate progress based on past observations
			prog = (int) ((now- QRYqueue[i].start) / (QRYqueue[i].runtime/100.0));
		if (BUNappend(tag, &(lng){QRYqueue[i].tag}, false) != GDK_SUCCEED)
			goto bailout;
		msg = AUTHgetUsername(&usr, QRYqueue[i].cntxt);
		if (msg != MAL_SUCCEED)
			goto bailout;

		if (BUNappend(sessionid, &(QRYqueue[i].cntxt->idx), false) != GDK_SUCCEED) {
			GDKfree(usr);
			goto bailout;
		}

		if (BUNappend(user, usr, false) != GDK_SUCCEED) {
			GDKfree(usr);
			goto bailout;
		}
		GDKfree(usr);
		if (BUNappend(query, QRYqueue[i].query, false) != GDK_SUCCEED ||
			BUNappend(activity, QRYqueue[i].status, false) != GDK_SUCCEED)
			goto bailout;

		/* convert number of seconds into a timestamp */
		tsn = timestamp_fromtime(QRYqueue[i].start);
		if (is_timestamp_nil(tsn)) {
			msg = createException(MAL, "SYSMONqueue", SQLSTATE(22003) "cannot convert time");
			goto bailout;
		}
		if (BUNappend(started, &tsn, false) != GDK_SUCCEED)
			goto bailout;

		if ( QRYqueue[i].mb->runtime == 0) {
			if (BUNappend(estimate, &timestamp_nil, false) != GDK_SUCCEED)
				goto bailout;
		} else {
			tsn = timestamp_add_usec(tsn, 1000 * QRYqueue[i].mb->runtime);
			if (is_timestamp_nil(tsn)) {
				msg = createException(MAL, "SYSMONqueue", SQLSTATE(22003) "cannot convert time");
				goto bailout;
			}
			if (BUNappend(estimate, &tsn, false) != GDK_SUCCEED)
				goto bailout;
		}
		if (BUNappend(oids, &QRYqueue[i].mb->tag, false) != GDK_SUCCEED ||
			BUNappend(progress, &prog, false) != GDK_SUCCEED)
			goto bailout;
	}
	MT_lock_unset(&mal_delayLock);
	BBPkeepref( *t =tag->batCacheid);
	BBPkeepref( *d =sessionid->batCacheid);
	BBPkeepref( *u =user->batCacheid);
	BBPkeepref( *s =started->batCacheid);
	BBPkeepref( *e = estimate->batCacheid);
	BBPkeepref( *a =activity->batCacheid);
	BBPkeepref( *p =progress->batCacheid);
	BBPkeepref( *o =oids->batCacheid);
	BBPkeepref( *q =query->batCacheid);
	return MAL_SUCCEED;

  bailout:
	MT_lock_unset(&mal_delayLock);
	BBPunfix(tag->batCacheid);
	BBPunfix(sessionid->batCacheid);
	BBPunfix(user->batCacheid);
	BBPunfix(query->batCacheid);
	BBPunfix(activity->batCacheid);
	BBPunfix(started->batCacheid);
	BBPunfix(estimate->batCacheid);
	BBPunfix(progress->batCacheid);
	BBPunfix(oids->batCacheid);
	return msg ? msg : createException(MAL, "SYSMONqueue", SQLSTATE(HY001) MAL_MALLOC_FAIL);
}

str
SYSMONpause(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	lng i, tag = 0;
	(void) mb;
	(void) stk;
	(void) pci;
	
	switch( getArgType(mb,pci,1)){
	case TYPE_sht: tag = *getArgReference_sht(stk,pci,1); break;
	case TYPE_int: tag = *getArgReference_int(stk,pci,1); break;
	case TYPE_lng: tag = *getArgReference_lng(stk,pci,1); break;
#ifdef HAVE_HGE
	case TYPE_hge:
		/* Does this happen?
		 * If so, what do we have TODO ? */
		throw(MAL, "SYSMONpause", "type hge not handled, yet");
#endif
	default:
		throw(MAL, "SYSMONpause", "Pause requires integer");
	}
	MT_lock_set(&mal_delayLock);
	for ( i = 0; QRYqueue[i].tag; i++)
		if( (lng) QRYqueue[i].tag == tag && (QRYqueue[i].cntxt->user == cntxt->user || cntxt->user == MAL_ADMIN)){
			QRYqueue[i].stk->status = 'p';
			QRYqueue[i].status = "paused";
		}
	MT_lock_unset(&mal_delayLock);
	return MAL_SUCCEED;
}

str
SYSMONresume(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	lng i,tag = 0;
	(void) mb;
	(void) stk;
	(void) pci;
	
	switch( getArgType(mb,pci,1)){
	case TYPE_sht: tag = *getArgReference_sht(stk,pci,1); break;
	case TYPE_int: tag = *getArgReference_int(stk,pci,1); break;
	case TYPE_lng: tag = *getArgReference_lng(stk,pci,1); break;
#ifdef HAVE_HGE
	case TYPE_hge:
		/* Does this happen?
		 * If so, what do we have TODO ? */
		throw(MAL, "SYSMONresume", "type hge not handled, yet");
#endif
	default:
		throw(MAL, "SYSMONresume", "Resume requires integer");
	}
	MT_lock_set(&mal_delayLock);
	for ( i = 0; QRYqueue[i].tag; i++)
		if( (lng)QRYqueue[i].tag == tag && (QRYqueue[i].cntxt->user == cntxt->user || cntxt->user == MAL_ADMIN)){
			QRYqueue[i].stk->status = 0;
			QRYqueue[i].status = "running";
		}
	MT_lock_unset(&mal_delayLock);
	return MAL_SUCCEED;
}

str
SYSMONstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	lng i,tag = 0;
	(void) mb;
	(void) stk;
	(void) pci;
	
	switch( getArgType(mb,pci,1)){
	case TYPE_sht: tag = *getArgReference_sht(stk,pci,1); break;
	case TYPE_int: tag = *getArgReference_int(stk,pci,1); break;
	case TYPE_lng: tag = *getArgReference_lng(stk,pci,1); break;
#ifdef HAVE_HGE
	case TYPE_hge:
		/* Does this happen?
		 * If so, what do we have TODO ? */
		throw(MAL, "SYSMONstop", "type hge not handled, yet");
#endif
	default:
		throw(MAL, "SYSMONstop", "Stop requires integer");
	}
	MT_lock_set(&mal_delayLock);
	for ( i = 0; QRYqueue[i].tag; i++)
		if( (lng) QRYqueue[i].tag == tag && (QRYqueue[i].cntxt->user == cntxt->user || cntxt->user == MAL_ADMIN)){
			QRYqueue[i].stk->status = 'q';
			QRYqueue[i].status = "stopping";
		}
	MT_lock_unset(&mal_delayLock);
	return MAL_SUCCEED;
}
