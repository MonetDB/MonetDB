/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sysmon.h"
#include "mal_authorize.h"
#include "mal_runtime.h"
#include "mtime.h"

/* (c) M.L. Kersten
 * The query runtime monitor facility is hardwired 
*/

str
SYSMONqueue(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *tag, *user, *query, *estimate, *started, *progress, *activity, *oids;
	bat *t = getArgReference_bat(stk,pci,0);
	bat *u = getArgReference_bat(stk,pci,1);
	bat *s = getArgReference_bat(stk,pci,2);
	bat *e = getArgReference_bat(stk,pci,3);
	bat *p = getArgReference_bat(stk,pci,4);
	bat *a = getArgReference_bat(stk,pci,5);
	bat *o = getArgReference_bat(stk,pci,6);
	bat *q = getArgReference_bat(stk,pci,7);
	lng now;
	int i, prog;
	str usr;
	timestamp ts, tsn;
	str msg;

	(void) cntxt;
	(void) mb;
	tag = COLnew(0, TYPE_lng, 256, TRANSIENT);
	user = COLnew(0, TYPE_str, 256, TRANSIENT);
	started = COLnew(0, TYPE_timestamp, 256, TRANSIENT);
	estimate = COLnew(0, TYPE_timestamp, 256, TRANSIENT);
	progress = COLnew(0, TYPE_int, 256, TRANSIENT);
	activity = COLnew(0, TYPE_str, 256, TRANSIENT);
	oids = COLnew(0, TYPE_oid, 256, TRANSIENT);
	query = COLnew(0, TYPE_str, 256, TRANSIENT);
	if ( tag == NULL || query == NULL || started == NULL || estimate == NULL || progress == NULL || activity == NULL || oids == NULL){
		if (tag) BBPunfix(tag->batCacheid);
		if (user) BBPunfix(user->batCacheid);
		if (query) BBPunfix(query->batCacheid);
		if (activity) BBPunfix(activity->batCacheid);
		if (started) BBPunfix(started->batCacheid);
		if (estimate) BBPunfix(estimate->batCacheid);
		if (progress) BBPunfix(progress->batCacheid);
		if (oids) BBPunfix(oids->batCacheid);
		throw(MAL, "SYSMONqueue", MAL_MALLOC_FAIL);
	}

	MT_lock_set(&mal_delayLock);
	for ( i = 0; i< QRYqueue[i].tag; i++)
	if( QRYqueue[i].query && (QRYqueue[i].cntxt->idx == 0 || QRYqueue[i].cntxt->user == cntxt->user)) {
		now= (lng) time(0);
		if ( (now-QRYqueue[i].start) > QRYqueue[i].runtime)
			prog =QRYqueue[i].runtime > 0 ? 100: int_nil;
		else
			// calculate progress based on past observations
			prog = (int) ((now- QRYqueue[i].start) / (QRYqueue[i].runtime/100.0));
		
		BUNappend(tag, &QRYqueue[i].tag, FALSE);
		msg = AUTHgetUsername(&usr, cntxt);
		if (msg != MAL_SUCCEED)
			goto bailout;

		BUNappend(user, usr, FALSE);
		GDKfree(usr);
		BUNappend(query, QRYqueue[i].query, FALSE);
		BUNappend(activity, QRYqueue[i].status, FALSE);

		/* convert number of seconds into a timestamp */
		now = QRYqueue[i].start * 1000;
		msg = MTIMEunix_epoch(&ts);
		if (msg)
			goto bailout;
		msg = MTIMEtimestamp_add(&tsn, &ts, &now);
		if (msg)
			goto bailout;
		BUNappend(started, &tsn, FALSE);

		if ( QRYqueue[i].mb->runtime == 0)
			BUNappend(estimate, timestamp_nil, FALSE);
		else{
			now = (QRYqueue[i].start * 1000 + QRYqueue[i].mb->runtime);
			msg = MTIMEunix_epoch(&ts);
			if (msg)
				goto bailout;
			msg = MTIMEtimestamp_add(&tsn, &ts, &now);
			if (msg)
				goto bailout;
			BUNappend(estimate, &tsn, FALSE);
		}
		BUNappend(oids, &QRYqueue[i].mb->tag, FALSE);
		BUNappend(progress, &prog, FALSE);
	}
	MT_lock_unset(&mal_delayLock);
	BBPkeepref( *t =tag->batCacheid);
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
	BBPunfix(user->batCacheid);
	BBPunfix(query->batCacheid);
	BBPunfix(activity->batCacheid);
	BBPunfix(started->batCacheid);
	BBPunfix(estimate->batCacheid);
	BBPunfix(progress->batCacheid);
	BBPunfix(oids->batCacheid);
	return msg;
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
		break;
#endif
	default:
		assert(0);
	}
	MT_lock_set(&mal_delayLock);
	for ( i = 0; QRYqueue[i].tag; i++)
	if( QRYqueue[i].tag == tag && (QRYqueue[i].cntxt->user == cntxt->user || cntxt->idx ==0)){
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
		break;
#endif
	default:
		assert(0);
	}
	MT_lock_set(&mal_delayLock);
	for ( i = 0; QRYqueue[i].tag; i++)
	if( QRYqueue[i].tag == tag && (QRYqueue[i].cntxt->user == cntxt->user || cntxt->idx ==0)){
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
		break;
#endif
	default:
		assert(0);
	}
	MT_lock_set(&mal_delayLock);
	for ( i = 0; QRYqueue[i].tag; i++)
	if( QRYqueue[i].tag == tag && (QRYqueue[i].cntxt->user == cntxt->user || cntxt->idx ==0)){
		QRYqueue[i].stk->status = 'q';
		QRYqueue[i].status = "stopping";
	}
	MT_lock_unset(&mal_delayLock);
	return MAL_SUCCEED;
}
