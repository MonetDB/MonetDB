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
	BAT *tag, *sessionid, *user, *started, *status, *query, *progress, *workers, *memory;
	bat *t = getArgReference_bat(stk,pci,0);
	bat *s = getArgReference_bat(stk,pci,1);
	bat *u = getArgReference_bat(stk,pci,2);
	bat *sd = getArgReference_bat(stk,pci,3);
	bat *ss = getArgReference_bat(stk,pci,4);
	bat *q = getArgReference_bat(stk,pci,5);
	bat *p = getArgReference_bat(stk,pci,6);
	bat *w = getArgReference_bat(stk,pci,7);
	bat *m = getArgReference_bat(stk,pci,8);
	lng i, qtag;
	int wrk, mem;
	str usr;
	timestamp tsn;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) mb;
	tag = COLnew(0, TYPE_lng, 256, TRANSIENT);
	sessionid = COLnew(0, TYPE_int, 256, TRANSIENT);
	user = COLnew(0, TYPE_str, 256, TRANSIENT);
	started = COLnew(0, TYPE_timestamp, 256, TRANSIENT);
	status = COLnew(0, TYPE_str, 256, TRANSIENT);
	query = COLnew(0, TYPE_str, 256, TRANSIENT);
	progress = COLnew(0, TYPE_int, 256, TRANSIENT);
	workers = COLnew(0, TYPE_int, 256, TRANSIENT);
	memory = COLnew(0, TYPE_int, 256, TRANSIENT);
	if ( tag == NULL || sessionid == NULL || user == NULL || query == NULL || started == NULL || progress == NULL || workers == NULL || memory == NULL){
		BBPreclaim(tag);
		BBPreclaim(sessionid);
		BBPreclaim(user);
		BBPreclaim(started);
		BBPreclaim(status);
		BBPreclaim(query);
		BBPreclaim(progress);
		BBPreclaim(workers);
		BBPreclaim(memory);
		throw(MAL, "SYSMONqueue", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	MT_lock_set(&mal_delayLock);
	for ( i = 0; i< qtop; i++)
	if( QRYqueue[i].query && (QRYqueue[i].cntxt->user == MAL_ADMIN || QRYqueue[i].cntxt->user == cntxt->user)) {
		qtag = (lng) QRYqueue[i].tag;
		if (BUNappend(tag, &qtag, false) != GDK_SUCCEED)
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
		
		wrk = (int) ATOMIC_GET(&QRYqueue[i].stk->workers);
		mem = (int) ATOMIC_GET(&QRYqueue[i].stk->memory);
		if (BUNappend(progress, &QRYqueue[i].progress, false) != GDK_SUCCEED ||
		    BUNappend(workers, &wrk, false) != GDK_SUCCEED ||
			BUNappend(memory, &mem, false) != GDK_SUCCEED)
			goto bailout;
	}
	MT_lock_unset(&mal_delayLock);
	BBPkeepref( *t =tag->batCacheid);
	BBPkeepref( *s =sessionid->batCacheid);
	BBPkeepref( *u =user->batCacheid);
	BBPkeepref( *sd =started->batCacheid);
	BBPkeepref( *ss =status->batCacheid);
	BBPkeepref( *q =query->batCacheid);
	BBPkeepref( *p =progress->batCacheid);
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
	BBPunfix(progress->batCacheid);
	BBPunfix(workers->batCacheid);
	BBPunfix(memory->batCacheid);
	return msg ? msg : createException(MAL, "SYSMONqueue", SQLSTATE(HY001) MAL_MALLOC_FAIL);
}

str
SYSMONpause(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	lng i, tag = 0;
	(void) mb;
	(void) stk;
	(void) pci;
	
	switch( getArgType(mb,pci,1)){
	case TYPE_bte: tag = *getArgReference_bte(stk,pci,1); break;
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
	case TYPE_bte: tag = *getArgReference_bte(stk,pci,1); break;
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
	case TYPE_bte: tag = *getArgReference_bte(stk,pci,1); break;
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
