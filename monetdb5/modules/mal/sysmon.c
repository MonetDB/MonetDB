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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
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
	int *t = (int*) getArgReference(stk,pci,0);
	int *u = (int*) getArgReference(stk,pci,1);
	int *s = (int*) getArgReference(stk,pci,2);
	int *e = (int*) getArgReference(stk,pci,3);
	int *p = (int*) getArgReference(stk,pci,4);
	int *a = (int*) getArgReference(stk,pci,5);
	int *o = (int*) getArgReference(stk,pci,6);
	int *q = (int*) getArgReference(stk,pci,7);
	lng now;
	int i, prog;
	str usr;
	timestamp ts, tsn;
	
	(void) cntxt;
	(void) mb;
	tag = BATnew(TYPE_void, TYPE_lng, 256);
	user = BATnew(TYPE_void, TYPE_str, 256);
	started = BATnew(TYPE_void, TYPE_lng, 256);
	estimate = BATnew(TYPE_void, TYPE_lng, 256);
	progress = BATnew(TYPE_void, TYPE_int, 256);
	activity = BATnew(TYPE_void, TYPE_str, 256);
	oids = BATnew(TYPE_void, TYPE_oid, 256);
	query = BATnew(TYPE_void, TYPE_str, 256);
	if ( tag == NULL || query == NULL || started == NULL || estimate == NULL || progress == NULL || activity == NULL || oids == NULL){
		if (tag) BBPreleaseref(tag->batCacheid);
		if (user) BBPreleaseref(user->batCacheid);
		if (query) BBPreleaseref(query->batCacheid);
		if (activity) BBPreleaseref(activity->batCacheid);
		if (started) BBPreleaseref(started->batCacheid);
		if (estimate) BBPreleaseref(estimate->batCacheid);
		if (progress) BBPreleaseref(progress->batCacheid);
		if (oids) BBPreleaseref(oids->batCacheid);
		throw(MAL, "SYSMONqueue", MAL_MALLOC_FAIL);
	}
	BATseqbase(tag, 0);
    BATkey(tag, TRUE);

	BATseqbase(user, 0);
    BATkey(user, TRUE);

	BATseqbase(query, 0);
    BATkey(query, TRUE);

	BATseqbase(activity, 0);
    BATkey(activity, TRUE);

	BATseqbase(estimate, 0);
    BATkey(estimate, TRUE);

	BATseqbase(started, 0);
    BATkey(started, TRUE);

	BATseqbase(progress, 0);
    BATkey(progress, TRUE);

	BATseqbase(oids, 0);
    BATkey(oids, TRUE);

	MT_lock_set(&mal_delayLock, "sysmon");
	for ( i = 0; i< QRYqueue[i].tag; i++)
	if( QRYqueue[i].query && (QRYqueue[i].cntxt->idx == 0 || QRYqueue[i].cntxt->user == cntxt->user)) {
		now= (lng) time(0);
		if ( (now-QRYqueue[i].start) > QRYqueue[i].runtime)
			prog =QRYqueue[i].runtime > 0 ? 100: int_nil;
		else
			// calculate progress based on past observations
			prog = (int) ((now- QRYqueue[i].start) / (QRYqueue[i].runtime/100.0));
		
		BUNappend(tag, &QRYqueue[i].tag, FALSE);
		AUTHgetUsername(&usr, &cntxt);

		BUNappend(user, usr, FALSE);
		BUNappend(query, QRYqueue[i].query, FALSE);
		BUNappend(activity, QRYqueue[i].status, FALSE);

		/* convert number of seconds into a timestamp */
		now = QRYqueue[i].start * 1000;
		(void) MTIMEunix_epoch(&ts);
		(void) MTIMEtimestamp_add(&tsn, &ts, &now);
		BUNappend(started, &tsn, FALSE);

		if ( QRYqueue[i].mb->runtime == 0)
			BUNappend(estimate, timestamp_nil, FALSE);
		else{
			now = (QRYqueue[i].start * 1000 + QRYqueue[i].mb->runtime);
			(void) MTIMEunix_epoch(&ts);
			(void) MTIMEtimestamp_add(&tsn, &ts, &now);
			BUNappend(estimate, &tsn, FALSE);
		}
		BUNappend(oids, &QRYqueue[i].mb->tag, FALSE);
		BUNappend(progress, &prog, FALSE);
	}
	MT_lock_unset(&mal_delayLock, "sysmon");
	BBPkeepref( *t =tag->batCacheid);
	BBPkeepref( *u =user->batCacheid);
	BBPkeepref( *s =started->batCacheid);
	BBPkeepref( *e = estimate->batCacheid);
	BBPkeepref( *a =activity->batCacheid);
	BBPkeepref( *p =progress->batCacheid);
	BBPkeepref( *o =oids->batCacheid);
	BBPkeepref( *q =query->batCacheid);
	return MAL_SUCCEED;
}

str
SYSMONpause(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	lng i, tag = 0;
	(void) mb;
	(void) stk;
	(void) pci;
	
	switch( getArgType(mb,pci,1)){
	case TYPE_sht: tag = *(sht*) getArgReference(stk,pci,1); break;
	case TYPE_int: tag = *(int*) getArgReference(stk,pci,1); break;
	case TYPE_lng: tag = *(lng*) getArgReference(stk,pci,1); 
	}
	MT_lock_set(&mal_delayLock, "sysmon");
	for ( i = 0; QRYqueue[i].tag; i++)
	if( QRYqueue[i].tag == tag && (QRYqueue[i].cntxt->user == cntxt->user || cntxt->idx ==0)){
		QRYqueue[i].stk->status = 'p';
		QRYqueue[i].status = "paused";
	}
	MT_lock_unset(&mal_delayLock, "sysmon");
	return MAL_SUCCEED;
}

str
SYSMONresume(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	lng i,tag = 0;
	(void) mb;
	(void) stk;
	(void) pci;
	
	switch( getArgType(mb,pci,1)){
	case TYPE_sht: tag = *(sht*) getArgReference(stk,pci,1); break;
	case TYPE_int: tag = *(int*) getArgReference(stk,pci,1); break;
	case TYPE_lng: tag = *(lng*) getArgReference(stk,pci,1); 
	}
	MT_lock_set(&mal_delayLock, "sysmon");
	for ( i = 0; QRYqueue[i].tag; i++)
	if( QRYqueue[i].tag == tag && (QRYqueue[i].cntxt->user == cntxt->user || cntxt->idx ==0)){
		QRYqueue[i].stk->status = 0;
		QRYqueue[i].status = "running";
	}
	MT_lock_unset(&mal_delayLock, "sysmon");
	return MAL_SUCCEED;
}

str
SYSMONstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	lng i,tag = 0;
	(void) mb;
	(void) stk;
	(void) pci;
	
	switch( getArgType(mb,pci,1)){
	case TYPE_sht: tag = *(sht*) getArgReference(stk,pci,1); break;
	case TYPE_int: tag = *(int*) getArgReference(stk,pci,1); break;
	case TYPE_lng: tag = *(lng*) getArgReference(stk,pci,1); 
	}
	MT_lock_set(&mal_delayLock, "sysmon");
	for ( i = 0; QRYqueue[i].tag; i++)
	if( QRYqueue[i].tag == tag && (QRYqueue[i].cntxt->user == cntxt->user || cntxt->idx ==0)){
		QRYqueue[i].stk->status = 'q';
		QRYqueue[i].status = "stopping";
	}
	MT_lock_unset(&mal_delayLock, "sysmon");
	return MAL_SUCCEED;
}
