/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * (c) Martin Kersten
 * This module provides an advisary lock manager for SQL transactions
 * that prefer waiting over transaction failures due to OCC
 * The table may only grow with lockable items
 * It could be extended with a semaphore for queue management
 */
#include "monetdb_config.h"
#include "oltp.h"
#include "gdk_time.h"

#define LOCKTIMEOUT (20 * 1000)
#define LOCKDELAY 20

typedef struct{
	Client cntxt;	// user holding the write lock
	lng start;		// time when it started
	lng retention;	// time when the lock is released
	lng total;		// accumulated lock time
	int used;		// how often it used, for balancing
	int locked;		// writelock set or not
} OLTPlockRecord;

static OLTPlockRecord oltp_locks[MAXOLTPLOCKS];
static int oltp_delay;

/*
static void
OLTPdump_(Client cntxt, str msg)
{
	int i;

	mnstr_printf(cntxt->fdout,"%s",msg);
	for(i=0; i< MAXOLTPLOCKS; i++)
	if( oltp_locks[i].locked)
		mnstr_printf(cntxt->fdout,"#[%i] %3d\n",i, (oltp_locks[i].cntxt ? oltp_locks[i].cntxt->idxx: -1));
}
*/

str
OLTPreset(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	int i;
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	
	MT_lock_set(&mal_oltpLock);

	for( i=0; i<MAXOLTPLOCKS; i++){
		oltp_locks[i].locked = 0;
		oltp_locks[i].cntxt = 0;
		oltp_locks[i].start = 0;
		oltp_locks[i].used = 0;
		oltp_locks[i].retention = 0;
	}
	MT_lock_unset(&mal_oltpLock);
	return MAL_SUCCEED;
}

str
OLTPenable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	(void) cntxt;

	oltp_delay = TRUE;
	return MAL_SUCCEED;
}

str
OLTPdisable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	OLTPreset(cntxt, mb, stk,pci);
	oltp_delay = FALSE;
	(void) cntxt;
	return MAL_SUCCEED;
}

str
OLTPinit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	// nothing to be done right now
	return OLTPreset(cntxt,mb,stk,pci);
}

// The locking is based in the hash-table.
// It contains all write locks outstanding
// A transaction may proceed if no element in its read set is locked

str
OLTPlock(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,lck;
	int clk, wait= GDKms();
	lng now;
	str sql,cpy;

	(void) stk;
	if ( oltp_delay == FALSE )
		return MAL_SUCCEED;

	TRC_DEBUG(MAL_SERVER, "%6d lock request for client: %d, pc %d", GDKms(), cntxt->idx, pci->pc);

	do{
		MT_lock_set(&mal_oltpLock);
		clk = GDKms();
		now = GDKusec();
		// check if all write locks are available 
		for( i=1; i< pci->argc; i++){
			lck= getVarConstant(mb, getArg(pci,i)).val.ival;
			if ( lck > 0 && (oltp_locks[lck].locked || oltp_locks[lck].retention > now ))
				break;
		}

		if( i  == pci->argc ){
			TRC_DEBUG(MAL_SERVER, "OLTP '%6d' set lock for client: %d\n", GDKms(), cntxt->idx);

			for( i=1; i< pci->argc; i++){
				lck= getVarConstant(mb, getArg(pci,i)).val.ival;
				// only set the write locks
				if( lck > 0){
					oltp_locks[lck].cntxt = cntxt;
					oltp_locks[lck].start = now;
					oltp_locks[lck].locked = 1;
					oltp_locks[lck].retention = 0;
				}
			}
			MT_lock_unset(&mal_oltpLock);
			return MAL_SUCCEED;
		} else {
			MT_lock_unset(&mal_oltpLock);
			TRC_DEBUG(MAL_SERVER, "%d delay imposed for client: %d\n", GDKms(), cntxt->idx);
			MT_sleep_ms(LOCKDELAY);
		}
	} while( clk - wait < LOCKTIMEOUT);

	TRC_DEBUG(MAL_SERVER, "%6d proceed query for client: %d\n", GDKms(), cntxt->idx);

	// if the time out is related to a copy_from query, we should not start it either.
	sql = getName("sql");
	cpy = getName("copy_from");

	for( i = 0; i < mb->stop; i++)
		if( getFunctionId(getInstrPtr(mb,i)) == cpy && getModuleId(getInstrPtr(mb,i)) == sql ){
			TRC_DEBUG(MAL_SERVER, "%6d bail out a concurrent copy into: %d\n", GDKms(), cntxt->idx);
			throw(SQL,"oltp.lock","Conflicts with other write operations\n");
		}
	return MAL_SUCCEED;
}

str
OLTPrelease(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,lck;
	lng delay,clk;

	(void) cntxt;
	(void) stk;
	if ( oltp_delay == FALSE )
		return MAL_SUCCEED;

	MT_lock_set(&mal_oltpLock);
	clk = GDKusec();

	TRC_DEBUG(MAL_SERVER, "%6d release the locks: %d", GDKms(), cntxt->idx);

	for( i=1; i< pci->argc; i++){
		lck= getVarConstant(mb, getArg(pci,i)).val.ival;
		if( lck > 0){
				oltp_locks[lck].total += clk - oltp_locks[lck].start;
				oltp_locks[lck].used ++;
				oltp_locks[lck].cntxt = 0;
				oltp_locks[lck].start = 0;
				oltp_locks[lck].locked = 0;
				delay = oltp_locks[lck].total/ oltp_locks[lck].used;
				if( delay > LOCKDELAY || delay < LOCKDELAY/10)
					delay = LOCKDELAY;
				oltp_locks[lck].retention = clk + delay;
				TRC_DEBUG(MAL_SERVER, "Retention period for lock: %d " LLFMT"\n", lck, delay);
			}
	}
	MT_lock_unset(&mal_oltpLock);
	return MAL_SUCCEED;
}

str
OLTPtable(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bs= NULL, *bu= NULL, *bl= NULL, *bq= NULL, *bc = NULL;
	bat *started = getArgReference_bat(stk,pci,0);
	bat *userid = getArgReference_bat(stk,pci,1);
	bat *lockid = getArgReference_bat(stk,pci,2);
	bat *used = getArgReference_bat(stk,pci,3);
	int i;
	str msg = MAL_SUCCEED; 
	timestamp tsn;

	(void) cntxt;
	(void) mb;

	bs = COLnew(0, TYPE_timestamp, 0, TRANSIENT);
	bu = COLnew(0, TYPE_str, 0, TRANSIENT);
	bl = COLnew(0, TYPE_int, 0, TRANSIENT);
	bc = COLnew(0, TYPE_int, 0, TRANSIENT);
	bq = COLnew(0, TYPE_str, 0, TRANSIENT);

	if( bs == NULL || bu == NULL || bl == NULL  || bq == NULL || bc == NULL){
		if( bs) BBPunfix(bs->batCacheid);
		if( bl) BBPunfix(bl->batCacheid);
		if( bu) BBPunfix(bu->batCacheid);
		if( bc) BBPunfix(bc->batCacheid);
		if( bq) BBPunfix(bq->batCacheid);
		throw(MAL,"oltp.table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	for( i = 0; msg ==  MAL_SUCCEED && i < MAXOLTPLOCKS; i++)
	if (oltp_locks[i].used ){
		tsn = oltp_locks[i].start ? timestamp_fromusec(oltp_locks[i].start) : timestamp_nil;
		if (BUNappend(bs, &tsn, false) != GDK_SUCCEED ||
			BUNappend(bu, oltp_locks[i].cntxt ? oltp_locks[i].cntxt->username : str_nil, false) != GDK_SUCCEED ||
			BUNappend(bl, &i, false) != GDK_SUCCEED ||
			BUNappend(bc, &oltp_locks[i].used, false) != GDK_SUCCEED)
			goto bailout;
	}
	//OLTPdump_(cntxt,"#lock table\n");
	BBPkeepref(*started = bs->batCacheid);
	BBPkeepref(*userid = bu->batCacheid);
	BBPkeepref(*lockid = bl->batCacheid);
	BBPkeepref(*used = bc->batCacheid);
	return msg;
  bailout:
	BBPunfix(bs->batCacheid);
	BBPunfix(bl->batCacheid);
	BBPunfix(bu->batCacheid);
	BBPunfix(bc->batCacheid);
	BBPunfix(bq->batCacheid);
	return msg ? msg : createException(MAL, "oltp.table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

str
OLTPis_enabled(int *ret) {
  *ret = oltp_delay;
  return MAL_SUCCEED;
}
