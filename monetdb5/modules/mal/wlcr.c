/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (c) Martin Kersten
 * This module collects the workload-capture-replay statements during transaction execution. 
 *
 * Each wlcr log file contains a serial log for a transaction batch. 
 * Each job is identified by the original owner of the query, the snapshot identity (name+nr) against
 * which it was ran, an indication of the kind of transaction and commit/rollback, its runtime (in ms) and starting time.
 *
 * Replaying the wlcr against another server based on the same snapshot should produce a perfect copy.
 * Each job should be executed using the credentials of the user issuing the transaction.
 * Any failuer encountered terminates the replication process.
 * 
 * All wlcr files should be stored on a shared file system for all replicas to access.
 * The default is a subdirectory of the database and act as a secondary database rebuild log.
 * The location can be overruled using a full path to a shared disk as GDKenvironemnt variable (wlcr_dir)
 *
 * The wlcr files have a textual format derived from the MAL statements.
 * This can be used to ease the implementation of the wlreplay
 *
 * The logs may only be removed after a new snapshot has been taken or wlcr is disabled
 */
#include "monetdb_config.h"
#include "mal_builder.h"
#include "wlcr.h"

static MT_Lock     wlcr_lock MT_LOCK_INITIALIZER("wlcr_lock");


int wlcr_duration = INT_MAX; // how long to capture default= 0
int wlcr_threshold = 0; // threshold (milliseconds) for keeping readonly queries
int wlcr_deltas = 1;  // sent the delta values
int wlcr_all = 1;	// also ship failed transaction
str wlcr_snapshot= "baseline";	// name assigned to the snapshot
int wlcr_unit = 0;	// last job executed

static char *wlcr_name[]= {"","query","update","catalog"};

static stream *wlcr_fd = 0;
static str wlcr_log = "/tmp/wlcr";

static InstrPtr
WLCRaddtime(Client cntxt, InstrPtr pci, InstrPtr p)
{
	char *tbuf;
    char ctm[26];
    time_t clk = pci->clock.tv_sec;

#ifdef HAVE_CTIME_R3
    tbuf = ctime_r(&clk, ctm, sizeof(ctm));
#else
#ifdef HAVE_CTIME_R
    tbuf = ctime_r(&clk, ctm);
#else
    tbuf = ctime(&clk);
#endif
#endif
    tbuf[19]=0;
	return pushStr(cntxt->wlcr, p, tbuf);
}

#define WLCR_start()\
{ Symbol s; \
	if( cntxt->wlcr == NULL){\
		s = newSymbol("wlrc", FUNCTIONsymbol);\
		cntxt->wlcr_kind = WLCR_QUERY;\
		cntxt->wlcr = s->def;\
		s->def = NULL;\
	} \
	if( cntxt->wlcr->stop == 0){\
		p = newStmt(cntxt->wlcr,"wlreplay","job");\
		p = pushStr(cntxt->wlcr,p, cntxt->username);\
		p = pushStr(cntxt->wlcr,p, wlcr_snapshot);\
		p = pushInt(cntxt->wlcr,p, wlcr_unit);\
		p = WLCRaddtime(cntxt,pci, p); \
		p->ticks = GDKms();\
}	}

str
WLCRproperties (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;

	(void) cntxt;
	(void) mb;

	i = *getArgReference_int(stk,pci,1);
	if ( i < 0)
		throw(MAL,"wlcr.properties","Duration must be a possitive number");
	wlcr_duration = i;

	i = *getArgReference_int(stk,pci,2);
	if ( i < 0)
		throw(MAL,"wlcr.properties","Duration must be a possitive number");
	wlcr_threshold = i;

	wlcr_deltas = *getArgReference_int(stk,pci,3) != 0;
	wlcr_all = *getArgReference_int(stk,pci,4) != 0;
	return MAL_SUCCEED;
}

str
WLCRjob(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str snapshot = *getArgReference_str(stk,pci,1);
	int tid = *getArgReference_int(stk,pci,2);

	(void) cntxt;
	(void) mb;

	if ( strcmp(snapshot, wlcr_snapshot))
		throw(MAL,"wlcr.job","Incompatible snapshot identifier");
	if ( tid < wlcr_unit)
		throw(MAL,"wlcr.job","Work unit identifier is before last one executed");
	return MAL_SUCCEED;
}

str
WLCRfin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}

str
WLCRquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	InstrPtr p;

	(void) stk;
	if ( strcmp("-- no query",getVarConstant(mb, getArg(pci,1)).val.sval) == 0)
		return MAL_SUCCEED;
	WLCR_start();
	p = newStmt(cntxt->wlcr, "wlreplay","query");
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,2)).val.sval);
	p->ticks = GDKms();
	return MAL_SUCCEED;
}

str
WLCRgeneric(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	InstrPtr p;
	int i, tpe, varid;
	(void) stk;
	
	WLCR_start();
	p = newStmt(cntxt->wlcr, "wlreplay",getFunctionId(pci));
	for( i = pci->retc; i< pci->argc; i++){
		tpe =getArgType(mb, pci, i);
		switch(tpe){
		case TYPE_str:
			p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci, i)).val.sval);
			break;
		default:
			varid = defConstant(cntxt->wlcr, tpe, getArgReference(stk, pci, i));
			p = pushArgument(cntxt->wlcr, p, varid);
		}
	}
	p->ticks = GDKms();
	cntxt->wlcr_kind = WLCR_CATALOG;
	return MAL_SUCCEED;
}

#define bulk(TPE1, TPE2)\
{	TPE1 *p = (TPE1 *) Tloc(b,0);\
	TPE1 *q = (TPE1 *) Tloc(b, BUNlast(b));\
	int k=0; \
	for( ; p < q; p++, k++){\
		if( k % 32 == 31){\
			pci = newStmt(cntxt->wlcr, "wlreplay",getFunctionId(pci));\
			pci = pushStr(cntxt->wlcr, pci, sch);\
			pci = pushStr(cntxt->wlcr, pci, tbl);\
			pci = pushStr(cntxt->wlcr, pci, col);\
			pci->ticks = GDKms();\
		}\
		pci = push##TPE2(cntxt->wlcr, pci ,*p);\
} }

static void
WLCRdatashipping(Client cntxt, MalBlkPtr mb, InstrPtr pci, int bid)
{	BAT *b;
	str sch,tbl,col;
	(void) cntxt;
	(void) mb;
	b= BATdescriptor(bid);
	assert(b);

// large BATs can also be re-created using the query.
// Copy into should always be expanded, because the source may not
// be accessible in the replica. TODO

	sch = GDKstrdup(getVarConstant(cntxt->wlcr, getArg(pci,1)).val.sval);
	tbl = GDKstrdup(getVarConstant(cntxt->wlcr, getArg(pci,2)).val.sval);
	col = GDKstrdup(getVarConstant(cntxt->wlcr, getArg(pci,3)).val.sval);
	if (cntxt->wlcr_kind < WLCR_UPDATE)
		cntxt->wlcr_kind = WLCR_UPDATE;
	switch( ATOMstorage(b->ttype)){
	case TYPE_bit: bulk(bit,Bit); break;
	case TYPE_bte: bulk(bte,Bte); break;
	case TYPE_sht: bulk(sht,Sht); break;
	case TYPE_int: bulk(int,Int); break;
	case TYPE_lng: bulk(lng,Lng); break;
	case TYPE_flt: bulk(flt,Flt); break;
	case TYPE_dbl: bulk(dbl,Dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: bulk(hge,Hge); break;
#endif
	case TYPE_str: 
		{	BATiter bi;
			BUN p,q;
			int k=0; 
			bi= bat_iterator(b);
			BATloop(b,p,q){
				if( k % 32 == 31){
					pci = newStmt(cntxt->wlcr, "wlreplay",getFunctionId(pci));
					pci = pushStr(cntxt->wlcr, pci, sch);
					pci = pushStr(cntxt->wlcr, pci, tbl);
					pci = pushStr(cntxt->wlcr, pci, col);
					pci->ticks = GDKms();
				}
				k++;
				pci = pushStr(cntxt->wlcr, pci ,(str) BUNtail(bi,p));
		} }
		break;
	default:
		cntxt->wlcr_kind = WLCR_CATALOG;
	}
	GDKfree(sch);
	GDKfree(tbl);
	GDKfree(col);
}

str
WLCRappend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	 InstrPtr p;
	int tpe, varid;
	(void) stk;
	(void) mb;
	
	WLCR_start();
	p = newStmt(cntxt->wlcr, "wlreplay","append");
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,2)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,3)).val.sval);
	p->ticks = GDKms();

	// extend the instructions with all values. 
	// If this become too large we can always switch to a "catalog" mode
	// forcing re-execution instead
	tpe= getArgType(mb,pci,4);
	if (isaBatType(tpe) ){
		// actually check the size of the BAT first, most have few elements
		WLCRdatashipping(cntxt, mb, p, stk->stk[getArg(pci,4)].val.bval);
	} else {
		ValRecord cst;
		if (VALcopy(&cst, getArgReference(stk,pci,4)) != NULL){
			varid = defConstant(cntxt->wlcr, tpe, &cst);
			p = pushArgument(cntxt->wlcr, p, varid);
		}
	}
	if( cntxt->wlcr_kind < WLCR_UPDATE)
		cntxt->wlcr_kind = WLCR_UPDATE;
	
	return MAL_SUCCEED;
}

str
WLCRdelete(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	InstrPtr p;	
	int tpe, varid;
	(void) stk;
	(void) mb;
	
	WLCR_start();
	p = newStmt(cntxt->wlcr, "wlreplay","delete");
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,2)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,3)).val.sval);
	p->ticks = GDKms();
	
	tpe= getArgType(mb,pci,4);
	if (isaBatType(tpe) ){
		WLCRdatashipping(cntxt, mb, p, stk->stk[getArg(pci,4)].val.bval);
	} else {
		ValRecord cst;
		if (VALcopy(&cst, getArgReference(stk,pci,4)) != NULL){
			varid = defConstant(cntxt->wlcr, tpe, &cst);
			p = pushArgument(cntxt->wlcr, p, varid);
		}
	}
	if( cntxt->wlcr_kind < WLCR_UPDATE)
		cntxt->wlcr_kind = WLCR_UPDATE;

	return MAL_SUCCEED;
}

str
WLCRupdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	 InstrPtr p;
	int tpe, varid;
	(void) stk;
	
	WLCR_start();
	p = newStmt(cntxt->wlcr, "wlreplay","updateOID");
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,2)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,3)).val.sval);
	p->ticks = GDKms();
	
	tpe= getArgType(mb,pci,4);
	if (isaBatType(tpe) ){
		WLCRdatashipping(cntxt, mb, p, stk->stk[getArg(pci,4)].val.bval);
	} else {
		ValRecord cst;
		if (VALcopy(&cst, getArgReference(stk,pci,4)) != NULL){
			varid = defConstant(cntxt->wlcr, tpe, &cst);
			p = pushArgument(cntxt->wlcr, p, varid);
		}
	}

	p = newStmt(cntxt->wlcr, "wlreplay","updateVALUE");
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,2)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,3)).val.sval);
	p->ticks = GDKms();
	
	tpe= getArgType(mb,pci,5);
	if (isaBatType(tpe) ){
		WLCRdatashipping(cntxt, mb, p, stk->stk[getArg(pci,5)].val.bval);
	} else {
		ValRecord cst;
		if (VALcopy(&cst, getArgReference(stk,pci,5)) != NULL){
			varid = defConstant(cntxt->wlcr, tpe, &cst);
			p = pushArgument(cntxt->wlcr, p, varid);
		}
	}
	if( cntxt->wlcr_kind < WLCR_UPDATE)
		cntxt->wlcr_kind = WLCR_UPDATE;

	return MAL_SUCCEED;
}

str
WLCRclear_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	 InstrPtr p;
	(void) stk;
	
	WLCR_start();
	p = newStmt(cntxt->wlcr, "wlr","clear_table");
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,2)).val.sval);
	p->ticks = GDKms();
	if( cntxt->wlcr_kind < WLCR_UPDATE)
		cntxt->wlcr_kind = WLCR_UPDATE;

	return MAL_SUCCEED;
}

static str
WLCRnewlogger(Client cntxt)
{
	(void) cntxt;
	// find next available file
	return GDKstrdup(wlcr_log);
}

static str
WLCRwrite(Client cntxt)
{	str fname;
	// save the wlcr record on a file and ship it to registered slaves
	if ( wlcr_fd == NULL){
		fname= WLCRnewlogger(cntxt);
		wlcr_fd = open_wastream(fname);
	}
	// Limit the size of the log files
	
	if ( wlcr_fd == NULL)
		throw(MAL,"wlcr.write","WLCR log file not accessible");

	if(cntxt->wlcr->stop == 0)
		return MAL_SUCCEED;

	newStmt(cntxt->wlcr,"wlreplay","fin");
	MT_lock_set(&wlcr_lock);
	printFunction(wlcr_fd, cntxt->wlcr, 0, LIST_MAL_DEBUG );
	(void) mnstr_flush(wlcr_fd);
	wlcr_unit++;
	MT_lock_unset(&wlcr_lock);

#ifdef _DEBUG_WLCR_
	printFunction(cntxt->fdout, cntxt->wlcr, 0, LIST_MAL_ALL );
#endif
	return MAL_SUCCEED;
}

str
WLCRcommit(Client cntxt)
{	str msg = MAL_SUCCEED;
	InstrPtr p;
	
	if(cntxt->wlcr && cntxt->wlcr->stop){
		p= getInstrPtr(cntxt->wlcr,0);
		p = pushStr(cntxt->wlcr,p,"commit");
		p = pushStr(cntxt->wlcr, p, wlcr_name[cntxt->wlcr_kind]);
		p = pushInt(cntxt->wlcr,p, GDKms() - p->ticks);
		getInstrPtr(cntxt->wlcr,0) = p;	// plan may be too long to find it automatically
		msg = WLCRwrite(cntxt);
		trimMalVariables(cntxt->wlcr, NULL);
		resetMalBlk(cntxt->wlcr, 0);
		cntxt->wlcr_kind = WLCR_QUERY;
	}
	return msg;
}

str
WLCRcommitCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	return WLCRcommit(cntxt);
}

str
WLCRrollback(Client cntxt)
{	str msg = MAL_SUCCEED;
	InstrPtr p;
	
	if( cntxt->wlcr){
		if (wlcr_all && cntxt->wlcr->stop){
			p= getInstrPtr(cntxt->wlcr,0);
			p = pushStr(cntxt->wlcr,p,"rollback");
			p = pushStr(cntxt->wlcr, p, wlcr_name[cntxt->wlcr_kind]);
			p = pushInt(cntxt->wlcr,p, GDKms() - p->ticks);
			getInstrPtr(cntxt->wlcr,0) = p;	// plan may be too long to find it automatically
			msg = WLCRwrite(cntxt);
		}
		trimMalVariables(cntxt->wlcr, NULL);
		resetMalBlk(cntxt->wlcr, 0);
		cntxt->wlcr_kind = WLCR_QUERY;
	}
	return msg;
}
str
WLCRrollbackCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	return WLCRrollback(cntxt);
}
