/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (c) 2017 Martin Kersten
 * This module collects the workload-capture-replay statements during transaction execution. 
 * It is used primarilly for replication management and workload replay
 *
 * The goal is to maintain a replica of a master database. All data of the master
 * is basically only available for read only access. Accidental corruption of this
 * data is avoided by setting ownership and access properties at the SQL level in the replica.
 *
 *
 * IMPLEMENTATION
 *
 * The replica directory should be on a shared (global) file system.
 * As default we use dbfarm/master.
 *
 * The binary dump for the database snapshot should be stored there in  master/bat.
 * The associated log files are stored as master/wlcr<number>.
 * Creation and restore of a snapshot should be a  monetdb option. TODO
 *
 * Replication management start when you run the command 
 * CALL wlcr.master("(full)path to snapshot dir")
 * It can also be passed as a command line parameter 
 * --set wlcr_dir="(full)path to snapshot dir"
 *
 * Each wlcr log file contains a serial log for a transaction batch. 
 * Each job is identified by the owner of the query, the snapshot tag,
 * commit/rollback, its starting time and runtime (in ms).
 *
 * Logging of queries can be further limited to those that satisfy a threshold.
 * CALL wlcr.master("(full)path to snapshot dir", threshold)
 * The threshold is given in milliseconds. A negative threshold leads to ignoring all queries.
 *
 * A replica server should issue the matching call
 * CALL wlcr.synchronize("(full)path to snapshot dir")
 *
 * During synchronization only updates are executed for the user responsible for the call. 
 * Queries are simply ignored unless needed as replacement for update actions.
 *
 * The alternative is to replay the log
 * CALL wlcr.replay("(full)path to snapshot dir")
 * In this mode all queries are executed under the credentials of the query owner, including those that lead to updates.
 *
 * Any failure encountered terminates the synchronization process, leaving a message in the merovingian log.
 *
 * The replay progress can be inspected using the function wlcr.drift() and wlcr.synced().
 * The latter is true if all accessible log files have been processed.
 * 
 * The wlcr files purposely have a textual format derived from the MAL statements.
 * It creates some overhead for copy into situations.
 *
 * The integrity of the wlcr directories is critical. For now we assume that all batches are available. 
 * We should detect that wlcr.master() is issued after updates have taken place on the snapshot TODO.
 *
 */
#include "monetdb_config.h"
#include <time.h>
#include "mal_builder.h"
#include "wlcr.h"

static MT_Lock     wlcr_lock MT_LOCK_INITIALIZER("wlcr_lock");


int wlcr_threshold = 0; // threshold (milliseconds) for keeping readonly queries
str wlcr_snapshot= 0;	// name assigned to the snapshot
int wlcr_batch = 0;	// last job executed

static char *wlcr_name[]= {"","query","update","catalog"};

static stream *wlcr_fd = 0;
static str wlcr_dir = 0;

/* The database snapshots are binary copies of the dbfarm/database/bat
 * New snapshots are created currently using the 'monetdb snapshot <db>' command
 * or a SQL procedure.
 * It requires a database halt.
 *
 * The wlcr logs are stored in the snapshot directory as a time-stamped list
 */
str 
WLCRmaster(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char path[PATHLENGTH];
	FILE *fd;
	int i = 1;
	(void) stk;
	(void) pci;

	wlcr_dir = GDKgetenv("gdk_wlcrdir");
	if (i< pci->argc+1 && getArgType(mb, pci, i) == TYPE_str){
		wlcr_dir =  *getArgReference_str(stk,pci,i);
		wlcr_dir = GDKfilepath(0,wlcr_dir,"master",0);
		i++;
	}
	// if the master director does not exit, create it
	if ( wlcr_dir == NULL)
		wlcr_dir = GDKfilepath(0,0,"master",0);

	if ( i < pci->argc+1 && getArgType(mb, pci, i) == TYPE_int)
		wlcr_threshold = *getArgReference_int(stk,pci,i);

	snprintf(path, PATHLENGTH,"%s%cwlcr",wlcr_dir, DIR_SEP);
	if( GDKcreatedir(path) == GDK_FAIL)
		mnstr_printf(cntxt->fdout,"#Could not create %s\n",wlcr_dir);
	mnstr_printf(cntxt->fdout,"#Snapshot directory '%s'\n", wlcr_dir);

	fd = fopen(path,"w");
	if ( fd == NULL)
		return createException(MAL,"wlcr.master","Unable to initialize WLCR %s", path);
	if( fscanf(fd,"%d %d", &wlcr_batch, &wlcr_threshold) != 3)
		fprintf(fd,"0 %d\n", wlcr_threshold);
	fclose(fd);
	mnstr_printf(cntxt->fdout,"#master wlcr_batch %d threshold %d\n",wlcr_batch, wlcr_threshold);
	return MAL_SUCCEED;
}

static InstrPtr
WLCRaddtime(Client cntxt, InstrPtr pci, InstrPtr p)
{
	char tbuf[26];
    time_t clk = pci->clock.tv_sec;
	struct tm ctm;

	ctm = *localtime(&clk);
	strftime(tbuf, 26, "%Y-%m-%dT%H:%M:%S",&ctm);
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
		p = pushStr(cntxt->wlcr,p, wlcr_snapshot? wlcr_snapshot:"dummy");\
		p = pushInt(cntxt->wlcr,p, wlcr_batch);\
		p = WLCRaddtime(cntxt,pci, p); \
		p->ticks = GDKms();\
}	}

str
WLCRjob(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str snapshot = *getArgReference_str(stk,pci,1);
	int tid = *getArgReference_int(stk,pci,2);

	(void) cntxt;
	(void) mb;

	if ( strcmp(snapshot, wlcr_snapshot))
		throw(MAL,"wlcr.job","Incompatible snapshot identifier");
	if ( tid < wlcr_batch)
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

// creation of file and updating the version file should be atomic TODO!!!
static str
WLCRloggerfile(Client cntxt)
{
	char path[PATHLENGTH];
	FILE *fd;

	(void) cntxt;
	snprintf(path,PATHLENGTH,"%s%cwlcr",wlcr_dir, DIR_SEP);
	mnstr_printf(cntxt->fdout,"#WLCRloggerfile %s\n",wlcr_dir);
	fd = fopen(path,"w");
	if( fd == NULL)
		return "unknown";
	fprintf(fd,"%d %d\n", wlcr_batch, wlcr_threshold);
	fclose(fd);
	snprintf(path,PATHLENGTH,"%s%cwlcr_%06d",wlcr_dir,DIR_SEP,wlcr_batch);
	wlcr_batch++;
	return GDKstrdup(path);
}

static str
WLCRwrite(Client cntxt)
{	str fname;
	// save the wlcr record on a file and ship it to registered slaves
	if( wlcr_dir ){
		if ( wlcr_fd == NULL){
			fname= WLCRloggerfile(cntxt);
			mnstr_printf(cntxt->fdout,"#WLCRloggerfile batch %s\n",fname);
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
		wlcr_batch++;
		MT_lock_unset(&wlcr_lock);
	}

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
		if (cntxt->wlcr->stop){
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
