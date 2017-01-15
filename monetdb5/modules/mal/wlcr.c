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
 * The goal is to easily clone a master database.  Accidental corruption of this
 * data should ne avoided by setting ownership and access properties at the SQL level in the replica.
 *
 *
 * IMPLEMENTATION
 *
 * A database can be set into 'master' mode only once using the SQL command:
 * CALL master()
 *
 * It creates a directory .../dbfarm/dbname/master to hold all necessary information
 * for the creation and maintenance of replicas.
 *
 * Every replica should start off with a copy of binary snapshot stored 
 * in .../dbfarm/dbname/master/bat or with an empty database.
 * The associated log files are stored as master/wlcr<number>.
 *
 * Each wlcr log file contains a serial log of committed transactions.
 * The log records are represented as ordinary MAL statements, which
 * are executed in serial mode.
 * Each transaction job is identified by the owner of the query, 
 * commit/rollback status, its starting time and runtime (in ms).
 * The end of the transaction is marked as exec()
 *
 * Logging of queries can be limited to those that satisfy an execution threshold.
 * SET replaythreshold= <number>
 * The threshold is given in milliseconds. A negative threshold leads to ignoring all queries.
 * The threshold setting is not saved because it is a client specific action.
 *
 * A replica is constructed in three steps. 
 * 1) a snapshot copy of the BAT directory is created within a SQL transaction.[TODO]
 * 2) a new database is created from this snapshot using monetdb[TODO]
 * 3) The logs are replayed to bring the snapshot up to date.
 *
 * Step 1) and 2) can be avoided by starting with an empty database and 
 * under the assumption that the log files reflect the complete history.
 * The monetdb program will check for this.
 *
 * Processing the log files starts in the background using the call.
 * CALL clone("dbname")
 * It will iterate through the log files, applying all transactions.
 * Queries are simply ignored unless needed as replacement for catalog actions..
 * [TODO] the user might want to take a time-stamped version only, ignoring all actions afterwards.
 *
 * The alternative is to replay the complete query log
 * CALL replay("dbname")
 * In this mode all queries are executed under the credentials of the query owner[TODO], 
 * including those that lead to updates.
 *
 * Any failure encountered during a log replay terminates the process,
 * leaving a message in the merovingian log.
 *
 * [TODO]The progress can be inspected using the boolean function wlcr.synced().
 * The latter is true if all accessible log files have been processed.
 * 
 * The wlcr files purposely have a textual format derived from the MAL statements.
 * Simplicity and ease of control has been the driving argument here.
 *
 * The integrity of the wlcr directories is critical. For now we assume that all batches are available. 
 *
 */
#include "monetdb_config.h"
#include <time.h>
#include "mal_builder.h"
#include "wlcr.h"

static MT_Lock     wlcr_lock MT_LOCK_INITIALIZER("wlcr_lock");


int wlcr_start = 0;	// first batch associated with snapshot
int wlcr_batch = 0;	// last batch job identifier 
int wlcr_tid = 0;	// last transaction id

static char *wlcr_name[]= {"","query","update","catalog"};

static str wlcr_dbname = 0; 
static stream *wlcr_fd = 0;
static str wlcr_dir = 0;

/* The database snapshots are binary copies of the dbfarm/database/bat
 * New snapshots are created currently using the 'monetdb snapshot <db>' command
 * or a SQL procedure.
 *
 * The wlcr logs are stored in the snapshot directory as a time-stamped list
 */

int
WLCRused(void)
{
	return wlcr_dir != NULL;
}
// creation of file and updating the version file should be atomic TODO!!!
// The log files are marked with the database name. This allows for easy recognition later on.
static str
WLCRloggerfile(Client cntxt)
{
	char path[PATHLENGTH];
	FILE *fd;

	(void) cntxt;
	snprintf(path,PATHLENGTH,"%s%c%s_%06d", wlcr_dir, DIR_SEP, wlcr_dbname, wlcr_batch);
	wlcr_fd = open_wastream(path);
	if( wlcr_fd == 0)
		throw(MAL,"wlcr.logger","Could not create %s\n",path);

	wlcr_batch++;
	wlcr_tid = 0;
	snprintf(path,PATHLENGTH,"%s%c%s_wlcr", wlcr_dir, DIR_SEP, wlcr_dbname);
#ifdef _WLCR_DEBUG_
	mnstr_printf(cntxt->fdout,"#WLCRloggerfile %s\n", wlcr_dir);
#endif
	fd = fopen(path,"w");
	if( fd == NULL)
		throw(MAL,"wlcr.logger","Could not access %s\n",path);
	fprintf(fd,"%d %d\n", wlcr_start, wlcr_batch );
	fclose(fd);
	return MAL_SUCCEED;
}

/*
 * The existence of the master directory should be checked upon server restart.
 * A new batch file should be created as a result.
 * We also have to keep track on the files that have been read by the clone from the parent.
 * Upon exit we should check the log file size. If empty we need not safe it. [TODO]
 */
str 
WLCRinit(Client cntxt)
{
	str dbname= (str) GDKgetenv("gdk_dbname");
	str dir;
	str msg = MAL_SUCCEED;
	FILE *fd;
	char path[PATHLENGTH];
	
	if( wlcr_dir){
#ifdef _WLCR_DEBUG_
		mnstr_printf(cntxt->fdout,"#WLCR already running\n");
#endif
		return MAL_SUCCEED;
	}

	if (dbname){
		wlcr_dbname = GDKstrdup(dbname);
		dir = GDKfilepath(0,0,"master",0);
		snprintf(path, PATHLENGTH,"%s%c%s_wlcr",dir, DIR_SEP, wlcr_dbname);
		fd = fopen(path,"r");
		if( fd){
			// database is in master tracking mode
			if( fscanf(fd,"%d %d", &wlcr_start, &wlcr_batch ) == 2){
				if( wlcr_batch < 0){
					// logging was stopped
					(void) fclose(fd);
					return MAL_SUCCEED;
				}
				wlcr_dir = dir;
#ifdef _WLCR_DEBUG_
				mnstr_printf(cntxt->fdout,"#Master control active:%d\n", wlcr_batch);
#endif
				(void) fclose(fd);
				msg = WLCRloggerfile(cntxt);
			} else{
#ifdef _WLCR_DEBUG_
				mnstr_printf(cntxt->fdout,"#Inconsistent master control:%d\n", wlcr_batch);
#endif
				(void) fclose(fd);
			}
		} 
#ifdef _WLCR_DEBUG_
		else
				mnstr_printf(cntxt->fdout,"#Master control not active\n");
#endif
	}
	return msg;
}

str 
WLCRexit(void)
{
	size_t sz;
	FILE *fd;
	char path[PATHLENGTH];

 	// only keep non-empty log files  [TODO]
	if( wlcr_fd){
		sz = getFileSize(wlcr_fd);
		if (sz == 0){
			snprintf(path, PATHLENGTH,"%s%c%s_wlcr",wlcr_dir, DIR_SEP,wlcr_dbname);
			fd = fopen(path,"w");
			if( fd == NULL)
				throw(MAL,"wlcr.exit","File can not be access");
			fprintf(fd,"%d %d\n", wlcr_start, wlcr_batch -1);
		}
	}
	return MAL_SUCCEED;
}

str
WLCRstop (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	FILE *fd;
	str	dir = GDKfilepath(0,0,"master",0);
	char path[PATHLENGTH];
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;

	if( wlcr_dir == NULL)
		throw(MAL,"wlcr.stop","Replica control not active");

	snprintf(path, PATHLENGTH,"%s%c%s_wlcr",dir, DIR_SEP,wlcr_dbname);
	fd = fopen(path,"w");
	if( fd == NULL)
		throw(MAL,"wlcr.stop","File can not be access");
	fprintf(fd,"%d %d\n", wlcr_start, -wlcr_batch );
	(void) fflush(fd);
	return MAL_SUCCEED;
}

str 
WLCRinitCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	return WLCRinit(cntxt);
}

str 
WLCRthreshold(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	cntxt->wlcr_threshold = * getArgReference_int(stk,pci,1);
	return MAL_SUCCEED;
}

str 
WLCRmaster(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char path[PATHLENGTH];
	FILE *fd;
	str msg = MAL_SUCCEED;
	(void) stk;
	(void) pci;

	(void) mb;

	WLCRinit(cntxt);
	// if the master directory does not exit, create it
	if ( wlcr_dir == NULL){
		wlcr_dir = GDKfilepath(0,0,"master",0);
		snprintf(path, PATHLENGTH,"%s%c%s_wlcr", wlcr_dir, DIR_SEP, wlcr_dbname);
		if( GDKcreatedir(path) == GDK_FAIL)
			throw(SQL,"wlcr.master","Could not create %s\n", wlcr_dir);
#ifdef _WLCR_DEBUG_
		mnstr_printf(cntxt->fdout,"#Snapshot directory '%s'\n", wlcr_dir);
#endif

		fd = fopen(path,"w");
		if ( fd == NULL)
			return createException(MAL,"wlcr.master","Unable to initialize WLCR %s", path);
		if( fscanf(fd,"%d %d", &wlcr_start, &wlcr_batch) != 2)
			fprintf(fd,"0 0\n");
		(void) fclose(fd);
	}
	if( wlcr_fd == NULL)
		msg = WLCRloggerfile(cntxt);
#ifdef _WLCR_DEBUG_
	mnstr_printf(cntxt->fdout,"#master batches %d file open %d\n",wlcr_batch,  wlcr_fd != NULL);
#endif
	return msg;
}

static InstrPtr
WLCRaddtime(Client cntxt, InstrPtr pci, InstrPtr p)
{
	char tbuf[26];
	struct timeval clock;
	time_t clk ;
	struct tm ctm;

	(void) pci;
	gettimeofday(&clock,NULL);
	clk = clock.tv_sec;
	ctm = *localtime(&clk);
	strftime(tbuf, 26, "%Y-%m-%dT%H:%M:%S",&ctm);
	return pushStr(cntxt->wlcr, p, tbuf);
}

#define WLCR_start(P)\
{ Symbol s; \
	if( cntxt->wlcr == NULL){\
		s = newSymbol("wlrc", FUNCTIONsymbol);\
		cntxt->wlcr_kind = WLCR_QUERY;\
		cntxt->wlcr = s->def;\
		s->def = NULL;\
	} \
	if( cntxt->wlcr->stop == 0){\
		P = newStmt(cntxt->wlcr,"clone","job");\
		P = pushStr(cntxt->wlcr, P, cntxt->username);\
		P = pushInt(cntxt->wlcr, P, wlcr_tid);\
		P = WLCRaddtime(cntxt,pci, P); \
		P->ticks = GDKms();\
}	}

str
WLCRjob(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tid = *getArgReference_int(stk,pci,1);

	(void) cntxt;
	(void) mb;

	if ( tid < wlcr_tid)
		throw(MAL,"wlcr.job","Work unit identifier is before last one executed");
	return MAL_SUCCEED;
}

str
WLCRexec(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
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
		return MAL_SUCCEED;	// ignore system internal queries.
	WLCR_start(p);
	p = newStmt(cntxt->wlcr, "clone","query");
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
	
	WLCR_start(p);
	p = newStmt(cntxt->wlcr, "clone",getFunctionId(pci));
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
			pci = newStmt(cntxt->wlcr, "clone",getFunctionId(pci));\
			pci = pushStr(cntxt->wlcr, pci, sch);\
			pci = pushStr(cntxt->wlcr, pci, tbl);\
			pci = pushStr(cntxt->wlcr, pci, col);\
			pci->ticks = GDKms();\
		}\
		pci = push##TPE2(cntxt->wlcr, pci ,*p);\
} }

#define updateBatch(TPE1,TPE2)\
{	TPE1 *x = (TPE1 *) Tloc(bval,0);\
	TPE1 *y = (TPE1 *) Tloc(bval, BUNlast(b));\
	int k=0; \
	for( ; x < y; x++, k++){\
		p = newStmt(cntxt->wlcr, "clone","update");\
		p = pushStr(cntxt->wlcr, p, sch);\
		p = pushStr(cntxt->wlcr, p, tbl);\
		p = pushStr(cntxt->wlcr, p, col);\
		p = pushOid(cntxt->wlcr, p, (ol? *ol++: o++));\
		p = push##TPE2(cntxt->wlcr, p ,*x);\
} }

static void
WLCRdatashipping(Client cntxt, MalBlkPtr mb, InstrPtr pci, int bid)
{	BAT *b;
	str sch,tbl,col;
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
					pci = newStmt(cntxt->wlcr, "clone",getFunctionId(pci));
					pci = pushStr(cntxt->wlcr, pci, sch);
					pci = pushStr(cntxt->wlcr, pci, tbl);
					pci = pushStr(cntxt->wlcr, pci, col);
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
	
	WLCR_start(p);
	p = newStmt(cntxt->wlcr, "clone","append");
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,2)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,3)).val.sval);

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
	
	WLCR_start(p);
	p = newStmt(cntxt->wlcr, "clone","delete");
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,2)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,3)).val.sval);
	
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
{	InstrPtr p;
	str sch,tbl,col;
	ValRecord cst;
	int tpe, varid;
	oid o = 0, *ol;
	
	sch = *getArgReference_str(stk,pci,1);
	tbl = *getArgReference_str(stk,pci,2);
	col = *getArgReference_str(stk,pci,3);
	WLCR_start(p);
	tpe= getArgType(mb,pci,5);
	if (isaBatType(tpe) ){
		BAT *b, *bval;
		b= BATdescriptor(stk->stk[getArg(pci,4)].val.bval);
		assert(b);
		bval= BATdescriptor(stk->stk[getArg(pci,5)].val.bval);
		assert(bval);
		o = b->hseqbase;
		ol = (oid*) Tloc(b,0);
		switch( ATOMstorage(bval->ttype)){
		case TYPE_bit: updateBatch(bit,Bit); break;
		case TYPE_bte: updateBatch(bte,Bte); break;
		case TYPE_sht: updateBatch(sht,Sht); break;
		case TYPE_int: updateBatch(int,Int); break;
		case TYPE_lng: updateBatch(lng,Lng); break;
		case TYPE_flt: updateBatch(flt,Flt); break;
		case TYPE_dbl: updateBatch(dbl,Dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: updateBatch(hge,Hge); break;
#endif
		case TYPE_str:
		{	BATiter bi;
			int k=0; 
			BUN x,y;
			bi = bat_iterator(bval);
			BATloop(bval,x,y){
				p = newStmt(cntxt->wlcr, "clone","update");
				p = pushStr(cntxt->wlcr, p, sch);
				p = pushStr(cntxt->wlcr, p, tbl);
				p = pushStr(cntxt->wlcr, p, col);
				p = pushOid(cntxt->wlcr, p, (ol? *ol++ : o++));
				p = pushStr(cntxt->wlcr, p , BUNtail(bi,x));
				k++;
		} }
		default:
			cntxt->wlcr_kind = WLCR_CATALOG;
		}
	} else {
		p = newStmt(cntxt->wlcr, "clone","update");
		p = pushStr(cntxt->wlcr, p, sch);
		p = pushStr(cntxt->wlcr, p, tbl);
		p = pushStr(cntxt->wlcr, p, col);
		o = *getArgReference_oid(stk,pci,4);
		p = pushOid(cntxt->wlcr,p, o);
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
	
	WLCR_start(p);
	p = newStmt(cntxt->wlcr, "clone","clear_table");
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,1)).val.sval);
	p = pushStr(cntxt->wlcr, p, getVarConstant(mb, getArg(pci,2)).val.sval);
	if( cntxt->wlcr_kind < WLCR_UPDATE)
		cntxt->wlcr_kind = WLCR_UPDATE;

	return MAL_SUCCEED;
}

static str
WLCRwrite(Client cntxt, str kind)
{	str msg;
	InstrPtr p;
	// save the wlcr record on a file 
	if( cntxt->wlcr == 0 || cntxt->wlcr->stop == 0)
		return MAL_SUCCEED;
	if( wlcr_dir ){
		if ( wlcr_fd == NULL){
			msg = WLCRloggerfile(cntxt);
			if( msg) 
				return msg;
		}
		// Limit the size of the log files
		
		if ( wlcr_fd == NULL)
			throw(MAL,"wlcr.write","WLCR log file not accessible");

		if(cntxt->wlcr->stop == 0)
			return MAL_SUCCEED;

		newStmt(cntxt->wlcr,"clone","exec");
		wlcr_tid++;
		MT_lock_set(&wlcr_lock);
		p = getInstrPtr(cntxt->wlcr,0);
		p = pushStr(cntxt->wlcr,p,kind);
		p = pushStr(cntxt->wlcr, p, wlcr_name[cntxt->wlcr_kind]);
		p = pushLng(cntxt->wlcr,p, GDKms() - p->ticks);
		printFunction(wlcr_fd, cntxt->wlcr, 0, LIST_MAL_DEBUG );
		(void) mnstr_flush(wlcr_fd);
		MT_lock_unset(&wlcr_lock);
		trimMalVariables(cntxt->wlcr, NULL);
		resetMalBlk(cntxt->wlcr, 0);
		cntxt->wlcr_kind = WLCR_QUERY;
	}

#ifdef _DEBUG_WLCR_
	printFunction(cntxt->fdout, cntxt->wlcr, 0, LIST_MAL_ALL );
#endif
	return MAL_SUCCEED;
}

str
WLCRcommit(int clientid)
{
		return WLCRwrite( &mal_clients[clientid], "commit");
}

str
WLCRcommitCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	return WLCRcommit(cntxt->idx);
}

str
WLCRrollback(int clientid)
{
	return WLCRwrite( &mal_clients[clientid], "rollback");
}
str
WLCRrollbackCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	return WLCRrollback(cntxt->idx);
}
