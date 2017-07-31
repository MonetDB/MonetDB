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
 * Copyright August 2008-2016 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * Martin Kersten
 * Petri-net continuous query scheduler
   The CQ scheduler is based on the long-standing and mature Petri-net technology. For completeness, we
   recap its salient points taken from Wikipedia. For more detailed information look at the science library.

   The cquery scheduler is a fair implementation of a Petri-net interpreter. It models all continuous queries as transitions,
   and the stream tables represent the places with all tokens. The firing condition is determined by an external clock
   or availability of sufficient number of tokens (set by window()). Unlike the pure Petri-net model, the number of tokens 
   taken out on each firing is set by the tumble().  
   The firing rule is an ordinary SQL procedure. It may result into placing multiple tokens into receiving baskets.

   The scheduling amongst the transistions is currently deterministic. Upon each round of the scheduler, it determines all
   transitions eligble to fire, i.e. have non-empty baskets or whose heartbeat ticks, which are then actived one after the other.
   Future implementations may relax this rigid scheme using a parallel implementation of the scheduler, such that each 
   transition by itself can decide to fire. However, when resources are limited to handle all complex continuous queries, 
   it may pay of to invest into a domain specific scheduler.

   The current implementation is limited to a fixed number of transitions. The scheduler can be stopped and restarted
   at any time. Even selectively for specific baskets. This provides the handle to debug a system before being deployed.
   In general, event processing through multiple layers of continous queries is too fast to trace them one by one.
   Some general statistics about number of events handled per transition is maintained, as well as the processing time
   for each continous query step. This provides the information to re-design the event handling system.
 */

#include "monetdb_config.h"
#include "sql_optimizer.h"
#include "sql_gencode.h"
#include "sql_cquery.h"
#include "sql_basket.h"
#include "mal_builder.h"
#include "opt_prelude.h"

static str statusname[7] = { "init", "register", "readytorun", "running", "waiting", "paused", "stopping"};

static str CQstartScheduler(void);
static int CQinit;
static int pnstatus;

static BAT *CQ_id_tick = 0;
static BAT *CQ_id_mod = 0;
static BAT *CQ_id_fcn = 0;
static BAT *CQ_id_time = 0;
static BAT *CQ_id_error = 0;
static BAT *CQ_id_stmt = 0;

CQnode pnet[MAXCQ];
int pnettop = 0;

static int pnstatus = CQINIT;
static int cycleDelay = 200; /* be careful, it affects response/throughput timings */
MT_Lock ttrLock MT_LOCK_INITIALIZER("cqueryLock");

#define SET_HEARTBEATS(X)  X * 1000 /* minimal 1 ms */

#define ALL_ROOT_CHECK(cntxt, malcal, name)                                                           \
	do {                                                                                              \
		smvc = ((backend *) cntxt->sqlcontext)->mvc;                                                  \
		if(!smvc)                                                                                     \
			throw(SQL,malcal,"##name##ALL CONTINUOUS: SQL clients only");                             \
		 else if (smvc->user_id != USER_MONETDB && smvc->role_id != ROLE_SYSADMIN)                    \
			throw(SQL,malcal,"##name##ALL CONTINUOUS: insufficient privileges for the current user"); \
	} while(0);

static void
CQfree(int idx)
{
	int i;
	if( pnet[idx].mb)
		freeMalBlk(pnet[idx].mb);
	if( pnet[idx].stk)
		GDKfree(pnet[idx].stk);
	GDKfree(pnet[idx].mod);
	GDKfree(pnet[idx].fcn);
	GDKfree(pnet[idx].stmt);
	// compact the pnet table
	for(i=idx; i<pnettop-1; i++)
		pnet[i] = pnet[i+1];
	pnettop--;
	memset((void*) (pnet+pnettop), 0, sizeof(CQnode));
}

/* We need a lock table for all stream tables considered
 * It is better to use a slot in the BATdescriptor
 * A sanity routine should be available to check for any forgotten lock frees.
 */

/*
static str
CQallLocksReleased(void)
{
	return MAL_SUCCEED;
}
*/

/* Initialization phase of scheduler */
static str
CQcleanuplog(void)
{
#ifdef DEBUG_CQUERY
	fprintf(stderr,"#cleaup query.log table\n");
#endif
	return MAL_SUCCEED;
}

static str
CQcreatelog(void){
	if( CQ_id_tick)
		return MAL_SUCCEED;
#ifdef DEBUG_CQUERY
	fprintf(stderr,"#create query.log table\n");
#endif
	CQ_id_tick = COLnew(0, TYPE_timestamp, 1<<16, TRANSIENT);
	CQ_id_mod = COLnew(0, TYPE_str, 1<<16, TRANSIENT);
	CQ_id_fcn = COLnew(0, TYPE_str, 1<<16, TRANSIENT);
	CQ_id_time = COLnew(0, TYPE_lng, 1<<16, TRANSIENT);
	CQ_id_error = COLnew(0, TYPE_str, 1<<16, TRANSIENT);
	CQ_id_stmt = COLnew(0, TYPE_str, 1<<16, TRANSIENT);
	if ( CQ_id_tick == 0 &&
		CQ_id_mod == 0 &&
		CQ_id_fcn == 0 &&
		CQ_id_time == 0 &&
		CQ_id_stmt == 0 &&
		CQ_id_error == 0){
			(void) CQcleanuplog();
			throw(MAL,"cquery.log",MAL_MALLOC_FAIL);
		}
	return MAL_SUCCEED;
}

static void
CQentry(int idx)
{
	CQcreatelog();
	if( BUNappend(CQ_id_tick, &pnet[idx].seen,FALSE) != GDK_SUCCEED ||
		BUNappend(CQ_id_mod, pnet[idx].mod,FALSE) != GDK_SUCCEED ||
		BUNappend(CQ_id_fcn, pnet[idx].fcn,FALSE) != GDK_SUCCEED ||
		BUNappend(CQ_id_time, &pnet[idx].time,FALSE) != GDK_SUCCEED ||
		BUNappend(CQ_id_error, (pnet[idx].error ? pnet[idx].error:""),FALSE) != GDK_SUCCEED ||
		BUNappend(CQ_id_stmt, (pnet[idx].stmt ? pnet[idx].stmt:""),FALSE) != GDK_SUCCEED )
		pnet[idx].error = createException(SQL,"cquery.logentry",MAL_MALLOC_FAIL);
}

str
CQlog( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	BAT *tickbat = 0, *modbat = 0, *fcnbat = 0, *timebat = 0, *errbat = 0;
	bat *tickret, *modret, *fcnret, *timeret, *errorret;

	(void) cntxt;
	(void) mb;

	tickret = getArgReference_bat(stk, pci, 0);
	modret = getArgReference_bat(stk, pci, 1);
	fcnret = getArgReference_bat(stk, pci, 2);
	timeret = getArgReference_bat(stk, pci, 3);
	errorret = getArgReference_bat(stk, pci, 4);
#ifdef DEBUG_CQUERY
	fprintf(stderr,"#produce query.log table\n");
#endif
	CQcreatelog();
	tickbat = COLcopy(CQ_id_tick, TYPE_timestamp, 0, TRANSIENT);
	if(tickbat == NULL)
		goto wrapup;
	modbat = COLcopy(CQ_id_mod, TYPE_str, 0, TRANSIENT);
	if(modbat == NULL)
		goto wrapup;
	fcnbat = COLcopy(CQ_id_fcn, TYPE_str, 0, TRANSIENT);
	if(fcnbat == NULL)
		goto wrapup;
	timebat = COLcopy(CQ_id_time, TYPE_lng, 0, TRANSIENT);
	if(timebat == NULL)
		goto wrapup;
	errbat = COLcopy(CQ_id_error, TYPE_str, 0, TRANSIENT);
	if(errbat == NULL)
		goto wrapup;
	BBPkeepref(*tickret = tickbat->batCacheid);
	BBPkeepref(*modret = modbat->batCacheid);
	BBPkeepref(*fcnret = fcnbat->batCacheid);
	BBPkeepref(*timeret = timebat->batCacheid);
	BBPkeepref(*errorret = errbat->batCacheid);
	return MAL_SUCCEED;
wrapup:
	if( tickbat) BBPunfix(tickbat->batCacheid);
	if( modbat) BBPunfix(modbat->batCacheid);
	if( fcnbat) BBPunfix(fcnbat->batCacheid);
	if( timebat) BBPunfix(timebat->batCacheid);
	if( errbat) BBPunfix(errbat->batCacheid);
	return MAL_SUCCEED;
}

str
CQstatus( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	BAT *tickbat = 0, *modbat = 0, *fcnbat = 0, *statusbat = 0, *errbat = 0, *stmtbat =0;
	bat *tickret = 0, *modret = 0, *fcnret = 0, *statusret = 0, *errorret = 0, *stmtret = 0;
	int idx;
	str msg= MAL_SUCCEED;

	(void) cntxt;
	(void) mb;

	tickret = getArgReference_bat(stk, pci, 0);
	modret = getArgReference_bat(stk, pci, 1);
	fcnret = getArgReference_bat(stk, pci, 2);
	statusret = getArgReference_bat(stk, pci, 3);
	errorret = getArgReference_bat(stk, pci, 4);
	stmtret = getArgReference_bat(stk, pci, 5);

	tickbat = COLnew(0, TYPE_timestamp, 0, TRANSIENT);
	if(tickbat == NULL)
		goto wrapup;
	modbat = COLnew(0, TYPE_str, 0, TRANSIENT);
	if(modbat == NULL)
		goto wrapup;
	fcnbat = COLnew(0, TYPE_str, 0, TRANSIENT);
	if(fcnbat == NULL)
		goto wrapup;
	statusbat = COLnew(0, TYPE_str, 0, TRANSIENT);
	if(statusbat == NULL)
		goto wrapup;
	stmtbat = COLnew(0, TYPE_str, 0, TRANSIENT);
	if(stmtbat == NULL)
		goto wrapup;
	errbat = COLnew(0, TYPE_str, 0, TRANSIENT);
	if(errbat == NULL)
		goto wrapup;

	for( idx = 0; msg == MAL_SUCCEED && idx < pnettop; idx++)
		if( BUNappend(tickbat, &pnet[idx].seen,FALSE) != GDK_SUCCEED ||
			BUNappend(modbat, pnet[idx].mod,FALSE) != GDK_SUCCEED ||
			BUNappend(fcnbat, pnet[idx].fcn,FALSE) != GDK_SUCCEED ||
			BUNappend(statusbat, statusname[pnet[idx].status],FALSE) != GDK_SUCCEED ||
			BUNappend(stmtbat, (pnet[idx].stmt ? pnet[idx].stmt:""),FALSE) != GDK_SUCCEED ||
			BUNappend(errbat, (pnet[idx].error ? pnet[idx].error:""),FALSE) != GDK_SUCCEED )
				msg = createException(SQL,"cquery.status",MAL_MALLOC_FAIL);

	BBPkeepref(*tickret = tickbat->batCacheid);
	BBPkeepref(*modret = modbat->batCacheid);
	BBPkeepref(*fcnret = fcnbat->batCacheid);
	BBPkeepref(*statusret = statusbat->batCacheid);
	BBPkeepref(*errorret = errbat->batCacheid);
	BBPkeepref(*stmtret = stmtbat->batCacheid);
	return msg;
wrapup:
	if( tickbat) BBPunfix(tickbat->batCacheid);
	if( modbat) BBPunfix(modbat->batCacheid);
	if( fcnbat) BBPunfix(fcnbat->batCacheid);
	if( statusbat) BBPunfix(statusbat->batCacheid);
	if( errbat) BBPunfix(errbat->batCacheid);
	if( stmtbat) BBPunfix(stmtbat->batCacheid);
	throw(SQL,"cquery.status",MAL_MALLOC_FAIL);
}

static int
CQlocate(str modname, str fcnname)
{
	int i;
	(void) modname;

	for (i = 0; i < pnettop; i++){
		// Actually we should maintain the schema name as well.
		// f (strcmp(pnet[i].mod, modname) == 0 && strcmp(pnet[i].fcn, fcnname) == 0)
		if (strcmp(pnet[i].fcn, fcnname) == 0)
			return i;
	}
	return i;
}

static str
CQlocateMb(MalBlkPtr mb, int* idx, str* res, const char* call)
{
	int i;
	InstrPtr sig = getInstrPtr(mb,0);
	str mb2str;

	for(i = 1; i< mb->stop; i++){
		sig= getInstrPtr(mb,i);
		if( getModuleId(sig) == userRef)
			break;
	}
	assert(i != mb->stop);
	if((mb2str = instruction2str(mb, NULL, sig, LIST_MAL_CALL)) == NULL) {
		throw(SQL,call,MAL_MALLOC_FAIL);
	}

	for (i = 0; i < pnettop; i++){
		if (strcmp(pnet[i].stmt, mb2str) == 0) {
			*idx = i;
			*res = mb2str;
			return MAL_SUCCEED;
		}
	}
	*idx = i;
	*res = mb2str;
	return MAL_SUCCEED;
}

/* capture and remember errors: WARNING no locks in this call yet! */
str
CQerror(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch = *getArgReference_str(stk,pci,1);
	str fcn = *getArgReference_str(stk,pci,2);
	str error = *getArgReference_str(stk,pci,3);
	int idx;

	(void) cntxt;
	(void) mb;

	idx = CQlocate(sch, fcn);
	if( idx == pnettop)
		throw(SQL,"cquery.error","The continuous procedure %s.%s is not accessible\n",sch,fcn);

	pnet[idx].error = GDKstrdup(error);
	return MAL_SUCCEED;
}

/* A debugging routine: WARNING no locks in this call yet! */
str
CQshow(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch = *getArgReference_str(stk,pci,1);
	str fcn = *getArgReference_str(stk,pci,2);
	int idx;

	(void) cntxt;
	(void) mb;

	idx = CQlocate(sch, fcn);
	if( idx == pnettop)
		throw(SQL,"cquery.show","The continuous procedure %s.%s is not accessible\n",sch,fcn);

	printFunction(cntxt->fdout, pnet[idx].mb, 0, LIST_MAL_NAME | LIST_MAL_VALUE  | LIST_MAL_MAPI);
	return MAL_SUCCEED;
}

/* Collect all input/output basket roles */
/* Make sure we do not re-use the same source more than once */
/* Avoid any concurreny conflict */
static str
CQanalysis(Client cntxt, MalBlkPtr mb, int idx)
{
	int i, j, bskt;
	InstrPtr p;
	str msg= MAL_SUCCEED, sch, tbl;
	(void) cntxt;

	p = getInstrPtr(mb, 0);
	for (i = 0; msg== MAL_SUCCEED && i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (getModuleId(p) == basketRef && (getFunctionId(p) == registerRef || getFunctionId(p) == bindRef)){
			sch = getVarConstant(mb, getArg(p,2)).val.sval;
			tbl = getVarConstant(mb, getArg(p,3)).val.sval;

			// find the stream basket definition
			bskt = BSKTlocate(sch,tbl);
			if( bskt == 0){
				msg = BSKTregisterInternal(cntxt,mb,sch,tbl);
				if( msg == MAL_SUCCEED){
					bskt = BSKTlocate(sch,tbl);
					if( bskt == 0){
						msg = createException(MAL,"cquery.analysis","basket registration missing\n");
						continue;
					}
				}
			}

			// we only need a single column for window size testing
			for( j=0; j< MAXSTREAMS && pnet[idx].baskets[j]; j++)
			if( strcmp(sch, baskets[pnet[idx].baskets[j]].table->s->base.name) == 0 &&
				strcmp(tbl, baskets[pnet[idx].baskets[j]].table->base.name) == 0 )
				break;
			if ( j == MAXSTREAMS){
				msg = createException(MAL,"cquery.analysis","too many stream table columns\n");
				continue;
			}

			if ( pnet[idx].baskets[j] )
				continue;

			pnet[idx].baskets[j] = bskt;
			pnet[idx].inout[j] = STREAM_IN;
		}

		// Pick up the window constraint from the query definition
		if (getModuleId(p) == cqueryRef && getFunctionId(p) == windowRef){
			/*int window = getVarConstant(mb, getArg(p,3)).val.ival;
			int stride;*/
			sch = getVarConstant(mb, getArg(p,1)).val.sval;
			tbl = getVarConstant(mb, getArg(p,2)).val.sval;

			// find the stream basket definition
			bskt = BSKTlocate(sch,tbl);
			if( bskt == 0){
				msg = BSKTregisterInternal(cntxt,mb,sch,tbl);
				if( msg == MAL_SUCCEED){
					bskt = BSKTlocate(sch,tbl);
					if( bskt == 0){
						msg = createException(MAL,"cquery.analysis","basket registration missing\n");
						continue;
					}
				}
			}
			/*if( p->argc == 5)
				stride = getVarConstant(mb, getArg(p,4)).val.ival;
			else stride = window;
			baskets[bskt].window = window;
			baskets[bskt].stride = stride;*/
		}
	}
	return msg;
}

/* locate the MAL representation of this operation and extract the flow */
/* If the operation is not available yet, it should be compiled from its
   definition retained in the SQL catalog */
static str
CQprocedureStmt(Client cntxt, MalBlkPtr mb, str schema, str nme)
{
	mvc *m = NULL;
	str msg = MAL_SUCCEED;
	sql_schema  *s;
	backend *be;
	node *o;
	sql_func *f;
	/*sql_trans *tr;*/

	msg = getSQLContext(cntxt, mb, &m, NULL);
	if ((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;
	s = mvc_bind_schema(m, schema);
	if (s == NULL)
		return createException(SQL,"cquery.register","Schema missing");
	/*tr = m->session->tr;*/
	for (o = s->funcs.set->h; o; o = o->next) {
		f = o->data;
		if (strcmp(f->base.name, nme) == 0) {
			be = (void *) backend_create(m, cntxt);
			if ( be->mvc->sa == NULL)
				be->mvc->sa = sa_create();
			//TODO fix result type
			backend_create_func(be, f, f->res,NULL);
			return MAL_SUCCEED;
		}
	}
	return createException(SQL,"cquery.register","SQL procedure missing");
}

static str
CQregisterInternal(Client cntxt, str modnme, str fcnnme)
{
	InstrPtr sig,q;
	str msg = MAL_SUCCEED;
	MalBlkPtr mb, nmb;
	Module scope;
	Symbol s = NULL;
	char buf[IDLENGTH];

	scope = findModule(cntxt->nspace, putName(modnme));
	if (scope)
		s = findSymbolInModule(scope, putName(fcnnme));

	if (s == NULL)
		return createException(SQL,"cquery.register","Could not find SQL procedure");

	mb = s->def;
	sig = getInstrPtr(mb,0);

	if (pnettop == MAXCQ)
		return createException(MAL,"cquery.register","Too many transitions");

#ifdef DEBUG_CQUERY
	fprintf(stderr, "#cquery register %s.%s\n", getModuleId(sig),getFunctionId(sig));
	fprintFunction(stderr,mb,0,LIST_MAL_ALL);
#endif
	memset((void*) (pnet+pnettop), 0, sizeof(CQnode));

	snprintf(buf,IDLENGTH,"wrap_%s_%s",modnme,fcnnme);
	s = newFunction(userRef, putName(buf), FUNCTIONsymbol);
	nmb = s->def;
	setArgType(nmb, nmb->stmt[0],0, TYPE_void);
	(void) newStmt(nmb, sqlRef, transactionRef);
	(void) newStmt(nmb, getModuleId(sig),getFunctionId(sig));
	// add also the remaining arguments
	q = newStmt(nmb, sqlRef, commitRef);
	setArgType(nmb,q, 0, TYPE_void);
	pushEndInstruction(nmb);
	chkProgram(cntxt->fdout, cntxt->nspace, nmb);
#ifdef DEBUG_CQUERY
	fprintFunction(stderr, nmb, 0, LIST_MAL_ALL);
#endif
	// and hand it over to the scheduler
	msg = CQregister(cntxt, nmb,0,0);
	if( msg != MAL_SUCCEED){
		freeSymbol(s);
	}
	return msg;
}

str
CQprocedure(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch= NULL;
	str nme= NULL;

	Symbol s = NULL;
	MalBlkPtr qry;
	str msg = NULL;
	InstrPtr p;
	//Module scope;
	int i;
	char name[IDLENGTH];

	/* check existing of the pre-compiled and activated function */
	sch = *getArgReference_str(stk, pci, 1);
	nme = *getArgReference_str(stk, pci, 2);
#ifdef DEBUG_CQUERY
	fprintf(stderr,"#cq: register the continues procedure %s.%s()\n",sch,nme);
#endif

	/* check existing of the pre-compiled function */
#ifdef DEBUG_CQUERY
	fprintf(stderr,"#cq: locate a SQL procedure %s.%s()\n",sch,nme);
#endif
	msg = CQprocedureStmt(cntxt, mb, sch, nme);
	if (msg)
		return msg;
	s = findSymbolInModule(cntxt->nspace, putName(nme));
	if (s == NULL)
		throw(SQL, "cquery.procedure", "Definition of procedure missing");
	qry = s->def;

	chkProgram(cntxt->fdout,cntxt->nspace,qry);
	if( qry->errors)
		msg = createException(SQL,"cquery.procedure","Error in continuous procedure");

#ifdef DEBUG_CQUERY
	fprintf(stderr,"#cq: register a new continuous query plan\n");
#endif
	// create a copy to be used for continuous processing?
	snprintf(name,IDLENGTH,"%s_%s",sch,nme);
	s = newFunction(userRef, putName(name), FUNCTIONsymbol);
	if (s == NULL)
		msg = createException(SQL, "cquery.procedure", "Procedure code does not exist.");

	freeMalBlk(s->def);
	s->def = copyMalBlk(qry);
	p = getInstrPtr(s->def, 0);
	setModuleId(p,userRef);
	setFunctionId(p, putName(name));
	insertSymbol(cntxt->nspace, s);
#ifdef DEBUG_CQUERY
	printFunction(cntxt->fdout, s->def, 0, LIST_MAL_ALL);
#endif
	/* optimize the code and register at scheduler */
	if (msg == MAL_SUCCEED)
		addtoMalBlkHistory(mb);
	if (msg == MAL_SUCCEED) {
#ifdef DEBUG_CQUERY
		fprintf(stderr,"#cq: continuous query plan\n");
#endif
		msg = CQregisterInternal(cntxt, userRef, putName(name));
	}

	// register all the baskets mentioned in the plan
	for( i=1; i< s->def->stop;i++){
		p= getInstrPtr(s->def,i);
		if( getModuleId(p) == basketRef && getFunctionId(p)== registerRef){
			BSKTregister(cntxt,s->def,0,p);
		}
	}
	return msg;
}

/* Every SQL statement is wrapped with a caller function that
 * regulates transaction bounds, debugger
 * The actual function is called with the arguments provided in the call.
 */

str
CQregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci )
{
	mvc* sqlcontext = ((backend *) cntxt->sqlcontext)->mvc;
	str msg = MAL_SUCCEED;
	InstrPtr sig = getInstrPtr(mb,0),q;
	MalBlkPtr other;
	Symbol s;
	int i, cycles = sqlcontext ? sqlcontext->cycles : DEFAULT_CP_CYCLES, heartbeats = sqlcontext ? sqlcontext->heartbeats : DEFAULT_CP_HEARTBEAT;

	(void) pci;

	if(cycles < 0 && cycles != DEFAULT_CP_CYCLES){
		msg = createException(SQL,"cquery.register","The cycles value must be non negative\n");
		goto finish;
	}
	if(heartbeats < 0){
		msg = createException(SQL,"cquery.register","The heartbeats value must be non negative\n");
		goto finish;
	}

	/* extract the actual procedure call and check for duplicate*/
	for(i = 1; i< mb->stop; i++){
		sig= getInstrPtr(mb,i);
		if( getModuleId(sig) == userRef)
			break;
	}
	if( i == mb->stop){
		msg = createException(SQL,"cquery.register","Cannot detect procedure call %s.%s.\n",
		getModuleId(sig), getFunctionId(sig));
		goto finish;
	}

#ifdef DEBUG_CQUERY
	fprintFunction(stderr, mb, 0, LIST_MAL_ALL);
#endif
	MT_lock_set(&ttrLock);

	// access the actual procedure body
	s = findSymbol(cntxt->nspace, getModuleId(sig), getFunctionId(sig));
	if ( s == NULL){
		msg = createException(SQL,"cquery.register","Cannot find procedure %s.%s.\n",
		getModuleId(sig), getFunctionId(sig));
		goto unlock;
	} else
		msg = CQanalysis(cntxt, s->def, pnettop);
	if( msg != MAL_SUCCEED) {
		CQfree(pnettop); // restore the entry
		goto unlock;
	}
	if(heartbeats != DEFAULT_CP_HEARTBEAT && pnet[pnettop].baskets[0]) {
		msg = createException(SQL, "cquery.register", "Heartbeat ignored, a window constraint exists\n");
		goto unlock;
	}

	other = copyMalBlk(mb);
	if(other == NULL) {
		msg = createException(SQL,"cquery.register",MAL_MALLOC_FAIL);
		goto unlock;
	}
	q = newStmt(other, sqlRef, transactionRef);
	if(q == NULL) {
		msg = createException(SQL,"cquery.register",MAL_MALLOC_FAIL);
		freeMalBlk(other);
		goto unlock;
	}
	setArgType(other,q, 0, TYPE_void);
	moveInstruction(other, getPC(other,q),i);
	q = newStmt(other, sqlRef, commitRef);
	if(q == NULL) {
		msg = createException(SQL,"cquery.register",MAL_MALLOC_FAIL);
		freeMalBlk(other);
		goto unlock;
	}
	setArgType(other,q, 0, TYPE_void);
	moveInstruction(other, getPC(other,q),i+2);
	chkProgram(cntxt->fdout, cntxt->nspace, other);

	pnet[pnettop].mod = GDKstrdup(getModuleId(sig));
	if(pnet[pnettop].mod == NULL) {
		msg = createException(SQL,"cquery.register",MAL_MALLOC_FAIL);
		freeMalBlk(other);
		goto unlock;
	}

	pnet[pnettop].fcn = GDKstrdup(getFunctionId(sig));
	if(pnet[pnettop].fcn == NULL) {
		msg = createException(SQL,"cquery.register",MAL_MALLOC_FAIL);
		freeMalBlk(other);
		GDKfree(pnet[pnettop].mod);
		goto unlock;
	}

	pnet[pnettop].stmt = instruction2str(other,stk,sig,LIST_MAL_CALL);
	if(pnet[pnettop].stmt == NULL) {
		msg = createException(SQL,"cquery.register",MAL_MALLOC_FAIL);
		freeMalBlk(other);
		GDKfree(pnet[pnettop].mod);
		GDKfree(pnet[pnettop].fcn);
		goto unlock;
	}

	pnet[pnettop].mb = other;

	pnet[pnettop].stk = prepareMALstack(other, other->vsize);
	if(pnet[pnettop].stk == NULL) {
		msg = createException(SQL,"cquery.register",MAL_MALLOC_FAIL);
		freeMalBlk(other);
		GDKfree(pnet[pnettop].mod);
		GDKfree(pnet[pnettop].fcn);
		GDKfree(pnet[pnettop].stmt);
		goto unlock;
	}
	pnet[pnettop].cycles = cycles;
	pnet[pnettop].beats = SET_HEARTBEATS(heartbeats);
	pnet[pnettop].run  = lng_nil;
	pnet[pnettop].seen = *timestamp_nil;
	pnet[pnettop].status = CQWAIT;
	pnettop++;

unlock:
	MT_lock_unset(&ttrLock);
finish:
	if(!msg && CQinit == 0) { /* start the scheduler if needed */
		msg = CQstartScheduler();
	}
	return msg;
}

static str
CQresumeInternal(Client cntxt, MalBlkPtr mb, int with_alter)
{
	mvc* sqlcontext = ((backend *) cntxt->sqlcontext)->mvc;
	str msg = MAL_SUCCEED, mb2str = NULL;
	int idx = 0, cycles = DEFAULT_CP_CYCLES, heartbeats = DEFAULT_CP_HEARTBEAT;

#ifdef DEBUG_CQUERY
	fprintf(stderr, "#resume scheduler\n");
#endif

	if(with_alter && sqlcontext) {
		cycles = sqlcontext->cycles;
		heartbeats = sqlcontext->heartbeats;
		if(cycles < 0 && cycles != DEFAULT_CP_CYCLES){
			msg = createException(SQL,"cquery.resume","The cycles value must be non negative\n");
			goto finish;
		}
		if(heartbeats < 0){
			msg = createException(SQL,"cquery.resume","The heartbeats value must be non negative\n");
			goto finish;
		}
	}

	MT_lock_set(&ttrLock);

	if(CQlocateMb(mb, &idx, &mb2str, "cquery.resume") != MAL_SUCCEED) {
		goto unlock;
	}
	if( idx == pnettop) {
		msg = createException(SQL, "cquery.resume", "The continuous procedure %s has not yet started\n", mb2str);
		goto unlock;
	}
	if( pnet[idx].status != CQPAUSE) {
		msg = createException(SQL, "cquery.resume", "The continuous procedure %s is already running\n", mb2str);
		goto unlock;
	}
	if(with_alter && heartbeats != DEFAULT_CP_HEARTBEAT && pnet[idx].baskets[0]) {
		msg = createException(SQL, "cquery.resume", "Heartbeat ignored, a window constraint exists\n");
		goto unlock;
	}

	pnet[idx].status = CQWAIT;
	if(with_alter) {
		pnet[idx].cycles = cycles;
		pnet[idx].beats = SET_HEARTBEATS(heartbeats);
	}

	/* start the scheduler if needed */
	if(CQinit == 0) {
		msg = CQstartScheduler();
	}

unlock:
	MT_lock_unset(&ttrLock);
	if(mb2str)
		GDKfree(mb2str);
finish:
	return msg;
}

str
CQresume(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, k =-1;
	InstrPtr q;
	(void) stk;
	(void) pci;

	for( i=1; i < mb->stop; i++){
		q = getInstrPtr(mb,i);

		if( q->token == ENDsymbol )
			break;
		if( getModuleId(q) == userRef){
			k = i;
			break;
		}
	}
	if( k >= 0 )
		return CQresumeInternal(cntxt, mb, 1);
	throw(SQL,"cquery.resume","The continuous procedure %s.%s was not found\n", getModuleId(getInstrPtr(mb,k)), getFunctionId(getInstrPtr(mb,k)));
}

str
CQresumeNoAlter(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, k =-1;
	InstrPtr q;
	(void) stk;
	(void) pci;

	for( i=1; i < mb->stop; i++){
		q = getInstrPtr(mb,i);

		if( q->token == ENDsymbol )
			break;
		if( getModuleId(q) == userRef){
			k = i;
			break;
		}
	}
	if( k >= 0 )
		return CQresumeInternal(cntxt, mb, 0);
	throw(SQL,"cquery.resume","The continuous procedure %s.%s was not found\n", getModuleId(getInstrPtr(mb,k)), getFunctionId(getInstrPtr(mb,k)));
}

str
CQresumeAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int i;
	mvc* smvc;

	ALL_ROOT_CHECK(cntxt, "cquery.resumeall", "RESUME ");

	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;

#ifdef DEBUG_CQUERY
	fprintf(stderr, "#resume scheduler\n");
#endif

	MT_lock_set(&ttrLock);
	for(i = 0 ; i < pnettop; i++)
		pnet[i].status = CQWAIT;

	/* start the scheduler if needed */
	if(CQinit == 0) {
		MT_lock_unset(&ttrLock);
		msg = CQstartScheduler();
	} else {
		MT_lock_unset(&ttrLock);
	}
	return msg;
}

static str
CQpauseInternal(MalBlkPtr mb)
{
	int idx = 0;
	str msg = MAL_SUCCEED, mb2str = NULL;

	MT_lock_set(&ttrLock);
	if(CQlocateMb(mb, &idx, &mb2str, "cquery.pause") != MAL_SUCCEED) {
		goto finish;
	}
	if( idx == pnettop) {
		msg = createException(SQL, "cquery.pause", "The continuous procedure %s has not yet started\n", mb2str);
		goto finish;
	}
	if( pnet[idx].status == CQPAUSE) {
		msg = createException(SQL, "cquery.pause", "The continuous procedure %s is already paused\n", mb2str);
		goto finish;
	}
	// actually wait if the query was running
	while( pnet[idx].status == CQRUNNING ){
		MT_lock_unset(&ttrLock);
		MT_sleep_ms(5);
		MT_lock_set(&ttrLock);
		if( pnet[idx].status == CQWAIT)
			break;
	}
	pnet[idx].status = CQPAUSE;

finish:
	MT_lock_unset(&ttrLock);
	if(mb2str)
		GDKfree(mb2str);
	return msg;
}

str
CQpause(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,k = -1;
	InstrPtr q;
	(void) cntxt;
	(void) stk;
	(void) pci;

	for( i=1; i < mb->stop; i++){
		q = getInstrPtr(mb,i);
		if( q->token == ENDsymbol )
			break;
		if( getModuleId(q) == userRef){
			k = i;
			break;
		}
	}
	if( k >= 0)
		return CQpauseInternal(mb);
	throw(SQL,"cquery.pause","The continuous procedure %s.%s was not found\n", getModuleId(getInstrPtr(mb,k)), getFunctionId(getInstrPtr(mb,k)));
}

str
CQpauseAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int i;
	mvc* smvc;

	ALL_ROOT_CHECK(cntxt, "cquery.pauseall", "PAUSE ");

	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;

#ifdef DEBUG_CQUERY
	fprintf(stderr, "#pause cqueries\n");
#endif

	MT_lock_set(&ttrLock);
	for(i = 0 ; i < pnettop; i++) {
		while( pnet[i].status == CQRUNNING ){
			MT_lock_unset(&ttrLock);
			MT_sleep_ms(5);
			MT_lock_set(&ttrLock);
			if( pnet[i].status == CQWAIT)
				break;
		}
		pnet[i].status = CQPAUSE;
	}
	MT_lock_unset(&ttrLock);
	return msg;
}

str
CQcycles(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch, fcn, msg = MAL_SUCCEED;
	int cycles, idx=0, last;
	(void) cntxt;
	(void) mb;

	MT_lock_set(&ttrLock);
	last = pnettop;
	if( pci->argc >2){
		sch = *getArgReference_str(stk,pci,1);
		fcn = *getArgReference_str(stk,pci,2);
		idx = CQlocate(sch, fcn);
		if( idx == pnettop) {
			msg = createException(SQL,"cquery.cycles","The continuous procedure %s.%s not accessible\n",sch,fcn);
			goto finish;
		}
		last = idx+1;
		cycles = *getArgReference_int(stk,pci,3);
	} else
		cycles = *getArgReference_int(stk,pci,1);
#ifdef DEBUG_CQUERY
	fprintf(stderr, "#set cycles \n");
#endif
	for( ; idx < last; idx++)
		pnet[idx].cycles = cycles;

	finish:
	MT_lock_unset(&ttrLock);
	return msg;
}

static void
CQsetbeat(int idx, int beats)
{
	int i;
	for( i=0; i < MAXSTREAMS; i++)
	if( pnet[idx].inout[i] == STREAM_IN && baskets[pnet[idx].baskets[i]].table->stream->window){
		// can not set the beat due to stream window constraint
		pnet[idx].beats = lng_nil;
		return;
	}
	pnet[idx].beats = SET_HEARTBEATS(beats);
}

str
CQheartbeat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch, fcn, msg = MAL_SUCCEED;
	int beats, idx=0, last= pnettop;
	(void) cntxt;
	(void) mb;

	MT_lock_set(&ttrLock);
	if( pci->argc >2){
		sch = *getArgReference_str(stk,pci,1);
		fcn = *getArgReference_str(stk,pci,2);
		idx = CQlocate(sch, fcn);
		if( idx == pnettop) {
			msg = createException(SQL,"cquery.heartbeat","The continuous procedure %s.%s not accessible\n",sch,fcn);
			goto finish;
		}
		last = idx+1;
		beats = *getArgReference_int(stk,pci,3);
#ifdef DEBUG_CQUERY
		fprintf(stderr, "#set the heartbeat of %s.%s to %d\n",sch,fcn,beats);
#endif
	} else{
		beats = *getArgReference_int(stk,pci,1);
#ifdef DEBUG_CQUERY
		fprintf(stderr, "#set the heartbeat %d ms\n",beats);
#endif
	}
	for( ; idx < last; idx++){
		if( pnet[idx].baskets[0]){
			msg = createException(SQL,"cquery.heartbeat","Heartbeat ignored, a window constraint exists\n");
			break;
		}
		CQsetbeat(idx, beats);
	}
	finish:
	MT_lock_unset(&ttrLock);
	return msg;
}

str
CQwait(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	unsigned int delay = (unsigned int) *getArgReference_int(stk,pci,1);

	(void) cntxt;
	(void) mb;
#ifdef DEBUG_CQUERY
	fprintf(stderr, "#scheduler wait %d ms\n",delay);
#endif
	MT_sleep_ms(delay);
	return MAL_SUCCEED;
}

/*Remove a specific continuous query from the scheduler */

static str
CQderegisterInternal(MalBlkPtr mb)
{
	int idx = 0;
	str msg = MAL_SUCCEED, mb2str = NULL;

	MT_lock_set(&ttrLock);
	if(CQlocateMb(mb, &idx, &mb2str, "cquery.deregister") != MAL_SUCCEED) {
		goto finish;
	}
	if(idx == pnettop) {
		msg = createException(SQL, "cquery.deregister", "The continuous procedure %s has not yet started\n", mb2str);
		goto finish;
	}
	pnet[idx].status = CQSTOP;
	MT_lock_unset(&ttrLock);
	// actually wait if the query was running
	while( pnet[idx].status != CQDEREGISTER ){
		MT_sleep_ms(5);
	}
	MT_lock_set(&ttrLock);
	CQfree(idx);
	if( pnettop == 0)
		pnstatus = CQSTOP;

finish:
	if(mb2str)
		GDKfree(mb2str);
	MT_lock_unset(&ttrLock);
	return msg;
}

str
CQderegister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, k= -1;
	InstrPtr q;
	(void) cntxt;
	(void) stk;
	(void) pci;

	for( i=1; i < mb->stop; i++){
		q = getInstrPtr(mb,i);
		if( q->token == ENDsymbol )
			break;
		if( getModuleId(q) == userRef){
			k = i;
			break;
		}
	}
	if( k>= 0 )
		return CQderegisterInternal(mb);
	throw(SQL,"cquery.deregister","The continuous procedure %s.%s was not found\n", getModuleId(getInstrPtr(mb,k)), getFunctionId(getInstrPtr(mb,k)));
}

str
CQderegisterAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int i;
	mvc* smvc;

	ALL_ROOT_CHECK(cntxt, "cquery.deregisterall", "STOP ");

	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;

	MT_lock_set(&ttrLock);

	for(i = pnettop - 1 ; i > -1; i--) {
		pnet[i].status = CQSTOP;
		MT_lock_unset(&ttrLock);

		// actually wait if the query was running
		while( pnet[i].status != CQDEREGISTER ){
			MT_sleep_ms(5);
		}
		MT_lock_set(&ttrLock);
		CQfree(i);
	}
	pnstatus = CQSTOP;

	MT_lock_unset(&ttrLock);
	return msg;
}

/* WARNING no locks in this call yet! */

str
CQdump(void *ret)
{
	int i, k;

	fprintf(stderr, "#scheduler status %s\n", statusname[pnstatus]);
	for (i = 0; i < pnettop; i++) {
		fprintf(stderr, "#[%d]\t%s.%s %s ",
				i, pnet[i].mod, pnet[i].fcn, statusname[pnet[i].status]);
		if ( pnet[i].beats != lng_nil)
			fprintf(stderr, "beats="LLFMT" ", pnet[i].beats);

		if( pnet[i].inout[0])
			fprintf(stderr, " streams ");
		for (k = 0; k < MAXSTREAMS && pnet[i].baskets[k]; k++)
		if( pnet[i].inout[k] == STREAM_IN)
			fprintf(stderr, "%s.%s ", baskets[pnet[i].baskets[k]].table->s->base.name, baskets[pnet[i].baskets[k]].table->base.name);
		if( pnet[i].inout[0])
			fprintf(stderr, " --> ");
		for (k = 0; k < MAXSTREAMS && pnet[i].baskets[k]; k++)
		if( pnet[i].inout[k] == STREAM_OUT)
			fprintf(stderr, "%s.%s ", baskets[pnet[i].baskets[k]].table->s->base.name, baskets[pnet[i].baskets[k]].table->base.name);
		if (pnet[i].error)
			fprintf(stderr, " errors:%s", pnet[i].error);
		fprintf(stderr, "\n");
	}
	(void) ret;
	return MAL_SUCCEED;
}

/*
 * The PetriNet scheduler lives in an separate thread.
 * It cycles through all transition nodes, hunting for paused queries that can fire.
 * The current policy is a simple round-robin. Later we will
 * experiment with more advanced schemes, e.g., priority queues.
 *
 * During each cycle step we first enable the transformations.
 *
 * Locking the streams is necessary to avoid concurrent changes.
 * Using a fixed order over the basket table, ensure no deadlock, but may render queries never to execute.
 */
static void
CQexecute( Client cntxt, int idx)
{
	CQnode *node= pnet+ idx;
	str msg;

	if( pnstatus != CQRUNNING)
		return;
	// first grab exclusive access to all streams.

#ifdef DEBUG_CQUERY
	fprintf(stderr, "#cquery.execute %s.%s locked\n",node->mod, node->fcn);
	fprintFunction(stderr, node->mb, 0, LIST_MAL_NAME | LIST_MAL_VALUE  | LIST_MAL_MAPI);
#endif

	msg = runMALsequence(cntxt, node->mb, 1, 0, node->stk, 0, 0);
	if( msg != MAL_SUCCEED)
		pnet[idx].error = msg;

	// release all locks held
#ifdef DEBUG_CQUERY
		fprintf(stderr, "#cquery.execute %s.%s finished %s\n", node->mod, node->fcn, (msg?msg:""));
#endif
	MT_lock_set(&ttrLock);
	if( node->status != CQSTOP)
		node->status = CQWAIT;
	MT_lock_unset(&ttrLock);
}

static void
CQscheduler(void *dummy)
{
	int i, j;
	int k = -1;
	int pntasks;
	int delay = cycleDelay;
	Client cntxt = (Client) dummy;
	str msg = MAL_SUCCEED;
	lng t, now;
	int claimed[MAXSTREAMS];
	BAT *b;

#ifdef DEBUG_CQUERY
	fprintf(stderr, "#cquery.scheduler started\n");
#endif

	CQinit = 1;
	MT_lock_set(&ttrLock);
	pnstatus = CQRUNNING; // global state 
	MT_lock_unset(&ttrLock);

	while( pnstatus != CQSTOP  && ! GDKexiting()){
		/* Determine which continuous queries are eligble to run.
  		   Collect latest statistics, note that we don't need a lock here,
		   because the count need not be accurate to the usec. It will simply
		   come back. We also only have to check the places that are marked
		   non empty. You can only trigger on empty baskets using a heartbeat */
		memset((void*) claimed, 0, sizeof(claimed));
		now = GDKusec();
		pntasks=0;
		MT_lock_set(&ttrLock); // analysis should be done with exclusive access
		for (k = i = 0; i < pnettop; i++)
		if ( pnet[i].status == CQWAIT ){
			pnet[i].enabled = pnet[i].error == 0 && (pnet[i].cycles > 0 || pnet[i].cycles == DEFAULT_CP_CYCLES);

			/* Queries are triggered by the heartbeat or  all window constraints */
			/* A heartbeat in combination with a window constraint is ambiguous */
			/* At least one constraint should be set */
			if( pnet[i].beats == lng_nil && pnet[i].baskets[0] == 0)
				pnet[i].enabled = 0;

			if( pnet[i].enabled && pnet[i].beats > 0){
				if( pnet[i].run != lng_nil ) {
					pnet[i].enabled = now >= pnet[i].run + pnet[i].beats;
#ifdef DEBUG_CQUERY_SCHEDULER
					fprintf(stderr,"#beat %s.%s  "LLFMT"("LLFMT") %s\n", pnet[i].mod, pnet[i].fcn,
						pnet[i].run + pnet[i].beats, now, (pnet[i].enabled? "enabled":"disabled"));
#endif
				}
			}

			/* check if all input baskets are available */
			for (j = 0; pnet[i].enabled && pnet[i].baskets[j] && (b = baskets[pnet[i].baskets[j]].bats[0]); j++)
				/* consider execution only if baskets are properly filled */
				if ( pnet[i].inout[j] == STREAM_IN && (BUN) baskets[pnet[i].baskets[j]].table->stream->window > BATcount(b)){
					pnet[i].enabled = 0;
					break;
				}

			/* check availability of all stream baskets */
			for (j = 0; pnet[i].enabled && pnet[i].baskets[j]; j++){
				for(k=0; claimed[k]; k++)
					if(claimed[k]  ==  pnet[i].baskets[j]){
						pnet[i].enabled = 0;
#ifdef DEBUG_CQUERY_SCHEDULER
						fprintf(stderr, "#cquery: %s.%s,disgarded \n", pnet[i].mod, pnet[i].fcn);
#endif
						break;
					}
				if (pnet[i].enabled && claimed[k] == 0)
					claimed[k] =  pnet[i].baskets[j];
			}

#ifdef DEBUG_CQUERY_SCHEDULER
			if( pnet[i].enabled)
				fprintf(stderr, "#cquery: %s.%s enabled \n", pnet[i].mod, pnet[i].fcn);
#endif
			pntasks += pnet[i].enabled;
		} else
		if( pnet[i].status == CQSTOP){
			pnet[i].status = CQDEREGISTER;
			pnet[i].enabled = 0;
		}
		(void) fflush(stderr);
		MT_lock_unset(&ttrLock);
#ifdef DEBUG_CQUERY_SCHEDULER
		if( pntasks)
			fprintf(stderr, "#Transitions %d enabled:\n",pntasks);
#else
		(void) pntasks;
#endif
		if( pnstatus == CQSTOP)
			continue;

		/* Execute each enabled transformation */
		/* Tricky part is here a single stream used by multiple transitions */
		for (i = 0; i < pnettop  ; i++)
		if( pnet[i].enabled){
#ifdef DEBUG_CQUERY
			fprintf(stderr, "#Run transition %s.%s cycle=%d \n", pnet[i].mod, pnet[i].fcn, pnet[i].cycles);
#endif

			t = GDKusec();
			// Fork MAL execution thread
			MT_lock_set(&ttrLock);
			if (pnet[i].status != CQWAIT){
				MT_lock_unset(&ttrLock);
				goto wrapup;
			}
			pnet[i].status = CQRUNNING;
			MT_lock_unset(&ttrLock);
			if( !GDKexiting())
				CQexecute(cntxt, i);
/*
				if (MT_create_thread(&pnet[i].tid, CQexecute, (void*) (pnet+i), MT_THR_JOINABLE) < 0){
					msg= createException(MAL,"petrinet.scheduler","Can not fork the thread");
				} else
*/
			if( pnet[i].cycles != DEFAULT_CP_CYCLES && pnet[i].cycles > 0)
				pnet[i].cycles--;
			pnet[i].run = now;				/* last executed */
			pnet[i].time = GDKusec() - t;   /* keep around in microseconds */
			(void) MTIMEcurrent_timestamp(&pnet[i].seen);
			pnet[i].enabled = 0;
			CQentry(i);
			if (msg != MAL_SUCCEED ){
				char buf[BUFSIZ];
				if (pnet[i].error == NULL) {
					snprintf(buf, BUFSIZ - 1, "Query %s.%s failed:%s", pnet[i].mod, pnet[i].fcn,msg);
					pnet[i].error = GDKstrdup(buf);
				} else
					GDKfree(msg);
			}
			delay = cycleDelay;
		}
wrapup:
		/* after one sweep all threads should be released */
/*
		for (m = 0; m < k; m++)
		if(pnet[enabled[m]].tid){
#ifdef DEBUG_CQUERY
			fprintf(stderr, "#Terminate query thread %s limit %d \n", pnet[enabled[m]].fcnname, pnet[enabled[m]].limit);
#endif
			MT_join_thread(pnet[enabled[m]].tid);
		}

#ifdef DEBUG_CQUERY
		if (pnstatus == CQRUNNING && cycleDelay) MT_sleep_ms(cycleDelay);  
#endif
		MT_sleep_ms(CQDELAY);  
*/
		/* we should actually delay until the next heartbeat or insertion into the streams */
		if (pntasks == 0  || pnstatus == CQPAUSE) {
#ifdef DEBUG_CQUERY
			fprintf(stderr, "#cquery.scheduler paused\n");
#endif
			if( pnettop == 0)
				break;
			MT_sleep_ms(delay);
			if( delay < 20 * cycleDelay)
				delay = (int) (delay *1.2);
		}
	}
#ifdef DEBUG_CQUERY
	fprintf(stderr, "#cquery.scheduler stopped\n");
#endif
	cntxt->fdin = 0;
	cntxt->fdout = 0;
	//MCcloseClient(cntxt); to be checked, removes too much
	pnstatus = CQINIT;
	CQinit = 0;
}

str
CQstartScheduler(void)
{
	MT_Id pid;
	Client cntxt;
	stream *fin, *fout;

#ifdef DEBUG_CQUERY
	fprintf(stderr, "#Start CQscheduler\n");
#endif
	fin =  open_rastream("fin_petri_sched");
	fout =  open_wastream("fout_petri_sched");
	cntxt = MCinitClient(0,bstream_create(fin,0),fout);

	if( cntxt == NULL)
		throw(MAL, "cquery.startScheduler","Could not initialize CQscheduler");
	if( SQLinitClient(cntxt) != MAL_SUCCEED)
		throw(MAL, "cquery.startScheduler","Could not initialize SQL context");

	if (pnstatus== CQINIT && MT_create_thread(&pid, CQscheduler, (void*) cntxt, MT_THR_JOINABLE) != 0){
#ifdef DEBUG_CQUERY
		fprintf(stderr, "#Start CQscheduler failed\n");
#endif
		throw(MAL, "cquery.startScheduler", "cquery creation failed");
	}
	(void) pid;
	return MAL_SUCCEED;
}

str
CQprelude(void *ret) {
	(void) ret;
	MT_lock_init(&ttrLock, "cqueryLock");
	printf("# MonetDB/Timetrails module loaded\n");
	return MAL_SUCCEED;
}
