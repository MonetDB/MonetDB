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

   The scheduling amongst the transitions is currently deterministic. Upon each round of the scheduler, it determines all
   transitions eligible to fire, i.e. have non-empty baskets or whose heartbeat ticks, which are then activated one after the other.
   Future implementations may relax this rigid scheme using a parallel implementation of the scheduler, such that each 
   transition by itself can decide to fire. However, when resources are limited to handle all complex continuous queries, 
   it may pay of to invest into a domain specific scheduler.

   The current implementation is limited to a fixed number of transitions. The scheduler can be stopped and restarted
   at any time. Even selectively for specific baskets. This provides the handle to debug a system before being deployed.
   In general, event processing through multiple layers of continuous queries is too fast to trace them one by one.
   Some general statistics about number of events handled per transition is maintained, as well as the processing time
   for each continuous query step. This provides the information to re-design the event handling system.
 */

#include "monetdb_config.h"
#include "sql_optimizer.h"
#include "sql_gencode.h"
#include "sql_cquery.h"
#include "sql_basket.h"
#include "sql_execute.h"
#include "mal_builder.h"
#include "opt_prelude.h"
#include "mtime.h"

static str statusname[7] = {"init", "register", "readytorun", "running", "waiting", "paused", "stopping"};

static str CQstartScheduler(void);
static int pnstatus = CQINIT;
static int cycleDelay = 200; /* be careful, it affects response/throughput timings */
static MT_Lock ttrLock;
static MT_Id cq_pid = 0;
static int CQ_counter = 0;

static BAT *CQ_id_tick = 0;
static BAT *CQ_id_alias = 0;
static BAT *CQ_id_time = 0;
static BAT *CQ_id_error = 0;

static CQnode *pnet = 0;
static int pnetLimit = 0, pnettop = 0;

#define CQ_SQL_QUERY_SIZE  1024

#define SET_HEARTBEATS(X) (X != HEARTBEAT_NIL) ? X : HEARTBEAT_NIL /* minimal 1 ms */

#define ALL_ROOT_CHECK(cntxt, malcal, name)                                                                            \
	do {                                                                                                               \
		smvc = ((backend *) cntxt->sqlcontext)->mvc;                                                                   \
		if(!smvc)                                                                                                      \
			throw(SQL,malcal,SQLSTATE(42000) "##name##ALL CONTINUOUS: SQL clients only\n");                            \
		 else if (smvc->user_id != USER_MONETDB && smvc->role_id != ROLE_SYSADMIN)                                     \
			throw(SQL,malcal,SQLSTATE(42000) "##name##ALL CONTINUOUS: insufficient privileges for the current user\n");\
	} while(0);

static void
CQfree(int idx)
{
	int i, j, k, found;
	str sch, tbl;
	InstrPtr p;

	if( pnet[idx].mb) {
		p = getInstrPtr(pnet[idx].mb, 0);
		GDKfree(p->fcnname); //Free the CQ id
		freeMalBlk(pnet[idx].mb);
	}
	if( pnet[idx].stk)
		freeStack(pnet[idx].stk);
	if(pnet[idx].error)
		GDKfree(pnet[idx].error);
	GDKfree(pnet[idx].alias);
	//try delete the baskets
	for( j=0; j< MAXSTREAMS && pnet[idx].baskets[j]; j++) {
		found = 0;
		sch = baskets[pnet[idx].baskets[j]].table->s->base.name;
		tbl = baskets[pnet[idx].baskets[j]].table->base.name;
		for( i=0; i < pnettop && !found; i++) {
			if(i != idx) {
				for( k=0; k< MAXSTREAMS && pnet[i].baskets[k] && !found; k++) {
					if (strcmp(sch, baskets[pnet[i].baskets[k]].table->s->base.name) == 0 &&
						strcmp(tbl, baskets[pnet[i].baskets[k]].table->base.name) == 0)
						found = 1;
				}
			}
		}
		if(!found) {
			BSKTclean(pnet[idx].baskets[j]);
		}
	}
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

static str
CQcreatelog(void){
	if( CQ_id_tick)
		return MAL_SUCCEED;
#ifdef DEBUG_CQUERY
	fprintf(stderr,"#create query.log table\n");
#endif
	CQ_id_tick = COLnew(0, TYPE_timestamp, 1<<16, TRANSIENT);
	CQ_id_time = COLnew(0, TYPE_lng, 1<<16, TRANSIENT);
	CQ_id_error = COLnew(0, TYPE_str, 1<<16, TRANSIENT);
	CQ_id_alias = COLnew(0, TYPE_str, 1<<16, TRANSIENT);
	if ( CQ_id_tick == 0 || CQ_id_time == 0 || CQ_id_alias == 0 || CQ_id_error == 0){
		if( CQ_id_tick) BBPunfix(CQ_id_tick->batCacheid);
		if( CQ_id_time) BBPunfix(CQ_id_time->batCacheid);
		if( CQ_id_error) BBPunfix(CQ_id_error->batCacheid);
		if( CQ_id_alias) BBPunfix(CQ_id_alias->batCacheid);
		throw(MAL,"cquery.log",SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	return MAL_SUCCEED;
}

static void
CQentry(int idx)
{
	CQcreatelog();
	if( BUNappend(CQ_id_tick, &pnet[idx].seen,FALSE) != GDK_SUCCEED ||
		BUNappend(CQ_id_time, &pnet[idx].time,FALSE) != GDK_SUCCEED ||
		BUNappend(CQ_id_error, (pnet[idx].error ? pnet[idx].error:""),FALSE) != GDK_SUCCEED ||
		BUNappend(CQ_id_alias, (pnet[idx].alias ? pnet[idx].alias:""),FALSE) != GDK_SUCCEED )
		pnet[idx].error = createException(SQL,"cquery.logentry",SQLSTATE(HY001) MAL_MALLOC_FAIL);
}

str
CQlog( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	BAT *tickbat = 0, *aliasbat =0, *timebat = 0, *errbat = 0;
	bat *tickret, *aliasret, *timeret, *errorret;

	(void) cntxt;
	(void) mb;

	tickret = getArgReference_bat(stk, pci, 0);
	aliasret = getArgReference_bat(stk, pci, 1);
	timeret = getArgReference_bat(stk, pci, 2);
	errorret = getArgReference_bat(stk, pci, 3);
#ifdef DEBUG_CQUERY
	fprintf(stderr,"#produce query.log table\n");
#endif
	MT_lock_set(&ttrLock);
	CQcreatelog();
	tickbat = COLcopy(CQ_id_tick, TYPE_timestamp, 0, TRANSIENT);
	if(tickbat == NULL)
		goto wrapup;
	aliasbat = COLcopy(CQ_id_alias, TYPE_str, 0, TRANSIENT);
	if(aliasbat == NULL)
		goto wrapup;
	timebat = COLcopy(CQ_id_time, TYPE_lng, 0, TRANSIENT);
	if(timebat == NULL)
		goto wrapup;
	errbat = COLcopy(CQ_id_error, TYPE_str, 0, TRANSIENT);
	if(errbat == NULL)
		goto wrapup;
	MT_lock_unset(&ttrLock);
	BBPkeepref(*tickret = tickbat->batCacheid);
	BBPkeepref(*aliasret = aliasbat->batCacheid);
	BBPkeepref(*timeret = timebat->batCacheid);
	BBPkeepref(*errorret = errbat->batCacheid);
	return MAL_SUCCEED;
wrapup:
	MT_lock_unset(&ttrLock);
	if( tickbat) BBPunfix(tickbat->batCacheid);
	if( aliasbat) BBPunfix(aliasbat->batCacheid);
	if( timebat) BBPunfix(timebat->batCacheid);
	if( errbat) BBPunfix(errbat->batCacheid);
	throw(SQL,"cquery.log",SQLSTATE(HY001) MAL_MALLOC_FAIL);
}

str
CQstatus( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	BAT *tickbat = 0, *aliasbat =0, *statusbat = 0, *errbat = 0;
	bat *tickret = 0, *aliasret = 0, *statusret = 0, *errorret = 0;
	int idx;
	str msg= MAL_SUCCEED;

	(void) cntxt;
	(void) mb;

	tickret = getArgReference_bat(stk, pci, 0);
	aliasret = getArgReference_bat(stk, pci, 1);
	statusret = getArgReference_bat(stk, pci, 2);
	errorret = getArgReference_bat(stk, pci, 3);

	tickbat = COLnew(0, TYPE_timestamp, 0, TRANSIENT);
	if(tickbat == NULL)
		goto wrapup;
	aliasbat = COLnew(0, TYPE_str, 0, TRANSIENT);
	if(aliasbat == NULL)
		goto wrapup;
	statusbat = COLnew(0, TYPE_str, 0, TRANSIENT);
	if(statusbat == NULL)
		goto wrapup;
	errbat = COLnew(0, TYPE_str, 0, TRANSIENT);
	if(errbat == NULL)
		goto wrapup;

	MT_lock_set(&ttrLock);
	for( idx = 0; msg == MAL_SUCCEED && idx < pnettop; idx++)
		if( BUNappend(tickbat, &pnet[idx].seen,FALSE) != GDK_SUCCEED ||
			BUNappend(aliasbat, (pnet[idx].alias ? pnet[idx].alias:""),FALSE) != GDK_SUCCEED ||
			BUNappend(statusbat, statusname[pnet[idx].status],FALSE) != GDK_SUCCEED ||
			BUNappend(errbat, (pnet[idx].error ? pnet[idx].error:""),FALSE) != GDK_SUCCEED )
				msg = createException(SQL,"cquery.status",SQLSTATE(HY001) MAL_MALLOC_FAIL);
	MT_lock_unset(&ttrLock);

	BBPkeepref(*tickret = tickbat->batCacheid);
	BBPkeepref(*aliasret = aliasbat->batCacheid);
	BBPkeepref(*statusret = statusbat->batCacheid);
	BBPkeepref(*errorret = errbat->batCacheid);
	return msg;
wrapup:
	if( tickbat) BBPunfix(tickbat->batCacheid);
	if( aliasbat) BBPunfix(aliasbat->batCacheid);
	if( statusbat) BBPunfix(statusbat->batCacheid);
	if( errbat) BBPunfix(errbat->batCacheid);
	throw(SQL,"cquery.status",SQLSTATE(HY001) MAL_MALLOC_FAIL);
}

int
CQlocateUDF(sql_func *f) //check if an UDF is being used
{
	int i, res = 0;

	MT_lock_set(&ttrLock);
	for (i = 0; i < pnettop && !res; i++){
		if (pnet[i].func == f) {
			res = 1;
		}
	}
	MT_lock_unset(&ttrLock);
	return res;
}

int
CQlocateBasketExternal(str schname, str tblname) //check if a stream table is being used by a continuous query
{
	int i, j, res = 0;

	MT_lock_set(&ttrLock);
	for( i=0; i < pnettop && !res; i++){
		for( j=0; j< MAXSTREAMS && pnet[i].baskets[j] && !res; j++){
			if( strcmp(schname, baskets[pnet[i].baskets[j]].table->s->base.name) == 0 &&
				strcmp(tblname, baskets[pnet[i].baskets[j]].table->base.name) == 0 )
				res = 1;
		}
	}
	MT_lock_unset(&ttrLock);
	return res;
}

static int
CQlocateAlias(str alias)
{
	int i;
	for (i = 0; i < pnettop; i++){
		if (strcmp(pnet[i].alias, alias) == 0) {
			return i;
		}
	}
	return i;
}

/* A debugging routine */
str
CQshow(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str alias = *getArgReference_str(stk,pci,1), msg = MAL_SUCCEED;
	int idx;

	(void) cntxt;
	(void) mb;

	MT_lock_set(&ttrLock);
	idx = CQlocateAlias(alias);
	if( idx == pnettop) {
		msg = createException(SQL,"cquery.show", SQLSTATE(42000) "The continuous query %s has not yet started\n",alias);
		goto finish;
	}
	printFunction(cntxt->fdout, pnet[idx].mb, 0, LIST_MAL_NAME | LIST_MAL_VALUE | LIST_MAL_MAPI);
finish:
	MT_lock_unset(&ttrLock);
	return msg;
}

/* Collect all input/output basket roles */
/* Make sure we do not re-use the same source more than once */
/* Avoid any concurrency conflict */
static str
CQanalysis(Client cntxt, MalBlkPtr mb, int idx)
{
	int i, j, bskt, binout;
	InstrPtr p;
	str msg= MAL_SUCCEED, sch, tbl;
	(void) cntxt;

	p = getInstrPtr(mb, 0);
	for (i = 0; msg== MAL_SUCCEED && i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (getModuleId(p) == basketRef && (getFunctionId(p) == registerRef || getFunctionId(p) == bindRef)){
			sch = getVarConstant(mb, getArg(p,2)).val.sval;
			tbl = getVarConstant(mb, getArg(p,3)).val.sval;
			binout = getVarConstant(mb, getArg(p,4)).val.ival;

			// find the stream basket definition
			if((msg = BSKTregisterInternal(cntxt,mb,sch,tbl,&bskt)) != MAL_SUCCEED){
				continue;
			}

			// we only need a single column for window size testing
			for( j=0; j< MAXSTREAMS && pnet[idx].baskets[j]; j++)
			if( strcmp(sch, baskets[pnet[idx].baskets[j]].table->s->base.name) == 0 &&
				strcmp(tbl, baskets[pnet[idx].baskets[j]].table->base.name) == 0 )
				break;
			if ( j == MAXSTREAMS){
				msg = createException(MAL,"cquery.analysis",SQLSTATE(3F000) "too many stream table columns\n");
				continue;
			}

			if ( pnet[idx].baskets[j] )
				continue;

			pnet[idx].baskets[j] = bskt;
			pnet[idx].inout[j] = binout == 0 ? STREAM_IN : STREAM_OUT;
		}
	}
	return msg;
}

#define FREE_CQ_MB(X)    \
	if(cq_id)            \
		GDKfree(cq_id);  \
	if(mb)               \
		freeMalBlk(mb);  \
	if(ralias)           \
		GDKfree(ralias); \
	goto X;

#define CQ_MALLOC_FAIL(X)                                                         \
	msg = createException(SQL,"cquery.register",SQLSTATE(HY001) MAL_MALLOC_FAIL); \
	FREE_CQ_MB(X)

/* Every SQL statement is wrapped with a caller function that
 * regulates transaction bounds, debugger
 * The actual function is called with the arguments provided in the call.
 */
str
CQregister(Client cntxt, str sname, str fname, int argc, atom **args, str alias, int which, lng heartbeats, lng startat, int cycles)
{
	str msg = MAL_SUCCEED, rschema = NULL, ralias = NULL;
	InstrPtr p = NULL, q = NULL;
	Symbol sym;
	CQnode *pnew;
	MalBlkPtr mb = NULL, prev;
	const char* err_message = (which & mod_continuous_function) ? "function" : "procedure";
	char* cq_id = NULL, *sqlquery = NULL;
	char buffer[IDLENGTH];
	int i, idx, varid, cid, freeMB = 0;
	/*size_t sqlquerysize = CQ_SQL_QUERY_SIZE, pos = 0;*/
	backend* be = (backend*) cntxt->sqlcontext;
	mvc *m = be->mvc;
	sql_schema *s;
	sql_subfunc *f = NULL;
	sql_func* found = NULL;
	list *l;
	node *argn = NULL;

	prev = be->mb;

	if(cycles < 0 && cycles != CYCLES_NIL){
		msg = createException(SQL,"cquery.register",SQLSTATE(42000) "The cycles value must be non negative\n");
		goto finish;
	}
	if(heartbeats < 0 && heartbeats != HEARTBEAT_NIL){
		msg = createException(SQL,"cquery.register",SQLSTATE(42000) "The heartbeats value must be non negative\n");
		goto finish;
	}
	if(startat < 0){
		msg = createException(SQL,"cquery.register",SQLSTATE(42000) "The start at value must be non negative\n");
		goto finish;
	}

	if (!m->sa) {
		if((m->sa = sa_create()) == NULL) {
			CQ_MALLOC_FAIL(finish)
		}
	}
	if (!be->mb) {
		if((be->mb = newMalBlk(8)) == NULL) {
			CQ_MALLOC_FAIL(finish)
		}
		freeMB = 1;
	}

	rschema = (!sname || strcmp(sname, str_nil) == 0) ? m->session->schema_name : sname;
	if((s = mvc_bind_schema(m, rschema)) == NULL) { //bind the schema
		msg = createException(SQL,"cquery.register",SQLSTATE(3F000) "Failed to bind schema %s\n", rschema);
		goto finish;
	}

	//Find the UDF
	f = sql_find_func(m->sa, s, fname, argc > 0 ? argc : -1, (which & mod_continuous_function) ? F_FUNC : F_PROC, NULL);
	if(!f && which & mod_continuous_function) { //If an UDF returns a table, it gets compiled into a F_UNION
		f = sql_find_func(m->sa, s, fname, argc > 0 ? argc : -1, F_UNION, NULL);
	}
	if(!f) {
		msg = createException(SQL,"cquery.register",SQLSTATE(3F000) "Failed to bind %s %s.%s\n", err_message, rschema, fname);
		FREE_CQ_MB(finish)
	}
	if((l = list_create(NULL)) == NULL) {
		CQ_MALLOC_FAIL(finish)
	}
	for (i = 0; i < argc; i++) { //prepare the arguments for the backend creation
		atom *a = args[i];
		list_append(l, stmt_varnr(be, i, &a->tpe));
	}
	if (backend_create_subfunc(be, f, l) < 0) { //create the backend function
		msg = createException(SQL,"cquery.register",SQLSTATE(3F000) "Failed to generate backend function\n");
		list_destroy(l);
		FREE_CQ_MB(finish)
	}
	list_destroy(l);

	found = f->func;
	/*if(found->res && list_length(found->res)) { //for functions we have to store the results somewhere
		if(IS_UNION(found)) { //if it is a UNION (returning a table), we store it in a
			if((sqlquery = GDKmalloc(CQ_SQL_QUERY_SIZE)) == NULL) {
				CQ_MALLOC_FAIL(finish)
			}
			sqlquery[0] = '\0';
			pos = 0;
			pos += snprintf(sqlquery + pos, sqlquerysize - pos, "create temporary stream table \"%s\" (", found->imp);
			for (argn = found->res->h; argn; argn = argn->next) {
				sql_arg* argument = (sql_arg *) argn->data;
				pos += snprintf(sqlquery + pos, sqlquerysize - pos, "\"%s\" %s%s",
								argument->name, argument->type.type->sqlname, argn->next ? "," : "");
			}
			pos += snprintf(sqlquery + pos, sqlquerysize - pos, ");");
			if((msg = SQLstatementIntern(cntxt, &sqlquery, "cquery.register", 1, 0, NULL))) {
				FREE_CQ_MB(finish)
			}
		} else if(IS_FUNC(found)) {

		}
	}*/

	MT_lock_set(&ttrLock);
	cid = ++CQ_counter;
	MT_lock_unset(&ttrLock);
	(void) snprintf(buffer, sizeof(buffer), "cq_%d", cid); //set the CQ ID
	if((cq_id = GDKstrdup(buffer)) == NULL) {
		CQ_MALLOC_FAIL(finish)
	}
	if((mb = newMalBlk(8)) == NULL) { //create MalBlk and initialize it
		CQ_MALLOC_FAIL(finish)
	}
	if((p = newInstruction(NULL, "user", cq_id)) == NULL) {
		CQ_MALLOC_FAIL(finish)
	}
	p->token = FUNCTIONsymbol;
	p->barrier = 0;
	if((varid = newVariable(mb, cq_id, strlen(cq_id), TYPE_void)) < 0) {
		CQ_MALLOC_FAIL(finish)
	}
	setDestVar(p, varid);
	pushInstruction(mb, p);

	if((q = newStmt(mb, sqlRef, transactionRef)) == NULL) {
		CQ_MALLOC_FAIL(finish)
	}
	setArgType(mb,q, 0, TYPE_void);
	if ((p = newStmt(mb, "user", fname)) == NULL) { //add the UDF call statement
		CQ_MALLOC_FAIL(finish)
	}
	/*if (f->res && list_length(f->res)) {
		sql_subtype *res = f->res->h->data;
		setVarType(mb, getArg(q, 0), res->type->localtype);
		setVarUDFtype(mb, getArg(q, 0));
	}*/
	for (i = 0, argn = found->ops->h; i < argc && argn; i++, argn = argn->next) { //add variables to the MAL block
		sql_subtype tpe = ((sql_arg *) argn->data)->type;
		atom *a = args[i];
		ValPtr val = (ValPtr) &a->data;
		ValRecord dst;

		dst.vtype = tpe.type->localtype;
		/* use base tpe.type->localtype for user types */
		if (val->vtype > TYPE_str)
			val->vtype = ATOMstorage(val->vtype);
		if (dst.vtype > TYPE_str)
			dst.vtype = ATOMstorage(dst.vtype);
		/* first convert into a new location */
		if (VARconvert(&dst, val, 0) != GDK_SUCCEED) {
			msg = createException(SQL,"cquery.register",SQLSTATE(3F000) "Error while making a SQL type conversion\n");
			FREE_CQ_MB(finish)
		}
		/* and finally copy the result */
		*val = dst;
		/* make sure we return the correct type (not the storage type) */
		val->vtype = tpe.type->localtype;
		p = pushValue(mb, p, val);
		if(val->vtype == TYPE_str) //if the input variable is of type str we must free it
			GDKfree(val->val.sval);
		if(p == NULL) {
			CQ_MALLOC_FAIL(finish);
		}
	}
	if((q = newStmt(mb, sqlRef, commitRef)) == NULL) {
		CQ_MALLOC_FAIL(finish)
	}
	setArgType(mb,q, 0, TYPE_void);
	if(pushEndInstruction(mb) == NULL) {
		CQ_MALLOC_FAIL(finish)
	}
	chkProgram(cntxt->usermodule, mb);
	if(mb->errors) {
		msg = createException(SQL,"cquery.register",SQLSTATE(3F000) "%s", mb->errors);
		FREE_CQ_MB(finish)
	}

	if(!alias || strcmp(alias, str_nil) == 0) { //set the alias
		ralias = GDKstrdup(fname);
	} else {
		ralias = GDKstrdup(alias);
	}
	if(ralias == NULL) {
		CQ_MALLOC_FAIL(finish)
	}
	if ((sym = findSymbol(cntxt->usermodule, "user", fname)) == NULL){ // access the actual procedure body
		msg = createException(SQL,"cquery.register",SQLSTATE(3F000) "Cannot find %s user.%s\n", err_message, fname);
		FREE_CQ_MB(finish)
	}

#ifdef DEBUG_CQUERY
	fprintFunction(stderr, mb, 0, LIST_MAL_ALL);
#endif
	MT_lock_set(&ttrLock);

	if( pnet == 0){
		pnew = (CQnode *) GDKzalloc((INITIAL_MAXCQ) * sizeof(CQnode));
		if( pnew == NULL) {
			CQ_MALLOC_FAIL(unlock)
		}
		pnetLimit = INITIAL_MAXCQ;
		pnet = pnew;
	} else if (pnettop == pnetLimit) {
		pnew = (CQnode *) GDKrealloc(pnet, (pnetLimit+INITIAL_MAXCQ) * sizeof(CQnode));
		if( pnew == NULL) {
			CQ_MALLOC_FAIL(unlock)
		}
		pnetLimit += INITIAL_MAXCQ;
		pnet = pnew;
	}

	idx = CQlocateAlias(ralias);
	if(idx != pnettop && pnet[idx].status != CQDELETE) {
		msg = createException(SQL,"cquery.register",SQLSTATE(3F000) "The continuous %s %s is already registered.\n",
				err_message, ralias);
		FREE_CQ_MB(unlock)
	}
	if((msg = CQanalysis(cntxt, sym->def, pnettop)) != MAL_SUCCEED) {
		FREE_CQ_MB(unlock)
	}
	if(heartbeats != HEARTBEAT_NIL) {
		for(i=0; i < MAXSTREAMS && pnet[pnettop].baskets[i]; i++) {
			if(baskets[pnet[pnettop].baskets[i]].window != DEFAULT_TABLE_WINDOW) {
				msg = createException(SQL, "cquery.register",
									  SQLSTATE(42000) "Heartbeat ignored, a window constraint exists\n");
				FREE_CQ_MB(unlock)
			}
		}
	}

	if((pnet[pnettop].stk = prepareMALstack(mb, mb->vsize)) == NULL) { //prepare MAL stack
		CQ_MALLOC_FAIL(unlock)
	}

	pnet[pnettop].alias = ralias;
	pnet[pnettop].func = found;
	pnet[pnettop].mb = mb;
	pnet[pnettop].cycles = cycles;
	pnet[pnettop].beats = SET_HEARTBEATS(heartbeats);
	//subtract the beats value so the CQ will start at the precise moment
	pnet[pnettop].run = startat - (pnet[pnettop].beats > 0 ? pnet[pnettop].beats : 0);
	pnet[pnettop].seen = *timestamp_nil;
	pnet[pnettop].status = CQWAIT;
	pnet[pnettop].error = MAL_SUCCEED;
	pnet[pnettop].time = 0;
	pnettop++;

unlock:
	if(!msg && cq_pid == 0) { /* start the scheduler if needed */
		if( pnettop == 1)
			pnstatus = CQINIT;
		MT_lock_unset(&ttrLock);
		msg = CQstartScheduler();
	} else {
		MT_lock_unset(&ttrLock);
	}
finish:
	if(freeMB)
		freeMalBlk(be->mb);
	if(sqlquery)
		GDKfree(sqlquery);
	be->mb = prev;
	return msg;
}

str
CQresume(str alias, int with_alter, lng heartbeats, lng startat, int cycles)
{
	str msg = MAL_SUCCEED;
	int idx = 0, j;

#ifdef DEBUG_CQUERY
	fprintf(stderr, "#resume scheduler\n");
#endif

	if(cycles < 0 && cycles != CYCLES_NIL){
		msg = createException(SQL,"cquery.resume",SQLSTATE(42000) "The cycles value must be non negative\n");
		goto finish;
	}
	if(heartbeats < 0 && heartbeats != HEARTBEAT_NIL){
		msg = createException(SQL,"cquery.resume",SQLSTATE(42000) "The heartbeats value must be non negative\n");
		goto finish;
	}
	if(startat < 0){
		msg = createException(SQL,"cquery.resume",SQLSTATE(42000) "The start at value must be non negative\n");
		goto finish;
	}

	MT_lock_set(&ttrLock);

	idx = CQlocateAlias(alias);
	if( idx == pnettop) {
		msg = createException(SQL, "cquery.resume",
							  SQLSTATE(42000) "The continuous query %s has not yet started\n", alias);
		goto unlock;
	}
	if( pnet[idx].status != CQPAUSE && pnet[idx].status != CQERROR) {
		msg = createException(SQL, "cquery.resume",
							  SQLSTATE(42000) "The continuous query %s is already running\n", alias);
		goto unlock;
	}
	if(with_alter && heartbeats != HEARTBEAT_NIL) {
		for(j=0; j < MAXSTREAMS && pnet[idx].baskets[j]; j++) {
			if(baskets[pnet[idx].baskets[j]].window != DEFAULT_TABLE_WINDOW) {
				msg = createException(SQL, "cquery.resume",
									  SQLSTATE(42000) "Heartbeat ignored, a window constraint exists\n");
				goto unlock;
			}
		}
	}

	pnet[idx].status = CQWAIT;
	if(with_alter) {
		pnet[idx].cycles = cycles;
		pnet[idx].beats = SET_HEARTBEATS(heartbeats);
		pnet[idx].run = startat - (pnet[idx].beats > 0 ? pnet[idx].beats : 0);
	}

	/* start the scheduler if needed */
	if(cq_pid == 0) {
		msg = CQstartScheduler();
	}

unlock:
	MT_lock_unset(&ttrLock);
finish:
	return msg;
}

str
CQresumeAll(void)
{
	str msg = MAL_SUCCEED;
	int i;
	//mvc* smvc;
	//ALL_ROOT_CHECK(cntxt, "cquery.resumeall", "RESUME ");

#ifdef DEBUG_CQUERY
	fprintf(stderr, "#resume scheduler\n");
#endif

	MT_lock_set(&ttrLock);
	for(i = 0 ; i < pnettop; i++)
		pnet[i].status = CQWAIT;

	/* start the scheduler if needed */
	if(cq_pid == 0) {
		MT_lock_unset(&ttrLock);
		msg = CQstartScheduler();
	} else {
		MT_lock_unset(&ttrLock);
	}
	return msg;
}

str
CQpause(str alias)
{
	int idx = 0;
	str msg = MAL_SUCCEED, this_alias = NULL;
	MT_Id myID = MT_getpid();

	MT_lock_set(&ttrLock);
	idx = CQlocateAlias(alias);
	if( idx == pnettop) {
		msg = createException(SQL, "cquery.pause",
							  SQLSTATE(42000) "The continuous query %s has not yet started\n", alias);
		goto finish;
	}
	if( pnet[idx].status == CQPAUSE || pnet[idx].status == CQERROR) {
		msg = createException(SQL, "cquery.pause",
							  SQLSTATE(42000) "The continuous query %s is already paused\n", alias);
		goto finish;
	}
	// actually wait if the query was running
	if(myID != cq_pid) {
		this_alias = pnet[idx].alias; //the CQ might get removed during the sleep calls, so we have to make this check
		while( idx < pnettop && this_alias == pnet[idx].alias && pnet[idx].status == CQRUNNING ){
			MT_lock_unset(&ttrLock);
			MT_sleep_ms(5);
			MT_lock_set(&ttrLock);
			if( idx >= pnettop || pnet[idx].status == CQWAIT || pnet[idx].status == CQERROR)
				break;
		}
	}
	if(idx < pnettop && this_alias == pnet[idx].alias && pnet[idx].status != CQERROR) {
		pnet[idx].status = CQPAUSE;
	}

finish:
	MT_lock_unset(&ttrLock);
	return msg;
}

str
CQpauseAll(void)
{
	str msg = MAL_SUCCEED, this_alias = NULL;
	int i;
	MT_Id myID = MT_getpid();
	//mvc* smvc;
	//ALL_ROOT_CHECK(cntxt, "cquery.pauseall", "PAUSE ");

#ifdef DEBUG_CQUERY
	fprintf(stderr, "#pause cqueries\n");
#endif

	MT_lock_set(&ttrLock);
	for(i = 0 ; i < pnettop; i++) {
		if(myID != cq_pid) {
			this_alias = pnet[i].alias;
			while (i < pnettop && this_alias == pnet[i].alias && pnet[i].status == CQRUNNING) {
				MT_lock_unset(&ttrLock);
				MT_sleep_ms(5);
				MT_lock_set(&ttrLock);
				if (i >= pnettop && (pnet[i].status == CQWAIT || pnet[i].status == CQERROR))
					break;
			}
		}
		if(i < pnettop && this_alias == pnet[i].alias && pnet[i].status != CQERROR) {
			pnet[i].status = CQPAUSE;
		}
	}
	MT_lock_unset(&ttrLock);
	return msg;
}

str
CQbeginAt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str alias, msg = MAL_SUCCEED;
	int idx=0, last;
	lng delay;
	(void) cntxt;
	(void) mb;

	MT_lock_set(&ttrLock);
	last = pnettop;
	if( pci->argc >2){
		alias = *getArgReference_str(stk,pci,1);
		idx = CQlocateAlias(alias);
		if( idx == pnettop) {
			msg = createException(SQL,"cquery.begintat",
								  SQLSTATE(42000) "The continuous query %s has not yet started\n",alias);
			goto finish;
		}
		last = idx+1;
		delay = *getArgReference_lng(stk,pci,2);
	} else
		delay = *getArgReference_lng(stk,pci,1);
#ifdef DEBUG_CQUERY
	fprintf(stderr, "#set begin at\n");
#endif
	if(delay < 0){
		msg = createException(SQL,"cquery.begintat",SQLSTATE(42000) "The delay value must be non negative\n");
		goto finish;
	}
	for( ; idx < last; idx++)
		pnet[idx].run = delay - (pnet[idx].beats > 0 ? pnet[idx].beats : 0);

finish:
	MT_lock_unset(&ttrLock);
	return msg;
}

str
CQcycles(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str alias, msg = MAL_SUCCEED;
	int cycles, idx=0, last;
	(void) cntxt;
	(void) mb;

	MT_lock_set(&ttrLock);
	last = pnettop;
	if( pci->argc >2){
		alias = *getArgReference_str(stk,pci,1);
		idx = CQlocateAlias(alias);
		if( idx == pnettop) {
			msg = createException(SQL,"cquery.cycles",
								  SQLSTATE(42000) "The continuous query %s has not yet started\n",alias);
			goto finish;
		}
		last = idx+1;
		cycles = *getArgReference_int(stk,pci,2);
	} else
		cycles = *getArgReference_int(stk,pci,1);
#ifdef DEBUG_CQUERY
	fprintf(stderr, "#set cycles\n");
#endif
	if(cycles < 0 && cycles != CYCLES_NIL){
		msg = createException(SQL,"cquery.cycles",SQLSTATE(42000) "The cycles value must be non negative\n");
		goto finish;
	}
	for( ; idx < last; idx++)
		pnet[idx].cycles = cycles;

finish:
	MT_lock_unset(&ttrLock);
	return msg;
}

str
CQheartbeat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str alias, msg = MAL_SUCCEED;
	int j, there_is_window_constraint, idx=0, last= pnettop;
	lng heartbeats;
	(void) cntxt;
	(void) mb;

	MT_lock_set(&ttrLock);
	if( pci->argc >2){
		alias = *getArgReference_str(stk,pci,1);
		idx = CQlocateAlias(alias);
		if( idx == pnettop) {
			msg = createException(SQL,"cquery.heartbeat",
								  SQLSTATE(42000) "The continuous query %s has not yet started\n",alias);
			goto finish;
		}
		last = idx+1;
		heartbeats = *getArgReference_lng(stk,pci,2);
#ifdef DEBUG_CQUERY
		fprintf(stderr, "#set the heartbeat of %s to %d\n",alias,beats);
#endif
	} else{
		heartbeats = *getArgReference_lng(stk,pci,1);
#ifdef DEBUG_CQUERY
		fprintf(stderr, "#set the heartbeat %d ms\n",beats);
#endif
	}
	if(heartbeats < 0){
		msg = createException(SQL,"cquery.heartbeat",SQLSTATE(42000) "The heartbeats value must be non negative\n");
		goto finish;
	}

	for( ; idx < last; idx++){
		there_is_window_constraint = 0;
		for(j=0; j < MAXSTREAMS && !there_is_window_constraint && pnet[idx].baskets[j]; j++) {
			if(baskets[pnet[idx].baskets[j]].window != DEFAULT_TABLE_WINDOW) {
				there_is_window_constraint = 1;
			}
		}
		if(heartbeats != HEARTBEAT_NIL && there_is_window_constraint) {
			msg = createException(SQL, "cquery.heartbeat",
								  SQLSTATE(42000) "Heartbeat ignored, a window constraint exists\n");
			goto finish;
		}
	}

	for( ; idx < last; idx++){
		lng new_hearbeats = SET_HEARTBEATS(heartbeats);
		if(new_hearbeats > pnet[idx].beats) { //has to do the alignment of the starting point
			pnet[idx].run -= (new_hearbeats - pnet[idx].beats);
		} else {
			pnet[idx].run += (pnet[idx].beats - new_hearbeats);
		}
		pnet[idx].beats = new_hearbeats;
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

/* Remove a specific continuous query from the scheduler */

str
CQderegister(str alias)
{
	int idx = 0;
	str msg = MAL_SUCCEED, this_alias = NULL;
	MT_Id myID = MT_getpid();

	MT_lock_set(&ttrLock);
	idx = CQlocateAlias(alias);
	if(idx == pnettop || pnet[idx].status == CQDELETE) {
		msg = createException(SQL, "cquery.deregister",
							  SQLSTATE(42000) "The continuous query %s has not yet started\n", alias);
		goto unlock;
	}
	if(myID != cq_pid) {
		pnet[idx].status = CQSTOP;
		this_alias = pnet[idx].alias;
		MT_lock_unset(&ttrLock);
		// actually wait if the query was running
		// the CQ might get removed during the sleep calls, so we have to make this check
		while (idx < pnettop && this_alias == pnet[idx].alias && pnet[idx].status != CQDEREGISTER) {
			MT_sleep_ms(5);
		}
		MT_lock_set(&ttrLock);
		if(idx < pnettop && this_alias == pnet[idx].alias) {
			CQfree(idx);
		}
		if( pnettop == 0) {
			pnstatus = CQSTOP;
			MT_lock_unset(&ttrLock);
			if(cq_pid > 0) {
				MT_join_thread(cq_pid);
				cq_pid = 0;
			}
			goto finish;
		}
	} else {
		pnet[idx].status = CQDELETE;
	}
unlock:
	MT_lock_unset(&ttrLock);
finish:
	return msg;
}

str
CQderegisterAll(void)
{
	str msg = MAL_SUCCEED, this_alias = NULL;
	int i;
	MT_Id myID = MT_getpid();
	//mvc* smvc;
	//ALL_ROOT_CHECK(cntxt, "cquery.deregisterall", "STOP ");

	MT_lock_set(&ttrLock);

	for(i = 0 ; i < pnettop; i++) {
		if(myID != cq_pid) {
			pnet[i].status = CQSTOP;
			this_alias = pnet[i].alias;
			MT_lock_unset(&ttrLock);
			// actually wait if the query was running
			while(i < pnettop && this_alias == pnet[i].alias && pnet[i].status != CQDEREGISTER ){
				MT_sleep_ms(5);
			}
			MT_lock_set(&ttrLock);
			if(i < pnettop && this_alias == pnet[i].alias) {
				CQfree(i);
			}
		} else {
			pnet[i].status = CQDELETE;
		}
		i--;
	}
	if(myID != cq_pid)
		pnstatus = CQSTOP;
	MT_lock_unset(&ttrLock);
	if(myID != cq_pid && cq_pid > 0) {
		MT_join_thread(cq_pid);
		cq_pid = 0;
	}
	return msg;
}

str
CQdump(void *ret)
{
	int i, k;

	MT_lock_set(&ttrLock);
	fprintf(stderr, "#scheduler status %s\n", statusname[pnstatus]);
	for (i = 0; i < pnettop; i++) {
		fprintf(stderr, "#[%d]\t%s.%s %s ", i, pnet[i].func->s->base.name, pnet[i].func->base.name, statusname[pnet[i].status]);
		fprintf(stderr, "beats="LLFMT" ", pnet[i].beats);
		fprintf(stderr, "run="LLFMT" ", pnet[i].run);
		fprintf(stderr, "cycles=%d ", pnet[i].cycles);
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
	MT_lock_unset(&ttrLock);
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
	fprintf(stderr, "#cquery.execute %s locked\n",node->alias);
	fprintFunction(stderr, node->mb, 0, LIST_MAL_NAME | LIST_MAL_VALUE | LIST_MAL_MAPI);
#endif

	MT_lock_unset(&ttrLock);
	msg = runMALsequence(cntxt, node->mb, 1, 0, node->stk, 0, 0);
	MT_lock_set(&ttrLock);

	if( msg != MAL_SUCCEED) {
		node->error = msg;
		node->status = CQERROR;
	} else if( node->status != CQSTOP && node->status != CQDELETE)
		node->status = CQWAIT;

#ifdef DEBUG_CQUERY
		fprintf(stderr, "#cquery.execute %s finished %s\n", node->alias, (msg?msg:""));
#endif
}

static void
CQscheduler(void *dummy)
{
	int i, j, k = -1, pntasks, delay = cycleDelay;
	Client cntxt = (Client) dummy;
	str msg = MAL_SUCCEED;
	lng t, now;
	timestamp aux;
	int claimed[MAXSTREAMS];
	BAT *b;

#ifdef DEBUG_CQUERY
	fprintf(stderr, "#cquery.scheduler started\n");
#endif

	MT_lock_set(&ttrLock);
	pnstatus = CQRUNNING; // global state

	while( pnstatus != CQSTOP  && ! GDKexiting()){
		/* Determine which continuous queries are eligible to run.
		   Collect latest statistics, note that we don't need a lock here,
		   because the count need not be accurate to the usec. It will simply
		   come back. We also only have to check the places that are marked
		   non empty. You can only trigger on empty baskets using a heartbeat */
		memset((void*) claimed, 0, sizeof(claimed));

		if((msg = MTIMEcurrent_timestamp(&aux)) != MAL_SUCCEED) {
			fprintf(stderr, "CQscheduler internal error: %s\n", msg);
			GDKfree(msg);
		}
		if((msg = MTIMEepoch2lng(&now, &aux)) != MAL_SUCCEED) {
			fprintf(stderr, "CQscheduler internal error: %s\n", msg);
			GDKfree(msg);
		}

		pntasks=0;
		for (k = i = 0; i < pnettop; i++) {
			if ( pnet[i].status == CQWAIT ){
				pnet[i].enabled = pnet[i].error == 0 && (pnet[i].cycles > 0 || pnet[i].cycles == CYCLES_NIL);
				/* Queries are triggered by the heartbeat or  all window constraints */
				/* A heartbeat in combination with a window constraint is ambiguous */
				/* At least one constraint should be set */
				if( pnet[i].beats == HEARTBEAT_NIL && pnet[i].baskets[0] == 0)
					pnet[i].enabled = 0;
				if( pnet[i].enabled && ((pnet[i].beats != HEARTBEAT_NIL && pnet[i].beats > 0) || pnet[i].run > 0)) {
					pnet[i].enabled = now >= pnet[i].run + (pnet[i].beats > 0 ? pnet[i].beats : 0);
#ifdef DEBUG_CQUERY_SCHEDULER
					fprintf(stderr,"#beat %s  "LLFMT"("LLFMT") %s\n", pnet[i].alias,
					pnet[i].run + (pnet[i].beats > 0 ? pnet[i].beats : 0), now, (pnet[i].enabled? "enabled":"disabled"));
#endif
				}
				/* check if all input baskets are available */
				for (j = 0; pnet[i].enabled && pnet[i].baskets[j] && (b = baskets[pnet[i].baskets[j]].bats[0]); j++)
					/* consider execution only if baskets are properly filled */
					if ( pnet[i].inout[j] == STREAM_IN && (BUN) baskets[pnet[i].baskets[j]].window > BATcount(b)){
						pnet[i].enabled = 0;
						break;
					}
				/* check availability of all stream baskets */
				for (j = 0; pnet[i].enabled && pnet[i].baskets[j]; j++){
					for(k=0; claimed[k]; k++)
						if(claimed[k]  ==  pnet[i].baskets[j]){
							pnet[i].enabled = 0;
#ifdef DEBUG_CQUERY_SCHEDULER
						fprintf(stderr, "#cquery: %s,disgarded\n", pnet[i].alias);
#endif
							break;
						}
					if (pnet[i].enabled && claimed[k] == 0)
						claimed[k] =  pnet[i].baskets[j];
				}
#ifdef DEBUG_CQUERY_SCHEDULER
				if( pnet[i].enabled)
					fprintf(stderr, "#cquery: %s enabled\n", pnet[i].alias);
#endif
				pntasks += pnet[i].enabled;
			} else if( pnet[i].status == CQSTOP) {
				pnet[i].status = CQDEREGISTER;
				pnet[i].enabled = 0;
			} else {
				pnet[i].enabled = 0;
			}
		}
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
			delay = cycleDelay;
#ifdef DEBUG_CQUERY
			fprintf(stderr, "#Run transition %s cycle=%d\n", pnet[i].alias, pnet[i].cycles);
#endif
			t = GDKusec();
			pnet[i].status = CQRUNNING;
			if( !GDKexiting())
				CQexecute(cntxt, i);
/*
				if (MT_create_thread(&pnet[i].tid, CQexecute, (void*) (pnet+i), MT_THR_JOINABLE) < 0){
					msg= createException(MAL,"petrinet.scheduler","Can not fork the thread");
				} else
*/
			if( pnet[i].cycles != CYCLES_NIL && pnet[i].cycles > 0) {
				pnet[i].cycles--;
				if(pnet[i].cycles == 0) { //if it was the last cycle of the CQ, remove it
					CQfree(i);
					i--;
					continue; //an entry was deleted, so jump over!
				}
			}
			pnet[i].run = now;				/* last executed */
			pnet[i].time = GDKusec() - t;   /* keep around in microseconds */
			(void) MTIMEcurrent_timestamp(&pnet[i].seen);
			pnet[i].enabled = 0;
			CQentry(i);
		}
		for(i = pnettop - 1; i >= 0; i--) { //more defensive way to stop continuous queries from the scheduler itself
			if( pnet[i].status == CQDELETE){
				CQfree(i);
			}
		}
		if( pnettop == 0)
			pnstatus = CQSTOP;
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
		if ((pntasks == 0 && pnstatus != CQSTOP) || pnstatus == CQPAUSE) {
#ifdef DEBUG_CQUERY
			fprintf(stderr, "#cquery.scheduler paused\n");
#endif
			MT_lock_unset(&ttrLock);
			MT_sleep_ms(delay);
			if( delay < 20 * cycleDelay)
				delay = (int) (delay *1.2);
			MT_lock_set(&ttrLock);
		}
	}
#ifdef DEBUG_CQUERY
	fprintf(stderr, "#cquery.scheduler stopped\n");
#endif
	pnstatus = CQINIT;
	cq_pid = 0;
	SQLexitClient(cntxt);
	MCcloseClient(cntxt, CQ_CLIENT);
	MT_lock_unset(&ttrLock);
}

str
CQstartScheduler(void)
{
	Client cntxt;
	stream *fin, *fout;
	bstream *bin;
	char* dbpath = GDKgetenv("gdk_dbpath"), *location;
	const char* fpsin = "fin_petri_sched";
	const char* fpsout = "fout_petri_sched";

#ifdef DEBUG_CQUERY
	fprintf(stderr, "#Start CQscheduler\n");
#endif

	if(!dbpath)
		throw(MAL, "cquery.startScheduler",SQLSTATE(42000) "The gdk_dbpath environment variable is not set\n");

	location = GDKmalloc(strlen(dbpath) + strlen(DIR_SEP_STR) + strlen(fpsin) + 1);
	if( location == NULL)
		throw(MAL, "cquery.startScheduler",SQLSTATE(HY001) MAL_MALLOC_FAIL);
	sprintf(location, "%s%s%s", dbpath, DIR_SEP_STR, fpsin);
	fin = open_rastream_and_create(location);
	GDKfree(location);
	if( fin == NULL)
		throw(MAL, "cquery.startScheduler",SQLSTATE(HY001) "Could not initialize CQscheduler\n");

	location = GDKmalloc(strlen(dbpath) + strlen(DIR_SEP_STR) + strlen(fpsout) + 1);
	if( location == NULL)
		throw(MAL, "cquery.startScheduler",SQLSTATE(HY001) MAL_MALLOC_FAIL);
	sprintf(location, "%s%s%s", dbpath, DIR_SEP_STR, fpsout);
	fout = open_wastream(location);
	GDKfree(location);
	if( fout == NULL) {
		mnstr_destroy(fin);
		throw(MAL, "cquery.startScheduler",SQLSTATE(HY001) "Could not initialize CQscheduler\n");
	}

	bin = bstream_create(fin,0);
	if( bin == NULL) {
		mnstr_destroy(fin);
		mnstr_destroy(fout);
		throw(MAL, "cquery.startScheduler",SQLSTATE(HY001) "Could not initialize CQscheduler\n");
	}

	cntxt = MCinitClient(0,bin,fout);
	if( cntxt == NULL) {
		bstream_destroy(cntxt->fdin);
		mnstr_destroy(cntxt->fdout);
		throw(MAL, "cquery.startScheduler",SQLSTATE(HY001) "Could not initialize CQscheduler\n");
	}

	cntxt->curmodule = cntxt->usermodule = userModule();
	if( cntxt->curmodule == NULL) {
		MCcloseClient(cntxt, CQ_CLIENT);
		throw(MAL, "cquery.startScheduler",SQLSTATE(HY001) "Could not initialize CQscheduler\n");
	}

	if( SQLinitClient(cntxt) != MAL_SUCCEED) {
		MCcloseClient(cntxt, CQ_CLIENT);
		throw(MAL, "cquery.startScheduler",SQLSTATE(HY001) "Could not initialize SQL context in CQscheduler\n");
	}

	if (pnstatus== CQINIT && MT_create_thread(&cq_pid, CQscheduler, (void*) cntxt, MT_THR_JOINABLE) != 0){
#ifdef DEBUG_CQUERY
		fprintf(stderr, "#Start CQscheduler failed\n");
#endif
		SQLexitClient(cntxt);
		MCcloseClient(cntxt, CQ_CLIENT);
		throw(MAL, "cquery.startScheduler",SQLSTATE(HY001) "Could not initialize client thread in CQscheduler\n");
	}
	return MAL_SUCCEED;
}

void
CQreset(void)
{
	if(pnet) {
		CQderegisterAll(); //stop all continuous queries
		GDKfree(pnet);
	}
	pnet = NULL;
	MT_lock_destroy(&ttrLock);
	(void) BSKTshutdown(); //this must be last!!
}

str
CQprelude(void *ret)
{
	(void) ret;
	MT_lock_init(&ttrLock, "cqueryLock");
	pnet = (CQnode *) GDKzalloc(INITIAL_MAXCQ * sizeof(CQnode));
	pnetLimit = INITIAL_MAXCQ;
	pnettop = 0;
	if(pnet == NULL)
		throw(MAL, "cquery.prelude",SQLSTATE(HY001) MAL_MALLOC_FAIL);
	cqfix_set(CQreset);
	printf("# MonetDB/Timetrails module loaded\n");
	return MAL_SUCCEED;
}
