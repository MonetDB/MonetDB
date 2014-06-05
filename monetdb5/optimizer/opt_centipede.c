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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * M. Kersten
 * Centipede
 * Documentation in accompanying paper.
 */
#include "monetdb_config.h"
#include "opt_centipede.h"
#include "opt_deadcode.h"
#include "opt_aliases.h"
#include "opt_garbageCollector.h"
#include "mal_builder.h"
#include "mal_recycle.h"
#include "mal_interpreter.h"
#include "algebra.h"

#define DEBUG_OPT_DETAIL
#define _DEBUG_OPT_CENTIPEDE_ 

#define BLOCKED 1	// Instruction should remain in main routine
#define PARTITION 2	// Instruction is part of the fragment routine
#define PIVOT    3	// Instruction is part of the consolidation routine
#define SUPPORTIVE 4// Instruction is part of fragment routine

/*
 * The columns are broken using fixed OID ranges.
 * Currently, we assume that all slices are of equal length.
 * The target instruction around which the query is broken.
 */
typedef	struct{
	InstrPtr target;
	str schema, table, column;
	int type, slice;	
	int lslices, hslices;  /* variables holding the range bound */
	lng rowcnt;
} Slices;

static int nrservers;

/*
 * The query will be controlled from the coordinator with a plan
 * geared at parallel execution 
*/
static MalBlkPtr
OPTexecController(Client cntxt, MalBlkPtr mb, MalBlkPtr pmb, InstrPtr ret, InstrPtr packs, Slices *slices, oid plantag, int *status)
{
	MalBlkPtr cmb;
	Symbol s;
	char nme[BUFSIZ], *plan, *stub;
	int barrier, x, i, j, k, *alias, nrpack;
	InstrPtr p=0, q, *pack;

	(void) p;
	/* define the query controller */
	//snprintf(nme, BUFSIZ, "%s_plan"OIDFMT, getFunctionId( getInstrPtr(mb,0)), plantag);
	//putName(nme, strlen(nme));
	plan = getFunctionId(getInstrPtr(pmb,0));
	snprintf(nme, BUFSIZ, "%s_stub"OIDFMT, getFunctionId( getInstrPtr(mb,0)), plantag);
	stub = putName(nme, strlen(nme));
	(void) stub;				/* only used if REMOTE_EXECUTION defined */

	snprintf(nme,BUFSIZ,"%s_cntrl"OIDFMT,getFunctionId( getInstrPtr(mb,0)), plantag);
	s = newFunction(userRef, putName(nme, strlen(nme)),FUNCTIONsymbol);
	if ( s == NULL)
		return 0;
	freeMalBlk(s->def);
	s->def = copyMalBlk(pmb);	/* get variables */
	cmb = s->def;
	if ( newMalBlkStmt(cmb,cmb->ssize) < 0 )
		return 0;
	nrpack= getInstrPtr(pmb,0)->retc;
	pack = (InstrPtr *) GDKzalloc(sizeof(InstrPtr) * nrpack);
	if( pack == NULL){
		GDKerror("centipede" MAL_MALLOC_FAIL);
		mb->errors++;
		return mb;
	}

	pushInstruction(cmb, copyInstruction(pmb->stmt[0]));
	getFunctionId( getInstrPtr(cmb,0)) = putName(nme,strlen(nme));
	insertSymbol(cntxt->nspace,s);

	/* initialize binds */
	q= newFcnCall(cmb, sqlRef, mvcRef);
	x= getArg(q,0);
	alias = (int*) GDKzalloc(nrservers * sizeof(int));
	if (alias == NULL){
		GDKerror("centipede" MAL_MALLOC_FAIL);
		GDKfree(pack);
		mb->errors++;
		return mb;
	}
	if( slices->column) {
		q= newInstruction(cmb, ASSIGNsymbol);
		getModuleId(q) = sqlRef;
		getFunctionId(q) = bindRef;
		q = pushArgument(cmb,q,x);
		j = getArg(q,0) = newTmpVariable(cmb,newBatType(TYPE_oid, slices->type));
		setVarUDFtype(cmb,j);
		setVarFixed(cmb,j);
		q= pushStr(cmb,q, slices->schema);
		q= pushStr(cmb,q, slices->table);
		q= pushStr(cmb,q, slices->column);
		q= pushInt(cmb,q, 0);
		pushInstruction(cmb,q);

		q= newInstruction(cmb, ASSIGNsymbol);
		getModuleId(q) = centipedeRef;
		getFunctionId(q) = vectorRef;
		q = pushArgument(cmb, q, j);
		for ( j = 0; j < nrservers +1; j++) {
			k = alias[j] = newTmpVariable(cmb, TYPE_oid);
			q= pushReturn(cmb,q, k);
		}
		pushInstruction(cmb,q);
	}
	/* pack[i] := mat.pack(x1,...xn) */
	if ( slices->column) {
		p = getInstrPtr(pmb,0);
#ifdef _DEBUG_OPT_CENTIPEDE_
	//mnstr_printf(cntxt->fdout,"#matpack plan \n");
	//printInstruction(cntxt->fdout, pmb,0,p,LIST_MAL_STMT);
	//printInstruction(cntxt->fdout, cmb,0,getInstrPtr(cmb,0),LIST_MAL_STMT);
#endif
		for ( k=0  ;k < nrpack ; k++)
		{
			pack[k] = newInstruction(cmb,ASSIGNsymbol);
			getModuleId(pack[k]) = matRef;
			getFunctionId(pack[k]) = packRef;
			getArg(pack[k],0) = getArg(packs,k); 
			assert(packs->argv[k] >=0);
		}
	}

#ifdef REMOTE_EXECUTION
	q= newFcnCall(cmb,schedulerRef,srvpoolRef);
#else
	q= newFcnCall(cmb,languageRef,dataflowRef);
#endif
	q->barrier= BARRIERsymbol;
	barrier = getArg(q,0);
	setVarType(cmb,x,TYPE_int);

#ifdef REMOTE_EXECUTION
	/* get servers to execute the query */
	q= newStmt(cmb,srvpoolRef,putName("query",5));
	q->retc= q->argc = 0;
	for( i = 0; i < nrservers; i++)
		q= pushReturn(cmb,q, newTmpVariable(cmb,TYPE_str));
	q= pushStr(cmb,q,plan);
	p= q;
#endif

	/* Inject the calls to the individual sub plans */
	for ( i = 0; i < nrservers ; i++) {
		q= copyInstruction(getInstrPtr(pmb,0));
		q->token = ASSIGNsymbol;
		q->barrier = 0;
		q->argc -= 2; /* remove the bounds */
		for ( j=0 ; j < q->retc; j++) {
			getArg(q,j) = newTmpVariable(cmb, getVarType(pmb,getArg(q,j)));
			pack[j] = pushArgument(cmb, pack[j], getArg(q,j));
		}

		if ( slices->column){
			/* add the splitter arguments */
			q= pushArgument(cmb,q,alias[i]);
			q= pushArgument(cmb,q,alias[i+1]);
		}
#ifdef REMOTE_EXECUTION
		/* for distributed execution we use the stub */
		getFunctionId(q) = stub;
		q= setArgument(cmb,q,q->retc, getArg(p,i));
#else
		getFunctionId(q) = plan;
#endif
		pushInstruction(cmb,q);
	}

	/* put all mat.pack instructions into the program
	  make sure that they have contiguous void headed columns 
	*/
	p = getInstrPtr(pmb,0);
	if ( slices->column) {
		for ( k=0 ; k < nrpack; k++) {
			/* after packing we may have to re-do groupings*/
			pushInstruction(cmb, pack[k]);
			setVarUsed(cmb, getArg(pack[k],0));
		}
	}
#ifdef _DEBUG_OPT_CENTIPEDE_
	mnstr_printf(cntxt->fdout,"\n#cmb structure\n");
	printFunction(cntxt->fdout, cmb, 0, LIST_MAL_STMT);
#endif

	(void) status;
	/* look for pivot operations in original plan */
	for ( i=1; i < mb->stop; i++)
	if (status[i] == PIVOT){
		char buf[BUFSIZ];
		q= copyInstruction(getInstrPtr(mb,i));
#ifdef _DEBUG_OPT_CENTIPEDE_
		mnstr_printf(cntxt->fdout,"#cmb include stmt %d status %d:",i,status[i]);
		printInstruction(cntxt->fdout, mb, 0, q,LIST_MAL_STMT);
		for(k=0; k<q->argc;k++)
			assert(getArg(q,k) >=0);
#endif
		if (getModuleId(q) == groupRef && (getFunctionId(q) == subgroupRef || getFunctionId(q) == subgroupdoneRef)){
			snprintf(buf,BUFSIZ,"Y_%d",getArg(q,q->retc));
			q= copyInstruction(q);
			k = findVariable(cmb,buf);
			assert(k >=0);
			if ( k == -1)
				getArg(q,q->retc) = newVariable(cmb,GDKstrdup(buf),TYPE_any);
			else getArg(q,q->retc) = k;
			pushInstruction(cmb,q);
		} else
		if (getModuleId(q) == aggrRef && getFunctionId(q) == countRef ){
			q= copyInstruction(q);
			getFunctionId(q) = sumRef;
			// correct the return statement
			setVarType(cmb, getArg(q,1), newBatType(TYPE_oid, TYPE_wrd));
			pushInstruction(cmb,q);
		} else
		if (getModuleId(q) == aggrRef && getFunctionId(q) == subcountRef ){
			q= copyInstruction(q);
			getFunctionId(q) = subsumRef;
			q= pushBit(cmb,q,1);
			getArg(q,1) = getArg(q,0);
			pushInstruction(cmb,q);
		} else
		if (getModuleId(q) == aggrRef && (getFunctionId(q)==subsumRef || getFunctionId(q) == subminRef ||
			getFunctionId(q) == submaxRef || getFunctionId(q) == subavgRef)){
			q= copyInstruction(q);
			getArg(q,1) = getArg(q,0);
			pushInstruction(cmb,q);
		} else
		if (getModuleId(q) == algebraRef && getFunctionId(q) == leftfetchjoinRef ){
			snprintf(buf,BUFSIZ,"Y_%d",getArg(q,q->argc-1));
			q= copyInstruction(q);
			k = findVariable(cmb,buf);
			if ( k >=0)
				getArg(q,q->argc-1) = k;
			pushInstruction(cmb,q);
		} else{
			q= copyInstruction(q);
			pushInstruction(cmb,q);
		}
	}
	/* finalize the dataflow block */
	q= newAssignment(cmb);
	q->barrier = EXITsymbol;
	getArg(q,0) = barrier;


	/* consolidate the result of the control function */
	ret = copyInstruction(ret);
	clrFunction(ret);
	ret->barrier = RETURNsymbol;
	for( i=0; i< ret->retc; i++)
		ret= pushArgument(mb,ret,getArg(ret,i));
	pushInstruction(cmb,ret);
	getInstrPtr(cmb,0)->argc-= 2; // remove the bounds

	// fix the calling of the cntrl function
	while ( cmb->stmt[0]->retc )
		delArgument(cmb->stmt[0],0);
	for( i =0; i< ret->retc; i++) 
		cmb->stmt[0]= pushReturn(cmb, cmb->stmt[0], getArg(ret,i));
	pushEndInstruction(cmb);
#ifdef _DEBUG_OPT_CENTIPEDE_
	mnstr_printf(cntxt->fdout,"#pmb stmt ");
	printInstruction(cntxt->fdout, pmb, 0, getInstrPtr(pmb,0),LIST_MAL_STMT);
	mnstr_printf(cntxt->fdout,"\n#cmb stmt\n");
	printInstruction(cntxt->fdout,cmb, 0, getInstrPtr(cmb,0),LIST_MAL_STMT);
	//mnstr_printf(cntxt->fdout,"#rough cntrl plan %d \n", cmb->errors);
	//printFunction(cntxt->fdout, cmb, 0, LIST_MAL_STMT);
#endif

	chkProgram(cntxt->fdout, cntxt->nspace, cmb);
#ifdef _DEBUG_OPT_CENTIPEDE_
	//mnstr_printf(cntxt->fdout,"#cntrl plan %d \n", cmb->errors);
	//printFunction(cntxt->fdout, cmb, 0, LIST_MAL_STMT);
#endif
	GDKfree(alias);
	GDKfree(pack);

	return cmb;
}

static MalBlkPtr
OPTplanStub(Client cntxt, MalBlkPtr mb, MalBlkPtr pmb, oid plantag)
{
	MalBlkPtr smb = 0;
	Symbol s;
	InstrPtr sig, q, ret;
	int j,k,conn, *arg;
	char nme[BUFSIZ];

	/* define the sub query stub for remote processing */
	snprintf(nme,BUFSIZ,"%s_stub"OIDFMT,getFunctionId( getInstrPtr(mb,0)), plantag);
	s = newFunction(userRef, putName(nme, strlen(nme)),FUNCTIONsymbol);
	if ( s == NULL)
		return 0;
	freeMalBlk(s->def);
	s->def = copyMalBlk(pmb);	/* get variables */
	smb = s->def;
	if ( newMalBlkStmt(smb,smb->ssize) < 0 )
		return 0;
	pushInstruction(smb, copyInstruction(pmb->stmt[0]));
	getFunctionId( getInstrPtr(smb,0)) = putName(nme,strlen(nme));
	insertSymbol(cntxt->nspace,s);

	conn = newTmpVariable(smb, TYPE_str);
	setArgument(smb,getInstrPtr(smb,0), getInstrPtr(smb,0)->retc, conn);
	/* conn = getArg(q,0);*/

	sig = getInstrPtr(smb,0);
	arg = (int*) GDKzalloc(sizeof(int) * sig->argc);
	if( arg == NULL){
		GDKerror("centipede" MAL_MALLOC_FAIL);
	}
	/* k:= remote.put(conn,kvar) */
	for (j= sig->retc+1; j < sig->argc; j++) {
		q= newFcnCall(smb,remoteRef,putRef);
		setVarType(smb, getArg(q,0), TYPE_str);
		setVarUDFtype(smb, getArg(q,0));
		q= pushArgument(smb,q,conn);
		q= pushArgument(smb,q,getArg(sig,j));
		arg[j]= getArg(q,0);
	}
	for (j= 0; j < sig->retc; j++) {
		q= newFcnCall(smb,remoteRef,putRef);
		setVarType(smb, getArg(q,0), TYPE_str);
		setVarUDFtype(smb, getArg(q,0));
		q= pushArgument(smb,q,conn);
		q= pushArgument(smb,q,getArg(sig,j));
		arg[j]= getArg(q,0);
	}

	/* (k1,...kn):= remote.exec(conn,srvpool,qry,version....) */
	snprintf(nme, BUFSIZ, "%s_plan"OIDFMT, getFunctionId( getInstrPtr(mb,0)), plantag);
	q = newInstruction(smb,ASSIGNsymbol);
	getModuleId(q) = remoteRef;
	getFunctionId(q) = execRef;
	q->retc=  q->argc= 0;
	for (j=0; j < sig->retc; j++)
		q = pushReturn(smb,q,arg[j]);
	q= pushArgument(smb,q,conn);
	q= pushStr(smb,q,userRef);
	q= pushStr(smb,q,putName(nme,strlen(nme)));
	/* deal with all arguments ! */
	for (j=sig->retc+1; j < sig->argc; j++)
		q = pushArgument(smb,q,arg[j]);
	pushInstruction(smb,q);

	/* return exec_qry; */
	ret = newInstruction(smb, ASSIGNsymbol);
	ret->barrier= RETURNsymbol;
	ret->argc = ret->retc = 0;
	/* l:=remote.get(conn,k) */
	for ( j=0; j< sig->retc; j++){
		q= newFcnCall(smb,remoteRef,getRef);
		q= pushArgument(smb,q,conn);
		q= pushArgument(smb,q,arg[j]);
		k= getArg(q,0);
		setVarType(smb,k, getArgType(smb,sig,j));
		setVarUDFtype(smb, k);
		ret = pushArgument(smb,ret,k);
		ret = pushReturn(smb,ret,getArg(sig,j));
	}

/*
    newCatchStmt(smb, "ANYexception");
    q = newStmt(smb, remoteRef, disconnectRef);
    pushArgument(smb, q, conn);
    newRaiseStmt(smb, "ANYexception");   
    newExitStmt(smb, "ANYexception");

    q = newStmt(smb, remoteRef, disconnectRef);
    pushArgument(smb, q, conn);
*/
	if ( sig->retc)
		pushInstruction(smb,ret);
    pushEndInstruction(smb);

	GDKfree(arg);
	return smb;
}


/* derive an OID partition from a bind column */
/* it leads to horizontal fragmentation by OID ranges */
static void
OPTmaterializePartition(MalBlkPtr mb, InstrPtr p, int low, int hgh)
{
	int v,oldvar;
	int i;
	InstrPtr q;

	for(i=0; i< p->retc; i++ ){
		oldvar = getArg(p,i);
		getArg(p,i) = v = newTmpVariable(mb, getVarType(mb,oldvar));
		setVarUDFtype(mb, v);
		setVarFixed(mb,v);

		q = newStmt(mb, algebraRef, sliceRef);
		q = pushArgument(mb, q, v);
		q = pushArgument(mb, q, low);
		q = pushArgument(mb, q, hgh);
		getArg(q,0)= oldvar;
	}
}


/*
 * For bind instructions we have to inject the horizontal slicing action
*/
static int 
OPTsliceColumn(Client cntxt, MalBlkPtr nmb, MalBlkPtr mb, InstrPtr p, Slices *slices)
{
	int parallel = 0;

	(void) cntxt;
	if ( ! (getModuleId(p) == sqlRef && getFunctionId(p) == bindRef )  &&
		 ! (getModuleId(p) == sqlRef && getFunctionId(p) == bindidxRef ) ) {
		pushInstruction(nmb,p);
		return 0;
	}
	if ( ! (strcmp(slices->schema, getVarConstant(mb, getArg(p,p->retc + 1)).val.sval) == 0 &&
		strcmp(slices->table, getVarConstant(mb, getArg(p,p->retc + 2 )).val.sval) == 0) ) {
		pushInstruction(nmb,p);
		return 0;
	}

	if ( slices->slice == 0){
		slices->slice = newTmpVariable(nmb, slices->type);
		setVarUDFtype(nmb, slices->slice);
		setVarUsed(nmb, slices->slice);
		nmb->stmt[0] = pushArgument(nmb, nmb->stmt[0], slices->lslices);
		nmb->stmt[0] = pushArgument(nmb, nmb->stmt[0], slices->hslices);
		parallel=2;	/* return number of arguments added for later disposal */
	} 
	pushInstruction(nmb,p);
	/* prepare access to partitions by injection of the materialize instructions */
	OPTmaterializePartition(nmb, p, slices->lslices, slices->hslices);
	return parallel;
} 

/* 
 * The plan is analysed for the maximal subplan that involves a partitioned table
 * and that does not require data exchanges.
 * Algebraic operators that can be executed on fragments are delegated too.
 * For example join(A,B) where A is fragmented and B is not can be done elsewhere.
 * In all cases we should ensure that the result of the remote execution can be
 * simply unioned together.
 * All plans should be uniquely tagged, because we have to avoid potential conflicts
 * when a connection is re-used by different client sessions.
*/

static void addvartolist(MalBlkPtr mb, InstrPtr *pq, int arg)
{
	int k;
	InstrPtr p = *pq;

	for ( k = 0; k < p->retc; k++)
	if (getArg(p,k) == arg)
		break;
	if ( k == p->retc){
		p = pushReturn(mb, p, arg);
		//p = pushArgument(mb, p, arg);
	}
	*pq = p;
}

#ifdef _DEBUG_OPT_CENTIPEDE_ 
static char *statusname[7]= {"", "blocked   ", "partition ", "pivot     ", "support   ", "exported ", "keeplocal "};
#endif

static void 
OPTbakePlans(Client cntxt, MalBlkPtr mb, Slices *slices)
{
	int *status,*vars;
	int i, j, k, limit, last;
	InstrPtr cntrlreturn, planreturn, orig, packs= 0, call, q = NULL, p = NULL, *old;
	Symbol s;
	MalBlkPtr plan, cntrl, stub;
	str msg= MAL_SUCCEED;
	char nme[BUFSIZ];
	oid plantag;

	status = GDKzalloc(mb->ssize * sizeof(int));
	if( status == 0)
		return;
	vars = GDKzalloc(mb->vsize * sizeof(int));
	if( vars == 0){
		GDKfree(status);
		return;
	}

	plantag= OIDnew(1);
	snprintf(nme,BUFSIZ,"%s_plan"OIDFMT,getFunctionId( getInstrPtr(mb,0)), plantag);
	s = newFunction(userRef, putName(nme, strlen(nme)),FUNCTIONsymbol);
	if ( s == NULL){
		GDKfree(status);
		GDKfree(vars);
		return;
	}
	freeMalBlk(s->def);
	s->def = copyMalBlk(mb);
	plan = s->def;

	limit = plan->stop;
	old = plan->stmt;
	if ( newMalBlkStmt(plan,plan->ssize) < 0 ){
		GDKfree(status);
		GDKfree(vars);
		return;
	}

#ifdef _DEBUG_OPT_CENTIPEDE_
	mnstr_printf(cntxt->fdout,"#Remote plan framework\n");
	mnstr_printf(cntxt->fdout,"#partition %s.%s.%s type %d\n",
		slices->schema,
		slices->table,
		(slices->column ? slices->column: ""),
		slices->type);
#else
	(void) msg;	/* only used when _DEBUG_OPT_CENTIPEDE_ is defined */
	(void) slices;
#endif
#define OIDS 1
#define VALS 2

	/* Phase 1: determine all variables/instructions indirectly dependent on a fragmented column */
	/* Instructions are marked as PARTITION if the have to be propagated */
	last = limit;
	status[0]= PARTITION;
	for ( j = old[0]->retc; j < old[0]->argc; j++)
		vars[getArg(old[0],j)]= SUPPORTIVE;

	for ( i = 1; i < limit ; i++) {
		p = old[i];
		if ( p->token == ENDsymbol || i > last) {
			status[i] = PARTITION;
			last = i;
		} else
		// incorporate both single/double target sql.bind operations
		if ( getModuleId(p) == sqlRef && 
			( getFunctionId(p) == tidRef  || getFunctionId(p) == bindRef || getFunctionId(p) == bindidxRef) &&
			strcmp(slices->schema, getVarConstant(mb, getArg(p, p->retc + 1)).val.sval) == 0 &&
			strcmp(slices->table, getVarConstant(mb, getArg(p, p->retc + 2)).val.sval) == 0 ) {
				status[i] = PARTITION;
				vars[getArg(p,0)] = PARTITION;
		} else
		if ( getModuleId(p) == sqlRef && getFunctionId(p) == deltaRef ){
			if ( vars[getArg(p,1)] == PARTITION ){
				status[i] = PARTITION;
				vars[getArg(p,0)] = PARTITION;
			}
		}  

		/* blocking instructions are those that require data exchange, aggregation or total view */
		if ( getModuleId(p) == algebraRef && (getFunctionId(p) == joinRef || getFunctionId(p) == leftjoinRef || getFunctionId(p) == leftfetchjoinRef) ) {
			if ( vars[getArg(p,p->retc)] == PARTITION ){
				status[i] = PARTITION;
				vars[getArg(p,0)] = PARTITION;
			} 
			if ( vars[getArg(p,p->retc)] == PIVOT ){
				status[i] = PIVOT;
				vars[getArg(p,0)] = PIVOT;
			} 
		} else
		if ( getModuleId(p) == algebraRef && (getFunctionId(p)== thetaselectRef  || getFunctionId(p) == selectRef || getFunctionId(p) == subselectRef ||getFunctionId(p) == thetasubselectRef || getFunctionId(p) == likesubselectRef)){
			if ( vars[getArg(p,p->retc)] == PARTITION ){
				status[i] = PARTITION;
				vars[getArg(p,0)] = PARTITION;
			}
		} else
		if (    getModuleId(p) == batRef && getFunctionId(p) == mirrorRef )  {
			if ( vars[getArg(p,p->retc)] == PARTITION ){
				status[i] = PARTITION;
				vars[getArg(p,0)] = PARTITION;
			}
		} else
		if (    getModuleId(p) == batRef && getFunctionId(p)==reverseRef )  {
			if ( vars[getArg(p,p->retc)] == PARTITION ){
				status[i] = PARTITION;
				vars[getArg(p,0)] = PARTITION;
			}
		} else
		if ( getModuleId(p) == aggrRef  && getFunctionId(p) == countRef) {
			if( vars[getArg(p,p->retc)] == PARTITION ) {
				vars[getArg(p,0)] = PIVOT;
				status[i]= PIVOT;
			}
		} else
		if ( getModuleId(p) == aggrRef && (getFunctionId(p) == subcountRef || getFunctionId(p) == subsumRef ||
			getFunctionId(p) == subminRef || getFunctionId(p) == submaxRef || getFunctionId(p) == subavgRef )){
			if (vars[getArg(p,p->retc)] == PARTITION || vars[getArg(p,p->retc)] == PIVOT ){
				status[i] = PIVOT;
				for(j = 0; j < p->argc; j++)
					vars[getArg(p,j)] = PIVOT;
			}
		} else
		if ( getModuleId(p) == groupRef && ( getFunctionId(p) == subgroupRef || getFunctionId(p) == subgroupdoneRef) && p->retc== 3){
			if ( vars[getArg(p,p->retc)]== PARTITION ){
				status[i] = PIVOT;
				for(j = 0; j < p->retc; j++)
					vars[getArg(p,j)] = PIVOT;
			}
		} else
		if ((getModuleId(p) == sqlRef && (getFunctionId(p) == resultSetRef || getFunctionId(p) == putName("exportValue",11))) || getModuleId(p) == ioRef ){
			status[i] = BLOCKED;
			vars[getArg(p,0)] = BLOCKED;
		} else 
		if ( getModuleId(p) == batcalcRef ){
			if ( vars[getArg(p,p->retc)] == PARTITION || vars[getArg(p,p->retc+1)] == PARTITION ){
				status[i] = PARTITION;
				vars[getArg(p,0)] = PARTITION;
			}
		} else 
		if ( getModuleId(p) == algebraRef && getFunctionId(p) == subsliceRef ){
			if ( vars[getArg(p,1)] == PARTITION ){
				status[i] = BLOCKED;
				vars[getArg(p,0)] = BLOCKED;
			}
		} else
		if ( getModuleId(p) == pqueueRef && getFunctionId(p) == utopn_minRef ){
			if ( vars[getArg(p,1)] == PARTITION ){
				status[i] = PIVOT;
				vars[getArg(p,0)] = PIVOT;
			}
		} else
		if ( getModuleId(p) == algebraRef && (getFunctionId(p) == subsortRef || getFunctionId(p) == sortRef || getFunctionId(p)== tinterRef) ) {
			status[i] = BLOCKED;
			for(j=0; j < p->retc; j++)
				vars[getArg(p,j)] = BLOCKED;
		} else
		if (    getModuleId(p) == batRef && getFunctionId(p)==appendRef )  {
			if ( vars[getArg(p,p->retc)] == PARTITION){
				status[i] = BLOCKED;
				for(j=0; j < p->retc; j++)
					vars[getArg(p,j)] = BLOCKED;
			}
		} 

		/* An instruction based on PARTITION and PIVOT arguments is PIVOTAL */
		for( j = p->retc; j < p->argc; j++)
		if (vars[getArg(p,j)] == BLOCKED ) 
			break;
		if ( j != p->argc && p->argc - p->retc > 0 )
			status[i]= BLOCKED;

		for ( j= 0; j< p->retc; j++)
		if (vars[getArg(p,j)] == 0)
			vars[getArg(p,j)] = status[i];

		if ( status[i] == PARTITION)
		for( j = p->retc; j < p->argc; j++)
			if (vars[getArg(p,j)] == 0)
				vars[getArg(p,j)] = SUPPORTIVE;
	}
#ifdef _DEBUG_OPT_CENTIPEDE_ 
/*
	mnstr_printf(cntxt->fdout,"\n#phase 1 show partition keys\n");
	for( i= 0; i< limit; i++)
	if (status[i] && old[i] ) {
		mnstr_printf(cntxt->fdout,"%s [%d] ",statusname[status[i]],i);
		for (j=0; j< old[i]->retc; j++){
			int x = old[i]->argv[j];
			mnstr_printf(cntxt->fdout,"%d=%d ",x,vars[x]);
		}
		printInstruction(cntxt->fdout, mb,0,old[i],LIST_MAL_STMT);
	}
*/
#endif

	/* Phase 2: extend the set with supportive instructions.
	   we have to avoid common ancestor dependency on partitioned variables
	*/
	for ( i = limit -1; i >= 0 ; i--)
	if ( status[i] == 0 || status[i]== BLOCKED ){
		p = old[i];

		for( j = 0; j < p->argc; j++)
		if ( vars[getArg(p,j)] == BLOCKED)
			break;

		if ( j == p->argc ) {
			/* does it produce partitioned or support variables */
			for( j = 0; j < p->retc; j++)
			if ( vars[getArg(p,j)] == SUPPORTIVE || vars[getArg(p,j)] == PARTITION || vars[getArg(p,j)] == PIVOT)  
				break;
		} else {
			status[i] = BLOCKED;
			for( j = 0; j < p->retc; j++)
			//if ( vars[getArg(p,j)] == 0)
				vars[getArg(p,j)] = BLOCKED;
		}

		if( j< p->retc && status[i] != BLOCKED ){
			for ( j= 0; j< p->argc; j++)
			if ( vars[getArg(p,j)] == 0)
				vars[getArg(p,j)] = SUPPORTIVE;
			status[i] = SUPPORTIVE;
		} else  {
			for ( j= 0; j< p->retc; j++)
			if ( vars[getArg(p,j)] == 0)
				vars[getArg(p,j)] = BLOCKED;
			status[i] = BLOCKED;
		}
	}
#ifdef _DEBUG_OPT_CENTIPEDE_ 
	mnstr_printf(cntxt->fdout,"\n#phase 2 show partition keys\n");
	for( i= 0; i< limit; i++)
	if (status[i] && old[i] ) {
		mnstr_printf(cntxt->fdout,"%s [%d] ",statusname[status[i]],i);
		for (j=0; j< old[i]->retc; j++){
			int x = old[i]->argv[j];
			mnstr_printf(cntxt->fdout,"%d=%d ",x,vars[x]);
		}
		printInstruction(cntxt->fdout, mb,0,old[i],LIST_MAL_STMT);
	}
#endif
	/* Phase 3: determine all variables to be exported  to the cntrl and main program
	   this is limited to all variables produced and consumed by a blocked instruction
	*/
	cntrlreturn= newInstruction(0,ASSIGNsymbol);
	cntrlreturn->barrier = RETURNsymbol;
	cntrlreturn->argc= cntrlreturn->retc = 0;
	planreturn = copyInstruction(cntrlreturn);
	packs = copyInstruction(cntrlreturn);


	for ( i = 0; i< limit; i++)
	if ( status[i]== BLOCKED  )
	{
		p = old[i];
		if ( p )
		for( j = p->retc; j < p->argc; j++)
		if ( isaBatType(getArgType(plan,p,j)) && (vars[getArg(p,j)]== PIVOT || vars[getArg(p,j)] == PARTITION ||vars[getArg(p,j)] == SUPPORTIVE)){
			addvartolist(plan,&cntrlreturn,getArg(p,j));
			addvartolist(plan,&planreturn,getArg(p,j));
			addvartolist(plan,&packs,getArg(p,j));
		}
	}

	/* Phase 4: Bake a new function that produces them */

	p = copyInstruction(getInstrPtr(mb, 0));
	pushInstruction(plan,p);

	/* keep the original variable list for the caller, but ignore local names */
	orig = copyInstruction(cntrlreturn);

	for ( i = 1; i < limit ; i++) 
		if( status[i] != BLOCKED ){
			p = copyInstruction(getInstrPtr(mb, i));
			if ( old[i]->token == ENDsymbol) {
				break;
			} else
			if (getModuleId(p) == sqlRef && (getFunctionId(p) == bindRef || getFunctionId(p) == bindidxRef))  
				OPTsliceColumn(cntxt, plan, mb, p, slices);
			else
			if ( getModuleId(p) == algebraRef && getFunctionId(p) == leftfetchjoinPathRef && p->argc-p->retc == 2){
				if ( vars[getArg(p,p->retc)] == PARTITION || vars[getArg(p,p->retc)]== PIVOT){
					getFunctionId(p) = leftjoinPathRef;
					addvartolist(plan,&packs,getArg(p,0));
					addvartolist(plan,&planreturn,getArg(p,0));
				}
				pushInstruction(plan,p);
			} else
			if ( getModuleId(p) == algebraRef && getFunctionId(p) == leftfetchjoinRef ){
				// overrule the leftfetchjoin when slicing
				q = getInstrPtr(plan,plan->stop-1);
				 if( getFunctionId(q) == sliceRef && getArg(p,p->argc-1) == getArg(q,0))
					getFunctionId(p) = leftjoinRef;
				if ( vars[getArg(p,p->retc)] == PIVOT){
					addvartolist(plan,&packs,getArg(p,0));
					addvartolist(plan,&planreturn,getArg(p,0));
					addvartolist(plan,&packs,getArg(p,p->argc-1));
					addvartolist(plan,&planreturn,getArg(p,p->argc-1));
				}
				pushInstruction(plan,p);
	#ifdef _DEBUG_OPT_CENTIPEDE_
				mnstr_printf(cntxt->fdout,"\n#pmb include stmt %d  %d\n",i, plan->stop);
				printInstruction(cntxt->fdout, mb, 0, p,LIST_MAL_STMT);
				mnstr_printf(cntxt->fdout,"#plan packs stmt\n");
				printInstruction(cntxt->fdout, plan,0,packs,LIST_MAL_STMT);
				mnstr_printf(cntxt->fdout,"#plan return stmt\n");
				printInstruction(cntxt->fdout, plan,0,planreturn,LIST_MAL_STMT);
	#endif
			} else
			if (getModuleId(p) == aggrRef && (getFunctionId(p) == subcountRef || getFunctionId(p) == subsumRef ||
				getFunctionId(p) == subminRef || getFunctionId(p) == submaxRef || getFunctionId(p) == subavgRef )){
				addvartolist(plan,&packs,getArg(p,0));
				addvartolist(plan,&planreturn,getArg(p,0));
				//addvartolist(plan,&packs,getArg(p,p->retc));
				//addvartolist(plan,&planreturn,getArg(p,p->retc));
	#ifdef _DEBUG_OPT_CENTIPEDE_
				mnstr_printf(cntxt->fdout,"\n#pmb include stmt %d  %d\n",i, plan->stop);
				printInstruction(cntxt->fdout, mb, 0, p,LIST_MAL_STMT);
				mnstr_printf(cntxt->fdout,"#plan packs stmt\n");
				printInstruction(cntxt->fdout, plan,0,packs,LIST_MAL_STMT);
				mnstr_printf(cntxt->fdout,"#plan return stmt\n");
				printInstruction(cntxt->fdout, plan,0,planreturn,LIST_MAL_STMT);
	#endif
				pushInstruction(plan,p);
			} else
			if (getModuleId(p) == aggrRef && getFunctionId(p) == countRef){
				addvartolist(plan,&packs,getArg(p,1));
				addvartolist(plan,&planreturn,getArg(p,0));
				pushInstruction(plan,p);
			} else
			if (getModuleId(p) == groupRef && (getFunctionId(p) == subgroupdoneRef || getFunctionId(p) == subgroupRef) ) {
				char buf[BUFSIZ];
				int id= getArg(p,p->retc);
				/* produce all code to get a reduced table across */
	#ifdef _DEBUG_OPT_CENTIPEDE_
				mnstr_printf(cntxt->fdout,"\n#pmb include stmt %d  %d\n",i, plan->stop);
				printInstruction(cntxt->fdout, mb, 0, p,LIST_MAL_STMT);
				mnstr_printf(cntxt->fdout,"#plan packs stmt\n");
				printInstruction(cntxt->fdout, plan,0,packs,LIST_MAL_STMT);
				mnstr_printf(cntxt->fdout,"#plan return stmt\n");
				printInstruction(cntxt->fdout, plan,0,planreturn,LIST_MAL_STMT);
	#endif
				pushInstruction(plan,p);
				// expand the group table through all group.subgroup operations
				q = newInstruction(plan,ASSIGNsymbol);
				getModuleId(q) = algebraRef;
				getFunctionId(q) = leftfetchjoinPathRef;
				q= pushArgument(plan,q,getArg(p,1));
				q= pushArgument(plan,q,getArg(p,p->argc-1));
				snprintf(buf,BUFSIZ,"Y_%d",id);
				getArg(q,0) = newVariable(plan, GDKstrdup(buf), newBatType(TYPE_oid, getColumnType(getVarType(plan,id))));
				addvartolist(plan,&planreturn,getArg(q,0));
				addvartolist(plan,&packs,getArg(q,0));
				setVarUsed(plan,getArg(q,0)); 
				// locate the related instructions
				for(j = plan->stop-1; j>0; j--){
					InstrPtr qq= getInstrPtr(plan,j);
					if( getModuleId(qq) != groupRef )
						continue;
					if ( getFunctionId(qq) == subgroupRef && getArg(qq,0) == getArg(p,p->argc-1)){
						InstrPtr pq;
						pq = newStmt(plan,algebraRef,leftfetchjoinRef);
						getArg(pq,0) = newTmpVariable(plan,getArgType(plan,qq,qq->argc-1));
						pq= pushArgument(plan,pq,getArg(qq,1));
						pq= pushArgument(plan,pq,getArg(qq,qq->argc-1));
						addvartolist(plan,&planreturn,getArg(pq,0));
						addvartolist(plan,&packs,getArg(pq,0));
					}
				}
				pushInstruction(plan,q);
			} else {
				pushInstruction(plan,p);
			}
		}
	k = planreturn->retc;
	for( i =0; i< k; i++) 
		planreturn= pushArgument(plan,planreturn, getArg(planreturn,i));
	getFunctionId(plan->stmt[0]) = putName(nme,strlen(nme));
	pushInstruction(plan,planreturn);
	pushEndInstruction(plan);
#ifdef _DEBUG_OPT_CENTIPEDE_ 
	mnstr_printf(cntxt->fdout,"\n#phase 3 return stmt\n");
	mnstr_printf(cntxt->fdout,"#cntrl return stmt\n");
	printInstruction(cntxt->fdout, mb,0,cntrlreturn,LIST_MAL_STMT);
	mnstr_printf(cntxt->fdout,"#plan return stmt\n");
	printInstruction(cntxt->fdout, plan,0,planreturn,LIST_MAL_STMT);
	mnstr_printf(cntxt->fdout,"#plan packs stmt\n");
	printInstruction(cntxt->fdout, plan,0,packs,LIST_MAL_STMT);
#endif

	/* fix the signature and modify the underlying plan */
	while ( plan->stmt[0]->retc )
		delArgument(plan->stmt[0],0);
	for( i =0; i< planreturn->retc; i++)
		plan->stmt[0]= pushReturn(plan, plan->stmt[0], getArg(planreturn,i));

	insertSymbol(cntxt->nspace,s);
#ifdef _DEBUG_OPT_CENTIPEDE_
	//mnstr_printf(cntxt->fdout,"#rough scnd plan errors %d \n", plan->errors);
	//printFunction(cntxt->fdout, plan, 0, LIST_MAL_STMT);
#endif

	/* construct the control plan for local/remote execution */
	cntrl = OPTexecController(cntxt, mb, plan, cntrlreturn, packs, slices, plantag, status);
	if ( cntrl)  {
		msg= optimizeMALBlock(cntxt, cntrl);
		chkProgram(cntxt->fdout, cntxt->nspace, cntrl);
	}
#ifdef _DEBUG_OPT_CENTIPEDE_
	mnstr_printf(cntxt->fdout,"#optimized control plan errors %d %s \n",cntrl->errors,msg?msg:"");
	printFunction(cntxt->fdout, cntrl, 0, LIST_MAL_STMT);
#endif

	call = copyInstruction(getInstrPtr(plan,0));
	call->barrier = 0;
	call->token = ASSIGNsymbol;
	call->argc -= 2; /* bounds are removed */
	getFunctionId(call) = getFunctionId(getInstrPtr(cntrl,0));
	insertInstruction(mb,call, 1);
	/* fix the signature and modify the underlying plan */
	while ( call->retc )
		delArgument(call,0);
	for( i =0; i< orig->retc; i++) {
		call= pushReturn(mb, call, getArg(orig,i));
		/* in the underlying plan we can remove all assignments handled by the cntrl() */
		for( j = 2; j < mb->stop; j++){
			InstrPtr q= getInstrPtr(mb,j);
			for( k=0; k< q->retc; k++)
			if ( getArg(q,k) == getArg(orig,i))
				getArg(q,k) = newTmpVariable(mb,TYPE_any);
		}
	}

#ifdef _DEBUG_OPT_DETAIL
	chkProgram(cntxt->fdout, cntxt->nspace, mb);
	mnstr_printf(cntxt->fdout,"#non-optimized main error %d %s\n", mb->errors, msg?msg:"");
	printFunction(cntxt->fdout, mb, 0, LIST_MAL_STMT);
#endif
	chkProgram(cntxt->fdout, cntxt->nspace, plan);
#ifdef _DEBUG_OPT_CENTIPEDE_
	mnstr_printf(cntxt->fdout,"#optimized remote plan error %d %s\n", plan->errors, msg?msg:"");
	printFunction(cntxt->fdout, plan, 0, LIST_MAL_STMT);
#endif

	/* construct the remote stub plan */
	stub = OPTplanStub(cntxt, mb, plan, plantag);
#ifdef _DEBUG_OPT_CENTIPEDE_
	mnstr_printf(cntxt->fdout,"#final 1 cntrl plan %d \n", cntrl->errors);
	printFunction(cntxt->fdout, cntrl, 0, LIST_MAL_STMT);
#endif
	if ( stub)  {
		msg= optimizeMALBlock(cntxt, stub);
		chkProgram(cntxt->fdout, cntxt->nspace, stub);
	}
#ifdef _DEBUG_OPT_CENTIPEDE_
	mnstr_printf(cntxt->fdout,"#stub plan errors %d %s \n",stub->errors,msg?msg:"");
	printFunction(cntxt->fdout, stub, 0, LIST_MAL_STMT);
#endif
	GDKfree(old);
	GDKfree(vars);
	GDKfree(status);
}

/*
 * The first step is to reduce the query to the persistent (readonly) part of the database
 * The general tactic is to identify instructions that are blocked in a distributed setting.
 * For those instruction we inject a multi-assignment to map is arguments to new variables
 * and the aliases are propagated thru the plan.
 * The next step is to derived a distribution consolidation plan for all arguments whose
 * portions are needed.
*/
static int
OPTreadonlyQuery(Client cntxt, MalBlkPtr mb)
{
	InstrPtr p, *old;
	int i,limit;
	limit = mb->stop;
	old = mb->stmt;
	if ( newMalBlkStmt(mb,mb->ssize) < 0 )
		return -1;

	for( i = 0; i < limit; i++){
		p = old[i];
		if ( getModuleId(p) == sqlRef){
			if( getFunctionId(p) == deltaRef){
				clrFunction(p);
				p->argc = 2;
			}
		}
		pushInstruction(mb,p);
	}
	GDKfree(old);
	(void) OPTdeadcodeImplementation(cntxt,mb,0,0);
	(void) OPTaliasesImplementation(cntxt,mb,0,0);
	(void) OPTgarbageCollectorImplementation(cntxt,mb,0,0);
	return 0;
}
int
OPTcentipedeImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	wrd r = 0, rowcnt=0;	/* table should be sizeable to consider parallel execution*/
	InstrPtr q, target= 0;
	Slices slices;
	str msg = NULL;

	(void)cntxt;
	(void) stk;
	(void) pci;

	msg = GDKgetenv("gdk_readonly");
	if( msg == 0 || strcmp(msg,"yes")) {
		//mnstr_printf(cntxt->fdout,"#WARNING centipede only works for readonly databases\n");
		//This is enforced by removing the sql.delta operations
		//return 0;
	}
	if ( nrservers == 0)
		nrservers = 2; /* to ease debugging now */

#ifdef _DEBUG_OPT_CENTIPEDE_
	//mnstr_printf(cntxt->fdout,"#original plan \n");
	//printFunction(cntxt->fdout, mb, 0, LIST_MAL_STMT);
#endif
	OPTreadonlyQuery(cntxt,mb);
#ifdef _DEBUG_OPT_CENTIPEDE_
	mnstr_printf(cntxt->fdout,"#readonly part of original plan \n");
	printFunction(cntxt->fdout, mb, 0, LIST_MAL_STMT);
#endif
	/* modify the block as we go */
	memset( (char*) &slices, 0, sizeof(slices));
	/* locate the largest non-partitioned table */
	/* Much more intelligence can be injected here */
	for (i=1; i< mb->stop; i++){
		q= getInstrPtr(mb,i);
		if ( getModuleId(q) == sqlRef && getFunctionId(q) == deltaRef){
			// Use a readonly view on the database TO BE FIXED
			clrFunction(q);
			q->argc =2;
			q->token = ASSIGNsymbol;
		}
		/* don't split insert BATs */
		if ( ! (getModuleId(q) == sqlRef && getFunctionId(q) == bindRef  && q->retc == 1) )
			continue;
		r = getVarRows(mb, getArg(q, 0));
		if (r > rowcnt ){
			rowcnt = r;
			target = q;
			r = 0;
		}
	}
	if (target == 0)
		return 0;

	/* the target becomes the table against which we break the query */
	/* we use the oid range of the target*/
	slices.target = target;
	slices.rowcnt = rowcnt;
	slices.schema = GDKstrdup(getVarConstant(mb, getArg(target,2)).val.sval);
	slices.table = GDKstrdup(getVarConstant(mb, getArg(target,3)).val.sval);
	slices.column = GDKstrdup(getVarConstant(mb,getArg(target,4)).val.sval);
	slices.type = getColumnType(getVarType(mb,getArg(target,0)));
	slices.lslices=  newTmpVariable(mb, TYPE_oid);
	slices.hslices=  newTmpVariable(mb, TYPE_oid);
	slices.slice = 0;

	OPTDEBUGcentipede
		mnstr_printf(cntxt->fdout,"#opt_centipede: target is %s.%s "
			" with " SSZFMT " rows into %d servers\n",
			slices.schema, slices.table, rowcnt, nrservers);

	/* derive a local plan based on forward flow reasoning */
	OPTbakePlans(cntxt, mb, &slices);
#ifdef _DEBUG_OPT_CENTIPEDE_
	//mnstr_printf(cntxt->fdout,"non-optimized final main plan: %d errors\n",mb->errors);
	//chkProgram(cntxt->fdout, cntxt->nspace, mb);
	//printFunction(cntxt->fdout, mb, 0, LIST_MAL_STMT);
#endif
	msg= optimizeMALBlock(cntxt, mb);
	chkProgram(cntxt->fdout, cntxt->nspace, mb);
#ifdef _DEBUG_OPT_CENTIPEDE_
	mnstr_printf(cntxt->fdout,"#final plan %s.%s.%s type %d %s\n",
		slices.schema,
		slices.table,
		(slices.column ? slices.column: ""),
		slices.type,
		msg?msg:"");
	printFunction(cntxt->fdout, mb, 0, LIST_MAL_STMT);
#endif
	GDKfree(slices.schema);
	GDKfree(slices.table);
	if (msg || mb->errors ) {
		/* restore MAL block */
		mnstr_printf(cntxt->fdout,"#partition %s\n", msg?msg:"generic error");
		if( msg)
			GDKfree(msg);
#ifdef _DEBUG_OPT_CENTIPEDE_
		printFunction(cntxt->fdout, mb, 0, LIST_MAL_STMT);
#endif
		return 0;
	}
	return 1;
}

/* Partitioning can be driven by value and oid ranges */
/* For value ranges min/max bounds are null values */
/* It is the heart of the approach and requires experimentation */
str
OPTvectorOid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	oid *o;
	BUN rows;
	int i,bid;
	BAT *b;
	(void) cntxt;
	(void) mb;

	bid = *(int*) getArgReference(stk, pci, pci->retc);
	b = BBPquickdesc(bid, FALSE);
	if (b == NULL)
		throw(SQL,"centipede.vector","Can not access BAT");
	rows = BATcount(b);
	o= (oid*) getArgReference(stk,pci,0);
	*o = 0;
	if ( pci->retc >= 2 ) {
		for ( i= 1; i < pci->retc-1; i++){
			o= (oid*) getArgReference(stk, pci, i);
			*o = (oid) ((rows * i ) / (pci->retc - 1)  + 1); /* last one excluded */
		}
		/* i == pci->retc-1 */
		o= (oid*) getArgReference(stk,pci,i);
		*o = oid_nil;
	}
	return MAL_SUCCEED;
}
