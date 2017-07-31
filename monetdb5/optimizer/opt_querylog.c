/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_querylog.h"
#include "mtime.h"
#include "querylog.h"

str 
OPTquerylogImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, limit, slimit;
	InstrPtr p = 0, *old= mb->stmt, q,r;
	int argc, io, user,nice,sys,idle,iowait,load, arg, start,finish, name;
	int xtime=0, rtime = 0, tuples=0;
	InstrPtr defineQuery = NULL;
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;


	// query log needed?
	if ( !QLOGisset() )
		return MAL_SUCCEED;
	(void) pci;
	(void) stk;		/* to fool compilers */
	(void) cntxt;
	/* gather information */
	for (i = 1; i < mb->stop; i++) {
		p = getInstrPtr(mb,i);
		if ( getModuleId(p) && idcmp(getModuleId(p), "querylog") == 0 && idcmp(getFunctionId(p),"define")==0){
			defineQuery= p;
			getVarConstant(mb,getArg(p,3)).val.lval = GDKusec()-getVarConstant(mb,getArg(p,3)).val.lval ;
		}
	}
	if ( defineQuery == NULL)
		/* nothing to do */
		return MAL_SUCCEED;

	limit= mb->stop;
	slimit= mb->ssize;
	if ( newMalBlkStmt(mb, mb->ssize) < 0)
		throw(MAL,"optimizer.querylog",MAL_MALLOC_FAIL);

	pushInstruction(mb, old[0]);
	/* run the querylog.define operation */
	defineQuery = copyInstruction(defineQuery);
	if( defineQuery == NULL)
		throw(MAL,"optimizer.querylog",MAL_MALLOC_FAIL);

	defineQuery->argc--;  // remoge MAL instruction count
	setFunctionId(defineQuery, appendRef);
	getArg(defineQuery,0) = newTmpVariable(mb,TYPE_any);
	defineQuery->token = ASSIGNsymbol;
	setModuleId(defineQuery,querylogRef);

	/* collect the initial statistics */
	q = newStmt(mb, "clients", "getUsername");
	name= getArg(q,0)= newVariable(mb,"name",4,TYPE_str);
	defineQuery = pushArgument(mb,defineQuery,name);
	q = newStmt(mb, "mtime", "current_timestamp");
	start= getArg(q,0)= newVariable(mb,"start",5,TYPE_timestamp);
	defineQuery = pushArgument(mb,defineQuery,start);
	pushInstruction(mb, defineQuery);

	q = newStmt(mb, sqlRef, "argRecord");
	for ( argc=1; argc < old[0]->argc; argc++)
		q = pushArgument(mb, q, getArg(old[0],argc));

	arg= getArg(q,0)= newVariable(mb,"args",4,TYPE_str);


	q = newStmt(mb, "alarm", "usec");
	xtime = getArg(q,0)= newVariable(mb,"xtime",5,TYPE_lng);
	user = newVariable(mb,"user",4,TYPE_lng);
	nice = newVariable(mb,"nice",4,TYPE_lng);
	sys = newVariable(mb,"sys",3,TYPE_lng);
	idle = newVariable(mb,"idle",4,TYPE_lng);
	iowait = newVariable(mb,"iowait",6,TYPE_lng);
	q = newStmt(mb, "profiler", "cpustats");
	q->retc= q->argc =0;
	q = pushReturn(mb,q,user);
	q = pushReturn(mb,q,nice);
	q = pushReturn(mb,q,sys);
	q = pushReturn(mb,q,idle);
	q = pushReturn(mb,q,iowait);
	q = newAssignment(mb);
	tuples= getArg(q,0) = newVariable(mb,"tuples",6,TYPE_lng);
	(void) pushLng(mb,q,1);

	for (i = 1; i < limit; i++) {
		p = old[i];
		
		if (getModuleId(p)==sqlRef && 
			(idcmp(getFunctionId(p),"exportValue")==0 ||
			 idcmp(getFunctionId(p),"exportResult")==0  ) ) {

			q = newStmt(mb, "alarm", "usec");
			r = newStmt(mb, calcRef, "-");
			r = pushArgument(mb, r, getArg(q,0));
			r = pushArgument(mb, r, xtime);
			getArg(r,0)=xtime;

			q = newStmt(mb, "alarm", "usec");
			rtime= getArg(q,0)= newVariable(mb,"rtime",5,TYPE_lng);
			pushInstruction(mb,p);
			continue;
		}
		if ( getModuleId(p) == sqlRef && idcmp(getFunctionId(p),"resultSet")==0  && isaBatType(getVarType(mb,getArg(p,3)))){
			q = newStmt(mb, "aggr", "count");
			getArg(q,0) = tuples;
			(void) pushArgument(mb,q, getArg(p,3));
			pushInstruction(mb,p);
			continue;
		}	
		if ( p->token== ENDsymbol || p->barrier == RETURNsymbol || p->barrier == YIELDsymbol){
			if ( rtime == 0){
				q = newStmt(mb, "alarm", "usec");
				r = newStmt(mb, calcRef, "-");
				r = pushArgument(mb, r, getArg(q,0));
				r = pushArgument(mb, r, xtime);
				getArg(r,0)=xtime;
				q = newStmt(mb, "alarm", "usec");
				rtime= getArg(q,0)= newVariable(mb,"rtime",5,TYPE_lng);
			}
			q = newStmt(mb, "alarm", "usec");
			r = newStmt(mb, calcRef, "-");
			r = pushArgument(mb, r, getArg(q,0));
			r = pushArgument(mb, r, rtime);
			getArg(r,0)=rtime;
			/*
			 * Post execution statistics gathering
			 */
			q = newStmt(mb, "mtime", "current_timestamp");
			finish= getArg(q,0)= newVariable(mb,"finish",6,TYPE_any);

			q = newStmt(mb, "profiler", "cpuload");
			load = newVariable(mb,"load",4,TYPE_int);
			getArg(q,0)= load;
			io = newVariable(mb,"io",2,TYPE_int);
			q= pushReturn(mb,q,io);
			q = pushArgument(mb,q,user);
			q = pushArgument(mb,q,nice);
			q = pushArgument(mb,q,sys);
			q = pushArgument(mb,q,idle);
			q = pushArgument(mb,q,iowait);

			q = newStmt(mb, querylogRef, "call");
			q = pushArgument(mb, q, start);
			q = pushArgument(mb, q, finish); 
			q = pushArgument(mb, q, arg);
			q = pushArgument(mb, q, tuples); 
			q = pushArgument(mb, q, xtime); 
			q = pushArgument(mb, q, rtime); 
			q = pushArgument(mb, q, load); 
			q = pushArgument(mb, q, io); 
			pushInstruction(mb,p);
			continue;
		}

		pushInstruction(mb,p);
		if (p->barrier == YIELDsymbol){
			/* the factory yield may return */
			q = newStmt(mb, "mtime", "current_timestamp");
			start= getArg(q,0)= newVariable(mb,"start",5,TYPE_any);
			q = newStmt(mb, sqlRef, "argRecord");
			for ( argc=1; argc < old[0]->argc; argc++)
				q = pushArgument(mb, q, getArg(old[0],argc));
			arg= getArg(q,0)= newVariable(mb,"args",4,TYPE_str);
			q = newAssignment(mb);
			q = pushLng(mb,q,0);
			q = newAssignment(mb);
			q = pushLng(mb,q,0);
			tuples= getArg(q,0)= newVariable(mb,"tuples",6,TYPE_lng);
			newFcnCall(mb,"profiler","setMemoryFlag");
			q->argc--;
			pushLng(mb,q,1);
			q = newStmt(mb, "alarm", "usec");
			xtime = getArg(q,0)= newVariable(mb,"xtime",5,TYPE_lng);
		}
	}

	for( ; i<slimit; i++)
		if(old[i])
			freeInstruction(old[i]);
	GDKfree(old);
	    /* Defense line against incorrect plans */
    if( 1){
        chkTypes(cntxt->usermodule, mb, FALSE);
        chkFlow(mb);
        chkDeclarations(mb);
    }
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=1 time=" LLFMT " usec","querylog", usec);
    newComment(mb,buf);
	addtoMalBlkHistory(mb);
	return msg;
}
