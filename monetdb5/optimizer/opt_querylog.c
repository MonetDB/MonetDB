/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_querylog.h"
#include "gdk_time.h"
#include "querylog.h"

str
OPTquerylogImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, limit, slimit, actions = 0;
	InstrPtr p = 0, *old= mb->stmt, q,r;
	int argc, io, user,nice,sys,idle,iowait,load, arg, start,finish, name;
	int xtime=0, rtime = 0, tuples=0;
	InstrPtr defineQuery = NULL;
	str msg = MAL_SUCCEED;


	// query log needed?
	if ( !QLOGisset() )
		goto wrapup;

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
		goto wrapup;

	actions++;
	limit= mb->stop;
	slimit= mb->ssize;
	if ( newMalBlkStmt(mb, mb->ssize) < 0)
		throw(MAL,"optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	pushInstruction(mb, old[0]);
	/* run the querylog.define operation */
	defineQuery = copyInstruction(defineQuery);
	if (defineQuery == NULL) {
		msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	defineQuery->argc--;  // remove MAL instruction count
	setFunctionId(defineQuery, appendRef);
	getArg(defineQuery,0) = newTmpVariable(mb,TYPE_any);
	defineQuery->token = ASSIGNsymbol;
	setModuleId(defineQuery,querylogRef);

	/* collect the initial statistics */
	q = newStmt(mb, "clients", "getUsername");
	if (q == NULL) {
		freeInstruction(defineQuery);
		msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	name= getArg(q,0)= newVariable(mb,"name",4,TYPE_str);
	pushInstruction(mb, q);
	defineQuery = pushArgument(mb,defineQuery,name);
	q = newStmt(mb, mtimeRef, "current_timestamp");
	if (q == NULL) {
		freeInstruction(defineQuery);
		msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	start= getArg(q,0)= newVariable(mb,"start",5,TYPE_timestamp);
	pushInstruction(mb, q);
	defineQuery = pushArgument(mb,defineQuery,start);
	pushInstruction(mb, defineQuery);

	q = newStmtArgs(mb, sqlRef, "argRecord", old[0]->argc);
	if (q == NULL) {
		msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	for ( argc=1; argc < old[0]->argc; argc++)
		q = pushArgument(mb, q, getArg(old[0],argc));

	arg= getArg(q,0)= newVariable(mb,"args",4,TYPE_str);
	pushInstruction(mb, q);


	q = newStmt(mb, alarmRef, "usec");
	if (q == NULL) {
		msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	pushInstruction(mb, q);
	xtime = getArg(q,0)= newVariable(mb,"xtime",5,TYPE_lng);
	user = newVariable(mb,"user",4,TYPE_lng);
	nice = newVariable(mb,"nice",4,TYPE_lng);
	sys = newVariable(mb,"sys",3,TYPE_lng);
	idle = newVariable(mb,"idle",4,TYPE_lng);
	iowait = newVariable(mb,"iowait",6,TYPE_lng);
	q = newStmt(mb, profilerRef, "cpustats");
	if (q == NULL) {
		msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	q->retc= q->argc =0;
	q = pushReturn(mb,q,user);
	q = pushReturn(mb,q,nice);
	q = pushReturn(mb,q,sys);
	q = pushReturn(mb,q,idle);
	q = pushReturn(mb,q,iowait);
	pushInstruction(mb, q);
	q = newAssignment(mb);
	if (q == NULL) {
		msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	tuples= getArg(q,0) = newVariable(mb,"tuples",6,TYPE_lng);
	(void) pushLng(mb,q,1);
	pushInstruction(mb, q);

	for (i = 1; i < limit; i++) {
		p = old[i];

		if (getModuleId(p)==sqlRef &&
			(idcmp(getFunctionId(p),"exportValue")==0 ||
			 idcmp(getFunctionId(p),"exportResult")==0  ) ) {

			q = newStmt(mb, alarmRef, "usec");
			if (q == NULL) {
				msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			pushInstruction(mb, q);
			r = newStmt(mb, calcRef, minusRef);
			if (r == NULL) {
				msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			r = pushArgument(mb, r, getArg(q,0));
			r = pushArgument(mb, r, xtime);
			getArg(r,0)=xtime;
			pushInstruction(mb, r);

			q = newStmt(mb, alarmRef, "usec");
			if (q == NULL) {
				msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			rtime= getArg(q,0)= newVariable(mb,"rtime",5,TYPE_lng);
			pushInstruction(mb, q);
			pushInstruction(mb,p);
			continue;
		}
		if ( getModuleId(p) == sqlRef && getFunctionId(p) == resultSetRef && isaBatType(getVarType(mb,getArg(p,3)))){
			q = newStmt(mb, aggrRef, countRef);
			if (q == NULL) {
				msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			getArg(q,0) = tuples;
			(void) pushArgument(mb,q, getArg(p,3));
			pushInstruction(mb, q);
			pushInstruction(mb,p);
			continue;
		}
		if ( p->token== ENDsymbol || p->barrier == RETURNsymbol || p->barrier == YIELDsymbol){
			if ( rtime == 0){
				q = newStmt(mb, alarmRef, "usec");
				if (q == NULL) {
					msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					break;
				}
				pushInstruction(mb, q);
				r = newStmt(mb, calcRef, minusRef);
				if (r == NULL) {
					msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					break;
				}
				r = pushArgument(mb, r, getArg(q,0));
				r = pushArgument(mb, r, xtime);
				getArg(r,0)=xtime;
				pushInstruction(mb, r);
				q = newStmt(mb, alarmRef, "usec");
				if (q == NULL) {
					msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					break;
				}
				rtime= getArg(q,0)= newVariable(mb,"rtime",5,TYPE_lng);
				pushInstruction(mb, q);
			}
			q = newStmt(mb, alarmRef, "usec");
			if (q == NULL) {
				msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			pushInstruction(mb, q);
			r = newStmt(mb, calcRef, minusRef);
			if (r == NULL) {
				msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			r = pushArgument(mb, r, getArg(q,0));
			r = pushArgument(mb, r, rtime);
			getArg(r,0)=rtime;
			pushInstruction(mb, r);
			/*
			 * Post execution statistics gathering
			 */
			q = newStmt(mb, mtimeRef, "current_timestamp");
			if (q == NULL) {
				msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			finish= getArg(q,0)= newVariable(mb,"finish",6,TYPE_any);
			pushInstruction(mb, q);

			q = newStmt(mb, profilerRef, "cpuload");
			if (q == NULL) {
				msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			load = newVariable(mb,"load",4,TYPE_int);
			getArg(q,0)= load;
			io = newVariable(mb,"io",2,TYPE_int);
			q= pushReturn(mb,q,io);
			q = pushArgument(mb,q,user);
			q = pushArgument(mb,q,nice);
			q = pushArgument(mb,q,sys);
			q = pushArgument(mb,q,idle);
			q = pushArgument(mb,q,iowait);
			pushInstruction(mb, q);

			q = newStmtArgs(mb, querylogRef, "call", 9);
			if (q == NULL) {
				msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			q = pushArgument(mb, q, start);
			q = pushArgument(mb, q, finish);
			q = pushArgument(mb, q, arg);
			q = pushArgument(mb, q, tuples);
			q = pushArgument(mb, q, xtime);
			q = pushArgument(mb, q, rtime);
			q = pushArgument(mb, q, load);
			q = pushArgument(mb, q, io);
			pushInstruction(mb, q);
			pushInstruction(mb,p);
			continue;
		}

		pushInstruction(mb,p);
		if (p->barrier == YIELDsymbol){
			/* the factory yield may return */
			q = newStmt(mb, mtimeRef, "current_timestamp");
			if (q == NULL) {
				msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			start= getArg(q,0)= newVariable(mb,"start",5,TYPE_any);
			pushInstruction(mb, q);
			q = newStmtArgs(mb, sqlRef, "argRecord", old[0]->argc);
			if (q == NULL) {
				msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			for ( argc=1; argc < old[0]->argc; argc++)
				q = pushArgument(mb, q, getArg(old[0],argc));
			arg= getArg(q,0)= newVariable(mb,"args",4,TYPE_str);
			pushInstruction(mb, q);
			q = newAssignment(mb);
			if (q == NULL) {
				msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			q = pushLng(mb,q,0);
			pushInstruction(mb, q);
			q = newAssignment(mb);
			if (q == NULL) {
				msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			q = pushLng(mb,q,0);
			tuples= getArg(q,0)= newVariable(mb,"tuples",6,TYPE_lng);
			p = newFcnCall(mb,profilerRef,"setMemoryFlag");
			if (p == NULL) {
				msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			q->argc--;
			pushLng(mb,q,1);
			pushInstruction(mb, q);
			pushInstruction(mb, p);
			q = newStmt(mb, alarmRef, "usec");
			if (q == NULL) {
				msg = createException(MAL, "optimizer.querylog", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			xtime = getArg(q,0)= newVariable(mb,"xtime",5,TYPE_lng);
			pushInstruction(mb, q);
		}
	}

  bailout:
	for( ; i<slimit; i++)
		if(old[i])
			pushInstruction(mb, old[i]);
	GDKfree(old);
	if (msg == MAL_SUCCEED) {
		/* Defense line against incorrect plans */
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
	/* keep actions taken as a fake argument*/
wrapup:
	(void) pushInt(mb, pci, actions);
	return msg;
}
