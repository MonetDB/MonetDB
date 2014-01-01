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
 * A tricky piece of code. We invalidate an instruction
 * based on the return variable.
 */
#include "monetdb_config.h"
#include "opt_history.h"

str 
OPTforgetPrevious(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	int pc;
	(void) cntxt;
	(void) stk;
	pc= getPC(mb,pci);
	assert ( pc > 0 && pc < mb->stop);
	mb->stmt[pc-1]->token = REMsymbol;
	return MAL_SUCCEED;
}

int 
OPThistoryImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, limit, slimit;
	InstrPtr p = 0, *old= mb->stmt, q,r;
	int argc, inblock, oublock, arg, ctime;
	int xtime=0, rtime = 0, foot=0, memory, tuples=0;
	int returnseen = 0;
	InstrPtr keepQuery = NULL;


	(void) pci;
	(void) stk;		/* to fool compilers */
	(void) cntxt;
	/* gather information */
	for (i = 1; i < mb->stop; i++) {
		p = getInstrPtr(mb,i);
		if ( getModuleId(p)== userRef && idcmp(getFunctionId(p),"keepquery")==0){
			keepQuery= p;
			getVarConstant(mb,getArg(p,4)).val.lval = 
				GDKusec()-getVarConstant(mb,getArg(p,4)).val.lval ;
		}
	}
	if ( keepQuery == NULL)
		/* nothing to do */
		return 0;

	limit= mb->stop;
	slimit= mb->ssize;
	if ( newMalBlkStmt(mb, 2 * mb->ssize) < 0)
		return 0; 

	pushInstruction(mb, old[0]);
	/* run the keepQuery operation once only */
	pushInstruction(mb, q = copyInstruction(keepQuery));
	q->token = ASSIGNsymbol;
	(void) newStmt1(mb, sqlRef, "forgetPrevious");

	/* collect the statistics */
	q = newStmt(mb, "mtime", "current_timestamp");
	ctime= getArg(q,0)= newVariable(mb,GDKstrdup("ctime"),TYPE_any);
	q = newStmt1(mb, sqlRef, "argRecord");
	for ( argc=1; argc < old[0]->argc; argc++)
		q = pushArgument(mb, q, getArg(old[0],argc));
	arg= getArg(q,0)= newVariable(mb,GDKstrdup("args"),TYPE_str);
		q = newAssignment(mb);
		q = pushLng(mb,q,0);
		memory= getArg(q,0)= newVariable(mb,GDKstrdup("memory"),TYPE_lng);
		q = newAssignment(mb);
		q = pushWrd(mb,q,0);
		tuples= getArg(q,0)= newVariable(mb,GDKstrdup("tuples"),TYPE_wrd);
	newFcnCall(mb,"profiler","setFootprintFlag");
	newFcnCall(mb,"profiler","setMemoryFlag");
	q->argc--;
	pushWrd(mb,q,1);
	q = newStmt(mb, "profiler", "getDiskReads");
	inblock = getArg(q,0)= newVariable(mb,GDKstrdup("inblock"),TYPE_lng);
	q = newStmt(mb, "profiler", "getDiskWrites");
	oublock= getArg(q,0)= newVariable(mb,GDKstrdup("oublock"),TYPE_lng);
	q = newStmt(mb, "alarm", "usec");
	xtime = getArg(q,0)= newVariable(mb,GDKstrdup("xtime"),TYPE_lng);

	for (i = 1; i < limit; i++) {
		p = old[i];
		
		if ( getModuleId(p)==sqlRef && 
			(idcmp(getFunctionId(p),"exportValue")==0 ||
			 idcmp(getFunctionId(p),"exportResult")==0  ) ){
			
			q = newStmt(mb, "alarm", "usec");
			r = newStmt1(mb, calcRef, "-");
			r = pushArgument(mb, r, getArg(q,0));
			r = pushArgument(mb, r, xtime);
			getArg(r,0)=xtime;

			q = newStmt(mb, "alarm", "usec");
			rtime= getArg(q,0)= newVariable(mb,GDKstrdup("rtime"),TYPE_lng);
			pushInstruction(mb,p);
			continue;
		}	
		if (getModuleId(p)==sqlRef && idcmp(getFunctionId(p),"resultSet")==0 ){
			q = newStmt(mb, "aggr", "count");
			tuples = getArg(q,0)= newVariable(mb,GDKstrdup("tuples"),TYPE_wrd);
			(void) pushArgument(mb,q, getArg(p,3));
		}
		if ( p->token== ENDsymbol || p->barrier == RETURNsymbol || p->barrier == YIELDsymbol){
			if (rtime ){
				q = newStmt(mb, "alarm", "usec");
				r = newStmt1(mb, calcRef, "-");
				r = pushArgument(mb, r, getArg(q,0));
				r = pushArgument(mb, r, rtime);
				getArg(r,0)=rtime;
			} else {
				q = newAssignment(mb);
				q = pushLng(mb,q,0);
				rtime= getArg(q,0)= newVariable(mb,GDKstrdup("rtime"),TYPE_lng);
			}
			if (returnseen && p->token == ENDsymbol) {
				pushInstruction(mb,p);
				continue;
			}
			returnseen++;
			/*
			 * @-
			 */
			q = newStmt(mb, "profiler", "getFootprint");
			foot = getArg(q,0)= newVariable(mb,GDKstrdup("foot"),TYPE_lng);
			q = newStmt(mb, "profiler", "getMemory");
			memory= getArg(q,0)= newVariable(mb,GDKstrdup("memory"),TYPE_lng);

			q = newStmt(mb, "profiler", "getDiskWrites");
			r = newStmt1(mb, calcRef, "-");
			r = pushArgument(mb, r, getArg(q,0));
			r = pushArgument(mb, r, oublock);
			getArg(r,0)=oublock;

			q = newStmt(mb, "profiler", "getDiskReads");
			r = newStmt1(mb, calcRef, "-");
			r = pushArgument(mb, r, getArg(q,0));
			r = pushArgument(mb, r, inblock);
			getArg(r,0)=inblock;

			q = newStmt(mb, "user", "keepcall");
			q = pushArgument(mb, q, getArg(keepQuery,1)); /* query identifier */
			q = pushArgument(mb, q, ctime); /* arguments */
			q = pushArgument(mb, q, arg);   /* arguments */
			q = pushArgument(mb, q, xtime); 
			q = pushArgument(mb, q, rtime); 
			q = pushArgument(mb, q, foot); 
			q = pushArgument(mb, q, memory); 
			q = pushArgument(mb, q, tuples); 
			q = pushArgument(mb, q, inblock);/* inblock */
			(void) pushArgument(mb, q, oublock);/* oublock */
			pushInstruction(mb,p);
			continue;
		}

		pushInstruction(mb,p);
		if (p->barrier == YIELDsymbol){
			/* the factory yield may return */
			q = newStmt(mb, "mtime", "current_timestamp");
			ctime= getArg(q,0)= newVariable(mb,GDKstrdup("ctime"),TYPE_any);
			q = newStmt1(mb, sqlRef, "argRecord");
			for ( argc=1; argc < old[0]->argc; argc++)
				q = pushArgument(mb, q, getArg(old[0],argc));
			arg= getArg(q,0)= newVariable(mb,GDKstrdup("args"),TYPE_str);
				q = newAssignment(mb);
				q = pushLng(mb,q,0);
				memory= getArg(q,0)= newVariable(mb,GDKstrdup("memory"),TYPE_lng);
				q = newAssignment(mb);
				q = pushWrd(mb,q,0);
				tuples= getArg(q,0)= newVariable(mb,GDKstrdup("tuples"),TYPE_wrd);
			newFcnCall(mb,"profiler","setFootprintFlag");
			newFcnCall(mb,"profiler","setMemoryFlag");
			q->argc--;
			pushWrd(mb,q,1);
			q = newStmt(mb, "profiler", "getDiskReads");
			inblock = getArg(q,0)= newVariable(mb,GDKstrdup("inblock"),TYPE_lng);
			q = newStmt(mb, "profiler", "getDiskWrites");
			oublock= getArg(q,0)= newVariable(mb,GDKstrdup("oublock"),TYPE_lng);
			q = newStmt(mb, "alarm", "usec");
			xtime = getArg(q,0)= newVariable(mb,GDKstrdup("xtime"),TYPE_lng);
		}
	}

	for( ; i<slimit; i++)
		if(old[i])
			freeInstruction(old[i]);
	GDKfree(old);
	return 1;
}
