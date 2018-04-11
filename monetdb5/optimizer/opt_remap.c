/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * The first attempt of the multiple optimizer is to locate
 * a properly typed multi-plexed implementation.
 * The policy is to search for bat<mod>.<fcn> before going
 * into the iterator code generation.
 */
#include "monetdb_config.h"
#include "opt_remap.h"
#include "opt_macro.h"
#include "opt_multiplex.h"

static int
OPTremapDirect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, Module scope){
	str mod,fcn;
	char buf[1024];
	int i, retc = pci->retc;
	InstrPtr p;
	str bufName, fcnName;

	(void) cntxt;
	(void) stk;
	mod = VALget(&getVar(mb, getArg(pci, retc+0))->value);
	fcn = VALget(&getVar(mb, getArg(pci, retc+1))->value);

	if(strncmp(mod,"bat",3)==0)
		mod+=3;
#ifdef DEBUG_OPT_REMAP
	fprintf(stderr,"#Found a candidate %s.%s\n",mod,fcn);
#endif

	snprintf(buf,1024,"bat%s",mod);
	bufName = putName(buf);
	fcnName = putName(fcn);
	if(bufName == NULL || fcnName == NULL)
		return 0;

	p= newInstruction(mb, bufName, fcnName);

	for(i=0; i<pci->retc; i++)
		if (i<1)
			getArg(p,i) = getArg(pci,i);
		else
			p = pushReturn(mb, p, getArg(pci,i));
	p->retc= p->argc= pci->retc;
	for(i= pci->retc+2; i<pci->argc; i++)
		p= pushArgument(mb,p,getArg(pci,i));
#ifdef DEBUG_OPT_REMAP
	fprintInstruction(stderr,mb,0,p,LIST_MAL_ALL);
#endif

	/* now see if we can resolve the instruction */
	typeChecker(scope,mb,p,TRUE);
	if( p->typechk== TYPE_UNKNOWN) {
#ifdef DEBUG_OPT_REMAP
		fprintf(stderr,"#type error\n");
		fprintInstruction(stderr,mb,0,p,LIST_MAL_ALL);
#endif

		freeInstruction(p);
		return 0;
	}
	pushInstruction(mb,p);
#ifdef DEBUG_OPT_REMAP
	fprintf(stderr,"success\n");
#endif
	return 1;
}

/*
 * Multiplex inline functions should be done with care.
 * The approach taken is to make a temporary copy of the function to be inlined.
 * To change all the statements to reflect the new situation
 * and, if no error occurs, replaces the target instruction
 * with this new block.
 *
 * By the time we get here, we know that function is
 * side-effect free.
 *
 * The multiplex upgrade is targeted at all function
 * arguments whose actual is a BAT and its formal
 * is a scalar.
 * This seems sufficient for the SQL generated PSM code,
 * but does in general not hold.
 * For example,
 *
 * function foo(b:int,c:bat[:oid,:int])
 * 	... d:= batcalc.+(b,c)
 * and
 * multiplex("user","foo",ba:bat[:oid,:int],ca:bat[:oid,:int])
 * upgrades the first argument. The naive upgrade of
 * the statement that would fail. The code below catches
 * most of them by simple prepending "bat" to the MAL function
 * name and leave it to the type resolver to generate the
 * error.
 *
 * The process terminates as soon as we
 * find an instruction that does not have a multiplex
 * counterpart.
 */
static int 
OPTmultiplexInline(Client cntxt, MalBlkPtr mb, InstrPtr p, int pc )
{
	MalBlkPtr mq;
	InstrPtr q = NULL, sig;
	char buf[1024];
	int i,j,k, actions=0;
	int refbat=0, retc = p->retc;
	bit *upgrade;
	Symbol s;
	str msg;


	s= findSymbol(cntxt->usermodule, 
			VALget(&getVar(mb, getArg(p, retc+0))->value),
			VALget(&getVar(mb, getArg(p, retc+1))->value));

	if( s== NULL || !isSideEffectFree(s->def) || 
		getInstrPtr(s->def,0)->retc != p->retc ) {
#ifdef DEBUG_OPT_REMAP
		if( s== NULL)
			fprintf(stderr,"#not found \n");
		else
			fprintf(stderr,"#side-effects\n");
#endif
		return 0;
	}
	/*
	 * Determine the variables to be upgraded and adjust their type
	 */
	if((mq = copyMalBlk(s->def)) == NULL) {
		return 0;
	}
	sig= getInstrPtr(mq,0);
#ifdef DEBUG_OPT_REMAP
	fprintf(stderr,"#Modify the code\n");
	fprintFunction(stderr,mq, 0, LIST_MAL_ALL);
	fprintInstruction(stderr,mb, 0, p,LIST_MAL_ALL);
#endif

	upgrade = (bit*) GDKzalloc(sizeof(bit)*mq->vtop);
	if( upgrade == NULL) {
		freeMalBlk(mq);
		return 0;
	}

	setVarType(mq, 0,newBatType(getArgType(mb,p,0)));
	clrVarFixed(mq,getArg(getInstrPtr(mq,0),0)); /* for typing */
	upgrade[getArg(getInstrPtr(mq,0),0)] = TRUE;

	for(i=3; i<p->argc; i++){
		if( !isaBatType( getArgType(mq,sig,i-2)) &&
			isaBatType( getArgType(mb,p,i)) ){

			if( getBatType(getArgType(mb,p,i)) != getArgType(mq,sig,i-2)){
#ifdef DEBUG_OPT_REMAP
				fprintf(stderr,"#Type mismatch %d\n",i);
#endif
				goto terminateMX;
			}
#ifdef DEBUG_OPT_REMAP
			fprintf(stderr,"#Upgrade type %d %d\n",i, getArg(sig,i-2));
#endif
			setVarType(mq, i-2,newBatType(getArgType(mb,p,i)));
			upgrade[getArg(sig,i-2)]= TRUE;
			refbat= getArg(sig,i-2);
		}
	}
	/*
	 * The next step is to check each instruction of the
	 * to-be-inlined function for arguments that require
	 * an upgrade and resolve it afterwards.
	 */
	for(i=1; i<mq->stop; i++) {
		int fnd = 0;

		q = getInstrPtr(mq,i);
		if (q->token == ENDsymbol)
			break;
		for(j=0; j<q->argc && !fnd; j++) 
			if (upgrade[getArg(q,j)]) {
				for(k=0; k<q->retc; k++){
					setVarType(mq,getArg(q,j),newBatType(getArgType(mq, q, j)));
					/* for typing */
					clrVarFixed(mq,getArg(q,k)); 
					if (!upgrade[getArg(q,k)]) {
						upgrade[getArg(q,k)]= TRUE;
						/* lets restart */
						i = 0;
					}
				}
				fnd = 1;
			}
		/* nil:type -> nil:bat[:oid,:type] */
		if (!getModuleId(q) && q->token == ASSIGNsymbol &&
		    q->argc == 2 && isVarConstant(mq, getArg(q,1)) && 
		    upgrade[getArg(q,0)] &&
			getArgType(mq,q,0) == TYPE_void &&
		    !isaBatType(getArgType(mq, q, 1)) ){
				/* handle nil assignment */
				if( ATOMcmp(getArgGDKType(mq, q, 1),
					VALptr(&getVar(mq, getArg(q,1))->value),
					ATOMnilptr(getArgType(mq, q, 1))) == 0) {
				ValRecord cst;
				int tpe = newBatType(getArgType(mq, q, 1));

				setVarType(mq,getArg(q,0),tpe);
				cst.vtype = TYPE_bat;
				cst.val.bval = bat_nil;
				cst.len = 0;
				getArg(q,1) = defConstant(mq, tpe, &cst);
				setVarType(mq, getArg(q,1), tpe);
			} else{
				/* handle constant tail setting */
				int tpe = newBatType(getArgType(mq, q, 1));

				setVarType(mq,getArg(q,0),tpe);
				setModuleId(q,algebraRef);
				setFunctionId(q,projectRef);
				q= pushArgument(mb,q, getArg(q,1));
				getArg(q,1)= refbat;
			}
		}
	}

	/* now upgrade the statements */
	for(i=1; i<mq->stop; i++){
		q= getInstrPtr(mq,i);
		if( q->token== ENDsymbol)
			break;
		for(j=0; j<q->argc; j++)
			if ( upgrade[getArg(q,j)]){
				if ( blockStart(q) || 
					 q->barrier== REDOsymbol || q->barrier==LEAVEsymbol )
					goto terminateMX;
				if (getModuleId(q)){
					snprintf(buf,1024,"bat%s",getModuleId(q));
					setModuleId(q,putName(buf));
					q->typechk = TYPE_UNKNOWN;

					actions++;
					/* now see if we can resolve the instruction */
					typeChecker(cntxt->usermodule,mq,q,TRUE);
					if( q->typechk== TYPE_UNKNOWN)
						goto terminateMX;
					break;
				}
				/* handle simple upgraded assignments as well */
				if ( q->token== ASSIGNsymbol &&
					 q->argc == 2  &&
					!(isaBatType( getArgType(mq,q,1))) ){
					setModuleId(q,algebraRef);
					setFunctionId(q,projectRef);
					q= pushArgument(mq,q, getArg(q,1));
					getArg(q,1)= refbat;
				
					actions++;
					q->typechk = TYPE_UNKNOWN;
					typeChecker(cntxt->usermodule,mq,q,TRUE);
					if( q->typechk== TYPE_UNKNOWN)
						goto terminateMX;
					break;
				}
		}
	}


	if(mq->errors){
terminateMX:
#ifdef DEBUG_OPT_REMAP
		fprintf(stderr,"Abort remap\n");
		if (q)
			fprintInstruction(stderr,mb,0,q,LIST_MAL_ALL);
#endif
		freeMalBlk(mq);
		GDKfree(upgrade);

		/* ugh ugh, fallback to non inline, but optimized code */
		msg = OPTmultiplexSimple(cntxt, s->def);
		if(msg) 
			freeException(msg);
		s->def->inlineProp = 0;
		return 0;
	}
	/*
	 * We have successfully constructed a variant
	 * of the to-be-inlined function. Put it in place
	 * of the original multiplex.
	 * But first, shift the arguments of the multiplex.
	 */
	delArgument(p,2);
	delArgument(p,1);
	inlineMALblock(mb,pc,mq);
#ifdef DEBUG_OPT_REMAP
	fprintInstruction(stderr,mb,0,p,LIST_MAL_ALL);
	fprintf(stderr,"#NEW BLOCK\n");
	fprintFunction(stderr,mq, 0, LIST_MAL_ALL);
	fprintf(stderr,"#INLINED RESULT\n");
	fprintFunction(stderr,mb, 0, LIST_MAL_ALL);
#endif
	freeMalBlk(mq);
	GDKfree(upgrade);
	return 1;
}
/*
 * The comparison multiplex operations with a constant head may be supported
 * by reverse of the operation.
 */
static struct{
	char *src, *dst;
	int len;
}OperatorMap[]={
{"<", ">",1},
{">", "<",1},
{">=", "<=",2},
{"<=", ">=",2},
{"==", "==",2},
{"!=", "!=",2},
{0,0,0}};

static int
OPTremapSwitched(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, Module scope){
	char *fcn;
	int r,i;
	(void) stk;
	(void) scope;

	if( !isMultiplex(pci) &&
	    !isVarConstant(mb,getArg(pci,1)) &&
	    !isVarConstant(mb,getArg(pci,2)) &&
	    !isVarConstant(mb,getArg(pci,4)) &&
		pci->argc != 5) 
			return 0;
	fcn = VALget(&getVar(mb, getArg(pci, 2))->value);
	for(i=0;OperatorMap[i].src;i++)
	if( strcmp(fcn,OperatorMap[i].src)==0){
		/* found a candidate for a switch */
		getVarConstant(mb, getArg(pci, 2)).val.sval = putNameLen(OperatorMap[i].dst,OperatorMap[i].len);
		getVarConstant(mb, getArg(pci, 2)).len = OperatorMap[i].len;
		r= getArg(pci,3); getArg(pci,3)=getArg(pci,4);getArg(pci,4)=r;
		r= OPTremapDirect(cntxt,mb, stk, pci, scope);

		/* always restore the allocated function name */
		getVarConstant(mb, getArg(pci, 2)).val.sval= fcn;
		assert(strlen(fcn) <= INT_MAX);
		getVarConstant(mb, getArg(pci, 2)).len= (int) strlen(fcn);

		if (r) return 1;

		/* restore the arguments */
		r= getArg(pci,3); getArg(pci,3)=getArg(pci,4);getArg(pci,4)=r;
	}
	return 0;
}

str
OPTremapImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{

	InstrPtr *old, p;
	int i, limit, slimit, doit= 0;
	Module scope = cntxt->usermodule;
	lng usec = GDKusec();
	char buf[256];
	str msg = MAL_SUCCEED;

	(void) pci;
	old = mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;
	if ( newMalBlkStmt(mb, mb->ssize) < 0 )
		throw(MAL,"optmizer.remap", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	for (i = 0; i < limit; i++) {
		p = old[i];
		if (isMultiplex(p)){
			/*
			 * The next step considered is to handle inlined functions.
			 * It means we have already skipped the most obvious ones,
			 * such as the calculator functions. It is particularly
			 * geared at handling the PSM code.
			 */
			str mod = VALget(&getVar(mb, getArg(p, 1))->value);
			str fcn = VALget(&getVar(mb, getArg(p, 2))->value);
			Symbol s = findSymbol(cntxt->usermodule, mod,fcn);

			if (s && s->def->inlineProp ){
#ifdef DEBUG_OPT_REMAP
				fprintf(stderr,"#Multiplex inline\n");
				fprintInstruction(stderr,mb,0,p,LIST_MAL_ALL);
#endif
				pushInstruction(mb, p);
				if( OPTmultiplexInline(cntxt,mb,p,mb->stop-1) )
					doit++;
#ifdef DEBUG_OPT_REMAP
				fprintf(stderr,"#actions %d\n",doit);
#endif
			} else if (OPTremapDirect(cntxt, mb, stk, p, scope) ||
				OPTremapSwitched(cntxt, mb, stk, p, scope)) {
				freeInstruction(p); 
				doit++;
			} else {
				pushInstruction(mb, p);
			}
		} else if (p->argc == 4 && 
			getModuleId(p) == aggrRef && 
			getFunctionId(p) == avgRef) {
			/* group aggr.avg -> aggr.sum/aggr.count */	
			InstrPtr sum, avg,t, iszero;
			InstrPtr cnt;
			sum = copyInstruction(p);
			if( sum == NULL)
				throw(MAL, "remap", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			cnt = copyInstruction(p);
			if( cnt == NULL){
				freeInstruction(sum);
				throw(MAL, "remap", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
			setFunctionId(sum, sumRef);
			setFunctionId(cnt, countRef);
			getArg(sum,0) = newTmpVariable(mb, getArgType(mb, p, 1));
			getArg(cnt,0) = newTmpVariable(mb, newBatType(TYPE_lng));
			pushInstruction(mb, sum);
			pushInstruction(mb, cnt);

			t = newInstruction(mb, batcalcRef, putName("=="));
			getArg(t,0) = newTmpVariable(mb, newBatType(TYPE_bit));
			t = pushArgument(mb, t, getDestVar(cnt));
			t = pushLng(mb, t, 0);
			pushInstruction(mb, t);
			iszero = t;

			t = newInstruction(mb, batcalcRef, dblRef);
			getArg(t,0) = newTmpVariable(mb, getArgType(mb, p, 0));
			t = pushArgument(mb, t, getDestVar(sum));
			pushInstruction(mb, t);
			sum = t;

			t = newInstruction(mb, batcalcRef, putName("ifthenelse"));
			getArg(t,0) = newTmpVariable(mb, getArgType(mb, p, 0));
			t = pushArgument(mb, t, getDestVar(iszero));
			t = pushNil(mb, t, TYPE_dbl);
			t = pushArgument(mb, t, getDestVar(sum));
			pushInstruction(mb, t);
			sum = t;

			t = newInstruction(mb, batcalcRef, dblRef);
			getArg(t,0) = newTmpVariable(mb, getArgType(mb, p, 0));
			t = pushArgument(mb, t, getDestVar(cnt));
			pushInstruction(mb, t);
			cnt = t;

			avg = newInstruction(mb, batcalcRef, divRef);
			getArg(avg, 0) = getArg(p, 0);
			avg = pushArgument(mb, avg, getDestVar(sum));
			avg = pushArgument(mb, avg, getDestVar(cnt));
			freeInstruction(p);
			pushInstruction(mb, avg);
		} else {
			pushInstruction(mb, p);
		}
	}
	for(; i<slimit; i++)
		if( old[i])
			freeInstruction(old[i]);
	GDKfree(old);
#ifdef DEBUG_OPT_REMAP
	if (doit){
		fprintf(stderr,"#After remap, before type check\n");
		fprintFunction(stderr, mb, 0,  LIST_MAL_ALL);
	}
#endif

	if (doit) 
		chkTypes(cntxt->usermodule,mb,TRUE);
    /* Defense line against incorrect plans */
    if( mb->errors == MAL_SUCCEED && doit > 0){
        chkTypes(cntxt->usermodule, mb, FALSE);
        chkFlow(mb);
        chkDeclarations(mb);
    }
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","remap",doit, usec);
    newComment(mb,buf);
	if( doit >= 0)
		addtoMalBlkHistory(mb);

	return msg;
}
