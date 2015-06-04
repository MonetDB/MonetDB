/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * (c) Martin Kersten
 * Implement a parallel sort-merge MAL program generator
 */
#include "monetdb_config.h"
#include "arrange.h"

str
ARNGcreate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int pieces = 3;
	int i, loopvar, bid, arg;
	BUN cnt, step=0,o;
	MalBlkPtr smb;
	MalStkPtr newstk;
	Symbol snew;
	InstrPtr q, pack;
	BAT *b;
	char name[IDLENGTH];
	str msg= MAL_SUCCEED;

	if( pci->argc == 3){
		pieces = stk->stk[pci->argv[2]].val.ival;
	} else {
		// educated guess needed on number of partitions
	}
#ifdef _DEBUG_INDEX_
	mnstr_printf(cntxt->fdout,"#bat.arrange pieces %d\n",pieces);
#endif

	if( pieces <0)
		throw(MAL,"bat.arrange","Positive number expected");

	bid= *getArgReference_bat(stk, pci,1);
	b= BATdescriptor(bid);
	if( b == NULL)
		throw(MAL,"bat.arrange",RUNTIME_OBJECT_MISSING);

	// create a temporary MAL function
	snprintf(name,IDLENGTH,"sort%d",rand() % 1000);
	snew= newFunction(putName("user",4), putName(name, strlen(name)), FUNCTIONsymbol);
	smb=  snew->def;
	q= getInstrPtr(smb, 0);
	arg = newTmpVariable(smb, getArgType(mb,pci,1)) ;
	pushArgument(smb, q, arg);
	getArg(q,0)=  newTmpVariable(smb, TYPE_void);

	resizeMalBlk(smb, 2* pieces +10, 2 * pieces + 10); // large enough
	// create the pack instruction first, as it will hold intermediate variables
	pack = newInstruction(0,ASSIGNsymbol);
	setModuleId(pack,putName("bat",3));
	setFunctionId(pack,putName("arrange",7));
	pack->argv[0]= newTmpVariable(smb, TYPE_void);
	pack = pushArgument(smb,pack, arg);
	setVarFixed(smb,getArg(pack,0));

	// the costly part executed as a parallel block
	loopvar = newTmpVariable(smb,TYPE_bit);
	q = newStmt(smb,putName("language",8),putName("dataflow",8));
	q->barrier= BARRIERsymbol;
	q->argv[0]= loopvar;

	cnt = BATcount(b);
	step = cnt / pieces;
	o = 0;
	for( i=0; i< pieces; i++){
		// add slice instruction
		q = newStmt(smb,putName("algebra",7),putName("slice",5));
		setVarType(smb, getArg(q,0), getArgType(mb,pci,1));
		setVarFixed(smb,getArg(q,0));
		q =  pushArgument(smb, q, arg);
		pack = pushArgument(smb,pack, getArg(q,0));
		q = pushOid(smb,q, o);
		if ( i == pieces -1){
			o= cnt;
		} else
			o+= step;
		q = pushOid(smb,q, o);
	}
	for( i=0; i< pieces; i++){
		// add sort instruction
		q = newStmt(smb, putName("algebra",7), putName("sortorder",9));
		setVarType(smb, getArg(q,0), newBatType(TYPE_oid,TYPE_oid));
		setVarFixed(smb,getArg(q,0));
		q= pushArgument(smb, q, pack->argv[2+i]);
		q= pushBit(smb,q, 0);
		q= pushBit(smb,q, 0);
		pack->argv[2+i]= getArg(q,0);
	}
	// finalize, check, and evaluate
	pushInstruction(smb,pack);
	q= newAssignment(smb);
	q->barrier= EXITsymbol;
	q->argv[0]=loopvar;
	pushEndInstruction(smb);
	chkProgram(cntxt->fdout, cntxt->nspace, smb);
	if( smb->errors)
		msg = createException(MAL,"bat.arrange","Type errors in generated code");
	else {
		// evaluate MAL block
		newstk = prepareMALstack(smb, smb->vsize);
		newstk->up = 0;
		VALcopy(&newstk->stk[arg], &stk->stk[getArg(pci,1)]);
		BBPincref(newstk->stk[arg].val.bval, TRUE);
        msg = runMALsequence(cntxt, smb, 1, 0, newstk, 0, 0);
		freeStack(newstk);
	}
#ifdef _DEBUG_INDEX_
	printFunction(cntxt->fdout, smb, 0, LIST_MAL_ALL);
#endif
	BBPunfix(b->batCacheid);
	// get rid of temporary MAL block
	freeSymbol(snew);
	return msg;
}

str
ARNGmerge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}
