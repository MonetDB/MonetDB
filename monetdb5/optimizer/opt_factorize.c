/*
* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_factorize.h"
#include "mal_interpreter.h"	/* for showErrors() */
#include "mal_builder.h"

/*
 * @+ Factorize Implementation
 * Factorize of the code is defensive. An intruction group is
 * added to the repeated part when analysis gets too complicated,
 * e.g. when a variable is used within a guarded block.
 * Moving instruction out of the guarded block is a separate
 * optimization step.
 */
static int
OPTallowed(InstrPtr p)
{
	if (getModuleId(p) && strcmp(getModuleId(p), "batcalc") == 0) {
		if (isUnsafeInstruction(p))
			return 0;
	}
	return 1;
}

int
OPTfactorizeImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, k,  v, noop = 0, se;
	InstrPtr *mbnew;
	InstrPtr p,sig;
	int fk = 0, sk = 0, blk = 0, blkstart = 0;
	int *varused, returnseen = 0, retvar=0;
	InstrPtr *first, *second;
	char buf[256];
	lng usec= GDKusec();

	(void) cntxt;
	(void) pci;
	(void) stk;		/* to fool compilers */

	setVariableScope(mb);
	varused = GDKmalloc(mb->vtop * sizeof(int));
	if ( varused == NULL)
		return 0;
	
	for (i = 0; i < mb->vtop; i++)
		varused[i] = 0;

	/* add parameters to use list */
	sig = getInstrPtr(mb, 0);
	for (i = 0; i < sig->argc; i++)
		varused[i] = 1;

	first = (InstrPtr *) GDKzalloc(mb->ssize * sizeof(InstrPtr));
	if ( first == NULL){
		GDKfree(varused);
		return 0;
	}
	second = (InstrPtr *) GDKzalloc(mb->ssize * sizeof(InstrPtr));
	if ( second == NULL){
		GDKfree(varused);
		GDKfree(first);
		return 0;
	}

	first[fk++] = getInstrPtr(mb, 0);	/* to become a factory */
	for (i = 1; i < mb->stop - 1; i++) {
		p = getInstrPtr(mb, i);
		se = 0;
		for (k = 0; k < p->argc; k++)
			if (varused[p->argv[k]])
				se++;

		/* detect blocks they are moved to the second part */
		/* a more clever scheme can be designed though */
		if (p->barrier) {
			if (p->barrier == BARRIERsymbol || p->barrier == CATCHsymbol) {
				if (blkstart == 0)
					blkstart = i;
				blk++;
			} else if (p->barrier == EXITsymbol) {
				blk--;
				if (blk == 0)
					blkstart = 0;
			}
		}

		/* beware, none of the target variables may live
		   before the cut point.  */
		for (k = 0; k < p->retc; k++)
			if (getBeginScope(mb, p->argv[k])< i || !OPTallowed(p))
				se = 0;
		if (p->barrier == RETURNsymbol) {
			se = 1;
			p->barrier = YIELDsymbol;
			returnseen = 1;
			retvar= getArg(p,0);
		}

		if (se == 0 && blk == 0)
			first[fk++] = p;
		else {
			if (blkstart) {
				/* copy old block stuff */
				for (k = blkstart; k < i; k++)
					second[sk++] = first[k];
				fk = blkstart;
				blkstart = 0;
			}
			second[sk++] = p;
			for (k = 0; k < p->retc; k++)
				varused[p->argv[k]] = 1;
		}
	}
	second[sk++] = getInstrPtr(mb, i);
	/* detect need for factorization, assume so */
	if (noop || sk == 0) {
		GDKfree(varused);
		GDKfree(first);
		GDKfree(second);
		/* remove the FToptimizer request */
		return 1;
	}

	first[0]->token = FACTORYsymbol;

	mbnew = (InstrPtr *) GDKmalloc((mb->stop + 4) * sizeof(InstrPtr));
	if ( mbnew == NULL) {
		GDKfree(varused);
		GDKfree(first);
		GDKfree(second);
		return 0;
	}
	GDKfree(mb->stmt);
	mb->stmt = mbnew;

	mb->stop = mb->stop + 4;

	k = 0;
	for (i = 0; i < fk; i++)
		mb->stmt[k++] = first[i];

	/* added control block */
	v = newVariable(mb, GDKstrdup("always"), TYPE_bit);
	p = newInstruction(NULL,ASSIGNsymbol);
	p->barrier = BARRIERsymbol;
	getArg(p,0) = v;
	p= pushBit(mb,p,TRUE);
	mb->stmt[k++] = p;

	for (i = 0; i < sk - 1; i++)
		mb->stmt[k++] = second[i];

	/* finalize the factory */
	if (returnseen == 0) {
		p= newInstruction(NULL,ASSIGNsymbol);
		p->barrier = YIELDsymbol;
		getArg(p,0)= getArg(sig,0);
		mb->stmt[k++] = p;
	}
	p = newInstruction(NULL,REDOsymbol);
	p= pushReturn(mb, p, v);
	mb->stmt[k++] = p;

	p = newInstruction(NULL,EXITsymbol);
	p= pushReturn(mb, p, v);
	mb->stmt[k++] = p;

	/* return a nil value */
	if ( getVarType(mb,retvar) != TYPE_void){
		p = newInstruction(NULL,RETURNsymbol);
		getArg(p,0) = getArg(sig,0);
		pushArgument(mb,p, retvar);
		mb->stmt[k++] = p;
	}
	/* add END statement */
	mb->stmt[k++] = second[i];

	mb->stop = k;

	GDKfree(varused);
	GDKfree(first);
	GDKfree(second);

    /* Defense line against incorrect plans */
    if( 1){
        chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
        chkFlow(cntxt->fdout, mb);
        chkDeclarations(cntxt->fdout, mb);
    }
    /* keep all actions taken as a post block comment */
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","factorize",1,GDKusec() - usec);
    newComment(mb,buf);

	return 1;
}
