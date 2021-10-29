/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_for.h"

#if 0
static InstrPtr
ReplaceWithNil(MalBlkPtr mb, InstrPtr p, int pos, int tpe)
{
	p = pushNil(mb, p, tpe); /* push at end */
	getArg(p, pos) = getArg(p, p->argc-1);
	p->argc--;
	return p;
}
#endif

static int
allConstExcept(MalBlkPtr mb, InstrPtr p, int except)
{
		for(int j=p->retc; j< p->argc; j++) {
			if (j != except && getArgType(mb, p, j) >= TYPE_any)
				return 0;
		}
		return 1;
}

str
OPTforImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, k, limit, slimit;
	InstrPtr p=0, *old=NULL;
	int actions = 0;
	int *varisfor=NULL, *varforvalue=NULL;
	str msg= MAL_SUCCEED;

	(void) cntxt;
	(void) stk;		/* to fool compilers */

	if (mb->inlineProp)
		goto wrapup;

	varisfor = GDKzalloc(2 * mb->vtop * sizeof(int));
	varforvalue = GDKzalloc(2 * mb->vtop * sizeof(int));
	if (varisfor == NULL || varforvalue == NULL)
		goto wrapup;

	limit = mb->stop;
	slimit = mb->ssize;
	old = mb->stmt;
	if (newMalBlkStmt(mb, mb->ssize) < 0) {
		GDKfree(varisfor);
		GDKfree(varforvalue);
		throw(MAL,"optimizer.for", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	// Consolidate the actual need for variables
	for (i = 0; i < limit; i++) {
		p = old[i];
		if( p == 0)
			continue; //left behind by others?
		if (p->retc == 1 && getModuleId(p) == forRef && getFunctionId(p) == decompressRef) {
			// remember we have encountered a for decompress function
			k =  getArg(p,0);
			varisfor[k] = getArg(p,1);
			varforvalue[k] = getArg(p, 2);
			continue;
		}
		int done = 0;
		for(j=p->retc; j< p->argc; j++){
			k = getArg(p,j);
			if (varisfor[k]) { // maybe we could delay this usage
				if (getModuleId(p) == algebraRef && getFunctionId(p) == projectionRef) {
					/* projection(cand, col) with col = for.decompress(o,min_val)
					 * v1 = projection(cand, o)
					 * for.decompress(v1, min_val) */
					InstrPtr r = copyInstruction(p);
					int tpe = getVarType(mb, varisfor[k]);
					int l = getArg(r, 0);
					getArg(r, 0) = newTmpVariable(mb, tpe);
					getArg(r, j) = varisfor[k];
					varisfor[l] = getArg(r,0);
					varforvalue[l] = varforvalue[k];
					pushInstruction(mb,r);
					done = 1;
					break;
				} else if (p->argc == 2 && p->retc == 1 && getFunctionId(p) == NULL) {
					/* a = b */
					int l = getArg(p, 0);
					varisfor[l] = varisfor[k];
					varforvalue[l] = varforvalue[k];
					done = 1;
					break;
				} else if (getModuleId(p) == algebraRef && getFunctionId(p) == subsliceRef) {
					/* pos = subslice(col, l, h) with col = for.decompress(o,min_val)
					 * pos = subslice(o, l, h) */
					InstrPtr r = copyInstruction(p);
					getArg(r, j) = varisfor[k];
					pushInstruction(mb,r);
					done = 1;
					break;
#if 0
				} else if (isSelect(p)) {
					if (getFunctionId(p) == thetaselectRef) {
						InstrPtr r = newInstructionArgs(mb, forRef, thetaselectRef, 6);

						getArg(r, 0) = getArg(p, 0);
						addArgument(mb, r, varisdict[k]);
						addArgument(mb, r, getArg(p, 2)); /* cand */
						addArgument(mb, r, vardictvalue[k]);
						addArgument(mb, r, getArg(p, 3)); /* val */
						addArgument(mb, r, getArg(p, 4)); /* op */
						pushInstruction(mb,r);
					} else if (getFunctionId(p) == selectRef && p->argc == 9) {
						/* select (c, s, l, h, li, hi, anti, unknown ) */
						InstrPtr r = newInstructionArgs(mb, dictRef, selectRef, 10);

						getArg(r, 0) = getArg(p, 0);
						addArgument(mb, r, varisdict[k]);
						addArgument(mb, r, getArg(p, 2)); /* cand */
						addArgument(mb, r, vardictvalue[k]);
						addArgument(mb, r, getArg(p, 3)); /* l */
						addArgument(mb, r, getArg(p, 4)); /* h */
						addArgument(mb, r, getArg(p, 5)); /* li */
						addArgument(mb, r, getArg(p, 6)); /* hi */
						addArgument(mb, r, getArg(p, 7)); /* anti */
						addArgument(mb, r, getArg(p, 8)); /* unknown */
						pushInstruction(mb,r);
					} else {
						/* pos = select(col, cand, l, h, ...) with col = dict.decompress(o,u)
					 	 * tp = select(u, nil, l, h, ...)
						 * tp2 = batcalc.bte/sht/int(tp)
						 * pos = intersect(o, tp2, cand, nil) */

						int cand = getArg(p, j+1);
						InstrPtr r = copyInstruction(p);
						getArg(r, j) = vardictvalue[k];
						if (cand)
							r = ReplaceWithNil(mb, r, j+1, TYPE_bat); /* no candidate list */
						pushInstruction(mb,r);

						int tpe = getVarType(mb, varisdict[k]);
						InstrPtr s = newInstructionArgs(mb, dictRef, putName("convert"), 3);
						getArg(s, 0) = newTmpVariable(mb, tpe);
						addArgument(mb, s, getArg(r, 0));
						pushInstruction(mb,s);

						InstrPtr t = newInstructionArgs(mb, algebraRef, intersectRef, 9);
						getArg(t, 0) = getArg(p, 0);
						addArgument(mb, t, varisdict[k]);
						addArgument(mb, t, getArg(s, 0));
						addArgument(mb, t, cand);
						t = pushNil(mb, t, TYPE_bat);
						t = pushBit(mb, t, TRUE);    /* nil matches */
						t = pushBit(mb, t, TRUE);     /* max_one */
						t = pushNil(mb, t, TYPE_lng); /* estimate */
						pushInstruction(mb,t);
					}
					done = 1;
					break;
				} else if (j == 2 && p->argc > j+1 && getModuleId(p) == algebraRef && getFunctionId(p) == joinRef
						&& varisdict[getArg(p, j+1)] && vardictvalue[k] == vardictvalue[getArg(p, j+1)]) {
					/* (r1, r2) = join(col1, col2, cand1, cand2, ...) with
					 *		col1 = dict.decompress(o1,u1), col2 = dict.decompress(o2,u2)
					 *		iff u1 == u2
					 *			(r1, r2) = algebra.join(o1, o2, cand1, cand2, ...) */
					int l = getArg(p, j+1);
					InstrPtr r = copyInstruction(p);
					getArg(r, j+0) = varisdict[k];
					getArg(r, j+1) = varisdict[l];
					pushInstruction(mb,r);
					done = 1;
					break;
				} else if (j == 2 && p->argc > j+1 && getModuleId(p) == algebraRef && getFunctionId(p) == joinRef
						&& varisdict[getArg(p, j+1)] && vardictvalue[k] != vardictvalue[getArg(p, j+1)]) {
					/* (r1, r2) = join(col1, col2, cand1, cand2, ...) with
					 *		col1 = dict.decompress(o1,u1), col2 = dict.decompress(o2,u2)
					 * (r1, r2) = dict.join(o1, u1, o2, u2, cand1, cand2, ...) */
					int l = getArg(p, j+1);
					InstrPtr r = newInstructionArgs(mb, dictRef, joinRef, 10);
					assert(p->argc==8);
					getArg(r, 0) = getArg(p, 0);
					r = pushReturn(mb, r, getArg(p, 1));
					r = addArgument(mb, r, varisdict[k]);
					r = addArgument(mb, r, vardictvalue[k]);
					r = addArgument(mb, r, varisdict[l]);
					r = addArgument(mb, r, vardictvalue[l]);
					r = addArgument(mb, r, getArg(p, 4));
					r = addArgument(mb, r, getArg(p, 5));
					r = addArgument(mb, r, getArg(p, 6));
					r = addArgument(mb, r, getArg(p, 7));
					pushInstruction(mb,r);
					done = 1;
					break;
#endif
				} else if ((isMapOp(p) || isMap2Op(p)) && (getFunctionId(p) == plusRef || getFunctionId(p) == minusRef)  && allConstExcept(mb, p, j)) {
					/* batcalc.-(1, col) with col = for.decompress(o,min_val)
					 * v1 = batcalc.-(1, min_val)
					 * for.decompress(o, v1) */
					/* we assume binary operators only ! */
					InstrPtr r = newInstructionArgs(mb, calcRef, getFunctionId(p), 3);
					int tpe = getBatType(getVarType(mb, getArg(p,0)));
					getArg(r, 0) = newTmpVariable(mb, tpe);
					int l = getArg(r, 0), m = getArg(p, 0);
					addArgument(mb, r, getArg(p, 1));
					addArgument(mb, r, getArg(p, 2));
					getArg(r, j) = varforvalue[k];

					/* new and old result are now min-values */
					varisfor[l] = varisfor[m] = varisfor[k];
					varforvalue[l] = varforvalue[m] = getArg(r,0);
					pushInstruction(mb,r);
					done = 1;
					break;
				} else if (getModuleId(p) == groupRef &&
						(getFunctionId(p) == subgroupRef || getFunctionId(p) == subgroupdoneRef ||
						 getFunctionId(p) == groupRef || getFunctionId(p) == groupdoneRef)) {
					/* group.group[done](col) | group.subgroup[done](col, grp) with col = for.decompress(o,min_val)
					 * v1 = group.group[done](o) | group.subgroup[done](o, grp) */
					int input = varisfor[k];
					InstrPtr r = copyInstruction(p);
					getArg(r, j) = input;
					pushInstruction(mb,r);
					done = 1;
					break;
				} else {
					/* need to decompress */
					int tpe = getArgType(mb, p, j);
					InstrPtr r = newInstructionArgs(mb, forRef, decompressRef, 3);
					getArg(r, 0) = newTmpVariable(mb, tpe);
					r = addArgument(mb, r, varisfor[k]);
					r = addArgument(mb, r, varforvalue[k]);
					pushInstruction(mb, r);

					getArg(p, j) = getArg(r, 0);
				}
			}
		}
		if (!done)
			pushInstruction(mb, p);
	}

	for(; i<slimit; i++)
		if (old[i])
			pushInstruction(mb, old[i]);
	/* Defense line against incorrect plans */
	if (actions > 0){
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
	/* keep all actions taken as a post block comment */
wrapup:
	/* keep actions taken as a fake argument*/
	(void) pushInt(mb, pci, actions);

	GDKfree(old);
	GDKfree(varisfor);
	GDKfree(varforvalue);
	return msg;
}
