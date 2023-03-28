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

	limit = mb->stop;

	for (i = 0; i < limit; i++) {
		p = mb->stmt[i];
		if (p && p->retc == 1 && getModuleId(p) == forRef && getFunctionId(p) == decompressRef) {
			break;
		}
	}
	if (i == limit)
		goto wrapup;			/* nothing to do */

	varisfor = GDKzalloc(2 * mb->vtop * sizeof(int));
	varforvalue = GDKzalloc(2 * mb->vtop * sizeof(int));
	if (varisfor == NULL || varforvalue == NULL)
		goto wrapup;

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
			freeInstruction(p);
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
					freeInstruction(p);
					done = 1;
					break;
				} else if (p->argc == 2 && p->retc == 1 && p->barrier == ASSIGNsymbol) {
					/* a = b */
					int l = getArg(p, 0);
					varisfor[l] = varisfor[k];
					varforvalue[l] = varforvalue[k];
					freeInstruction(p);
					done = 1;
					break;
				} else if (getModuleId(p) == algebraRef && getFunctionId(p) == subsliceRef) {
					/* pos = subslice(col, l, h) with col = for.decompress(o,min_val)
					 * pos = subslice(o, l, h) */
					InstrPtr r = copyInstruction(p);
					getArg(r, j) = varisfor[k];
					pushInstruction(mb,r);
					freeInstruction(p);
					done = 1;
					break;
				} else if ((getModuleId(p) == batRef && getFunctionId(p) == mirrorRef) || (getModuleId(p) == batcalcRef && getFunctionId(p) == identityRef)) {
					/* id = mirror/identity(col) with col = for.decompress(o,min_val)
					 * id = mirror/identity(o) */
					InstrPtr r = copyInstruction(p);
					getArg(r, j) = varisfor[k];
					pushInstruction(mb,r);
					freeInstruction(p);
					done = 1;
					break;
				} else// if (isSelect(p)) {
					if (getFunctionId(p) == thetaselectRef) {
						/* pos = thetaselect(col, cand, l, ...) with col = for.decompress(o, minval)
						 * l = calc.-(l, minval);
						 * nl = calc.bte(l);
						 * or
						 * nl = calc.sht(l);
					 	 * pos = select(o, cand, nl,  ...) */

						InstrPtr q = newInstructionArgs(mb, calcRef, minusRef, 3);
						int tpe = getVarType(mb, getArg(p,3));
						getArg(q, 0) = newTmpVariable(mb, tpe);
						q = addArgument(mb, q, getArg(p, 3));
						q = addArgument(mb, q, varforvalue[k]);
						pushInstruction(mb,q);

						InstrPtr r;
						tpe = getBatType(getVarType(mb, varisfor[k]));
						if (tpe == TYPE_bte)
							r = newInstructionArgs(mb, calcRef, putName("bte"), 2);
						else
							r = newInstructionArgs(mb, calcRef, putName("sht"), 2);
						getArg(r, 0) = newTmpVariable(mb, tpe);
						r = addArgument(mb, r, getArg(q, 0));
						pushInstruction(mb,r);

						q = copyInstruction(p);
						getArg(q, j) = varisfor[k];
						getArg(q, 3) = getArg(r, 0);
						pushInstruction(mb,q);
#if 0
					} else if (getFunctionId(p) == selectRef && p->argc == 9) {
						/* select (c, s, l, h, li, hi, anti, unknown ) */
						InstrPtr r = newInstructionArgs(mb, dictRef, selectRef, 10);

						getArg(r, 0) = getArg(p, 0);
						r = addArgument(mb, r, varisdict[k]);
						r = addArgument(mb, r, getArg(p, 2)); /* cand */
						r = addArgument(mb, r, vardictvalue[k]);
						r = addArgument(mb, r, getArg(p, 3)); /* l */
						r = addArgument(mb, r, getArg(p, 4)); /* h */
						r = addArgument(mb, r, getArg(p, 5)); /* li */
						r = addArgument(mb, r, getArg(p, 6)); /* hi */
						r = addArgument(mb, r, getArg(p, 7)); /* anti */
						r = addArgument(mb, r, getArg(p, 8)); /* unknown */
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
						s = addArgument(mb, s, getArg(r, 0));
						pushInstruction(mb,s);

						InstrPtr t = newInstructionArgs(mb, algebraRef, intersectRef, 9);
						getArg(t, 0) = getArg(p, 0);
						t = addArgument(mb, t, varisdict[k]);
						t = addArgument(mb, t, getArg(s, 0));
						t = addArgument(mb, t, cand);
						t = pushNil(mb, t, TYPE_bat);
						t = pushBit(mb, t, TRUE);    /* nil matches */
						t = pushBit(mb, t, TRUE);     /* max_one */
						t = pushNil(mb, t, TYPE_lng); /* estimate */
						pushInstruction(mb,t);
					}
#endif
					freeInstruction(p);
					done = 1;
					break;
				} else if ((isMapOp(p) || isMap2Op(p)) && (getFunctionId(p) == plusRef || getFunctionId(p) == minusRef) &&
							p->argc > 2 && getBatType(getArgType(mb, p, 2)) != TYPE_oid && allConstExcept(mb, p, j)) {
					/* filter out unary batcalc.- with and without a candidate list */
					/* batcalc.-(1, col) with col = for.decompress(o,min_val)
					 * v1 = calc.-(1, min_val)
					 * for.decompress(o, v1) */
					/* we assume binary operators only ! */
					InstrPtr r = newInstructionArgs(mb, calcRef, getFunctionId(p), 3);
					int tpe = getBatType(getVarType(mb, getArg(p,0)));
					getArg(r, 0) = newTmpVariable(mb, tpe);
					int l = getArg(r, 0), m = getArg(p, 0);
					r = addArgument(mb, r, getArg(p, 1));
					r = addArgument(mb, r, getArg(p, 2));
					getArg(r, j) = varforvalue[k];

					/* new and old result are now min-values */
					varisfor[l] = varisfor[m] = varisfor[k];
					varforvalue[l] = varforvalue[m] = getArg(r,0);
					pushInstruction(mb,r);
					freeInstruction(p);
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
					freeInstruction(p);
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
					actions++;
				}
			}
		}
		if (done)
			actions++;
		else
			pushInstruction(mb, p);
	}

	for(; i<slimit; i++)
		if (old[i])
			freeInstruction(old[i]);
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
