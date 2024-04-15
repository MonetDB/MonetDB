/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "opt_dict.h"

static inline InstrPtr
ReplaceWithNil(MalBlkPtr mb, InstrPtr p, int pos)
{
	p = pushNilBat(mb, p);	/* push at end */
	getArg(p, pos) = getArg(p, p->argc - 1);
	p->argc--;
	return p;
}

static inline bool
allConstExcept(MalBlkPtr mb, InstrPtr p, int except)
{
	for (int j = p->retc; j < p->argc; j++) {
		if (j != except && getArgType(mb, p, j) >= TYPE_any)
			return false;
	}
	return true;
}

str
OPTdictImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, k, limit, slimit;
	InstrPtr p = NULL, *old = NULL;
	int actions = 0;
	int *varisdict = NULL, *vardictvalue = NULL;
	bit *dictunique = NULL;
	str msg = MAL_SUCCEED;

	(void) stk;					/* to fool compilers */

	if (mb->inlineProp || MB_LARGE(mb))
		goto wrapup1;

	ma_open(cntxt->ta);
	varisdict = ma_zalloc(cntxt->ta, 2 * mb->vtop * sizeof(int));
	vardictvalue = ma_zalloc(cntxt->ta, 2 * mb->vtop * sizeof(int));
	dictunique = ma_zalloc(cntxt->ta, 2 * mb->vtop * sizeof(bit));
	if (varisdict == NULL || vardictvalue == NULL || dictunique == NULL)
		goto wrapup;

	limit = mb->stop;
	slimit = mb->ssize;
	old = mb->stmt;
	if (newMalBlkStmt(mb, mb->ssize) < 0) {
		ma_close(cntxt->ta);
		throw(MAL, "optimizer.dict", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	/* Consolidate the actual need for variables */
	for (i = 0; mb->errors == NULL && i < limit; i++) {
		p = old[i];
		if (p == NULL)
			continue;			/* left behind by others? */
		if (p->retc == 1 && getModuleId(p) == dictRef
			&& getFunctionId(p) == decompressRef) {
			/* remember we have encountered a dict decompress function */
			k = getArg(p, 0);
			varisdict[k] = getArg(p, 1);
			vardictvalue[k] = getArg(p, 2);
			dictunique[k] = 1;
			freeInstruction(p);
			old[i] = NULL;
			continue;
		}
		bool done = false;
		for (j = p->retc; j < p->argc; j++) {
			k = getArg(p, j);
			if (varisdict[k]) {	/* maybe we could delay this usage */
				if (getModuleId(p) == algebraRef
					&& getFunctionId(p) == projectionRef) {
					/* projection(cand, col) with col = dict.decompress(o,u)
					 * v1 = projection(cand, o)
					 * dict.decompress(v1, u) */
					InstrPtr r = copyInstruction(mb, p);
					if (r == NULL) {
						msg = createException(MAL, "optimizer.dict",
											  SQLSTATE(HY013) MAL_MALLOC_FAIL);
						break;
					}
					int tpe = getVarType(mb, varisdict[k]);
					int l = getArg(r, 0);
					getArg(r, 0) = newTmpVariable(mb, tpe);
					getArg(r, j) = varisdict[k];
					varisdict[l] = getArg(r, 0);
					vardictvalue[l] = vardictvalue[k];
					dictunique[l] = dictunique[k];
					pushInstruction(mb, r);
					freeInstruction(p);
					old[i] = NULL;
					done = true;
					break;
				} else if (p->argc == 2 && p->retc == 1
						   && p->barrier == ASSIGNsymbol) {
					/* a = b */
					int l = getArg(p, 0);
					varisdict[l] = varisdict[k];
					vardictvalue[l] = vardictvalue[k];
					dictunique[l] = dictunique[k];
					freeInstruction(p);
					old[i] = NULL;
					done = true;
					break;
				} else if (getModuleId(p) == algebraRef
						   && getFunctionId(p) == subsliceRef) {
					/* pos = subslice(col, l, h) with col = dict.decompress(o,u)
					 * pos = subslice(o, l, h) */
					InstrPtr r = copyInstruction(mb, p);
					if (r == NULL) {
						msg = createException(MAL, "optimizer.dict",
											  SQLSTATE(HY013) MAL_MALLOC_FAIL);
						break;
					}
					getArg(r, j) = varisdict[k];
					pushInstruction(mb, r);
					freeInstruction(p);
					old[i] = NULL;
					done = true;
					break;
				} else if ((getModuleId(p) == batRef
							&& getFunctionId(p) == mirrorRef)
						   || (getModuleId(p) == batcalcRef
							   && getFunctionId(p) == identityRef)) {
					/* id = mirror/identity(col) with col = dict.decompress(o,u)
					 * id = mirror/identity(o) */
					InstrPtr r = copyInstruction(mb, p);
					if (r == NULL) {
						msg = createException(MAL, "optimizer.dict",
											  SQLSTATE(HY013) MAL_MALLOC_FAIL);
						break;
					}
					getArg(r, j) = varisdict[k];
					pushInstruction(mb, r);
					freeInstruction(p);
					old[i] = NULL;
					done = true;
					break;
				} else if (isSelect(p)) {
					if (getFunctionId(p) == thetaselectRef) {
						InstrPtr r = newInstructionArgs(mb, dictRef, thetaselectRef, 6);
						if (r == NULL) {
							msg = createException(MAL, "optimizer.dict",
												  SQLSTATE(HY013)
												  MAL_MALLOC_FAIL);
							break;
						}

						getArg(r, 0) = getArg(p, 0);
						r = pushArgument(mb, r, varisdict[k]);
						r = pushArgument(mb, r, getArg(p, 2));	/* cand */
						r = pushArgument(mb, r, vardictvalue[k]);
						r = pushArgument(mb, r, getArg(p, 3));	/* val */
						r = pushArgument(mb, r, getArg(p, 4));	/* op */
						pushInstruction(mb, r);
					} else if (getFunctionId(p) == selectRef && p->argc == 9) {
						/* select (c, s, l, h, li, hi, anti, unknown ) */
						InstrPtr r = newInstructionArgs(mb, dictRef, selectRef, 10);
						if (r == NULL) {
							msg = createException(MAL, "optimizer.dict",
												  SQLSTATE(HY013)
												  MAL_MALLOC_FAIL);
							break;
						}

						getArg(r, 0) = getArg(p, 0);
						r = pushArgument(mb, r, varisdict[k]);
						r = pushArgument(mb, r, getArg(p, 2));	/* cand */
						r = pushArgument(mb, r, vardictvalue[k]);
						r = pushArgument(mb, r, getArg(p, 3));	/* l */
						r = pushArgument(mb, r, getArg(p, 4));	/* h */
						r = pushArgument(mb, r, getArg(p, 5));	/* li */
						r = pushArgument(mb, r, getArg(p, 6));	/* hi */
						r = pushArgument(mb, r, getArg(p, 7));	/* anti */
						r = pushArgument(mb, r, getArg(p, 8));	/* unknown */
						pushInstruction(mb, r);
					} else {
						/* pos = select(col, cand, l, h, ...) with col = dict.decompress(o,u)
						 * tp = select(u, nil, l, h, ...)
						 * tp2 = batcalc.bte/sht/int(tp)
						 * pos = intersect(o, tp2, cand, nil) */

						int has_cand = getArgType(mb, p, 2) == newBatType(TYPE_oid);
						InstrPtr r = copyInstruction(mb, p);
						InstrPtr s = newInstructionArgs(mb, dictRef, putName("convert"), 3);
						InstrPtr t = newInstructionArgs(mb, algebraRef, intersectRef, 9);
						if (r == NULL || s == NULL || t == NULL) {
							freeInstruction(r);
							freeInstruction(s);
							freeInstruction(t);
							msg = createException(MAL, "optimizer.dict",
												  SQLSTATE(HY013)
												  MAL_MALLOC_FAIL);
							break;
						}

						getArg(r, 0) = newTmpVariable(mb, newBatType(TYPE_oid));
						getArg(r, j) = vardictvalue[k];
						if (has_cand)
							r = ReplaceWithNil(mb, r, 2);	/* no candidate list */
						pushInstruction(mb, r);

						int tpe = getVarType(mb, varisdict[k]);
						getArg(s, 0) = newTmpVariable(mb, tpe);
						s = pushArgument(mb, s, getArg(r, 0));
						pushInstruction(mb, s);

						getArg(t, 0) = getArg(p, 0);
						t = pushArgument(mb, t, varisdict[k]);
						t = pushArgument(mb, t, getArg(s, 0));
						if (has_cand)
							t = pushArgument(mb, t, getArg(p, 2));
						else
							t = pushNilBat(mb, t);
						t = pushNilBat(mb, t);
						t = pushBit(mb, t, TRUE);	/* nil matches */
						t = pushBit(mb, t, TRUE);	/* max_one */
						t = pushNil(mb, t, TYPE_lng);	/* estimate */
						pushInstruction(mb, t);
					}
					freeInstruction(p);
					old[i] = NULL;
					done = true;
					break;
				} else if (j == 2 && p->argc > j + 1
						   && getModuleId(p) == algebraRef
						   && getFunctionId(p) == joinRef
						   && varisdict[getArg(p, j + 1)]
						   && vardictvalue[k] == vardictvalue[getArg(p, j + 1)]) {
					/* (r1, r2) = join(col1, col2, cand1, cand2, ...) with
					 *              col1 = dict.decompress(o1,u1), col2 = dict.decompress(o2,u2)
					 *              iff u1 == u2
					 *                      (r1, r2) = algebra.join(o1, o2, cand1, cand2, ...) */
					int l = getArg(p, j + 1);
					InstrPtr r = copyInstruction(mb, p);
					if (r == NULL) {
						msg = createException(MAL, "optimizer.dict",
											  SQLSTATE(HY013) MAL_MALLOC_FAIL);
						break;
					}
					getArg(r, j + 0) = varisdict[k];
					getArg(r, j + 1) = varisdict[l];
					pushInstruction(mb, r);
					freeInstruction(p);
					old[i] = NULL;
					done = true;
					break;
				} else if (j == 2 && p->argc > j + 1
						   && getModuleId(p) == algebraRef
						   && getFunctionId(p) == joinRef
						   && varisdict[getArg(p, j + 1)]
						   && vardictvalue[k] != vardictvalue[getArg(p, j + 1)]) {
					/* (r1, r2) = join(col1, col2, cand1, cand2, ...) with
					 *              col1 = dict.decompress(o1,u1), col2 = dict.decompress(o2,u2)
					 * (r1, r2) = dict.join(o1, u1, o2, u2, cand1, cand2, ...) */
					int l = getArg(p, j + 1);
					InstrPtr r = newInstructionArgs(mb, dictRef, joinRef, 10);
					if (r == NULL) {
						msg = createException(MAL, "optimizer.dict",
											  SQLSTATE(HY013) MAL_MALLOC_FAIL);
						break;
					}
					assert(p->argc == 8);
					getArg(r, 0) = getArg(p, 0);
					r = pushReturn(mb, r, getArg(p, 1));
					r = pushArgument(mb, r, varisdict[k]);
					r = pushArgument(mb, r, vardictvalue[k]);
					r = pushArgument(mb, r, varisdict[l]);
					r = pushArgument(mb, r, vardictvalue[l]);
					r = pushArgument(mb, r, getArg(p, 4));
					r = pushArgument(mb, r, getArg(p, 5));
					r = pushArgument(mb, r, getArg(p, 6));
					r = pushArgument(mb, r, getArg(p, 7));
					pushInstruction(mb, r);
					freeInstruction(p);
					old[i] = NULL;
					done = true;
					break;
				} else if ((isMapOp(p) || isMap2Op(p))
						   && allConstExcept(mb, p, j)) {
					/* batcalc.-(1, col) with col = dict.decompress(o,u)
					 * v1 = batcalc.-(1, u)
					 * dict.decompress(o, v1) */
					InstrPtr r = copyInstruction(mb, p);
					if (r == NULL) {
						msg = createException(MAL, "optimizer.dict",
											  SQLSTATE(HY013) MAL_MALLOC_FAIL);
						break;
					}
					int tpe = getVarType(mb, getArg(p, 0));
					int l = getArg(r, 0), m = getArg(p, 0);
					getArg(r, 0) = newTmpVariable(mb, tpe);
					getArg(r, j) = vardictvalue[k];

					/* new and old result are now dicts */
					varisdict[l] = varisdict[m] = varisdict[k];
					vardictvalue[l] = vardictvalue[m] = getArg(r, 0);
					dictunique[l] = 0;
					pushInstruction(mb, r);
					freeInstruction(p);
					old[i] = NULL;
					done = true;
					break;
				} else if (getModuleId(p) == groupRef
						   && (getFunctionId(p) == subgroupRef
							   || getFunctionId(p) == subgroupdoneRef
							   || getFunctionId(p) == groupRef
							   || getFunctionId(p) == groupdoneRef)) {
					/* group.group[done](col) | group.subgroup[done](col, grp) with col = dict.decompress(o,u)
					 * v1 = group.group[done](o) | group.subgroup[done](o, grp) */
					int input = varisdict[k];
					if (!dictunique[k]) {
						/* make new dict and renumber the inputs */

						int tpe = getVarType(mb, varisdict[k]);
						/*(o,v) = compress(vardictvalue[k]); */
						InstrPtr r = newInstructionArgs(mb, dictRef, compressRef, 3);
						InstrPtr s = newInstructionArgs(mb, dictRef, renumberRef, 3);
						if (r == NULL || s == NULL) {
							freeInstruction(r);
							freeInstruction(s);
							msg = createException(MAL, "optimizer.dict",
												  SQLSTATE(HY013)
												  MAL_MALLOC_FAIL);
							break;
						}
						/* dynamic type problem ie could be bte or sht, use same type as input dict */
						getArg(r, 0) = newTmpVariable(mb, tpe);
						r = pushReturn(mb, r,
									   newTmpVariable(mb,
													  getArgType(mb, p, j)));
						r = pushArgument(mb, r, vardictvalue[k]);
						pushInstruction(mb, r);

						/* newvar = renumber(varisdict[k], o); */
						getArg(s, 0) = newTmpVariable(mb, tpe);
						s = pushArgument(mb, s, varisdict[k]);
						s = pushArgument(mb, s, getArg(r, 0));
						pushInstruction(mb, s);

						input = getArg(s, 0);
					}
					InstrPtr r = copyInstruction(mb, p);
					if (r == NULL) {
						msg = createException(MAL, "optimizer.dict",
											  SQLSTATE(HY013) MAL_MALLOC_FAIL);
						break;
					}
					getArg(r, j) = input;
					pushInstruction(mb, r);
					freeInstruction(p);
					old[i] = NULL;
					done = true;
					break;
				} else {
					/* need to decompress */
					int tpe = getArgType(mb, p, j);
					InstrPtr r = newInstructionArgs(mb, dictRef, decompressRef, 3);
					if (r == NULL) {
						msg = createException(MAL, "optimizer.dict",
											  SQLSTATE(HY013) MAL_MALLOC_FAIL);
						break;
					}
					getArg(r, 0) = newTmpVariable(mb, tpe);
					r = pushArgument(mb, r, varisdict[k]);
					r = pushArgument(mb, r, vardictvalue[k]);
					pushInstruction(mb, r);

					getArg(p, j) = getArg(r, 0);
					actions++;
				}
			}
		}
		if (msg)
			break;
		if (done)
			actions++;
		else {
			pushInstruction(mb, p);
			old[i] = NULL;
		}
	}

	for (; i < slimit; i++)
		if (old[i])
			freeInstruction(old[i]);
	/* Defense line against incorrect plans */
	if (msg == MAL_SUCCEED && actions > 0) {
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
	/* keep all actions taken as a post block comment */
  wrapup:
	ma_close(cntxt->ta);
	//GDKfree(old);
  wrapup1:
	/* keep actions taken as a fake argument */
	(void) pushInt(mb, pci, actions);

	return msg;
}
