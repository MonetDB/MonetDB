/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_pushselect.h"
#include "mal_interpreter.h"	/* for showErrors() */

static InstrPtr
PushArgument(MalBlkPtr mb, InstrPtr p, int arg, int pos)
{
	int i;

	p = pushArgument(mb, p, arg); /* push at end */
	for (i = p->argc-1; i > pos; i--) 
		getArg(p, i) = getArg(p, i-1);
	getArg(p, pos) = arg;
	return p;
}

static InstrPtr
PushNil(MalBlkPtr mb, InstrPtr p, int pos, int tpe)
{
	int i, arg;

	p = pushNil(mb, p, tpe); /* push at end */
	arg = getArg(p, p->argc-1);
	for (i = p->argc-1; i > pos; i--) 
		getArg(p, i) = getArg(p, i-1);
	getArg(p, pos) = arg;
	return p;
}

static InstrPtr
ReplaceWithNil(MalBlkPtr mb, InstrPtr p, int pos, int tpe)
{
	p = pushNil(mb, p, tpe); /* push at end */
	getArg(p, pos) = getArg(p, p->argc-1);
	p->argc--;
	return p;
}


#define MAX_TABLES 64

typedef struct subselect_t {
	int nr;
	int tid[MAX_TABLES];
	int subselect[MAX_TABLES];
} subselect_t;

static int
subselect_add( subselect_t *subselects, int tid, int subselect )
{
	int i;

	for (i = 0; i<subselects->nr; i++) {
		if (subselects->tid[i] == tid) {
			if (subselects->subselect[i] == subselect) 
				return i;
			else
				return -1;
		}
	}
	if (i >= MAX_TABLES)
		return -1;
	subselects->nr++;
	subselects->tid[i] = tid;
	subselects->subselect[i] = subselect;
	return i;
}

static int
subselect_find_tids( subselect_t *subselects, int subselect)
{
	int i;

	for (i = 0; i<subselects->nr; i++) {
		if (subselects->subselect[i] == subselect) {
			return subselects->tid[i];
		}
	}
	return -1;
}

static int
subselect_find_subselect( subselect_t *subselects, int tid)
{
	int i;

	for (i = 0; i<subselects->nr; i++) {
		if (subselects->tid[i] == tid) {
			return subselects->subselect[i];
		}
	}
	return -1;
}

static int
lastbat_arg(MalBlkPtr mb, InstrPtr p)
{
	int i = 0;
	for (i=p->retc; i<p->argc; i++) {
		int type = getArgType(mb, p, i);
		if (!isaBatType(type) && type != TYPE_bat)
			break;
	}
	if (i < p->argc)
		return i-1;
	return 0;
}

/* check for updates inbetween assignment to variables newv and oldv */
static int 
no_updates(InstrPtr *old, int *vars, int oldv, int newv) 
{
	while(newv > oldv) {
		InstrPtr q = old[vars[newv]];

		if (isUpdateInstruction(q)) 
			return 0;
		newv = getArg(q, 1);
	}
	return 1;
}

str
OPTpushselectImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, limit, slimit, actions=0, *vars, *nvars = NULL, *slices = NULL, push_down_delta = 0, nr_topn = 0, nr_likes = 0;
	char *rslices = NULL, *oclean = NULL;
	InstrPtr p, *old;
	subselect_t subselects;
	char buf[256];
	lng usec = GDKusec();

	subselects = (subselect_t) {0};
	if( mb->errors)
		return MAL_SUCCEED;

#ifdef DEBUG_OPT_PUSHSELECT
	fprintf(stderr,"#Push select optimizer started\n");
#endif
	(void) stk;
	(void) pci;
	vars= (int*) GDKzalloc(sizeof(int)* mb->vtop);
	if( vars == NULL)
		throw(MAL,"optimizer.pushselect", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	limit = mb->stop;
	slimit= mb->ssize;
	old = mb->stmt;

	/* check for bailout conditions */
	for (i = 1; i < limit; i++) {
		int lastbat;
		p = old[i];

		for (j = 0; j<p->retc; j++) {
 			int res = getArg(p, j);
			vars[res] = i;
		}

		if (getModuleId(p) == algebraRef && 
			(getFunctionId(p) == intersectRef || 
			 getFunctionId(p) == differenceRef)) {
			GDKfree(vars);
			goto wrapup;
		}

		if (isSlice(p))
			nr_topn++;

		if (isLikeOp(p))
			nr_likes++;

		if (getModuleId(p) == sqlRef && getFunctionId(p) == deltaRef)
			push_down_delta++;

		if (/* DISABLES CODE */ (0) && getModuleId(p) == sqlRef && getFunctionId(p) == tidRef) { /* rewrite equal table ids */
			int sname = getArg(p, 2), tname = getArg(p, 3), s;

			for (s = 0; s < subselects.nr; s++) {
				InstrPtr q = old[vars[subselects.tid[s]]];
				int Qsname = getArg(q, 2), Qtname = getArg(q, 3);

				if (no_updates(old, vars, getArg(q,1), getArg(p,1)) &&
				    ((sname == Qsname && tname == Qtname) ||
				    (/* DISABLES CODE */ (0) && strcmp(getVarConstant(mb, sname).val.sval, getVarConstant(mb, Qsname).val.sval) == 0 &&
				     strcmp(getVarConstant(mb, tname).val.sval, getVarConstant(mb, Qtname).val.sval) == 0))) {
					clrFunction(p);
					p->retc = 1;
					p->argc = 2;
					getArg(p, 1) = getArg(q, 0);
					break;
				}
			}
		}
		lastbat = lastbat_arg(mb, p);
		if (isSelect(p) && p->retc == 1 &&
		   /* no cand list */ getArgType(mb, p, lastbat) != newBatType(TYPE_oid)) {
			int i1 = getArg(p, 1), tid = 0;
			InstrPtr q = old[vars[i1]];

			/* find the table ids */
			while(!tid) {
				if (getModuleId(q) == algebraRef && getFunctionId(q) == projectionRef) {
					int i1 = getArg(q, 1);
					InstrPtr s = old[vars[i1]];
	
					if (getModuleId(s) == sqlRef && getFunctionId(s) == tidRef) 
						tid = getArg(q, 1);
					if (s->argc == 2 && s->retc == 1) {
						int i1 = getArg(s, 1);
						InstrPtr s = old[vars[i1]];
						if (getModuleId(s) == sqlRef && getFunctionId(s) == tidRef) 
							tid = getArg(q, 1);
					}
					break;
				} else if (isMapOp(q) && q->retc == 1 && q->argc >= 2 && isaBatType(getArgType(mb, q, 1))) {
					int i1 = getArg(q, 1);
					q = old[vars[i1]];
				} else if (isMapOp(q) && q->retc == 1 && q->argc >= 3 && isaBatType(getArgType(mb, q, 2))) {
					int i2 = getArg(q, 2);
					q = old[vars[i2]];
				} else {
					break;
				}
			}
			if (tid && subselect_add(&subselects, tid, getArg(p, 0)) < 0) {
				GDKfree(vars);
				goto wrapup;
			}
		}
		/* left hand side */
		if ( (GDKdebug & (1<<15)) &&
		     isMatJoinOp(p) && p->retc == 2) { 
			int i1 = getArg(p, 2), tid = 0;
			InstrPtr q = old[vars[i1]];

			/* find the table ids */
			while(!tid) {
				if (getModuleId(q) == algebraRef && getFunctionId(q) == projectionRef) {
					int i1 = getArg(q, 1);
					InstrPtr s = old[vars[i1]];
	
					if (getModuleId(s) == sqlRef && getFunctionId(s) == tidRef) 
						tid = getArg(q, 1);
					break;
				} else if (isMapOp(q) && q->argc >= 2 && isaBatType(getArgType(mb, q, 1))) {
					int i1 = getArg(q, 1);
					q = old[vars[i1]];
				} else if (isMapOp(q) && q->argc >= 3 && isaBatType(getArgType(mb, q, 2))) {
					int i2 = getArg(q, 2);
					q = old[vars[i2]];
				} else {
					break;
				}
			}
			if (tid && subselect_add(&subselects, tid, getArg(p, 0)) < 0) {
				GDKfree(vars);
				goto wrapup;
			}
		}
		/* right hand side */
		if ( (GDKdebug & (1<<15)) &&
		     isMatJoinOp(p) && p->retc == 2) { 
			int i1 = getArg(p, 3), tid = 0;
			InstrPtr q = old[vars[i1]];

			/* find the table ids */
			while(!tid) {
				if (getModuleId(q) == algebraRef && getFunctionId(q) == projectionRef) {
					int i1 = getArg(q, 1);
					InstrPtr s = old[vars[i1]];
	
					if (getModuleId(s) == sqlRef && getFunctionId(s) == tidRef) 
						tid = getArg(q, 1);
					break;
				} else if (isMapOp(q) && q->argc >= 2 && isaBatType(getArgType(mb, q, 1))) {
					int i1 = getArg(q, 1);
					q = old[vars[i1]];
				} else if (isMapOp(q) && q->argc >= 3 && isaBatType(getArgType(mb, q, 2))) {
					int i2 = getArg(q, 2);
					q = old[vars[i2]];
				} else {
					break;
				}
			}
			if (tid && subselect_add(&subselects, tid, getArg(p, 1)) < 0) {
				GDKfree(vars);
				goto wrapup;
			}
		}
	}

	if ((!subselects.nr && !nr_topn && !nr_likes) || newMalBlkStmt(mb, mb->ssize) <0 ) {
		GDKfree(vars);
		goto wrapup;
	}
	pushInstruction(mb,old[0]);

	for (i = 1; i < limit; i++) {
		p = old[i];

		/* rewrite batalgebra.like + select -> likeselect */
		if (getModuleId(p) == algebraRef && p->retc == 1 && getFunctionId(p) == selectRef) { 
			int var = getArg(p, 1);
			InstrPtr q = mb->stmt[vars[var]]; /* BEWARE: the optimizer may not add or remove statements ! */

			if (isLikeOp(q)) { /* TODO check if getArg(p, 3) value == TRUE */
				InstrPtr r = newInstruction(mb, algebraRef, likeselectRef);
				int has_cand = (getArgType(mb, p, 2) == newBatType(TYPE_oid)); 
				int a, anti = (getFunctionId(q)[0] == 'n'), ignore_case = (getFunctionId(q)[anti?4:0] == 'i');

				getArg(r,0) = getArg(p,0);
				r = pushArgument(mb, r, getArg(q, 1));
				if (has_cand)
					r = pushArgument(mb, r, getArg(p, 2));
				for(a = 2; a<q->argc; a++)
					r = pushArgument(mb, r, getArg(q, a));
				if (r->argc < (4+has_cand))
					r = pushStr(mb, r, ""); /* default esc */ 
				if (r->argc < (5+has_cand))
					r = pushBit(mb, r, ignore_case);
				if (r->argc < (6+has_cand))
					r = pushBit(mb, r, anti);
				freeInstruction(p);
				p = r;
				actions++;
			}
		}

		/* inject table ids into subselect 
		 * s = subselect(c, C1..) => subselect(c, t, C1..)
		 */
		if (isSelect(p) && p->retc == 1) { 
			int tid = 0;

			if ((tid = subselect_find_tids(&subselects, getArg(p, 0))) >= 0) {
				int lastbat = lastbat_arg(mb, p);
				if (getArgType(mb, p, lastbat) == TYPE_bat) /* empty candidate list bat_nil */
					getArg(p, lastbat) = tid;
				else
					p = PushArgument(mb, p, tid, lastbat+1);
				/* make sure to resolve again */
				p->token = ASSIGNsymbol; 
				p->typechk = TYPE_UNKNOWN;
        			p->fcn = NULL;
        			p->blk = NULL;
				actions++;
			}
		}
		else if ( (GDKdebug & (1<<15)) &&
			 isMatJoinOp(p) && p->retc == 2
			 ) { 
			int ltid = 0, rtid = 0, done = 0;
			int range = 0;

			if ((ltid = subselect_find_tids(&subselects, getArg(p, 0))) >= 0 && 
			    (rtid = subselect_find_tids(&subselects, getArg(p, 1))) >= 0) {
				p = PushArgument(mb, p, ltid, 4+range);
				p = PushArgument(mb, p, rtid, 5+range);
				done = 1;
			} else if ((ltid = subselect_find_tids(&subselects, getArg(p, 0))) >= 0) { 
				p = PushArgument(mb, p, ltid, 4+range);
				p = PushNil(mb, p, 5+range, TYPE_bat); 
				done = 1;
			} else if ((rtid = subselect_find_tids(&subselects, getArg(p, 1))) >= 0) {
				p = PushNil(mb, p, 4+range, TYPE_bat); 
				p = PushArgument(mb, p, rtid, 5+range);
				done = 1;
			}
			if (done) {
				p = pushBit(mb, p, FALSE); /* do not match nils */
				p = pushNil(mb, p, TYPE_lng); /* no estimate */

				/* make sure to resolve again */
				p->token = ASSIGNsymbol; 
				p->typechk = TYPE_UNKNOWN;
        			p->fcn = NULL;
        			p->blk = NULL;
				actions++;
			}
		}
		/* Leftfetchjoins involving rewriten table ids need to be flattend
		 * l = projection(t, c); => l = c;
		 * and
		 * l = projection(s, ntids); => l = s;
		 */
		else if (getModuleId(p) == algebraRef && getFunctionId(p) == projectionRef) {
			int var = getArg(p, 1);
			
			if (subselect_find_subselect(&subselects, var) > 0) {
				InstrPtr q = newAssignment(mb);

				getArg(q, 0) = getArg(p, 0); 
				(void) pushArgument(mb, q, getArg(p, 2));
				actions++;
				freeInstruction(p);
				continue;
			} else { /* deletes/updates use table ids */
				int var = getArg(p, 2);
				InstrPtr q = mb->stmt[vars[var]]; /* BEWARE: the optimizer may not add or remove statements ! */

				if (q->token == ASSIGNsymbol) {
					var = getArg(q, 1);
					q = mb->stmt[vars[var]]; 
				}
				if (subselect_find_subselect(&subselects, var) > 0) {
					InstrPtr qq = newAssignment(mb);
					/* TODO: check result */

					getArg(qq, 0) = getArg(p, 0); 
					(void) pushArgument(mb, qq, getArg(p, 1));
					actions++;
					freeInstruction(p);
					continue;
				}
				/* c = sql.delta(b,uid,uval,ins);
		 		 * l = projection(x, c); 
		 		 * into
		 		 * l = sql.projectdelta(x,b,uid,uval,ins);
		 		 */
				else if (getModuleId(q) == sqlRef && getFunctionId(q) == deltaRef && q->argc == 5) {
					q = copyInstruction(q);
					if( q == NULL){
						for (; i<limit; i++) 
							if (old[i])
								pushInstruction(mb,old[i]);
						GDKfree(slices);
						GDKfree(rslices);
						GDKfree(old);
						throw(MAL,"optimizer.pushselect", SQLSTATE(HY001) MAL_MALLOC_FAIL);
					}

					setFunctionId(q, projectdeltaRef);
					getArg(q, 0) = getArg(p, 0); 
					q = PushArgument(mb, q, getArg(p, 1), 1);
					freeInstruction(p);
					p = q;
					actions++;
				}
			}
		}
		pushInstruction(mb,p);
	}
	for (; i<limit; i++) 
		if (old[i])
			pushInstruction(mb,old[i]);
	for (; i<slimit; i++) 
		if (old[i])
			freeInstruction(old[i]);
	GDKfree(old);
	if (!push_down_delta) {
		GDKfree(vars);
		goto wrapup;
	}

	/* now push selects through delta's */
	limit = mb->stop;
	slimit= mb->ssize;
	old = mb->stmt;

	nvars = (int*) GDKzalloc(sizeof(int)* mb->vtop); 
	slices = (int*) GDKzalloc(sizeof(int)* mb->vtop);
	rslices = (char*) GDKzalloc(sizeof(char)* mb->vtop);
	oclean = (char*) GDKzalloc(sizeof(char)* mb->vtop);
	if (!nvars || !slices || !rslices || !oclean || newMalBlkStmt(mb, mb->stop+(5*push_down_delta)) <0 ) {
		mb->stmt = old;
		GDKfree(vars);
		GDKfree(nvars);
		GDKfree(slices);
		GDKfree(rslices);
		GDKfree(oclean);
		goto wrapup;
	}
	pushInstruction(mb,old[0]);

	for (i = 1; i < limit; i++) {
		int lastbat;
		p = old[i];

		for (j = 0; j<p->retc; j++) {
 			int res = getArg(p, j);
			vars[res] = i;
		}

		/* push subslice under projectdelta */
		if (isSlice(p) && p->retc == 1) {
			int var = getArg(p, 1);
			InstrPtr q = old[vars[var]];
			if (q && getModuleId(q) == sqlRef && getFunctionId(q) == projectdeltaRef) {
				InstrPtr r = copyInstruction(p);
				InstrPtr s = copyInstruction(q);

				rslices[getArg(q,0)] = 1; /* mark projectdelta as rewriten */
				rslices[getArg(p,0)] = 1; /* mark slice as rewriten */

				if (r == NULL || s == NULL){
					GDKfree(vars);
					GDKfree(nvars);
					GDKfree(slices);
					GDKfree(rslices);
					GDKfree(oclean);
					GDKfree(old);
					throw(MAL,"optimizer.pushselect", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				}

				/* slice the candidates */
				setFunctionId(r, sliceRef);
				nvars[getArg(p,0)] =  getArg(r, 0) = 
				    newTmpVariable(mb, getArgType(mb, r, 0));
				slices[getArg(q, 1)] = getArg(p, 0); 

				setVarCList(mb,getArg(r,0));
				getArg(r, 1) = getArg(s, 1); 
				pushInstruction(mb,r);

				nvars[getArg(q,0)] =  getArg(s, 0) = 
				    newTmpVariable(mb, getArgType(mb, s, 0));
				getArg(s, 1) = getArg(r, 0); /* use result of slice */
				pushInstruction(mb, s);
				oclean[i] = 1;
				actions++;
				continue;
			}
		}
		/* Leftfetchjoins involving rewriten sliced candidates ids need to be flattend
		 * l = projection(t, c); => l = c;
		 * and
		 * l = projection(s, ntids); => l = s;
		 */
		else if (getModuleId(p) == algebraRef && getFunctionId(p) == projectionRef) {
			int var = getArg(p, 1);
			InstrPtr r = old[vars[var]], q;
			
			if (r && isSlice(r) && rslices[var] && getArg(r, 0) == getArg(p, 1)) {
				int col = getArg(p, 2);

				if (!rslices[col]) { /* was the deltaproject rewriten (sliced) */
					InstrPtr s = old[vars[col]], u = NULL;

					if (s && getModuleId(s) == algebraRef && getFunctionId(s) == projectRef) {
						col = getArg(s, 1);	
						u = s;
					 	s = old[vars[col]];
					}
					if (s && getModuleId(s) == sqlRef && getFunctionId(s) == projectdeltaRef) {
						InstrPtr t = copyInstruction(s);
						if (t == NULL){
							GDKfree(vars);
							GDKfree(nvars);
							GDKfree(slices);
							GDKfree(rslices);
							GDKfree(oclean);
							GDKfree(old);
							throw(MAL,"optimizer.pushselect", SQLSTATE(HY001) MAL_MALLOC_FAIL);
						}

						getArg(t, 1) = nvars[getArg(r, 0)]; /* use result of slice */
						rslices[col] = 1;
						nvars[getArg(s,0)] =  getArg(t, 0) = 
				    			newTmpVariable(mb, getArgType(mb, t, 0));
						pushInstruction(mb, t);
						if (u) { /* add again */
							if((t = copyInstruction(u)) == NULL) {
								GDKfree(vars);
								GDKfree(nvars);
								GDKfree(slices);
								GDKfree(rslices);
								GDKfree(oclean);
								GDKfree(old);
								throw(MAL,"optimizer.pushselect", SQLSTATE(HY001) MAL_MALLOC_FAIL);
							}
							getArg(t, 1) = nvars[getArg(t,1)];
							pushInstruction(mb, t);
						}
					}
				}
				q = newAssignment(mb);
				getArg(q, 0) = getArg(p, 0); 
				(void) pushArgument(mb, q, getArg(p, 2));
				if (nvars[getArg(p, 2)] > 0)
					getArg(q, 1) = nvars[getArg(p, 2)];
				oclean[i] = 1;
				actions++;
				continue;
			}
		} else if (p->argc >= 2 && slices[getArg(p, 1)] != 0) {
			/* use new slice candidate list */
			assert(slices[getArg(p,1)] == nvars[getArg(p,1)]);
			getArg(p, 1) = slices[getArg(p, 1)];
		}
		/* remap */
		for (j = p->retc; j<p->argc; j++) {
 			int var = getArg(p, j);
			if (nvars[var] > 0) {
				getArg(p, j) = nvars[var];
			}
		}

		/* c = delta(b, uid, uvl, ins)
		 * s = select(c, C1..)
		 *
		 * nc = select(b, C1..)
		 * ni = select(ins, C1..)
		 * nu = select(uvl, C1..)
		 * s = subdelta(nc, uid, nu, ni);
		 *
		 * doesn't handle Xselect(x, .. z, C1.. cases) ie multicolumn selects
		 */
		lastbat = lastbat_arg(mb, p);
		if (isSelect(p) && p->retc == 1 && lastbat == 2) {
			int var = getArg(p, 1);
			InstrPtr q = old[vars[var]];

			if (q && q->token == ASSIGNsymbol) {
				var = getArg(q, 1);
				q = old[vars[var]]; 
			}
			if (q && getModuleId(q) == sqlRef && getFunctionId(q) == deltaRef) {
				InstrPtr r = copyInstruction(p);
				InstrPtr s = copyInstruction(p);
				InstrPtr t = copyInstruction(p);
				InstrPtr u = copyInstruction(q);
		
				if( r == NULL || s == NULL || t== NULL ||u == NULL){
					freeInstruction(r);
					freeInstruction(s);
					freeInstruction(t);
					freeInstruction(u);
					GDKfree(vars);
					GDKfree(nvars);
					GDKfree(slices);
					GDKfree(rslices);
					GDKfree(oclean);
					GDKfree(old);
					throw(MAL,"optimizer.pushselect", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				}
				getArg(r, 0) = newTmpVariable(mb, newBatType(TYPE_oid));
				setVarCList(mb,getArg(r,0));
				getArg(r, 1) = getArg(q, 1); /* column */
				r->typechk = TYPE_UNKNOWN;
				pushInstruction(mb,r);
				getArg(s, 0) = newTmpVariable(mb, newBatType(TYPE_oid));
				setVarCList(mb,getArg(s,0));
				getArg(s, 1) = getArg(q, 3); /* updates */
				s = ReplaceWithNil(mb, s, 2, TYPE_bat); /* no candidate list */
				setArgType(mb, s, 2, newBatType(TYPE_oid));
				/* make sure to resolve again */
				s->token = ASSIGNsymbol; 
				s->typechk = TYPE_UNKNOWN;
        			s->fcn = NULL;
        			s->blk = NULL;
				pushInstruction(mb,s);
				getArg(t, 0) = newTmpVariable(mb, newBatType(TYPE_oid));
				setVarCList(mb,getArg(t,0));
				getArg(t, 1) = getArg(q, 4); /* inserts */
				pushInstruction(mb,t);

				setFunctionId(u, subdeltaRef);
				getArg(u, 0) = getArg(p,0);
				getArg(u, 1) = getArg(r,0);
				getArg(u, 2) = getArg(p,2); /* pre-cands */
				getArg(u, 3) = getArg(q,2); /* update ids */
				getArg(u, 4) = getArg(s,0);
				u = pushArgument(mb, u, getArg(t,0));
				u->typechk = TYPE_UNKNOWN;
				pushInstruction(mb,u);	
				oclean[i] = 1;
				continue;
			}
		}
		pushInstruction(mb,p);
	}
	for (j=1; j<i; j++)
		if (old[j] && oclean[j])
			freeInstruction(old[j]);
	for (; i<limit; i++)
		if (old[i])
			freeInstruction(old[i]);
			//pushInstruction(mb,old[i]);
	GDKfree(vars);
	GDKfree(nvars);
	GDKfree(slices);
	GDKfree(rslices);
	GDKfree(oclean);
	GDKfree(old);

    /* Defense line against incorrect plans */
    if( actions > 0){
        chkTypes(cntxt->usermodule, mb, FALSE);
        chkFlow(mb);
        chkDeclarations(mb);
    }
wrapup:
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","pushselect",actions, usec);
    newComment(mb,buf);
	if( actions >= 0)
		addtoMalBlkHistory(mb);

	return MAL_SUCCEED;
}
