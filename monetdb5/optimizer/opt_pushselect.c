/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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

int
OPTpushselectImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, limit, slimit, actions=0, *vars, *slices = NULL, push_down_delta = 0, nr_topn = 0, nr_likes = 0;
	char *rslices = NULL;
	InstrPtr p, *old;
	subselect_t subselects;
	char buf[256];
	lng usec = GDKusec();

	memset(&subselects, 0, sizeof(subselects));
	if( mb->errors) 
		return 0;

#ifdef DEBUG_OPT_PUSHSELECT
		mnstr_printf(cntxt->fdout,"#Push select optimizer started\n");
#endif
	(void) stk;
	(void) pci;
	vars= (int*) GDKzalloc(sizeof(int)* mb->vtop);
	if( vars == NULL)
		return 0;

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
			(getFunctionId(p) == subinterRef || 
			 getFunctionId(p) == subdiffRef)) {
			GDKfree(vars);
			goto wrapup;
		}

		if (isSlice(p))
			nr_topn++;

		if (isLikeOp(p))
			nr_likes++;

		if (getModuleId(p) == sqlRef && getFunctionId(p) == deltaRef)
			push_down_delta++;

		if (0 && getModuleId(p) == sqlRef && getFunctionId(p) == tidRef) { /* rewrite equal table ids */
			int sname = getArg(p, 2), tname = getArg(p, 3), s;

			for (s = 0; s < subselects.nr; s++) {
				InstrPtr q = old[vars[subselects.tid[s]]];
				int Qsname = getArg(q, 2), Qtname = getArg(q, 3);

				if (no_updates(old, vars, getArg(q,1), getArg(p,1)) &&
				    ((sname == Qsname && tname == Qtname) ||
				    (0 && strcmp(getVarConstant(mb, sname).val.sval, getVarConstant(mb, Qsname).val.sval) == 0 &&
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
		if (isSubSelect(p) && p->retc == 1 &&
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

		/* rewrite batalgebra.like + subselect -> likesubselect */
		if (getModuleId(p) == algebraRef && p->retc == 1 && getFunctionId(p) == subselectRef) { 
			int var = getArg(p, 1);
			InstrPtr q = mb->stmt[vars[var]]; /* BEWARE: the optimizer may not add or remove statements ! */

			if (isLikeOp(q)) { /* TODO check if getArg(p, 3) value == TRUE */
				InstrPtr r = newInstruction(mb, algebraRef, likesubselectRef);
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
		if (isSubSelect(p) && p->retc == 1) { 
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

	slices = (int*) GDKzalloc(sizeof(int)* mb->vtop);
	rslices = (char*) GDKzalloc(sizeof(char)* mb->vtop);
	if (!slices || !rslices || newMalBlkStmt(mb, mb->stop+(5*push_down_delta)) <0 ) {
		mb->stmt = old;
		GDKfree(vars);
		GDKfree(slices);
		GDKfree(rslices);
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
			if (getModuleId(q) == sqlRef && getFunctionId(q) == projectdeltaRef) {
				InstrPtr r = copyInstruction(p);
				InstrPtr s = copyInstruction(q);

				/* keep new output of slice */
				slices[getArg(s, 1)] = getArg(p, 0); 
				rslices[getArg(p,0)] = 1;
				/* slice the candidates */
				setFunctionId(r, sliceRef);
				getArg(r, 0) = getArg(p, 0); 
				setVarCList(mb,getArg(r,0));
				getArg(r, 1) = getArg(s, 1); 
				pushInstruction(mb,r);

				/* dummy result for the old q, will be removed by deadcode optimizer */
				getArg(q, 0) = newTmpVariable(mb, getArgType(mb, q, 0));

				getArg(s, 1) = getArg(r, 0); /* use result of subslice */
				pushInstruction(mb, s);

				freeInstruction(p);
				old[i] = r; 
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
			InstrPtr r = old[vars[var]];
			
			if (r && isSlice(r) && rslices[getArg(p,1)] != 0 && getArg(r, 0) == getArg(p, 1)) {
				InstrPtr q = newAssignment(mb);

				getArg(q, 0) = getArg(p, 0); 
				(void) pushArgument(mb, q, getArg(p, 2));
				actions++;
				freeInstruction(p);
				old[i] = NULL;
				continue;
			}
		} else if (p->argc >= 2 && slices[getArg(p, 1)] != 0) {
			/* use new slice candidate list */
			getArg(p, 1) = slices[getArg(p, 1)];
		}

		/* c = delta(b, uid, uvl, ins)
		 * s = subselect(c, C1..)
		 *
		 * nc = subselect(b, C1..)
		 * ni = subselect(ins, C1..)
		 * nu = subselect(uvl, C1..)
		 * s = subdelta(nc, uid, nu, ni);
		 *
		 * doesn't handle Xsubselect(x, .. z, C1.. cases) ie multicolumn selects
		 */
		lastbat = lastbat_arg(mb, p);
		if (isSubSelect(p) && p->retc == 1 && lastbat == 2) {
			int var = getArg(p, 1);
			InstrPtr q = old[vars[var]];

			if (q->token == ASSIGNsymbol) {
				var = getArg(q, 1);
				q = old[vars[var]]; 
			}
			if (getModuleId(q) == sqlRef && getFunctionId(q) == deltaRef) {
				InstrPtr r = copyInstruction(p);
				InstrPtr s = copyInstruction(p);
				InstrPtr t = copyInstruction(p);
				InstrPtr u = copyInstruction(q);
		
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
				freeInstruction(p);
				old[i] = NULL;
				continue;
			}
		}
		pushInstruction(mb,p);
	}
	for (; i<limit; i++) 
		if (old[i])
			pushInstruction(mb,old[i]);
	GDKfree(vars);
	GDKfree(slices);
	GDKfree(rslices);
	GDKfree(old);

    /* Defense line against incorrect plans */
    if( actions > 0){
        chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
        chkFlow(cntxt->fdout, mb);
        chkDeclarations(cntxt->fdout, mb);
    }
wrapup:
    /* keep all actions taken as a post block comment */
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","pushselect",actions,GDKusec() - usec);
    newComment(mb,buf);

	return actions;
}
