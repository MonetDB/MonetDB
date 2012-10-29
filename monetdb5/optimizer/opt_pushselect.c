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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
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

int
OPTpushselectImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, limit, slimit, actions=0, *vars;
	InstrPtr p, *old;
	subselect_t subselects;

	subselects.nr = 0;
	if( mb->errors) 
		return 0;

	OPTDEBUGpushselect
		mnstr_printf(cntxt->fdout,"#Range select optimizer started\n");
	(void) stk;
	(void) pci;
        vars= (int*) GDKmalloc(sizeof(int)* mb->vtop);
	limit = mb->stop;
	slimit= mb->ssize;
	old = mb->stmt;

	/* check for bailout conditions */
	for (i = 1; i < limit; i++) {
		p = old[i];

		for (j = 0; j<p->retc; j++) {
 			int res = getArg(p, j);
			vars[res] = i;
		}

		if (getModuleId(p) == algebraRef && 
			(getFunctionId(p) == tintersectRef || getFunctionId(p) == tdifferenceRef)) 
			return 0;

		if (getModuleId(p) == sqlRef && getFunctionId(p) == tidRef) { /* rewrite equal tids */
			int sname = getArg(p, 2), tname = getArg(p, 3), s;

			for (s = 0; s < subselects.nr; s++) {
				InstrPtr q = old[vars[subselects.tid[s]]];
				int Qsname = getArg(q, 2), Qtname = getArg(q, 3);

				if (sname == Qsname && tname == Qtname) {
					clrFunction(p);
					p->retc = 1;
					p->argc = 2;
					getArg(p, 1) = getArg(q, 0);
				}
			}
		}
		if (getModuleId(p) == algebraRef &&
		   (getFunctionId(p) == subselectRef || getFunctionId(p) == thetasubselectRef ||
		   (getFunctionId(p) == likesubselectRef && !isaBatType(getArgType(mb, p, 2)) && !isaBatType(getArgType(mb, p, 3)))) && 
		   /* no cand list */ getArgType(mb, p, 2) != newBatType(TYPE_oid, TYPE_oid)) {
			int i1 = getArg(p, 1), tid = 0;
			InstrPtr q = old[vars[i1]];

			/* find the tids */
			while(!tid) {
				if (getModuleId(q) == algebraRef && getFunctionId(q) == leftfetchjoinRef) {
					int i1 = getArg(q, 1);
					InstrPtr s = old[vars[i1]];
	
					if (getModuleId(s) == sqlRef && getFunctionId(s) == tidRef) 
						tid = getArg(q, 1);
					break;
				} else if (getModuleId(q) == batcalcRef && q->argc >= 2 && isaBatType(getArgType(mb, q, 1))) {
					int i1 = getArg(q, 1);
					q = old[vars[i1]];
				} else if (getModuleId(q) == batcalcRef && q->argc >= 3 && isaBatType(getArgType(mb, q, 2))) {
					int i2 = getArg(q, 2);
					q = old[vars[i2]];
				} else {
					break;
				}
			}
			if (tid && subselect_add(&subselects, tid, getArg(p, 0)) < 0) {
				GDKfree(vars);
				return 0;
			}
		}
	}

	if (!subselects.nr || newMalBlkStmt(mb, mb->ssize+20) <0 ) {
		GDKfree(vars);
		return 0;
	}
	pushInstruction(mb,old[0]);

	for (i = 1; i < limit; i++) {
		p = old[i];

		/* inject tids into subselect 
		 * s = subselect(c, C1..) => subselect(c, t, C1..)
		 */
		if (getModuleId(p) == algebraRef && 
		   (getFunctionId(p) == subselectRef || getFunctionId(p) == thetasubselectRef || getFunctionId(p) == likesubselectRef)) { 
			int tid = 0;

			/* if find subselect */
			if ((tid = subselect_find_tids(&subselects, getArg(p, 0))) >= 0) {
				p = PushArgument(mb, p, tid, 2);
				p->token = ASSIGNsymbol; 
				p->typechk = TYPE_UNKNOWN;
        			p->fcn = NULL;
        			p->blk = NULL;
				actions++;
			}
		}
		/* Leftfetchjoins involving rewriten tids need to be flattend
		 * l = leftfetchjoin(t, c); => l = c;
		 *
		 * and
		 *
		 * l = leftfetchjoin(s, ntids); => l = s;
		 */
		else if (getModuleId(p) == algebraRef && getFunctionId(p) == leftfetchjoinRef) {
			int var = getArg(p, 1);
			
			if (subselect_find_subselect(&subselects, var) > 0) {
				InstrPtr q = newAssignment(mb);

				getArg(q, 0) = getArg(p, 0); 
				q = pushArgument(mb, q, getArg(p, 2));
				actions++;
				continue;
			} else { /* deletes/updates use tids */
				int var = getArg(p, 2);
				InstrPtr q = mb->stmt[vars[var]];

				if (q->token == ASSIGNsymbol) {
					var = getArg(q, 1);
					q = mb->stmt[vars[var]];
				}
				if (subselect_find_subselect(&subselects, var) > 0) {
					InstrPtr q = newAssignment(mb);

					getArg(q, 0) = getArg(p, 0); 
					q = pushArgument(mb, q, getArg(p, 1));
					actions++;
					continue;
				}
				/* 
		 		 * c = sql.delta(b,ins,upd);
		 		 * l = leftfetchjoin(x, c); 
		 		 * 
		 		 * into
		 		 *
		 		 * l = sql.project(b,x,ins,upd);
		 		 */
				else if (getModuleId(q) == sqlRef && getFunctionId(q) == deltaRef && q->argc == 4) {
					q = copyInstruction(q);
					setFunctionId(q, delta_projectRef);
					getArg(q, 0) = getArg(p, 0); 
					p = PushArgument(mb, q, getArg(p, 1), 1);
					actions++;
				}
			}
		}
		pushInstruction(mb,p);
	}
	GDKfree(vars);
	for (; i<limit; i++) 
		if (old[i])
			pushInstruction(mb,old[i]);
	for (; i<slimit; i++) 
		if (old[i])
			freeInstruction(old[i]);
	GDKfree(old);
	return actions;
}
