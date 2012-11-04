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
#include "opt_mergetable.h"

typedef enum mat_type_t {
	mat_none = 0,	/* Simple mat aligned operations (ie batcalc etc) */
	mat_grp = 1,	/* result of phase one of a mat - group.new/derive */
	mat_ext = 2,	/* after mat_grp the extend gets a mat.mirror */
	mat_tpn = 3,	/* Phase one of topn on a mat */
	mat_slc = 4,	/* Phase one of topn on a mat */
	mat_rdr = 5	/* Phase one of sorting, ie sorted the parts sofar */
} mat_type_t;

/* TODO have a seperate mat_variable list, ie old_var, mat instruction */
/* including first or second result */

typedef struct mat {
	InstrPtr mi;	/* mat instruction */
	int mv;		/* mat variable */
	mat_type_t type;	/* type of operation */
} mat_t;

static mat_type_t
mat_type( mat_t *mat, int n) 
{
	mat_type_t type = mat_none;
	(void)mat;
	(void)n;
	return type;
}

static int
is_a_mat(int idx, mat_t *mat, int top){
	int i;
	for(i =0; i<top; i++)
		if (mat[i].mv == idx) 
			return i;
	return -1;
}

static int
nr_of_mats(InstrPtr p, mat_t *mat, int mtop)
{
	int j,cnt=0;
	for(j=p->retc; j<p->argc; j++)
		if (is_a_mat(getArg(p,j), mat, mtop) >= 0) 
			cnt++;
	return cnt;
}

static int
nr_of_bats(MalBlkPtr mb, InstrPtr p)
{
	int j,cnt=0;
	for(j=p->retc; j<p->argc; j++)
		if (isaBatType(getArgType(mb,p,j))) 
			cnt++;
	return cnt;
}

#if 0
static void
mat_shift(mat_t *mat, int m, int *mtop)
{
	int j;

	for(j=m; j < *mtop-1; j++)
		mat[j] = mat[j+1];
	*mtop = *mtop-1; 
}
#endif

inline static int
mat_add(mat_t *mat, int mtop, InstrPtr q, mat_type_t type) 
{
	mat[mtop].mi = q;
	mat[mtop].mv = getArg(q,0);
	mat[mtop].type = type;
	return mtop+1;
}
static InstrPtr 
mat_pack(MalBlkPtr mb, mat_t *mat, int m)
{
	InstrPtr r;

	OPTDEBUGmergetable {
		mnstr_printf(GDKout,"#MAT optimizer, mat_pack\n");
		printInstruction(GDKout, mb, 0, mat[m].mi, LIST_MAL_ALL);
	}

	if( mat[m].mi->argc-mat[m].mi->retc == 1){
		/* simple assignment is sufficient */
		r = newInstruction(mb, ASSIGNsymbol);
		getArg(r,0) = getArg(mat[m].mi,0);
		getArg(r,1) = getArg(mat[m].mi,1);
		r->retc = 1;
		r->argc = 2;
	} else {
		int l;

		r = newInstruction(mb, ASSIGNsymbol);
		setModuleId(r, matRef);
		setFunctionId(r, packRef);
		getArg(r,0) = getArg(mat[m].mi, 0);
		for(l=mat[m].mi->retc; l< mat[m].mi->argc; l++)
			r= pushArgument(mb,r, getArg(mat[m].mi,l));
	}
	pushInstruction(mb, r);
	return r;
}

static InstrPtr
mat_delta(MalBlkPtr mb, InstrPtr p, mat_t *mat, int m, int n, int o, int e, int mvar, int nvar, int ovar, int evar)
{
	int tpe, k;
	InstrPtr r = NULL;

	r = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r,matRef);
	setFunctionId(r,newRef);
	getArg(r, 0) = getArg(p,0);
	tpe = getArgType(mb,p,0);

	for(k=1; k < mat[m].mi->argc; k++) {
		InstrPtr q = copyInstruction(p);

		/* remove last argument */
		if (k < mat[m].mi->argc-1)
			q->argc--;
		/* make sure to resolve again */
		q->token = ASSIGNsymbol; 
		q->typechk = TYPE_UNKNOWN;
        	q->fcn = NULL;
        	q->blk = NULL;

		getArg(q, 0) = newTmpVariable(mb, tpe);
		getArg(q, mvar) = getArg(mat[m].mi, k);
		getArg(q, nvar) = getArg(mat[n].mi, k);
		getArg(q, ovar) = getArg(mat[o].mi, k);
		if (e >= 0)
			getArg(q, evar) = getArg(mat[e].mi, k);
		pushInstruction(mb, q);
		r = pushArgument(mb, r, getArg(q, 0));
	}
	return r;
}


static InstrPtr
mat_apply1(MalBlkPtr mb, InstrPtr p, mat_t *mat, int m, int var)
{
	int tpe, k;
	InstrPtr r = NULL;

	r = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r,matRef);
	setFunctionId(r,newRef);
	getArg(r, 0) = getArg(p,0);
	tpe = getArgType(mb,p,0);

	for(k=1; k < mat[m].mi->argc; k++) {
		InstrPtr q = copyInstruction(p);

		getArg(q, 0) = newTmpVariable(mb, tpe);
		getArg(q, var) = getArg(mat[m].mi, k);
		pushInstruction(mb, q);
		r = pushArgument(mb, r, getArg(q, 0));
	}
	return r;
}

static InstrPtr
mat_apply3(MalBlkPtr mb, InstrPtr p, mat_t *mat, int m, int n, int o, int mvar, int nvar, int ovar)
{
	int tpe, k;
	InstrPtr r = NULL;

	r = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r,matRef);
	setFunctionId(r,newRef);
	getArg(r, 0) = getArg(p,0);
	tpe = getArgType(mb,p,0);

	for(k=1; k < mat[m].mi->argc; k++) {
		InstrPtr q = copyInstruction(p);

		getArg(q, 0) = newTmpVariable(mb, tpe);
		getArg(q, mvar) = getArg(mat[m].mi, k);
		getArg(q, nvar) = getArg(mat[n].mi, k);
		getArg(q, ovar) = getArg(mat[o].mi, k);
		pushInstruction(mb, q);
		r = pushArgument(mb, r, getArg(q, 0));
	}
	return r;
}

static InstrPtr
mat_apply2(MalBlkPtr mb, InstrPtr p, mat_t *mat, int m, int n, int mvar, int nvar)
{
	int tpe, k;
	InstrPtr r = NULL;

	r = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r,matRef);
	setFunctionId(r,newRef);
	getArg(r, 0) = getArg(p,0);
	tpe = getArgType(mb,p,0);

	for(k=1; k < mat[m].mi->argc; k++) {
		InstrPtr q = copyInstruction(p);

		getArg(q, 0) = newTmpVariable(mb, tpe);
		getArg(q, mvar) = getArg(mat[m].mi, k);
		getArg(q, nvar) = getArg(mat[n].mi, k);
		pushInstruction(mb, q);
		r = pushArgument(mb, r, getArg(q, 0));
	}
	return r;
}


static char *
aggr_phase2(char *aggr)
{
	if (aggr == countRef || aggr == count_no_nilRef)
		return sumRef;
	/* min/max/sum/prod and unique are fine */
	return aggr;
}

static void
mat_aggr(MalBlkPtr mb, InstrPtr p, mat_t *mat, int m)
{
	int tp = getArgType(mb,p,0), k;
	int battp = (getModuleId(p)==aggrRef)?newBatType(TYPE_oid,tp):tp;
	int v = newTmpVariable(mb, battp);
	InstrPtr r = NULL, s = NULL, q = NULL;

	OPTDEBUGmergetable {
		mnstr_printf(GDKout,"#MAT optimizer, mat_aggr\n");
		printInstruction(GDKout, mb, 0, p, LIST_MAL_ALL);
	}

	/* we pack the partitial result */
	r = newInstruction(mb,ASSIGNsymbol);
	setModuleId(r, matRef);
	setFunctionId(r, packRef);
	getArg(r,0) = v;
	for(k=1; k< mat[m].mi->argc; k++) {
		q = newInstruction(mb,ASSIGNsymbol);
		setModuleId(q,getModuleId(p));
		setFunctionId(q,getFunctionId(p));
		getArg(q,0) = newTmpVariable(mb, tp);
		q = pushArgument(mb,q,getArg(mat[m].mi,k));
		pushInstruction(mb,q);
		
		r = pushArgument(mb,r,getArg(q,0));
	}
	pushInstruction(mb,r);

	if (getModuleId(p) == aggrRef) {
		s = newInstruction(mb,ASSIGNsymbol);
		setModuleId(s, algebraRef);
		setFunctionId(s, selectNotNilRef);
		getArg(s,0) = newTmpVariable(mb, newBatType(TYPE_oid,tp));
		s = pushArgument(mb, s, getArg(r,0));
		pushInstruction(mb, s);
		r = s;
	}

	s = newInstruction(mb,ASSIGNsymbol);
	setModuleId(s,getModuleId(p));
	setFunctionId(s, aggr_phase2(getFunctionId(p)));
	getArg(s,0) = getArg(p,0);
	s = pushArgument(mb, s, getArg(r,0));
	pushInstruction(mb, s);
}

int
OPTmergetableImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p) 
{
	InstrPtr *old;
	mat_t *mat;
	int oldtop, fm, fn, fo, fe, i, k, m, n, o, e, mtop=0, slimit;
	int size, match, actions=0, error=0;

	(void) cntxt;
	/* the number of MATs is limited to the variable stack*/
	mat = (mat_t*) GDKzalloc(mb->vtop * sizeof(mat_t));
	if ( mat == NULL)
		return 0;

	old = mb->stmt;
	oldtop= mb->stop;
	slimit = mb->ssize;
	size = (mb->stop * 1.2 < mb->ssize)? mb->ssize:(int)(mb->stop * 1.2);
	mb->stmt = (InstrPtr *) GDKzalloc(size * sizeof(InstrPtr));
	if ( mb->stmt == NULL){
		mb->stmt = old;
		return 0;
	}
	mb->ssize = size;
	mb->stop = 0;

	for( i=0; i<oldtop; i++){
		int bats = 0;
		InstrPtr r;

		p = old[i];
		if (getModuleId(p) == matRef && 
		   (getFunctionId(p) == newRef || getFunctionId(p) == packRef)){
			mtop = mat_add(mat, mtop, p, mat_none);
			continue;
		}

		if (getModuleId(p) == batcalcRef && 
		   (getFunctionId(p) == mark_grpRef || getFunctionId(p) == dense_rank_grpRef)) { 
			/* Mergetable cannot handle order related batcalc operations */
			error++;
			goto fail;
		}
		/*
		 * If the instruction does not contain MAT references it can simply be added.
		 * Otherwise we have to decide on either packing them or replacement.
		 */
		if ((match = nr_of_mats(p, mat, mtop)) == 0) {
			pushInstruction(mb, copyInstruction(p));
			continue;
		}
		bats = nr_of_bats(mb, p);

		/*
		 * Aggregate handling is a prime target for optimization.
		 * The simple cases are dealt with first.
		 * Handle the rewrite v:=aggr.count(b) and sum()
		 * And the min/max is as easy
		 */
		if (match == 1 && p->argc == 2 &&
		   ((getModuleId(p)==aggrRef &&
			(getFunctionId(p)== countRef || 
			 getFunctionId(p)== count_no_nilRef || 
			 getFunctionId(p)== minRef ||
			 getFunctionId(p)== maxRef ||
			 getFunctionId(p)== sumRef ||
		    	 getFunctionId(p) == prodRef)) ||
		    (getModuleId(p) == algebraRef &&
		     getFunctionId(p) == tuniqueRef)) &&
			(m=is_a_mat(getArg(p,1), mat, mtop)) >= 0) {
			mat_aggr(mb, p, mat, m);
			actions++;
			continue;
		} 

		for( fm= p->argc-1; fm>p->retc ; fm--)
			if ((m=is_a_mat(getArg(p,fm), mat, mtop)) >= 0)
				break;

		for( fn= fm-1; fn>p->retc ; fn--)
			if ((n=is_a_mat(getArg(p,fn), mat, mtop)) >= 0)
				break;

		for( fo= fn-1; fo>p->retc ; fo--)
			if ((o=is_a_mat(getArg(p,fo), mat, mtop)) >= 0)
				break;

		for( fe= fo-1; fe>p->retc ; fe--)
			if ((e=is_a_mat(getArg(p,fe), mat, mtop)) >= 0)
				break;

		/* delta* operator have a ins bat as last argument, we move the inserts into the last delta statement, ie
  		 * all but last need to remove one argument */
		if (match == 3 && bats == 4 && isDelta(p) && 
		   (m=is_a_mat(getArg(p,fm), mat, mtop)) >= 0 &&
		   (n=is_a_mat(getArg(p,fn), mat, mtop)) >= 0 &&
		   (o=is_a_mat(getArg(p,fo), mat, mtop)) >= 0){
			if ((r = mat_delta(mb, p, mat, m, n, o, -1, fm, fn, fo, 0)) != NULL)
				mtop = mat_add(mat, mtop, r, mat_type(mat, m));
			actions++;
			continue;
		}
		if (match == 4 && bats == 5 && isDelta(p) && 
		   (m=is_a_mat(getArg(p,fm), mat, mtop)) >= 0 &&
		   (n=is_a_mat(getArg(p,fn), mat, mtop)) >= 0 &&
		   (o=is_a_mat(getArg(p,fo), mat, mtop)) >= 0 &&
		   (e=is_a_mat(getArg(p,fe), mat, mtop)) >= 0){
			if ((r = mat_delta(mb, p, mat, m, n, o, e, fm, fn, fo, fe)) != NULL)
				mtop = mat_add(mat, mtop, r, mat_type(mat, m));
			actions++;
			continue;
		}

		/* subselect on insert, should use last tid only */
		if (match == 1 && fm == 2 && getModuleId(p) == algebraRef && p->retc == 1 &&
		   (getFunctionId(p) == subselectRef || getFunctionId(p) == thetasubselectRef || getFunctionId(p) == likesubselectRef) &&
		   (m=is_a_mat(getArg(p,fm), mat, mtop)) >= 0) {
			r = copyInstruction(p);
			getArg(r, fm) = getArg(mat[m].mi, mat[m].mi->argc-1);
			pushInstruction(mb, r);
			actions++;
			continue;
		}

		if (match == 3 && bats == 3 && (isFragmentGroup(p) || isFragmentGroup2(p) || isMapOp(p)) &&  p->retc != 2 &&
		   (m=is_a_mat(getArg(p,fm), mat, mtop)) >= 0 &&
		   (n=is_a_mat(getArg(p,fn), mat, mtop)) >= 0 &&
		   (o=is_a_mat(getArg(p,fo), mat, mtop)) >= 0){
			assert(mat[m].mi->argc == mat[n].mi->argc); 
			if ((r = mat_apply3(mb, p, mat, m, n, o, fm, fn, fo)) != NULL)
				mtop = mat_add(mat, mtop, r, mat_type(mat, m));
			actions++;
			continue;
		}
		if (match == 2 && bats == 2 && (isFragmentGroup(p) || isFragmentGroup2(p) || isMapOp(p)) &&  p->retc != 2 &&
		   (m=is_a_mat(getArg(p,fm), mat, mtop)) >= 0 &&
		   (n=is_a_mat(getArg(p,fn), mat, mtop)) >= 0){
			assert(mat[m].mi->argc == mat[n].mi->argc); 
			if ((r = mat_apply2(mb, p, mat, m, n, fm, fn)) != NULL)
				mtop = mat_add(mat, mtop, r, mat_type(mat, m));
			actions++;
			continue;
		}

		if (match == 1 && bats == 1 && (isFragmentGroup(p) || isMapOp(p) || 
		   (!getModuleId(p) && !getFunctionId(p) && p->barrier == 0 /* simple assignment */)) && p->retc != 2 && 
		   (m=is_a_mat(getArg(p,fm), mat, mtop)) >= 0){
			if ((r = mat_apply1(mb, p, mat, m, fm)) != NULL)
				mtop = mat_add(mat, mtop, r, mat_type(mat, m));
			actions++;
			continue;
		}

		/*
		 * All other instructions should be checked for remaining MAT dependencies.
		 * It requires MAT materialization.
		 */
		OPTDEBUGmergetable mnstr_printf(GDKout, "# %s.%s %d\n", getModuleId(p), getFunctionId(p), match);

		for (k = p->retc; k<p->argc; k++) {
			if((m=is_a_mat(getArg(p,k), mat, mtop)) >= 0){
				mat_pack(mb, mat, m);
				actions++;
			}
		}
		pushInstruction(mb, copyInstruction(p));
	}
	(void) stk; 
	chkTypes(cntxt->fdout, cntxt->nspace,mb, TRUE);

	OPTDEBUGmergetable {
		mnstr_printf(GDKout,"#Result of multi table optimizer\n");
		(void) optimizerCheck(cntxt,mb,"merge test",1,0,0);
		printFunction(GDKout, mb, 0, LIST_MAL_ALL);
	}

	if ( mb->errors == 0) {
		for(i=0; i<slimit; i++)
			if (old[i]) 
				freeInstruction(old[i]);
		GDKfree(old);
	}

fail:
	if (error || mb->errors){
		actions= 0;
		OPTDEBUGmergetable 
			mnstr_printf(GDKout, "## %s.%s\n", getModuleId(p), getFunctionId(p));

		for(i=0; i<mb->stop; i++)
			if (mb->stmt[i])
				freeInstruction(mb->stmt[i]);
		GDKfree(mb->stmt);
		mb->stmt = old;
		mb->ssize = slimit;
		mb->stop = oldtop;
		for(i=0; i<mb->stop; i++) {
			InstrPtr p = mb->stmt[i];
			if (p && getModuleId(p) == matRef && getFunctionId(p) == newRef){
				/* simply drop this function, for the base binding is available */
				p->token = NOOPsymbol;
			}
		}
		OPTDEBUGmergetable mnstr_printf(GDKout,"Result of multi table optimizer FAILED\n");
	}
	DEBUGoptimizers
		mnstr_printf(cntxt->fdout,"#opt_mergetable: %d merge actions\n",actions);
	GDKfree(mat);
	return actions;
}
