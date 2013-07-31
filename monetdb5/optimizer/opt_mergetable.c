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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
*/
#include "monetdb_config.h"
#include "opt_mergetable.h"

typedef enum mat_type_t {
	mat_none = 0,	/* Simple mat aligned operations (ie batcalc etc) */
	mat_grp = 1,	/* result of phase one of a mat - group.new/derive */
	mat_ext = 2,	/* mat_grp extend */
	mat_cnt = 3,	/* mat_grp count */
	mat_tpn = 4,	/* Phase one of topn on a mat */
	mat_slc = 5,	/* Last phase of topn (or just slice) on a mat */
	mat_rdr = 6	/* Phase one of sorting, ie sorted the parts sofar */
} mat_type_t;

typedef struct mat {
	InstrPtr mi;		/* mat instruction */
	InstrPtr org;		/* orignal instruction */
	int mv;			/* mat variable */
	int im;			/* input mat, for attribute of sub relations */
	int pm;			/* parent mat, for sub relations */
	mat_type_t type;	/* type of operation */
	int packed;
	int pushed;		 
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

inline static int
mat_add(mat_t *mat, int mtop, InstrPtr q, mat_type_t type, char *func) 
{
	mat[mtop].mi = q;
	mat[mtop].org = NULL;
	mat[mtop].mv = getArg(q,0);
	mat[mtop].type = type;
	mat[mtop].pm = -1;
	mat[mtop].packed = 0;
	mat[mtop].pushed = 0;
	(void)func;
	//printf (" mtop %d %s\n", mtop, func);
	return mtop+1;
}

/* some mat's have intermediates (with intermediate result variables), therefor
 * we pass the old output mat variable */
inline static int
mat_add_var(mat_t *mat, int mtop, InstrPtr q, InstrPtr p, int var, mat_type_t type, int inputmat, int parentmat) 
{
	mat[mtop].mi = q;
	mat[mtop].org = p;
	mat[mtop].mv = var;
	mat[mtop].type = type;
	mat[mtop].im = inputmat;
	mat[mtop].pm = parentmat;
	mat[mtop].packed = 0;
	mat[mtop].pushed = 1;
	return mtop+1;
}

static void 
mat_pack(MalBlkPtr mb, mat_t *mat, int m)
{
	InstrPtr r;

	if (mat[m].packed)
		return ;
	if((mat[m].mi->argc-mat[m].mi->retc) == 1){
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
	mat[m].packed = 1;
	pushInstruction(mb, r);
}

static void
setPartnr(MalBlkPtr mb, int ivar, int ovar, int pnr)
{
	int tpnr = -1;
	VarPtr partnr = (ivar >= 0)?varGetProp(mb, ivar, toriginProp):NULL;
	ValRecord val;

	if (partnr) {
		varSetProp(mb, ovar, toriginProp, op_eq, &partnr->value);
		tpnr = partnr->value.val.ival;
	}
	val.val.ival = pnr;
	val.vtype = TYPE_int;
	varSetProp(mb, ovar, horiginProp, op_eq, &val);
	(void)tpnr;
	//printf("%d %d ", pnr, tpnr);
}

static void
propagatePartnr(MalBlkPtr mb, int ivar, int ovar, int pnr)
{
	/* prop head ids to tail */
	int tpnr = -1;
	VarPtr partnr = varGetProp(mb, ivar, horiginProp);
	ValRecord val;

	val.val.ival = pnr;
	val.vtype = TYPE_int;
	if (partnr) {
		varSetProp(mb, ovar, toriginProp, op_eq, &partnr->value);
		tpnr = partnr->value.val.ival;
	} 
	varSetProp(mb, ovar, horiginProp, op_eq, &val);
	(void)tpnr;
	//printf("%d %d ", pnr, tpnr);
}

static void
propagateMirror(MalBlkPtr mb, int ivar, int ovar)
{
	/* prop head ids to head and tail */
	VarPtr partnr = varGetProp(mb, ivar, horiginProp);

	if (partnr) {
		varSetProp(mb, ovar, toriginProp, op_eq, &partnr->value);
		varSetProp(mb, ovar, horiginProp, op_eq, &partnr->value);
	} 
}

static int 
overlap( MalBlkPtr mb, int lv, int rv, int lnr, int rnr, int ontails)
{
	VarPtr lpartnr = varGetProp(mb, lv, toriginProp); 
	VarPtr rpartnr = varGetProp(mb, rv, (ontails)?toriginProp:horiginProp); 

	if (!lpartnr && !rpartnr)
		return lnr == rnr;
	if (!rpartnr) 
		return lpartnr->value.val.ival == rnr; 
	if (!lpartnr)
		return rpartnr->value.val.ival == lnr; 
	return lpartnr->value.val.ival == rpartnr->value.val.ival; 
}

static void
mat_set_prop( MalBlkPtr mb, InstrPtr p)
{
	int k, tpe = getArgType(mb, p, 0);

	tpe = getTailType(tpe);
	for(k=1; k < p->argc; k++) {
		setPartnr(mb, -1, getArg(p,k), k);
		if (tpe == TYPE_oid)
			propagateMirror(mb, getArg(p,k), getArg(p,k));
	}
}

static InstrPtr
mat_delta(MalBlkPtr mb, InstrPtr p, mat_t *mat, int m, int n, int o, int e, int mvar, int nvar, int ovar, int evar)
{
	int tpe, k, is_subdelta = (getFunctionId(p) == subdeltaRef);
	InstrPtr r = NULL;

	//printf("# %s.%s(%d,%d,%d,%d)", getModuleId(p), getFunctionId(p), m, n, o, e);

	r = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r,matRef);
	setFunctionId(r,packRef);
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
		setPartnr(mb, is_subdelta?getArg(mat[m].mi, k):-1, getArg(q,0), k);
		r = pushArgument(mb, r, getArg(q, 0));
	}
	return r;
}


static InstrPtr
mat_apply1(MalBlkPtr mb, InstrPtr p, mat_t *mat, int m, int var)
{
	int tpe, k, is_select = isSubSelect(p), is_mirror = (getFunctionId(p) == mirrorRef);
	int is_identity = (getFunctionId(p) == identityRef && getModuleId(p) == batcalcRef);
	int ident_var = 0;
	InstrPtr r = NULL, q;

	//printf("# %s.%s(%d)", getModuleId(p), getFunctionId(p), m);

	r = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r,matRef);
	setFunctionId(r,packRef);
	getArg(r, 0) = getArg(p,0);
	tpe = getArgType(mb,p,0);

	if (is_identity) {
		q = newInstruction(mb, ASSIGNsymbol);
		getArg(q, 0) = newTmpVariable(mb, TYPE_oid);
		q->retc = 1;
		q->argc = 1;
		q = pushOid(mb, q, 0);
		ident_var = getArg(q, 0);
		pushInstruction(mb, q);
	}
	for(k=1; k < mat[m].mi->argc; k++) {
		q = copyInstruction(p);

		getArg(q, 0) = newTmpVariable(mb, tpe);
		if (is_identity)
			getArg(q, 1) = newTmpVariable(mb, TYPE_oid);
		getArg(q, var+is_identity) = getArg(mat[m].mi, k);
		if (is_identity) {
			getArg(q, 3) = ident_var;
			q->retc = 2;
			q->argc = 4;
			/* make sure to resolve again */
			q->token = ASSIGNsymbol; 
			q->typechk = TYPE_UNKNOWN;
        		q->fcn = NULL;
        		q->blk = NULL;
		}
		ident_var = getArg(q, 1);
		pushInstruction(mb, q);
		if (is_mirror || is_identity) {
			propagateMirror(mb, getArg(mat[m].mi, k), getArg(q,0));
		} else if (is_select)
			propagatePartnr(mb, getArg(mat[m].mi, k), getArg(q,0), k);
		else
			setPartnr(mb, -1, getArg(q,0), k);
		r = pushArgument(mb, r, getArg(q, 0));
	}
	return r;
}

static InstrPtr
mat_apply2(MalBlkPtr mb, InstrPtr p, mat_t *mat, int m, int n, int mvar, int nvar)
{
	int tpe, k, is_select = isSubSelect(p);
	InstrPtr r = NULL;

	//printf("# %s.%s(%d,%d)", getModuleId(p), getFunctionId(p), m, n);

	r = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r,matRef);
	setFunctionId(r,packRef);
	getArg(r, 0) = getArg(p,0);
	tpe = getArgType(mb,p,0);

	for(k=1; k < mat[m].mi->argc; k++) {
		InstrPtr q = copyInstruction(p);

		getArg(q, 0) = newTmpVariable(mb, tpe);
		getArg(q, mvar) = getArg(mat[m].mi, k);
		getArg(q, nvar) = getArg(mat[n].mi, k);
		pushInstruction(mb, q);
		if (is_select)
			setPartnr(mb, getArg(q,2), getArg(q,0), k);
		else
			setPartnr(mb, -1, getArg(q,0), k);
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
	setFunctionId(r,packRef);
	getArg(r, 0) = getArg(p,0);
	tpe = getArgType(mb,p,0);

	//printf("# %s.%s(%d,%d,%d)", getModuleId(p), getFunctionId(p), m, n, o);

	for(k=1; k < mat[m].mi->argc; k++) {
		InstrPtr q = copyInstruction(p);

		getArg(q, 0) = newTmpVariable(mb, tpe);
		getArg(q, mvar) = getArg(mat[m].mi, k);
		getArg(q, nvar) = getArg(mat[n].mi, k);
		getArg(q, ovar) = getArg(mat[o].mi, k);
		pushInstruction(mb, q);
		setPartnr(mb, -1, getArg(q,0), k);
		r = pushArgument(mb, r, getArg(q, 0));
	}
	return r;
}


static int
mat_setop(MalBlkPtr mb, InstrPtr p, mat_t *mat, int mtop, int m, int n)
{
	int tpe = getArgType(mb,p, 0), k, j;
	InstrPtr r = newInstruction(mb, ASSIGNsymbol);

	setModuleId(r,matRef);
	setFunctionId(r,packRef);
	getArg(r,0) = getArg(p,0);
	
	//printf("# %s.%s(%d,%d)", getModuleId(p), getFunctionId(p), m, n);
	assert(m>=0 || n>=0);
	if (m >= 0 && n >= 0) {
		int nr = 1;
		for(k=1; k<mat[m].mi->argc; k++) { 
			InstrPtr q = copyInstruction(p);
			InstrPtr s = newInstruction(mb, ASSIGNsymbol);

			setModuleId(s,matRef);
			setFunctionId(s,packRef);
			getArg(s,0) = newTmpVariable(mb, tpe);
	
			for (j=1; j<mat[n].mi->argc; j++) {
				if (overlap(mb, getArg(mat[m].mi, k), getArg(mat[n].mi, j), -1, -2, 1)){
					s = pushArgument(mb,s,getArg(mat[n].mi,j));
				}
			}
			pushInstruction(mb,s);

			getArg(q,0) = newTmpVariable(mb, tpe);
			getArg(q,1) = getArg(mat[m].mi,k);
			getArg(q,2) = getArg(s,0);
			setPartnr(mb, getArg(mat[m].mi,k), getArg(q,0), nr);
			pushInstruction(mb,q);

			r = pushArgument(mb,r,getArg(q,0));
			nr++;
		}
	} else {
		assert(m >= 0);
		for(k=1; k<mat[m].mi->argc; k++) {
			InstrPtr q = copyInstruction(p);

			getArg(q,0) = newTmpVariable(mb, tpe);
			getArg(q,1) = getArg(mat[m].mi, k);
			pushInstruction(mb,q);

			setPartnr(mb, getArg(q, 2), getArg(q,0), k);
			r = pushArgument(mb, r, getArg(q,0));
		}
	}

	mtop = mat_add(mat, mtop, r, mat_none, getFunctionId(p));
	return mtop;
}

static int
mat_leftfetchjoin(MalBlkPtr mb, InstrPtr p, mat_t *mat, int mtop, int m, int n)
{
	int tpe = getArgType(mb,p, 0), k, j;
	InstrPtr r = newInstruction(mb, ASSIGNsymbol);

	setModuleId(r,matRef);
	setFunctionId(r,packRef);
	getArg(r,0) = getArg(p,0);
	
	//printf("# %s.%s(%d,%d)", getModuleId(p), getFunctionId(p), m, n);
	assert(m>=0 || n>=0);
	if (m >= 0 && n >= 0) {
		int nr = 1;
		for(k=1; k<mat[m].mi->argc; k++) { 
			for (j=1; j<mat[n].mi->argc; j++) {
				if (overlap(mb, getArg(mat[m].mi, k), getArg(mat[n].mi, j), k, j, 0)){
					InstrPtr q = copyInstruction(p);

					getArg(q,0) = newTmpVariable(mb, tpe);
					getArg(q,1) = getArg(mat[m].mi,k);
					getArg(q,2) = getArg(mat[n].mi,j);
					pushInstruction(mb,q);
		
					setPartnr(mb, getArg(mat[n].mi, j), getArg(q,0), nr);
					r = pushArgument(mb,r,getArg(q,0));

					nr++;
					break;
				}
			}
		}
	} else {
		assert(m >= 0);
		for(k=1; k<mat[m].mi->argc; k++) {
			InstrPtr q = copyInstruction(p);

			getArg(q,0) = newTmpVariable(mb, tpe);
			getArg(q,1) = getArg(mat[m].mi, k);
			pushInstruction(mb,q);

			setPartnr(mb, getArg(q, 2), getArg(q,0), k);
			r = pushArgument(mb, r, getArg(q,0));
		}
	}

	mtop = mat_add(mat, mtop, r, mat_none, getFunctionId(p));
	return mtop;
}

static int
mat_join2(MalBlkPtr mb, InstrPtr p, mat_t *mat, int mtop, int m, int n)
{
	int tpe = getArgType(mb,p, 0), j,k, nr = 1;
	InstrPtr l = newInstruction(mb, ASSIGNsymbol);
	InstrPtr r = newInstruction(mb, ASSIGNsymbol);

	setModuleId(l,matRef);
	setFunctionId(l,packRef);
	getArg(l,0) = getArg(p,0);

	setModuleId(r,matRef);
	setFunctionId(r,packRef);
	getArg(r,0) = getArg(p,1);

	//printf("# %s.%s(%d,%d)", getModuleId(p), getFunctionId(p), m, n);
	
	assert(m>=0 || n>=0);
	if (m >= 0 && n >= 0) {
		for(k=1; k<mat[m].mi->argc; k++) {
			for (j=1; j<mat[n].mi->argc; j++) {
				InstrPtr q = copyInstruction(p);

				getArg(q,0) = newTmpVariable(mb, tpe);
				getArg(q,1) = newTmpVariable(mb, tpe);
				getArg(q,2) = getArg(mat[m].mi,k);
				getArg(q,3) = getArg(mat[n].mi,j);
				pushInstruction(mb,q);
	
				propagatePartnr(mb, getArg(mat[m].mi, k), getArg(q,0), nr);
				propagatePartnr(mb, getArg(mat[n].mi, j), getArg(q,1), nr);

				/* add result to mat */
				l = pushArgument(mb,l,getArg(q,0));
				r = pushArgument(mb,r,getArg(q,1));
				nr++;
			}
		}
	} else {
		int mv = (m>=0)?m:n;
		int av = (m>=0)?0:1;
		int bv = (m>=0)?1:0;

		for(k=1; k<mat[mv].mi->argc; k++) {
			InstrPtr q = copyInstruction(p);

			getArg(q,0) = newTmpVariable(mb, tpe);
			getArg(q,1) = newTmpVariable(mb, tpe);
			getArg(q,p->retc+av) = getArg(mat[mv].mi, k);
			pushInstruction(mb,q);

			propagatePartnr(mb, getArg(mat[mv].mi, k), getArg(q,av), k);
			propagatePartnr(mb, getArg(p, p->retc+bv), getArg(q,bv), k);

			/* add result to mat */
			l = pushArgument(mb, l, getArg(q,0));
			r = pushArgument(mb, r, getArg(q,1));
		}
	}
	mtop = mat_add(mat, mtop, l, mat_none, getFunctionId(p));
	mtop = mat_add(mat, mtop, r, mat_none, getFunctionId(p));
	return mtop;
}

static int
mat_join3(MalBlkPtr mb, InstrPtr p, mat_t *mat, int mtop, int m, int n, int o)
{
	int tpe = getArgType(mb,p, 0), j,k, nr = 1;
	InstrPtr l = newInstruction(mb, ASSIGNsymbol);
	InstrPtr r = newInstruction(mb, ASSIGNsymbol);

	setModuleId(l,matRef);
	setFunctionId(l,packRef);
	getArg(l,0) = getArg(p,0);

	setModuleId(r,matRef);
	setFunctionId(r,packRef);
	getArg(r,0) = getArg(p,1);

	//printf("# %s.%s(%d,%d)", getModuleId(p), getFunctionId(p), m, n);
	
	assert(m>=0 || n>=0);
	if (m >= 0 && n >= 0 && o >= 0) {
		assert(mat[n].mi->argc == mat[o].mi->argc);
		for(k=1; k<mat[m].mi->argc; k++) {
			for (j=1; j<mat[n].mi->argc; j++) {
				InstrPtr q = copyInstruction(p);

				getArg(q,0) = newTmpVariable(mb, tpe);
				getArg(q,1) = newTmpVariable(mb, tpe);
				getArg(q,2) = getArg(mat[m].mi,k);
				getArg(q,3) = getArg(mat[n].mi,j);
				getArg(q,4) = getArg(mat[o].mi,j);
				pushInstruction(mb,q);
	
				propagatePartnr(mb, getArg(mat[m].mi, k), getArg(q,0), nr);
				propagatePartnr(mb, getArg(mat[n].mi, j), getArg(q,1), nr);

				/* add result to mat */
				l = pushArgument(mb,l,getArg(q,0));
				r = pushArgument(mb,r,getArg(q,1));
				nr++;
			}
		}
	} else {
		int mv = (m>=0)?m:n;
		int av = (m>=0)?0:1;
		int bv = (m>=0)?1:0;

		for(k=1; k<mat[mv].mi->argc; k++) {
			InstrPtr q = copyInstruction(p);

			getArg(q,0) = newTmpVariable(mb, tpe);
			getArg(q,1) = newTmpVariable(mb, tpe);
			getArg(q,p->retc+av) = getArg(mat[mv].mi, k);
			if (o >= 0)
				getArg(q,p->retc+2) = getArg(mat[o].mi, k);
			pushInstruction(mb,q);

			propagatePartnr(mb, getArg(mat[mv].mi, k), getArg(q,av), k);
			propagatePartnr(mb, getArg(p, p->retc+bv), getArg(q,bv), k);

			/* add result to mat */
			l = pushArgument(mb, l, getArg(q,0));
			r = pushArgument(mb, r, getArg(q,1));
		}
	}
	mtop = mat_add(mat, mtop, l, mat_none, getFunctionId(p));
	mtop = mat_add(mat, mtop, r, mat_none, getFunctionId(p));
	return mtop;
}


static char *
aggr_phase2(char *aggr)
{
	if (aggr == countRef || aggr == count_no_nilRef)
		return sumRef;
	if (aggr == subcountRef)
		return subsumRef;
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

static int
chain_by_length(mat_t *mat, int g)
{
	int cnt = 0;
	while(g >= 0) {
		g = mat[g].pm;
		cnt++;
	}
	return cnt;
}

static int
walk_n_back(mat_t *mat, int g, int cnt)
{
	while(cnt > 0){ 
		g = mat[g].pm;
		cnt--;
	}
	return g;
}

static int
group_by_ext(mat_t *mat, int mtop, int g)
{
	int i;

	for(i=g; i< mtop; i++){ 
		if (mat[i].pm == g)
			return i;
	}
	return 0;
}

/* In some cases we have non groupby attribute columns, these require 
 * gext.leftfetchjoin(mat.pack(per partition ext.leftfetchjoins(x))) 
 */

static int
mat_group_project(MalBlkPtr mb, InstrPtr p, mat_t *mat, int mtop, int e, int a)
{
	int tp = getArgType(mb,p,0), k;
	int tail = getTailType(tp);
	InstrPtr ai1 = newInstruction(mb, ASSIGNsymbol), r;

	setModuleId(ai1,matRef);
	setFunctionId(ai1,packRef);
	getArg(ai1,0) = newTmpVariable(mb, tp);

	assert(mat[e].mi->argc == mat[a].mi->argc);
	for(k=1; k<mat[a].mi->argc; k++) {
		InstrPtr q = copyInstruction(p);

		getArg(q,0) = newTmpVariable(mb, tp);
		getArg(q,1) = getArg(mat[e].mi,k);
		getArg(q,2) = getArg(mat[a].mi,k);
		pushInstruction(mb,q);

		/* pack the result into a mat */
		ai1 = pushArgument(mb,ai1,getArg(q,0));
	}
	pushInstruction(mb, ai1);

	r = copyInstruction(p);
	getArg(r,1) = mat[e].mv;
	getArg(r,2) = getArg(ai1,0);
	pushInstruction(mb,r);
	if (tail == TYPE_oid)
		mtop = mat_add_var(mat, mtop, ai1, r, getArg(r, 0), mat_ext,  -1, -1);
	return mtop;
}

static void
mat_group_aggr(MalBlkPtr mb, InstrPtr p, mat_t *mat, int b, int g, int e)
{
	int tp = getArgType(mb,p,0), k;
	char *aggr2 = aggr_phase2(getFunctionId(p));
	InstrPtr ai1 = newInstruction(mb, ASSIGNsymbol), ai2;

	setModuleId(ai1,matRef);
	setFunctionId(ai1,packRef);
	getArg(ai1,0) = newTmpVariable(mb, tp);

	for(k=1; k<mat[b].mi->argc; k++) {
		InstrPtr q = copyInstruction(p);
		getArg(q,0) = newTmpVariable(mb, tp);
		getArg(q,1) = getArg(mat[b].mi,k);
		getArg(q,2) = getArg(mat[g].mi,k);
		getArg(q,3) = getArg(mat[e].mi,k);
		pushInstruction(mb,q);

		/* pack the result into a mat */
		ai1 = pushArgument(mb,ai1,getArg(q,0));
	}
	pushInstruction(mb, ai1);

 	ai2 = newInstruction(mb, ASSIGNsymbol);
	setModuleId(ai2, aggrRef);
	setFunctionId(ai2, aggr2);
	getArg(ai2,0) = getArg(p,0);
	ai2 = pushArgument(mb, ai2, getArg(ai1, 0));
	ai2 = pushArgument(mb, ai2, mat[g].mv);
	ai2 = pushArgument(mb, ai2, mat[e].mv);
	ai2 = pushBit(mb, ai2, 1); /* skip nils */
	if (getFunctionId(p) != subminRef && getFunctionId(p) != submaxRef)
		ai2 = pushBit(mb, ai2, 1);
	pushInstruction(mb, ai2);
}

/* The mat_group_{new,derive} keep an ext,attr1..attrn table.
 * This is the input for the final second phase group by.
 */
static void
mat_pack_group(MalBlkPtr mb, mat_t *mat, int mtop, int g)
{
	int cnt = chain_by_length(mat, g), i;
	InstrPtr cur = NULL;

	for(i=cnt-1; i>=0; i--) {
		InstrPtr grp = newInstruction(mb, ASSIGNsymbol);
		int ogrp = walk_n_back(mat, g, i);
		int oext = group_by_ext(mat, mtop, ogrp);
		int attr = mat[oext].im;

		setModuleId(grp,groupRef);
		setFunctionId(grp, i?subgroupRef:subgroupdoneRef);
		
		getArg(grp,0) = mat[ogrp].mv;
		grp = pushReturn(mb, grp, mat[oext].mv);
		grp = pushReturn(mb, grp, newTmpVariable(mb, newBatType( TYPE_oid, TYPE_wrd)));
		grp = pushArgument(mb, grp, getArg(mat[attr].mi, 0));
		if (cur) 
			grp = pushArgument(mb, grp, getArg(cur, 0));
		pushInstruction(mb, grp);
		cur = grp;
	}
	mat[g].im = -1; /* only pack once */
}

/* 
 * foreach parent subgroup, do the 
 * 	e2.leftfetchjoin(grp.leftfetchjoin((ext.leftfetchjoin(b))) 
 * and one for the current group 
 */
static int
mat_group_attr(MalBlkPtr mb, mat_t *mat, int mtop, int g, InstrPtr cext, int push )
{
        int cnt = chain_by_length(mat, g), i;	/* number of attributes */
	int ogrp = g; 				/* previous group */

	for(i = 0; i < cnt; i++) {
		int agrp = walk_n_back(mat, ogrp, i); 
		int b = mat[agrp].im;
		int aext = group_by_ext(mat, mtop, agrp);
		int a = mat[aext].im;
		int atp = getArgType(mb,mat[a].mi,0), k;
		InstrPtr attr = newInstruction(mb, ASSIGNsymbol);

		setModuleId(attr,matRef);
		setFunctionId(attr,packRef);
		//getArg(attr,0) = newTmpVariable(mb, atp);
		getArg(attr,0) = getArg(mat[b].mi,0);

		for (k = 1; k<mat[a].mi->argc; k++ ) {
			InstrPtr r = newInstruction(mb, ASSIGNsymbol);
			InstrPtr q = newInstruction(mb, ASSIGNsymbol);

			setModuleId(r, algebraRef);
			setFunctionId(r, leftfetchjoinRef);
			getArg(r, 0) = newTmpVariable(mb, newBatType(TYPE_oid,TYPE_oid));
			r = pushArgument(mb, r, getArg(cext,k));
			r = pushArgument(mb, r, getArg(mat[ogrp].mi,k));
			pushInstruction(mb,r);

			setModuleId(q, algebraRef);
			setFunctionId(q, leftfetchjoinRef);
			getArg(q, 0) = newTmpVariable(mb, atp);
			q = pushArgument(mb, q, getArg(r,0));
			q = pushArgument(mb, q, getArg(mat[a].mi,k));
			pushInstruction(mb,q);
	
			attr = pushArgument(mb, attr, getArg(q, 0)); 
		}
		if (push)
			pushInstruction(mb,attr);
		mtop = mat_add_var(mat, mtop, attr, NULL, getArg(attr, 0), mat_ext,  -1, -1);
		mat[mtop-1].pushed = push;
		/* keep new attribute with the group extend */
		mat[aext].im = mtop-1;
	}	
	return mtop;
}

static int
mat_group_new(MalBlkPtr mb, InstrPtr p, mat_t *mat, int mtop, int b)
{
	int tp0 = getArgType(mb,p,0);
	int tp1 = getArgType(mb,p,1);
	int tp2 = getArgType(mb,p,2);
	int atp = getArgType(mb,p,3), i, a, g, push = 0;
	InstrPtr r0, r1, r2, attr;

	if (getFunctionId(p) == subgroupdoneRef)
		push = 1;

	r0 = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r0,matRef);
	setFunctionId(r0,packRef);
	getArg(r0,0) = newTmpVariable(mb, tp0);

	r1 = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r1,matRef);
	setFunctionId(r1,packRef);
	getArg(r1,0) = newTmpVariable(mb, tp1);

	r2 = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r2,matRef);
	setFunctionId(r2,packRef);
	getArg(r2,0) = newTmpVariable(mb, tp2);

	/* we keep an extend, attr table result, which will later be used
	 * when we pack the group result */
	attr = newInstruction(mb, ASSIGNsymbol);
	setModuleId(attr,matRef);
	setFunctionId(attr,packRef);
	getArg(attr,0) = getArg(mat[b].mi,0);

	for(i=1; i<mat[b].mi->argc; i++) {
		InstrPtr q = copyInstruction(p), r;
		getArg(q, 0) = newTmpVariable(mb, tp0);
		getArg(q, 1) = newTmpVariable(mb, tp1);
		getArg(q, 2) = newTmpVariable(mb, tp2);
		getArg(q, 3) = getArg(mat[b].mi, i);
		pushInstruction(mb, q);

		/* add result to mats */
		r0 = pushArgument(mb,r0,getArg(q,0));
		r1 = pushArgument(mb,r1,getArg(q,1));
		r2 = pushArgument(mb,r2,getArg(q,2));

		r = newInstruction(mb, ASSIGNsymbol);
		setModuleId(r, algebraRef);
		setFunctionId(r, leftfetchjoinRef);
		getArg(r, 0) = newTmpVariable(mb, atp);

		r = pushArgument(mb, r, getArg(q,1));
		r = pushArgument(mb, r, getArg(mat[b].mi,i));
		pushInstruction(mb,r);

		attr = pushArgument(mb, attr, getArg(r, 0)); 
	}
	pushInstruction(mb,r0);
	pushInstruction(mb,r1);
	pushInstruction(mb,r2);
	if (push)
		pushInstruction(mb,attr);

	/* create mat's for the intermediates */
	a = mtop = mat_add_var(mat, mtop, attr, NULL, getArg(attr, 0), mat_ext,  -1, -1);
	mat[mtop-1].pushed = push;
	g = mtop = mat_add_var(mat, mtop, r0, p, getArg(p, 0), mat_grp, b, -1);
	mtop = mat_add_var(mat, mtop, r1, p, getArg(p, 1), mat_ext, a-1, mtop-1); /* point back at group */
	mtop = mat_add_var(mat, mtop, r2, p, getArg(p, 2), mat_cnt, -1, mtop-1); /* point back at ext */
	if (push)
		mat_pack_group(mb, mat, mtop, g-1);
	return mtop;
}

static int
mat_group_derive(MalBlkPtr mb, InstrPtr p, mat_t *mat, int mtop, int b, int g)
{
	int tp0 = getArgType(mb,p,0);
	int tp1 = getArgType(mb,p,1);
	int tp2 = getArgType(mb,p,2); 
	int atp = getArgType(mb,p,3), i, a, push = 0; 
	InstrPtr r0, r1, r2, attr;

	if (getFunctionId(p) == subgroupdoneRef)
		push = 1;

	if (mat[g].im == -1){ /* allready packed */
		pushInstruction(mb, copyInstruction(p));
		return mtop;
	}

	r0 = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r0,matRef);
	setFunctionId(r0,packRef);
	getArg(r0,0) = newTmpVariable(mb, tp0);

	r1 = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r1,matRef);
	setFunctionId(r1,packRef);
	getArg(r1,0) = newTmpVariable(mb, tp1);

	r2 = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r2,matRef);
	setFunctionId(r2,packRef);
	getArg(r2,0) = newTmpVariable(mb, tp2);
	
	/* we keep an extend, attr table result, which will later be used
	 * when we pack the group result */
	attr = newInstruction(mb, ASSIGNsymbol);
	setModuleId(attr,matRef);
	setFunctionId(attr,packRef);
	getArg(attr,0) = getArg(mat[b].mi,0);

	/* we need overlapping ranges */
	for(i=1; i<mat[b].mi->argc; i++) {
		InstrPtr q = copyInstruction(p), r;

		getArg(q,0) = newTmpVariable(mb, tp0);
		getArg(q,1) = newTmpVariable(mb, tp1);
		getArg(q,2) = newTmpVariable(mb, tp2);
		getArg(q,3) = getArg(mat[b].mi,i);
		getArg(q,4) = getArg(mat[g].mi,i);
		pushInstruction(mb,q);
	
		/* add result to mats */
		r0 = pushArgument(mb,r0,getArg(q,0));
		r1 = pushArgument(mb,r1,getArg(q,1));
		r2 = pushArgument(mb,r2,getArg(q,2));

		r = newInstruction(mb, ASSIGNsymbol);
		setModuleId(r, algebraRef);
		setFunctionId(r, leftfetchjoinRef);
		getArg(r, 0) = newTmpVariable(mb, atp);

		r = pushArgument(mb, r, getArg(q,1));
		r = pushArgument(mb, r, getArg(mat[b].mi,i));
		pushInstruction(mb,r);

		attr = pushArgument(mb, attr, getArg(r, 0)); 
	}
	pushInstruction(mb,r0);
	pushInstruction(mb,r1);
	pushInstruction(mb,r2);
	if (push)
		pushInstruction(mb,attr);

	mtop = mat_group_attr(mb, mat, mtop, g, r1, push);

	/* create mat's for the intermediates */
	a = mtop = mat_add_var(mat, mtop, attr, NULL, getArg(attr, 0), mat_ext,  -1, -1);
	mat[mtop-1].pushed = push;
	g = mtop = mat_add_var(mat, mtop, r0, p, getArg(p, 0), mat_grp, b, g);
	mtop = mat_add_var(mat, mtop, r1, p, getArg(p, 1), mat_ext, a-1, mtop-1); /* point back at group */
	mtop = mat_add_var(mat, mtop, r2, p, getArg(p, 2), mat_cnt, -1, mtop-1); /* point back at ext */

	if (push)
		mat_pack_group(mb, mat, mtop, g-1);
	return mtop;
}

static void
mat_topn_project(MalBlkPtr mb, InstrPtr p, mat_t *mat, int m, int n)
{
	int tpe = getArgType(mb, p, 0), k;
	InstrPtr pck, q;

	pck = newInstruction(mb, ASSIGNsymbol);
	setModuleId(pck,matRef);
	setFunctionId(pck,packRef);
	getArg(pck,0) = newTmpVariable(mb, tpe);

	for(k=1; k<mat[m].mi->argc; k++) { 
		InstrPtr q = copyInstruction(p);

		getArg(q,0) = newTmpVariable(mb, tpe);
		getArg(q,1) = getArg(mat[m].mi, k);
		getArg(q,2) = getArg(mat[n].mi, k);
		pushInstruction(mb, q);

		pck = pushArgument(mb, pck, getArg(q, 0));
	}
	pushInstruction(mb, pck);

       	q = copyInstruction(p);
	getArg(q,2) = getArg(pck,0);
	pushInstruction(mb, q);
}

static void
mat_pack_topn(MalBlkPtr mb, InstrPtr slc, mat_t *mat, int m)
{
	/* find chain of topn's */
	int cnt = chain_by_length(mat, m), i;
	InstrPtr cur = NULL;

	for(i=cnt-1; i>=0; i--) {
		int otpn = walk_n_back(mat, m, i), var = 1, k;
		int attr = mat[otpn].im;
		int tpe = getVarType(mb, getArg(mat[attr].mi,0));
		InstrPtr pck, tpn, otopn = mat[otpn].org, a;

	        pck = newInstruction(mb, ASSIGNsymbol);
		setModuleId(pck,matRef);
		setFunctionId(pck,packRef);
		getArg(pck,0) = newTmpVariable(mb, tpe);

		/* m.leftfetchjoin(attr); */
		for(k=1; k < mat[attr].mi->argc; k++) {
			InstrPtr q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, leftjoinRef);
			getArg(q, 0) = newTmpVariable(mb, tpe);

			q = pushArgument(mb, q, getArg(slc, k));
			q = pushArgument(mb, q, getArg(mat[attr].mi, k));
			pushInstruction(mb, q);

			pck = pushArgument(mb, pck, getArg(q,0));
		}
		pushInstruction(mb, pck);

		if (cur) {
			InstrPtr mirror = newInstruction(mb, ASSIGNsymbol);
			setModuleId(mirror, batRef);
			setFunctionId(mirror, mirrorRef);
			getArg(mirror, 0) = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid));
			pushArgument(mb, mirror, getArg(cur, 0));
			pushInstruction(mb, mirror);

			a = newInstruction(mb, ASSIGNsymbol);
			setModuleId(a, algebraRef);
			setFunctionId(a, leftjoinRef);
			getArg(a, 0) = newTmpVariable(mb, tpe);
			pushArgument(mb, a, getArg(mirror, 0));
			pushArgument(mb, a, getArg(pck, 0));
			pushInstruction(mb, a);
		} else {
			a = pck;
		}

		tpn = copyInstruction(otopn);
		var = 1;
		if (cur) {
			getArg(tpn, 1) = getArg(cur, 0);
			var++;
		}
		getArg(tpn, var) = getArg(a,0);
		pushInstruction(mb, tpn);
		cur = tpn;
	}
}

static int
mat_topn(MalBlkPtr mb, InstrPtr p, mat_t *mat, int mtop, int m, int n)
{
	int tpe = getArgType(mb,p,0), k, is_slice = isSlice(p), zero = -1;
	InstrPtr pck, q, r;

	/* dummy mat instruction (needed to share result of p) */
	pck = newInstruction(mb,ASSIGNsymbol);
	setModuleId(pck, matRef);
	setFunctionId(pck, packRef);
	getArg(pck,0) = getArg(p,0);

	if (is_slice) {
		ValRecord cst;
		cst.vtype= getArgType(mb,p,2);
		cst.val.wval= 0;
		zero = defConstant(mb, cst.vtype, &cst);
	}
	for(k=1; k< mat[m].mi->argc; k++) {
		q = copyInstruction(p);
		getArg(q,0) = newTmpVariable(mb, tpe);
		getArg(q,1) = getArg(mat[m].mi,k);
		if (is_slice) /* lower bound should always be 0 on partial slices */
			getArg(q,2) = zero;
		else if (n >= 0)
			getArg(q,2) = getArg(mat[n].mi,k);
		pushInstruction(mb,q);
		
		pck = pushArgument(mb, pck, getArg(q,0));
	}

	mtop = mat_add_var(mat, mtop, pck, p, getArg(p,0), is_slice?mat_slc:mat_tpn, (n>=0)?n:m, (n>=0)?m:-1);
	mat[mtop-1].pushed = 0;

	if (is_slice) {
		/* real instruction */
		r = newInstruction(mb,ASSIGNsymbol);
		setModuleId(r, matRef);
		setFunctionId(r, packRef);
		getArg(r,0) = newTmpVariable(mb, tpe);
	
		for(k=1; k< pck->argc; k++) 
			r = pushArgument(mb, r, getArg(pck,k));
		pushInstruction(mb,r);

		if (mat[m].type == mat_tpn) 
			mat_pack_topn(mb, pck, mat, m);

		/* topn/slice over merged parts */
		q = copyInstruction(p);
		if (mat[m].type != mat_tpn) 
			getArg(q,1) = getArg(r,0);
		pushInstruction(mb,q);
	}
	return mtop;
}

int
OPTmergetableImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p) 
{
	InstrPtr *old;
	mat_t *mat;
	int oldtop, fm, fn, fo, fe, i, k, m, n, o, e, mtop=0, slimit;
	int size=0, match, actions=0, distinct_topn = 0, topn_res = 0, groupdone = 0, *vars;

	old = mb->stmt;
	oldtop= mb->stop;

        vars= (int*) GDKmalloc(sizeof(int)* mb->vtop);
	/* check for bailout conditions */
	for (i = 1; i < oldtop; i++) {
		int j;

		p = old[i];

		for (j = 0; j<p->retc; j++) {
 			int res = getArg(p, j);
			vars[res] = i;
		}

		/* pack if there is a group statement following a groupdone (ie aggr(distinct)) */
		if (getModuleId(p) == groupRef && p->argc == 5 && 
		   (getFunctionId(p) == subgroupRef || getFunctionId(p) == subgroupdoneRef)) {
			InstrPtr q = old[vars[getArg(p, p->argc-1)]]; /* group result from a previous group(done) */

			if (getModuleId(q) == groupRef && getFunctionId(q) == subgroupdoneRef)
				groupdone = 1;
		}

		if (isTopn(p))
			topn_res = getArg(p, 0);
		if (getModuleId(p) == algebraRef && getFunctionId(p) == markTRef && getArg(p, 1) == topn_res)
			distinct_topn = 1;
	}
	GDKfree(vars);

	/* the number of MATs is limited to the variable stack*/
	mat = (mat_t*) GDKzalloc(mb->vtop * sizeof(mat_t));
	if ( mat == NULL) 
		return 0;

	slimit = mb->ssize;
	size = (mb->stop * 1.2 < mb->ssize)? mb->ssize:(int)(mb->stop * 1.2);
	mb->stmt = (InstrPtr *) GDKzalloc(size * sizeof(InstrPtr));
	if ( mb->stmt == NULL) {
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
			mat_set_prop(mb, p);
			mtop = mat_add(mat, mtop, p, mat_none, getFunctionId(p));
			mat[mtop-1].pushed = 1;
			continue;
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

		/* (l,r) Join (L, R, ..) */
		if (match > 0 && isMatJoinOp(p) && p->argc >= 3 && p->retc == 2 &&
				match <= 3 && bats >= 2) {
		   	m = is_a_mat(getArg(p,p->retc), mat, mtop);
		   	n = is_a_mat(getArg(p,p->retc+1), mat, mtop);
		   	o = is_a_mat(getArg(p,p->retc+2), mat, mtop);

			if (bats == 3 && match >= 2)
				mtop = mat_join3(mb, p, mat, mtop, m, n, o);
			else
				mtop = mat_join2(mb, p, mat, mtop, m, n);
			actions++;
			continue;
		}
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

		if (match == 1 && bats == 1 && p->argc == 4 && isSlice(p) &&
	 	   ((m=is_a_mat(getArg(p,p->retc), mat, mtop)) >= 0)) {
			mtop = mat_topn(mb, p, mat, mtop, m, -1);
			actions++;
			continue;
		}

		if (!distinct_topn && match == 1 && bats == 1 && p->argc == 3 && isTopn(p) &&
	 	   ((m=is_a_mat(getArg(p,p->retc), mat, mtop)) >= 0)) {
			mtop = mat_topn(mb, p, mat, mtop, m, -1);
			actions++;
			continue;
		}
		if (!distinct_topn && match == 2 && bats == 2 && p->argc == 4 && isTopn(p) &&
	 	   ((m=is_a_mat(getArg(p,p->retc), mat, mtop)) >= 0) &&
	 	   ((n=is_a_mat(getArg(p,p->retc+1), mat, mtop)) >= 0)) {
			mtop = mat_topn(mb, p, mat, mtop, m, n);
			actions++;
			continue;
		}

		/* Now we handle subgroup and aggregation statements. */
		if (!groupdone && match == 1 && bats == 1 && p->argc == 4 && getModuleId(p) == groupRef && 
		   (getFunctionId(p) == subgroupRef || getFunctionId(p) == subgroupdoneRef) && 
	 	   ((m=is_a_mat(getArg(p,p->retc), mat, mtop)) >= 0)) {
			mtop = mat_group_new(mb, p, mat, mtop, m);
			actions++;
			continue;
		}
		if (!groupdone && match == 2 && bats == 2 && p->argc == 5 && getModuleId(p) == groupRef && 
		   (getFunctionId(p) == subgroupRef || getFunctionId(p) == subgroupdoneRef) && 
		   ((m=is_a_mat(getArg(p,p->retc), mat, mtop)) >= 0) &&
		   ((n=is_a_mat(getArg(p,p->retc+1), mat, mtop)) >= 0) && 
		     mat[n].im >= 0 /* not packed */) {
			mtop = mat_group_derive(mb, p, mat, mtop, m, n);
			actions++;
			continue;
		}
		/* TODO sub'aggr' with cand list */
		if (match == 3 && bats == 3 && getModuleId(p) == aggrRef && p->argc >= 4 &&
		   (getFunctionId(p) == subcountRef ||
		    getFunctionId(p) == subminRef ||
		    getFunctionId(p) == submaxRef ||
		    getFunctionId(p) == subsumRef ||
		    getFunctionId(p) == subprodRef) &&
		   ((m=is_a_mat(getArg(p,1), mat, mtop)) >= 0) &&
		   ((n=is_a_mat(getArg(p,2), mat, mtop)) >= 0) &&
		   ((o=is_a_mat(getArg(p,3), mat, mtop)) >= 0)) {
			mat_group_aggr(mb, p, mat, m, n, o);
			actions++;
			continue;
		}
		/* Handle cases of ext.leftfetchjoin and .leftfetchjoin(grp) */
		if (match == 2 && getModuleId(p) == algebraRef &&
		    getFunctionId(p) == leftfetchjoinRef &&
		   (m=is_a_mat(getArg(p,1), mat, mtop)) >= 0 &&
		   (n=is_a_mat(getArg(p,2), mat, mtop)) >= 0 &&
		   (mat[m].type == mat_ext || mat[n].type == mat_grp)) {
			assert(mat[m].pushed);
			if (!mat[n].pushed) 
				mtop = mat_group_project(mb, p, mat, mtop, m, n);
			else
				pushInstruction(mb, copyInstruction(p));
			continue;
		}

		/* Handle cases of slice.leftfetchjoin */
		if (match == 2 && getModuleId(p) == algebraRef &&
		    getFunctionId(p) == leftfetchjoinRef &&
		   (m=is_a_mat(getArg(p,1), mat, mtop)) >= 0 &&
		   (n=is_a_mat(getArg(p,2), mat, mtop)) >= 0 &&
		   (mat[m].type == mat_slc)) {
			mat_topn_project(mb, p, mat, m, n);
			actions++;
			continue;
		}

		/* Handle leftfetchjoin */
		if (match > 0 && getModuleId(p) == algebraRef &&
		    getFunctionId(p) == leftfetchjoinRef && 
		   (m=is_a_mat(getArg(p,1), mat, mtop)) >= 0) { 
		   	n=is_a_mat(getArg(p,2), mat, mtop);
			mtop = mat_leftfetchjoin(mb, p, mat, mtop, m, n);
			actions++;
			continue;
		}
		/* Handle setops */
		if (match > 0 && getModuleId(p) == algebraRef &&
		    (getFunctionId(p) == tdiffRef || 
		     getFunctionId(p) == tinterRef) && 
		   (m=is_a_mat(getArg(p,1), mat, mtop)) >= 0) { 
		   	n=is_a_mat(getArg(p,2), mat, mtop);
			mtop = mat_setop(mb, p, mat, mtop, m, n);
			actions++;
			continue;
		}

		m = n = o = e = -1;
		for( fm= p->argc-1; fm>=p->retc ; fm--)
			if ((m=is_a_mat(getArg(p,fm), mat, mtop)) >= 0)
				break;

		for( fn= fm-1; fn>=p->retc ; fn--)
			if ((n=is_a_mat(getArg(p,fn), mat, mtop)) >= 0)
				break;

		for( fo= fn-1; fo>=p->retc ; fo--)
			if ((o=is_a_mat(getArg(p,fo), mat, mtop)) >= 0)
				break;

		for( fe= fo-1; fe>=p->retc ; fe--)
			if ((e=is_a_mat(getArg(p,fe), mat, mtop)) >= 0)
				break;

		/* delta* operator have a ins bat as last argument, we move the inserts into the last delta statement, ie
  		 * all but last need to remove one argument */
		if (match == 3 && bats == 4 && isDelta(p) && 
		   (m=is_a_mat(getArg(p,fm), mat, mtop)) >= 0 &&
		   (n=is_a_mat(getArg(p,fn), mat, mtop)) >= 0 &&
		   (o=is_a_mat(getArg(p,fo), mat, mtop)) >= 0){
			if ((r = mat_delta(mb, p, mat, m, n, o, -1, fm, fn, fo, 0)) != NULL)
				mtop = mat_add(mat, mtop, r, mat_type(mat, m), getFunctionId(p));
			actions++;
			continue;
		}
		if (match == 4 && bats == 5 && isDelta(p) && 
		   (m=is_a_mat(getArg(p,fm), mat, mtop)) >= 0 &&
		   (n=is_a_mat(getArg(p,fn), mat, mtop)) >= 0 &&
		   (o=is_a_mat(getArg(p,fo), mat, mtop)) >= 0 &&
		   (e=is_a_mat(getArg(p,fe), mat, mtop)) >= 0){
			if ((r = mat_delta(mb, p, mat, m, n, o, e, fm, fn, fo, fe)) != NULL)
				mtop = mat_add(mat, mtop, r, mat_type(mat, m), getFunctionId(p));
			actions++;
			continue;
		}

		/* subselect on insert, should use last tid only */
		if (match == 1 && fm == 2 && isSubSelect(p) && p->retc == 1 &&
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
				mtop = mat_add(mat, mtop, r, mat_type(mat, m), getFunctionId(p));
			actions++;
			continue;
		}
		if (match == 2 && bats == 2 && (isFragmentGroup(p) || isFragmentGroup2(p) || isMapOp(p)) &&  p->retc != 2 &&
		   (m=is_a_mat(getArg(p,fm), mat, mtop)) >= 0 &&
		   (n=is_a_mat(getArg(p,fn), mat, mtop)) >= 0){
			assert(mat[m].mi->argc == mat[n].mi->argc); 
			if ((r = mat_apply2(mb, p, mat, m, n, fm, fn)) != NULL)
				mtop = mat_add(mat, mtop, r, mat_type(mat, m), getFunctionId(p));
			actions++;
			continue;
		}

		if (match == 1 && bats == 1 && (isFragmentGroup(p) || isMapOp(p) || 
		   (!getModuleId(p) && !getFunctionId(p) && p->barrier == 0 /* simple assignment */)) && p->retc != 2 && 
		   (m=is_a_mat(getArg(p,fm), mat, mtop)) >= 0){
			if ((r = mat_apply1(mb, p, mat, m, fm)) != NULL)
				mtop = mat_add(mat, mtop, r, mat_type(mat, m), getFunctionId(p));
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
	for (i=0; i<mtop; i++) {
		if (mat[i].mi && !mat[i].pushed)
			freeInstruction(mat[i].mi);
	}
	GDKfree(mat);
	return actions;
}
