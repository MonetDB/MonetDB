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
	mat_cnt = 3,	/* after mat_grp the extend gets a mat.mirror */
	mat_tpn = 4,	/* Phase one of topn on a mat */
	mat_slc = 5,	/* Phase one of topn on a mat */
	mat_rdr = 6	/* Phase one of sorting, ie sorted the parts sofar */
} mat_type_t;

/* TODO have a seperate mat_variable list, ie old_var, mat instruction */
/* including first or second result */

typedef struct mat {
	InstrPtr mi;		/* mat instruction */
	int mv;			/* mat variable */
	int im;			/* input mat, for attribute of sub group relations */
	int pm;			/* parent mat, for sub group relations */
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

inline static int
mat_add(mat_t *mat, int mtop, InstrPtr q, mat_type_t type) 
{
	mat[mtop].mi = q;
	mat[mtop].mv = getArg(q,0);
	mat[mtop].type = type;
	mat[mtop].pm = -1;
	return mtop+1;
}

/* some mat's have intermediates (with intermediate result variables), therefor
 * we pass the old output mat variable */
inline static int
mat_add_var(mat_t *mat, int mtop, InstrPtr q, int var, mat_type_t type, int inputmat, int parentmat) 
{
	mat[mtop].mi = q;
	mat[mtop].mv = var;
	mat[mtop].type = type;
	mat[mtop].im = inputmat;
	mat[mtop].pm = parentmat;
	return mtop+1;
}

static InstrPtr 
mat_pack(MalBlkPtr mb, mat_t *mat, int m)
{
	InstrPtr r;

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
group_by_length(mat_t *mat, int g)
{
	int cnt = 0;
	while(g >= 0) {
		g = mat[g].pm;
		cnt++;
	}
	return cnt;
}

static int
group_by_group(mat_t *mat, int g, int cnt)
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
	ai2 = pushInt(mb, ai2, 1); /* skip nils */
	ai2 = pushInt(mb, ai2, 0); /* continue on errors */
	pushInstruction(mb, ai2);
}

/* The mat_group_{new,derive} keep an ext,attr1..attrn table.
 * This is the input for the final second phase group by.
 */
static void
mat_pack_group(MalBlkPtr mb, mat_t *mat, int mtop, int g)
{
	int cnt = group_by_length(mat, g), i;
	InstrPtr cur = NULL;

	for(i=cnt-1; i>=0; i--) {
		InstrPtr grp = newInstruction(mb, ASSIGNsymbol);
		int ogrp = group_by_group(mat, g, i);
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
mat_group_attr(MalBlkPtr mb, mat_t *mat, int mtop, int g, InstrPtr cext )
{
        int cnt = group_by_length(mat, g), i;	/* number of attributes */
	int ogrp = g; 				/* previous group */

	for(i = 0; i < cnt; i++) {
		int agrp = group_by_group(mat, ogrp, i); 
		int b = mat[agrp].im;
		int aext = group_by_ext(mat, mtop, agrp);
		int a = mat[aext].im;
		int atp = getArgType(mb,mat[a].mi,0), k;
		InstrPtr attr = newInstruction(mb, ASSIGNsymbol);

		setModuleId(attr,matRef);
		setFunctionId(attr,newRef);
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
		pushInstruction(mb,attr);
		mtop = mat_add_var(mat, mtop, attr, getArg(attr, 0), mat_ext,  -1, -1);
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
	int atp = getArgType(mb,p,3), i, a, g;
	InstrPtr r0, r1, r2, attr;

	r0 = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r0,matRef);
	setFunctionId(r0,newRef);
	getArg(r0,0) = newTmpVariable(mb, tp0);

	r1 = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r1,matRef);
	setFunctionId(r1,newRef);
	getArg(r1,0) = newTmpVariable(mb, tp1);

	r2 = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r2,matRef);
	setFunctionId(r2,newRef);
	getArg(r2,0) = newTmpVariable(mb, tp2);

	/* we keep an extend, attr table result, which will later be used
	 * when we pack the group result */
	attr = newInstruction(mb, ASSIGNsymbol);
	setModuleId(attr,matRef);
	setFunctionId(attr,newRef);
	//getArg(attr,0) = newTmpVariable(mb, atp);
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
	pushInstruction(mb,attr);

	/* create mat's for the intermediates */
	a = mtop = mat_add_var(mat, mtop, attr, getArg(attr, 0), mat_ext,  -1, -1);
	g = mtop = mat_add_var(mat, mtop, r0, getArg(p, 0), mat_grp, b, -1);
	mtop = mat_add_var(mat, mtop, r1, getArg(p, 1), mat_ext, a-1, mtop-1); /* point back at group */
	mtop = mat_add_var(mat, mtop, r2, getArg(p, 2), mat_cnt, -1, mtop-1); /* point back at ext */
	if (getFunctionId(p) == subgroupdoneRef)
		mat_pack_group(mb, mat, mtop, g-1);
	return mtop;
}

static int
mat_group_derive(MalBlkPtr mb, InstrPtr p, mat_t *mat, int mtop, int b, int g)
{
	int tp0 = getArgType(mb,p,0);
	int tp1 = getArgType(mb,p,1);
	int tp2 = getArgType(mb,p,2); 
	int atp = getArgType(mb,p,3), i, a; 
	InstrPtr r0, r1, r2, attr;

	if (mat[g].im == -1){ /* allready packed */
		pushInstruction(mb, copyInstruction(p));
		return mtop;
	}

	r0 = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r0,matRef);
	setFunctionId(r0,newRef);
	getArg(r0,0) = newTmpVariable(mb, tp0);

	r1 = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r1,matRef);
	setFunctionId(r1,newRef);
	getArg(r1,0) = newTmpVariable(mb, tp1);

	r2 = newInstruction(mb, ASSIGNsymbol);
	setModuleId(r2,matRef);
	setFunctionId(r2,newRef);
	getArg(r2,0) = newTmpVariable(mb, tp2);
	
	/* we keep an extend, attr table result, which will later be used
	 * when we pack the group result */
	attr = newInstruction(mb, ASSIGNsymbol);
	setModuleId(attr,matRef);
	setFunctionId(attr,newRef);
	//getArg(attr,0) = newTmpVariable(mb, atp);
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
	pushInstruction(mb,attr);

	mtop = mat_group_attr(mb, mat, mtop, g, r1);

	/* create mat's for the intermediates */
	a = mtop = mat_add_var(mat, mtop, attr, getArg(attr, 0), mat_ext,  -1, -1);
	g = mtop = mat_add_var(mat, mtop, r0, getArg(p, 0), mat_grp, b, g);
	mtop = mat_add_var(mat, mtop, r1, getArg(p, 1), mat_ext, a-1, mtop-1); /* point back at group */
	mtop = mat_add_var(mat, mtop, r2, getArg(p, 2), mat_cnt, -1, mtop-1); /* point back at ext */

	if (getFunctionId(p) == subgroupdoneRef)
		mat_pack_group(mb, mat, mtop, g-1);
	return mtop;
}

int
OPTmergetableImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p) 
{
	InstrPtr *old;
	mat_t *mat;
	int oldtop, fm, fn, fo, fe, i, k, m, n, o, e, mtop=0, slimit;
	int size=0, match, actions=0;


	old = mb->stmt;
	oldtop= mb->stop;

	/* check for bailout conditions */
	for (i = 1; i < oldtop; i++) {
		p = old[i];

		/* bail out on multiple subgroups (ie distinct) */
		if (getModuleId(p) == groupRef && getFunctionId(p) == subgroupdoneRef) {
			if (size > 0)
				return 0;
			size++;
		}
		if ((getModuleId(p) == batcalcRef || getModuleId(p) == sqlRef) && 
		   (getFunctionId(p) == rankRef || getFunctionId(p) == rank_grpRef ||
		    getFunctionId(p) == mark_grpRef || getFunctionId(p) == dense_rank_grpRef)) { 
			/* Mergetable cannot handle order related batcalc ops */
			return 0;
		}
		if (getModuleId(p) == aggrRef && 
		    getFunctionId(p) == submedianRef)
			return 0;
	}

	/* the number of MATs is limited to the variable stack*/
	mat = (mat_t*) GDKzalloc(mb->vtop * sizeof(mat_t));
	if ( mat == NULL)
		return 0;

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

		/* Now we handle subgroup and aggregation statements. */
		if (match == 1 && bats == 1 && p->argc == 4 && getModuleId(p) == groupRef && 
		   (getFunctionId(p) == subgroupRef || getFunctionId(p) == subgroupdoneRef) && 
	 	   ((m=is_a_mat(getArg(p,p->retc), mat, mtop)) >= 0)) {
			mtop = mat_group_new(mb, p, mat, mtop, m);
			actions++;
			continue;
		}
		if (match == 2 && bats == 2 && p->argc == 5 && getModuleId(p) == groupRef && 
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
		/* Handle cases of ext.leftjoin and .leftjoin(grp) */
		if (match == 2 && getModuleId(p) == algebraRef &&
		    getFunctionId(p) == leftfetchjoinRef && 
		   (m=is_a_mat(getArg(p,1), mat, mtop)) >= 0 &&
		   (n=is_a_mat(getArg(p,2), mat, mtop)) >= 0 &&
		   (mat[m].type == mat_ext || mat[n].type == mat_grp)) {
			pushInstruction(mb, copyInstruction(p));
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

	DEBUGoptimizers
		mnstr_printf(cntxt->fdout,"#opt_mergetable: %d merge actions\n",actions);
	GDKfree(mat);
	return actions;
}
