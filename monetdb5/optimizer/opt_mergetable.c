/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "opt_mergetable.h"

typedef enum mat_type_t {
	mat_none = 0,				/* Simple mat aligned operations (ie batcalc etc) */
	mat_grp = 1,				/* result of phase one of a mat - group.new/derive */
	mat_ext = 2,				/* mat_grp extend */
	mat_cnt = 3,				/* mat_grp count */
	mat_tpn = 4,				/* Phase one of topn on a mat */
	mat_slc = 5,				/* Last phase of topn (or just slice) on a mat */
	mat_rdr = 6					/* Phase one of sorting, ie sorted the parts so far */
} mat_type_t;

typedef struct mat {
	InstrPtr mi;				/* mat instruction */
	InstrPtr org;				/* original instruction */
	int mv;						/* mat variable */
	int im;						/* input mat, for attribute of sub relations */
	int pm;						/* parent mat, for sub relations */
	mat_type_t type;			/* type of operation */
	int packed;
	int pushed;					/* set if instruction pushed and shouldn't be freed */
} mat_t;

typedef struct matlist {
	mat_t *v;
	int *vars;					/* result variable is a mat */
	int top;
	int size;

	int *horigin;
	int *torigin;
	int vsize;
} matlist_t;

static inline mat_type_t
mat_type(mat_t *mat, int n)
{
	mat_type_t type = mat_none;
	(void) mat;
	(void) n;
	return type;
}

static inline int
is_a_mat(int idx, const matlist_t *ml)
{
	if (ml->vars[idx] >= 0 && !ml->v[ml->vars[idx]].packed)
		return ml->vars[idx];
	return -1;
}

static int
nr_of_mats(InstrPtr p, const matlist_t *ml)
{
	int j, cnt = 0;
	for (j = p->retc; j < p->argc; j++)
		if (is_a_mat(getArg(p, j), ml) >= 0)
			cnt++;
	return cnt;
}

static int
nr_of_bats(MalBlkPtr mb, InstrPtr p)
{
	int j, cnt = 0;
	for (j = p->retc; j < p->argc; j++)
		if (isaBatType(getArgType(mb, p, j))
			&& !isVarConstant(mb, getArg(p, j)))
			cnt++;
	return cnt;
}

static int
nr_of_nilbats(MalBlkPtr mb, InstrPtr p)
{
	int j, cnt = 0;
	for (j = p->retc; j < p->argc; j++)
		if (isaBatType(getArgType(mb, p, j))
				&& isVarConstant(mb, getArg(p, j))
				&& getVarConstant(mb, getArg(p, j)).val.bval == bat_nil)
			cnt++;
	return cnt;
}

/* some mat's have intermediates (with intermediate result variables),
 * therefore we pass the old output mat variable */
inline static int
mat_add_var(Client c, matlist_t *ml, InstrPtr q, InstrPtr p, int var, mat_type_t type,
			int inputmat, int parentmat, int pushed)
{
	if (ml->top == ml->size) {
		int s = ml->size * 2;
		mat_t *v = (mat_t *) ma_zalloc(c->ta, s * sizeof(mat_t));
		if (!v)
			return -1;
		memcpy(v, ml->v, ml->top * sizeof(mat_t));
		// GDKfree(ml->v);
		ml->size = s;
		ml->v = v;
	}
	mat_t *dst = &ml->v[ml->top];
	dst->mi = q;
	dst->org = p;
	dst->mv = var;
	dst->type = type;
	dst->im = inputmat;
	dst->pm = parentmat;
	dst->packed = 0;
	dst->pushed = pushed;
	if (ml->vars[var] < 0 || dst->type != mat_ext) {
		if (ml->vars[var] >= 0) {
			ml->v[ml->vars[var]].packed = 1;
		}
		ml->vars[var] = ml->top;
	}
	++ml->top;
	return 0;
}

inline static int
mat_add(Client c, matlist_t *ml, InstrPtr q, mat_type_t type, const char *func)
{
	(void) func;
	//printf (" ml.top %d %s\n", ml.top, func);
	return mat_add_var(c, ml, q, NULL, getArg(q, 0), type, -1, -1, 0);
}

static void
matlist_pack(matlist_t *ml, int m)
{
	int i, idx = ml->v[m].mv;

	assert(ml->v[m].packed == 0);
	ml->v[m].packed = 1;
	ml->vars[idx] = -1;

	for (i = 0; i < ml->top; i++)
		if (!ml->v[i].packed && ml->v[i].mv == idx) {
			ml->vars[idx] = i;
			break;
		}
}

static str
mat_pack(MalBlkPtr mb, matlist_t *ml, int m)
{
	InstrPtr r;

	if (ml->v[m].packed)
		return MAL_SUCCEED;

	if ((ml->v[m].mi->argc - ml->v[m].mi->retc) == 1) {
		/* simple assignment is sufficient */
		r = newInstruction(mb, NULL, NULL);
		if (r == NULL) {
			throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		getArg(r, 0) = getArg(ml->v[m].mi, 0);
		getArg(r, 1) = getArg(ml->v[m].mi, 1);
		r->retc = 1;
		r->argc = 2;
	} else {
		int l;

		r = newInstructionArgs(mb, matRef, packRef, ml->v[m].mi->argc);
		if (r == NULL) {
			throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		getArg(r, 0) = getArg(ml->v[m].mi, 0);
		for (l = ml->v[m].mi->retc; l < ml->v[m].mi->argc; l++)
			r = pushArgument(mb, r, getArg(ml->v[m].mi, l));
		if (mb->errors) {
			freeInstruction(mb, r);
			char *msg = mb->errors;
			mb->errors = NULL;
			return msg;
		}
	}
	matlist_pack(ml, m);
	pushInstruction(mb, r);
	return MAL_SUCCEED;
}

static int
checksize(Client c, matlist_t *ml, int v)
{
	if (v >= ml->vsize) {
		int sz = ml->vsize, i, *nhorigin, *ntorigin, *nvars;

		int nvsize = ml->vsize * 2;
		assert(v < nvsize);
		if (v >= nvsize)
			nvsize = v + 10;
		nhorigin = (int *) MA_RENEW_ARRAY(c->ta, int, ml->horigin, nvsize, sz);
		if (nhorigin == NULL)
			return -1;
		ml->horigin = nhorigin;
		ntorigin = (int *) MA_RENEW_ARRAY(c->ta, int, ml->torigin, nvsize, sz);
		if (ntorigin == NULL)
			return -1;
		ml->torigin = ntorigin;
		nvars = (int *) MA_RENEW_ARRAY(c->ta, int, ml->vars, nvsize, sz);
		if (nvars == NULL)
			return -1;
		ml->vars = nvars;
		ml->vsize = nvsize;

		for (i = sz; i < ml->vsize; i++) {
			ml->horigin[i] = ml->torigin[i] = -1;
			ml->vars[i] = -1;
		}
	}
	return 0;
}

static int
setPartnr(Client c, matlist_t *ml, int ivar, int ovar, int pnr)
{
	int tpnr = -1;

	if (checksize(c, ml, ivar) || checksize(c, ml, ovar))
		return -1;
	if (ivar >= 0)
		tpnr = ml->torigin[ivar];
	if (tpnr >= 0)
		ml->torigin[ovar] = tpnr;
	assert(ovar < ml->vsize);
	ml->horigin[ovar] = pnr;
	//printf("%d %d ", pnr, tpnr);
	return 0;
}

static int
propagatePartnr(Client c, matlist_t *ml, int ivar, int ovar, int pnr)
{
	/* prop head ids to tail */
	int tpnr = -1;

	if (checksize(c, ml, ivar) || checksize(c, ml, ovar))
		return -1;
	if (ivar >= 0)
		tpnr = ml->horigin[ivar];
	if (tpnr >= 0)
		ml->torigin[ovar] = tpnr;
	assert(ovar < ml->vsize);
	ml->horigin[ovar] = pnr;
	//printf("%d %d ", pnr, tpnr);
	return 0;
}

static int
propagateMirror(Client c, matlist_t *ml, int ivar, int ovar)
{
	/* prop head ids to head and tail */
	int tpnr;

	if (checksize(c, ml, ivar) || checksize(c, ml, ovar))
		return -1;
	tpnr = ml->horigin[ivar];
	if (tpnr >= 0) {
		assert(ovar < ml->vsize);
		ml->horigin[ovar] = tpnr;
		ml->torigin[ovar] = tpnr;
	}
	return 0;
}

static int
overlap(Client c, matlist_t *ml, int lv, int rv, int lnr, int rnr, int ontails)
{
	int lpnr, rpnr;

	if (checksize(c, ml, lv) || checksize(c, ml, rv))
		return -1;
	lpnr = ml->torigin[lv];
	rpnr = (ontails) ? ml->torigin[rv] : ml->horigin[rv];

	if (lpnr < 0 && rpnr < 0)
		return lnr == rnr;
	if (rpnr < 0)
		return lpnr == rnr;
	if (lpnr < 0)
		return rpnr == lnr;
	return lpnr == rpnr;
}

static int
mat_set_prop(Client c, matlist_t *ml, MalBlkPtr mb, InstrPtr p)
{
	int k, tpe = getArgType(mb, p, 0);

	tpe = getBatType(tpe);
	for (k = 1; k < p->argc; k++) {
		if (setPartnr(c, ml, -1, getArg(p, k), k))
			return -1;
		if (tpe == TYPE_oid && propagateMirror(c, ml, getArg(p, k), getArg(p, k)))
			return -1;
	}
	return 0;
}

static InstrPtr
mat_delta(Client c, matlist_t *ml, MalBlkPtr mb, InstrPtr p, mat_t *mat, int m, int n,
		  int o, int e, int mvar, int nvar, int ovar, int evar)
{
	int tpe, k, j, is_subdelta = (getFunctionId(p) == subdeltaRef),
		is_projectdelta = (getFunctionId(p) == projectdeltaRef);
	InstrPtr r = NULL;
	int pushed = 0;

	//printf("# %s.%s(%d,%d,%d,%d)", getModuleId(p), getFunctionId(p), m, n, o, e);

	if ((r = newInstructionArgs(mb, matRef, packRef, mat[m].mi->argc)) == NULL)
		return NULL;
	getArg(r, 0) = getArg(p, 0);
	tpe = getArgType(mb, p, 0);

	/* Handle like mat_projection, ie overlapping partitions */
	if (evar == 1 && mat[e].mi->argc != mat[m].mi->argc) {
		int nr = 1;
		for (k = 1; k < mat[e].mi->argc; k++) {
			for (j = 1; j < mat[m].mi->argc; j++) {
				InstrPtr q;
				switch (overlap(c, ml, getArg(mat[e].mi, k), getArg(mat[m].mi, j), k, j, 0)) {
				case 0:
					continue;
				case -1:
					return NULL;
				case 1:
					q = copyInstruction(mb, p);
					if (!q) {
						freeInstruction(mb, r);
						return NULL;
					}
					getArg(q, 0) = newTmpVariable(mb, tpe);
					getArg(q, mvar) = getArg(mat[m].mi, j);
					getArg(q, nvar) = getArg(mat[n].mi, j);
					getArg(q, ovar) = getArg(mat[o].mi, j);
					getArg(q, evar) = getArg(mat[e].mi, k);
					pushInstruction(mb, q);
					if (mb->errors) {
						freeInstruction(mb, r);
						return NULL;
					}
					if (setPartnr(c, ml, getArg(mat[m].mi, j), getArg(q, 0), nr)) {
						freeInstruction(mb, r);
						return NULL;
					}
					r = pushArgument(mb, r, getArg(q, 0));

					nr++;
					break;
				}
			}
		}
	} else {
		for (k = 1; k < mat[m].mi->argc; k++) {
			InstrPtr q = copyInstruction(mb, p);
			if (!q) {
				freeInstruction(mb, r);
				return NULL;
			}
			getArg(q, 0) = newTmpVariable(mb, tpe);
			getArg(q, mvar) = getArg(mat[m].mi, k);
			getArg(q, nvar) = getArg(mat[n].mi, k);
			getArg(q, ovar) = getArg(mat[o].mi, k);
			if (e >= 0)
				getArg(q, evar) = getArg(mat[e].mi, k);
			pushInstruction(mb, q);
			if (mb->errors) {
				freeInstruction(mb, r);
				return NULL;
			}
			if (setPartnr(c, ml, is_subdelta ? getArg(mat[m].mi, k) : -1, getArg(q, 0), k)) {
				freeInstruction(mb, r);
				return NULL;
			}
			r = pushArgument(mb, r, getArg(q, 0));
		}
		if (evar == 1 && e >= 0 && mat[e].type == mat_slc && is_projectdelta) {
			InstrPtr q = newInstruction(mb, algebraRef, projectionRef);
			if (q == NULL) {
				freeInstruction(mb, r);
				return NULL;
			}
			getArg(q, 0) = getArg(r, 0);
			q = pushArgument(mb, q, getArg(mat[e].mi, 0));
			getArg(r, 0) = newTmpVariable(mb, tpe);
			q = pushArgument(mb, q, getArg(r, 0));
			pushInstruction(mb, r);
			pushInstruction(mb, q);
			if (mb->errors)
				return NULL;
			pushed = 1;
			r = q;
		}
	}
	if (mat_add_var(c, ml, r, NULL, getArg(r, 0), mat_type(mat, m), -1, -1, pushed)) {
		freeInstruction(mb, r);
		return NULL;
	}
	if (pushed)
		matlist_pack(ml, ml->top - 1);
	return r;
}

static InstrPtr
mat_assign(Client c, MalBlkPtr mb, InstrPtr p, matlist_t *ml)
{
	InstrPtr r = NULL;
	mat_t *mat = ml->v;

	for (int i = 0; i < p->retc; i++) {
		int m = is_a_mat(getArg(p, p->retc + i), ml);
		assert(is_a_mat(getArg(p, i), ml) < 0 && m >= 0);

		if ((r = newInstructionArgs(mb, matRef, packRef, mat[m].mi->argc)) == NULL)
			return NULL;
		getArg(r, 0) = getArg(p, i);
		for (int k = 1; k < mat[m].mi->argc; k++) {
			/* reuse inputs of old mat */
			r = pushArgument(mb, r, getArg(mat[m].mi, k));
			if (setPartnr(c, ml, -1, getArg(mat[m].mi, k), k)) {
				freeInstruction(mb, r);
				return NULL;
			}
		}
		if (mat_add(c, ml, r, mat_none, getFunctionId(p))) {
			freeInstruction(mb, r);
			return NULL;
		}
	}
	return r;
}

static int
mat_apply1(Client c, MalBlkPtr mb, InstrPtr p, matlist_t *ml, int m, int var)
{
	int tpe, k, is_select = isSelect(p),
		is_mirror = (getFunctionId(p) == mirrorRef);
	int is_identity = (getFunctionId(p) == identityRef
					   && getModuleId(p) == batcalcRef);
	int ident_var = 0, is_assign = (getFunctionId(p) == NULL), n = 0;
	InstrPtr r = NULL, q;
	mat_t *mat = ml->v;

	assert(!is_assign);

	assert(p->retc == 1);

	/* Find the mat we overwrite */
	if (is_assign) {
		n = is_a_mat(getArg(p, 0), ml);
		is_assign = (n >= 0);
	}

	if (m < 0
		|| (r = newInstructionArgs(mb, matRef, packRef, mat[m].mi->argc)) == NULL)
		return -1;
	getArg(r, 0) = getArg(p, 0);
	tpe = getArgType(mb, p, 0);

	if (is_identity) {
		if ((q = newInstruction(mb, NULL, NULL)) == NULL) {
			freeInstruction(mb, r);
			return -1;
		}
		getArg(q, 0) = newTmpVariable(mb, TYPE_oid);
		q->retc = 1;
		q->argc = 1;
		q = pushOid(mb, q, 0);
		ident_var = getArg(q, 0);
		pushInstruction(mb, q);
		if (mb->errors) {
			freeInstruction(mb, r);
			return -1;
		}
	}
	for (k = 1; k < mat[m].mi->argc; k++) {
		int res = 0;
		if ((q = copyInstruction(mb, p)) == NULL) {
			freeInstruction(mb, r);
			return -1;
		}

		if (is_assign)
			getArg(q, 0) = getArg(mat[n].mi, k);
		else
			getArg(q, 0) = newTmpVariable(mb, tpe);
		if (is_identity)
			getArg(q, 1) = newTmpVariable(mb, TYPE_oid);
		getArg(q, var +is_identity) = getArg(mat[m].mi, k);
		if (is_identity) {
			getArg(q, 3) = ident_var;
			q->retc = 2;
			q->argc = 4;
			/* make sure to resolve again */
			q->token = ASSIGNsymbol;
			q->typeresolved = false;
			q->fcn = NULL;
			q->blk = NULL;
		}
		ident_var = getArg(q, 1);
		pushInstruction(mb, q);
		if (mb->errors) {
			freeInstruction(mb, r);
			return -1;
		}
		if (is_mirror || is_identity) {
			res = propagateMirror(c, ml, getArg(mat[m].mi, k), getArg(q, 0));
		} else if (is_select)
			res = propagatePartnr(c, ml, getArg(mat[m].mi, k), getArg(q, 0), k);
		else
			res = setPartnr(c, ml, -1, getArg(q, 0), k);
		if (res) {
			freeInstruction(mb, r);
			return -1;
		}
		r = pushArgument(mb, r, getArg(q, 0));
	}
	if (mb->errors) {
		freeInstruction(mb, r);
		return -1;
	}
	if (!r || mat_add(c, ml, r, mat_type(ml->v, m), getFunctionId(p))) {
		freeInstruction(mb, r);
		return -1;
	}
	return 0;
}

static int
mat_apply(Client c, MalBlkPtr mb, InstrPtr p, matlist_t *ml, int nrmats)
{
	int matvar[8], fargument[8], k, l, parts = 0;

	if (nrmats == 1
		&& ((getModuleId(p) == batcalcRef && getFunctionId(p) == identityRef)
			|| (getModuleId(p) == batRef && getFunctionId(p) == mirrorRef)))
		return mat_apply1(c, mb, p, ml, is_a_mat(getArg(p, 1), ml), 1);
	assert(nrmats <= 8);

	assert(p->retc < p->argc);	/* i.e. matvar[0] gets initialized */
	for (k = p->retc, l = 0; k < p->argc; k++) {
		int mv = is_a_mat(getArg(p, k), ml);
		if (mv >= 0) {
			matvar[l] = mv;
			fargument[l] = k;
			l++;
			if (parts == 0)
				parts = ml->v[mv].mi->argc;
			if (parts != ml->v[mv].mi->argc)
				return -1;
		}
	}

	InstrPtr *r = (InstrPtr *) ma_alloc(c->ta, sizeof(InstrPtr) * p->retc);
	if (!r)
		return -1;
	for (k = 0; k < p->retc; k++) {
		if ((r[k] = newInstructionArgs(mb, matRef, packRef, parts)) == NULL) {
			while (k > 0)
				freeInstruction(mb, r[--k]);
			return -1;
		}
		getArg(r[k], 0) = getArg(p, k);
	}

	for (k = 1; k < ml->v[matvar[0]].mi->argc; k++) {
		int tpe;
		InstrPtr q = copyInstruction(mb, p);
		if (q == NULL) {
			for (k = 0; k < p->retc; k++)
				freeInstruction(mb, r[k]);
			return -1;
		}

		for (l = 0; l < p->retc; l++) {
			tpe = getArgType(mb, p, l);
			getArg(q, l) = newTmpVariable(mb, tpe);
		}
		for (l = 0; l < nrmats; l++)
			getArg(q, fargument[l]) = getArg(ml->v[matvar[l]].mi, k);
		pushInstruction(mb, q);
		for (l = 0; l < p->retc; l++) {
			if (setPartnr(c, ml, -1, getArg(q, l), k)) {
				for (k = 0; k < p->retc; k++)
					freeInstruction(mb, r[k]);
				return -1;
			}
			r[l] = pushArgument(mb, r[l], getArg(q, l));
		}
		if (mb->errors) {
			for (k = 0; k < p->retc; k++)
				freeInstruction(mb, r[k]);
			return -1;
		}
	}
	for (k = 0; k < p->retc; k++) {
		if (mat_add_var(c, ml, r[k], NULL, getArg(r[k], 0),
						mat_type(ml->v, matvar[0]), -1, -1, 0)) {
			for (l = k; l < p->retc; l++)
				freeInstruction(mb, r[l]);
			return -1;
		}
	}
	return 0;
}


static int
mat_setop(Client c, MalBlkPtr mb, InstrPtr p, matlist_t *ml, int m, int n, int o)
{
	int tpe = getArgType(mb, p, 0), k, j;
	mat_t *mat = ml->v;
	InstrPtr r = newInstructionArgs(mb, matRef, packRef, mat[m].mi->argc);

	if (!r)
		return -1;

	getArg(r, 0) = getArg(p, 0);

	//printf("# %s.%s(%d,%d)", getModuleId(p), getFunctionId(p), m, n);
	assert(m >= 0 || n >= 0);
	if (m >= 0 && n >= 0) {
		int nr = 1;

		assert(o < 0 || mat[m].mi->argc == mat[o].mi->argc);

		for (k = 1; mb->errors == NULL && k < mat[m].mi->argc; k++) {
			InstrPtr q = copyInstruction(mb, p);
			InstrPtr s = newInstructionArgs(mb, matRef, packRef,
											mat[n].mi->argc);
			int ttpe = 0;

			if (q == NULL
				|| s == NULL
				|| (getArg(s, 0) = newTmpVariable(mb, getArgType(mb, mat[n].mi, k))) < 0) {
				freeInstruction(mb, q);
				freeInstruction(mb, s);
				freeInstruction(mb, r);
				return -1;
			}

			ttpe = getArgType(mb, mat[n].mi, 0);
			for (j = 1; j < mat[n].mi->argc; j++) {
				int ov = 0;
				if (getBatType(ttpe) != TYPE_oid
					|| (ov = overlap(c, ml, getArg(mat[m].mi, k),
									 getArg(mat[n].mi, j), k, j, 1)) == 1) {
					s = pushArgument(mb, s, getArg(mat[n].mi, j));
				}
				if (ov == -1) {
					freeInstruction(mb, q);
					freeInstruction(mb, s);
					freeInstruction(mb, r);
					return -1;
				}
			}
			if (s->retc == 1 && s->argc == 2) {	/* only one input, change into an assignment */
				getFunctionId(s) = NULL;
				getModuleId(s) = NULL;
				s->token = ASSIGNsymbol;
				s->typeresolved = false;
				s->fcn = NULL;
				s->blk = NULL;
			}
			pushInstruction(mb, s);

			getArg(q, 0) = newTmpVariable(mb, tpe);
			getArg(q, 1) = getArg(mat[m].mi, k);
			getArg(q, 2) = getArg(s, 0);
			if (o >= 0)
				getArg(q, 3) = getArg(mat[o].mi, k);
			if (setPartnr(c, ml, getArg(mat[m].mi, k), getArg(q, 0), nr)) {
				freeInstruction(mb, q);
				freeInstruction(mb, r);
				return -1;
			}
			pushInstruction(mb, q);

			r = pushArgument(mb, r, getArg(q, 0));
			nr++;
		}
	} else {
		assert(m >= 0);
		assert(o < 0 || mat[m].mi->argc == mat[o].mi->argc);

		for (k = 1; mb->errors == NULL && k < mat[m].mi->argc; k++) {
			InstrPtr q = copyInstruction(mb, p);
			if (!q) {
				freeInstruction(mb, r);
				return -1;
			}

			getArg(q, 0) = newTmpVariable(mb, tpe);
			getArg(q, 1) = getArg(mat[m].mi, k);
			if (o >= 0)
				getArg(q, 3) = getArg(mat[o].mi, k);
			pushInstruction(mb, q);

			if (setPartnr(c, ml, getArg(q, 2), getArg(q, 0), k)) {
				freeInstruction(mb, r);
				return -1;
			}
			r = pushArgument(mb, r, getArg(q, 0));
		}
	}

	if (mb->errors || mat_add(c, ml, r, mat_none, getFunctionId(p))) {
		freeInstruction(mb, r);
		return -1;
	}
	return 0;
}

static int
mat_projection(Client c, MalBlkPtr mb, InstrPtr p, matlist_t *ml, int m, int n)
{
	int tpe = getArgType(mb, p, 0), k, j;
	mat_t *mat = ml->v;
	InstrPtr r;

	//printf("# %s.%s(%d,%d)", getModuleId(p), getFunctionId(p), m, n);
	assert(m >= 0 || n >= 0);
	if (m >= 0 && n >= 0) {
		int nr = 1;
		r = newInstructionArgs(mb, matRef, packRef,
							   mat[m].mi->argc * mat[n].mi->argc);

		if (!r)
			return -1;

		getArg(r, 0) = getArg(p, 0);

		for (k = 1; mb->errors == NULL && k < mat[m].mi->argc; k++) {
			for (j = 1; j < mat[n].mi->argc; j++) {
				InstrPtr q;
				switch (overlap(c, ml, getArg(mat[m].mi, k),
								getArg(mat[n].mi, j), k, j, 0)) {
				case 0:
					continue;
				case -1:
					freeInstruction(mb, r);
					return -1;
				case 1:
					q = copyInstruction(mb, p);

					if (!q) {
						freeInstruction(mb, r);
						return -1;
					}

					getArg(q, 0) = newTmpVariable(mb, tpe);
					getArg(q, 1) = getArg(mat[m].mi, k);
					getArg(q, 2) = getArg(mat[n].mi, j);
					pushInstruction(mb, q);

					if (mb->errors || setPartnr(c, ml, getArg(mat[n].mi, j), getArg(q, 0), nr)) {
						freeInstruction(mb, r);
						return -1;
					}
					r = pushArgument(mb, r, getArg(q, 0));

					nr++;
					break;
				}
				break;			/* only in case of overlap */
			}
		}
	} else {
		assert(m >= 0);
		r = newInstructionArgs(mb, matRef, packRef, mat[m].mi->argc);

		if (!r)
			return -1;

		getArg(r, 0) = getArg(p, 0);

		for (k = 1; mb->errors == NULL && k < mat[m].mi->argc; k++) {
			InstrPtr q = copyInstruction(mb, p);

			if (!q) {
				freeInstruction(mb, r);
				return -1;
			}

			getArg(q, 0) = newTmpVariable(mb, tpe);
			getArg(q, 1) = getArg(mat[m].mi, k);
			pushInstruction(mb, q);

			if (mb->errors || setPartnr(c, ml, getArg(q, 2), getArg(q, 0), k)) {
				freeInstruction(mb, r);
				return -1;
			}
			r = pushArgument(mb, r, getArg(q, 0));
		}
	}

	if (mb->errors || mat_add(c, ml, r, mat_none, getFunctionId(p))) {
		freeInstruction(mb, r);
		return -1;
	}
	return 0;
}

static int
mat_join2(Client c, MalBlkPtr mb, InstrPtr p, matlist_t *ml, int m, int n, int lc, int rc)
{
	int tpe1 = getArgType(mb, p, 0), tpe2 = getArgType(mb, p, 1), j, k, nr = 1;
	mat_t *mat = ml->v;
	InstrPtr l;
	InstrPtr r;

	//printf("# %s.%s(%d,%d)", getModuleId(p), getFunctionId(p), m, n);

	assert(m >= 0 || n >= 0);
	if (m >= 0 && n >= 0) {
		l = newInstructionArgs(mb, matRef, packRef,
							   mat[m].mi->argc * mat[n].mi->argc);
		r = newInstructionArgs(mb, matRef, packRef,
							   mat[m].mi->argc * mat[n].mi->argc);
		if (!l || !r) {
			freeInstruction(mb, l);
			freeInstruction(mb, r);
			return -1;
		}

		getArg(l, 0) = getArg(p, 0);
		getArg(r, 0) = getArg(p, 1);

		for (k = 1; k < mat[m].mi->argc; k++) {
			for (j = 1; j < mat[n].mi->argc; j++) {
				InstrPtr q = copyInstruction(mb, p);

				if (!q) {
					freeInstruction(mb, l);
					freeInstruction(mb, r);
					return -1;
				}

				getArg(q, 0) = newTmpVariable(mb, tpe1);
				getArg(q, 1) = newTmpVariable(mb, tpe2);
				getArg(q, 2) = getArg(mat[m].mi, k);
				getArg(q, 3) = getArg(mat[n].mi, j);
				if (lc >= 0)
					getArg(q, 4) = getArg(mat[lc].mi, k);
				if (rc >= 0)
					getArg(q, 5) = getArg(mat[rc].mi, j);
				pushInstruction(mb, q);

				if (mb->errors
					|| propagatePartnr(c, ml, getArg(mat[m].mi, k), getArg(q, 0),
									   nr)
					|| propagatePartnr(c, ml, getArg(mat[n].mi, j), getArg(q, 1),
									   nr)) {
					freeInstruction(mb, r);
					freeInstruction(mb, l);
					return -1;
				}

				/* add result to mat */
				l = pushArgument(mb, l, getArg(q, 0));
				r = pushArgument(mb, r, getArg(q, 1));
				nr++;
			}
		}
	} else {
		int mv = (m >= 0) ? m : n;
		int av = (m < 0);
		int bv = (m >= 0);
		int mc = (lc >= 0) ? lc : rc;

		l = newInstructionArgs(mb, matRef, packRef, mat[mv].mi->argc);
		r = newInstructionArgs(mb, matRef, packRef, mat[mv].mi->argc);
		if (!l || !r) {
			freeInstruction(mb, l);
			freeInstruction(mb, r);
			return -1;
		}

		getArg(l, 0) = getArg(p, 0);
		getArg(r, 0) = getArg(p, 1);

		for (k = 1; k < mat[mv].mi->argc; k++) {
			InstrPtr q = copyInstruction(mb, p);

			if (!q) {
				freeInstruction(mb, l);
				freeInstruction(mb, r);
				return -1;
			}

			getArg(q, 0) = newTmpVariable(mb, tpe1);
			getArg(q, 1) = newTmpVariable(mb, tpe2);
			getArg(q, p->retc + av) = getArg(mat[mv].mi, k);
			if (mc >= 0)
				getArg(q, p->retc + 2 + av) = getArg(mat[mc].mi, k);
			pushInstruction(mb, q);

			if (mb->errors
				|| propagatePartnr(c, ml, getArg(mat[mv].mi, k), getArg(q, av), k)
				|| propagatePartnr(c, ml, getArg(p, p->retc + bv), getArg(q, bv),
								   k)) {
				freeInstruction(mb, l);
				freeInstruction(mb, r);
				return -1;
			}

			/* add result to mat */
			l = pushArgument(mb, l, getArg(q, 0));
			r = pushArgument(mb, r, getArg(q, 1));
		}
	}
	if (mb->errors || mat_add(c, ml, l, mat_none, getFunctionId(p))) {
		freeInstruction(mb, l);
		freeInstruction(mb, r);
		return -1;
	}
	if (mat_add(c, ml, r, mat_none, getFunctionId(p))) {
		freeInstruction(mb, r);
		return -1;
	}
	return 0;
}

static int
mat_rangejoin(Client c, MalBlkPtr mb, InstrPtr p, matlist_t *ml, int m, int n)
{
	int tpe1 = getArgType(mb, p, 0), tpe2 = getArgType(mb, p, 1), k, nr = 1;
	mat_t *mat = ml->v;
	InstrPtr l;
	InstrPtr r;

	//printf("# %s.%s(%d,%d)", getModuleId(p), getFunctionId(p), m, n);

	assert(m >= 0 && n >= 0 && mat[m].mi->argc == mat[n].mi->argc);
	l = newInstructionArgs(mb, matRef, packRef, mat[m].mi->argc * mat[n].mi->argc);
	r = newInstructionArgs(mb, matRef, packRef, mat[m].mi->argc * mat[n].mi->argc);
	if (!l || !r) {
		freeInstruction(mb, l);
		freeInstruction(mb, r);
		return -1;
	}

	getArg(l, 0) = getArg(p, 0);
	getArg(r, 0) = getArg(p, 1);

	for (k = 1; k < mat[m].mi->argc; k++) {
		InstrPtr q = copyInstruction(mb, p);

		if (!q) {
			freeInstruction(mb, l);
			freeInstruction(mb, r);
			return -1;
		}

		getArg(q, 0) = newTmpVariable(mb, tpe1);
		getArg(q, 1) = newTmpVariable(mb, tpe2);

		getArg(q, 3) = getArg(mat[m].mi, k);
		getArg(q, 4) = getArg(mat[n].mi, k);
		pushInstruction(mb, q);

		if (mb->errors || propagatePartnr(c, ml, getArg(mat[m].mi, k), getArg(q, 0), nr)
				       || propagatePartnr(c, ml, getArg(mat[n].mi, k), getArg(q, 1), nr)) {
			freeInstruction(mb, r);
			freeInstruction(mb, l);
			return -1;
		}

		/* add result to mat */
		l = pushArgument(mb, l, getArg(q, 0));
		r = pushArgument(mb, r, getArg(q, 1));
		nr++;
	}
	if (mb->errors || mat_add(c, ml, l, mat_none, getFunctionId(p))) {
		freeInstruction(mb, l);
		freeInstruction(mb, r);
		return -1;
	}
	if (mat_add(c, ml, r, mat_none, getFunctionId(p))) {
		freeInstruction(mb, r);
		return -1;
	}
	return 0;
}

static int
join_split(Client cntxt, InstrPtr p, int args)
{
	char *name = NULL;
	size_t len;
	int i, res = 0;
	Symbol sym;
	MalBlkPtr mb;
	InstrPtr q;

	if (args <= 3)				/* we assume there are no 2x1 joins! */
		return 1;

	len = strlen(getFunctionId(p));
	name = ma_alloc(cntxt->ta, len + 3);
	if (!name)
		return -2;
	strncpy(name, getFunctionId(p), len - 7);
	strcpy(name + len - 7, "join");

	sym = findSymbol(cntxt->usermodule, getModuleId(p), name);
	assert(sym);
	mb = sym->def;

	assert(0);
	q = mb->stmt[0];
	for (i = q->retc; i < q->argc; i++) {
		if (isaBatType(getArgType(mb, q, i)))
			res++;
		else
			break;
	}
	return res - 1;
}

/* 1 or 2 mat lists:
 * 	in case of one take the second half of the code
 * 	in case of two we need to detect the list lengths.
 *
 * input is one list of arguments (just total length of mats)
 */
static int
mat_joinNxM(Client cntxt, MalBlkPtr mb, InstrPtr p, matlist_t *ml, int args)
{
	int tpe1 = getArgType(mb, p, 0), tpe2 = getArgType(mb, p, 1), j, k, nr = 1;
	InstrPtr l;
	InstrPtr r;
	mat_t *mat = ml->v;
	int *mats = (int *) ma_zalloc(cntxt->ta, sizeof(int) * args);
	int nr_mats = 0, first = -1, res = 0;

	if (!mats) {
		return -1;
	}

	for (j = 0; j < args; j++) {
		mats[j] = is_a_mat(getArg(p, p->retc + j), ml);
		if (mats[j] != -1) {
			nr_mats++;
			if (first < 0)
				first = j;
		}
	}

	//printf("# %s.%s(%d,%d)", getModuleId(p), getFunctionId(p), m, n);

	if (args == nr_mats) {
		int mv1 = mats[0], i;
		int mv2 = mats[args - 1];
		int split = join_split(cntxt, p, args);
		int nr_mv1 = split;

		if (split == -2) {
			return -1;
		}
		if (split < 0) {
			mb->errors = createException(MAL, "mergetable.join",
										 SQLSTATE(42000) " incorrect split level");
			return 0;
		}

		l = newInstructionArgs(mb, matRef, packRef,
							   mat[mv1].mi->argc * mat[mv2].mi->argc);
		r = newInstructionArgs(mb, matRef, packRef,
							   mat[mv1].mi->argc * mat[mv2].mi->argc);
		if (l == NULL || r == NULL) {
			freeInstruction(mb, l);
			freeInstruction(mb, r);
			return -1;
		}
		getArg(l, 0) = getArg(p, 0);
		getArg(r, 0) = getArg(p, 1);

		/* now detect split point */
		for (k = 1; k < mat[mv1].mi->argc; k++) {
			for (j = 1; j < mat[mv2].mi->argc; j++) {
				InstrPtr q = copyInstruction(mb, p);
				if (!q) {
					freeInstruction(mb, r);
					freeInstruction(mb, l);
					return -1;
				}

				getArg(q, 0) = newTmpVariable(mb, tpe1);
				getArg(q, 1) = newTmpVariable(mb, tpe2);
				for (i = 0; i < nr_mv1; i++)
					getArg(q, q->retc + i) = getArg(mat[mats[i]].mi, k);
				for (; i < nr_mats; i++)
					getArg(q, q->retc + i) = getArg(mat[mats[i]].mi, j);
				pushInstruction(mb, q);

				if (mb->errors
					|| propagatePartnr(cntxt, ml, getArg(mat[mv1].mi, k), getArg(q, 0),
									   nr)
					|| propagatePartnr(cntxt, ml, getArg(mat[mv2].mi, j), getArg(q, 1),
									   nr)) {
					freeInstruction(mb, r);
					freeInstruction(mb, l);
					return -1;
				}

				/* add result to mat */
				l = pushArgument(mb, l, getArg(q, 0));
				r = pushArgument(mb, r, getArg(q, 1));
				nr++;
			}
		}
	} else {
		/* only one side
		 * mats from first..first+nr_mats
		 */
		int mv = mats[first];

		l = newInstructionArgs(mb, matRef, packRef, mat[mv].mi->argc);
		r = newInstructionArgs(mb, matRef, packRef, mat[mv].mi->argc);
		if (l == NULL || r == NULL) {
			freeInstruction(mb, l);
			freeInstruction(mb, r);
			return -1;
		}
		getArg(l, 0) = getArg(p, 0);
		getArg(r, 0) = getArg(p, 1);

		for (k = 1; k < mat[mv].mi->argc; k++) {
			InstrPtr q = copyInstruction(mb, p);
			if (!q) {
				freeInstruction(mb, r);
				freeInstruction(mb, l);
				return -1;
			}

			getArg(q, 0) = newTmpVariable(mb, tpe1);
			getArg(q, 1) = newTmpVariable(mb, tpe2);
			for (j = 0; j < nr_mats; j++) {
				assert(mat[mats[first]].mi->argc == mat[mats[first + j]].mi->argc);
				getArg(q, p->retc + first + j) = getArg(mat[mats[first + j]].mi, k);
			}
			if (mb->errors
				|| propagatePartnr(cntxt, ml, getArg(mat[mv].mi, k),
								   getArg(q, (first != 0)), k)
				|| propagatePartnr(cntxt, ml,
								   getArg(p, p->retc + (first) ? nr_mats : 0),
								   getArg(q, (first == 0)), k)) {
				freeInstruction(mb, q);
				freeInstruction(mb, r);
				freeInstruction(mb, l);
				return -1;
			}
			pushInstruction(mb, q);

			/* add result to mat */
			l = pushArgument(mb, l, getArg(q, 0));
			r = pushArgument(mb, r, getArg(q, 1));
		}
	}
	if (mb->errors || mat_add(cntxt, ml, l, mat_none, getFunctionId(p))) {
		freeInstruction(mb, l);
		freeInstruction(mb, r);
		res = -1;
	} else if (mat_add(cntxt, ml, r, mat_none, getFunctionId(p))) {
		freeInstruction(mb, r);
		res = -1;
	}
	return res;
}


static const char *
aggr_phase2(const char *aggr, int type_dbl)
{
	if (aggr == countRef || aggr == count_no_nilRef
		|| (aggr == avgRef && type_dbl))
		return sumRef;
	if (aggr == subcountRef || (aggr == subavgRef && type_dbl))
		return subsumRef;
	/* min/max/sum/prod and unique are fine */
	return aggr;
}

static str
mat_aggr(MalBlkPtr mb, InstrPtr p, mat_t *mat, int m)
{
	int tp = getArgType(mb, p, 0), k, tp2 = TYPE_lng, i;
	int battp = (getModuleId(p) == aggrRef) ? newBatType(tp) : tp, battp2 = 0;
	int isAvg = (getFunctionId(p) == avgRef);
	InstrPtr r = NULL, s = NULL, q = NULL, u = NULL, v = NULL;

	/* we pack the partial result */
	r = newInstructionArgs(mb, matRef, packRef, mat[m].mi->argc);
	if (r == NULL)
		throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	getArg(r, 0) = newTmpVariable(mb, battp);

	if (isAvg) {				/* remainders or counts */
		battp2 = newBatType(tp2);
		u = newInstructionArgs(mb, matRef, packRef, mat[m].mi->argc);
		if (u == NULL) {
			freeInstruction(mb, r);
			throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		getArg(u, 0) = newTmpVariable(mb, battp2);
	}
	if (isAvg && tp != TYPE_dbl) {	/* counts */
		battp2 = newBatType(tp2);
		v = newInstructionArgs(mb, matRef, packRef, mat[m].mi->argc);
		if (v == NULL) {
			freeInstruction(mb, r);
			freeInstruction(mb, u);
			throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		getArg(v, 0) = newTmpVariable(mb, battp2);
	}
	for (k = 1; mb->errors == NULL && k < mat[m].mi->argc; k++) {
		q = newInstruction(mb, NULL, NULL);
		if (q == NULL) {
			freeInstruction(mb, r);
			freeInstruction(mb, u);
			freeInstruction(mb, v);
			throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		if (isAvg && tp == TYPE_dbl)
			setModuleId(q, batcalcRef);
		else
			setModuleId(q, getModuleId(p));
		setFunctionId(q, getFunctionId(p));
		getArg(q, 0) = newTmpVariable(mb, tp);
		if (isAvg)
			q = pushReturn(mb, q, newTmpVariable(mb, tp2));
		if (isAvg && tp != TYPE_dbl)
			q = pushReturn(mb, q, newTmpVariable(mb, tp2));
		q = pushArgument(mb, q, getArg(mat[m].mi, k));
		for (i = q->argc; i < p->argc; i++)
			q = pushArgument(mb, q, getArg(p, i));
		pushInstruction(mb, q);

		r = pushArgument(mb, r, getArg(q, 0));
		if (isAvg)
			u = pushArgument(mb, u, getArg(q, 1));
		if (isAvg && tp != TYPE_dbl)
			v = pushArgument(mb, v, getArg(q, 2));
	}
	pushInstruction(mb, r);
	if (isAvg)
		pushInstruction(mb, u);
	if (isAvg && tp != TYPE_dbl)
		pushInstruction(mb, v);

	/* Filter empty partitions */
	if (mb->errors == NULL && getModuleId(p) == aggrRef && !isAvg) {
		s = newInstruction(mb, algebraRef, selectNotNilRef);
		if (s == NULL) {
			throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		getArg(s, 0) = newTmpVariable(mb, battp);
		s = pushArgument(mb, s, getArg(r, 0));
		pushInstruction(mb, s);
		r = s;
	}

	/* for avg we do sum (avg*(count/sumcount) ) */
	if (mb->errors == NULL && isAvg && tp == TYPE_dbl) {
		InstrPtr v, w, x, y, cond;

		/* lng w = sum counts */
		w = newInstruction(mb, aggrRef, sumRef);
		if (w == NULL) {
			throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		getArg(w, 0) = newTmpVariable(mb, tp2);
		w = pushArgument(mb, w, getArg(u, 0));
		pushInstruction(mb, w);

		/*  y=count = ifthenelse(w=count==0,NULL,w=count)  */
		cond = newInstruction(mb, calcRef, eqRef);
		if (cond == NULL) {
			throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		getArg(cond, 0) = newTmpVariable(mb, TYPE_bit);
		cond = pushArgument(mb, cond, getArg(w, 0));
		cond = pushLng(mb, cond, 0);
		pushInstruction(mb, cond);

		y = newInstruction(mb, calcRef, ifthenelseRef);
		if (y == NULL) {
			throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		getArg(y, 0) = newTmpVariable(mb, tp2);
		y = pushArgument(mb, y, getArg(cond, 0));
		y = pushNil(mb, y, tp2);
		y = pushArgument(mb, y, getArg(w, 0));
		pushInstruction(mb, y);

		/* dbl v = double(count) */
		v = newInstruction(mb, batcalcRef, dblRef);
		if (v == NULL) {
			throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		getArg(v, 0) = newTmpVariable(mb, newBatType(TYPE_dbl));
		v = pushArgument(mb, v, getArg(u, 0));
		pushInstruction(mb, v);

		/* dbl x = v / y */
		x = newInstruction(mb, batcalcRef, divRef);
		if (x == NULL) {
			throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		getArg(x, 0) = newTmpVariable(mb, newBatType(TYPE_dbl));
		x = pushArgument(mb, x, getArg(v, 0));
		x = pushArgument(mb, x, getArg(y, 0));
		if (isaBatType(getArgType(mb, x, 0)))
			x = pushNilBat(mb, x);
		if (isaBatType(getArgType(mb, y, 0)))
			x = pushNilBat(mb, x);
		pushInstruction(mb, x);

		/* dbl w = avg * x */
		w = newInstruction(mb, batcalcRef, mulRef);
		if (w == NULL) {
			throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		getArg(w, 0) = newTmpVariable(mb, battp);
		w = pushArgument(mb, w, getArg(r, 0));
		w = pushArgument(mb, w, getArg(x, 0));
		if (isaBatType(getArgType(mb, r, 0)))
			w = pushNilBat(mb, w);
		if (isaBatType(getArgType(mb, x, 0)))
			w = pushNilBat(mb, w);
		pushInstruction(mb, w);

		r = w;

		/* filter nils */
		s = newInstruction(mb, algebraRef, selectNotNilRef);
		if (s == NULL) {
			throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		getArg(s, 0) = newTmpVariable(mb, battp);
		s = pushArgument(mb, s, getArg(r, 0));
		pushInstruction(mb, s);
		r = s;
	}

	if (mb->errors == NULL) {
		s = newInstruction(mb, getModuleId(p),
						   aggr_phase2(getFunctionId(p), tp == TYPE_dbl));
		if (s == NULL) {
			throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		getArg(s, 0) = getArg(p, 0);
		s = pushArgument(mb, s, getArg(r, 0));
		if (isAvg && tp != TYPE_dbl) {
			s = pushArgument(mb, s, getArg(u, 0));
			s = pushArgument(mb, s, getArg(v, 0));
		}
		pushInstruction(mb, s);
	}
	if (mb->errors) {
		str msg = mb->errors;
		mb->errors = NULL;
		return msg;
	}
	return MAL_SUCCEED;
}

static int
chain_by_length(mat_t *mat, int g)
{
	int cnt = 0;
	while (g >= 0) {
		g = mat[g].pm;
		cnt++;
	}
	return cnt;
}

static int
walk_n_back(mat_t *mat, int g, int cnt)
{
	while (cnt > 0) {
		g = mat[g].pm;
		cnt--;
	}
	return g;
}

static int
group_by_ext(matlist_t *ml, int g)
{
	int i;

	for (i = g; i < ml->top; i++) {
		if (ml->v[i].pm == g)
			return i;
	}
	return 0;
}

/* In some cases we have non groupby attribute columns, these require
 * gext.projection(mat.pack(per partition ext.projections(x)))
 */

static int
mat_group_project(Client c, MalBlkPtr mb, InstrPtr p, matlist_t *ml, int e, int a)
{
	int tp = getArgType(mb, p, 0), k;
	mat_t *mat = ml->v;
	InstrPtr ai1 = newInstructionArgs(mb, matRef, packRef, mat[a].mi->argc), r;

	if (!ai1)
		return -1;

	getArg(ai1, 0) = newTmpVariable(mb, tp);
	if (mb->errors) {
		freeInstruction(mb, ai1);
		return -1;
	}

	assert(mat[e].mi->argc == mat[a].mi->argc);
	for (k = 1; k < mat[a].mi->argc; k++) {
		InstrPtr q = copyInstruction(mb, p);
		if (!q) {
			freeInstruction(mb, ai1);
			return -1;
		}

		getArg(q, 0) = newTmpVariable(mb, tp);
		getArg(q, 1) = getArg(mat[e].mi, k);
		getArg(q, 2) = getArg(mat[a].mi, k);
		pushInstruction(mb, q);
		if (mb->errors) {
			freeInstruction(mb, ai1);
			return -1;
		}
		if (setPartnr(c, ml, getArg(mat[a].mi, k), getArg(q, 0), k)) {
			freeInstruction(mb, ai1);
			return -1;
		}

		/* pack the result into a mat */
		ai1 = pushArgument(mb, ai1, getArg(q, 0));
	}
	pushInstruction(mb, ai1);
	if (mb->errors) {
		return -1;
	}

	if ((r = copyInstruction(mb, p)) == NULL)
		return -1;
	getArg(r, 1) = mat[e].mv;
	getArg(r, 2) = getArg(ai1, 0);
	pushInstruction(mb, r);
	return mb->errors ? -1 : 0;
}

/* Per partition aggregates are merged and aggregated together. For
 * most (handled) aggregates that's relatively simple. AVG is somewhat
 * more complex. */
static int
mat_group_aggr(MalBlkPtr mb, InstrPtr p, mat_t *mat, int b, int g, int e)
{
	int tp = getArgType(mb, p, 0), k, tp2 = 0, tpe = getBatType(tp);
	const char *aggr2 = aggr_phase2(getFunctionId(p), tpe == TYPE_dbl);
	int isAvg = (getFunctionId(p) == subavgRef);
	InstrPtr ai1 = newInstructionArgs(mb, matRef, packRef, mat[b].mi->argc),
		ai10 = NULL, ai11 = NULL, ai2;

	if (!ai1)
		return -1;

	getArg(ai1, 0) = newTmpVariable(mb, tp);

	if (mb->errors == NULL && isAvg) {				/* remainders or counts */
		tp2 = newBatType(TYPE_lng);
		ai10 = newInstructionArgs(mb, matRef, packRef, mat[b].mi->argc);
		if (!ai10) {
			freeInstruction(mb, ai1);
			return -1;
		}
		getArg(ai10, 0) = newTmpVariable(mb, tp2);
	}
	if (mb->errors == NULL && isAvg && tpe != TYPE_dbl) {	/* counts */
		tp2 = newBatType(TYPE_lng);
		ai11 = newInstructionArgs(mb, matRef, packRef, mat[b].mi->argc);
		if (!ai11) {
			freeInstruction(mb, ai1);
			freeInstruction(mb, ai10);
			return -1;
		}
		getArg(ai11, 0) = newTmpVariable(mb, tp2);
	}

	for (k = 1; mb->errors == NULL && k < mat[b].mi->argc; k++) {
		int off = 0;
		InstrPtr q = copyInstructionArgs(mb, p, p->argc + (isAvg && tpe == TYPE_dbl));
		if (!q) {
			freeInstruction(mb, ai1);
			freeInstruction(mb, ai10);
			return -1;
		}

		getArg(q, 0) = newTmpVariable(mb, tp);
		if (isAvg && tpe == TYPE_dbl) {
			off = 1;
			getArg(q, 1) = newTmpVariable(mb, tp2);
			q = pushArgument(mb, q, getArg(q, 1));	/* push at end, create space */
			q->retc = 2;
			getArg(q, q->argc - 1) = getArg(q, q->argc - 2);
			getArg(q, q->argc - 2) = getArg(q, q->argc - 3);
		} else if (isAvg) {
			getArg(q, 1) = newTmpVariable(mb, tp2);
			getArg(q, 2) = newTmpVariable(mb, tp2);
			off = 2;
		}
		getArg(q, 1 + off) = getArg(mat[b].mi, k);
		getArg(q, 2 + off) = getArg(mat[g].mi, k);
		getArg(q, 3 + off) = getArg(mat[e].mi, k);
		pushInstruction(mb, q);

		/* pack the result into a mat */
		ai1 = pushArgument(mb, ai1, getArg(q, 0));
		if (isAvg)
			ai10 = pushArgument(mb, ai10, getArg(q, 1));
		if (isAvg && tpe != TYPE_dbl)
			ai11 = pushArgument(mb, ai11, getArg(q, 2));
	}
	pushInstruction(mb, ai1);
	if (isAvg)
		pushInstruction(mb, ai10);
	if (isAvg && tpe != TYPE_dbl)
		pushInstruction(mb, ai11);

	/* for avg we do sum (avg*(count/sumcount) ) */
	if (mb->errors == NULL && isAvg && tpe == TYPE_dbl) {
		InstrPtr r, s, v, w, cond;

		/* lng s = sum counts */
		s = newInstruction(mb, aggrRef, subsumRef);
		if (s == NULL)
			return -1;
		getArg(s, 0) = newTmpVariable(mb, tp2);
		s = pushArgument(mb, s, getArg(ai10, 0));
		s = pushArgument(mb, s, mat[g].mv);
		s = pushArgument(mb, s, mat[e].mv);
		s = pushBit(mb, s, 1);	/* skip nils */
		pushInstruction(mb, s);

		/*  w=count = ifthenelse(s=count==0,NULL,s=count)  */
		cond = newInstruction(mb, batcalcRef, eqRef);
		if (cond == NULL)
			return -1;
		getArg(cond, 0) = newTmpVariable(mb, newBatType(TYPE_bit));
		cond = pushArgument(mb, cond, getArg(s, 0));
		cond = pushLng(mb, cond, 0);
		pushInstruction(mb, cond);

		w = newInstruction(mb, batcalcRef, ifthenelseRef);
		if (w == NULL)
			return -1;
		getArg(w, 0) = newTmpVariable(mb, tp2);
		w = pushArgument(mb, w, getArg(cond, 0));
		w = pushNil(mb, w, TYPE_lng);
		w = pushArgument(mb, w, getArg(s, 0));
		pushInstruction(mb, w);

		/* fetchjoin with groups */
		r = newInstruction(mb, algebraRef, projectionRef);
		if (r == NULL)
			return -1;
		getArg(r, 0) = newTmpVariable(mb, tp2);
		r = pushArgument(mb, r, mat[g].mv);
		r = pushArgument(mb, r, getArg(w, 0));
		pushInstruction(mb, r);
		s = r;

		/* dbl v = double(count) */
		v = newInstruction(mb, batcalcRef, dblRef);
		if (v == NULL)
			return -1;
		getArg(v, 0) = newTmpVariable(mb, newBatType(TYPE_dbl));
		v = pushArgument(mb, v, getArg(ai10, 0));
		pushInstruction(mb, v);

		/* dbl r = v / s */
		r = newInstruction(mb, batcalcRef, divRef);
		if (r == NULL)
			return -1;
		getArg(r, 0) = newTmpVariable(mb, newBatType(TYPE_dbl));
		r = pushArgument(mb, r, getArg(v, 0));
		r = pushArgument(mb, r, getArg(s, 0));
		if (isaBatType(getArgType(mb, v, 0)))
			r = pushNilBat(mb, r);
		if (isaBatType(getArgType(mb, s, 0)))
			r = pushNilBat(mb, r);
		pushInstruction(mb, r);

		/* dbl s = avg * r */
		s = newInstruction(mb, batcalcRef, mulRef);
		if (s == NULL)
			return -1;
		getArg(s, 0) = newTmpVariable(mb, tp);
		s = pushArgument(mb, s, getArg(ai1, 0));
		s = pushArgument(mb, s, getArg(r, 0));
		if (isaBatType(getArgType(mb, ai1, 0)))
			s = pushNilBat(mb, s);
		if (isaBatType(getArgType(mb, r, 0)))
			s = pushNilBat(mb, s);
		pushInstruction(mb, s);

		ai1 = s;
	}
	ai2 = newInstruction(mb, aggrRef, aggr2);
	if (ai2 == NULL)
		return -1;
	getArg(ai2, 0) = getArg(p, 0);
	if (isAvg && tpe != TYPE_dbl) {
		getArg(ai2, 1) = getArg(p, 1);
		getArg(ai2, 2) = getArg(p, 2);
	}
	ai2 = pushArgument(mb, ai2, getArg(ai1, 0));
	if (isAvg && tpe != TYPE_dbl) {
		ai2 = pushArgument(mb, ai2, getArg(ai10, 0));
		ai2 = pushArgument(mb, ai2, getArg(ai11, 0));
	}
	ai2 = pushArgument(mb, ai2, mat[g].mv);
	ai2 = pushArgument(mb, ai2, mat[e].mv);
	ai2 = pushBit(mb, ai2, 1);	/* skip nils */
	pushInstruction(mb, ai2);
	if (mb->errors)
		return -1;
	return 0;
}

/* The mat_group_{new,derive} keep an ext,attr1..attrn table.
 * This is the input for the final second phase group by.
 */
static int
mat_pack_group(MalBlkPtr mb, matlist_t *ml, int g)
{
	mat_t *mat = ml->v;
	int cnt = chain_by_length(mat, g), i;
	InstrPtr cur = NULL;

	for (i = cnt - 1; i >= 0; i--) {
		/* if cur is non-NULL, it's a subgroup; if i is zero, it's "done" */
		InstrPtr grp = newInstruction(mb, groupRef,
									  cur ? i ? subgroupRef : subgroupdoneRef : i ?
									  groupRef : groupdoneRef);
		if (grp == NULL)
			return -1;
		int ogrp = walk_n_back(mat, g, i);
		int oext = group_by_ext(ml, ogrp);
		int attr = mat[oext].im;

		getArg(grp, 0) = mat[ogrp].mv;
		grp = pushReturn(mb, grp, mat[oext].mv);
		grp = pushReturn(mb, grp, newTmpVariable(mb, newBatType(TYPE_lng)));
		grp = pushArgument(mb, grp, getArg(mat[attr].mi, 0));
		if (cur)
			grp = pushArgument(mb, grp, getArg(cur, 0));
		pushInstruction(mb, grp);
		cur = grp;
		if (mb->errors)
			return -1;
	}
	mat[g].im = -1;				/* only pack once */
	return 0;
}

/*
 * foreach parent subgroup, do the
 * 	e2.projection(grp.projection((ext.projection(b)))
 * and one for the current group
 */
static int
mat_group_attr(Client c, MalBlkPtr mb, matlist_t *ml, int g, InstrPtr cext, int push)
{
	int cnt = chain_by_length(ml->v, g), i;	/* number of attributes */
	int ogrp = g;				/* previous group */

	for (i = 0; i < cnt; i++) {
		int agrp = walk_n_back(ml->v, ogrp, i);
		int b = ml->v[agrp].im;
		int aext = group_by_ext(ml, agrp);
		int a = ml->v[aext].im;
		int atp = getArgType(mb, ml->v[a].mi, 0), k;
		InstrPtr attr = newInstructionArgs(mb, matRef, packRef, ml->v[a].mi->argc);
		if (attr == NULL) {
			return -1;
		}

		//getArg(attr,0) = newTmpVariable(mb, atp);
		getArg(attr, 0) = getArg(ml->v[b].mi, 0);

		for (k = 1; mb->errors == NULL && k < ml->v[a].mi->argc; k++) {
			InstrPtr r = newInstruction(mb, algebraRef, projectionRef);
			InstrPtr q = newInstruction(mb, algebraRef, projectionRef);
			if (r == NULL || q == NULL) {
				freeInstruction(mb, attr);
				freeInstruction(mb, r);
				freeInstruction(mb, q);
				return -1;
			}

			getArg(r, 0) = newTmpVariable(mb, newBatType(TYPE_oid));
			r = pushArgument(mb, r, getArg(cext, k));
			r = pushArgument(mb, r, getArg(ml->v[ogrp].mi, k));
			pushInstruction(mb, r);

			getArg(q, 0) = newTmpVariable(mb, atp);
			q = pushArgument(mb, q, getArg(r, 0));
			q = pushArgument(mb, q, getArg(ml->v[a].mi, k));
			pushInstruction(mb, q);

			attr = pushArgument(mb, attr, getArg(q, 0));
		}
		if (push)
			pushInstruction(mb, attr);
		if (mb->errors || mat_add_var(c, ml, attr, NULL, getArg(attr, 0), mat_ext,
									  -1, -1, push)) {
			if (!push)
				freeInstruction(mb, attr);
			return -1;
		}
		/* keep new attribute with the group extend */
		ml->v[aext].im = ml->top - 1;
	}
	return 0;
}

static int
mat_group_new(Client c, MalBlkPtr mb, InstrPtr p, matlist_t *ml, int b)
{
	int tp0 = getArgType(mb, p, 0);
	int tp1 = getArgType(mb, p, 1);
	int tp2 = getArgType(mb, p, 2);
	int atp = getArgType(mb, p, 3), i, a, g, push = 0;
	InstrPtr r0, r1, r2, attr;

	if (getFunctionId(p) == subgroupdoneRef || getFunctionId(p) == groupdoneRef)
		push = 1;

	r0 = newInstructionArgs(mb, matRef, packRef, ml->v[b].mi->argc);
	if (r0 == NULL)
		return -1;
	getArg(r0, 0) = newTmpVariable(mb, tp0);

	r1 = newInstructionArgs(mb, matRef, packRef, ml->v[b].mi->argc);
	if (r1 == NULL) {
		freeInstruction(mb, r0);
		return -1;
	}
	getArg(r1, 0) = newTmpVariable(mb, tp1);

	r2 = newInstructionArgs(mb, matRef, packRef, ml->v[b].mi->argc);
	if (r2 == NULL) {
		freeInstruction(mb, r0);
		freeInstruction(mb, r1);
		return -1;
	}
	getArg(r2, 0) = newTmpVariable(mb, tp2);

	/* we keep an extend, attr table result, which will later be used
	 * when we pack the group result */
	attr = newInstructionArgs(mb, matRef, packRef, ml->v[b].mi->argc);
	if (attr == NULL) {
		freeInstruction(mb, r0);
		freeInstruction(mb, r1);
		freeInstruction(mb, r2);
		return -1;
	}
	getArg(attr, 0) = getArg(ml->v[b].mi, 0);

	for (i = 1; mb->errors == NULL && i < ml->v[b].mi->argc; i++) {
		InstrPtr q = copyInstruction(mb, p), r;
		if (!q) {
			freeInstruction(mb, r0);
			freeInstruction(mb, r1);
			freeInstruction(mb, r2);
			freeInstruction(mb, attr);
			return -1;
		}

		getArg(q, 0) = newTmpVariable(mb, tp0);
		getArg(q, 1) = newTmpVariable(mb, tp1);
		getArg(q, 2) = newTmpVariable(mb, tp2);
		getArg(q, 3) = getArg(ml->v[b].mi, i);
		pushInstruction(mb, q);
		if (setPartnr(c, ml, getArg(ml->v[b].mi, i), getArg(q, 0), i)
			|| setPartnr(c, ml, getArg(ml->v[b].mi, i), getArg(q, 1), i)
			|| setPartnr(c, ml, getArg(ml->v[b].mi, i), getArg(q, 2), i)) {
			freeInstruction(mb, r0);
			freeInstruction(mb, r1);
			freeInstruction(mb, r2);
			freeInstruction(mb, attr);
			return -1;
		}

		/* add result to mats */
		r0 = pushArgument(mb, r0, getArg(q, 0));
		r1 = pushArgument(mb, r1, getArg(q, 1));
		r2 = pushArgument(mb, r2, getArg(q, 2));

		r = newInstruction(mb, algebraRef, projectionRef);
		if (r == NULL) {
			freeInstruction(mb, r0);
			freeInstruction(mb, r1);
			freeInstruction(mb, r2);
			freeInstruction(mb, attr);
			return -1;
		}
		getArg(r, 0) = newTmpVariable(mb, atp);
		r = pushArgument(mb, r, getArg(q, 1));
		r = pushArgument(mb, r, getArg(ml->v[b].mi, i));
		if (setPartnr(c, ml, getArg(ml->v[b].mi, i), getArg(r, 0), i)) {
			freeInstruction(mb, r0);
			freeInstruction(mb, r1);
			freeInstruction(mb, r2);
			freeInstruction(mb, attr);
			freeInstruction(mb, r);
			return -1;
		}
		pushInstruction(mb, r);

		attr = pushArgument(mb, attr, getArg(r, 0));
	}
	pushInstruction(mb, r0);
	pushInstruction(mb, r1);
	pushInstruction(mb, r2);
	if (push)
		pushInstruction(mb, attr);

	/* create mat's for the intermediates */
	a = ml->top;
	if (mb->errors || mat_add_var(c, ml, attr, NULL, getArg(attr, 0), mat_ext,
								  -1, -1, push)) {
		if (!push)
			freeInstruction(mb, attr);
		return -1;
	}
	g = ml->top;
	if (mat_add_var(c, ml, r0, p, getArg(p, 0), mat_grp, b, -1, 1)
		|| mat_add_var(c, ml, r1, p, getArg(p, 1), mat_ext, a, ml->top - 1, 1)	/* point back at group */
		|| mat_add_var(c, ml, r2, p, getArg(p, 2), mat_cnt, -1, ml->top - 1, 1)) /* point back at ext */
		return -1;
	if (push)
		return mat_pack_group(mb, ml, g);
	return 0;
}

static int
mat_group_derive(Client c, MalBlkPtr mb, InstrPtr p, matlist_t *ml, int b, int g)
{
	int tp0 = getArgType(mb, p, 0);
	int tp1 = getArgType(mb, p, 1);
	int tp2 = getArgType(mb, p, 2);
	int atp = getArgType(mb, p, 3), i, a, push = 0;
	InstrPtr r0, r1, r2, attr;

	if (getFunctionId(p) == subgroupdoneRef || getFunctionId(p) == groupdoneRef)
		push = 1;

	if (ml->v[g].im == -1) {	/* already packed */
		InstrPtr q = copyInstruction(mb, p);
		if (!q)
			return -1;
		pushInstruction(mb, q);
		return 0;
	}

	r0 = newInstructionArgs(mb, matRef, packRef, ml->v[b].mi->argc);
	if (r0 == NULL)
		return -1;
	getArg(r0, 0) = newTmpVariable(mb, tp0);

	r1 = newInstructionArgs(mb, matRef, packRef, ml->v[b].mi->argc);
	if (r1 == NULL) {
		freeInstruction(mb, r0);
		return -1;
	}
	getArg(r1, 0) = newTmpVariable(mb, tp1);

	r2 = newInstructionArgs(mb, matRef, packRef, ml->v[b].mi->argc);
	if (r2 == NULL) {
		freeInstruction(mb, r0);
		freeInstruction(mb, r1);
		return -1;
	}
	getArg(r2, 0) = newTmpVariable(mb, tp2);

	/* we keep an extend, attr table result, which will later be used
	 * when we pack the group result */
	attr = newInstructionArgs(mb, matRef, packRef, ml->v[b].mi->argc);
	if (attr == NULL) {
		freeInstruction(mb, r0);
		freeInstruction(mb, r1);
		freeInstruction(mb, r2);
		return -1;
	}
	getArg(attr, 0) = getArg(ml->v[b].mi, 0);

	/* we need overlapping ranges */
	for (i = 1; mb->errors == NULL && i < ml->v[b].mi->argc; i++) {
		InstrPtr q = copyInstruction(mb, p), r;
		if (!q) {
			freeInstruction(mb, r0);
			freeInstruction(mb, r1);
			freeInstruction(mb, r2);
			freeInstruction(mb, attr);
			return -1;
		}

		getArg(q, 0) = newTmpVariable(mb, tp0);
		getArg(q, 1) = newTmpVariable(mb, tp1);
		getArg(q, 2) = newTmpVariable(mb, tp2);
		getArg(q, 3) = getArg(ml->v[b].mi, i);
		getArg(q, 4) = getArg(ml->v[g].mi, i);
		pushInstruction(mb, q);
		if (setPartnr(c, ml, getArg(ml->v[b].mi, i), getArg(q, 0), i)
			|| setPartnr(c, ml, getArg(ml->v[b].mi, i), getArg(q, 1), i)
			|| setPartnr(c, ml, getArg(ml->v[b].mi, i), getArg(q, 2), i)) {
			freeInstruction(mb, r0);
			freeInstruction(mb, r1);
			freeInstruction(mb, r2);
			freeInstruction(mb, attr);
			return -1;
		}

		/* add result to mats */
		r0 = pushArgument(mb, r0, getArg(q, 0));
		r1 = pushArgument(mb, r1, getArg(q, 1));
		r2 = pushArgument(mb, r2, getArg(q, 2));

		r = newInstruction(mb, algebraRef, projectionRef);
		if (r == NULL) {
			freeInstruction(mb, r0);
			freeInstruction(mb, r1);
			freeInstruction(mb, r2);
			freeInstruction(mb, attr);
			return -1;
		}
		getArg(r, 0) = newTmpVariable(mb, atp);
		r = pushArgument(mb, r, getArg(q, 1));
		r = pushArgument(mb, r, getArg(ml->v[b].mi, i));
		if (setPartnr(c, ml, getArg(ml->v[b].mi, i), getArg(r, 0), i)) {
			freeInstruction(mb, r0);
			freeInstruction(mb, r1);
			freeInstruction(mb, r2);
			freeInstruction(mb, attr);
			freeInstruction(mb, r);
			return -1;
		}
		pushInstruction(mb, r);

		attr = pushArgument(mb, attr, getArg(r, 0));
	}
	pushInstruction(mb, r0);
	pushInstruction(mb, r1);
	pushInstruction(mb, r2);
	if (push)
		pushInstruction(mb, attr);

	if (mb->errors || mat_group_attr(c, mb, ml, g, r1, push))
		return -1;

	/* create mat's for the intermediates */
	a = ml->top;
	if (mat_add_var(c, ml, attr, NULL, getArg(attr, 0), mat_ext, -1, -1, push)) {
		if (!push)
			freeInstruction(mb, attr);
		return -1;
	}
	if (mat_add_var(c, ml, r0, p, getArg(p, 0), mat_grp, b, g, 1))
		return -1;
	g = ml->top - 1;
	if (mat_add_var(c, ml, r1, p, getArg(p, 1), mat_ext, a, ml->top - 1, 1) ||	/* point back at group */
		mat_add_var(c, ml, r2, p, getArg(p, 2), mat_cnt, -1, ml->top - 1, 1))	/* point back at ext */
		return -1;
	if (push)
		return mat_pack_group(mb, ml, g);
	return 0;
}

static int
mat_topn_project(MalBlkPtr mb, InstrPtr p, mat_t *mat, int m, int n)
{
	int tpe = getArgType(mb, p, 0), k;
	InstrPtr pck, q;

	pck = newInstructionArgs(mb, matRef, packRef, mat[m].mi->argc);
	if (pck == NULL)
		return -1;
	getArg(pck, 0) = newTmpVariable(mb, tpe);

	for (k = 1; mb->errors == NULL && k < mat[m].mi->argc; k++) {
		InstrPtr q = copyInstruction(mb, p);
		if (!q) {
			freeInstruction(mb, pck);
			return -1;
		}

		getArg(q, 0) = newTmpVariable(mb, tpe);
		getArg(q, 1) = getArg(mat[m].mi, k);
		getArg(q, 2) = getArg(mat[n].mi, k);
		pushInstruction(mb, q);

		pck = pushArgument(mb, pck, getArg(q, 0));
	}
	pushInstruction(mb, pck);

	if (mb->errors || (q = copyInstruction(mb, p)) == NULL)
		return -1;
	getArg(q, 2) = getArg(pck, 0);
	pushInstruction(mb, q);
	if (mb->errors)
		return -1;
	return 0;
}

static int
mat_pack_topn(MalBlkPtr mb, InstrPtr slc, mat_t *mat, int m)
{
	/* find chain of topn's */
	int cnt = chain_by_length(mat, m), i;
	InstrPtr cur = NULL;

	for (i = cnt - 1; mb->errors == NULL && i >= 0; i--) {
		int otpn = walk_n_back(mat, m, i), var = 1, k;
		int attr = mat[otpn].im;
		int tpe = getVarType(mb, getArg(mat[attr].mi, 0));
		InstrPtr pck, tpn, otopn = mat[otpn].org, a;

		pck = newInstructionArgs(mb, matRef, packRef, mat[attr].mi->argc);
		if (pck == NULL)
			return -1;
		getArg(pck, 0) = newTmpVariable(mb, tpe);

		/* m.projection(attr); */
		for (k = 1; mb->errors == NULL && k < mat[attr].mi->argc; k++) {
			InstrPtr q = newInstruction(mb, algebraRef, projectionRef);
			if (q == NULL) {
				freeInstruction(mb, pck);
				return -1;
			}
			getArg(q, 0) = newTmpVariable(mb, tpe);

			q = pushArgument(mb, q, getArg(slc, k));
			q = pushArgument(mb, q, getArg(mat[attr].mi, k));
			pushInstruction(mb, q);

			pck = pushArgument(mb, pck, getArg(q, 0));
		}
		pushInstruction(mb, pck);

		a = pck;

		if (mb->errors || (tpn = copyInstruction(mb, otopn)) == NULL)
			return -1;
		var = 1;
		if (cur) {
			getArg(tpn, tpn->retc + var) = getArg(cur, 0);
			var ++;
			if (cur->retc == 2) {
				getArg(tpn, tpn->retc + var) = getArg(cur, 1);
				var ++;
			}
		}
		getArg(tpn, tpn->retc) = getArg(a, 0);
		pushInstruction(mb, tpn);
		cur = tpn;
	}
	if (mb->errors)
		return -1;
	return 0;
}

static int
mat_topn(Client c, MalBlkPtr mb, InstrPtr p, matlist_t *ml, int m, int n, int o)
{
	int tpe = getArgType(mb, p, 0), k, is_slice = isSlice(p), zero = -1;
	InstrPtr pck, gpck = NULL, q, r;
	int with_groups = (p->retc == 2), piv = 0, topn2 = (n >= 0);

	assert(topn2 || o < 0);
	/* dummy mat instruction (needed to share result of p) */
	pck = newInstructionArgs(mb, matRef, packRef, ml->v[m].mi->argc);
	if (pck == NULL)
		return -1;
	getArg(pck, 0) = getArg(p, 0);

	if (with_groups) {
		gpck = newInstructionArgs(mb, matRef, packRef, ml->v[m].mi->argc);
		if (gpck == NULL) {
			freeInstruction(mb, pck);
			return -1;
		}
		getArg(gpck, 0) = getArg(p, 1);
	}

	if (is_slice) {
		ValRecord cst;
		cst.vtype = getArgType(mb, p, 2);
		cst.val.lval = 0;
		cst.len = 0;
		zero = defConstant(mb, cst.vtype, &cst);
		if (zero < 0) {
			freeInstruction(mb, pck);
			return -1;
		}
	}
	assert((n < 0 && o < 0)
		   || (ml->v[m].mi->argc == ml->v[n].mi->argc
			   && ml->v[m].mi->argc == ml->v[o].mi->argc));

	for (k = 1; mb->errors == NULL && k < ml->v[m].mi->argc; k++) {
		if ((q = copyInstruction(mb, p)) == NULL) {
			freeInstruction(mb, gpck);
			freeInstruction(mb, pck);
			return -1;
		}
		getArg(q, 0) = newTmpVariable(mb, tpe);
		if (with_groups)
			getArg(q, 1) = newTmpVariable(mb, tpe);
		getArg(q, q->retc) = getArg(ml->v[m].mi, k);
		if (is_slice)			/* lower bound should always be 0 on partial slices */
			getArg(q, q->retc + 1) = zero;
		else if (topn2) {
			getArg(q, q->retc + 1) = getArg(ml->v[n].mi, k);
			getArg(q, q->retc + 2) = getArg(ml->v[o].mi, k);
		}
		pushInstruction(mb, q);

		pck = pushArgument(mb, pck, getArg(q, 0));
		if (with_groups)
			gpck = pushArgument(mb, gpck, getArg(q, 1));
	}

	piv = ml->top;
	if (mb->errors || mat_add_var(c, ml, pck, p, getArg(p, 0), is_slice ? mat_slc : mat_tpn, m, n, 0)) {
		freeInstruction(mb, pck);
		freeInstruction(mb, gpck);
		return -1;
	}
	if (with_groups
		&& mat_add_var(c, ml, gpck, p, getArg(p, 1), is_slice ? mat_slc : mat_tpn,
					   m, piv, 0)) {
		freeInstruction(mb, gpck);
		return -1;
	}

	if (is_slice || p->retc == 1 /* single result, ie last of the topn's */ ) {
		if (ml->v[m].type == mat_tpn || !is_slice) {
			if (mat_pack_topn(mb, pck, ml->v, (!is_slice) ? piv : m))
				return -1;
		}

		/* topn/slice over merged parts */
		if (is_slice) {
			/* real instruction */
			r = newInstructionArgs(mb, matRef, packRef, pck->argc);
			if (r == NULL)
				return -1;
			getArg(r, 0) = newTmpVariable(mb, tpe);

			for (k = 1; k < pck->argc; k++)
				r = pushArgument(mb, r, getArg(pck, k));
			pushInstruction(mb, r);

			if ((q = copyInstruction(mb, p)) == NULL)
				return -1;
			setFunctionId(q, subsliceRef);
			if (ml->v[m].type != mat_tpn || is_slice)
				getArg(q, 1) = getArg(r, 0);
			pushInstruction(mb, q);
		}

		ml->v[piv].type = mat_slc;
	}
	if (mb->errors)
		return -1;
	return 0;
}

static int
mat_sample(Client c, MalBlkPtr mb, InstrPtr p, matlist_t *ml, int m)
{
	/* transform
	 * a := sample.subuniform(b,n);
	 * into
	 * t1 := sample.subuniform(b1,n);
	 * t2 := sample.subuniform(b2,n);
	 * ...
	 * t0 := mat.pack(t1,t2,...);
	 * tn := sample.subuniform(t0,n);
	 * a := algebra.projection(tn,t0);
	 *
	 * Note that this does *not* give a uniform sample of the original
	 * bat b!
	 */

	int tpe = getArgType(mb, p, 0), k, piv;
	InstrPtr pck, q, r;

	pck = newInstructionArgs(mb, matRef, packRef, ml->v[m].mi->argc);
	if (pck == NULL)
		return -1;
	getArg(pck, 0) = newTmpVariable(mb, tpe);

	for (k = 1; mb->errors == NULL && k < ml->v[m].mi->argc; k++) {
		if ((q = copyInstruction(mb, p)) == NULL) {
			freeInstruction(mb, pck);
			return -1;
		}
		getArg(q, 0) = newTmpVariable(mb, tpe);
		getArg(q, q->retc) = getArg(ml->v[m].mi, k);
		pushInstruction(mb, q);
		pck = pushArgument(mb, pck, getArg(q, 0));
	}

	piv = ml->top;
	if (mb->errors || mat_add_var(c, ml, pck, p, getArg(p, 0), mat_slc, m, -1, 1)) {
		freeInstruction(mb, pck);
		return -1;
	}
	pushInstruction(mb, pck);

	if ((q = copyInstruction(mb, p)) == NULL)
		return -1;
	getArg(q, 0) = newTmpVariable(mb, tpe);
	getArg(q, q->retc) = getArg(pck, 0);
	pushInstruction(mb, q);

	r = newInstruction(mb, algebraRef, projectionRef);
	if (r == NULL)
		return -1;
	getArg(r, 0) = getArg(p, 0);
	r = pushArgument(mb, r, getArg(q, 0));
	r = pushArgument(mb, r, getArg(pck, 0));
	pushInstruction(mb, r);

	matlist_pack(ml, piv);
	ml->v[piv].type = mat_slc;
	if (mb->errors)
		return -1;
	return 0;
}

str
OPTmergetableImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
							InstrPtr pci)
{
	InstrPtr p, *old;
	matlist_t ml;
	int oldtop, fm, fn, fo, fe, i, k, m, n, o, e, slimit, bailout = 0;
	int size = 0, match, actions = 0, distinct_topn = 0, /*topn_res = 0, */ groupdone = 0, *vars;	//, maxvars;
	char *group_input;
	str msg = MAL_SUCCEED;

	ma_open(cntxt->ta);

	if (isOptimizerUsed(mb, pci, mitosisRef) <= 0)
		goto cleanup2;
	old = mb->stmt;
	oldtop = mb->stop;

	vars = (int *) ma_alloc(cntxt->ta, sizeof(int) * mb->vtop);
	//maxvars = mb->vtop;
	group_input = (char *) ma_zalloc(cntxt->ta, sizeof(char) * mb->vtop);
	if (vars == NULL || group_input == NULL) {
		ma_close(cntxt->ta);
		throw(MAL, "optimizer.mergetable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	/* check for bailout conditions */
	for (i = 1; i < oldtop && !bailout; i++) {
		int j;

		p = old[i];

		for (j = 0; j < p->retc; j++) {
			int res = getArg(p, j);
			vars[res] = i;
		}

		/* pack if there is a group statement following a groupdone (ie aggr(distinct)) */
		if (getModuleId(p) == algebraRef && getFunctionId(p) == groupedfirstnRef)
				groupdone = 1;
		if (getModuleId(p) == groupRef && p->argc == 5
			&& (getFunctionId(p) == subgroupRef
				|| getFunctionId(p) == subgroupdoneRef
				|| getFunctionId(p) == groupRef
				|| getFunctionId(p) == groupdoneRef)) {
			InstrPtr q = old[vars[getArg(p, p->argc - 1)]];	/* group result from a previous group(done) */

			if (getFunctionId(q) == subgroupdoneRef
				|| getFunctionId(q) == groupdoneRef)
				groupdone = 1;
		}
		/* bail out if there is a input for a group, which has been used for a group already (solves problems with cube like groupings) */
		if (getModuleId(p) == groupRef
			&& (getFunctionId(p) == subgroupRef
				|| getFunctionId(p) == subgroupdoneRef
				|| getFunctionId(p) == groupRef
				|| getFunctionId(p) == groupdoneRef)) {
			int input = getArg(p, p->retc);	/* argument one is first input */

			if (group_input[input]) {
				TRC_INFO(MAL_OPTIMIZER,
						 "Mergetable bailout on group input reuse in group statement\n");
				bailout = 1;
			}

			group_input[input] = 1;
		}
		if (getModuleId(p) == algebraRef && getFunctionId(p) == selectNotNilRef) {
			TRC_INFO(MAL_OPTIMIZER, "Mergetable bailout not nil ref\n");
			bailout = 1;
		}
		if (getModuleId(p) == algebraRef && getFunctionId(p) == semijoinRef) {
			TRC_INFO(MAL_OPTIMIZER, "Mergetable bailout semijoin ref\n");
			bailout = 1;
		}
		if (getModuleId(p) == algebraRef && getFunctionId(p) == thetajoinRef) {
			assert(p->argc == 9);
			if (p->argc == 9
				&& getVarConstant(mb,
								  getArg(p,
										 6)).val.ival == 6 /* op == '<>' */ ) {
				TRC_INFO(MAL_OPTIMIZER, "Mergetable bailout thetajoin ref\n");
				bailout = 1;
			}
		}
		if (isSample(p)) {
			bailout = 1;
		}
		/*
		   if (isTopn(p))
		   topn_res = getArg(p, 0);
		 */
		/* not idea how to detect this yet */
		//distinct_topn = 1;
	}

	ml.horigin = 0;
	ml.torigin = 0;
	ml.v = 0;
	ml.vars = 0;
	if (bailout)
		goto cleanup;

	/* the number of MATs is limited to the variable stack */
	ml.size = mb->vtop;
	ml.top = 0;
	ml.v = (mat_t *) ma_zalloc(cntxt->ta, ml.size * sizeof(mat_t));
	ml.vsize = mb->vsize;
	ml.horigin = (int *) ma_alloc(cntxt->ta, sizeof(int) * ml.vsize);
	ml.torigin = (int *) ma_alloc(cntxt->ta, sizeof(int) * ml.vsize);
	ml.vars = (int *) ma_alloc(cntxt->ta, sizeof(int) * ml.vsize);
	if (ml.v == NULL || ml.horigin == NULL || ml.torigin == NULL
		|| ml.vars == NULL) {
		goto cleanup;
	}
	for (i = 0; i < ml.vsize; i++) {
		ml.horigin[i] = ml.torigin[i] = -1;
		ml.vars[i] = -1;
	}

	slimit = mb->ssize;
	size = (mb->stop * 1.2 < mb->ssize) ? mb->ssize : (int) (mb->stop * 1.2);
	if (newMalBlkStmt(mb, size) < 0) {
		msg = createException(MAL, "optimizer.mergetable",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto cleanup;
	}

	for (i = 0; i < oldtop; i++) {
		int bats = 0, nilbats = 0;
		InstrPtr r, cp;

		p = old[i];

		if (p->token == ENDsymbol)	/* don't copy the optimizer pipeline added after final instruction */
			break;
		if (getModuleId(p) == matRef
			&& (getFunctionId(p) == newRef || getFunctionId(p) == packRef)) {
			if (mat_set_prop(cntxt, &ml, mb, p)
				|| mat_add_var(cntxt, &ml, p, NULL, getArg(p, 0), mat_none, -1, -1,
							   1)) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			continue;
		}

		/*
		 * If the instruction does not contain MAT references it can simply be added.
		 * Otherwise we have to decide on either packing them or replacement.
		 */
		if ((match = nr_of_mats(p, &ml)) == 0) {
			cp = copyInstruction(mb, p);
			if (!cp) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			pushInstruction(mb, cp);
			continue;
		}
		bats = nr_of_bats(mb, p);
		nilbats = nr_of_nilbats(mb, p);

		/* left joins can match at isMatJoinOp, so run this check beforehand */
		if (match > 0 && isMatLeftJoinOp(p) && p->argc >= 5 && p->retc == 2
			&& (match == 1 || match == 2) && bats + nilbats == 4) {
			m = is_a_mat(getArg(p, p->retc), &ml);
			o = is_a_mat(getArg(p, p->retc + 2), &ml);

			if ((match == 1 && m >= 0) || (match == 2 && m >= 0 && o >= 0)) {
				if (mat_join2(cntxt, mb, p, &ml, m, -1, o, -1)) {
					msg = createException(MAL, "optimizer.mergetable",
										  SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto cleanup;
				}
				actions++;
				continue;
			}
		}

		/* (l,r) Join (L, R, ..)
		 * 2 -> (l,r) equi/theta joins (l,r)
		 * 3 -> (l,r) range-joins (l,r1,r2)
		 * NxM -> (l,r) filter-joins (l1,..,ln,r1,..,rm)
		 */
		if (match > 0 && isMatJoinOp(p) && !isMatLeftJoinOp(p) && p->argc >= 5
			&& p->retc == 2 && bats + nilbats >= 4) {
			if (bats + nilbats == 4) {
				m = is_a_mat(getArg(p, p->retc), &ml);
				n = is_a_mat(getArg(p, p->retc + 1), &ml);
				o = is_a_mat(getArg(p, p->retc + 2), &ml);
				e = is_a_mat(getArg(p, p->retc + 3), &ml);
				if (mat_join2(cntxt, mb, p, &ml, m, n, o, e)) {
					msg = createException(MAL, "optimizer.mergetable",
										  SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto cleanup;
				}
			} else {
				if (bats + nilbats == 5 && !is_a_mat(getArg(p, p->retc), &ml) && match == 2) {
					n = is_a_mat(getArg(p, p->retc + 1), &ml);
					o = is_a_mat(getArg(p, p->retc + 2), &ml);
					if (mat_rangejoin(cntxt, mb, p, &ml, n, o)) {
						msg = createException(MAL, "optimizer.mergetable",
										  SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto cleanup;
					}
				} else
				if (mat_joinNxM(cntxt, mb, p, &ml, bats)) {
					msg = createException(MAL, "optimizer.mergetable",
										  SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto cleanup;
				}
			}
			actions++;
			continue;
		}
		if (match > 0 && getModuleId(p) == algebraRef
			&& getFunctionId(p) == crossRef && p->argc == 5 && p->retc == 2
			&& bats == 2) {
			int max_one = (isVarConstant(mb, getArg(p, 4))
						   && getVarConstant(mb, getArg(p, 4)).val.btval);
			if (!max_one) {
				m = is_a_mat(getArg(p, p->retc), &ml);
				n = is_a_mat(getArg(p, p->retc + 1), &ml);
				if (mat_join2(cntxt, mb, p, &ml, m, n, -1, -1)) {
					msg = createException(MAL, "optimizer.mergetable",
										  SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto cleanup;
				}
				actions++;
				continue;
			}
		}
		/*
		 * Aggregate handling is a prime target for optimization.
		 * The simple cases are dealt with first.
		 * Handle the rewrite v:=aggr.count(b) and sum()
		 * And the min/max is as easy
		 */
		if (match == 1 && p->argc >= 2 &&
			((getModuleId(p) == aggrRef &&
			  (getFunctionId(p) == countRef
			   || getFunctionId(p) == count_no_nilRef
			   || getFunctionId(p) == minRef || getFunctionId(p) == maxRef
			   || getFunctionId(p) == avgRef || getFunctionId(p) == sumRef
			   || getFunctionId(p) == prodRef)))
			&& (m = is_a_mat(getArg(p, p->retc + 0), &ml)) >= 0) {
			if ((msg = mat_aggr(mb, p, ml.v, m)) != MAL_SUCCEED)
				goto cleanup;
			actions++;
			continue;
		}

		if (match == 1 && bats == 1 && p->argc == 4 && isSlice(p)
			&& ((m = is_a_mat(getArg(p, p->retc), &ml)) >= 0)) {
			if (mat_topn(cntxt, mb, p, &ml, m, -1, -1)) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			actions++;
			continue;
		}

		if (match == 1 && bats == 1 && p->argc == 3 && isSample(p)
			&& ((m = is_a_mat(getArg(p, p->retc), &ml)) >= 0)) {
			if (mat_sample(cntxt, mb, p, &ml, m)) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			actions++;
			continue;
		}

		if (!distinct_topn && match == 1 && bats == 1 && isTopn(p)
			&& ((m = is_a_mat(getArg(p, p->retc), &ml)) >= 0)) {
			if (mat_topn(cntxt, mb, p, &ml, m, -1, -1)) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			actions++;
			continue;
		}
		if (!distinct_topn && match == 3 && bats == 3 && isTopn(p)
			&& ((m = is_a_mat(getArg(p, p->retc), &ml)) >= 0)
			&& ((n = is_a_mat(getArg(p, p->retc + 1), &ml)) >= 0)
			&& ((o = is_a_mat(getArg(p, p->retc + 2), &ml)) >= 0)) {
			if (mat_topn(cntxt, mb, p, &ml, m, n, o)) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			actions++;
			continue;
		}

		/* Now we handle subgroup and aggregation statements. */
		if (!groupdone && match == 1 && bats == 1 && p->argc == 4
			&& getModuleId(p) == groupRef &&
			(getFunctionId(p) == subgroupRef
			 || getFunctionId(p) == subgroupdoneRef
			 || getFunctionId(p) == groupRef
			 || getFunctionId(p) == groupdoneRef)
			&& ((m = is_a_mat(getArg(p, p->retc), &ml)) >= 0)) {
			if (mat_group_new(cntxt, mb, p, &ml, m)) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			actions++;
			continue;
		}
		if (!groupdone && match == 2 && bats == 2 && p->argc == 5
			&& getModuleId(p) == groupRef &&
			(getFunctionId(p) == subgroupRef
			 || getFunctionId(p) == subgroupdoneRef
			 || getFunctionId(p) == groupRef
			 || getFunctionId(p) == groupdoneRef)
			&& ((m = is_a_mat(getArg(p, p->retc), &ml)) >= 0)
			&& ((n = is_a_mat(getArg(p, p->retc + 1), &ml)) >= 0)
			&& ml.v[n].im >= 0 /* not packed */ ) {
			if (mat_group_derive(cntxt, mb, p, &ml, m, n)) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			actions++;
			continue;
		}
		/* TODO sub'aggr' with cand list */
		if (match == 3 && bats == 3 && getModuleId(p) == aggrRef && p->argc >= 4
			&& (getFunctionId(p) == subcountRef
				|| getFunctionId(p) == subminRef
				|| getFunctionId(p) == submaxRef
				|| getFunctionId(p) == subavgRef
				|| getFunctionId(p) == subsumRef
				|| getFunctionId(p) == subprodRef)
			&& ((m = is_a_mat(getArg(p, p->retc + 0), &ml)) >= 0)
			&& ((n = is_a_mat(getArg(p, p->retc + 1), &ml)) >= 0)
			&& ((o = is_a_mat(getArg(p, p->retc + 2), &ml)) >= 0)) {
			if (mat_group_aggr(mb, p, ml.v, m, n, o)) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			actions++;
			continue;
		}
		/* Handle cases of ext.projection and .projection(grp) */
		if (match == 2 && getModuleId(p) == algebraRef
			&& getFunctionId(p) == projectionRef
			&& (m = is_a_mat(getArg(p, 1), &ml)) >= 0
			&& (n = is_a_mat(getArg(p, 2), &ml)) >= 0
			&& (ml.v[m].type == mat_ext || ml.v[n].type == mat_grp)) {
			assert(ml.v[m].pushed);
			if (!ml.v[n].pushed) {
				if (mat_group_project(cntxt, mb, p, &ml, m, n)) {
					msg = createException(MAL, "optimizer.mergetable",
										  SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto cleanup;
				}
			} else {
				cp = copyInstruction(mb, p);
				if (!cp) {
					msg = createException(MAL, "optimizer.mergetable",
										  SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto cleanup;
				}
				pushInstruction(mb, cp);
			}
			continue;
		}
		if (match == 1 && getModuleId(p) == algebraRef
			&& getFunctionId(p) == projectRef
			&& (m = is_a_mat(getArg(p, 1), &ml)) >= 0
			&& (ml.v[m].type == mat_ext)) {
			assert(ml.v[m].pushed);
			cp = copyInstruction(mb, p);
			if (!cp) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			pushInstruction(mb, cp);
			continue;
		}

		/* Handle cases of slice.projection */
		if (match == 2 && getModuleId(p) == algebraRef
			&& getFunctionId(p) == projectionRef
			&& (m = is_a_mat(getArg(p, 1), &ml)) >= 0
			&& (n = is_a_mat(getArg(p, 2), &ml)) >= 0
			&& (ml.v[m].type == mat_slc)) {
			if (mat_topn_project(mb, p, ml.v, m, n)) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			actions++;
			continue;
		}

		/* Handle projection */
		if (match > 0
			&&
			((getModuleId(p) == algebraRef && getFunctionId(p) == projectionRef)
			 || ((getModuleId(p) == dictRef || getModuleId(p) == forRef)
				 && getFunctionId(p) == decompressRef))
			&& (m = is_a_mat(getArg(p, 1), &ml)) >= 0) {
			n = is_a_mat(getArg(p, 2), &ml);
			if (mat_projection(cntxt, mb, p, &ml, m, n)) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			actions++;
			continue;
		}
		/* Handle setops */
		if (match > 0 && getModuleId(p) == algebraRef
			&& (getFunctionId(p) == differenceRef
				|| getFunctionId(p) == intersectRef)
			&& (m = is_a_mat(getArg(p, 1), &ml)) >= 0) {
			n = is_a_mat(getArg(p, 2), &ml);
			o = is_a_mat(getArg(p, 3), &ml);
			if (mat_setop(cntxt, mb, p, &ml, m, n, o)) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			actions++;
			continue;
		}

		if (match == p->retc && p->argc == (p->retc * 2)
			&& getFunctionId(p) == NULL) {
			if ((r = mat_assign(cntxt, mb, p, &ml)) == NULL) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			actions++;
			continue;
		}

		m = n = o = e = -1;
		for (fm = p->argc - 1; fm >= p->retc; fm--)
			if ((m = is_a_mat(getArg(p, fm), &ml)) >= 0)
				break;

		for (fn = fm - 1; fn >= p->retc; fn--)
			if ((n = is_a_mat(getArg(p, fn), &ml)) >= 0)
				break;

		for (fo = fn - 1; fo >= p->retc; fo--)
			if ((o = is_a_mat(getArg(p, fo), &ml)) >= 0)
				break;

		for (fe = fo - 1; fe >= p->retc; fe--)
			if ((e = is_a_mat(getArg(p, fe), &ml)) >= 0)
				break;

		/* delta* operator */
		if (match == 3 && bats == 3 && isDelta(p)
			&& (m = is_a_mat(getArg(p, fm), &ml)) >= 0
			&& (n = is_a_mat(getArg(p, fn), &ml)) >= 0
			&& (o = is_a_mat(getArg(p, fo), &ml)) >= 0) {
			if ((r = mat_delta(cntxt, &ml, mb, p, ml.v, m, n, o, -1, fm, fn, fo, 0)) != NULL) {
				actions++;
			} else {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}

			continue;
		}
		if (match == 4 && bats == 4 && isDelta(p)
			&& (m = is_a_mat(getArg(p, fm), &ml)) >= 0
			&& (n = is_a_mat(getArg(p, fn), &ml)) >= 0
			&& (o = is_a_mat(getArg(p, fo), &ml)) >= 0
			&& (e = is_a_mat(getArg(p, fe), &ml)) >= 0) {
			if ((r = mat_delta(cntxt, &ml, mb, p, ml.v, m, n, o, e, fm, fn, fo, fe)) != NULL) {
				actions++;
			} else {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			continue;
		}

		/* select on insert, should use last tid only */
#if 0
		if (match == 1 && fm == 2 && isSelect(p) && p->retc == 1 && (m = is_a_mat(getArg(p, fm), &ml)) >= 0 && !ml.v[m].packed &&	/* not packed yet */
			(getArg(p, fm - 1) > maxvars
			 || getModuleId(old[vars[getArg(p, fm - 1)]]) == sqlRef)) {
			if ((r = copyInstruction(mb, p)) == NULL) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			getArg(r, fm) = getArg(ml.v[m].mi, ml.v[m].mi->argc - 1);
			pushInstruction(mb, r);
			actions++;
			continue;
		}
#endif

		/* select on update, with nil bat */
		if (match == 1 && fm == 1 && isSelect(p) && p->retc == 1
			&& (m = is_a_mat(getArg(p, fm), &ml)) >= 0 && bats == 2
			&& isaBatType(getArgType(mb, p, 2))
			&& isVarConstant(mb, getArg(p, 2))
			&& is_bat_nil(getVarConstant(mb, getArg(p, 2)).val.bval)) {
			if (mat_apply1(cntxt, mb, p, &ml, m, fm)) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			actions++;
			continue;
		}

		/* handle dict select */
		if ((match == 1 || match == bats - 1) && p->retc == 1 && isSelect(p)
			&& getModuleId(p) == dictRef) {
			if (mat_apply(cntxt, mb, p, &ml, match)) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			actions++;
			continue;
		}
		/* handle dict renumber */
		if (match == 1 && match == bats - 1 && p->retc == 1
			&& getFunctionId(p) == renumberRef && getModuleId(p) == dictRef) {
			if (mat_apply(cntxt, mb, p, &ml, match)) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			actions++;
			continue;
		}

		if (match == bats && p->retc == 1
			&& (isMap2Op(p) || isMapOp(p) || isFragmentGroup(p)
				|| isFragmentGroup2(p))) {
			if (mat_apply(cntxt, mb, p, &ml, match)) {
				msg = createException(MAL, "optimizer.mergetable",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			actions++;
			continue;
		}

		/*
		 * All other instructions should be checked for remaining MAT dependencies.
		 * It requires MAT materialization.
		 */

		for (k = p->retc; msg == MAL_SUCCEED && k < p->argc; k++) {
			if ((m = is_a_mat(getArg(p, k), &ml)) >= 0) {
				msg = mat_pack(mb, &ml, m);
				if (msg)
					break;
			}
		}
		if (msg)
			break;

		cp = copyInstruction(mb, p);
		if (!cp) {
			msg = createException(MAL, "optimizer.mergetable",
								  SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto cleanup;
		}
		pushInstruction(mb, cp);
	}
	(void) stk;

	for (; i < oldtop; i++) {	/* add optimizer pipeline back again */
		pushInstruction(mb, old[i]);
	}

	if (mb->errors == MAL_SUCCEED) {
		for (i = 0; i < slimit; i++) {
			if (old[i] && old[i]->token == ENDsymbol)	/* don't free optimizer calls */
				break;
			freeInstruction(mb, old[i]);
		}
		//GDKfree(old);
	}
	for (i = 0; i < ml.top; i++) {
		if (ml.v[i].mi && !ml.v[i].pushed)
			freeInstruction(mb, ml.v[i].mi);
	}
  cleanup:
	//if (vars)
	//	GDKfree(vars);
	//if (ml.v)
	//	GDKfree(ml.v);
	//if (ml.horigin)
	//	GDKfree(ml.horigin);
	//if (ml.torigin)
	//	GDKfree(ml.torigin);
	//if (ml.vars)
	//	GDKfree(ml.vars);
	if (mb->errors) {
		freeException(msg);
		msg = mb->errors;
		mb->errors = NULL;
	}
	/* Defense line against incorrect plans */
	if (actions > 0 && msg == MAL_SUCCEED) {
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
  cleanup2:
	/* keep actions taken as a fake argument */
	if (msg == MAL_SUCCEED) {
		(void) pushInt(mb, pci, actions);
		msg = mb->errors;		/* may well be NULL */
		mb->errors = NULL;
	}

#ifndef NDEBUG
	if (bailout)
		TRC_INFO(MAL_OPTIMIZER, "Merge table bailout\n");
#endif
	ma_close(cntxt->ta);
	return msg;
}
