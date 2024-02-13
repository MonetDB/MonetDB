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
#include "sql_gencode.h"
#include "sql_statement.h"
#include "sql_pp_statement.h"
#include "rel_exp.h"

#include "mal_builder.h"
#include "opt_prelude.h"

/* Generate incremental or partitioned aggr statements */
stmt *
stmt_pp_aggr(backend *be, stmt *op1, stmt *grp, stmt *ext, sql_subfunc *op, int reduce, int no_nil, int nil_if_empty)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *mod, *aggrfunc;
	sql_subtype *res = op->res->h->data;
	int restype = res->type->localtype;
	bool complex_aggr = false;
	int *stmt_nr = NULL;
	int avg = 0;

	if (op1->nr < 0)
		return NULL;
	if (backend_create_subfunc(be, op, NULL) < 0)
		return NULL;
	mod = sql_func_mod(op->func);
	aggrfunc = backend_function_imp(be, op->func);

	if (LANG_INT_OR_MAL(op->func->lang)) {
		if (strcmp(aggrfunc, "avg") == 0)
			avg = 1;

		/* For the single value aggregates, we use the incremental
 		 * aggr. functions from the module 'iaggr' */
		if (!grp && (strcmp(aggrfunc, "count") == 0 ||
			     strcmp(aggrfunc, "min") == 0 ||
			     strcmp(aggrfunc, "max") == 0)) /* incremental versions TODO do for other aggr functions */
			mod = putName("iaggr");
		else if (!grp && avg && restype == TYPE_dbl)
			mod = putName("batcalc");

		if (avg || strcmp(aggrfunc, "sum") == 0 || strcmp(aggrfunc, "prod") == 0
			|| strcmp(aggrfunc, "str_group_concat") == 0)
			complex_aggr = true;
	}

	int argc = 1
		+ 2 * avg
		+ (LANG_EXT(op->func->lang) != 0)
		+ 2 * (op->func->lang == FUNC_LANG_C || op->func->lang == FUNC_LANG_CPP)
		+ (op->func->lang == FUNC_LANG_PY || op->func->lang == FUNC_LANG_R)
		+ (op1->type != st_list ? 1 : list_length(op1->op4.lval))
		+ (grp ? 4 : avg + 1);

	if (grp) {
		if ((grp && grp->nr < 0) || (ext && ext->nr < 0))
			return NULL;

		q = newStmtArgs(mb, mod, aggrfunc, argc);
		if (q == NULL)
			return NULL;
		setVarType(mb, getArg(q, 0), newBatType(restype));
		if (avg) { /* for avg also return rest and count */
			/* TODO: check with the 'new-avg' branch (?). We'll
 			 * want to choose between avg/rest and avg+cnt */
			if (restype != TYPE_dbl)
				q = pushReturn(mb, q, newTmpVariable(mb, newBatType(TYPE_lng)));
			q = pushReturn(mb, q, newTmpVariable(mb, newBatType(TYPE_lng)));
		}
	} else {
		int nrargs = (op1->type != st_list ? 1 : list_length(op1->op4.lval));
		q = newStmtArgs(mb, mod, aggrfunc, argc);
		if (q == NULL)
			return NULL;
		if (complex_aggr) {
			setVarType(mb, getArg(q, 0), (grp|| nrargs>1)?newBatType(restype):restype);
			if (avg) { /* for avg also return rest and count */
				/* TODO: check with the 'new-avg' branch (?). We'll
				 * want to choose between avg/rest and avg+cnt */
				if (restype != TYPE_dbl)
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_lng));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_lng));
			}
		}
	}

	if (LANG_EXT(op->func->lang))
		q = pushPtr(mb, q, op->func);
	if (op->func->lang == FUNC_LANG_R ||
		op->func->lang >= FUNC_LANG_PY ||
		op->func->lang == FUNC_LANG_C ||
		op->func->lang == FUNC_LANG_CPP) {
		if (!grp) {
			setVarType(mb, getArg(q, 0), restype);
		}
		if (op->func->lang == FUNC_LANG_C) {
			q = pushBit(mb, q, 0);
		} else if (op->func->lang == FUNC_LANG_CPP) {
			q = pushBit(mb, q, 1);
		}
 		q = pushStr(mb, q, op->func->query);
	}

	if (grp && grp != op1)
		q = pushArgument(mb, q, grp->nr);

	if (op1->type != st_list) {
		q = pushArgument(mb, q, op1->nr);
	} else {
		int i;
		node *n;

		for (i=0, n = op1->op4.lval->h; n; n = n->next, i++) {
			stmt *op = n->data;

			if (stmt_nr)
				q = pushArgument(mb, q, stmt_nr[i]);
			else
				q = pushArgument(mb, q, op->nr);
		}
	}
	if (grp) {
		if (LANG_INT_OR_MAL(op->func->lang) && grp != op1 && strncmp(aggrfunc, "count", 5) == 0)
			q = pushBit(mb, q, no_nil);
	} else if (LANG_INT_OR_MAL(op->func->lang) && no_nil && strncmp(aggrfunc, "count", 5) == 0) {
		q = pushBit(mb, q, no_nil);
	} else if (LANG_INT_OR_MAL(op->func->lang) && !nil_if_empty && strncmp(aggrfunc, "sum", 3) == 0) {
		q = pushBit(mb, q, FALSE);
	} else if (LANG_INT_OR_MAL(op->func->lang) && avg && (restype != TYPE_dbl)) { /* push candidates */
		q = pushNil(mb, q, TYPE_bat);
		q = pushBit(mb, q, no_nil);
	}
	q->inout = 0;
	pushInstruction(mb, q);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_aggr);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = op1;
		if (grp) {
			s->op2 = grp;
			s->op3 = ext;
			s->nrcols = 1;
		} else {
			if (!reduce)
				s->nrcols = 1;
		}
		s->key = reduce;
		s->aggr = reduce;
		s->flag = no_nil;
		s->op4.funcval = op;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_group_locked(backend *be, stmt *s, stmt *grp, stmt *ext, stmt *cnt, stmt *pp)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (s->nr < 0)
		return NULL;
	if (grp && (grp->nr < 0 || ext->nr < 0 || (cnt && cnt->nr < 0)))
		return NULL;

	q = newStmt(mb, groupRef, groupRef);
	if(!q)
		return NULL;

	/* output variables extent */
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, getArg(pp->q, 2));
	if (grp)
		q = pushArgument(mb, q, grp->nr);
	q = pushArgument(mb, q, s->nr);
	pushInstruction(mb, q);
	if (q) {
		stmt *ns = stmt_create(be->mvc->sa, st_group);
		if (ns == NULL) {
			freeInstruction(q);
			return NULL;
		}

		ns->op1 = s;

		if (grp) {
			ns->op2 = grp;
			ns->op3 = ext;
			ns->op4.stval = cnt;
		}
		ns->nrcols = s->nrcols;
		ns->key = 0;
		ns->q = q;
		ns->nr = getDestVar(q);
		return ns;
	}
	return NULL;
}

stmt *
stmt_group_partitioned(backend *be, stmt *s, stmt *grp, stmt *ext, stmt *cnt)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	int tt = tail_type(s)->type->localtype;

	if (s->nr < 0)
		return NULL;
	if (grp && (grp->nr < 0 || ext->nr < 0 || (cnt && cnt->nr < 0)))
		return NULL;

	q = newStmt(mb, groupRef, groupRef);
	if(!q)
		return NULL;

	/* output variables extent and hist */
	q = pushReturn(mb, q, newTmpVariable(mb, newBatType(tt)));
	q = pushArgument(mb, q, be->pipeline);
	if (grp) {
		q = pushArgument(mb, q, grp->nr);
		q = pushArgument(mb, q, ext->nr); /* needed for parent hash pointer */
	}
	q = pushArgument(mb, q, s->nr);

	pushInstruction(mb, q);
	if (q) {
		stmt *ns = stmt_create(be->mvc->sa, st_group);
		if (ns == NULL) {
			freeInstruction(q);
			return NULL;
		}

		ns->op1 = s;

		if (grp) {
			ns->op2 = grp;
			ns->op3 = ext;
			ns->op4.stval = cnt;
		}
		ns->nrcols = s->nrcols;
		ns->key = 0;
		ns->q = q;
		ns->nr = getDestVar(q);
		return ns;
	}
	return NULL;
}

stmt *
stmt_limit_partitioned(backend *be, stmt *col, stmt *piv, stmt *gid, stmt *offset, stmt *limit)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	int l, c, len;

	if (col->nr < 0 || offset->nr < 0 || limit->nr < 0)
		return NULL;
	if (piv && (piv->nr < 0 || gid->nr < 0))
		return NULL;

	c = (col) ? col->nr : 0;

	/* first insert single value into a bat */
	if (col->nrcols == 0) {
		int k, tt = tail_type(col)->type->localtype;

		q = newStmt(mb, batRef, newRef);
		if (q == NULL)
			return NULL;
		setVarType(mb, getArg(q, 0), newBatType(tt));
		q = pushType(mb, q, tt);
		if (q == NULL)
			return NULL;
		k = getDestVar(q);
		pushInstruction(mb, q);

		q = newStmt(mb, batRef, appendRef);
		q = pushArgument(mb, q, k);
		q = pushArgument(mb, q, c);
		if (q == NULL)
			return NULL;
		pushInstruction(mb, q);
		c = k;
	}

	q = newStmt(mb, calcRef, plusRef);
	q = pushArgument(mb, q, offset->nr);
	q = pushArgument(mb, q, limit->nr);
	if (q == NULL)
		return NULL;
	len = getDestVar(q);
	pushInstruction(mb, q);

	/* since both arguments of algebra.subslice are
	   inclusive correct the LIMIT value by
	   subtracting 1 */
	q = newStmt(mb, calcRef, minusRef);
	q = pushArgument(mb, q, len);
	q = pushInt(mb, q, 1);
	if (q == NULL)
		return NULL;
	len = getDestVar(q);
	pushInstruction(mb, q);

	q = newStmtArgs(mb, algebraRef, subsliceRef, 8);
	/* returns gid, rid, hid */
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any)); /* rid */
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any)); /* hid for topn/heap sink */
	q->inout = 2;
	q = pushArgument(mb, q, c);
	q = pushArgument(mb, q, offset->nr);
	q = pushArgument(mb, q, len);
	q = pushArgument(mb, q, be->pipeline);
	if (q == NULL)
		return NULL;
	l = getDestVar(q);
	pushInstruction(mb, q);

	/* retrieve the single values again */
	if (col->nrcols == 0) {
		q = newStmt(mb, algebraRef, findRef);
		q = pushArgument(mb, q, l);
		q = pushOid(mb, q, 0);
		if (q == NULL)
			return NULL;
		l = getDestVar(q);
		pushInstruction(mb, q);
	}

	stmt *ns = stmt_create(be->mvc->sa, piv?st_limit2:st_limit);
	if (ns == NULL) {
		freeInstruction(q);
		return NULL;
	}

	ns->op1 = col;
	ns->op2 = offset;
	ns->op3 = limit;
	ns->nrcols = col->nrcols;
	ns->key = col->key;
	ns->aggr = col->aggr;
	ns->q = q;
	ns->nr = l;
	return ns;
}

/* output: shared bat var id, must always be a positive number */
stmt *
stmt_unique_sharedout(backend *be, stmt *s, int output)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (s->nr < 0)
		return NULL;

	q = newStmt(mb, algebraRef, uniqueRef);
	if(!q)
		return NULL;

	assert(output > 0);
	q = pushReturn(mb, q, output);
	q->inout = 1;
	q = pushArgument(mb, q, be->pipeline);
	q = pushArgument(mb, q, s->nr);
	q = pushNil(mb, q, TYPE_bat); /* candidate list */
	pushInstruction(mb, q);
	if (q) {
		stmt *ns = stmt_create(be->mvc->sa, st_unique);
		if (ns == NULL) {
			freeInstruction(q);
			return NULL;
		}

		ns->op1 = s;
		ns->nrcols = s->nrcols;
		ns->key = 1;
		ns->q = q;
		ns->nr = getDestVar(q);
		return ns;
	}
	return NULL;
}

InstrPtr
stmt_hash_new(backend *be, int tt, lng estimate, int parent)
{
	InstrPtr q = newStmt(be->mb, putName("hash"), newRef);

	if (q == NULL)
		return NULL;
	setVarType(be->mb, getArg(q, 0), newBatType(tt));
	q = pushType(be->mb, q, tt);
	assert (estimate >= 0);
	q = pushInt(be->mb, q, (int)estimate);
	if (parent)
		q = pushArgument(be->mb, q, parent);
	pushInstruction(be->mb, q);
	return q;
}

InstrPtr
stmt_hash_new_payload(backend *be, int tt, lng nr_slots, lng pld_size, int parent)
{
	InstrPtr q = newStmt(be->mb, putName("hash"), new_payloadRef);
	if (q == NULL) return NULL;

	setVarType(be->mb, getArg(q, 0), newBatType(tt));
	q = pushType(be->mb, q, tt);
	q = pushOid(be->mb, q, (oid)nr_slots);
	q = pushOid(be->mb, q, (oid)pld_size);
	q = pushArgument(be->mb, q, parent);
	pushInstruction(be->mb, q);
	return q;
}

InstrPtr
stmt_hash_build_table(backend *be, int tt, lng nr_slots, lng pld_size, int parent)
{
	InstrPtr q = newStmt(be->mb, putName("hash"), new_payloadRef);
	if (q == NULL) return NULL;

	setVarType(be->mb, getArg(q, 0), newBatType(tt));
	q = pushType(be->mb, q, tt);
	q = pushOid(be->mb, q, (oid)nr_slots);
	q = pushOid(be->mb, q, (oid)pld_size);
	q = pushArgument(be->mb, q, parent);
	pushInstruction(be->mb, q);
	return q;
}

InstrPtr
stmt_part_new(backend *be, int nr_parts)
{
	InstrPtr q = newStmt(be->mb, putName("part"), newRef);

	if (q == NULL)
		//return -1;
		return NULL;
	setVarType(be->mb, getArg(q, 0), newBatType(TYPE_oid));
	q = pushInt(be->mb, q, nr_parts);
	return q;
}

InstrPtr
stmt_mat_new(backend *be, int tt, int nr_parts)
{
	InstrPtr q = newStmt(be->mb, putName("mat"), newRef);

	if (q == NULL)
		//return -1;
		return NULL;
	setVarType(be->mb, getArg(q, 0), newBatType(tt));
	q = pushType(be->mb, q, tt);
	q = pushInt(be->mb, q, nr_parts);
	return q;
}

stmt *
stmt_heapn_projection(backend *be, int sel, int del, int ins, stmt *c, stmt *all)
{

	InstrPtr q = newStmt(be->mb, getName("heapn"), getName("projection"));
	sql_subtype *t = tail_type(c);
	int tt = newBatType(t->type->localtype);
	getArg(q, 0) = newTmpVariable(be->mb, tt);

	q->inout = 0;
	q = pushArgument(be->mb, q, sel);
	q = pushArgument(be->mb, q, del);
	q = pushArgument(be->mb, q, ins);
	q = pushArgument(be->mb, q, c->nr);
	q = pushArgument(be->mb, q, all->nr);
	q = pushArgument(be->mb, q, be->pipeline);
	pushInstruction(be->mb, q);

	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_join);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->op1 = c;
		s->op2 = c;
		s->flag = cmp_project;
		s->key = 0;
		s->nrcols = c->nrcols;
		s->nr = getDestVar(q);
		s->q = q;
		s->tname = c->tname;
		s->cname = c->cname;
		return s;
	}
	return NULL;
}

/* Generate the stmt to return the n-th (i.e. X_&&) slice of the input BAT
 * (i.e. X_19):
 *   (X_80:bat[:str], !X_19:bat[:str]) := slicer.nth_slice(X_77:int);
 */
stmt *
stmt_nth_slice(backend *be, stmt *col, int slicer)
{
	sql_subtype *tp = tail_type(col);
	int tt = tp->type->localtype;
	if (slicer != 1)
		return col;

	InstrPtr q = NULL;
	q = newStmt(be->mb, slicerRef, nth_sliceRef);
	setVarType(be->mb, getArg(q, 0), newBatType(tt));
	//q = pushArgument(be->mb, q, col->nr);
	q = pushReturn(be->mb, q, col->nr);
	q->inout = 1;
	q = pushArgument(be->mb, q, be->pp);

	pushInstruction(be->mb, q);
	stmt *ns = stmt_create(be->mvc->sa, st_temp);
	if (ns == NULL) {
		freeInstruction(q);
		return NULL;
	}

	ns->op1 = col;
	ns->nrcols = col->nrcols;
	ns->key = col->key;
	ns->aggr = col->aggr;
	ns->q = q;
	ns->nr = getArg(q, 0);
	ns->op4.typeval = *tp;
	return ns;
}

/* Generate the stmt to compute the number of slices to be created dynamically:
 *   X_42:int := slicer.no_slices(X_24:bat[:...]);
 */
stmt *
stmt_no_slices(backend *be, stmt *col)
{
	InstrPtr q = newStmt(be->mb, slicerRef, no_slicesRef);
	q = pushArgument(be->mb, q, col->nr);

	pushInstruction(be->mb, q);
	stmt *ns = stmt_create(be->mvc->sa, st_result); /* ?? */
	if (ns == NULL) {
		freeInstruction(q);
		return NULL;
	}

	ns->op1 = col;
	ns->nrcols = 1;
	ns->key = 1;
	ns->aggr = 1;
	ns->q = q;
	ns->nr = getArg(q, 0);
	ns->op4.typeval = *sql_bind_localtype("int");
	return ns;
}

stmt *
stmt_slice(backend *be, stmt *col, stmt *limit)
{
	/* FIXME: this pp function doesn't seem to be used, also it seems to
         * use a none-pp algebra.slice function
	 */
	sql_subtype *tp = tail_type(col);
	int tt = tp->type->localtype;

	InstrPtr q = NULL;
	q = newStmt(be->mb, algebraRef, sliceRef);
	setVarType(be->mb, getArg(q, 0), newBatType(tt));
	q = pushArgument(be->mb, q, col->nr);
	q = pushLng(be->mb, q, 0);
	q = pushArgument(be->mb, q, limit->nr);
	pushInstruction(be->mb, q);

	stmt *ns = stmt_create(be->mvc->sa, st_temp);
	if (ns == NULL) {
		freeInstruction(q);
		return NULL;
	}

	ns->op1 = col;
	ns->op2 = limit;
	ns->nrcols = col->nrcols;
	ns->key = col->key;
	ns->aggr = col->aggr;
	ns->q = q;
	ns->nr = getArg(q, 0);
	ns->op4.typeval = *tp;
	return ns;
}

/* Generate a
 *   language.pipelines(42:int); # nrparts, or
 *   language.pipelines(X_42:int); # dynamic,
 *     # where X42 might have been dynamically generated by, e.g.,
 *     # slicer.no_slices(X_24:bat[:...]);
 */
static stmt *
stmt_pp_create_nrparts_or_dynamic(backend *be, int nrparts, int input)
{
	InstrPtr q = newStmtArgs(be->mb, languageRef, "pipelines", 4);
	if (q == NULL)
		return NULL;
	q->barrier = BARRIERsymbol;
	q = pushReturn(be->mb, q, newTmpVariable(be->mb, TYPE_int));
	q = pushReturn(be->mb, q, newTmpVariable(be->mb, TYPE_ptr));
	if (input >= 0 && nrparts == 0)
		q = pushArgument(be->mb, q, input);
	else
		q = pushInt(be->mb, q, nrparts);

	pushInstruction(be->mb, q);
	be->nrparts = nrparts;
	be->pp = getArg(q, 1); /* counter */
	be->pipeline = getArg(q, 2); /* pipeline */
	int label = getArg(q, 0);

	InstrPtr r = newStmtArgs(be->mb, calcRef, ">=", 3);
	r->barrier = LEAVEsymbol;
	getArg(r, 0) = label;
	r = pushArgument(be->mb, r, be->pp);
	if (input >= 0 && nrparts == 0)
		r = pushArgument(be->mb, r, input);
	else
		r = pushInt(be->mb, r, nrparts);
	pushInstruction(be->mb, r);

	if (r && q) {
		stmt *s = stmt_create(be->mvc->sa, st_none);

		s->nr = label;
		s->q = q;
		return s;
	}
	return NULL;
}

/* The following functions generate the control stmt-s to start, repeat or end
 * a pipeline block.
 */


/* Generates:
 *   language.pipelines(<nrparts>:int);
 */
stmt *
stmt_pp_start_nrparts(backend *be, int nrparts)
{
	return stmt_pp_create_nrparts_or_dynamic(be, nrparts, -1);
}

/* Generates:
 *   language.pipelines(X_<input>:int); # dynamic,
 */
stmt *
stmt_pp_start_dynamic(backend *be, int input)
{
	return stmt_pp_create_nrparts_or_dynamic(be, 0, input);
}

int
stmt_pp_jump(backend *be, stmt *label, int nrparts)
{
	InstrPtr r = newStmtArgs(be->mb, putName("pipeline"), "counter", 2);
	if (r == NULL)
		return -1;
	getArg(r, 0) = getArg(label->q, 1); /* counter */
	r = pushArgument(be->mb, r, getArg(label->q, 2) /* pipeline */);
	pushInstruction(be->mb, r);

	r = newStmtArgs(be->mb, calcRef, "<", 3);
	if (r == NULL)
		return -1;
	r->barrier = REDOsymbol;
	getArg(r, 0) = label->nr;
	r = pushArgument(be->mb, r, getArg(label->q, 1)); /* current nr */
	r = pushArgument(be->mb, r, getArg(label->q, 3)); /* nrparts */
	(void)nrparts;
	//r = pushInt(be->mb, r, nrparts);
	pushInstruction(be->mb, r);
	if (r)
		return 0;
	return -1;
}

int
stmt_pp_end(backend *be, stmt *label)
{
	be->pp = be->nrparts = be->pipeline = 0;
	be->ppstmt = NULL;
	InstrPtr q = newAssignmentArgs(be->mb, 2);
	if (q == NULL)
		return -1;
	getArg(q, 0) = label->nr;
	q->argc = q->retc = 1;
	q->barrier = EXITsymbol;
	pushInstruction(be->mb, q);
	if (q)
		return 0;
	return -1;
}

