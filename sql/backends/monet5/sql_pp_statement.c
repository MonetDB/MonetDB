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
#include "sql_statement.h"
#include "sql_pp_statement.h"
#include "rel_exp.h"

#include "mal_builder.h"
#include "opt_prelude.h"


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

InstrPtr
stmt_hash_new(backend *be, int tt, lng estimate, int parent)
{
	InstrPtr q = newStmt(be->mb, putName("hash"), newRef);

	if (q == NULL)
		//return -1;
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

stmt *
stmt_slicer(backend *be, stmt *col, int slicer)
{
	sql_subtype *tp = tail_type(col);
	int tt = tp->type->localtype;
	if (slicer != 1)
		return col;

	InstrPtr q = NULL;
	q = newStmt(be->mb, slicerRef, sliceRef);
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

stmt *
stmt_slices(backend *be, stmt *col)
{
	InstrPtr q = newStmt(be->mb, slicerRef, slicesRef);
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

static stmt *
pp_create_nrparts_or_dynamic(backend *be, int nrparts, int input)
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

stmt *
pp_create(backend *be, int nrparts)
{
	return pp_create_nrparts_or_dynamic(be, nrparts, -1);
}

stmt *
pp_dynamic(backend *be, int input)
{
	return pp_create_nrparts_or_dynamic(be, 0, input);
}

int
pp_jump(backend *be, stmt *label, int nrparts)
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
pp_end(backend *be, stmt *label)
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

