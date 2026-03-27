/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "sql_gencode.h"
#include "sql_statement.h"
#include "sql_pp_statement.h"
#include "rel_exp.h"

#include "mal_builder.h"

void
set_need_pipeline(backend *be)
{
	if(be->need_pipeline)
		assert(0);
	be->need_pipeline = true;
}

bool
get_need_pipeline(backend *be)
{
	/* get and reset */
	bool r = be->need_pipeline;
	be->need_pipeline = false;
	return r;
}

void
set_pipeline(backend *be, stmt *pp)
{
	be->ppstmt = pp;
}

stmt *
get_pipeline(backend *be)
{
	return be->ppstmt;
}

/* Generate incremental or partitioned aggr statements */
stmt *
stmt_pp_aggr(backend *be, stmt *op1, stmt *grp, stmt *ext, sql_subfunc *op, int reduce, int no_nil, int nil_if_empty)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *mod, *aggrfunc;
	sql_subtype *res = op->res->h->data;
	int restype = res->type->localtype, intype = 0;
	bool complex_aggr = false;
	int *stmt_nr = NULL;
	int avg = 0, sum = 0;

	if (op1->nr < 0)
		return NULL;
	if (backend_create_subfunc(be, op, NULL) < 0)
		return NULL;
	mod = sql_func_mod(op->func);
	aggrfunc = backend_function_imp(be, op->func);

	if (LANG_INT_OR_MAL(op->func->lang)) {
		if (strcmp(aggrfunc, "avg") == 0)
			avg = 1;
		if (strcmp(aggrfunc, "sum") == 0)
			sum = 1;


		if (op1->type == st_list) {
			stmt *s = op1->op4.lval->h->data;
			intype = tail_type(s)->type->localtype;
		} else {
			intype = tail_type(op1)->type->localtype;
		}
		if (avg && restype == TYPE_dbl && intype != TYPE_dbl && intype != TYPE_flt)
			restype = intype;
		if (sum && restype != TYPE_dbl && restype != TYPE_flt)
			sum = 0;


		if (op1->type == st_list) {
			stmt *s = op1->op4.lval->h->data;
			intype = tail_type(s)->type->localtype;
		} else {
			intype = tail_type(op1)->type->localtype;
		}
		if (avg && restype == TYPE_dbl && intype != TYPE_dbl && intype != TYPE_flt)
			restype = intype;

		/* For the single value aggregates, we use the incremental
		 * aggr. functions from the module 'iaggr' */
		if (!grp && (strcmp(aggrfunc, "count") == 0 ||
			     strcmp(aggrfunc, "min") == 0 ||
			     strcmp(aggrfunc, "max") == 0 ||
				 strcmp(aggrfunc, "null") == 0 )) /* incremental versions TODO do for other aggr functions */
			mod = putName("iaggr");
		//else if (!grp && avg && restype == TYPE_dbl)
			//mod = putName("batcalc");

		if (avg || strcmp(aggrfunc, "sum") == 0 || strcmp(aggrfunc, "prod") == 0
			|| strcmp(aggrfunc, "str_group_concat") == 0)
			complex_aggr = true;
	}

	int argc = 1
		+ 2 * avg
		+ 2 * sum
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
		if (avg || sum) { /* for avg also return remainder (or error) and count */
			q = pushReturn(mb, q, newTmpVariable(mb, newBatType((restype == TYPE_dbl || restype == TYPE_flt)? restype : TYPE_lng)));
			q = pushReturn(mb, q, newTmpVariable(mb, newBatType(TYPE_lng)));
		}
	} else {
		int nrargs = (op1->type != st_list ? 1 : list_length(op1->op4.lval));
		q = newStmtArgs(mb, mod, aggrfunc, argc);
		if (q == NULL)
			return NULL;
		if (complex_aggr) {
			setVarType(mb, getArg(q, 0), (grp|| nrargs>1)?newBatType(restype):restype);
			if (avg || sum) { /* for avg also return remainder (or error) and count */
				q = pushReturn(mb, q, newTmpVariable(mb, (restype == TYPE_dbl || restype == TYPE_flt) ? restype : TYPE_lng));
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
		q = pushNilBat(mb, q);
		q = pushBit(mb, q, no_nil);
	}
	q->inout = 0;
	pushInstruction(mb, q);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_aggr);
		if(!s) {
			freeInstruction(mb, q);
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
			freeInstruction(mb, q);
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
			freeInstruction(mb, q);
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
		freeInstruction(mb, q);
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
	q = pushNilBat(mb, q); /* candidate list */
	pushInstruction(mb, q);
	if (q) {
		stmt *ns = stmt_create(be->mvc->sa, st_unique);
		if (ns == NULL) {
			freeInstruction(mb, q);
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

stmt *
stmt_oahash_new(backend *be, sql_subtype *tpe, lng estimate, int parent, int nrparts)
{
	InstrPtr q = newStmt(be->mb, nrparts?putName("mat"):putName("oahash"), newRef);
	if (q == NULL)
		return NULL;

	assert (estimate >= 0);

	int tt = tpe->type->localtype;
	setVarType(be->mb, getArg(q, 0), newBatType(tt)); /* ht_sink */
	q = pushType(be->mb, q, tt);
	if (nrparts)
		q = pushArgument(be->mb, q, nrparts);
	q = pushLng(be->mb, q, estimate);
	if (parent)
		q = pushArgument(be->mb, q, parent);
	pushInstruction(be->mb, q);

	stmt *s = stmt_none(be);
	s->op4.typeval = *tpe;
	s->q = q;
	s->nr = getArg(q, 0);
	s->nrcols = 1;
	return s;
}

stmt *
stmt_oahash_hshmrk_init(backend *be, stmt *stmts_ht, bool moveup)
{
	InstrPtr q = newStmt(be->mb, putName("oahash"), "hashmark_init");
	if (q == NULL)
		return NULL;

	assert(stmts_ht->op2 || stmts_ht->op4.lval->t->data);
	setVarType(be->mb, getArg(q, 0), newBatType(TYPE_bit));
	/* hp_gid or the last hash-column */
	stmt *ht = stmts_ht->op4.lval->t->data;
	stmt *hp = stmts_ht->op2?stmts_ht->op2:NULL;
	if (!ht->nrcols) {
		ht = const_column(be, ht);
		if (moveup) {
			moveInstruction(be->mb, be->mb->stop-1, be->pp_pc++);
		}
	}
	q = pushArgument(be->mb, q, ht->nr);
	if (hp)
		q = pushArgument(be->mb, q, hp->nr);
	else
		q = pushNilBat(be->mb, q);
	pushInstruction(be->mb, q);
	if (moveup) {
		moveInstruction(be->mb, be->mb->stop-1, be->pp_pc++);
	}

	stmt *s = stmt_none(be);
	s->op4.typeval = *sql_fetch_localtype(TYPE_bit);
	s->q = q;
	s->nr = getArg(q, 0);
	s->nrcols = 1;
	return s;
}

stmt *
stmt_oahash_build_ht(backend *be, stmt *ht, stmt *key, stmt *prnt, const stmt *pp)
{
	InstrPtr q = newStmt(be->mb, putName("oahash"), prnt?putName("build_combined_table"):putName("build_table"));
	if (q == NULL) return NULL;

	setVarType(be->mb, getArg(q, 0), newBatType(TYPE_oid)); /* slot_id */
	q = pushReturn(be->mb, q, ht->nr);
	q = pushArgument(be->mb, q, key->nr);
	if (prnt)
		q = pushArgument(be->mb, q, prnt->nr);
	q = pushArgument(be->mb, q, getArg(pp->q, 2) /* pipeline ptr*/);
	q->inout = 1;
	pushInstruction(be->mb, q);

	stmt *s = stmt_none(be);
	if (s == NULL) return NULL;
	s->op4.typeval = *tail_type(ht);
	s->nr = getArg(q, 0);
	s->nrcols = key->nrcols;
	s->q = q;

	return s;
}

stmt *
stmt_oahash_frequency(backend *be, stmt *freq, stmt *prnt, bool occ_cnt, const stmt *pp)
{
	InstrPtr q = newStmt(be->mb, putName("oahash"), putName("frequency"));
	if (q == NULL)
		return NULL;

	if (occ_cnt) {
		setVarType(be->mb, getArg(q, 0), newBatType(TYPE_oid));
		q = pushReturn(be->mb, q, freq->nr);
		q->inout = 1;
	} else {
		getArg(q, 0) = freq->nr;
		q->inout = 0;
	}
	q = pushArgument(be->mb, q, prnt->nr);
	q = pushArgument(be->mb, q, getArg(pp->q, 2) /* pipeline ptr*/);
	pushInstruction(be->mb, q);

	stmt *s = stmt_none(be);
	if (s == NULL) return NULL;
	s->op4.typeval = *tail_type(freq);
	s->nr = getArg(q, 0);
	s->nrcols = 1;
	s->q = q;

	return s;
}

stmt *
stmt_oahash_hash(backend *be, stmt *key, stmt *prev, stmt *ht)
{
	InstrPtr q = newStmt(be->mb, putName("oahash"), prev?putName("combined_hash"):putName("hash"));
	if (q == NULL)
		return NULL;
	setVarType(be->mb, getArg(q, 0), newBatType(TYPE_lng)); /* hsh */
	q = pushArgument(be->mb, q, key->nr);
	if (prev) {
		q = pushArgument(be->mb, q, getArg(prev->q, 0));
		q = pushArgument(be->mb, q, getArg(prev->q, 1));
		q = pushArgument(be->mb, q, ht->nr);
	}
	pushInstruction(be->mb, q);

	stmt *s = stmt_none(be);
	if (s == NULL) return NULL;
	s->op4.typeval = *sql_fetch_localtype(TYPE_lng);
	s->nr = getArg(q, 0);
	s->nrcols = 1;
	s->q = q;

	return s;
}

stmt *
stmt_oahash_probe(backend *be, stmt *key, stmt *prev, stmt *rhs_ht, stmt *freq, stmt *outer, bool single, bool semantics, bool eq, bool outerjoin, bool groupedjoin, const stmt *pp)
{
	InstrPtr q = newStmt(be->mb, putName("oahash"), prev == NULL?
									groupedjoin?putName("mprobe")         :outerjoin?putName("oprobe")         :eq?putName("probe"):putName("nprobe"):
									groupedjoin?putName("combined_mprobe"):outerjoin?putName("combined_oprobe"):putName("combined_probe"));
	if (q == NULL)
		return NULL;
	setVarType(be->mb, getArg(q, 0), newBatType(TYPE_oid));
	q = pushReturn(be->mb, q, newTmpVariable(be->mb, newBatType(TYPE_oid)));
	if (outerjoin || groupedjoin)
		q = pushReturn(be->mb, q, outer?outer->nr:newTmpVariable(be->mb, newBatType(TYPE_bit)));
	q = pushArgument(be->mb, q, key->nr);
	if (prev) {
		q = pushArgument(be->mb, q, getArg(prev->q, 0));
		q = pushArgument(be->mb, q, getArg(prev->q, 1));
	}
	q = pushArgument(be->mb, q, rhs_ht->nr);
	if (single) {
		assert(freq);
		q = pushArgument(be->mb, q, freq->nr);
	}
	q = pushBit(be->mb, q, single);
	q = pushBit(be->mb, q, semantics);
	q = pushArgument(be->mb, q, getArg(pp->q, 2) /* pipeline ptr*/);
	pushInstruction(be->mb, q);

	stmt *s = stmt_none(be);
	if (s == NULL) return NULL;
	s->op4.typeval = *sql_fetch_localtype(TYPE_oid);
	s->nr = getArg(q, 0);
	s->nrcols = 1;
	s->q = q;
	return s;
}

stmt *
stmt_algebra_project(backend *be, stmt *inout, stmt *pos, stmt *val, const char *fname, const stmt *pp)
{
	InstrPtr q = newStmt(be->mb, getName("algebra"), fname);
	if (q == NULL) return NULL;
	getArg(q, 0) = inout->nr;
	q->inout = 0;
	q = pushArgument(be->mb, q, pos->nr);
	q = pushArgument(be->mb, q, val->nr);
	q = pushArgument(be->mb, q, getArg(pp->q, 2));
	pushInstruction(be->mb, q);

	stmt *s = stmt_none(be);
	if (s == NULL) return NULL;
	s->op4.typeval = *tail_type(val);
	s->nr = getArg(q, 0);
	s->nrcols = 1;
	s->q = q;
	return s;
}

stmt *
stmt_oahash_expand(backend *be, const stmt *prb_res, const stmt *freq, bit outer)
{
	if (!freq)
		return (stmt*)prb_res; /* should be just first result ! */
	InstrPtr q = newStmtArgs(be->mb, putName("oahash"), putName("expand"), 5);
	if (q == NULL)
		return NULL;
	setVarType(be->mb, getArg(q, 0), newBatType(TYPE_oid)); /* expanded */
	q = pushArgument(be->mb, q, getArg(prb_res->q, 0));
	q = pushArgument(be->mb, q, getArg(prb_res->q, 1));
	q = pushArgument(be->mb, q, freq->nr);
	q = pushBit(be->mb, q, outer);
	pushInstruction(be->mb, q);

	stmt *s = stmt_none(be);
	if (s == NULL) return NULL;
	s->op4.typeval = *sql_fetch_localtype(TYPE_oid);
	s->nr = getArg(q, 0);
	s->nrcols = prb_res->nrcols;
	s->q = q;
	return s;
}

stmt *
stmt_oahash_explode(backend *be, const stmt *prb_res, const stmt *freq, const stmt *ht_sink, bit outer)
{
	sql_subtype *st = sql_fetch_localtype(TYPE_oid);
	InstrPtr q = newStmtArgs(be->mb, putName("oahash"), putName("explode"), 5);
	if (q == NULL)
		return NULL;
	setVarType(be->mb, getArg(q, 0), newBatType(st->type->localtype));
	q = pushArgument(be->mb, q, getArg(prb_res->q, 1));
	q = pushArgument(be->mb, q, freq->nr);
	q = pushArgument(be->mb, q, ht_sink->nr);
	q = pushBit(be->mb, q, outer);
	pushInstruction(be->mb, q);

	stmt *s = stmt_none(be);
	if (s == NULL) return NULL;
	s->op4.typeval = *st;
	s->nr = getArg(q, 0);
	s->nrcols = 1;
	s->q = q;
	return s;
}

stmt *
stmt_oahash_explode_unmatched(backend *be, const stmt *ht, const stmt *mrk, const stmt *freq)
{
	InstrPtr q = newStmt(be->mb, putName("oahash"), putName("explode_unmatched"));
	if (q == NULL)
		return NULL;
	setVarType(be->mb, getArg(q, 0), newBatType(TYPE_oid)); /* expanded */
	q = pushArgument(be->mb, q, ht->nr);
	q = pushArgument(be->mb, q, mrk->nr);
	q = pushArgument(be->mb, q, freq->nr);
	pushInstruction(be->mb, q);

	stmt *s = stmt_none(be);
	if (s == NULL) return NULL;
	s->op4.typeval = *sql_fetch_localtype(TYPE_oid);
	s->nr = getArg(q, 0);
	s->nrcols = 1;
	s->q = q;
	return s;
}

stmt *
stmt_oahash_project_cart(backend *be, stmt *col, stmt *repeat, bool outer, bool expand)
{
	InstrPtr q = newStmt(be->mb, putName("oahash"), expand?putName("expand_cartesian"):putName("explode_cartesian"));
	if (q == NULL) return NULL;

	setVarType(be->mb, getArg(q, 0), newBatType(TYPE_oid));
	q = pushArgument(be->mb, q, col->nr);
	q = pushArgument(be->mb, q, repeat->nr);
	q = pushBit(be->mb, q, outer);
	pushInstruction(be->mb, q);

	stmt *s = stmt_none(be);
	if (s == NULL) return NULL;
	s->op4.typeval = *sql_fetch_localtype(TYPE_oid);
	s->nr = getArg(q, 0);
	s->nrcols = col->nrcols;
	s->q = q;
	return s;
}

InstrPtr
stmt_part_new(backend *be, int nr_parts)
{
	InstrPtr q = newStmt(be->mb, putName("part"), newRef);

	if (q == NULL)
		return NULL;
	setVarType(be->mb, getArg(q, 0), newBatType(TYPE_oid));
	q = pushInt(be->mb, q, nr_parts);
	pushInstruction(be->mb, q);
	return q;
}

stmt *
stmt_mat_new(backend *be, sql_subtype *tpe, int nr_parts)
{
	int tt = tpe->type->localtype;
	InstrPtr q = newStmt(be->mb, putName("mat"), newRef);

	if (q == NULL)
		return NULL;
	setVarType(be->mb, getArg(q, 0), newBatType(tt));
	q = pushType(be->mb, q, tt);
	q = pushInt(be->mb, q, nr_parts);
	pushInstruction(be->mb, q);

	stmt *s = stmt_create(be->mvc->sa, st_alias);
	s->op4.typeval = *tpe;
	s->q = q;
	s->nr = q->argv[0];
	s->nrcols = 2;
	return s;
}

InstrPtr
stmt_sop_new(backend *be, int nr_workers)
{
	InstrPtr q = newStmt(be->mb, putName("sop"), newRef);

	if (q == NULL)
		return NULL;
	setVarType(be->mb, getArg(q, 0), newBatType(TYPE_oid));
	q = pushInt(be->mb, q, nr_workers);
	pushInstruction(be->mb, q);
	return q;
}

/* Generate the stmt to return the n-th (i.e. X_&&) slice of the input BAT
 * (i.e. X_19):
 *   (X_80:bat[:str], !X_19:bat[:str]) := slicer.nth_slice(X_77:int);
 */
stmt *
stmt_nth_slice(backend *be, stmt *col, bool hash)
{
	sql_subtype *tp = hash?NULL:tail_type(col);
	int tt = hash?TYPE_oid:tp->type->localtype;

	InstrPtr q = NULL;
	q = newStmt(be->mb, hash?putName("oahash") : slicerRef, nth_sliceRef);
	if (hash)
		setVarType(be->mb, getArg(q, 0), newBatType(TYPE_oid));
	else
		setVarType(be->mb, getArg(q, 0), newBatType(tt));
	q = pushArgument(be->mb, q, col->nr);
	q = pushArgument(be->mb, q, be->pp);

	pushInstruction(be->mb, q);
	stmt *ns = stmt_create(be->mvc->sa, st_temp);
	if (ns == NULL) {
		freeInstruction(be->mb, q);
		return NULL;
	}

	ns->op1 = col;
	ns->nrcols = col->nrcols;
	ns->key = col->key;
	ns->aggr = col->aggr;
	ns->q = q;
	ns->nr = getArg(q, 0);
	if (tp)
		ns->op4.typeval = *tp;
	return ns;
}

/* Generate the stmt to compute the number of slices to be created dynamically:
 *   X_42:int := slicer.no_slices(X_24:bat[:...]);
 */
stmt *
stmt_no_slices(backend *be, stmt *col, bool hash)
{
	InstrPtr q = newStmt(be->mb, hash?putName("oahash") : slicerRef, no_slicesRef);
	q = pushArgument(be->mb, q, col->nr);

	pushInstruction(be->mb, q);
	stmt *ns = stmt_create(be->mvc->sa, st_result); /* ?? */
	if (ns == NULL) {
		freeInstruction(be->mb, q);
		return NULL;
	}

	ns->op1 = col;
	ns->nrcols = 1;
	ns->key = 1;
	ns->aggr = 1;
	ns->q = q;
	ns->nr = getArg(q, 0);
	ns->op4.typeval = *sql_fetch_localtype(TYPE_int);
	return ns;
}

stmt *
table_no_slices(backend *be, sql_table *t)
{
	InstrPtr q = newStmt(be->mb, sqlRef, no_slicesRef);
	q = pushStr(be->mb, q, t->s->base.name);
	q = pushStr(be->mb, q, t->base.name);

	pushInstruction(be->mb, q);
	if (be->pp_pc)
		moveInstruction(be->mb, be->mb->stop-1, be->pp_pc++);
	stmt *ns = stmt_create(be->mvc->sa, st_result); /* ?? */
	if (ns == NULL) {
		freeInstruction(be->mb, q);
		return NULL;
	}

	ns->nrcols = 1;
	ns->key = 1;
	ns->aggr = 1;
	ns->q = q;
	ns->nr = getArg(q, 0);
	ns->op4.typeval = *sql_fetch_localtype(TYPE_int);
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
		freeInstruction(be->mb, q);
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
 *   barrier (X_13:bit, X_14:int, X_15:ptr) := language.pipelines(42:int); # nrparts, or
 *   barrier (X_13:bit, X_14:int, X_15:ptr) := language.pipelines(X_42:int); # dynamic,
 *     # where X42 might have been dynamically generated by, e.g.,
 *     # slicer.no_slices(X_24:bat[:...]);
 */
static stmt *
pipeline_start(backend *be, int nrparts, int input, int sink)
{
	assert(!be->pp);
	be->pp_pc = be->mb->stop;
	InstrPtr q = newStmtArgs(be->mb, languageRef, pipelinesRef, 4);
	if (q == NULL)
		return NULL;
	q->barrier = BARRIERsymbol;
	setArgType(be->mb, q, 0, TYPE_bit);
	q = pushReturn(be->mb, q, newTmpVariable(be->mb, TYPE_int));
	q = pushReturn(be->mb, q, newTmpVariable(be->mb, TYPE_ptr));
	if (sink > 0) { /* TODO handle case for both a sink and input/nrparts */
	//	assert(0);
		q = pushArgument(be->mb, q, sink);
	} else if (input >= 0 && nrparts == 0) {
		assert(0);
		q = pushArgument(be->mb, q, input);
	} else {
		assert(nrparts < 0);
		q = pushInt(be->mb, q, nrparts);
	}

	pushInstruction(be->mb, q);
	be->nrparts = nrparts;
	int label = getArg(q, 0);	 /* barrier */
	be->pp = getArg(q, 1);		 /* counter */
	be->pipeline = getArg(q, 2); /* pipeline handle */

	if (q) {
		stmt *s = stmt_none(be);

		s->nr = label;
		s->q = q;
		return s;
	}
	return NULL;
}

static stmt *
pipeline_leave(backend *be, stmt *pp)
{
	InstrPtr r = NULL;
	int nrparts = be->nrparts;
	if (nrparts >= 0) {
		r = newStmtArgs(be->mb, calcRef, ">=", 3);
		if (r == NULL)
			return NULL;
		r->barrier = LEAVEsymbol;
		getArg(r, 0) = pp->nr;
		r = pushArgument(be->mb, r, be->pp);
		r = pushArgument(be->mb, r, getArg(pp->q, pp->q->retc) /* nrparts first arg of pipeline */);
		pushInstruction(be->mb, r);
	} else if (!be->source) {
		assert(0);
		r = newAssignment(be->mb);
		if (r == NULL)
			return NULL;
		r->barrier = LEAVEsymbol;
		getArg(r, 0) = pp->nr;
		r = pushBit(be->mb, r, FALSE);
	} else {
		r = newStmtArgs(be->mb, putName("pipeline"), "done", 4);
		if (r == NULL)
			return NULL;
		r = pushArgument(be->mb, r, be->source /* source/sink bat */);
		r = pushBit(be->mb, r, false);
		r = pushArgument(be->mb, r, getArg(pp->q, 2) /* pipeline */);
		//int arg = getDestVar(r);
		pushInstruction(be->mb, r);
		/*
		r = newStmtArgs(be->mb, calcRef, "not", 2);
		if (r == NULL)
			return NULL;
			*/
		r->barrier = LEAVEsymbol;
		getArg(r, 0) = pp->nr;
		//r = pushArgument(be->mb, r, arg);
		//pushInstruction(be->mb, r);
	}
	if (r) {
		stmt *s = stmt_none(be);

		s->nr = pp->nr;
		s->q = r;
		return s;
	}
	return NULL;
}

static stmt *
stmt_pp_create_nrparts_or_dynamic(backend *be, int nrparts, int input, int source, bool leave, int sink)
{
	stmt *s = pipeline_start(be, nrparts, input, sink);

	be->source = source;
	if ((nrparts >= 0 || leave) && pipeline_leave(be, s))
		return s;
	return s;
}

/* The following functions generate the control stmt-s to start, repeat or end
 * a pipeline block.
 *
 * Based on how the block is started the leave,redo and exit bits are later
 * generated.
 */


/* Generates:
 *   language.pipelines(<nrparts>:int);
 */
// catalog based horizontal slices
stmt *
stmt_pp_start_nrparts(backend *be, int nrparts)
{
	int sink = be->sink;
	be->sink = 0;
	return stmt_pp_create_nrparts_or_dynamic(be, nrparts, -1, 0, false, sink);
}

/* Generates:
 *   language.pipelines(X_<input>:int); # dynamic,
 */
// slicer based sharding
stmt *
stmt_pp_start_dynamic(backend *be, int input)
{
	int sink = be->sink;
	be->sink = 0;
	return stmt_pp_create_nrparts_or_dynamic(be, 0, input, 0, false, sink);
}

/* Generates:
 *   language.pipelines(X_<input>:int); # dynamic,
 */
// file_loader based slices
stmt *
stmt_pp_start_generator(backend *be, int source, bool leave)
{
	int sink = be->sink;
	be->sink = 0;
	return stmt_pp_create_nrparts_or_dynamic(be, -1, -1, source, leave, sink);
}

int
stmt_pp_jump(backend *be, stmt *label, int nrparts)
{
	InstrPtr r = NULL;
	int arg = 0;

	if (!be->source) {
		r = newStmtArgs(be->mb, putName("pipeline"), "counter", 2);
		if (r == NULL)
			return -1;
		getArg(r, 0) = getArg(label->q, 1); /* counter */
		r = pushArgument(be->mb, r, getArg(label->q, 2) /* pipeline */);
	} else {
		r = newStmtArgs(be->mb, putName("pipeline"), "done", 4);
		if (r == NULL)
			return -1;
		//getArg(r, 0) = getArg(label->q, 1); /* counter variable */
		r = pushArgument(be->mb, r, be->source /* source/sink bat */);
		r = pushBit(be->mb, r, true);
		r = pushArgument(be->mb, r, getArg(label->q, 2) /* pipeline */);
		arg = getDestVar(r);
	}
	pushInstruction(be->mb, r);

	if (be->source > 0)
		r = newStmtArgs(be->mb, calcRef, "not", 2);
	else if (be->nrparts >= 0)
		r = newStmtArgs(be->mb, calcRef, "<", 3);
	else
		r = newStmtArgs(be->mb, calcRef, ">", 3);
		//r = newAssignment(be->mb);
	if (r == NULL)
		return -1;
	r->barrier = REDOsymbol;
	getArg(r, 0) = label->nr;
	if (be->source > 0) {
		r = pushArgument(be->mb, r, arg);
	} else if (be->nrparts < 0) { /* crude stop, ie set counter < -nr_threads */
		r = pushArgument(be->mb, r, getArg(label->q, 1)); /* current nr */
		r = pushInt(be->mb, r, 0);
	} else /*if (be->nrparts >= 0) */{
		r = pushArgument(be->mb, r, getArg(label->q, 1)); /* current nr */
		r = pushArgument(be->mb, r, getArg(label->q, 3)); /* nrparts */
	//} else {
		//pushBit(be->mb, r, true);
	}
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
	be->source = 0;
	be->pp = be->pp_pc = be->nrparts = be->pipeline = be->concatcnt = 0;
	be->ppstmt = NULL;
	InstrPtr q = newAssignmentArgs(be->mb, 2);
	if (q == NULL)
		return -1;
	getArg(q, 0) = label->nr;
	q->argc = q->retc = 1;
	q->barrier = EXITsymbol;
	pushInstruction(be->mb, q);

	if (q) {
		if (be->updates) {
			q = newStmt(be->mb, sqlRef, mvcRef);
			q->argv[0] = be->mvc_var;
			q->argv[1] = be->mvc_var;
			q->argc++;
			pushInstruction(be->mb, q);
			be->updates = false;
		}

		if (be->cleanup) {
			q = newStmt(be->mb, "language", "pass");
			q = pushArgument(be->mb, q, be->cleanup);
			pushInstruction(be->mb, q);
			be->cleanup = 0;
		}
		return 0;
	}

	return -1;
}

void
pp_cleanup(backend *be, int var)
{
	assert(be->cleanup == 0);
	be->cleanup = var;
}


stmt *
stmt_merge(backend *be, stmt *lobc, stmt *robc, bool asc, bool nlast, stmt *zl, stmt *zb, stmt *za)
{
	if (lobc == NULL || robc == NULL)
		return NULL;

	InstrPtr q = newStmtArgs(be->mb, "sort", "merge", zl?10:7);
	q = pushReturn(be->mb, q, newTmpVariable(be->mb, TYPE_any));
	q = pushReturn(be->mb, q, newTmpVariable(be->mb, TYPE_any));
	pushArgument(be->mb, q, lobc->nr);
	pushArgument(be->mb, q, robc->nr);
	if (zl) {
		pushArgument(be->mb, q, zl->nr);
		pushArgument(be->mb, q, zb->nr);
		pushArgument(be->mb, q, za->nr);
	}
	q = pushBit(be->mb, q, !asc);
	q = pushBit(be->mb, q, nlast);
	pushInstruction(be->mb, q);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_order);
		if (s == NULL) {
			return NULL;
		}

		s->op1 = lobc;
		s->op2 = zl;
		s->op3 = zb;
		//s->op4.stmt = za;
		s->key = 0;
		s->nrcols = lobc->nrcols;
		s->nr = getDestVar(q);
		s->q = q;
		s->tname = lobc->tname;
		s->cname = lobc->cname;
		s->label = lobc->label;
		return s;
	}
	if (ma_get_eb(be->mvc->sa)->enabled)
		eb_error(ma_get_eb(be->mvc->sa), be->mvc->errstr[0] ? be->mvc->errstr : be->mb->errors ? be->mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_mproject(backend *be, stmt *zl, stmt *lc, stmt *rc, int pipeline)
{
	if (zl == NULL || lc == NULL || rc == NULL)
		return NULL;
	assert(lc->nrcols);
	InstrPtr q = newStmt(be->mb, "sort", "mproject");
	pushArgument(be->mb, q, zl->nr);
	pushArgument(be->mb, q, lc->nr);
	pushArgument(be->mb, q, rc->nr);
	if (pipeline)
		pushArgument(be->mb, q, pipeline);
	pushInstruction(be->mb, q);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_join);
		if (s == NULL) {
			return NULL;
		}

		s->op1 = zl;
		s->op2 = lc;
		s->op3 = rc;
		s->flag = cmp_project;
		s->key = 0;
		s->nrcols = lc->nrcols;
		s->nr = getDestVar(q);
		s->q = q;
		s->tname = lc->tname;
		s->cname = lc->cname;
		s->label = lc->label;
		return s;
	}
	if (ma_get_eb(be->mvc->sa)->enabled)
		eb_error(ma_get_eb(be->mvc->sa), be->mvc->errstr[0] ? be->mvc->errstr : be->mb->errors ? be->mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_pp_alias(backend *be, InstrPtr q, sql_exp *e, int resultargnr)
{
	stmt *s = stmt_none(be);
	s->op4.typeval = *exp_subtype(e);
	assert(resultargnr >= 0 && resultargnr < q->retc);
	s->nr = getArg(q, resultargnr);
	s->key = s->nrcols = 1;
	s->q = q;
	s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
	s->q = q;
	return s;
}


int
stmt_concat(backend *be, int p_block, int nr)
{
		InstrPtr q = newStmt(be->mb, "pipeline", "concat"); /* multi - relation pipeline */
		//if (p_block > 0)
		//	pushArgument(be->mb, q, p_block);
		q = pushInt(be->mb, q, nr);
		pushInstruction(be->mb, q);
		int source = getDestVar(q);
		if (p_block <= 0)
			set_pipeline(be, stmt_pp_start_generator(be, source, true));
		else
			moveInstruction(be->mb, be->mb->stop-1, be->pp_pc++);
		be->source = source;
		be->concatcnt = 0;
		return source;
}

/* concat.blockid (concat, blockid, nothingdonesofar) := bit
 * if blockid matches the active blockid and nothingelse was done return true */
int
stmt_concat_barrier(backend *be, int concat, int blockid, int prefcond)
{
	InstrPtr q = newStmt(be->mb, "pipeline", "concat_block");
	if (q == NULL)
		return -1;
	q = pushArgument(be->mb, q, concat);
	q = pushInt(be->mb, q, blockid);
	if (prefcond) {
		q = pushArgument(be->mb, q, prefcond);
	} else {
		q = pushBit(be->mb, q, FALSE);
	}
	q = pushArgument(be->mb, q, be->pipeline);
	q->barrier = BARRIERsymbol;
	pushInstruction(be->mb, q);
	return getArg(q, 0);
}

int
stmt_concat_barrier_end(backend *be, int barrier)
{
	InstrPtr q = newAssignmentArgs(be->mb, 2);
	if (q == NULL)
		return -1;
	getArg(q, 0) = barrier;
	q->argc = q->retc = 1;
	q->barrier = EXITsymbol;
	pushInstruction(be->mb, q);
	return 0;
}

int
stmt_concat_add_source(backend *be)
{
	/* statement at top of stack need to be moved and added to the concat iterator */
	int next = getDestVar(be->mb->stmt[be->mb->stop-1]);
	moveInstruction(be->mb, be->mb->stop-1, be->pp_pc++);
	InstrPtr q = newStmt(be->mb, "pipeline", "concat_add");
	q->argv[0] = be->source;
	q = pushArgument(be->mb, q, be->source);
	q = pushArgument(be->mb, q, next);
	pushInstruction(be->mb, q);
	moveInstruction(be->mb, be->mb->stop-1, be->pp_pc++);
	be->concatcnt++;
	return 0;
}

int
stmt_concat_add_subconcat(backend *be, int p_source, int p_concatcnt)
{
	InstrPtr q = newStmt(be->mb, "pipeline", "concat_add");
	q->argv[0] = p_source;
	q = pushArgument(be->mb, q, p_source);
	q = pushArgument(be->mb, q, be->source);
	pushInstruction(be->mb, q);
	/* statement at top of stack need to be moved and added to the concat iterator */
	moveInstruction(be->mb, be->mb->stop-1, be->pp_pc++);
	be->source = p_source;
	be->concatcnt = p_concatcnt;
	be->concatcnt++;
	return 0;
}

/* allowed value combinations for (nr_slices, var_nr_slices):
 * (-1, -1): a dummy pipeline.counter
 * (nr_slices, -1): a static pipeline.counter(<nr_slices>)
 * (-1, var_nr_slices): a dynamic pipeline.counter(<var_nr_slices>)
 */
int
pp_counter(backend *be, int nr_slices, int var_nr_slices, bool sync)
{
	assert(!(nr_slices > 0 && var_nr_slices > 0));
	InstrPtr q = newStmt(be->mb, "pipeline", "counter");
	if (var_nr_slices == -1)
		q = pushInt(be->mb, q, nr_slices);
	else
		q = pushArgument(be->mb, q, var_nr_slices);
	if (sync)
		q = pushBit(be->mb, q, TRUE);
	pushInstruction(be->mb, q);
	return getArg(q, 0);
}

int
pp_counter_get(backend *be, int counter)
{
	InstrPtr q = newStmt(be->mb, "pipeline", "counter_get");
	q->argv[0] = be->pp;
	q = pushArgument(be->mb, q, counter);
	q = pushArgument(be->mb, q, be->pipeline);
	pushInstruction(be->mb, q);
	return getArg(q, 0);
}

int
pp_claim(backend *be, int resultset, int nrrows)
{
	InstrPtr q = newStmt(be->mb, "pipeline", "claim");
	q = pushArgument(be->mb, q, resultset);
	q = pushArgument(be->mb, q, nrrows);
	pushInstruction(be->mb, q);
	return getDestVar(q);
}

stmt *
source_next(backend *be, sql_subtype *tpe)
{
	InstrPtr q = newStmt(be->mb, putName("source"), putName("next"));
	if (q == NULL)
		return NULL;

	int tt = tpe->type->localtype;
	setVarType(be->mb, getArg(q, 0), newBatType(tt));
	q = pushArgument(be->mb, q, be->source);
	pushInstruction(be->mb, q);

	stmt *s = stmt_none(be);
	s->op4.typeval = *tpe;
	s->q = q;
	s->nr = getArg(q, 0);
	s->nrcols = 1;
	return s;
}
