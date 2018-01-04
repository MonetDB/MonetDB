/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_mem.h"
#include "sql_stack.h"
#include "sql_statement.h"
#include "sql_gencode.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_optimizer.h"

#include "mal_namespace.h"
#include "mal_builder.h"
#include "mal_debugger.h"
#include "opt_prelude.h"

#include <string.h>

/*
 * Some utility routines to generate code
 * The equality operator in MAL is '==' instead of '='.
 */
static const char *
convertMultiplexMod(const char *mod, const char *op)
{
	if (strcmp(op, "=") == 0)
		return "calc";
	return mod;
}

static const char *
convertMultiplexFcn(const char *op)
{
	if (strcmp(op, "=") == 0)
		return "==";
	return op;
}

static const char *
convertOperator(const char *op)
{
	if (strcmp(op, "=") == 0)
		return "==";
	return op;
}

static InstrPtr
multiplex2(MalBlkPtr mb, const char *mod, const char *name, int o1, int o2, int rtype)
{
	InstrPtr q = NULL;

	q = newStmt(mb, malRef, multiplexRef);
	if (q == NULL)
		return NULL;
	setVarType(mb, getArg(q, 0), newBatType(rtype));
	setVarUDFtype(mb, getArg(q, 0));
	q = pushStr(mb, q, convertMultiplexMod(mod, name));
	q = pushStr(mb, q, convertMultiplexFcn(name));
	q = pushArgument(mb, q, o1);
	q = pushArgument(mb, q, o2);
	return q;
}

static InstrPtr
dump_1(MalBlkPtr mb, const char *mod, const char *name, stmt *o1)
{
	InstrPtr q = NULL;

	if (o1->nr < 0)
		return NULL;
	q = newStmt(mb, mod, name);
	q = pushArgument(mb, q, o1->nr);
	return q;
}

static InstrPtr
dump_2(MalBlkPtr mb, const char *mod, const char *name, stmt *o1, stmt *o2)
{
	InstrPtr q = NULL;

	if (o1->nr < 0 || o2->nr < 0)
		return NULL;
	q = newStmt(mb, mod, name);
	q = pushArgument(mb, q, o1->nr);
	q = pushArgument(mb, q, o2->nr);
	return q;
}

static InstrPtr
pushPtr(MalBlkPtr mb, InstrPtr q, ptr val)
{
	int _t;
	ValRecord cst;

	if (q == NULL)
		return NULL;
	cst.vtype= TYPE_ptr;
	cst.val.pval = val;
	cst.len = 0;
	_t = defConstant(mb, TYPE_ptr, &cst);
	return pushArgument(mb, q, _t);
}

static InstrPtr
pushSchema(MalBlkPtr mb, InstrPtr q, sql_table *t)
{
	if (t->s)
		return pushArgument(mb, q, getStrConstant(mb,t->s->base.name));
	else
		return pushNil(mb, q, TYPE_str);
}

int
stmt_key(stmt *s)
{
	const char *nme = column_name(NULL, s);

	return hash_key(nme);
}

/* #TODO make proper traversal operations */
stmt *
stmt_atom_string(backend *be, const char *S)
{
	const char *s = sql2str(sa_strdup(be->mvc->sa, S));
	sql_subtype t;

	sql_find_subtype(&t, "varchar", _strlen(s), 0);
	return stmt_atom(be, atom_string(be->mvc->sa, &t, s));
}

stmt *
stmt_atom_string_nil(backend *be)
{
	sql_subtype t;

	sql_find_subtype(&t, "clob", 0, 0);
	return stmt_atom(be, atom_string(be->mvc->sa, &t, NULL));
}

stmt *
stmt_atom_int(backend *be, int i)
{
	sql_subtype t;

	sql_find_subtype(&t, "int", 32, 0);
	return stmt_atom(be, atom_int(be->mvc->sa, &t, i));
}

stmt *
stmt_atom_lng(backend *be, lng i)
{
	sql_subtype t;

	sql_find_subtype(&t, "bigint", 64, 0);
	return stmt_atom(be, atom_int(be->mvc->sa, &t, i));
}

stmt *
stmt_atom_lng_nil(backend *be)
{
	sql_subtype t;

	sql_find_subtype(&t, "bigint", 64, 0);
	return stmt_atom(be, atom_general(be->mvc->sa, &t, NULL));
}

stmt *
stmt_bool(backend *be, int b)
{
	sql_subtype t;

	sql_find_subtype(&t, "boolean", 0, 0);
	if (b) {
		return stmt_atom(be, atom_bool(be->mvc->sa, &t, TRUE));
	} else {
		return stmt_atom(be, atom_bool(be->mvc->sa, &t, FALSE));
	}
}

static stmt *
stmt_create(sql_allocator *sa, st_type type)
{
	stmt *s = SA_NEW(sa, stmt);
	if(!s)
		return NULL;

	s->type = type;
	s->op1 = NULL;
	s->op2 = NULL;
	s->op3 = NULL;
	s->op4.lval = NULL;
	s->flag = 0;
	s->nrcols = 0;
	s->key = 0;
	s->aggr = 0;
	s->nr = 0;
	s->partition = 0;
	s->tname = s->cname = NULL;
	return s;
}

stmt *
stmt_group(backend *be, stmt *s, stmt *grp, stmt *ext, stmt *cnt, int done)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (s->nr < 0)
		return NULL;
	if (grp && (grp->nr < 0 || ext->nr < 0 || cnt->nr < 0)) 
		return NULL;

	q = newStmt(mb, groupRef, done ? grp ? subgroupdoneRef : groupdoneRef : grp ? subgroupRef : groupRef);
	if(!q)
		return NULL;

	/* output variables extent and hist */
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, s->nr);
	if (grp)
		q = pushArgument(mb, q, grp->nr);
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
stmt_none(backend *be)
{
	return stmt_create(be->mvc->sa, st_none);
}

static int
create_bat(MalBlkPtr mb, int tt)
{
	InstrPtr q = newStmt(mb, batRef, newRef);

	if (q == NULL)
		return -1;
	setVarType(mb, getArg(q, 0), newBatType(tt));
	setVarUDFtype(mb, getArg(q, 0));
	q = pushType(mb, q, tt);
	return getDestVar(q);
}

static int *
dump_table(sql_allocator *sa, MalBlkPtr mb, sql_table *t)
{
	int i = 0;
	node *n;
	int *l = SA_NEW_ARRAY(sa, int, list_length(t->columns.set) + 1);

	if (!l)
		return NULL;

	/* tid column */
	if ((l[i++] = create_bat(mb, TYPE_oid)) < 0) 
		return NULL;

	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;

		if ((l[i++] = create_bat(mb, c->type.type->localtype)) < 0)
			return NULL;
	}
	return l;
}

stmt *
stmt_var(backend *be, const char *varname, sql_subtype *t, int declare, int level)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	char buf[IDLENGTH];

	if (level == 1 ) { /* global */
		int tt = t->type->localtype;

		q = newStmt(mb, sqlRef, putName("getVariable"));
		q = pushArgument(mb, q, be->mvc_var);
		q = pushStr(mb, q, varname);
		if (q == NULL)
			return NULL;
		setVarType(mb, getArg(q, 0), tt);
		setVarUDFtype(mb, getArg(q, 0));
	} else if (!declare) {
		(void) snprintf(buf, sizeof(buf), "A%s", varname);
		q = newAssignment(mb);
		q = pushArgumentId(mb, q, buf);
	} else {
		int tt;

		tt = t->type->localtype;
		(void) snprintf(buf, sizeof(buf), "A%s", varname);
		q = newInstruction(mb, NULL, NULL);
		if (q == NULL) {
			return NULL;
		}
		q->argc = q->retc = 0;
		q = pushArgumentId(mb, q, buf);
		q = pushNil(mb, q, tt);
		pushInstruction(mb, q);
		if (q == NULL)
			return NULL;
		q->retc++;
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_var);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		if (t)
			s->op4.typeval = *t;
		else
			s->op4.typeval.type = NULL;
		s->flag = declare + (level << 1);
		s->key = 1;
		s->q = q;
		s->nr = getDestVar(q);
		return s;
	}
	return NULL;
}

stmt *
stmt_vars(backend *be, const char *varname, sql_table *t, int declare, int level)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	int *l;

	(void)varname;
	/* declared table */
	if ((l = dump_table(be->mvc->sa, mb, t)) != NULL) {
		stmt *s = stmt_create(be->mvc->sa, st_var);

		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		t->data = l;
		/*
		s->op2 = (stmt*)l; 
		s->op3 = (stmt*)t;
		*/
		s->flag = declare + (level << 1);
		s->key = 1;
		s->nr = l[0];
		return s;
	}
	return NULL;
}

stmt *
stmt_varnr(backend *be, int nr, sql_subtype *t)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = newAssignment(mb);

	if (!q)
		return NULL;
	if (be->mvc->argc && be->mvc->args[nr]->varid >= 0) {
		q = pushArgument(mb, q, be->mvc->args[nr]->varid);
	} else {
		char buf[IDLENGTH];

		(void) snprintf(buf, sizeof(buf), "A%d", nr);
		q = pushArgumentId(mb, q, buf);
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_var);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->op1 = NULL;
		if (t)
			s->op4.typeval = *t;
		else
			s->op4.typeval.type = NULL;
		s->flag = nr;
		s->key = 1;
		s->q = q;
		s->nr = getDestVar(q);
		return s;
	}
	return NULL;
}

stmt *
stmt_table(backend *be, stmt *cols, int temp)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = newAssignment(mb);

	if (cols->nr < 0)
		return NULL;

	if (cols->type != st_list) {
		q = newStmt(mb, sqlRef, printRef);
		q = pushStr(mb, q, "not a valid output list\n");
		if (q == NULL)
			return NULL;
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_table);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->op1 = cols;
		s->flag = temp;
		return s;
	}
	return NULL;
}

stmt *
stmt_temp(backend *be, sql_subtype *t)
{
	int tt = t->type->localtype;
	MalBlkPtr mb = be->mb;
	InstrPtr q = newStmt(mb, batRef, newRef);

	if (q == NULL)
		return NULL;
	setVarType(mb, getArg(q, 0), newBatType(tt));
	setVarUDFtype(mb, getArg(q, 0));
	q = pushType(mb, q, tt);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_temp);

		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}
		s->op4.typeval = *t;
		s->nrcols = 1;
		s->q = q;
		s->nr = getDestVar(q);
		return s;
	}
	return NULL;
}

stmt *
stmt_tid(backend *be, sql_table *t, int partition)
{
	int tt = TYPE_oid;
	MalBlkPtr mb = be->mb;
	InstrPtr q;

	if (!t->s && t->data) { /* declared table */
		stmt *s = stmt_create(be->mvc->sa, st_tid);
		int *l = t->data;

		assert(partition == 0);
		s->partition = partition;
		s->op4.tval = t;
		s->nrcols = 1;
		s->nr = l[0];
		return s;
	}
       	q = newStmt(mb, sqlRef, tidRef);
	if (q == NULL)
		return NULL;
	setVarType(mb, getArg(q, 0), newBatType(tt));
	setVarUDFtype(mb, getArg(q, 0));
	q = pushArgument(mb, q, be->mvc_var);
	q = pushSchema(mb, q, t);
	q = pushStr(mb, q, t->base.name);
	if (q == NULL)
		return NULL;
	if (t && (!isRemote(t) && !isMergeTable(t)) && partition) {
		sql_trans *tr = be->mvc->session->tr;
		BUN rows = (BUN) store_funcs.count_col(tr, t->columns.set->h->data, 1);
		setRowCnt(mb,getArg(q,0),rows);
		if (t->p && 0)
			setMitosisPartition(q, t->p->base.id);
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_tid);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->partition = partition;
		s->op4.tval = t;
		s->nrcols = 1;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_bat(backend *be, sql_column *c, int access, int partition)
{
	int tt = c->type.type->localtype;
	MalBlkPtr mb = be->mb;
	InstrPtr q;

	/* for read access tid.project(col) */
	if (!c->t->s && c->t->data) { /* declared table */
		stmt *s = stmt_create(be->mvc->sa, st_bat);
		int *l = c->t->data;

		assert(partition == 0);
		s->partition = partition;
		s->op4.cval = c;
		s->nrcols = 1;
		s->flag = access;
		s->nr = l[c->colnr+1];
		return s;
	}
       	q = newStmt(mb, sqlRef, bindRef);
	if (q == NULL)
		return NULL;
	if (access == RD_UPD_ID) {
		q = pushReturn(mb, q, newTmpVariable(mb, newBatType(tt)));
		setVarUDFtype(mb, getArg(q, 0));
		setVarUDFtype(mb, getArg(q, 1));
	} else {
		setVarType(mb, getArg(q, 0), newBatType(tt));
		setVarUDFtype(mb, getArg(q, 0));
	}
	q = pushArgument(mb, q, be->mvc_var);
	q = pushSchema(mb, q, c->t);
	q = pushArgument(mb, q, getStrConstant(mb,c->t->base.name));
	q = pushArgument(mb, q, getStrConstant(mb,c->base.name));
	q = pushArgument(mb, q, getIntConstant(mb,access));
	if (q == NULL)
		return NULL;

	if (access == RD_UPD_ID) {
		setVarType(mb, getArg(q, 1), newBatType(tt));
		setVarUDFtype(mb, getArg(q, 1));
	}
	if (access != RD_INS && partition) {
		sql_trans *tr = be->mvc->session->tr;

		if (c && (!isRemote(c->t) && !isMergeTable(c->t))) {
			BUN rows = (BUN) store_funcs.count_col(tr, c, 1);
			setRowCnt(mb,getArg(q,0),rows);
			if (c->t->p && 0)
				setMitosisPartition(q, c->t->p->base.id);
		}
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_bat);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->partition = partition;
		s->op4.cval = c;
		s->nrcols = 1;
		s->flag = access;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_idxbat(backend *be, sql_idx *i, int access, int partition)
{
	int tt = hash_index(i->type)?TYPE_lng:TYPE_oid;
	MalBlkPtr mb = be->mb;
	InstrPtr q = newStmt(mb, sqlRef, bindidxRef);

	if (q == NULL)
		return NULL;

	if (access == RD_UPD_ID) {
		q = pushReturn(mb, q, newTmpVariable(mb, newBatType(tt)));
	} else {
		setVarType(mb, getArg(q, 0), newBatType(tt));
		setVarUDFtype(mb, getArg(q, 0));
	}

	q = pushArgument(mb, q, be->mvc_var);
	q = pushSchema(mb, q, i->t);
	q = pushArgument(mb, q, getStrConstant(mb, i->t->base.name));
	q = pushArgument(mb, q, getStrConstant(mb, i->base.name));
	q = pushArgument(mb, q, getIntConstant(mb, access));
	if (q == NULL)
		return NULL;

	if (access == RD_UPD_ID) {
		setVarType(mb, getArg(q, 1), newBatType(tt));
		setVarUDFtype(mb, getArg(q, 1));
	}
	if (access != RD_INS && partition) {
		sql_trans *tr = be->mvc->session->tr;

		if (i && (!isRemote(i->t) && !isMergeTable(i->t))) {
			BUN rows = (BUN) store_funcs.count_idx(tr, i, 1);
			setRowCnt(mb,getArg(q,0),rows);
			if (i->t->p && 0)
				setMitosisPartition(q, i->t->p->base.id);
		}
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_idxbat);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->partition = partition;
		s->op4.idxval = i;
		s->nrcols = 1;
		s->flag = access;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_append_col(backend *be, sql_column *c, stmt *b, int fake)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (b->nr < 0)
		return NULL;

	if (!c->t->s && c->t->data) { /* declared table */
		int *l = c->t->data;

		if (c->colnr == 0) { /* append to tid column */
			q = newStmt(mb, sqlRef, "grow");
			q = pushArgument(mb, q, l[0]);
			q = pushArgument(mb, q, b->nr);
		} 
		q = newStmt(mb, batRef, appendRef);
		q = pushArgument(mb, q, l[c->colnr+1]);
		q = pushArgument(mb, q, b->nr);
		q = pushBit(mb, q, TRUE);
		getArg(q,0) = l[c->colnr+1]; 
	} else if (!fake) {	/* fake append */
		q = newStmt(mb, sqlRef, appendRef);
		q = pushArgument(mb, q, be->mvc_var);
		if (q == NULL)
			return NULL;
		getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
		q = pushSchema(mb, q, c->t);
		q = pushStr(mb, q, c->t->base.name);
		q = pushStr(mb, q, c->base.name);
		q = pushArgument(mb, q, b->nr);
		if (q == NULL)
			return NULL;
		be->mvc_var = getDestVar(q);
	} else {
		return b;
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_append_col);

		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = b;
		s->op4.cval = c;
		s->q = q;
		s->nr = getDestVar(q);
		return s;
	}
	return NULL;
}

stmt *
stmt_append_idx(backend *be, sql_idx *i, stmt *b)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (b->nr < 0)
		return NULL;

	q = newStmt(mb, sqlRef, appendRef);
	q = pushArgument(mb, q, be->mvc_var);
	if (q == NULL)
		return NULL;
	getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
	q = pushSchema(mb, q, i->t);
	q = pushStr(mb, q, i->t->base.name);
	q = pushStr(mb, q, sa_strconcat(be->mvc->sa, "%", i->base.name));
	q = pushArgument(mb, q, b->nr);
	if (q == NULL)
		return NULL;
	be->mvc_var = getDestVar(q);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_append_idx);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->op1 = b;
		s->op4.idxval = i;
		s->q = q;
		s->nr = getDestVar(q);
		return s;
	}
	return NULL;
}

stmt *
stmt_update_col(backend *be, sql_column *c, stmt *tids, stmt *upd)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (tids->nr < 0 || upd->nr < 0)
		return NULL;

	if (!c->t->s && c->t->data) { /* declared table */
		int *l = c->t->data;

		q = newStmt(mb, batRef, updateRef);
		q = pushArgument(mb, q, l[c->colnr+1]);
		q = pushArgument(mb, q, tids->nr);
		q = pushArgument(mb, q, upd->nr);
	} else {
		q = newStmt(mb, sqlRef, updateRef);
		q = pushArgument(mb, q, be->mvc_var);
		if (q == NULL)
			return NULL;
		getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
		q = pushSchema(mb, q, c->t);
		q = pushStr(mb, q, c->t->base.name);
		q = pushStr(mb, q, c->base.name);
		q = pushArgument(mb, q, tids->nr);
		q = pushArgument(mb, q, upd->nr);
		if (q == NULL)
			return NULL;
		be->mvc_var = getDestVar(q);
	}
	if (q){
		stmt *s = stmt_create(be->mvc->sa, st_update_col);

		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = tids;
		s->op2 = upd;
		s->op4.cval = c;
		s->q = q;
		s->nr = getDestVar(q);
		return s;
	}
	return NULL;
}


stmt *
stmt_update_idx(backend *be, sql_idx *i, stmt *tids, stmt *upd)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (tids->nr < 0 || upd->nr < 0)
		return NULL;

	q = newStmt(mb, sqlRef, updateRef);
	q = pushArgument(mb, q, be->mvc_var);
	if (q == NULL)
		return NULL;
	getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
	q = pushSchema(mb, q, i->t);
	q = pushStr(mb, q, i->t->base.name);
	q = pushStr(mb, q, sa_strconcat(be->mvc->sa, "%", i->base.name));
	q = pushArgument(mb, q, tids->nr);
	q = pushArgument(mb, q, upd->nr);
	if (q == NULL)
		return NULL;
	be->mvc_var = getDestVar(q);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_update_idx);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->op1 = tids;
		s->op2 = upd;
		s->op4.idxval = i;
		s->q = q;
		s->nr = getDestVar(q);
		return s;
	}
	return NULL;
}

stmt *
stmt_delete(backend *be, sql_table *t, stmt *tids)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (tids->nr < 0)
		return NULL;

	if (!t->s && t->data) { /* declared table */
		int *l = t->data;

		q = newStmt(mb, batRef, deleteRef);
		q = pushArgument(mb, q, l[0]);
		q = pushArgument(mb, q, tids->nr);
	} else {
		q = newStmt(mb, sqlRef, deleteRef);
		q = pushArgument(mb, q, be->mvc_var);
		if (q == NULL)
			return NULL;
		getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
		q = pushSchema(mb, q, t);
		q = pushStr(mb, q, t->base.name);
		q = pushArgument(mb, q, tids->nr);
		if (q == NULL)
			return NULL;
		be->mvc_var = getDestVar(q);
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_delete);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->op1 = tids;
		s->op4.tval = t;
		s->q = q;
		s->nr = getDestVar(q);
		return s;
	}
	return NULL;
}

stmt *
stmt_const(backend *be, stmt *s, stmt *val)
{
	InstrPtr q = NULL;
	MalBlkPtr mb = be->mb;

	if (val)
		q = dump_2(mb, algebraRef, projectRef, s, val);
	else 
		q = dump_1(mb, algebraRef, projectRef, s);
	if (q) {
		stmt *ns = stmt_create(be->mvc->sa, st_const);
		if (ns == NULL) {
			freeInstruction(q);
			return NULL;
		}

		ns->op1 = s;
		ns->op2 = val;
		ns->nrcols = s->nrcols;
		ns->key = s->key;
		ns->aggr = s->aggr;
		ns->q = q;
		ns->nr = getDestVar(q);
		return ns;
	}
	return NULL;
}

stmt *
stmt_gen_group(backend *be, stmt *gids, stmt *cnts)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = dump_2(mb, algebraRef, groupbyRef, gids, cnts);

	if (q) {
		stmt *ns = stmt_create(be->mvc->sa, st_gen_group);
		if (ns == NULL) {
			freeInstruction(q);
			return NULL;
		}

		ns->op1 = gids;
		ns->op2 = cnts;
	
		ns->nrcols = gids->nrcols;
		ns->key = 0;
		ns->aggr = 0;
		ns->q = q;
		ns->nr = getDestVar(q);
		return ns;
	}
	return NULL;
}

stmt *
stmt_mirror(backend *be, stmt *s)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = dump_1(mb, batRef, mirrorRef, s);

	if (q) {
		stmt *ns = stmt_create(be->mvc->sa, st_mirror);
		if (ns == NULL) {
			freeInstruction(q);
			return NULL;
		}

		ns->op1 = s;
		ns->nrcols = 2;
		ns->key = s->key;
		ns->aggr = s->aggr;
		ns->q = q;
		ns->nr = getDestVar(q);
		return ns;
	}
	return NULL;
}

stmt *
stmt_result(backend *be, stmt *s, int nr)
{
	stmt *ns;

	if (s->type == st_join && s->flag == cmp_joined) {
		if (nr)
			return s->op2;
		return s->op1;
	}

	if (s->op1->nr < 0)
		return NULL;

	ns = stmt_create(be->mvc->sa, st_result);
	if(!ns) {
		return NULL;
	}
	if (s->op1->type == st_join && s->op1->flag == cmp_joined) {
		assert(0);
	} else if (nr) {
		int v = getArg(s->q, nr);

		assert(s->q->retc >= nr);
		ns->nr = v;
	} else {
		ns->nr = s->nr;
	}
	ns->op1 = s;
	ns->flag = nr;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->aggr = s->aggr;
	return ns;
}


/* limit maybe atom nil */
stmt *
stmt_limit(backend *be, stmt *col, stmt *piv, stmt *gid, stmt *offset, stmt *limit, int distinct, int dir, int last, int order)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	int l, p, g, c;

	if (col->nr < 0 || offset->nr < 0 || limit->nr < 0)
		return NULL;
	if (piv && (piv->nr < 0 || gid->nr < 0))
		return NULL;

	c = (col) ? col->nr : 0;
	p = (piv) ? piv->nr : 0;
	g = (gid) ? gid->nr : 0;

	/* first insert single value into a bat */
	if (col->nrcols == 0) {
		int k, tt = tail_type(col)->type->localtype;

		q = newStmt(mb, batRef, newRef);
		if (q == NULL)
			return NULL;
		setVarType(mb, getArg(q, 0), newBatType(tt));
		setVarUDFtype(mb, getArg(q, 0));
		q = pushType(mb, q, tt);
		if (q == NULL)
			return NULL;
		k = getDestVar(q);

		q = newStmt(mb, batRef, appendRef);
		q = pushArgument(mb, q, k);
		q = pushArgument(mb, q, c);
		if (q == NULL)
			return NULL;
		c = k;
	}
	if (order) {
		int topn = 0;

		q = newStmt(mb, calcRef, "+");
		q = pushArgument(mb, q, offset->nr);
		q = pushArgument(mb, q, limit->nr);
		if (q == NULL)
			return NULL;
		topn = getDestVar(q);

		q = newStmt(mb, algebraRef, firstnRef);
		if (!last) /* we need the groups for the next firstn */
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, c);
		if (p)
			q = pushArgument(mb, q, p);
		if (g)
			q = pushArgument(mb, q, g);
		q = pushArgument(mb, q, topn);
		q = pushBit(mb, q, dir != 0);
		q = pushBit(mb, q, distinct != 0);

		if (q == NULL)
			return NULL;
		l = getArg(q, 0);
		l = getDestVar(q);
	} else {
		int len;

		q = newStmt(mb, calcRef, "+");
		q = pushArgument(mb, q, offset->nr);
		q = pushArgument(mb, q, limit->nr);
		if (q == NULL)
			return NULL;
		len = getDestVar(q);

		/* since both arguments of algebra.subslice are
		   inclusive correct the LIMIT value by
		   subtracting 1 */
		q = newStmt(mb, calcRef, "-");
		q = pushArgument(mb, q, len);
		q = pushInt(mb, q, 1);
		if (q == NULL)
			return NULL;
		len = getDestVar(q);

		q = newStmt(mb, algebraRef, subsliceRef);
		q = pushArgument(mb, q, c);
		q = pushArgument(mb, q, offset->nr);
		q = pushArgument(mb, q, len);
		if (q == NULL)
			return NULL;
		l = getDestVar(q);
	}
	/* retrieve the single values again */
	if (col->nrcols == 0) {
		q = newStmt(mb, algebraRef, findRef);
		q = pushArgument(mb, q, l);
		q = pushOid(mb, q, 0);
		if (q == NULL)
			return NULL;
		l = getDestVar(q);
	}
	if (q) {
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
	return NULL;
}

stmt *
stmt_sample(backend *be, stmt *s, stmt *sample)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	
	if (s->nr < 0 || sample->nr < 0)
		return NULL;
	q = newStmt(mb, sampleRef, subuniformRef);
	q = pushArgument(mb, q, s->nr);
	q = pushArgument(mb, q, sample->nr);
	if (q) {
		stmt *ns = stmt_create(be->mvc->sa, st_sample);
		if (ns == NULL) {
			freeInstruction(q);
			return NULL;
		}

		ns->op1 = s;
		ns->op2 = sample;
		ns->nrcols = s->nrcols;
		ns->key = s->key;
		ns->aggr = s->aggr;
		ns->flag = 0;
		ns->q = q;
		ns->nr = getDestVar(q);
		return ns;
	}
	return NULL;
}


stmt *
stmt_order(backend *be, stmt *s, int direction)
{
	int reverse = (direction <= 0);
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (s->nr < 0)
		return NULL;
	q = newStmt(mb, algebraRef, sortRef);
	/* both ordered result and oid's order en subgroups */
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, s->nr);
	q = pushBit(mb, q, reverse);
	q = pushBit(mb, q, FALSE);
	if (q == NULL)
		return NULL;

	if (q) {
		stmt *ns = stmt_create(be->mvc->sa, st_order);
		if (ns == NULL) {
			freeInstruction(q);
			return NULL;
		}

		ns->op1 = s;
		ns->flag = direction;
		ns->nrcols = s->nrcols;
		ns->key = s->key;
		ns->aggr = s->aggr;
		ns->q = q;
		ns->nr = getDestVar(q);
		return ns;
	}
	return NULL;
}

stmt *
stmt_reorder(backend *be, stmt *s, int direction, stmt *orderby_ids, stmt *orderby_grp)
{
	int reverse = (direction <= 0);
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (s->nr < 0 || orderby_ids->nr < 0 || orderby_grp->nr < 0)
		return NULL;
	q = newStmt(mb, algebraRef, sortRef);
	/* both ordered result and oid's order en subgroups */
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, s->nr);
	q = pushArgument(mb, q, orderby_ids->nr);
	q = pushArgument(mb, q, orderby_grp->nr);
	q = pushBit(mb, q, reverse);
	q = pushBit(mb, q, FALSE);
	if (q == NULL)
		return NULL;
	if (q) {
		stmt *ns = stmt_create(be->mvc->sa, st_reorder);
		if (ns == NULL) {
			freeInstruction(q);
			return NULL;
		}

		ns->op1 = s;
		ns->op2 = orderby_ids;
		ns->op3 = orderby_grp;
		ns->flag = direction;
		ns->nrcols = s->nrcols;
		ns->key = s->key;
		ns->aggr = s->aggr;
		ns->nr = getDestVar(q);
		ns->q = q;
		return ns;
	}
	return NULL;
}

stmt *
stmt_atom(backend *be, atom *a)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = newStmt(mb, calcRef, atom_type(a)->type->base.name);

	if (!q)
		return NULL;
	if (atom_null(a)) {
		q = pushNil(mb, q, atom_type(a)->type->localtype);
	} else {
		int k;
		if((k = constantAtom(be, mb, a)) == -1) {
			freeInstruction(q);
			return NULL;
		}

		q = pushArgument(mb, q, k);
	}
	/* digits of the result timestamp/daytime */
	if (EC_TEMP_FRAC(atom_type(a)->type->eclass))
		q = pushInt(mb, q, atom_type(a)->digits);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_atom);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->op4.aval = a;
		s->key = 1;		/* values are also unique */
		s->q = q;
		s->nr = getDestVar(q);
		return s;
	}
	return NULL;
}

stmt *
stmt_genselect(backend *be, stmt *lops, stmt *rops, sql_subfunc *f, stmt *sub, int anti)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *mod, *op;
	node *n;
	int k;

	if (backend_create_subfunc(be, f, NULL) < 0)
		return NULL;
	op = sql_func_imp(f->func);
	mod = sql_func_mod(f->func);

	if (rops->nrcols >= 1) {
		bit need_not = FALSE;

		q = newStmt(mb, malRef, multiplexRef);
		setVarType(mb, getArg(q, 0), newBatType(TYPE_bit));
		setVarUDFtype(mb, getArg(q, 0));
		q = pushStr(mb, q, convertMultiplexMod(mod, op));
		q = pushStr(mb, q, convertMultiplexFcn(op));
		for (n = lops->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;
	
			q = pushArgument(mb, q, op->nr);
		}
		for (n = rops->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;
	
			q = pushArgument(mb, q, op->nr);
		}
		k = getDestVar(q);

		q = newStmt(mb, algebraRef, selectRef);
		q = pushArgument(mb, q, k);
		if (sub)
			q = pushArgument(mb, q, sub->nr);
		q = pushBit(mb, q, !need_not);
		q = pushBit(mb, q, !need_not);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, anti);
	} else {
		node *n;

		op = sa_strconcat(be->mvc->sa, op, selectRef);
		q = newStmt(mb, mod, convertOperator(op));
		// push pointer to the SQL structure into the MAL call
		// allows getting argument names for example
		if (LANG_EXT(f->func->lang))
			q = pushPtr(mb, q, f); // nothing to see here, please move along
		// f->query contains the R code to be run
		if (f->func->lang == FUNC_LANG_R || f->func->lang >= FUNC_LANG_PY)
			q = pushStr(mb, q, f->func->query);

		for (n = lops->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;

			q = pushArgument(mb, q, op->nr);
		}
		/* candidate lists */
		if (sub)
			q = pushArgument(mb, q, sub->nr);
		else
			q = pushNil(mb, q, TYPE_bat); 

		for (n = rops->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;

			q = pushArgument(mb, q, op->nr);
		}

		q = pushBit(mb, q, anti);
	}

	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_uselect);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->op1 = lops;
		s->op2 = rops;
		s->op3 = sub;
		s->flag = cmp_filter;
		s->nrcols = (lops->nrcols == 2) ? 2 : 1;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_uselect(backend *be, stmt *op1, stmt *op2, comp_type cmptype, stmt *sub, int anti)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	int l, r;

	if (op1->nr < 0 || op2->nr < 0 || (sub && sub->nr < 0))
		return NULL;
	l = op1->nr;
	r = op2->nr;

	if (op2->nrcols >= 1) {
		bit need_not = FALSE;
		const char *mod = calcRef;
		const char *op = "=";
		int k;

		switch (cmptype) {
		case cmp_equal:
			op = "=";
			break;
		case cmp_notequal:
			op = "!=";
			break;
		case cmp_lt:
			op = "<";
			break;
		case cmp_lte:
			op = "<=";
			break;
		case cmp_gt:
			op = ">";
			break;
		case cmp_gte:
			op = ">=";
			break;
		default:
			showException(GDKout, SQL, "sql", "Unknown operator");
		}

		if ((q = multiplex2(mb, mod, convertOperator(op), l, r, TYPE_bit)) == NULL) 
			return NULL;
		k = getDestVar(q);

		q = newStmt(mb, algebraRef, selectRef);
		q = pushArgument(mb, q, k);
		if (sub)
			q = pushArgument(mb, q, sub->nr);
		q = pushBit(mb, q, !need_not);
		q = pushBit(mb, q, !need_not);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, anti);
		if (q == NULL)
			return NULL;
		k = getDestVar(q);
	} else {
		assert (cmptype != cmp_filter);
		q = newStmt(mb, algebraRef, thetaselectRef);
		q = pushArgument(mb, q, l);
		if (sub)
			q = pushArgument(mb, q, sub->nr);
		q = pushArgument(mb, q, r);
		switch (cmptype) {
		case cmp_equal:
			q = pushStr(mb, q, "==");
			break;
		case cmp_notequal:
			q = pushStr(mb, q, "!=");
			break;
		case cmp_lt:
			q = pushStr(mb, q, "<");
			break;
		case cmp_lte:
			q = pushStr(mb, q, "<=");
			break;
		case cmp_gt:
			q = pushStr(mb, q, ">");
			break;
		case cmp_gte:
			q = pushStr(mb, q, ">=");
			break;
		default:
			showException(GDKout, SQL, "sql", "SQL2MAL: error impossible select compare\n");
			if (q)
				freeInstruction(q);
			q = NULL;
		}
		if (q == NULL)
			return NULL;
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_uselect);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->op1 = op1;
		s->op2 = op2;
		s->op3 = sub;
		s->flag = cmptype;
		s->nrcols = (op1->nrcols == 2) ? 2 : 1;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

/*
static int
range_join_convertable(stmt *s, stmt **base, stmt **L, stmt **H)
{
	int ls = 0, hs = 0;
	stmt *l = NULL, *h = NULL;
	stmt *bl = s->op2, *bh = s->op3;
	int tt = tail_type(s->op2)->type->localtype;

#ifdef HAVE_HGE
	if (tt > TYPE_hge)
#else
	if (tt > TYPE_lng)
#endif
		return 0;
	if (s->op2->type == st_Nop && list_length(s->op2->op1->op4.lval) == 2) {
		bl = s->op2->op1->op4.lval->h->data;
		l = s->op2->op1->op4.lval->t->data;
	}
	if (s->op3->type == st_Nop && list_length(s->op3->op1->op4.lval) == 2) {
		bh = s->op3->op1->op4.lval->h->data;
		h = s->op3->op1->op4.lval->t->data;
	}

	if (((ls = (l && strcmp(s->op2->op4.funcval->func->base.name, "sql_sub") == 0 && l->nrcols == 0)) || (hs = (h && strcmp(s->op3->op4.funcval->func->base.name, "sql_add") == 0 && h->nrcols == 0))) && (ls || hs) && bl == bh) {
		*base = bl;
		*L = l;
		*H = h;
		return 1;
	}
	return 0;
}

static int
argumentZero(MalBlkPtr mb, int tpe)
{
	ValRecord cst;
	str msg;

	cst.vtype = TYPE_int;
	cst.val.ival = 0;
	msg = convertConstant(tpe, &cst);
	if( msg)
		freeException(msg); // will not be called
	return defConstant(mb, tpe, &cst);
}
*/


static InstrPtr
select2_join2(backend *be, stmt *op1, stmt *op2, stmt *op3, int cmp, stmt *sub, int anti, int swapped, int type)
{
	MalBlkPtr mb = be->mb;
	InstrPtr r, p, q;
	int l;
	const char *cmd = (type == st_uselect2) ? selectRef : rangejoinRef;

	if (op1->nr < 0 && (sub && sub->nr < 0))
		return NULL;
	l = op1->nr;
	if ((op2->nrcols > 0 || op3->nrcols > 0) && (type == st_uselect2)) {
		int k, symmetric = cmp&CMP_SYMMETRIC;
		const char *mod = calcRef;
		const char *OP1 = "<", *OP2 = "<";

		if (op2->nr < 0 || op3->nr < 0)
			return NULL;

		if (cmp & 1)
			OP1 = "<=";
		if (cmp & 2)
			OP2 = "<=";

		if (cmp&1 && cmp&2) {
			if (symmetric)
				p = newStmt(mb, batcalcRef, betweensymmetricRef);
			else
				p = newStmt(mb, batcalcRef, betweenRef);
			p = pushArgument(mb, p, l);
			p = pushArgument(mb, p, op2->nr);
			p = pushArgument(mb, p, op3->nr);
			k = getDestVar(p);
		} else {
			if ((q = multiplex2(mb, mod, convertOperator(OP1), l, op2->nr, TYPE_bit)) == NULL)
				return NULL;

			if ((r = multiplex2(mb, mod, convertOperator(OP2), l, op3->nr, TYPE_bit)) == NULL)
				return NULL;
			p = newStmt(mb, batcalcRef, andRef);
			p = pushArgument(mb, p, getDestVar(q));
			p = pushArgument(mb, p, getDestVar(r));
			if (p == NULL)
				return NULL;
			k = getDestVar(p);
		}

		q = newStmt(mb, algebraRef, selectRef);
		q = pushArgument(mb, q, k);
		if (sub)
			q = pushArgument(mb, q, sub->nr);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, (anti)?TRUE:FALSE);
		if (q == NULL)
			return NULL;
	} else {
		/* if st_join2 try to convert to bandjoin */
		/* ie check if we subtract/add a constant, to the
	   	same column */
		/* move this optimization into the relational phase! */
	/*
		stmt *base, *low = NULL, *high = NULL;
		if (type == st_join2 && range_join_convertable(s, &base, &low, &high)) {
			int tt = tail_type(base)->type->localtype;
	
			if ((rs = _dumpstmt(sql, mb, base)) < 0)
				return -1;
			if (low) {
				if ((r1 = _dumpstmt(sql, mb, low)) < 0)
					return -1;
			} else
				r1 = argumentZero(mb, tt);
			if (high) {
				if ((r2 = _dumpstmt(sql, mb, high)) < 0)
					return -1;
			} else
				r2 = argumentZero(mb, tt);
			cmd = bandjoinRef;
		}
	*/

		int r1 = op2->nr;
		int r2 = op3->nr;
		int rs = 0;
		/*
		if (!rs) {
			r1 = op2->nr;
			r2 = op3->nr;
		}
		*/
		q = newStmt(mb, algebraRef, cmd);
		if (type == st_join2)
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, l);
		if (sub) /* only for uselect2 */
			q = pushArgument(mb, q, sub->nr);
		if (rs) {
			q = pushArgument(mb, q, rs);
		} else {
			q = pushArgument(mb, q, r1);
			q = pushArgument(mb, q, r2);
		}
		if (type == st_join2) {
			q = pushNil(mb, q, TYPE_bat);
			q = pushNil(mb, q, TYPE_bat);
		}

		switch (cmp & 3) {
		case 0:
			q = pushBit(mb, q, FALSE);
			q = pushBit(mb, q, FALSE);
			break;
		case 1:
			q = pushBit(mb, q, TRUE);
			q = pushBit(mb, q, FALSE);
			break;
		case 2:
			q = pushBit(mb, q, FALSE);
			q = pushBit(mb, q, TRUE);
			break;
		case 3:
			q = pushBit(mb, q, TRUE);
			q = pushBit(mb, q, TRUE);
			break;
		}
		if (type == st_join2)
			q = pushNil(mb, q, TYPE_lng); /* estimate */
		if (type == st_uselect2) {
			q = pushBit(mb, q, anti);
			if (q == NULL)
				return NULL;
		}
		if (q == NULL)
			return NULL;
		if (swapped) {
			InstrPtr r = newInstruction(mb,  NULL, NULL);
			if (r == NULL)
				return NULL;
			getArg(r, 0) = newTmpVariable(mb, TYPE_any);
			r = pushReturn(mb, r, newTmpVariable(mb, TYPE_any));
			r = pushArgument(mb, r, getArg(q,1));
			r = pushArgument(mb, r, getArg(q,0)); 
			pushInstruction(mb, r);
			q = r;
		}
	}
	return q;
}

stmt *
stmt_uselect2(backend *be, stmt *op1, stmt *op2, stmt *op3, int cmp, stmt *sub, int anti)
{
	InstrPtr q = select2_join2(be, op1, op2, op3, cmp, sub, anti, 0, st_uselect2);

	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_uselect2);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->op1 = op1;
		s->op2 = op2;
		s->op3 = op3;
		s->op4.stval = sub;
		s->flag = cmp;
		s->nrcols = (op1->nrcols == 2) ? 2 : 1;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_tunion(backend *be, stmt *op1, stmt *op2)
{
	InstrPtr q = NULL;
	MalBlkPtr mb = be->mb;

	q = dump_2(mb, batRef, mergecandRef, op1, op2);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_tunion);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}
	
		s->op1 = op1;
		s->op2 = op2;
		s->nrcols = op1->nrcols;
		s->key = op1->key;
		s->aggr = op1->aggr;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_tdiff(backend *be, stmt *op1, stmt *op2)
{
	InstrPtr q = NULL;
	MalBlkPtr mb = be->mb;

	if (op1->nr < 0 || op2->nr < 0)
		return NULL;
	q = newStmt(mb, algebraRef, differenceRef);
	q = pushArgument(mb, q, op1->nr); /* left */
	q = pushArgument(mb, q, op2->nr); /* right */
	q = pushNil(mb, q, TYPE_bat); /* left candidate */
	q = pushNil(mb, q, TYPE_bat); /* right candidate */
	q = pushBit(mb, q, FALSE);    /* nil matches */
	q = pushNil(mb, q, TYPE_lng); /* estimate */

	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_tdiff);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->op1 = op1;
		s->op2 = op2;
		s->nrcols = op1->nrcols;
		s->key = op1->key;
		s->aggr = op1->aggr;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_tinter(backend *be, stmt *op1, stmt *op2)
{
	InstrPtr q = NULL;
	MalBlkPtr mb = be->mb;

	if (op1->nr < 0 || op2->nr < 0)
		return NULL;
	q = newStmt(mb, algebraRef, intersectRef);
	q = pushArgument(mb, q, op1->nr); /* left */
	q = pushArgument(mb, q, op2->nr); /* right */
	q = pushNil(mb, q, TYPE_bat); /* left candidate */
	q = pushNil(mb, q, TYPE_bat); /* right candidate */
	q = pushBit(mb, q, FALSE);    /* nil matches */
	q = pushNil(mb, q, TYPE_lng); /* estimate */

	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_tinter);

		s->op1 = op1;
		s->op2 = op2;
		s->nrcols = op1->nrcols;
		s->key = op1->key;
		s->aggr = op1->aggr;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_join(backend *be, stmt *op1, stmt *op2, int anti, comp_type cmptype)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	int left = (cmptype == cmp_left);
	const char *sjt = "join";

	(void)anti;

	if (left) {
		cmptype = cmp_equal;
		sjt = "leftjoin";
	}
	if (op1->nr < 0 || op2->nr < 0)
		return NULL;

	switch (cmptype) {
	case cmp_equal:
		q = newStmt(mb, algebraRef, sjt);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		q = pushNil(mb, q, TYPE_bat);
		q = pushNil(mb, q, TYPE_bat);
		q = pushBit(mb, q, FALSE);
		q = pushNil(mb, q, TYPE_lng);
		if (q == NULL)
			return NULL;
		break;
	case cmp_equal_nil: /* nil == nil */
		q = newStmt(mb, algebraRef, sjt);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		q = pushNil(mb, q, TYPE_bat);
		q = pushNil(mb, q, TYPE_bat);
		q = pushBit(mb, q, TRUE);
		q = pushNil(mb, q, TYPE_lng);
		if (q == NULL)
			return NULL;
		break;
	case cmp_notequal:
		q = newStmt(mb, algebraRef, antijoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		q = pushNil(mb, q, TYPE_bat);
		q = pushNil(mb, q, TYPE_bat);
		q = pushBit(mb, q, FALSE);
		q = pushNil(mb, q, TYPE_lng);
		if (q == NULL)
			return NULL;
		break;
	case cmp_lt:
	case cmp_lte:
	case cmp_gt:
	case cmp_gte:
		q = newStmt(mb, algebraRef, thetajoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		q = pushNil(mb, q, TYPE_bat);
		q = pushNil(mb, q, TYPE_bat);
		if (cmptype == cmp_lt)
			q = pushInt(mb, q, -1);
		else if (cmptype == cmp_lte)
			q = pushInt(mb, q, -2);
		else if (cmptype == cmp_gt)
			q = pushInt(mb, q, 1);
		else if (cmptype == cmp_gte)
			q = pushInt(mb, q, 2);
		q = pushBit(mb, q, TRUE);
		q = pushNil(mb, q, TYPE_lng);
		if (q == NULL)
			return NULL;
		break;
	case cmp_all:	/* aka cross table */
		q = newStmt(mb, algebraRef, crossRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		if (q == NULL)
			return NULL;
		break;
	case cmp_joined:
		q = op1->q;
		break;
	default:
		showException(GDKout, SQL, "sql", "SQL2MAL: error impossible\n");
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_join);

		s->op1 = op1;
		s->op2 = op2;
		s->flag = cmptype;
		s->key = 0;
		s->nrcols = 2;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

static InstrPtr 
stmt_project_join(backend *be, stmt *op1, stmt *op2, stmt *ins) 
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (op1->nr < 0 || op2->nr < 0)
		return NULL;
	/* delta bat */
	if (ins) {
		int uval = getArg(op2->q, 1);

		if (ins->nr < 0)
			return NULL;
		q = newStmt(mb, sqlRef, deltaRef);
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		q = pushArgument(mb, q, uval);
		q = pushArgument(mb, q, ins->nr);
	} else {
		/* projections, ie left is void headed */
		q = newStmt(mb, algebraRef, projectionRef);
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		if (q == NULL)
			return NULL;
		/*
		if (s->key) {
			q = newStmt(mb, batRef, putName("setKey"));
			q = pushArgument(mb, q, s->nr);
			q = pushBit(mb, q, TRUE);
		}
		*/
		
	}
	return q;
}

stmt *
stmt_project(backend *be, stmt *op1, stmt *op2)
{
	InstrPtr q = stmt_project_join(be, op1, op2, NULL);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_join);

		s->op1 = op1;
		s->op2 = op2;
		s->flag = cmp_project;
		s->key = 0;
		s->nrcols = 2;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_project_delta(backend *be, stmt *col, stmt *upd, stmt *ins)
{
	InstrPtr q = stmt_project_join(be, col, upd, ins);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_join);

		s->op1 = col;
		s->op2 = upd;
		s->op3 = ins;
		s->flag = cmp_project;
		s->key = 0;
		s->nrcols = 2;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_left_project(backend *be, stmt *op1, stmt *op2, stmt *op3)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	if (op1->nr < 0 || op2->nr < 0 || op3->nr < 0)
		return NULL;

	q = newStmt(mb, sqlRef, projectRef);
	q = pushArgument(mb, q, op1->nr);
	q = pushArgument(mb, q, op2->nr);
	q = pushArgument(mb, q, op3->nr);

	if (q){
		stmt *s = stmt_create(be->mvc->sa, st_join);

		s->op1 = op1;
		s->op2 = op2;
		s->op3 = op3;
		s->flag = cmp_left_project;
		s->key = 0;
		s->nrcols = 2;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_join2(backend *be, stmt *l, stmt *ra, stmt *rb, int cmp, int anti, int swapped)
{
	InstrPtr q = select2_join2(be, l, ra, rb, cmp, NULL, anti, swapped, st_join2);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_join2);

		s->op1 = l;
		s->op2 = ra;
		s->op3 = rb;
		s->flag = cmp;
		s->nrcols = 2;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	} 
	return NULL;
}

stmt *
stmt_genjoin(backend *be, stmt *l, stmt *r, sql_subfunc *op, int anti, int swapped)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *mod, *fimp;
	node *n;

	(void)anti;
	if (backend_create_subfunc(be, op, NULL) < 0)
		return NULL;
	mod = sql_func_mod(op->func);
	fimp = sql_func_imp(op->func);
	fimp = sa_strconcat(be->mvc->sa, fimp, "join");

	/* filter qualifying tuples, return oids of h and tail */
	q = newStmt(mb, mod, fimp);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	for (n = l->op4.lval->h; n; n = n->next) {
		stmt *op = n->data;

		q = pushArgument(mb, q, op->nr);
	}

	for (n = r->op4.lval->h; n; n = n->next) {
		stmt *op = n->data;

		q = pushArgument(mb, q, op->nr);
	}
	q = pushNil(mb, q, TYPE_bat); /* candidate lists */
	q = pushNil(mb, q, TYPE_bat); /* candidate lists */
	q = pushBit(mb, q, TRUE);     /* nil_matches */
	q = pushNil(mb, q, TYPE_lng); /* estimate */

	if (swapped) {
		InstrPtr r = newInstruction(mb,  NULL, NULL);
		if (r == NULL)
			return NULL;
		getArg(r, 0) = newTmpVariable(mb, TYPE_any);
		r = pushReturn(mb, r, newTmpVariable(mb, TYPE_any));
		r = pushArgument(mb, r, getArg(q,1));
		r = pushArgument(mb, r, getArg(q,0)); 
		pushInstruction(mb, r);
		q = r;
	}

	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_joinN);

		s->op1 = l;
		s->op2 = r;
		s->op4.funcval = op;
		s->nrcols = 2;
		if (swapped)
			s->flag |= SWAPPED;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}	
	return NULL;
}

stmt *
stmt_rs_column(backend *be, stmt *rs, int i, sql_subtype *tpe)
{
	InstrPtr q = NULL;

	if (rs->nr < 0)
		return NULL;
	q = rs->q;
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_rs_column);

		s->op1 = rs;
		s->op4.typeval = *tpe;
		s->flag = i;
		s->nrcols = 1;
		s->key = 0;
		s->q = q;
		s->nr = getArg(q, s->flag);
		return s;
	}
	return NULL;
}

/*
 * The dump_header produces a sequence of instructions for
 * the front-end to prepare presentation of a result table.
 *
 * A secondary scheme is added to assemblt all information
 * in columns first. Then it can be returned to the environment.
 */
#define NEWRESULTSET

#define meta(Id,Tpe) \
q = newStmt(mb, batRef, newRef);\
q= pushType(mb,q, Tpe);\
Id = getArg(q,0); \
list = pushArgument(mb,list,Id);

#define metaInfo(Id,Tpe,Val)\
p = newStmt(mb, batRef, appendRef);\
p = pushArgument(mb,p, Id);\
p = push##Tpe(mb,p, Val);\
Id = getArg(p,0);


static int
dump_export_header(mvc *sql, MalBlkPtr mb, list *l, int file, const char * format, const char * sep,const char * rsep,const char * ssep,const char * ns)
{
	node *n;
	InstrPtr q = NULL;
	int ret = -1;
	// gather the meta information
	int tblId, nmeId, tpeId, lenId, scaleId, k;
	InstrPtr p= NULL, list;

	list = newInstruction(mb, sqlRef, export_tableRef);
	getArg(list,0) = newTmpVariable(mb,TYPE_int);
	if( file >= 0){
		list  = pushArgument(mb, list, file);
		list  = pushStr(mb, list, format);
		list  = pushStr(mb, list, sep);
		list  = pushStr(mb, list, rsep);
		list  = pushStr(mb, list, ssep);
		list  = pushStr(mb, list, ns);
	}
	k = list->argc;
	meta(tblId,TYPE_str);
	meta(nmeId,TYPE_str);
	meta(tpeId,TYPE_str);
	meta(lenId,TYPE_int);
	meta(scaleId,TYPE_int);

	for (n = l->h; n; n = n->next) {
		stmt *c = n->data;
		sql_subtype *t = tail_type(c);
		const char *tname = table_name(sql->sa, c);
		const char *sname = schema_name(sql->sa, c);
		const char *_empty = "";
		const char *tn = (tname) ? tname : _empty;
		const char *sn = (sname) ? sname : _empty;
		const char *cn = column_name(sql->sa, c);
		const char *ntn = sql_escape_ident(tn);
		const char *nsn = sql_escape_ident(sn);
		size_t fqtnl;
		char *fqtn = NULL;

		if (ntn && nsn && (fqtnl = strlen(ntn) + 1 + strlen(nsn) + 1) ){
			fqtn = NEW_ARRAY(char, fqtnl);
			if(fqtn) {
				snprintf(fqtn, fqtnl, "%s.%s", nsn, ntn);
				metaInfo(tblId, Str, fqtn);
				metaInfo(nmeId, Str, cn);
				metaInfo(tpeId, Str, (t->type->localtype == TYPE_void ? "char" : t->type->sqlname));
				metaInfo(lenId, Int, t->digits);
				metaInfo(scaleId, Int, t->scale);
				list = pushArgument(mb, list, c->nr);
				_DELETE(fqtn);
			} else
				q = NULL;
		} else
			q = NULL;
		c_delete(ntn);
		c_delete(nsn);
		if (q == NULL)
			return -1;
	}
	// add the correct variable ids
	getArg(list,k++) = tblId;
	getArg(list,k++) = nmeId;
	getArg(list,k++) = tpeId;
	getArg(list,k++) = lenId;
	getArg(list,k) = scaleId;
	ret = getArg(list,0);
	pushInstruction(mb,list);
	return ret;
}


stmt *
stmt_export(backend *be, stmt *t, const char *sep, const char *rsep, const char *ssep, const char *null_string, stmt *file)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	int fnr;
	list *l;

	if (t->nr < 0)
		return NULL;
	l = t->op4.lval;
	if (file) {
		if (file->nr < 0)
			return NULL;
		fnr = file->nr;
        } else {
		q = newAssignment(mb);
		q = pushStr(mb,q,"stdout");
		fnr = getArg(q,0);
	}
	if (t->type == st_list) {
		if (dump_export_header(be->mvc, mb, l, fnr, "csv", sep, rsep, ssep, null_string) < 0)
			return NULL;
	} else {
		q = newStmt(mb, sqlRef, raiseRef);
		q = pushStr(mb, q, "not a valid output list\n");
		if (q == NULL)
			return NULL;
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_export);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = t;
		s->op2 = file;
		s->q = q;
		s->nr = 1;
		return s;
	}
	return NULL;
}

stmt *
stmt_trans(backend *be, int type, stmt *chain, stmt *name)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (chain->nr < 0)
		return NULL;

	switch(type){
	case DDL_RELEASE:
		q = newStmt(mb, sqlRef, transaction_releaseRef);
		break;
	case DDL_COMMIT:
		q = newStmt(mb, sqlRef, transaction_commitRef);
		break;
	case DDL_ROLLBACK:
		q = newStmt(mb, sqlRef, transaction_rollbackRef);
		break;
	case DDL_TRANS:
		q = newStmt(mb, sqlRef, transaction_beginRef);
		break;
	default:
		showException(GDKout, SQL, "sql.trans", "transaction unknown type");
	}
	q = pushArgument(mb, q, chain->nr);
	if (name)
		q = pushArgument(mb, q, name->nr);
	else
		q = pushNil(mb, q, TYPE_str);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_trans);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = chain;
		s->op2 = name;
		s->flag = type;
		s->q = q;
		s->nr = getDestVar(q);
		return s;
	}
	return NULL;
}

stmt *
stmt_catalog(backend *be, int type, stmt *args)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	node *n;
	int if_exists =0;

	if (args->nr < 0)
		return NULL;

	/* cast them into properly named operations */
	switch(type){
	case DDL_CREATE_SEQ:	q = newStmt(mb, sqlcatalogRef, create_seqRef); break;
	case DDL_ALTER_SEQ:	q = newStmt(mb, sqlcatalogRef, alter_seqRef); break;
	case DDL_DROP_SEQ:	q = newStmt(mb, sqlcatalogRef, drop_seqRef); break;
	case DDL_CREATE_SCHEMA:	q = newStmt(mb, sqlcatalogRef, create_schemaRef); break;
	case DDL_DROP_SCHEMA_IF_EXISTS: if_exists =1;
		/* fall through */
	case DDL_DROP_SCHEMA:	q = newStmt(mb, sqlcatalogRef, drop_schemaRef); break;
	case DDL_CREATE_TABLE:	q = newStmt(mb, sqlcatalogRef, create_tableRef); break;
	case DDL_CREATE_VIEW:	q = newStmt(mb, sqlcatalogRef, create_viewRef); break;
	case DDL_DROP_TABLE_IF_EXISTS: if_exists =1;
		/* fall through */
	case DDL_DROP_TABLE:	q = newStmt(mb, sqlcatalogRef, drop_tableRef); break;
	case DDL_DROP_VIEW_IF_EXISTS: if_exists = 1;
		/* fall through */
	case DDL_DROP_VIEW:	q = newStmt(mb, sqlcatalogRef, drop_viewRef); break;
	case DDL_DROP_CONSTRAINT:	q = newStmt(mb, sqlcatalogRef, drop_constraintRef); break;
	case DDL_ALTER_TABLE:	q = newStmt(mb, sqlcatalogRef, alter_tableRef); break;
	case DDL_CREATE_TYPE:	q = newStmt(mb, sqlcatalogRef, create_typeRef); break;
	case DDL_DROP_TYPE:	q = newStmt(mb, sqlcatalogRef, drop_typeRef); break;
	case DDL_GRANT_ROLES:	q = newStmt(mb, sqlcatalogRef, grant_rolesRef); break;
	case DDL_REVOKE_ROLES:	q = newStmt(mb, sqlcatalogRef, revoke_rolesRef); break;
	case DDL_GRANT:		q = newStmt(mb, sqlcatalogRef, grantRef); break;
	case DDL_REVOKE:	q = newStmt(mb, sqlcatalogRef, revokeRef); break;
	case DDL_GRANT_FUNC:	q = newStmt(mb, sqlcatalogRef, grant_functionRef); break;
	case DDL_REVOKE_FUNC:	q = newStmt(mb, sqlcatalogRef, revoke_functionRef); break;
	case DDL_CREATE_USER:	q = newStmt(mb, sqlcatalogRef, create_userRef); break;
	case DDL_DROP_USER:		q = newStmt(mb, sqlcatalogRef, drop_userRef); break;
	case DDL_ALTER_USER:	q = newStmt(mb, sqlcatalogRef, alter_userRef); break;
	case DDL_RENAME_USER:	q = newStmt(mb, sqlcatalogRef, rename_userRef); break;
	case DDL_CREATE_ROLE:	q = newStmt(mb, sqlcatalogRef, create_roleRef); break;
	case DDL_DROP_ROLE:		q = newStmt(mb, sqlcatalogRef, drop_roleRef); break;
	case DDL_DROP_INDEX:	q = newStmt(mb, sqlcatalogRef, drop_indexRef); break;
	case DDL_DROP_FUNCTION:	q = newStmt(mb, sqlcatalogRef, drop_functionRef); break;
	case DDL_CREATE_FUNCTION:	q = newStmt(mb, sqlcatalogRef, create_functionRef); break;
	case DDL_CREATE_TRIGGER:	q = newStmt(mb, sqlcatalogRef, create_triggerRef); break;
	case DDL_DROP_TRIGGER:	q = newStmt(mb, sqlcatalogRef, drop_triggerRef); break;
	case DDL_ALTER_TABLE_ADD_TABLE:	q = newStmt(mb, sqlcatalogRef, alter_add_tableRef); break;
	case DDL_ALTER_TABLE_DEL_TABLE:	q = newStmt(mb, sqlcatalogRef, alter_del_tableRef); break;
	case DDL_ALTER_TABLE_SET_ACCESS:q = newStmt(mb, sqlcatalogRef, alter_set_tableRef); break;
	default:
		showException(GDKout, SQL, "sql", "catalog operation unknown\n");
	}
	// pass all arguments as before
	for (n = args->op4.lval->h; n; n = n->next) {
		stmt *c = n->data;

		q = pushArgument(mb, q, c->nr);
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_catalog);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		if( if_exists)
			pushInt(mb,q,1);
		s->op1 = args;
		s->flag = type;
		s->q = q;
		s->nr = getDestVar(q);
		return s;
	}
	return NULL;
}

void
stmt_set_nrcols(stmt *s)
{
	int nrcols = 0;
	int key = 1;
	node *n;
	list *l = s->op4.lval;

	assert(s->type == st_list);
	for (n = l->h; n; n = n->next) {
		stmt *f = n->data;

		if (!f)
			continue;
		if (f->nrcols > nrcols)
			nrcols = f->nrcols;
		key &= f->key;
	}
	s->nrcols = nrcols;
	s->key = key;
}

stmt *
stmt_list(backend *be, list *l)
{
	stmt *s = stmt_create(be->mvc->sa, st_list);
	if(!s) {
		return NULL;
	}
	s->op4.lval = l;
	stmt_set_nrcols(s);
	return s;
}

static InstrPtr
dump_header(mvc *sql, MalBlkPtr mb, stmt *s, list *l)
{
	node *n;
	InstrPtr q = NULL;
	// gather the meta information
	int tblId, nmeId, tpeId, lenId, scaleId, k;
	InstrPtr p = NULL, list;

	list = newInstruction(mb,sqlRef, resultSetRef);
	if(!list) {
		return NULL;
	}
	getArg(list,0) = newTmpVariable(mb,TYPE_int);
	k = list->argc;
	meta(tblId,TYPE_str);
	meta(nmeId,TYPE_str);
	meta(tpeId,TYPE_str);
	meta(lenId,TYPE_int);
	meta(scaleId,TYPE_int);

	(void) s;

	for (n = l->h; n; n = n->next) {
		stmt *c = n->data;
		sql_subtype *t = tail_type(c);
		const char *tname = table_name(sql->sa, c);
		const char *sname = schema_name(sql->sa, c);
		const char *_empty = "";
		const char *tn = (tname) ? tname : _empty;
		const char *sn = (sname) ? sname : _empty;
		const char *cn = column_name(sql->sa, c);
		const char *ntn = sql_escape_ident(tn);
		const char *nsn = sql_escape_ident(sn);
		size_t fqtnl;
		char *fqtn = NULL;

		if (ntn && nsn && (fqtnl = strlen(ntn) + 1 + strlen(nsn) + 1) ){
			fqtn = NEW_ARRAY(char, fqtnl);
			if(fqtn) {
				snprintf(fqtn, fqtnl, "%s.%s", nsn, ntn);
				metaInfo(tblId,Str,fqtn);
				metaInfo(nmeId,Str,cn);
				metaInfo(tpeId,Str,(t->type->localtype == TYPE_void ? "char" : t->type->sqlname));
				metaInfo(lenId,Int,t->digits);
				metaInfo(scaleId,Int,t->scale);
				list = pushArgument(mb,list,c->nr);
				_DELETE(fqtn);
			} else
				q = NULL;
		} else
			q = NULL;
		c_delete(ntn);
		c_delete(nsn);
		if (q == NULL)
			return NULL;
	}
	// add the correct variable ids
	getArg(list,k++) = tblId;
	getArg(list,k++) = nmeId;
	getArg(list,k++) = tpeId;
	getArg(list,k++) = lenId;
	getArg(list,k) = scaleId;
	pushInstruction(mb,list);
	return list;
}

stmt *
stmt_output(backend *be, stmt *lst)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	list *l = lst->op4.lval;

	int cnt = list_length(l), ok = 0;
	stmt *first;
	node *n;

	n = l->h;
	first = n->data;

	/* single value result, has a fast exit */
	if (cnt == 1 && first->nrcols <= 0 ){
		stmt *c = n->data;
		sql_subtype *t = tail_type(c);
		const char *tname = table_name(be->mvc->sa, c);
		const char *sname = schema_name(be->mvc->sa, c);
		const char *_empty = "";
		const char *tn = (tname) ? tname : _empty;
		const char *sn = (sname) ? sname : _empty;
		const char *cn = column_name(be->mvc->sa, c);
		const char *ntn = sql_escape_ident(tn);
		const char *nsn = sql_escape_ident(sn);
		size_t fqtnl;
		char *fqtn = NULL;

		if(ntn && nsn) {
			fqtnl = strlen(ntn) + 1 + strlen(nsn) + 1;
			fqtn = NEW_ARRAY(char, fqtnl);
			if(fqtn) {
				ok = 1;
				snprintf(fqtn, fqtnl, "%s.%s", nsn, ntn);

				q = newStmt(mb, sqlRef, resultSetRef);
				getArg(q,0) = newTmpVariable(mb,TYPE_int);
				if (q) {
					q = pushStr(mb, q, fqtn);
					q = pushStr(mb, q, cn);
					q = pushStr(mb, q, t->type->localtype == TYPE_void ? "char" : t->type->sqlname);
					q = pushInt(mb, q, t->digits);
					q = pushInt(mb, q, t->scale);
					q = pushInt(mb, q, t->type->eclass);
					q = pushArgument(mb, q, c->nr);
				}
			}
		}
		c_delete(ntn);
		c_delete(nsn);
		_DELETE(fqtn);
		if(!ok)
			return NULL;
	} else {
		if ((q = dump_header(be->mvc, mb, lst, l)) == NULL) 
			return NULL;
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_output);

		s->op1 = lst;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_affected_rows(backend *be, stmt *l)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (l->nr < 0)
		return NULL;
	q = newStmt(mb, sqlRef, affectedRowsRef);
	q = pushArgument(mb, q, be->mvc_var);
	if (q == NULL)
		return NULL;
	getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
	q = pushArgument(mb, q, l->nr);
	if (q == NULL)
		return NULL;
	be->mvc_var = getDestVar(q);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_affected_rows);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = l;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_append(backend *be, stmt *c, stmt *a)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (c->nr < 0 || a->nr < 0)
		return NULL;
	q = newStmt(mb, batRef, appendRef);
	q = pushArgument(mb, q, c->nr);
	q = pushArgument(mb, q, a->nr);
	q = pushBit(mb, q, TRUE);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_append);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = c;
		s->op2 = a;
		s->nrcols = c->nrcols;
		s->key = c->key;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_table_clear(backend *be, sql_table *t)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
       
	if (!t->s && t->data) { /* declared table */
		int *l = t->data; 
		int cnt = list_length(t->columns.set)+1, i;

		for (i = 0; i < cnt; i++) {
			q = newStmt(mb, batRef, "clear");
			q = pushArgument(mb, q, l[i]);
			l[i] = getDestVar(q);
		}
	} else {
		q = newStmt(mb, sqlRef, clear_tableRef);
		q = pushSchema(mb, q, t);
		q = pushStr(mb, q, t->base.name);
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_table_clear);

		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op4.tval = t;
		s->nrcols = 0;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_exception(backend *be, stmt *cond, const char *errstr, int errcode)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (cond->nr < 0) 
		return NULL;

	/* if(bit(l)) { error(r);}  ==raising an exception */
	q = newStmt(mb, sqlRef, assertRef);
	q = pushArgument(mb, q, cond->nr);
	q = pushStr(mb, q, errstr);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_exception);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		assert(cond);
		s->op1 = cond;
		(void)errcode;
		s->nrcols = 0;
		s->q = q;
		s->nr = getDestVar(q);
		return s;
	}
	return NULL;
}

stmt *
stmt_convert(backend *be, stmt *v, sql_subtype *f, sql_subtype *t)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *convert = t->type->base.name;
	/* convert types and make sure they are rounded up correctly */

	if (v->nr < 0)
		return NULL;

	if (t->type->localtype == f->type->localtype && (t->type->eclass == f->type->eclass || (EC_VARCHAR(f->type->eclass) && EC_VARCHAR(t->type->eclass))) && !EC_INTERVAL(f->type->eclass) && f->type->eclass != EC_DEC && (t->digits == 0 || f->digits == t->digits)) {
		return v;
	}

	/* external types have sqlname convert functions,
	   these can generate errors (fromstr cannot) */
	if (t->type->eclass == EC_EXTERNAL)
		convert = t->type->sqlname;

	if (t->type->eclass == EC_MONTH) 
		convert = "month_interval";
	else if (t->type->eclass == EC_SEC)
		convert = "second_interval";

	/* Lookup the sql convert function, there is no need
	 * for single value vs bat, this is handled by the
	 * mal function resolution */
	if (v->nrcols == 0) {	/* simple calc */
		q = newStmt(mb, calcRef, convert);
	} else if (v->nrcols > 0 &&
		(t->type->localtype > TYPE_str || f->type->eclass == EC_DEC || t->type->eclass == EC_DEC || EC_INTERVAL(t->type->eclass) || EC_TEMP(t->type->eclass) || (EC_VARCHAR(t->type->eclass) && !(f->type->eclass == EC_STRING && t->digits == 0)))) {
		int type = t->type->localtype;

		q = newStmt(mb, malRef, multiplexRef);
		if (q == NULL)
			return NULL;
		setVarType(mb, getArg(q, 0), newBatType(type));
		setVarUDFtype(mb, getArg(q, 0));
		q = pushStr(mb, q, convertMultiplexMod(calcRef, convert));
		q = pushStr(mb, q, convertMultiplexFcn(convert));
	} else
		q = newStmt(mb, batcalcRef, convert);

	/* convert to string is complex, we need full type info and mvc for the timezone */
	if (EC_VARCHAR(t->type->eclass) && !(f->type->eclass == EC_STRING && t->digits == 0)) {
		q = pushInt(mb, q, f->type->eclass);
		q = pushInt(mb, q, f->digits);
		q = pushInt(mb, q, f->scale);
		q = pushInt(mb, q, type_has_tz(f));
	} else if (f->type->eclass == EC_DEC)
		/* scale of the current decimal */
		q = pushInt(mb, q, f->scale);
	q = pushArgument(mb, q, v->nr);

	if (t->type->eclass == EC_DEC || EC_TEMP_FRAC(t->type->eclass) || EC_INTERVAL(t->type->eclass)) {
		/* digits, scale of the result decimal */
		q = pushInt(mb, q, t->digits);
		if (!EC_TEMP_FRAC(t->type->eclass))
			q = pushInt(mb, q, t->scale);
	}
	/* convert to string, give error on to large strings */
	if (EC_VARCHAR(t->type->eclass) && !(f->type->eclass == EC_STRING && t->digits == 0))
		q = pushInt(mb, q, t->digits);
	/* convert a string to a time(stamp) with time zone */
	if (EC_VARCHAR(f->type->eclass) && EC_TEMP_FRAC(t->type->eclass) && type_has_tz(t))
		q = pushInt(mb, q, type_has_tz(t));
	if (t->type->eclass == EC_GEOM) {
		/* push the type and coordinates of the column */
		q = pushInt(mb, q, t->digits);
		/* push the SRID of the whole columns */
		q = pushInt(mb, q, t->scale);
		/* push the type and coordinates of the inserted value */
		//q = pushInt(mb, q, f->digits);
		/* push the SRID of the inserted value */
		//q = pushInt(mb, q, f->scale);
		/* we decided to create the EWKB type also used by PostGIS and has the SRID provided by the user inside alreay */
		/* push the SRID provided for this value */
		/* GEOS library is able to store in the returned wkb the type an
 		 * number if coordinates but not the SRID so SRID should be provided 
 		 * from this level */
/*		if(be->argc > 1)
			f->scale = ((ValRecord)((atom*)(be->mvc)->args[1])->data).val.ival;
			
			q = pushInt(mb, q, f->digits);
			q = pushInt(mb, q, f->scale);
*/			//q = pushInt(mb, q, ((ValRecord)((atom*)(be->mvc)->args[1])->data).val.ival);
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_convert);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = v;
		s->nrcols = 0;	/* function without arguments returns single value */
		s->key = v->key;
		s->nrcols = v->nrcols;
		s->aggr = v->aggr;
		s->op4.typeval = *t;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_unop(backend *be, stmt *op1, sql_subfunc *op)
{
	list *ops = sa_list(be->mvc->sa);
	list_append(ops, op1);
	return stmt_Nop(be, stmt_list(be, ops), op);
}

stmt *
stmt_binop(backend *be, stmt *op1, stmt *op2, sql_subfunc *op)
{
	list *ops = sa_list(be->mvc->sa);
	list_append(ops, op1);
	list_append(ops, op2);
	return stmt_Nop(be, stmt_list(be, ops), op);
}

stmt *
stmt_Nop(backend *be, stmt *ops, sql_subfunc *f)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *mod, *fimp;
	sql_subtype *tpe = NULL;
	int special = 0;

	node *n;
	stmt *o = NULL;

	if (list_length(ops->op4.lval)) {
		for (n = ops->op4.lval->h, o = n->data; n; n = n->next) {
			stmt *c = n->data;

			if (o->nrcols < c->nrcols)
				o = c;
		}
	}

	if (backend_create_subfunc(be, f, ops->op4.lval) < 0)
		return NULL;
	mod = sql_func_mod(f->func);
	fimp = sql_func_imp(f->func);
	if (o && o->nrcols > 0 && f->func->type != F_LOADER) {
		sql_subtype *res = f->res->h->data;
		fimp = convertMultiplexFcn(fimp);
		q = NULL;
		if (strcmp(fimp, "rotate_xor_hash") == 0 &&
		    strcmp(mod, calcRef) == 0 &&
		    (q = newStmt(mb, mkeyRef, putName("bulk_rotate_xor_hash"))) == NULL)
			return NULL;
		if (!q) {
			if (f->func->type == F_UNION)
				q = newStmt(mb, batmalRef, multiplexRef);
			else
				q = newStmt(mb, malRef, multiplexRef);
			if (q == NULL)
				return NULL;
			setVarType(mb, getArg(q, 0), newBatType(res->type->localtype));
			setVarUDFtype(mb, getArg(q, 0));
			q = pushStr(mb, q, mod);
			q = pushStr(mb, q, fimp);
		} else {
			setVarType(mb, getArg(q, 0), newBatType(res->type->localtype));
			setVarUDFtype(mb, getArg(q, 0));
		}
	} else {
		fimp = convertOperator(fimp);
		q = newStmt(mb, mod, fimp);
		
		if (f->res && list_length(f->res)) {
			sql_subtype *res = f->res->h->data;

			setVarType(mb, getArg(q, 0), res->type->localtype);
			setVarUDFtype(mb, getArg(q, 0));
		}
	}
	if (LANG_EXT(f->func->lang))
		q = pushPtr(mb, q, f);
	if (f->func->lang == FUNC_LANG_C) {
		q = pushBit(mb, q, 0);
	} else if (f->func->lang == FUNC_LANG_CPP) {
		q = pushBit(mb, q, 1);
	}
	if (f->func->lang == FUNC_LANG_R || f->func->lang >= FUNC_LANG_PY ||
		f->func->lang == FUNC_LANG_C || f->func->lang == FUNC_LANG_CPP) {
		q = pushStr(mb, q, f->func->query);	
	}
	/* first dynamic output of copy* functions */
	if (f->func->type == F_UNION || (f->func->type == F_LOADER && f->res != NULL))
		q = table_func_create_result(mb, q, f->func, f->res);
	if (list_length(ops->op4.lval))
		tpe = tail_type(ops->op4.lval->h->data);
	if (strcmp(fimp, "round") == 0 && tpe && tpe->type->eclass == EC_DEC)
		special = 1;

	for (n = ops->op4.lval->h; n; n = n->next) {
		stmt *op = n->data;

		q = pushArgument(mb, q, op->nr);
		if (special) {
			q = pushInt(mb, q, tpe->digits);
			setVarUDFtype(mb, getArg(q, q->argc-1));
			q = pushInt(mb, q, tpe->scale);
			setVarUDFtype(mb, getArg(q, q->argc-1));
		}
		special = 0;
	}
	
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_Nop);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = ops;
		if (o) {
			s->nrcols = o->nrcols;
			s->key = o->key;
			s->aggr = o->aggr;
		} else {
			s->nrcols = 0;
			s->key = 1;
		}
		s->op4.funcval = f;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_func(backend *be, stmt *ops, const char *name, sql_rel *rel, int f_union)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *mod = "user";
	node *n;
	prop *p = NULL;

	/* dump args */
	if (ops && ops->nr < 0)
		return NULL;

	p = find_prop(rel->p, PROP_REMOTE);
	if (p) 
		rel->p = prop_remove(rel->p, p);
	rel = rel_optimizer(be->mvc, rel);
	if (p) {
		p->p = rel->p;
		rel->p = p;
	}

	if (monet5_create_relational_function(be->mvc, mod, name, rel, ops, NULL, 1) < 0)
		 return NULL;

	if (f_union) 
		q = newStmt(mb, batmalRef, multiplexRef);
	else
		q = newStmt(mb, mod, name);
	q = relational_func_create_result(be->mvc, mb, q, rel);
	if (f_union) {
		q = pushStr(mb, q, mod);
		q = pushStr(mb, q, name);
	}
	if (ops) {
		for (n = ops->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;

			q = pushArgument(mb, q, op->nr);
		}
	}

	if (q) {
		node *n;
		sql_allocator *sa = be->mvc->sa;
		stmt *o = NULL, *s = stmt_create(sa, st_func);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = ops;
		s->op2 = stmt_atom_string(be, name);
		s->op4.rel = rel;
		s->flag = f_union;
		if (ops && list_length(ops->op4.lval)) {
			for (n = ops->op4.lval->h, o = n->data; n; n = n->next) {
				stmt *c = n->data;
	
				if (o->nrcols < c->nrcols)
					o = c;
			}
		}

		if (o) {
			s->nrcols = o->nrcols;
			s->key = o->key;
			s->aggr = o->aggr;
		} else {
			s->nrcols = 0;
			s->key = 1;
		}
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_aggr(backend *be, stmt *op1, stmt *grp, stmt *ext, sql_subaggr *op, int reduce, int no_nil)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *mod, *aggrfunc;
	char aggrF[64];
	sql_subtype *res = op->res->h->data;
	int restype = res->type->localtype;
	int complex_aggr = 0;
	int abort_on_error, *stmt_nr = NULL;

	if (op1->nr < 0)
		return NULL;
	if (backend_create_subaggr(be, op) < 0)
		return NULL;
	mod = op->aggr->mod;
	aggrfunc = op->aggr->imp;

	if (strcmp(aggrfunc, "avg") == 0 || strcmp(aggrfunc, "sum") == 0 || strcmp(aggrfunc, "prod") == 0)
		complex_aggr = 1;
	/* some "sub" aggregates have an extra argument "abort_on_error" */
	abort_on_error = complex_aggr || strncmp(aggrfunc, "stdev", 5) == 0 || strncmp(aggrfunc, "variance", 8) == 0;

	if (ext) {
		snprintf(aggrF, 64, "sub%s", aggrfunc);
		aggrfunc = aggrF;
		if (grp->nr < 0 || ext->nr < 0)
			return NULL;

		q = newStmt(mb, mod, aggrfunc);
		if (q == NULL)
			return NULL;
		setVarType(mb, getArg(q, 0), newBatType(restype));
		setVarUDFtype(mb, getArg(q, 0));
	} else {
		q = newStmt(mb, mod, aggrfunc);
		if (q == NULL)
			return NULL;
		if (complex_aggr) {
			setVarType(mb, getArg(q, 0), restype);
			setVarUDFtype(mb, getArg(q, 0));
		}
	}

	if (LANG_EXT(op->aggr->lang))
		q = pushPtr(mb, q, op->aggr);
	if (op->aggr->lang == FUNC_LANG_R ||
		op->aggr->lang >= FUNC_LANG_PY || 
		op->aggr->lang == FUNC_LANG_C ||
		op->aggr->lang == FUNC_LANG_CPP) {
		if (!grp) {
			setVarType(mb, getArg(q, 0), restype);
			setVarUDFtype(mb, getArg(q, 0));
		}
		if (op->aggr->lang == FUNC_LANG_C) {
			q = pushBit(mb, q, 0);
		} else if (op->aggr->lang == FUNC_LANG_CPP) {
			q = pushBit(mb, q, 1);
		}
 		q = pushStr(mb, q, op->aggr->query);
	}

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
		q = pushArgument(mb, q, grp->nr);
		q = pushArgument(mb, q, ext->nr);
		if (q == NULL)
			return NULL;
		q = pushBit(mb, q, no_nil);
		if (abort_on_error)
			q = pushBit(mb, q, TRUE);
	} else if (no_nil && strncmp(aggrfunc, "count", 5) == 0) {
		q = pushBit(mb, q, no_nil);
	}
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
		s->op4.aggrval = op;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

static stmt *
stmt_alias_(backend *be, stmt *op1, const char *tname, const char *alias)
{
	stmt *s = stmt_create(be->mvc->sa, st_alias);
	if(!s) {
		return NULL;
	}
	s->op1 = op1;
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->aggr = op1->aggr;

	s->tname = tname;
	s->cname = alias;
	s->nr = op1->nr;
	s->q = op1->q;
	return s;
}

stmt *
stmt_alias(backend *be, stmt *op1, const char *tname, const char *alias)
{
	if (((!op1->tname && !tname) ||
	    (op1->tname && tname && strcmp(op1->tname, tname)==0)) &&
	    op1->cname && strcmp(op1->cname, alias)==0)
		return op1;
	return stmt_alias_(be, op1, tname, alias);
}

sql_subtype *
tail_type(stmt *st)
{
	for (;;) {
		switch (st->type) {
		case st_const:
			st = st->op2;
			continue;

		case st_uselect:
		case st_uselect2:
		case st_limit:
		case st_limit2:
		case st_sample:
		case st_tunion:
		case st_tdiff:
		case st_tinter:
		case st_append:
		case st_alias:
		case st_gen_group:
		case st_order:
			st = st->op1;
			continue;

		case st_list:
			st = st->op4.lval->h->data;
			continue;

		case st_bat:
			return &st->op4.cval->type;
		case st_idxbat:
			if (hash_index(st->op4.idxval->type)) {
				return sql_bind_localtype("lng");
			} else if (st->op4.idxval->type == join_idx) {
				return sql_bind_localtype("oid");
			}
			/* fall through */
		case st_join:
		case st_join2:
		case st_joinN:
			if (st->flag == cmp_project) {
				st = st->op2;
				continue;
			}
			/* fall through */
		case st_reorder:
		case st_group:
		case st_result:
		case st_tid:
		case st_mirror:
			return sql_bind_localtype("oid");
		case st_table_clear:
			return sql_bind_localtype("lng");

		case st_aggr: {
			list *res = st->op4.aggrval->res;

			if (res && list_length(res) == 1)
				return res->h->data;

			return NULL;
		}
		case st_Nop: {
			list *res = st->op4.funcval->res;

			if (res && list_length(res) == 1)
				return res->h->data;
			return NULL;
		}
		case st_atom:
			return atom_type(st->op4.aval);
		case st_convert:
		case st_temp:
		case st_single:
		case st_rs_column:
			return &st->op4.typeval;
		case st_var:
			if (st->op4.typeval.type)
				return &st->op4.typeval;
			/* fall through */
		case st_exception:
			return NULL;
		case st_table:
			return sql_bind_localtype("bat");
		default:
			assert(0);
			return NULL;
		}
	}
}

int
stmt_has_null(stmt *s)
{
	switch (s->type) {
	case st_aggr:
	case st_Nop:
	case st_uselect:
	case st_uselect2:
	case st_atom:
		return 0;
	case st_join:
		return stmt_has_null(s->op2);
	case st_bat:
		return s->op4.cval->null;

	default:
		return 1;
	}
}

static const char *
func_name(sql_allocator *sa, const char *n1, const char *n2)
{
	int l1 = _strlen(n1), l2;

	if (!sa)
		return n1;
	if (!n2)
		return sa_strdup(sa, n1);
	l2 = _strlen(n2);

	if (l2 > 16) {		/* only support short names */
		char *ns = SA_NEW_ARRAY(sa, char, l2 + 1);
		if(!ns)
			return NULL;
		strncpy(ns, n2, l2);
		ns[l2] = 0;
		return ns;
	} else {
		char *ns = SA_NEW_ARRAY(sa, char, l1 + l2 + 2), *s = ns;
		if(!ns)
			return NULL;
		strncpy(ns, n1, l1);
		ns += l1;
		*ns++ = '_';
		strncpy(ns, n2, l2);
		ns += l2;
		*ns = '\0';
		return s;
	}
}

const char *_column_name(sql_allocator *sa, stmt *st);

const char *
column_name(sql_allocator *sa, stmt *st)
{
	if (!st->cname)
		st->cname = _column_name(sa, st);
	return st->cname;
}

const char *
_column_name(sql_allocator *sa, stmt *st)
{
	switch (st->type) {
	case st_order:
	case st_reorder:
		return column_name(sa, st->op1);
	case st_const:
	case st_join:
	case st_join2:
	case st_joinN:
		return column_name(sa, st->op2);

	case st_mirror:
	case st_group:
	case st_result:
	case st_append:
	case st_gen_group:
	case st_uselect:
	case st_uselect2:
	case st_limit:
	case st_limit2:
	case st_sample:
	case st_tunion:
	case st_tdiff:
	case st_tinter:
	case st_convert:
		return column_name(sa, st->op1);
	case st_Nop:
	{
		const char *cn = column_name(sa, st->op1);
		return func_name(sa, st->op4.funcval->func->base.name, cn);
	}
	case st_aggr:
	{
		const char *cn = column_name(sa, st->op1);
		return func_name(sa, st->op4.aggrval->aggr->base.name, cn);
	}
	case st_alias:
		return column_name(sa, st->op3);
	case st_bat:
		return st->op4.cval->base.name;
	case st_atom:
		if (st->op4.aval->data.vtype == TYPE_str)
			return atom2string(sa, st->op4.aval);
		/* fall through */
	case st_var:
	case st_temp:
	case st_single:
		if (sa)
			return sa_strdup(sa, "single_value");
		return "single_value";

	case st_list:
		if (list_length(st->op4.lval))
			return column_name(sa, st->op4.lval->h->data);
		/* fall through */
	case st_rs_column:
		return NULL;
	default:
		return NULL;
	}
}

const char *_table_name(sql_allocator *sa, stmt *st);

const char *
table_name(sql_allocator *sa, stmt *st)
{
	if (!st->tname)
		st->tname = _table_name(sa, st);
	return st->tname;
}

const char *
_table_name(sql_allocator *sa, stmt *st)
{
	switch (st->type) {
	case st_const:
	case st_join:
	case st_join2:
	case st_joinN:
	case st_append:
		return table_name(sa, st->op2);
	case st_mirror:
	case st_group:
	case st_result:
	case st_gen_group:
	case st_uselect:
	case st_uselect2:
	case st_limit:
	case st_limit2:
	case st_sample:
	case st_tunion:
	case st_tdiff:
	case st_tinter:
	case st_aggr:
		return table_name(sa, st->op1);

	case st_table_clear:
		return st->op4.tval->base.name;
	case st_idxbat:
	case st_bat:
	case st_tid:
		return st->op4.cval->t->base.name;
	case st_alias:
		if (st->tname)
			return st->tname;
		else
			/* there are no table aliases, ie look into the base column */
			return table_name(sa, st->op1);
	case st_atom:
		if (st->op4.aval->data.vtype == TYPE_str && st->op4.aval->data.val.sval && _strlen(st->op4.aval->data.val.sval))
			return st->op4.aval->data.val.sval;
		return NULL;

	case st_list:
		if (list_length(st->op4.lval) && st->op4.lval->h)
			return table_name(sa, st->op4.lval->h->data);
		return NULL;

	case st_var:
	case st_temp:
	case st_single:
	default:
		return NULL;
	}
}

const char *
schema_name(sql_allocator *sa, stmt *st)
{
	switch (st->type) {
	case st_const:
	case st_join:
	case st_join2:
	case st_joinN:
		return schema_name(sa, st->op2);
	case st_mirror:
	case st_group:
	case st_result:
	case st_append:
	case st_gen_group:
	case st_uselect:
	case st_uselect2:
	case st_limit:
	case st_limit2:
	case st_sample:
	case st_tunion:
	case st_tdiff:
	case st_tinter:
	case st_convert:
	case st_Nop:
	case st_aggr:
		return schema_name(sa, st->op1);
	case st_alias:
		/* there are no schema aliases, ie look into the base column */
		return schema_name(sa, st->op1);
	case st_bat:
		return st->op4.cval->t->s->base.name;
	case st_atom:
		return NULL;
	case st_var:
	case st_temp:
	case st_single:
		return NULL;
	case st_list:
		if (list_length(st->op4.lval))
			return schema_name(sa, st->op4.lval->h->data);
		return NULL;
	default:
		return NULL;
	}
}

stmt *
stmt_cond(backend *be, stmt *cond, stmt *outer, int loop /* 0 if, 1 while */, int anti )
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (cond->nr < 0)
		return NULL;
	if (anti) {
		sql_subtype *bt = sql_bind_localtype("bit");
		sql_subfunc *not = sql_bind_func(be->mvc->sa, NULL, "not", bt, NULL, F_FUNC);
		sql_subfunc *or = sql_bind_func(be->mvc->sa, NULL, "or", bt, bt, F_FUNC);
		sql_subfunc *isnull = sql_bind_func(be->mvc->sa, NULL, "isnull", bt, NULL, F_FUNC);
		cond = stmt_binop(be, 
			stmt_unop(be, cond, not),
			stmt_unop(be, cond, isnull), or);
	}
	if (!loop) {	/* if */
		q = newAssignment(mb);
		if (q == NULL)
			return NULL;
		q->barrier = BARRIERsymbol;
		q = pushArgument(mb, q, cond->nr);
		if (q == NULL)
			return NULL;
	} else {	/* while */
		int c;

		if (outer->nr < 0)
			return NULL;
		/* leave barrier */
		q = newStmt(mb, calcRef, notRef);
		q = pushArgument(mb, q, cond->nr);
		if (q == NULL)
			return NULL;
		c = getArg(q, 0);

		q = newAssignment(mb);
		if (q == NULL)
			return NULL;
		getArg(q, 0) = outer->nr;
		q->barrier = LEAVEsymbol;
		q = pushArgument(mb, q, c);
		if (q == NULL)
			return NULL;
	}
	if (q){
		stmt *s = stmt_create(be->mvc->sa, st_cond);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->flag = loop;
		s->op1 = cond;
		s->nr = getArg(q, 0);
		return s;
	}
	return NULL;
}

stmt *
stmt_control_end(backend *be, stmt *cond)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (cond->nr < 0)
		return NULL;

	if (cond->flag) {	/* while */
		/* redo barrier */
		q = newAssignment(mb);
		if (q == NULL)
			return NULL;
		getArg(q, 0) = cond->nr;
		q->argc = q->retc = 1;
		q->barrier = REDOsymbol;
		q = pushBit(mb, q, TRUE);
		if (q == NULL)
			return NULL;
	} else {
		q = newAssignment(mb);
		if (q == NULL)
			return NULL;
		getArg(q, 0) = cond->nr;
		q->argc = q->retc = 1;
		q->barrier = EXITsymbol;
	}
	q = newStmt(mb, sqlRef, mvcRef);
	if (q == NULL)
		return NULL;
	be->mvc_var = getDestVar(q);
	if (q){
		stmt *s = stmt_create(be->mvc->sa, st_control_end);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = cond;
		s->nr = getArg(q, 0);
		return s;
	}
	return NULL;
}


static InstrPtr
dump_cols(MalBlkPtr mb, list *l, InstrPtr q)
{
	int i;
	node *n;

	if (q == NULL)
		return NULL;
	q->retc = q->argc = 0;
	for (i = 0, n = l->h; n; n = n->next, i++) {
		stmt *c = n->data;

		q = pushArgument(mb, q, c->nr);
	}
	if (q == NULL)
		return NULL;
	q->retc = q->argc;
	/* Lets make it a propper assignment */
	for (i = 0, n = l->h; n; n = n->next, i++) {
		stmt *c = n->data;

		q = pushArgument(mb, q, c->nr);
	}
	return q;
}

stmt *
stmt_return(backend *be, stmt *val, int nr_declared_tables)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (val->nr < 0)
		return NULL;
	q = newInstruction(mb, NULL, NULL);
	if (q == NULL)
		return NULL;
	q->barrier= RETURNsymbol;
	if (val->type == st_table) {
		list *l = val->op1->op4.lval;

		q = dump_cols(mb, l, q);
	} else {
		getArg(q, 0) = getArg(getInstrPtr(mb, 0), 0);
		q = pushArgument(mb, q, val->nr);
	}
	if (q == NULL)
		return NULL;
	pushInstruction(mb, q);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_return);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = val;
		s->flag = nr_declared_tables;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_assign(backend *be, const char *varname, stmt *val, int level)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (val && val->nr < 0)
		return NULL;
	if (level != 1) {	
		char buf[IDLENGTH];

		if (!val) {
			/* drop declared table */
			assert(0);
		}
		(void) snprintf(buf, sizeof(buf), "A%s", varname);
		q = newInstruction(mb, NULL, NULL);
		if (q == NULL) {
			return NULL;
		}
		q->argc = q->retc = 0;
		q = pushArgumentId(mb, q, buf);
		if (q == NULL)
			return NULL;
		pushInstruction(mb, q);
		if (mb->errors)
			return NULL;
		q->retc++;
	} else {
		q = newStmt(mb, sqlRef, setVariableRef);
		q = pushArgument(mb, q, be->mvc_var);
		q = pushStr(mb, q, varname);
		if (q == NULL)
			return NULL;
		getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
		be->mvc_var = getDestVar(q);
	}
	q = pushArgument(mb, q, val->nr);
	if (q){
		stmt *s = stmt_create(be->mvc->sa, st_assign);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op2 = val;
		s->flag = (level << 1);
		s->q = q;
		s->nr = 1;
		return s;
	}
	return NULL;
}

stmt *
const_column(backend *be, stmt *val)
{
	sql_subtype *ct = tail_type(val);
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	int tt = ct->type->localtype;

	if (val->nr < 0) 
		return NULL;
	q = newStmt(mb, sqlRef, singleRef);
	if (q == NULL)
		return NULL;
	setVarType(mb, getArg(q, 0), newBatType(tt));
	q = pushArgument(mb, q, val->nr);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_single);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = val;
		s->op4.typeval = *ct;
		s->nrcols = 1;

		s->tname = val->tname;
		s->cname = val->cname;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}
