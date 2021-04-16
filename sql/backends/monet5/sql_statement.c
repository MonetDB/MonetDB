/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_mem.h"
#include "sql_stack.h"
#include "sql_statement.h"
#include "sql_gencode.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_unnest.h"
#include "rel_optimizer.h"

#include "mal_namespace.h"
#include "mal_builder.h"
#include "mal_debugger.h"
#include "opt_prelude.h"

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
	if( _t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

static InstrPtr
pushSchema(MalBlkPtr mb, InstrPtr q, sql_table *t)
{
	if (t->s)
		return pushArgument(mb, q, getStrConstant(mb,t->s->base.name));
	else
		return pushNil(mb, q, TYPE_str);
}

static int
is_tid_chain(stmt *sel)
{
	while (sel && sel->type != st_tid && sel->cand)
		sel = sel->cand;
	if (sel && sel->type == st_tid)
		return 1;
	return 0;
}

stmt *
stmt_project_column_on_cand(backend *be, stmt *sel, stmt *c)
{
	stmt *res = NULL;

	if (c->cand == sel)
		return c;
	if (!sel)
		return res;
	assert(c->type == st_alias || (c->type == st_join && c->flag == cmp_project) || c->type == st_bat || c->type == st_idxbat || c->type == st_single || c->type == st_const || c->type == st_Nop);
	if (c->type != st_alias) {
		res = stmt_project(be, sel, c);
	} else if (c->op1->type == st_mirror && is_tid_chain(sel)) { /* alias with mirror (ie full row ids) */
		res = stmt_alias(be, sel, c->tname, c->cname);
	} else { /* st_alias */
		stmt *s = c->op1;
		if (s->nrcols == 0)
			s = stmt_const(be, sel, NULL, s);
		else
			s = stmt_project(be, sel, s);
		res = stmt_alias(be, s, c->tname, c->cname);
	}
	res->cand = sel;
	return res;
}

/* #TODO make proper traversal operations */
stmt *
stmt_atom_string(backend *be, const char *S)
{
	char *s = NULL;
	sql_subtype t;

	if (S) {
		s = sa_strdup(be->mvc->sa, S);
		sql_find_subtype(&t, "varchar", _strlen(s), 0);
	} else {
		sql_find_subtype(&t, "clob", 0, 0);
	}

	return stmt_atom(be, atom_string(be->mvc->sa, &t, s));
}

stmt *
stmt_atom_int(backend *be, int i)
{
	sql_subtype t;

	sql_find_subtype(&t, "int", 32, 0);
	return stmt_atom(be, is_int_nil(i) ? atom_general(be->mvc->sa, &t, NULL) : atom_int(be->mvc->sa, &t, i));
}

stmt *
stmt_atom_lng(backend *be, lng i)
{
	sql_subtype t;

	sql_find_subtype(&t, "bigint", 64, 0);
	return stmt_atom(be, is_lng_nil(i) ? atom_general(be->mvc->sa, &t, NULL) : atom_int(be->mvc->sa, &t, i));
}

stmt *
stmt_bool(backend *be, int b)
{
	sql_subtype t;

	sql_find_subtype(&t, "boolean", 0, 0);
	return stmt_atom(be, atom_bool(be->mvc->sa, &t, b ? TRUE: FALSE));
}

static stmt *
stmt_create(sql_allocator *sa, st_type type)
{
	stmt *s = SA_NEW(sa, stmt);

	if (!s)
		return NULL;
	*s = (stmt) {
		.type = type,
	};
	return s;
}

void
stmt_set_nrcols(rel_bin_stmt *s)
{
	unsigned nrcols = 0;
	int key = 1;

	for (node *n = s->cols->h; n; n = n->next) {
		stmt *f = n->data;

		assert(f);
		if (f->nrcols > nrcols)
			nrcols = f->nrcols;
		key &= f->key;
	}
	s->nrcols = nrcols;
	s->key = key;
}

rel_bin_stmt *
create_rel_bin_stmt(sql_allocator *sa, list *cols, stmt *cand, stmt *grp, stmt *ext, stmt *cnt)
{
	rel_bin_stmt *rs = SA_NEW(sa, rel_bin_stmt);

	if (!rs)
		return NULL;
	*rs = (rel_bin_stmt) {
		.cols = cols,
		.cand = cand,
		.grp = grp,
		.ext = ext,
		.cnt = cnt,
		.nrcols = 0
	};

	stmt_set_nrcols(rs);
	return rs;
}

stmt *
stmt_group(backend *be, stmt *s, stmt *sel, stmt *grp, stmt *ext, stmt *cnt, int done)
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
	if (sel)
		q = pushArgument(mb, q, sel->nr);
	/*
	else
		q = pushNil(mb, q, TYPE_bat);
		*/
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
		ns->cand = sel;
		return ns;
	}
	return NULL;
}

stmt *
stmt_unique(backend *be, stmt *s)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (s->nr < 0)
		return NULL;

	q = newStmt(mb, algebraRef, uniqueRef);
	if(!q)
		return NULL;

	q = pushArgument(mb, q, s->nr);
	q = pushNil(mb, q, TYPE_bat); /* candidate list */
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

static int
create_bat(MalBlkPtr mb, int tt)
{
	InstrPtr q = newStmt(mb, batRef, newRef);

	if (q == NULL)
		return -1;
	setVarType(mb, getArg(q, 0), newBatType(tt));
	q = pushType(mb, q, tt);
	return getDestVar(q);
}

static int *
dump_table(sql_allocator *sa, MalBlkPtr mb, sql_table *t)
{
	int i = 0;
	node *n;
	int *l = SA_NEW_ARRAY(sa, int, ol_length(t->columns) + 1);

	if (!l)
		return NULL;

	/* tid column */
	if ((l[i++] = create_bat(mb, TYPE_oid)) < 0)
		return NULL;

	for (n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data;

		if ((l[i++] = create_bat(mb, c->type.type->localtype)) < 0)
			return NULL;
	}
	return l;
}

stmt *
stmt_var(backend *be, const char *sname, const char *varname, sql_subtype *t, int declare, int level)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	char *buf;

	if (level == 0) { /* global */
		int tt = t->type->localtype;

		assert(sname);
		q = newStmt(mb, sqlRef, getVariableRef);
		q = pushArgument(mb, q, be->mvc_var);
		q = pushStr(mb, q, sname); /* all global variables have a schema */
		q = pushStr(mb, q, varname);
		if (q == NULL)
			return NULL;
		setVarType(mb, getArg(q, 0), tt);
	} else if (!declare) {
		char levelstr[16];

		assert(!sname);
		snprintf(levelstr, sizeof(levelstr), "%d", level);
		buf = SA_NEW_ARRAY(be->mvc->sa, char, strlen(levelstr) + strlen(varname) + 3);
		if (!buf)
			return NULL;
		stpcpy(stpcpy(stpcpy(stpcpy(buf, "A"), levelstr), "%"), varname); /* mangle variable name */
		q = newAssignment(mb);
		q = pushArgumentId(mb, q, buf);
	} else {
		int tt = t->type->localtype;
		char levelstr[16];

		assert(!sname);
		snprintf(levelstr, sizeof(levelstr), "%d", level);
		buf = SA_NEW_ARRAY(be->mvc->sa, char, strlen(levelstr) + strlen(varname) + 3);
		if (!buf)
			return NULL;
		stpcpy(stpcpy(stpcpy(stpcpy(buf, "A"), levelstr), "%"), varname); /* mangle variable name */

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

		ATOMIC_PTR_SET(&t->data, l);
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
	char buf[IDLENGTH];

	if (!q)
		return NULL;

	(void) snprintf(buf, sizeof(buf), "A%d", nr);
	q = pushArgumentId(mb, q, buf);
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
stmt_table(backend *be, rel_bin_stmt *relst, int temp)
{
	stmt *s = stmt_create(be->mvc->sa, st_table);

	if (s == NULL)
		return NULL;
	s->nr = !list_empty(relst->cols) ? ((stmt*)relst->cols->h->data)->nr : 0;
	s->op4.relstval = relst;
	s->flag = temp;
	s->nrcols = relst->nrcols;
	return s;
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

	if (!t->s && ATOMIC_PTR_GET(&t->data)) { /* declared table */
		stmt *s = stmt_create(be->mvc->sa, st_tid);
		int *l = ATOMIC_PTR_GET(&t->data);

		if (s == NULL) {
			return NULL;
		}
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
	q = pushArgument(mb, q, be->mvc_var);
	q = pushSchema(mb, q, t);
	q = pushStr(mb, q, t->base.name);
	if (q == NULL)
		return NULL;
	if (t && (!isRemote(t) && !isMergeTable(t)) && partition) {
		sql_trans *tr = be->mvc->session->tr;
		sqlstore *store = tr->store;
		BUN rows = (BUN) store->storage_api.count_col(tr, ol_first_node(t->columns)->data, QUICK);
		setRowCnt(mb,getArg(q,0),rows);
	}

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

stmt *
stmt_bat(backend *be, sql_column *c, int access, int partition)
{
	int tt = c->type.type->localtype;
	MalBlkPtr mb = be->mb;
	InstrPtr q;

	/* for read access tid.project(col) */
	if (!c->t->s && ATOMIC_PTR_GET(&c->t->data)) { /* declared table */
		stmt *s = stmt_create(be->mvc->sa, st_bat);
		int *l = ATOMIC_PTR_GET(&c->t->data);

		if (s == NULL) {
			return NULL;
		}
		assert(partition == 0);
		s->partition = partition;
		s->op4.cval = c;
		s->nrcols = 1;
		s->flag = access;
		s->nr = l[c->colnr+1];
		s->tname = c->t?c->t->base.name:NULL;
		s->cname = c->base.name;
		return s;
	}
	q = newStmtArgs(mb, sqlRef, bindRef, 9);
	if (q == NULL)
		return NULL;
	if (access == RD_UPD_ID) {
		q = pushReturn(mb, q, newTmpVariable(mb, newBatType(tt)));
	} else {
		setVarType(mb, getArg(q, 0), newBatType(tt));
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
	}
	if (partition) {
		sql_trans *tr = be->mvc->session->tr;
		sqlstore *store = tr->store;

		if (c && (!isRemote(c->t) && !isMergeTable(c->t))) {
			BUN rows = (BUN) store->storage_api.count_col(tr, c, QUICK);
			setRowCnt(mb,getArg(q,0),rows);
		}
	}

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
	s->tname = c->t->base.name;
	s->cname = c->base.name;
	return s;
}

stmt *
stmt_idxbat(backend *be, sql_idx *i, int access, int partition)
{
	int tt = hash_index(i->type)?TYPE_lng:TYPE_oid;
	MalBlkPtr mb = be->mb;
	InstrPtr q = newStmtArgs(mb, sqlRef, bindidxRef, 9);

	if (q == NULL)
		return NULL;

	if (access == RD_UPD_ID) {
		q = pushReturn(mb, q, newTmpVariable(mb, newBatType(tt)));
	} else {
		setVarType(mb, getArg(q, 0), newBatType(tt));
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
	}
	if (partition) {
		sql_trans *tr = be->mvc->session->tr;
		sqlstore *store = tr->store;

		if (i && (!isRemote(i->t) && !isMergeTable(i->t))) {
			BUN rows = (BUN) store->storage_api.count_idx(tr, i, QUICK);
			setRowCnt(mb,getArg(q,0),rows);
		}
	}

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
	s->tname = i->t->base.name;
	s->cname = i->base.name;
	return s;
}

stmt *
stmt_append_col(backend *be, sql_column *c, stmt *offset, stmt *b, int fake)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (b->nr < 0)
		return NULL;

	if (!c->t->s && ATOMIC_PTR_GET(&c->t->data)) { /* declared table */
		int *l = ATOMIC_PTR_GET(&c->t->data);

		if (c->colnr == 0) { /* append to tid column */
			q = newStmt(mb, sqlRef, growRef);
			q = pushArgument(mb, q, l[0]);
			q = pushArgument(mb, q, b->nr);
		}
		q = newStmt(mb, batRef, appendRef);
		q = pushArgument(mb, q, l[c->colnr+1]);
		q = pushArgument(mb, q, b->nr);
		q = pushBit(mb, q, TRUE);
		if (q)
			getArg(q,0) = l[c->colnr+1];
	} else if (!fake) {	/* fake append */
		if (offset->nr < 0)
			return NULL;
		q = newStmt(mb, sqlRef, appendRef);
		q = pushArgument(mb, q, be->mvc_var);
		if (q == NULL)
			return NULL;
		getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
		q = pushSchema(mb, q, c->t);
		q = pushStr(mb, q, c->t->base.name);
		q = pushStr(mb, q, c->base.name);
		q = pushArgument(mb, q, offset->nr);
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
		s->op2 = offset;
		s->op4.cval = c;
		s->q = q;
		s->nr = getDestVar(q);
		return s;
	}
	return NULL;
}

stmt *
stmt_append_idx(backend *be, sql_idx *i, stmt *offset, stmt *b)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (offset->nr < 0 || b->nr < 0)
		return NULL;

	q = newStmt(mb, sqlRef, appendRef);
	q = pushArgument(mb, q, be->mvc_var);
	if (q == NULL)
		return NULL;
	getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
	q = pushSchema(mb, q, i->t);
	q = pushStr(mb, q, i->t->base.name);
	q = pushStr(mb, q, sa_strconcat(be->mvc->sa, "%", i->base.name));
	q = pushArgument(mb, q, offset->nr);
	q = pushArgument(mb, q, b->nr);
	if (q == NULL)
		return NULL;
	be->mvc_var = getDestVar(q);

	stmt *s = stmt_create(be->mvc->sa, st_append_idx);
	if (s == NULL) {
		freeInstruction(q);
		return NULL;
	}

	s->op1 = b;
	s->op2 = offset;
	s->op4.idxval = i;
	s->q = q;
	s->nr = getDestVar(q);
	return s;
}

stmt *
stmt_update_col(backend *be, sql_column *c, stmt *tids, stmt *upd)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (tids->nr < 0 || upd->nr < 0)
		return NULL;

	if (!c->t->s && ATOMIC_PTR_GET(&c->t->data)) { /* declared table */
		int *l = ATOMIC_PTR_GET(&c->t->data);

		q = newStmt(mb, batRef, replaceRef);
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

stmt *
stmt_delete(backend *be, sql_table *t, stmt *tids)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (tids->nr < 0)
		return NULL;

	if (!t->s && ATOMIC_PTR_GET(&t->data)) { /* declared table */
		int *l = ATOMIC_PTR_GET(&t->data);

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
stmt_const(backend *be, stmt *s, stmt *sel, stmt *val)
{
	InstrPtr q = NULL;
	MalBlkPtr mb = be->mb;
	stmt *pc = sel ? sel : s;

	q = dump_2(mb, algebraRef, projectRef, pc, val);
	if (q) {
		stmt *ns = stmt_create(be->mvc->sa, st_const);
		if (ns == NULL) {
			freeInstruction(q);
			return NULL;
		}

		ns->op1 = pc;
		ns->op2 = val;
		ns->nrcols = s->nrcols;
		ns->key = s->key;
		ns->aggr = s->aggr;
		ns->q = q;
		ns->nr = getDestVar(q);
		ns->tname = val->tname;
		ns->cname = val->cname;
		ns->cand = sel;
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
	ns->cand = s->cand;
	return ns;
}

/* limit maybe atom nil */
stmt *
stmt_limit(backend *be, stmt *col, stmt *piv, stmt *gid, stmt *offset, stmt *limit, int distinct, int dir, int nullslast, int last, int order)
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

		q = newStmt(mb, calcRef, plusRef);
		q = pushArgument(mb, q, offset->nr);
		q = pushArgument(mb, q, limit->nr);
		if (q == NULL)
			return NULL;
		topn = getDestVar(q);

		q = newStmtArgs(mb, algebraRef, firstnRef, 9);
		if (!last) /* we need the groups for the next firstn */
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, c);
		if (p)
			q = pushArgument(mb, q, p);
		else
			q = pushNil(mb, q, TYPE_bat);
		if (g)
			q = pushArgument(mb, q, g);
		else
			q = pushNil(mb, q, TYPE_bat);
		q = pushArgument(mb, q, topn);
		q = pushBit(mb, q, dir);
		q = pushBit(mb, q, nullslast);
		q = pushBit(mb, q, distinct != 0);

		if (q == NULL)
			return NULL;
		l = getArg(q, 0);
		l = getDestVar(q);
	} else {
		int len;

		q = newStmt(mb, calcRef, plusRef);
		q = pushArgument(mb, q, offset->nr);
		q = pushArgument(mb, q, limit->nr);
		if (q == NULL)
			return NULL;
		len = getDestVar(q);

		/* since both arguments of algebra.subslice are
		   inclusive correct the LIMIT value by
		   subtracting 1 */
		q = newStmt(mb, calcRef, minusRef);
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

stmt *
stmt_sample(backend *be, stmt *s, stmt *sample, stmt *seed)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (s->nr < 0 || sample->nr < 0)
		return NULL;
	q = newStmt(mb, sampleRef, subuniformRef);
	q = pushArgument(mb, q, s->nr);
	q = pushArgument(mb, q, sample->nr);

	if (seed) {
		if (seed->nr < 0)
			return NULL;

		q = pushArgument(mb, q, seed->nr);
	}

	if (q) {
		stmt *ns = stmt_create(be->mvc->sa, st_sample);
		if (ns == NULL) {
			freeInstruction(q);
			return NULL;
		}

		ns->op1 = s;
		ns->op2 = sample;

		if (seed) {
			ns->op3 = seed;
		}

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
stmt_order(backend *be, stmt *s, int direction, int nullslast)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (s->nr < 0)
		return NULL;
	q = newStmt(mb, algebraRef, sortRef);
	/* both ordered result and oid's order en subgroups */
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, s->nr);
	q = pushBit(mb, q, !direction);
	q = pushBit(mb, q, nullslast);
	q = pushBit(mb, q, FALSE);
	if (q == NULL)
		return NULL;


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

stmt *
stmt_reorder(backend *be, stmt *s, int direction, int nullslast, stmt *orderby_ids, stmt *orderby_grp)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (s->nr < 0 || orderby_ids->nr < 0 || orderby_grp->nr < 0)
		return NULL;
	q = newStmtArgs(mb, algebraRef, sortRef, 9);
	/* both ordered result and oid's order en subgroups */
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, s->nr);
	q = pushArgument(mb, q, orderby_ids->nr);
	q = pushArgument(mb, q, orderby_grp->nr);
	q = pushBit(mb, q, !direction);
	q = pushBit(mb, q, nullslast);
	q = pushBit(mb, q, FALSE);
	if (q == NULL)
		return NULL;

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

stmt *
stmt_atom(backend *be, atom *a)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = EC_TEMP_FRAC(atom_type(a)->type->eclass) ? newStmt(mb, calcRef, atom_type(a)->type->base.name) : newAssignment(mb);

	if (!q)
		return NULL;
	if (atom_null(a)) {
		q = pushNil(mb, q, atom_type(a)->type->localtype);
	} else {
		int k;
		if ((k = constantAtom(be, mb, a)) == -1) {
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
stmt_genselect(backend *be, stmt *lops, stmt *rops, sql_subfunc *f, stmt *sel, int anti)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *mod = sql_func_mod(f->func), *fimp = sql_func_imp(f->func);
	int k, push_cands = strcmp(mod, "algebra") == 0, all_cands_pushed = 1;

	if (backend_create_subfunc(be, f, NULL) < 0)
		return NULL;

	if (rops->nrcols >= 1) {
		bit need_not = FALSE;
		int narg = 3 + list_length(lops->op4.lval) + list_length(rops->op4.lval);

		if (push_cands) {
			char batmodule[16];
			stpcpy(stpcpy(batmodule, "bat"), mod);
			q = newStmtArgs(mb, batmodule, fimp, narg);
		} else {
			q = newStmtArgs(mb, malRef, multiplexRef, narg);
			q = pushStr(mb, q, convertMultiplexMod(mod, fimp));
			q = pushStr(mb, q, convertMultiplexFcn(fimp));
		}
		setVarType(mb, getArg(q, 0), newBatType(TYPE_bit));

		for (node *n = lops->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;

			if (!push_cands && sel && op->nrcols > 0 && !op->cand) /* don't push cands twice */
				op = stmt_project_column_on_cand(be, sel, op);
			else if (push_cands && op->nrcols > 0)
				all_cands_pushed &= op->cand != NULL;
			q = pushArgument(mb, q, op->nr);
		}
		for (node *n = rops->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;

			if (!push_cands && sel && op->nrcols > 0 && !op->cand) /* don't push cands twice */
				op = stmt_project_column_on_cand(be, sel, op);
			else if (push_cands && op->nrcols > 0)
				all_cands_pushed &= op->cand != NULL;
			q = pushArgument(mb, q, op->nr);
		}
		if (!all_cands_pushed && push_cands) {
			for (node *n = lops->op4.lval->h; n; n = n->next) {
				stmt *op = n->data;
				if (op->nrcols) {
					if (!sel || op->cand)
						q = pushNil(mb, q, TYPE_bat);
					else
						q = pushArgument(mb, q, sel->nr);
				}
			}
			for (node *n = rops->op4.lval->h; n; n = n->next) {
				stmt *op = n->data;
				if (op->nrcols) {
					if (!sel || op->cand)
						q = pushNil(mb, q, TYPE_bat);
					else
						q = pushArgument(mb, q, sel->nr);
				}
			}
			all_cands_pushed = 1;
		}
		k = getDestVar(q);

		q = newStmtArgs(mb, algebraRef, selectRef, 9);
		q = pushArgument(mb, q, k);
		if (push_cands) {
			if (sel && !all_cands_pushed)
				q = pushArgument(mb, q, sel->nr);
			else
				q = pushNil(mb, q, TYPE_bat);
		}
		q = pushBit(mb, q, !need_not);
		q = pushBit(mb, q, !need_not);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, anti);
		if (all_cands_pushed && sel) {
			int nr = getDestVar(q);
			q = newStmt(mb, algebraRef, projectionRef);
			q = pushArgument(mb, q, nr);
			q = pushArgument(mb, q, sel->nr);
		}
	} else {
		fimp = sa_strconcat(be->mvc->sa, fimp, selectRef);
		q = newStmtArgs(mb, mod, convertMultiplexFcn(fimp), 9);
		// push pointer to the SQL structure into the MAL call
		// allows getting argument names for example
		if (LANG_EXT(f->func->lang))
			q = pushPtr(mb, q, f); // nothing to see here, please move along
		// f->query contains the R code to be run
		if (f->func->lang == FUNC_LANG_R || f->func->lang >= FUNC_LANG_PY)
			q = pushStr(mb, q, f->func->query);

		for (node *n = lops->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;

			if (!push_cands && sel && op->nrcols > 0 && !op->cand) /* don't push cands twice */
				op = stmt_project_column_on_cand(be, sel, op);
			else if (push_cands && op->nrcols > 0)
				all_cands_pushed &= op->cand != NULL;
			q = pushArgument(mb, q, op->nr);
		}
		/* candidate lists */
		if (push_cands) {
			if (sel && !all_cands_pushed)
				q = pushArgument(mb, q, sel->nr);
			else
				q = pushNil(mb, q, TYPE_bat);
		}

		for (node *n = rops->op4.lval->h; n; n = n->next) {
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
		s->op3 = sel;
		s->key = lops->nrcols == 0 && rops->nrcols == 0;
		s->flag = cmp_filter;
		s->nrcols = lops->nrcols;
		s->nr = getDestVar(q);
		s->q = q;
		s->cand = sel;
		return s;
	}
	return NULL;
}

stmt *
stmt_uselect(backend *be, stmt *op1, stmt *op2, comp_type cmptype, stmt *sel, int anti, int is_semantics)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	int l, r;

	if (op1->nr < 0 || op2->nr < 0 || (sel && sel->nr < 0))
		return NULL;
	l = op1->nr;
	r = op2->nr;

	if (op2->nrcols >= 1 && op1->nrcols == 0) { /* swap */
		stmt *v = op1;
		op1 = op2;
		op2 = v;
		int n = l;
		l = r;
		r = n;
		cmptype = swap_compare(cmptype);
	}
	if (op2->nrcols >= 1) {
		bit need_not = FALSE;
		const char *op = "==";
		int k;

		switch (cmptype) {
		case mark_in:
		case mark_notin:
		case cmp_equal:
			op = "==";
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
			TRC_ERROR(SQL_EXECUTION, "Unknown operator\n");
		}

		q = newStmtArgs(mb, batcalcRef, op, 9);
		q = pushArgument(mb, q, l);
		q = pushArgument(mb, q, r);

		/* cands, some already handled the previous selection */
		if (op1->nrcols) {
			if (!sel || !op1->cand)
				q = pushNil(mb, q, TYPE_bat);
			else
				q = pushArgument(mb, q, sel->nr);
		}
		if (op2->nrcols) {
			if (!sel || !op2->cand)
				q = pushNil(mb, q, TYPE_bat);
			else
				q = pushArgument(mb, q, sel->nr);
		}
		if (is_semantics)
			q = pushBit(mb, q, TRUE);
		k = getDestVar(q);

		q = newStmtArgs(mb, algebraRef, selectRef, 9);
		q = pushArgument(mb, q, k);
		q = pushBit(mb, q, !need_not);
		q = pushBit(mb, q, !need_not);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, anti);
		if (q == NULL)
			return NULL;
		if (sel) { /* get back too the correct ids */
			int nr = getDestVar(q);
			q = newStmt(mb, algebraRef, projectionRef);
			q = pushArgument(mb, q, nr);
			q = pushArgument(mb, q, sel->nr);
		}
		k = getDestVar(q);
	} else {
		assert (cmptype != cmp_filter);
		int pushed = 0;
		if (is_semantics) {
			assert(cmptype == cmp_equal || cmptype == cmp_notequal);
			if (cmptype == cmp_notequal)
				anti = !anti;
			q = newStmtArgs(mb, algebraRef, selectRef, 9);
			q = pushArgument(mb, q, l);
			if (!op1->cand && op1->nrcols && sel) /* don't push cands again */
				q = pushArgument(mb, q, sel->nr);
			else {
				q = pushNil(mb, q, TYPE_bat);
				pushed = 1;
			}
			q = pushArgument(mb, q, r);
			q = pushArgument(mb, q, r);
			q = pushBit(mb, q, TRUE);
			q = pushBit(mb, q, TRUE);
			q = pushBit(mb, q, anti);
		} else {
			q = newStmt(mb, algebraRef, thetaselectRef);
			q = pushArgument(mb, q, l);
			if (!op1->cand && op1->nrcols && sel) /* don't push cands again */
				q = pushArgument(mb, q, sel->nr);
			else {
				q = pushNil(mb, q, TYPE_bat);
				pushed = 1;
			}
			q = pushArgument(mb, q, r);
			switch (cmptype) {
			case mark_in:
			case mark_notin: /* we use a anti join, todo handle null (not) in empty semantics */
			case cmp_equal:
				q = pushStr(mb, q, anti?"!=":"==");
				break;
			case cmp_notequal:
				q = pushStr(mb, q, anti?"==":"!=");
				break;
			case cmp_lt:
				q = pushStr(mb, q, anti?">=":"<");
				break;
			case cmp_lte:
				q = pushStr(mb, q, anti?">":"<=");
				break;
			case cmp_gt:
				q = pushStr(mb, q, anti?"<=":">");
				break;
			case cmp_gte:
				q = pushStr(mb, q, anti?"<":">=");
				break;
			default:
				TRC_ERROR(SQL_EXECUTION, "Impossible select compare\n");
				if (q)
					freeInstruction(q);
				q = NULL;
			}
		}
		if (q && sel && pushed) {
			int nr = getDestVar(q);
			q = newStmt(mb, algebraRef, projectionRef);
			q = pushArgument(mb, q, nr);
			q = pushArgument(mb, q, sel->nr);
		}
		if (q == NULL)
			return NULL;
	}

	stmt *s = stmt_create(be->mvc->sa, st_uselect);
	if (s == NULL) {
		freeInstruction(q);
		return NULL;
	}

	s->op1 = op1;
	s->op2 = op2;
	s->op3 = sel;
	s->flag = cmptype;
	s->key = op1->nrcols == 0 && op2->nrcols == 0;
	s->nrcols = op1->nrcols;
	s->nr = getDestVar(q);
	s->q = q;
	s->cand = sel;
	return s;
}

static InstrPtr
select2_join2(backend *be, stmt *op1, stmt *op2, stmt *op3, int cmp, stmt *sel, int anti, int swapped, int type, int reduce)
{
	MalBlkPtr mb = be->mb;
	InstrPtr p, q;
	int l;
	const char *cmd = (type == st_uselect2) ? selectRef : rangejoinRef;

	if (op1->nr < 0 || (sel && sel->nr < 0))
		return NULL;
	l = op1->nr;
	if (((cmp & CMP_BETWEEN && cmp & CMP_SYMMETRIC) || op2->nrcols > 0 || op3->nrcols > 0 || !reduce) && (type == st_uselect2)) {
		int k, nrcols = (op1->nrcols || op2->nrcols || op3->nrcols);

		if (op2->nr < 0 || op3->nr < 0)
			return NULL;

		if (nrcols)
			p = newStmtArgs(mb, batcalcRef, betweenRef, 12);
		else
			p = newStmtArgs(mb, calcRef, betweenRef, 9);
		p = pushArgument(mb, p, l);
		p = pushArgument(mb, p, op2->nr);
		p = pushArgument(mb, p, op3->nr);

		/* cands, some already handled the previous selection */
		if (op1->nrcols) {
			if (!sel || !op1->cand)
				p = pushNil(mb, p, TYPE_bat);
			else
				p = pushArgument(mb, p, sel->nr);
		}
		if (op2->nrcols) {
			if (!sel || !op2->cand)
				p = pushNil(mb, p, TYPE_bat);
			else
				p = pushArgument(mb, p, sel->nr);
		}
		if (op3->nrcols) {
			if (!sel || !op3->cand)
				p = pushNil(mb, p, TYPE_bat);
			else
				p = pushArgument(mb, p, sel->nr);
		}

		p = pushBit(mb, p, (cmp & CMP_SYMMETRIC) != 0); /* symmetric */
		p = pushBit(mb, p, (cmp & 1) != 0);	    /* lo inclusive */
		p = pushBit(mb, p, (cmp & 2) != 0);	    /* hi inclusive */
		p = pushBit(mb, p, FALSE);		    /* nils_false */
		p = pushBit(mb, p, (anti)?TRUE:FALSE);	    /* anti */
		if (!reduce)
			return p;
		k = getDestVar(p);

		q = newStmtArgs(mb, algebraRef, selectRef, 9);
		q = pushArgument(mb, q, k);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, FALSE);
		/* project */
		if (sel) { /* get back too the correct ids */
			int nr = getDestVar(q);
			q = newStmt(mb, algebraRef, projectionRef);
			q = pushArgument(mb, q, nr);
			q = pushArgument(mb, q, sel->nr);
		}
		if (q == NULL)
			return NULL;
	} else {
		int r1 = op2->nr;
		int r2 = op3->nr;
		int rs = 0;
		q = newStmtArgs(mb, algebraRef, cmd, 12);
		if (type == st_join2)
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, l);
		if (!op1->cand && op1->nrcols && sel) /* don't push cands again */
			q = pushArgument(mb, q, sel->nr);
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
		q = pushBit(mb, q, anti);
		if (type == st_uselect2) {
			if (cmp & CMP_BETWEEN)
				q = pushBit(mb, q, TRUE); /* all nil's are != */
		} else {
			q = pushBit(mb, q, (cmp & CMP_SYMMETRIC)?TRUE:FALSE);
		}
		if (type == st_join2)
			q = pushNil(mb, q, TYPE_lng); /* estimate */
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
stmt_uselect2(backend *be, stmt *op1, stmt *op2, stmt *op3, int cmp, stmt *sub, int anti, int reduce)
{
	InstrPtr q = select2_join2(be, op1, op2, op3, cmp, sub, anti, 0, st_uselect2, reduce);

	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_uselect2);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->op1 = op1;
		s->op2 = op2;
		s->op3 = op3;
		s->flag = cmp;
		s->nrcols = op1->nrcols;
		s->key = op1->nrcols == 0 && op2->nrcols == 0 && op3->nrcols == 0;
		s->nr = getDestVar(q);
		s->q = q;
		s->cand = sub;
		s->reduce = reduce;
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
stmt_tdiff(backend *be, stmt *op1, stmt *op2, stmt *lcand)
{
	InstrPtr q = NULL;
	MalBlkPtr mb = be->mb;

	if (op1->nr < 0 || op2->nr < 0)
		return NULL;
	q = newStmt(mb, algebraRef, differenceRef);
	q = pushArgument(mb, q, op1->nr); /* left */
	q = pushArgument(mb, q, op2->nr); /* right */
	if (lcand)
		q = pushArgument(mb, q, lcand->nr); /* left */
	else
		q = pushNil(mb, q, TYPE_bat); /* left candidate */
	q = pushNil(mb, q, TYPE_bat); /* right candidate */
	q = pushBit(mb, q, FALSE);    /* nil matches */
	q = pushBit(mb, q, FALSE);    /* do not clear nils */
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
stmt_tdiff2(backend *be, stmt *op1, stmt *op2, stmt *lcand)
{
	InstrPtr q = NULL;
	MalBlkPtr mb = be->mb;

	if (op1->nr < 0 || op2->nr < 0)
		return NULL;
	q = newStmt(mb, algebraRef, differenceRef);
	q = pushArgument(mb, q, op1->nr); /* left */
	q = pushArgument(mb, q, op2->nr); /* right */
	if (lcand)
		q = pushArgument(mb, q, lcand->nr); /* left */
	else
		q = pushNil(mb, q, TYPE_bat); /* left candidate */
	q = pushNil(mb, q, TYPE_bat); /* right candidate */
	q = pushBit(mb, q, FALSE);     /* nil matches */
	q = pushBit(mb, q, TRUE);     /* not in */
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
stmt_tinter(backend *be, stmt *op1, stmt *op2, bool single)
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
	q = pushBit(mb, q, single?TRUE:FALSE);    /* max_one */
	q = pushNil(mb, q, TYPE_lng); /* estimate */

	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_tinter);
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
stmt_join_cand(backend *be, stmt *op1, stmt *op2, stmt *lcand, stmt *rcand, int anti, comp_type cmptype, int need_left, int is_semantics, bool single)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *sjt = joinRef;

	(void)anti;

	if (need_left) {
		cmptype = cmp_equal;
		sjt = leftjoinRef;
	}
	if (op1->nr < 0 || op2->nr < 0)
		return NULL;

	assert (!single || cmptype == cmp_all);

	switch (cmptype) {
	case mark_in:
	case mark_notin: /* we use a anti join, todo handle null (not) in empty */
	case cmp_equal:
		q = newStmt(mb, algebraRef, sjt);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		if (!lcand || op1->cand == lcand)
			q = pushNil(mb, q, TYPE_bat);
		else
			q = pushArgument(mb, q, lcand->nr);
		if (!rcand)
			q = pushNil(mb, q, TYPE_bat);
		else
			q = pushArgument(mb, q, rcand->nr);
		q = pushBit(mb, q, is_semantics?TRUE:FALSE);
		q = pushNil(mb, q, TYPE_lng);
		if (q == NULL)
			return NULL;
		break;
	case cmp_notequal:
		q = newStmtArgs(mb, algebraRef, thetajoinRef, 9);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		if (!lcand || op1->cand == lcand)
			q = pushNil(mb, q, TYPE_bat);
		else
			q = pushArgument(mb, q, lcand->nr);
		if (!rcand)
			q = pushNil(mb, q, TYPE_bat);
		else
			q = pushArgument(mb, q, rcand->nr);
		q = pushInt(mb, q, JOIN_NE);
		q = pushBit(mb, q, is_semantics?TRUE:FALSE);
		q = pushNil(mb, q, TYPE_lng);
		if (q == NULL)
			return NULL;
		break;
	case cmp_lt:
	case cmp_lte:
	case cmp_gt:
	case cmp_gte:
		q = newStmtArgs(mb, algebraRef, thetajoinRef, 9);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		if (!lcand || op1->cand == lcand)
			q = pushNil(mb, q, TYPE_bat);
		else
			q = pushArgument(mb, q, lcand->nr);
		if (!rcand)
			q = pushNil(mb, q, TYPE_bat);
		else
			q = pushArgument(mb, q, rcand->nr);
		if (cmptype == cmp_lt)
			q = pushInt(mb, q, JOIN_LT);
		else if (cmptype == cmp_lte)
			q = pushInt(mb, q, JOIN_LE);
		else if (cmptype == cmp_gt)
			q = pushInt(mb, q, JOIN_GT);
		else if (cmptype == cmp_gte)
			q = pushInt(mb, q, JOIN_GE);
		q = pushBit(mb, q, is_semantics?TRUE:FALSE);
		q = pushNil(mb, q, TYPE_lng);
		if (q == NULL)
			return NULL;
		break;
	case cmp_all:	/* aka cross table */
		q = newStmt(mb, algebraRef, crossRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		q = pushBit(mb, q, single?TRUE:FALSE); /* max_one */
		assert(!lcand && !rcand);
		if (q == NULL)
			return NULL;
		break;
	case cmp_joined:
		q = op1->q;
		break;
	default:
		TRC_ERROR(SQL_EXECUTION, "Impossible action\n");
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_join);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

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

stmt *
stmt_join(backend *be, stmt *l, stmt *r, int anti, comp_type cmptype, int need_left, int is_semantics, bool single)
{
	return stmt_join_cand(be, l, r, NULL, NULL, anti, cmptype, need_left, is_semantics, single);
}

stmt *
stmt_semijoin(backend *be, stmt *op1, stmt *op2, stmt *lcand, stmt *rcand, int is_semantics, bool single)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (op1->nr < 0 || op2->nr < 0)
		return NULL;

	if (single) {
		q = newStmtArgs(mb, algebraRef, semijoinRef, 9);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	} else
		q = newStmt(mb, algebraRef, intersectRef);
	q = pushArgument(mb, q, op1->nr);
	q = pushArgument(mb, q, op2->nr);
	if (lcand)
		q = pushArgument(mb, q, lcand->nr);
	else
		q = pushNil(mb, q, TYPE_bat);
	if (rcand)
		q = pushArgument(mb, q, rcand->nr);
	else
		q = pushNil(mb, q, TYPE_bat);
	q = pushBit(mb, q, is_semantics?TRUE:FALSE);
	q = pushBit(mb, q, single?TRUE:FALSE); /* max_one */
	q = pushNil(mb, q, TYPE_lng);
	if (q == NULL)
		return NULL;

	stmt *s = stmt_create(be->mvc->sa, st_semijoin);
	if (s == NULL) {
		freeInstruction(q);
		return NULL;
	}

	s->op1 = op1;
	s->op2 = op2;
	s->flag = cmp_equal;
	s->key = 0;
	s->nrcols = 1;
	if (single)
		s->nrcols = 2;
	s->nr = getDestVar(q);
	s->q = q;
	return s;
}

static InstrPtr
stmt_project_join(backend *be, stmt *op1, stmt *op2, bool delta)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (op1->nr < 0 || op2->nr < 0)
		return NULL;
	/* delta bat */
	if (delta) {
		int uval = getArg(op2->q, 1);

		q = newStmt(mb, sqlRef, deltaRef);
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		q = pushArgument(mb, q, uval);
	} else {
		/* projections, ie left is void headed */
		q = newStmt(mb, algebraRef, projectionRef);
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		if (q == NULL)
			return NULL;
	}
	return q;
}

stmt *
stmt_project(backend *be, stmt *op1, stmt *op2)
{
	if (!op2->nrcols)
		return stmt_const(be, op1, NULL, op2);
	InstrPtr q = stmt_project_join(be, op1, op2, false);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_join);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->op1 = op1;
		s->op2 = op2;
		s->flag = cmp_project;
		s->key = 0;
		s->nrcols = MAX(op1->nrcols,op2->nrcols);
		s->nr = getDestVar(q);
		s->q = q;
		s->tname = op2->tname;
		s->cname = op2->cname;
		return s;
	}
	return NULL;
}

stmt *
stmt_project_delta(backend *be, stmt *col, stmt *upd)
{
	InstrPtr q = stmt_project_join(be, col, upd, true);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_join);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->op1 = col;
		s->op2 = upd;
		s->flag = cmp_project;
		s->key = 0;
		s->nrcols = 2;
		s->nr = getDestVar(q);
		s->q = q;
		s->tname = col->tname;
		s->cname = col->cname;
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
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

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
	InstrPtr q = select2_join2(be, l, ra, rb, cmp, NULL, anti, swapped, st_join2, 1/*reduce semantics*/);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_join2);
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

		s->op1 = l;
		s->op2 = ra;
		s->op3 = rb;
		s->flag = cmp;
		s->nrcols = 2;
		s->nr = getDestVar(q);
		s->q = q;
		s->reduce = 1;
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

	if (backend_create_subfunc(be, op, NULL) < 0)
		return NULL;
	mod = sql_func_mod(op->func);
	fimp = sql_func_imp(op->func);
	fimp = sa_strconcat(be->mvc->sa, fimp, "join");

	/* filter qualifying tuples, return oids of h and tail */
	q = newStmtArgs(mb, mod, fimp, list_length(l->op4.lval) + list_length(r->op4.lval) + 7);
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
	q = pushBit(mb, q, anti?TRUE:FALSE); /* 'not' matching */

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
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

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
		if (s == NULL) {
			freeInstruction(q);
			return NULL;
		}

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

#define meta(P, Id, Tpe, Args)						\
	do {											\
		P = newStmtArgs(mb, batRef, packRef, Args);	\
		Id = getArg(P,0);							\
		setVarType(mb, Id, newBatType(Tpe));		\
		setVarFixed(mb, Id);						\
		list = pushArgument(mb, list, Id);			\
	} while (0)

#define metaInfo(P,Tpe,Val)						\
	do {										\
		P = push##Tpe(mb, P, Val);				\
	} while (0)


static int
dump_export_header(mvc *sql, MalBlkPtr mb, list *l, int file, const char * format, const char * sep,const char * rsep,const char * ssep,const char * ns, int onclient)
{
	node *n;
	bool error = false;
	int ret = -1;
	int args;

	// gather the meta information
	int tblId, nmeId, tpeId, lenId, scaleId;
	InstrPtr list;
	InstrPtr tblPtr, nmePtr, tpePtr, lenPtr, scalePtr;

	args = list_length(l) + 1;

	list = newInstructionArgs(mb, sqlRef, export_tableRef, args + 13);
	getArg(list,0) = newTmpVariable(mb,TYPE_int);
	if( file >= 0){
		list = pushArgument(mb, list, file);
		list = pushStr(mb, list, format);
		list = pushStr(mb, list, sep);
		list = pushStr(mb, list, rsep);
		list = pushStr(mb, list, ssep);
		list = pushStr(mb, list, ns);
		list = pushInt(mb, list, onclient);
	}
	meta(tblPtr, tblId, TYPE_str, args);
	meta(nmePtr, nmeId, TYPE_str, args);
	meta(tpePtr, tpeId, TYPE_str, args);
	meta(lenPtr, lenId, TYPE_int, args);
	meta(scalePtr, scaleId, TYPE_int, args);
	if(tblPtr == NULL || nmePtr == NULL || tpePtr == NULL || lenPtr == NULL || scalePtr == NULL)
		return -1;

	for (n = l->h; n; n = n->next) {
		stmt *c = n->data;
		sql_subtype *t = tail_type(c);
		const char *tname = table_name(sql->sa, c);
		const char *sname = schema_name(sql->sa, c);
		const char *_empty = "";
		const char *tn = (tname) ? tname : _empty;
		const char *sn = (sname) ? sname : _empty;
		const char *cn = column_name(sql->sa, c);
		const char *ntn = sql_escape_ident(sql->ta, tn);
		const char *nsn = sql_escape_ident(sql->ta, sn);
		size_t fqtnl;
		char *fqtn = NULL;

		if (ntn && nsn && (fqtnl = strlen(ntn) + 1 + strlen(nsn) + 1) ){
			fqtn = SA_NEW_ARRAY(sql->ta, char, fqtnl);
			if(fqtn) {
				snprintf(fqtn, fqtnl, "%s.%s", nsn, ntn);
				metaInfo(tblPtr, Str, fqtn);
				metaInfo(nmePtr, Str, cn);
				metaInfo(tpePtr, Str, (t->type->localtype == TYPE_void ? "char" : t->type->sqlname));
				metaInfo(lenPtr, Int, t->digits);
				metaInfo(scalePtr, Int, t->scale);
				list = pushArgument(mb, list, c->nr);
			} else
				error = true;
		} else
			error = true;
		if(error)
			return -1;
	}
	sa_reset(sql->ta);
	ret = getArg(list,0);
	pushInstruction(mb,list);
	return ret;
}

stmt *
stmt_export(backend *be, rel_bin_stmt *st, const char *sep, const char *rsep, const char *ssep, const char *null_string, int onclient, stmt *file)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	int fnr;
	list *l = st->cols;

	if (file) {
		if (file->nr < 0)
			return NULL;
		fnr = file->nr;
	} else {
		q = newAssignment(mb);
		q = pushStr(mb,q,"stdout");
		fnr = getArg(q,0);
	}
	if (dump_export_header(be->mvc, mb, l, fnr, "csv", sep, rsep, ssep, null_string, onclient) < 0)
		return NULL;
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_export);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op2 = file;
		s->op4.relstval = st;
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
	case ddl_release:
		q = newStmt(mb, sqlRef, transaction_releaseRef);
		break;
	case ddl_commit:
		q = newStmt(mb, sqlRef, transaction_commitRef);
		break;
	case ddl_rollback:
		q = newStmt(mb, sqlRef, transaction_rollbackRef);
		break;
	case ddl_trans:
		q = newStmt(mb, sqlRef, transaction_beginRef);
		break;
	default:
		TRC_ERROR(SQL_EXECUTION, "Unknown transaction type\n");
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
stmt_catalog(backend *be, int type, list *args)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	/* cast them into properly named operations */
	const char *ref;
	switch(type){
	case ddl_create_seq:			ref = create_seqRef;		break;
	case ddl_alter_seq:				ref = alter_seqRef;			break;
	case ddl_drop_seq:				ref = drop_seqRef;			break;
	case ddl_create_schema:			ref = create_schemaRef;		break;
	case ddl_drop_schema:			ref = drop_schemaRef;		break;
	case ddl_create_table:			ref = create_tableRef;		break;
	case ddl_create_view:			ref = create_viewRef;		break;
	case ddl_drop_table:			ref = drop_tableRef;		break;
	case ddl_drop_view:				ref = drop_viewRef;			break;
	case ddl_drop_constraint:		ref = drop_constraintRef;	break;
	case ddl_alter_table:			ref = alter_tableRef;		break;
	case ddl_create_type:			ref = create_typeRef;		break;
	case ddl_drop_type:				ref = drop_typeRef;			break;
	case ddl_grant_roles:			ref = grant_rolesRef;		break;
	case ddl_revoke_roles:			ref = revoke_rolesRef;		break;
	case ddl_grant:					ref = grantRef;				break;
	case ddl_revoke:				ref = revokeRef;			break;
	case ddl_grant_func:			ref = grant_functionRef;	break;
	case ddl_revoke_func:			ref = revoke_functionRef;	break;
	case ddl_create_user:			ref = create_userRef;		break;
	case ddl_drop_user:				ref = drop_userRef;			break;
	case ddl_alter_user:			ref = alter_userRef;		break;
	case ddl_rename_user:			ref = rename_userRef;		break;
	case ddl_create_role:			ref = create_roleRef;		break;
	case ddl_drop_role:				ref = drop_roleRef;			break;
	case ddl_drop_index:			ref = drop_indexRef;		break;
	case ddl_drop_function:			ref = drop_functionRef;		break;
	case ddl_create_function:		ref = create_functionRef;	break;
	case ddl_create_trigger:		ref = create_triggerRef;	break;
	case ddl_drop_trigger:			ref = drop_triggerRef;		break;
	case ddl_alter_table_add_table:	ref = alter_add_tableRef;	break;
	case ddl_alter_table_del_table:	ref = alter_del_tableRef;	break;
	case ddl_alter_table_set_access:ref = alter_set_tableRef;	break;
	case ddl_alter_table_add_range_partition: ref = alter_add_range_partitionRef; break;
	case ddl_alter_table_add_list_partition: ref = alter_add_value_partitionRef; break;
	case ddl_comment_on:			ref = comment_onRef;		break;
	case ddl_rename_schema:			ref = rename_schemaRef;		break;
	case ddl_rename_table:			ref = rename_tableRef;		break;
	case ddl_rename_column:			ref = rename_columnRef;		break;
	default:
		TRC_ERROR(SQL_EXECUTION, "Unknown catalog operation\n");
		return NULL;
	}
	q = newStmtArgs(mb, sqlcatalogRef, ref, list_length(args) + 1);
	// pass all arguments as before
	for (node *n = args->h; n; n = n->next) {
		stmt *c = n->data;

		q = pushArgument(mb, q, c->nr);
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_catalog);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op4.lval = args;
		s->flag = type;
		s->q = q;
		s->nr = getDestVar(q);
		return s;
	}
	return NULL;
}

stmt *
stmt_list(backend *be, list *l)
{
	unsigned nrcols = 0;
	int key = 1;
	stmt *s = stmt_create(be->mvc->sa, st_list);

	if (!s)
		return NULL;

	for (node *n = l->h; n; n = n->next) { /* set nr cols */
		stmt *f = n->data;

		if (!f)
			continue;
		if (f->nrcols > nrcols)
			nrcols = f->nrcols;
		key &= f->key;
		s->nr = f->nr;
	}
	s->nrcols = nrcols;
	s->key = key;
	s->op4.lval = l;
	return s;
}

static InstrPtr
dump_header(mvc *sql, MalBlkPtr mb, list *l)
{
	node *n;
	bool error = false;
	// gather the meta information
	int tblId, nmeId, tpeId, lenId, scaleId;
	int args;
	InstrPtr list;
	InstrPtr tblPtr, nmePtr, tpePtr, lenPtr, scalePtr;

	args = list_length(l) + 1;

	list = newInstructionArgs(mb,sqlRef, resultSetRef, args + 5);
	if(!list) {
		return NULL;
	}
	getArg(list,0) = newTmpVariable(mb,TYPE_int);
	meta(tblPtr, tblId, TYPE_str, args);
	meta(nmePtr, nmeId, TYPE_str, args);
	meta(tpePtr, tpeId, TYPE_str, args);
	meta(lenPtr, lenId, TYPE_int, args);
	meta(scalePtr, scaleId, TYPE_int, args);
	if(tblPtr == NULL || nmePtr == NULL || tpePtr == NULL || lenPtr == NULL || scalePtr == NULL)
		return NULL;

	for (n = l->h; n; n = n->next) {
		stmt *c = n->data;
		sql_subtype *t = tail_type(c);
		const char *tname = table_name(sql->sa, c);
		const char *sname = schema_name(sql->sa, c);
		const char *_empty = "";
		const char *tn = (tname) ? tname : _empty;
		const char *sn = (sname) ? sname : _empty;
		const char *cn = column_name(sql->sa, c);
		const char *ntn = sql_escape_ident(sql->ta, tn);
		const char *nsn = sql_escape_ident(sql->ta, sn);
		size_t fqtnl;
		char *fqtn = NULL;

		if (ntn && nsn && (fqtnl = strlen(ntn) + 1 + strlen(nsn) + 1) ){
			fqtn = SA_NEW_ARRAY(sql->ta, char, fqtnl);
			if(fqtn) {
				snprintf(fqtn, fqtnl, "%s.%s", nsn, ntn);
				metaInfo(tblPtr,Str,fqtn);
				metaInfo(nmePtr,Str,cn);
				metaInfo(tpePtr,Str,(t->type->localtype == TYPE_void ? "char" : t->type->sqlname));
				metaInfo(lenPtr,Int,t->digits);
				metaInfo(scalePtr,Int,t->scale);
				list = pushArgument(mb,list,c->nr);
			} else
				error = true;
		} else
			error = true;
		if (error)
			return NULL;
	}
	sa_reset(sql->ta);
	pushInstruction(mb,list);
	return list;
}

int
stmt_output(backend *be, rel_bin_stmt *st)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	list *l = st->cols;
	int cnt = list_length(l), ok = 0;
	node *n = l->h;
	stmt *first = n->data;

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
		const char *ntn = sql_escape_ident(be->mvc->ta, tn);
		const char *nsn = sql_escape_ident(be->mvc->ta, sn);
		size_t fqtnl;
		char *fqtn = NULL;

		if (ntn && nsn) {
			fqtnl = strlen(ntn) + 1 + strlen(nsn) + 1;
			fqtn = SA_NEW_ARRAY(be->mvc->ta, char, fqtnl);
			if (fqtn) {
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
		sa_reset(be->mvc->ta);
		if (!ok)
			return -1;
	} else {
		if ((q = dump_header(be->mvc, mb, l)) == NULL)
			return -1;
	}
	return 0;
}

int
stmt_affected_rows(backend *be, int lastnr)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	q = newStmt(mb, sqlRef, affectedRowsRef);
	q = pushArgument(mb, q, be->mvc_var);
	if (q == NULL)
		return -1;
	getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
	q = pushArgument(mb, q, lastnr);
	if (q == NULL)
		return -1;
	be->mvc_var = getDestVar(q);
	return 0;
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
stmt_append_bulk(backend *be, stmt *c, list *l)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	bool needs_columns = false;

	if (c->nr < 0)
		return NULL;

	/* currently appendBulk accepts its inputs all either scalar or vectors
	   if there is one vector and any scala, then the scalars mut be upgraded to vectors */
	for (node *n = l->h; n; n = n->next) {
		stmt *t = n->data;
		needs_columns |= t->nrcols > 0;
	}
	if (needs_columns) {
		for (node *n = l->h; n; n = n->next) {
			stmt *t = n->data;
			if (t->nrcols == 0)
				n->data = const_column(be, t);
		}
	}

	q = newStmtArgs(mb, batRef, appendBulkRef, list_length(l) + 3);
	q = pushArgument(mb, q, c->nr);
	q = pushBit(mb, q, TRUE);
	for (node *n = l->h ; n ; n = n->next) {
		stmt *a = n->data;
		q = pushArgument(mb, q, a->nr);
	}
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_append_bulk);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = c;
		s->op4.lval = l;
		s->nrcols = c->nrcols;
		s->key = c->key;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_claim(backend *be, sql_table *t, stmt *cnt)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (!t || cnt->nr < 0)
		return NULL;
	if (!t->s) /* declared table */
		assert(0);
	q = newStmtArgs(mb, sqlRef, claimRef, 5);
	q = pushArgument(mb, q, be->mvc_var);
	q = pushSchema(mb, q, t);
	q = pushStr(mb, q, t->base.name);
	q = pushArgument(mb, q, cnt->nr);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_claim);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = cnt;
		s->op4.tval = t;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_replace(backend *be, stmt *r, stmt *id, stmt *val)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (r->nr < 0)
		return NULL;

	q = newStmt(mb, batRef, replaceRef);
	q = pushArgument(mb, q, r->nr);
	q = pushArgument(mb, q, id->nr);
	q = pushArgument(mb, q, val->nr);
	q = pushBit(mb, q, TRUE); /* forced */
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_replace);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = r;
		s->op2 = id;
		s->op3 = val;
		s->nrcols = r->nrcols;
		s->key = r->key;
		s->nr = getDestVar(q);
		s->q = q;
		s->cand = r->cand;
		return s;
	}
	return NULL;
}

stmt *
stmt_table_clear(backend *be, sql_table *t)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (!t->s && ATOMIC_PTR_GET(&t->data)) { /* declared table */
		int *l = ATOMIC_PTR_GET(&t->data), cnt = ol_length(t->columns)+1;

		for (int i = 0; i < cnt; i++) {
			q = newStmt(mb, batRef, deleteRef);
			q = pushArgument(mb, q, l[i]);
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

/* The type setting is not propagated to statements such as st_bat and st_append,
	because they are not considered projections */
static void
tail_set_type(stmt *st, sql_subtype *t)
{
	for (;;) {
		switch (st->type) {
		case st_const:
			st = st->op2;
			continue;
		case st_alias:
		case st_gen_group:
		case st_order:
			st = st->op1;
			continue;
		case st_list:
			st = st->op4.lval->h->data;
			continue;
		case st_table:
			if (!list_empty(st->op4.relstval->cols)) {
				st = st->op4.relstval->cols->h->data;
				continue;
			}
			return;
		case st_join:
		case st_join2:
		case st_joinN:
			if (st->flag == cmp_project) {
				st = st->op2;
				continue;
			}
			return;
		case st_aggr:
		case st_Nop: {
			list *res = st->op4.funcval->res;

			if (res && list_length(res) == 1)
				res->h->data = t;
			return;
		}
		case st_atom:
			atom_set_type(st->op4.aval, t);
			return;
		case st_convert:
		case st_temp:
		case st_single:
			st->op4.typeval = *t;
			return;
		case st_var:
			if (st->op4.typeval.type)
				st->op4.typeval = *t;
			return;
		default:
			return;
		}
	}
}

#define trivial_string_conversion(x) ((x) == EC_BIT || (x) == EC_CHAR || (x) == EC_STRING || (x) == EC_NUM || (x) == EC_POS || (x) == EC_FLT \
									  || (x) == EC_DATE || (x) == EC_BLOB || (x) == EC_MONTH)

stmt *
stmt_convert(backend *be, stmt *v, stmt *sel, sql_subtype *f, sql_subtype *t)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *convert = t->type->base.name;
	int push_cands = (t->type->eclass != EC_EXTERNAL || !strcmp(convert, "uuid")); /* uuids conversions support candidate lists */
	/* convert types and make sure they are rounded up correctly */

	if (v->nr < 0)
		return NULL;

	if (f->type->eclass != EC_EXTERNAL && t->type->eclass != EC_EXTERNAL &&
		/* general cases */
		((t->type->localtype == f->type->localtype && t->type->eclass == f->type->eclass &&
		!EC_INTERVAL(f->type->eclass) && f->type->eclass != EC_DEC && (t->digits == 0 || f->digits == t->digits) && type_has_tz(t) == type_has_tz(f)) ||
		/* trivial decimal cases */
		(f->type->eclass == EC_DEC && t->type->eclass == EC_DEC && f->scale == t->scale && f->type->localtype == t->type->localtype) ||
		/* trivial string cases */
		(EC_VARCHAR(f->type->eclass) && EC_VARCHAR(t->type->eclass) && (t->digits == 0 || (f->digits > 0 && t->digits >= f->digits))))) {
		/* set output type. Despite the MAL code already being generated, the output type may still be checked */
		tail_set_type(v, t);
		return v;
	}

	/* external types have sqlname convert functions,
	   these can generate errors (fromstr cannot) */
	if (t->type->eclass == EC_EXTERNAL)
		convert = t->type->sqlname;
	else if (t->type->eclass == EC_MONTH)
		convert = "month_interval";
	else if (t->type->eclass == EC_SEC)
		convert = "second_interval";

	/* Lookup the sql convert function, there is no need
	 * for single value vs bat, this is handled by the
	 * mal function resolution */
	if (v->nrcols == 0 && (!sel || sel->nrcols == 0)) {	/* simple calc */
		q = newStmtArgs(mb, calcRef, convert, 13);
	} else if ((v->nrcols > 0 || (sel && sel->nrcols > 0)) && !push_cands) {
		int type = t->type->localtype;

		/* with our current implementation, all internal SQL types have candidate list support on their conversions */
		if (sel && !v->cand)
			v = stmt_project_column_on_cand(be, sel, v);
		q = newStmtArgs(mb, malRef, multiplexRef, 15);
		if (q == NULL)
			return NULL;
		setVarType(mb, getArg(q, 0), newBatType(type));
		q = pushStr(mb, q, convertMultiplexMod(calcRef, convert));
		q = pushStr(mb, q, convertMultiplexFcn(convert));
	} else {
		if (v->nrcols == 0 && sel && !v->cand)
			v = stmt_const(be, sel, sel, v);
		q = newStmtArgs(mb, batcalcRef, convert, 13);
	}

	/* convert to string is complex, we need full type info and mvc for the timezone */
	if (EC_VARCHAR(t->type->eclass) && !(trivial_string_conversion(f->type->eclass) && t->digits == 0)) {
		q = pushInt(mb, q, f->type->eclass);
		q = pushInt(mb, q, f->digits);
		q = pushInt(mb, q, f->scale);
		q = pushInt(mb, q, type_has_tz(f));
	} else if (f->type->eclass == EC_DEC) {
		/* scale of the current decimal */
		q = pushInt(mb, q, f->scale);
	} else if (f->type->eclass == EC_SEC && (EC_COMPUTE(t->type->eclass) || t->type->eclass == EC_DEC)) {
		/* scale of the current decimal */
		q = pushInt(mb, q, 3);
	}
	q = pushArgument(mb, q, v->nr);
	if (v->nrcols > 0 && push_cands) {
		if (v->cand || !sel) { /* Don't push cands twice */
			q = pushNil(mb, q, TYPE_bat);
		} else {
			q = pushArgument(mb, q, sel->nr);
		}
	}
	if (t->type->eclass == EC_DEC || EC_TEMP_FRAC(t->type->eclass) || EC_INTERVAL(t->type->eclass)) {
		/* digits, scale of the result decimal */
		q = pushInt(mb, q, t->digits);
		if (!EC_TEMP_FRAC(t->type->eclass))
			q = pushInt(mb, q, t->scale);
	}
	/* convert to string, give error on to large strings */
	if (EC_VARCHAR(t->type->eclass) && !(trivial_string_conversion(f->type->eclass) && t->digits == 0))
		q = pushInt(mb, q, t->digits);
	/* convert a string to a time(stamp) with time zone */
	if (EC_VARCHAR(f->type->eclass) && EC_TEMP_TZ(t->type->eclass))
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
		s->cand = sel;
		return s;
	}
	return NULL;
}

stmt *
stmt_unop(backend *be, stmt *op1, stmt *sel, sql_subfunc *op)
{
	return stmt_Nop(be, stmt_list(be, list_append(sa_list(be->mvc->sa), op1)), sel, op);
}

stmt *
stmt_binop(backend *be, stmt *op1, stmt *op2, stmt *sel, sql_subfunc *op)
{
	list *ops = sa_list(be->mvc->sa);
	list_append(ops, op1);
	list_append(ops, op2);
	return stmt_Nop(be, stmt_list(be, ops), sel, op);
}

stmt *
stmt_Nop(backend *be, stmt *ops, stmt *sel, sql_subfunc *f)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *mod = sql_func_mod(f->func), *fimp = sql_func_imp(f->func);
	sql_subtype *tpe = NULL;
	int push_cands = (strcmp(mod, "calc") == 0 || strcmp(mod, "mmath") == 0 || strcmp(mod, "mtime") == 0 || strcmp(mod, "mkey") == 0 ||
		(strcmp(mod, "str") == 0 && batstr_func_has_candidates(fimp)) || strcmp(mod, "algebra") == 0 || strcmp(mod, "blob") == 0);
	int pushed = 0, nrcols = 0;
	node *n;
	stmt *o = NULL;

	if (list_length(ops->op4.lval)) {
		for (n = ops->op4.lval->h, o = n->data; n; n = n->next) {
			stmt *op = n->data;

			if (!push_cands && sel && op->nrcols > 0 && !op->cand){ /* don't push cands twice */
				n->data = op = stmt_project_column_on_cand(be, sel, op);
				pushed = 1;
			}
			if (o->nrcols < op->nrcols)
				o = op;
			if (op->nrcols)
				nrcols++;
		}
	}

	/* handle nullif */
	if (list_length(ops->op4.lval) == 2 && strcmp(mod, "") == 0 && strcmp(fimp, "") == 0) {
		stmt *e1 = ops->op4.lval->h->data;
		stmt *e2 = ops->op4.lval->h->next->data;
		int nrcols = 0;

		nrcols = e1->nrcols>e2->nrcols ? e1->nrcols:e2->nrcols;
		/* nullif(e1,e2) -> ifthenelse(e1==e2),NULL,e1) */
		if (strcmp(f->func->base.name, "nullif") == 0) {
			const char *mod = (!nrcols)?calcRef:batcalcRef;
			sql_subtype *t = tail_type(e1);
			int tt = t->type->localtype;
			q = newStmt(mb, mod, "==");
			q = pushArgument(mb, q, e1->nr);
			q = pushArgument(mb, q, e2->nr);
			if (e1->nrcols)
				q = pushNil(mb, q, TYPE_bat);
			if (e2->nrcols)
				q = pushNil(mb, q, TYPE_bat);
			int nr = getDestVar(q);

			q = newStmt(mb, mod, "ifthenelse");
			q = pushArgument(mb, q, nr);
			q = pushNil(mb, q, tt);
			q = pushArgument(mb, q, e1->nr);
		}
	}
	if (!q) {
		int default_nargs = (f->res && list_length(f->res) ? list_length(f->res) : 1) + list_length(ops->op4.lval) + (o && o->nrcols > 0 ? 6 : 4);

		if (backend_create_subfunc(be, f, ops->op4.lval) < 0)
			return NULL;
		mod = sql_func_mod(f->func);
		fimp = convertMultiplexFcn(sql_func_imp(f->func));
		if (o && o->nrcols > 0 && f->func->type != F_LOADER && f->func->type != F_PROC) {
			sql_subtype *res = f->res->h->data;

			if (push_cands) {
				char batmodule[16];
				stpcpy(stpcpy(batmodule, "bat"), mod);
				q = newStmtArgs(mb, batmodule, fimp, default_nargs);
			} else {
				q = newStmtArgs(mb, f->func->type == F_UNION ? batmalRef : malRef, multiplexRef, default_nargs);
				q = pushStr(mb, q, mod);
				q = pushStr(mb, q, fimp);
			}
			setVarType(mb, getArg(q, 0), newBatType(res->type->localtype));
		} else {
			q = newStmtArgs(mb, mod, fimp, default_nargs);

			if (f->res && list_length(f->res)) {
				sql_subtype *res = f->res->h->data;

				setVarType(mb, getArg(q, 0), res->type->localtype);
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

		for (n = ops->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;
			q = pushArgument(mb, q, op->nr);
		}
		/* push candidate lists if that's the case */
		if (push_cands && (f->func->type == F_FUNC || f->func->type == F_FILT) && f->func->lang == FUNC_LANG_INT) {
			for (n = ops->op4.lval->h; n; n = n->next) {
				stmt *op = n->data;

				if (op->nrcols > 0) {
					if (!sel || op->cand) { /* Don't push cands twice */
						q = pushNil(mb, q, TYPE_bat);
					} else {
						q = pushArgument(mb, q, sel->nr);
					}
					pushed = 1;
				}
			}
		}
		/* special case for round function on decimals */
		if (strcmp(mod, "calc") == 0 && strcmp(fimp, "round") == 0 && tpe && tpe->type->eclass == EC_DEC && ops->op4.lval->h && ops->op4.lval->h->data) {
			q = pushInt(mb, q, tpe->digits);
			q = pushInt(mb, q, tpe->scale);
		}
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
		s->cand = (pushed || (!push_cands && sel && nrcols))?sel:NULL;
		return s;
	}
	return NULL;
}

stmt *
stmt_direct_func(backend *be, InstrPtr q)
{
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_func);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->flag = op_union;
		s->nrcols = 3;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}

stmt *
stmt_func(backend *be, stmt *ops, stmt *sel, const char *name, sql_rel *rel, int f_union)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *mod = "user";
	prop *p = NULL;
	sql_allocator *sa = be->mvc->sa;
	stmt *o = NULL, *s = NULL;

	/* dump args */
	if (ops && ops->nr < 0)
		return NULL;

	p = find_prop(rel->p, PROP_REMOTE);
	if (p)
		rel->p = prop_remove(rel->p, p);
	rel = sql_processrelation(be->mvc, rel, 1, 1);
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
	if (ops && list_length(ops->op4.lval)) {
		o = ops->op4.lval->h->data;
		for (node *n = ops->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;

			if (sel && op->nrcols > 0 && !op->cand) /* don't push cands twice */
				op = stmt_project_column_on_cand(be, sel, op);
			if (o->nrcols < op->nrcols)
				o = op;
			q = pushArgument(mb, q, op->nr);
		}
	}

	if (q) {
		if (!(s = stmt_create(sa, st_func))) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = ops;
		s->op2 = stmt_atom_string(be, name);
		s->op4.rel = rel;
		s->flag = f_union;

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
		s->cand = sel;
		return s;
	}
	return NULL;
}

stmt *
stmt_aggr(backend *be, stmt *op1, stmt *op1_cand, stmt *grp, stmt *ext, sql_subfunc *op, int reduce, int no_nil, int nil_if_empty)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *mod, *aggrfunc;
	sql_subtype *res = op->res->h->data;
	int restype = res->type->localtype;
	bool complex_aggr = false;
	bool abort_on_error;
	int *stmt_nr = NULL;
	int avg = 0, pushed = 0;

	if (op1->nr < 0)
		return NULL;
	if (backend_create_subfunc(be, op, NULL) < 0)
		return NULL;
	mod = op->func->mod;
	aggrfunc = op->func->imp;

	if (strcmp(aggrfunc, "avg") == 0)
		avg = 1;
	if (avg || strcmp(aggrfunc, "sum") == 0 || strcmp(aggrfunc, "prod") == 0
		|| strcmp(aggrfunc, "str_group_concat") == 0)
		complex_aggr = true;
	if (restype == TYPE_dbl)
		avg = 0;
	/* some "sub" aggregates have an extra argument "abort_on_error" */
	abort_on_error = complex_aggr || strncmp(aggrfunc, "stdev", 5) == 0 || strncmp(aggrfunc, "variance", 8) == 0 ||
					strncmp(aggrfunc, "covariance", 10) == 0 || strncmp(aggrfunc, "corr", 4) == 0;

	int argc = 1
		+ 2 * avg
		+ (LANG_EXT(op->func->lang) != 0)
		+ 2 * (op->func->lang == FUNC_LANG_C || op->func->lang == FUNC_LANG_CPP)
		+ (op->func->lang == FUNC_LANG_PY || op->func->lang == FUNC_LANG_R)
		+ (op1->type != st_list ? 1 : list_length(op1->op4.lval))
		+ (grp ? 4 : avg + 1);

	if (ext) {
		char *aggrF = SA_NEW_ARRAY(be->mvc->sa, char, strlen(aggrfunc) + 4);
		if (!aggrF)
			return NULL;
		stpcpy(stpcpy(aggrF, "sub"), aggrfunc);
		aggrfunc = aggrF;
		if (grp && (grp->nr < 0 || ext->nr < 0))
			return NULL;

		q = newStmtArgs(mb, mod, aggrfunc, argc);
		if (q == NULL)
			return NULL;
		setVarType(mb, getArg(q, 0), newBatType(restype));
		if (avg) { /* for avg also return rest and count */
			q = pushReturn(mb, q, newTmpVariable(mb, newBatType(TYPE_lng)));
			q = pushReturn(mb, q, newTmpVariable(mb, newBatType(TYPE_lng)));
		}
	} else {
		q = newStmtArgs(mb, mod, aggrfunc, argc);
		if (q == NULL)
			return NULL;
		if (complex_aggr) {
			setVarType(mb, getArg(q, 0), restype);
			if (avg) { /* for avg also return rest and count */
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

	if (op1->type != st_list) {
		q = pushArgument(mb, q, op1->nr);
		if (op1_cand && op1->cand == op1_cand)
			pushed = 1;
	} else {
		int i;
		node *n;

		for (i=0, n = op1->op4.lval->h; n; n = n->next, i++) {
			stmt *op = n->data;

			if (stmt_nr)
				q = pushArgument(mb, q, stmt_nr[i]);
			else
				q = pushArgument(mb, q, op->nr);
			if (op1_cand && op->cand == op1_cand && op->nrcols)
				pushed = 1;
		}
	}
	if (grp) {
		q = pushArgument(mb, q, grp->nr);
		q = pushArgument(mb, q, ext->nr);
		/* push candidates */
		if (op1_cand && !pushed)
			q = pushArgument(mb, q, op1_cand->nr);
		else if (avg)
			q = pushNil(mb, q, TYPE_bat);
		if (q == NULL)
			return NULL;
		q = pushBit(mb, q, no_nil);
		if (!avg && abort_on_error)
			q = pushBit(mb, q, TRUE);
	} else if (no_nil && strncmp(aggrfunc, "count", 5) == 0) {
		q = pushBit(mb, q, no_nil);
	} else if (!nil_if_empty && strncmp(aggrfunc, "sum", 3) == 0) {
		q = pushBit(mb, q, FALSE);
	} else if (avg) { /* push candidates */
		q = pushNil(mb, q, TYPE_bat);
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
		s->op4.funcval = op;
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
	s->cand = op1->cand;

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
		case st_semijoin:
		case st_limit:
		case st_limit2:
		case st_sample:
		case st_tunion:
		case st_tdiff:
		case st_tinter:
			return sql_bind_localtype("oid");
		case st_uselect2:
			if (!st->reduce)
				return sql_bind_localtype("bit");
			return sql_bind_localtype("oid");
		case st_append:
		case st_append_bulk:
		case st_replace:
		case st_alias:
		case st_gen_group:
		case st_order:
			st = st->op1;
			continue;
		case st_table:
			if (!list_empty(st->op4.relstval->cols)) {
				st = st->op4.relstval->cols->h->data;
				continue;
			}
			return NULL;
		case st_list:
			st = st->op4.lval->h->data;
			continue;
		case st_bat:
			return &st->op4.cval->type;
		case st_idxbat:
			if (hash_index(st->op4.idxval->type)) {
				return sql_bind_localtype("lng");
			} else if (oid_index(st->op4.idxval->type)) {
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
		case st_aggr:
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
	case st_semijoin:
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
	size_t l1 = _strlen(n1), l2;

	if (!sa)
		return n1;
	if (!n2)
		return sa_strdup(sa, n1);
	l2 = _strlen(n2);

	if (l2 > 16) {		/* only support short names */
		char *ns = SA_NEW_ARRAY(sa, char, l2 + 1);
		if(!ns)
			return NULL;
		snprintf(ns, l2 + 1, "%s", n2);
		return ns;
	} else {
		char *ns = SA_NEW_ARRAY(sa, char, l1 + l2 + 2), *s = ns;
		if(!ns)
			return NULL;
		snprintf(ns, l1 + l2 + 2, "%s_%s", n1, n2);
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
	case st_append_bulk:
	case st_replace:
	case st_gen_group:
	case st_semijoin:
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
	case st_aggr:
	{
		const char *cn = column_name(sa, st->op1);
		return func_name(sa, st->op4.funcval->func->base.name, cn);
	}
	case st_alias:
		if (st->op3)
			return column_name(sa, st->op3);
		break;
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
	case st_table:
		if (!list_empty(st->op4.relstval->cols))
			return column_name(sa, st->op4.relstval->cols->h->data);
		return NULL;
	case st_list:
		if (list_length(st->op4.lval))
			return column_name(sa, st->op4.lval->h->data);
		/* fall through */
	case st_rs_column:
		return NULL;
	default:
		return NULL;
	}
	return NULL;
}

const char *
table_name(sql_allocator *sa, stmt *st)
{
	(void)sa;
	return st->tname;
}

const char *
schema_name(sql_allocator *sa, stmt *st)
{
	switch (st->type) {
	case st_const:
	case st_semijoin:
	case st_join:
	case st_join2:
	case st_joinN:
		return schema_name(sa, st->op2);
	case st_mirror:
	case st_group:
	case st_result:
	case st_append:
	case st_append_bulk:
	case st_replace:
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
		sql_subfunc *not = sql_bind_func(be->mvc, "sys", "not", bt, NULL, F_FUNC);
		sql_subfunc *or = sql_bind_func(be->mvc, "sys", "or", bt, bt, F_FUNC);
		sql_subfunc *isnull = sql_bind_func(be->mvc, "sys", "isnull", bt, NULL, F_FUNC);
		cond = stmt_binop(be,
			stmt_unop(be, cond, NULL, not),
			stmt_unop(be, cond, NULL, isnull), NULL, or);
	}
	if (!loop) {	/* if */
		q = newAssignment(mb);
		if (q == NULL)
			return NULL;
		q->barrier = BARRIERsymbol;
		q = pushArgument(mb, q, cond->nr);
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
	stmt *s = stmt_create(be->mvc->sa, st_control_end);
	if(!s) {
		freeInstruction(q);
		return NULL;
	}
	s->op1 = cond;
	s->nr = getArg(q, 0);
	return s;
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
	int args = 0;
	if (val->type == st_table)
		args = 2 * list_length(val->op4.relstval->cols);
	if (args < MAXARG)
		args = MAXARG;
	q = newInstructionArgs(mb, NULL, NULL, args);
	if (q == NULL)
		return NULL;
	q->barrier= RETURNsymbol;
	if (val->type == st_table) {
		q = dump_cols(mb, val->op4.relstval->cols, q);
	} else {
		getArg(q, 0) = getArg(getInstrPtr(mb, 0), 0);
		q = pushArgument(mb, q, val->nr);
	}
	if (q == NULL)
		return NULL;
	pushInstruction(mb, q);

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

stmt *
stmt_assign(backend *be, const char *sname, const char *varname, stmt *val, int level)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (val && val->nr < 0)
		return NULL;
	if (level != 0) {
		char *buf,  levelstr[16];

		if (!val) {
			/* drop declared table */
			assert(0);
		}

		assert(!sname);
		snprintf(levelstr, sizeof(levelstr), "%d", level);
		buf = SA_NEW_ARRAY(be->mvc->sa, char, strlen(levelstr) + strlen(varname) + 3);
		if (!buf)
			return NULL;
		stpcpy(stpcpy(stpcpy(stpcpy(buf, "A"), levelstr), "%"), varname); /* mangle variable name */
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
		assert(sname); /* all global variables have a schema */
		q = newStmt(mb, sqlRef, setVariableRef);
		q = pushArgument(mb, q, be->mvc_var);
		q = pushStr(mb, q, sname);
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
	q = newStmt(mb, batRef, singleRef);
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

stmt *
stmt_fetch(backend *be, stmt *val)
{
	sql_subtype *ct;
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	int tt;

	if (val->nr < 0)
		return NULL;
	/* pick from first column on a table case */
	if (val->type == st_table) {
		if (list_length(val->op4.relstval->cols) > 1)
			return NULL;
		val = val->op4.relstval->cols->h->data;
	}
	ct = tail_type(val);
	tt = ct->type->localtype;

	q = newStmt(mb, algebraRef, fetchRef);
	if (q == NULL)
		return NULL;
	setVarType(mb, getArg(q, 0), tt);
	q = pushArgument(mb, q, val->nr);
	q = pushOid(mb, q, 0);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_single);
		if(!s) {
			freeInstruction(q);
			return NULL;
		}
		s->op1 = val;
		s->op4.typeval = *ct;
		s->nrcols = 0;

		s->tname = val->tname;
		s->cname = val->cname;
		s->nr = getDestVar(q);
		s->q = q;
		return s;
	}
	return NULL;
}
