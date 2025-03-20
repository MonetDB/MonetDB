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
#include "sql_mem.h"
#include "sql_stack.h"
#include "sql_statement.h"
#include "sql_gencode.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_prop.h"

#include "mal_namespace.h"
#include "mal_builder.h"

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
multiplex2(MalBlkPtr mb, const char *mod, const char *name, int o1, int o2, int rtype)
{
	InstrPtr q = NULL;

	q = newStmt(mb, malRef, multiplexRef);
	if (q == NULL)
		return NULL;
	setVarType(mb, getArg(q, 0), newBatType(rtype));
	q = pushStr(mb, q, convertMultiplexMod(mod, name));
	q = pushStr(mb, q, convertMultiplexFcn(name));
	q = pushArgument(mb, q, o1);
	q = pushArgument(mb, q, o2);
	pushInstruction(mb, q);
	return q;
}

static InstrPtr
dump_1(MalBlkPtr mb, const char *mod, const char *name, stmt *o1)
{
	InstrPtr q = NULL;

	if (o1 == NULL || o1->nr < 0)
		return NULL;
	q = newStmt(mb, mod, name);
	q = pushArgument(mb, q, o1->nr);
	pushInstruction(mb, q);
	return q;
}

static InstrPtr
dump_2(MalBlkPtr mb, const char *mod, const char *name, stmt *o1, stmt *o2)
{
	InstrPtr q = NULL;

	if (o1 == NULL || o2 == NULL || o1->nr < 0 || o2->nr < 0)
		return NULL;
	q = newStmt(mb, mod, name);
	q = pushArgument(mb, q, o1->nr);
	q = pushArgument(mb, q, o2->nr);
	pushInstruction(mb, q);
	return q;
}

InstrPtr
pushPtr(MalBlkPtr mb, InstrPtr q, ptr val)
{
	int _t;
	ValRecord cst;

	if (q == NULL || mb->errors)
		return q;
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
	const char *s = sa_strdup(be->mvc->sa, S);
	sql_subtype t;

	if (s == NULL)
		return NULL;
	sql_find_subtype(&t, "varchar", _strlen(s), 0);
	return stmt_atom(be, atom_string(be->mvc->sa, &t, s));
}

stmt *
stmt_atom_string_nil(backend *be)
{
	sql_subtype t;

	sql_find_subtype(&t, "varchar", 0, 0);
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
	return stmt_atom(be, atom_general(be->mvc->sa, &t, NULL, 0));
}

stmt *
stmt_bool(backend *be, int b)
{
	sql_subtype t;

	sql_find_subtype(&t, "boolean", 0, 0);

	if (b == bit_nil) {
		return stmt_atom(be, atom_bool(be->mvc->sa, &t, bit_nil));
	} else if (b) {
		return stmt_atom(be, atom_bool(be->mvc->sa, &t, TRUE));
	} else {
		return stmt_atom(be, atom_bool(be->mvc->sa, &t, FALSE));
	}
}

static stmt *
stmt_create(allocator *sa, st_type type)
{
	stmt *s = SA_NEW(sa, stmt);

	if (!s)
		return NULL;
	*s = (stmt) {
		.type = type,
	};
	return s;
}

stmt *
stmt_group(backend *be, stmt *s, stmt *grp, stmt *ext, stmt *cnt, int done)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (s == NULL || s->nr < 0)
		goto bailout;
	if (grp && (grp->nr < 0 || ext->nr < 0 || cnt->nr < 0))
		goto bailout;

	q = newStmt(mb, groupRef, done ? grp ? subgroupdoneRef : groupdoneRef : grp ? subgroupRef : groupRef);
	if (q == NULL)
		goto bailout;

	/* output variables extent and hist */
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, s->nr);
	if (grp)
		q = pushArgument(mb, q, grp->nr);

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *ns = stmt_create(be->mvc->sa, st_group);
	be->mvc->sa->eb.enabled = enabled;
	if (ns == NULL) {
		freeInstruction(q);
		goto bailout;
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
	pushInstruction(mb, q);
	return ns;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_unique(backend *be, stmt *s)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (s == NULL || s->nr < 0)
		goto bailout;

	q = newStmt(mb, algebraRef, uniqueRef);
	if (q == NULL)
		goto bailout;

	q = pushArgument(mb, q, s->nr);
	q = pushNilBat(mb, q); /* candidate list */

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *ns = stmt_create(be->mvc->sa, st_unique);
	be->mvc->sa->eb.enabled = enabled;
	if (ns == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	ns->op1 = s;
	ns->nrcols = s->nrcols;
	ns->key = 1;
	ns->q = q;
	ns->nr = getDestVar(q);
	pushInstruction(mb, q);
	return ns;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
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
	q = pushType(mb, q, tt);
	pushInstruction(mb, q);
	return getDestVar(q);
}

stmt *
stmt_bat_new(backend *be, sql_subtype *tpe, lng estimate)
{
	InstrPtr q = newStmt(be->mb, batRef, newRef);
	int tt = tpe->type->localtype;

	if (q == NULL)
		return NULL;
	if (tt == TYPE_void)
		tt = TYPE_bte;
	setVarType(be->mb, getArg(q, 0), newBatType(tt));
	q = pushType(be->mb, q, tt);
	if (estimate > 0)
		q = pushInt(be->mb, q, (int)estimate);
	pushInstruction(be->mb, q);

	stmt *s = stmt_create(be->mvc->sa, st_alias);
	s->op4.typeval = *tpe;
	s->q = q;
	s->nr = q->argv[0];
	s->nrcols = 2;
	return s;
}

static int *
dump_table(allocator *sa, MalBlkPtr mb, sql_table *t)
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
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, be->mvc_var);
		q = pushStr(mb, q, sname); /* all global variables have a schema */
		q = pushStr(mb, q, varname);
		setVarType(mb, getArg(q, 0), tt);
	} else if (!declare) {
		char levelstr[16];

		assert(!sname);
		snprintf(levelstr, sizeof(levelstr), "%d", level);
		buf = SA_NEW_ARRAY(be->mvc->sa, char, strlen(levelstr) + strlen(varname) + 3);
		if (!buf)
			goto bailout;
		stpcpy(stpcpy(stpcpy(stpcpy(buf, "A"), levelstr), "%"), varname); /* mangle variable name */
		q = newAssignment(mb);
		if (q == NULL)
			goto bailout;
		q = pushArgumentId(mb, q, buf);
	} else {
		int tt = t->type->localtype;
		char levelstr[16];

		assert(!sname);
		snprintf(levelstr, sizeof(levelstr), "%d", level);
		buf = SA_NEW_ARRAY(be->mvc->sa, char, strlen(levelstr) + strlen(varname) + 3);
		if (!buf)
			goto bailout;
		stpcpy(stpcpy(stpcpy(stpcpy(buf, "A"), levelstr), "%"), varname); /* mangle variable name */

		q = newInstruction(mb, NULL, NULL);
		if (q == NULL) {
			goto bailout;
		}
		q->argc = q->retc = 0;
		q = pushArgumentId(mb, q, buf);
		q = pushNil(mb, q, tt);
		q->retc++;
	}
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_var);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	if (t)
		s->op4.typeval = *t;
	else
		s->op4.typeval.type = NULL;
	s->flag = declare + (level << 1);
	s->key = 1;
	s->q = q;
	s->nr = getDestVar(q);
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_vars(backend *be, const char *varname, sql_table *t, int declare, int level)
{
	MalBlkPtr mb = be->mb;
	int *l;

	(void)varname;
	/* declared table */
	if ((l = dump_table(be->mvc->sa, mb, t)) != NULL) {
		stmt *s = stmt_create(be->mvc->sa, st_var);

		if (s == NULL) {
			return NULL;
		}

		ATOMIC_PTR_SET(&t->data, l);
		/*
		s->op2 = (stmt*)l;
		s->op3 = (stmt*)t;
		*/
		s->flag = declare + (level << 1);
		s->key = 1;
		s->nr = l[0];
		return s;
	}
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_varnr(backend *be, int nr, sql_subtype *t)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = newAssignment(mb);
	char buf[IDLENGTH];

	if (q == NULL)
		goto bailout;

	(void) snprintf(buf, sizeof(buf), "A%d", nr);
	q = pushArgumentId(mb, q, buf);

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_var);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
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
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_table(backend *be, stmt *cols, int temp)
{
	MalBlkPtr mb = be->mb;

	if (cols == NULL || cols->nr < 0)
		goto bailout;

	stmt *s = stmt_create(be->mvc->sa, st_table);

	if (s == NULL)
		goto bailout;

	if (cols->type != st_list) {
	    InstrPtr q = newAssignment(mb);
		if (q == NULL)
			goto bailout;
		pushInstruction(mb, q);
		q = newStmt(mb, sqlRef, printRef);
		if (q == NULL)
			goto bailout;
		q = pushStr(mb, q, "not a valid output list\n");
		pushInstruction(mb, q);
	}
	s->op1 = cols;
	s->flag = temp;
	s->nr = cols->nr;
	s->nrcols = cols->nrcols;
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_temp(backend *be, sql_subtype *t)
{
	int tt = t->type->localtype;
	MalBlkPtr mb = be->mb;
	InstrPtr q = newStmt(mb, batRef, newRef);

	if (q == NULL)
		goto bailout;
	setVarType(mb, getArg(q, 0), newBatType(tt));
	q = pushType(mb, q, tt);
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_temp);
	be->mvc->sa->eb.enabled = enabled;

	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}
	s->op4.typeval = *t;
	s->nrcols = 1;
	s->q = q;
	s->nr = getDestVar(q);
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_blackbox_result(backend *be, InstrPtr q, int retnr, sql_subtype *t)
{
	if (q == NULL)
		return NULL;
	stmt *s = stmt_create(be->mvc->sa, st_result);
	if (s == NULL)
		return NULL;
	s->op4.typeval = *t;
	s->nrcols = 1;
	s->q = q;
	s->nr = getArg(q, retnr);
	s->flag = retnr;
	return s;
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
			goto bailout;
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
		goto bailout;
	setVarType(mb, getArg(q, 0), newBatType(tt));
	q = pushArgument(mb, q, be->mvc_var);
	q = pushSchema(mb, q, t);
	q = pushStr(mb, q, t->base.name);
	if (t && isTable(t) && partition) {
		sql_trans *tr = be->mvc->session->tr;
		sqlstore *store = tr->store;
		BUN rows = (BUN) store->storage_api.count_col(tr, ol_first_node(t->columns)->data, RDONLY);
		setRowCnt(mb,getArg(q,0),rows);
	}

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_tid);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	s->partition = partition;
	s->op4.tval = t;
	s->nrcols = 1;
	s->nr = getDestVar(q);
	s->q = q;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

static sql_column *
find_real_column(backend *be, sql_column *c)
{
	if (c && c->t && c->t->s && c->t->persistence == SQL_DECLARED_TABLE) {
		sql_table *nt = find_sql_table_id(be->mvc->session->tr, c->t->s, c->t->base.id);
		if (nt) {
			node *n = ol_find_id(nt->columns, c->base.id);
			if (n)
				return n->data;
		}
	}
	return c;
}

stmt *
stmt_bat(backend *be, sql_column *c, int access, int partition)
{
	int tt = c->type.type->localtype;
	MalBlkPtr mb = be->mb;
	InstrPtr q;

	c = find_real_column(be, c);

	if (access == RD_EXT)
		partition = 0;

	/* for read access tid.project(col) */
	if (!c->t->s && ATOMIC_PTR_GET(&c->t->data)) { /* declared table */
		stmt *s = stmt_create(be->mvc->sa, st_bat);
		int *l = ATOMIC_PTR_GET(&c->t->data);

		if (s == NULL) {
			goto bailout;
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
		goto bailout;
	if (c->storage_type && access != RD_EXT) {
		sql_trans *tr = be->mvc->session->tr;
		sqlstore *store = tr->store;
		BAT *b = store->storage_api.bind_col(tr, c, QUICK);
		if (!b) {
			freeInstruction(q);
			goto bailout;
		}
		tt = b->ttype;
	}
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

	if (access == RD_UPD_ID) {
		setVarType(mb, getArg(q, 1), newBatType(tt));
	}
	if (partition) {
		sql_trans *tr = be->mvc->session->tr;
		sqlstore *store = tr->store;

		if (c && isTable(c->t)) {
			BUN rows = (BUN) store->storage_api.count_col(tr, c, RDONLY);
			setRowCnt(mb,getArg(q,0),rows);
		}
	}

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_bat);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	s->partition = partition;
	s->op4.cval = c;
	s->nrcols = 1;
	s->flag = access;
	s->nr = getDestVar(q);
	s->q = q;
	s->tname = c->t->base.name;
	s->cname = c->base.name;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_idxbat(backend *be, sql_idx *i, int access, int partition)
{
	int tt = hash_index(i->type)?TYPE_lng:TYPE_oid;
	MalBlkPtr mb = be->mb;
	InstrPtr q = newStmtArgs(mb, sqlRef, bindidxRef, 9);

	if (q == NULL)
		goto bailout;

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

	if (access == RD_UPD_ID) {
		setVarType(mb, getArg(q, 1), newBatType(tt));
	}
	if (partition) {
		sql_trans *tr = be->mvc->session->tr;
		sqlstore *store = tr->store;

		if (i && isTable(i->t)) {
			BUN rows = (BUN) store->storage_api.count_idx(tr, i, QUICK);
			setRowCnt(mb,getArg(q,0),rows);
		}
	}

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_idxbat);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	s->partition = partition;
	s->op4.idxval = i;
	s->nrcols = 1;
	s->flag = access;
	s->nr = getDestVar(q);
	s->q = q;
	s->tname = i->t->base.name;
	s->cname = i->base.name;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_append_col(backend *be, sql_column *c, stmt *offset, stmt *b, int *mvc_var_update, int fake)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (b == NULL || b->nr < 0)
		goto bailout;

	if (!c->t->s && ATOMIC_PTR_GET(&c->t->data)) { /* declared table */
		int *l = ATOMIC_PTR_GET(&c->t->data);

		if (c->colnr == 0) { /* append to tid column */
			q = newStmt(mb, sqlRef, growRef);
			if (q == NULL)
				goto bailout;
			q = pushArgument(mb, q, l[0]);
			q = pushArgument(mb, q, b->nr);
			pushInstruction(mb, q);
		}
		q = newStmt(mb, batRef, appendRef);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, l[c->colnr+1]);
		q = pushArgument(mb, q, b->nr);
		q = pushBit(mb, q, TRUE);
		getArg(q,0) = l[c->colnr+1];
	} else if (!fake) {	/* fake append */
		if (offset == NULL || offset->nr < 0)
			goto bailout;
		q = newStmt(mb, sqlRef, appendRef);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, be->mvc_var);
		int tmpvar = newTmpVariable(mb, TYPE_int);
		getArg(q, 0) = tmpvar;
		if (mvc_var_update != NULL)
			*mvc_var_update = tmpvar;
		q = pushSchema(mb, q, c->t);
		q = pushStr(mb, q, c->t->base.name);
		q = pushStr(mb, q, c->base.name);
		q = pushArgument(mb, q, offset->nr);
		/* also the offsets */
		assert(offset->q->retc == 2);
		q = pushArgument(mb, q, getArg(offset->q, 1));
		q = pushArgument(mb, q, b->nr);
		if (mvc_var_update != NULL)
			*mvc_var_update = getDestVar(q);
	} else {
		return b;
	}
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_append_col);
	be->mvc->sa->eb.enabled = enabled;

	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = b;
	s->op2 = offset;
	s->op4.cval = c;
	s->q = q;
	s->nr = getDestVar(q);
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_append_idx(backend *be, sql_idx *i, stmt *offset, stmt *b)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (offset == NULL || b == NULL || offset->nr < 0 || b->nr < 0)
		goto bailout;

	q = newStmt(mb, sqlRef, appendRef);
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, be->mvc_var);
	getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
	q = pushSchema(mb, q, i->t);
	q = pushStr(mb, q, i->t->base.name);
	q = pushStr(mb, q, sa_strconcat(be->mvc->sa, "%", i->base.name));
	q = pushArgument(mb, q, offset->nr);
	/* also the offsets */
	assert(offset->q->retc == 2);
	q = pushArgument(mb, q, getArg(offset->q, 1));
	q = pushArgument(mb, q, b->nr);
	be->mvc_var = getDestVar(q);

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_append_idx);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	s->op1 = b;
	s->op2 = offset;
	s->op4.idxval = i;
	s->q = q;
	s->nr = getDestVar(q);
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_update_col(backend *be, sql_column *c, stmt *tids, stmt *upd)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (tids == NULL || upd == NULL || tids->nr < 0 || upd->nr < 0)
		goto bailout;

	if (!c->t->s && ATOMIC_PTR_GET(&c->t->data)) { /* declared table */
		int *l = ATOMIC_PTR_GET(&c->t->data);

		q = newStmt(mb, batRef, replaceRef);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, l[c->colnr+1]);
		q = pushArgument(mb, q, tids->nr);
		q = pushArgument(mb, q, upd->nr);
	} else {
		q = newStmt(mb, sqlRef, updateRef);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, be->mvc_var);
		getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
		q = pushSchema(mb, q, c->t);
		q = pushStr(mb, q, c->t->base.name);
		q = pushStr(mb, q, c->base.name);
		q = pushArgument(mb, q, tids->nr);
		q = pushArgument(mb, q, upd->nr);
		be->mvc_var = getDestVar(q);
	}
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_update_col);
	be->mvc->sa->eb.enabled = enabled;

	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = tids;
	s->op2 = upd;
	s->op4.cval = c;
	s->q = q;
	s->nr = getDestVar(q);
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}


stmt *
stmt_update_idx(backend *be, sql_idx *i, stmt *tids, stmt *upd)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (tids == NULL || upd == NULL || tids->nr < 0 || upd->nr < 0)
		goto bailout;

	q = newStmt(mb, sqlRef, updateRef);
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, be->mvc_var);
	getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
	q = pushSchema(mb, q, i->t);
	q = pushStr(mb, q, i->t->base.name);
	q = pushStr(mb, q, sa_strconcat(be->mvc->sa, "%", i->base.name));
	q = pushArgument(mb, q, tids->nr);
	q = pushArgument(mb, q, upd->nr);
	be->mvc_var = getDestVar(q);
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_update_idx);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	s->op1 = tids;
	s->op2 = upd;
	s->op4.idxval = i;
	s->q = q;
	s->nr = getDestVar(q);
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_delete(backend *be, sql_table *t, stmt *tids)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (tids == NULL || tids->nr < 0)
		goto bailout;

	if (!t->s && ATOMIC_PTR_GET(&t->data)) { /* declared table */
		int *l = ATOMIC_PTR_GET(&t->data);

		q = newStmt(mb, batRef, deleteRef);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, l[0]);
		q = pushArgument(mb, q, tids->nr);
	} else {
		q = newStmt(mb, sqlRef, deleteRef);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, be->mvc_var);
		getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
		q = pushSchema(mb, q, t);
		q = pushStr(mb, q, t->base.name);
		q = pushArgument(mb, q, tids->nr);
		be->mvc_var = getDestVar(q);
	}
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_delete);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	s->op1 = tids;
	s->op4.tval = t;
	s->q = q;
	s->nr = getDestVar(q);
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_const(backend *be, stmt *s, stmt *val)
{
	InstrPtr q = NULL;
	MalBlkPtr mb = be->mb;

	if (s == NULL)
		goto bailout;
	if (val)
		q = dump_2(mb, algebraRef, projectRef, s, val);
	else
		q = dump_1(mb, algebraRef, projectRef, s);
	if (q) {
		stmt *ns = stmt_create(be->mvc->sa, st_const);
		if (ns == NULL) {
			goto bailout;
		}

		ns->op1 = s;
		ns->op2 = val;
		ns->nrcols = s->nrcols;
		ns->key = s->key;
		ns->aggr = s->aggr;
		ns->q = q;
		ns->nr = getDestVar(q);
		ns->tname = val->tname;
		ns->cname = val->cname;
		return ns;
	}
  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_gen_group(backend *be, stmt *gids, stmt *cnts)
{
	MalBlkPtr mb = be->mb;

	if (gids == NULL || cnts == NULL)
		goto bailout;

	InstrPtr q = dump_2(mb, algebraRef, groupbyRef, gids, cnts);

	if (q) {
		stmt *ns = stmt_create(be->mvc->sa, st_gen_group);
		if (ns == NULL) {
			goto bailout;
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
  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_mirror(backend *be, stmt *s)
{
	MalBlkPtr mb = be->mb;

	if (s == NULL)
		goto bailout;

	InstrPtr q = dump_1(mb, batRef, mirrorRef, s);

	if (q) {
		stmt *ns = stmt_create(be->mvc->sa, st_mirror);
		if (ns == NULL) {
			goto bailout;
		}

		ns->op1 = s;
		ns->nrcols = 2;
		ns->key = s->key;
		ns->aggr = s->aggr;
		ns->q = q;
		ns->nr = getDestVar(q);
		return ns;
	}
  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

#define MARKJOIN 100
stmt *
stmt_result(backend *be, stmt *s, int nr)
{
	stmt *ns;

	if (s == NULL)
		return NULL;

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

		assert(s->q->retc > nr);
		ns->nr = v;
	} else {
		ns->nr = s->nr;
	}
	ns->op1 = s;
	if (!nr && (s->type == st_order || s->type == st_reorder))
		ns->op4.typeval = *tail_type(s->op1);
	else if (nr && ((s->type == st_join && s->flag == MARKJOIN) || (s->type == st_uselect2 && s->flag == MARKJOIN)))
		ns->op4.typeval = *sql_bind_localtype("bit");
	else
		ns->op4.typeval = *sql_bind_localtype("oid");
	ns->flag = nr;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->aggr = s->aggr;
	return ns;
}


/* limit maybe atom nil */
stmt *
stmt_limit(backend *be, stmt *col, stmt *piv, stmt *gid, stmt *offset, stmt *limit, int distinct, int dir, int nullslast, int nr_obe, int order)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	int l, g, c;

	if (col == NULL || offset == NULL || limit == NULL || col->nr < 0 || offset->nr < 0 || limit->nr < 0)
		goto bailout;
	if (piv && (piv->nr < 0 || (gid && gid->nr < 0)))
		goto bailout;

	c = (col) ? col->nr : 0;
	g = (gid) ? gid->nr : 0;

	/* first insert single value into a bat */
	if (col->nrcols == 0) {
		int k, tt = tail_type(col)->type->localtype;

		q = newStmt(mb, batRef, newRef);
		if (q == NULL)
			goto bailout;
		setVarType(mb, getArg(q, 0), newBatType(tt));
		q = pushType(mb, q, tt);
		k = getDestVar(q);
		pushInstruction(mb, q);

		q = newStmt(mb, batRef, appendRef);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, k);
		q = pushArgument(mb, q, c);
		pushInstruction(mb, q);
		c = k;
	}
	if (order) {
		if (piv && piv->q) {
			q = piv->q;
			q = pushArgument(mb, q, c);
			q = pushBit(mb, q, dir);
			q = pushBit(mb, q, nullslast);
			return piv;
		} else {
			int topn = 0;

			q = newStmt(mb, calcRef, plusRef);
			if (q == NULL)
				goto bailout;
			q = pushArgument(mb, q, offset->nr);
			q = pushArgument(mb, q, limit->nr);
			topn = getDestVar(q);
			pushInstruction(mb, q);

			if (!gid || (piv && !piv->q)) { /* use algebra.firstn (possibly concurrently) */
				int p = (piv) ? piv->nr : 0;
				q = newStmtArgs(mb, algebraRef, firstnRef, 9);
				if (q == NULL)
					goto bailout;
				if (nr_obe > 1) /* we need the groups for the next firstn */
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, c);
				if (p)
					q = pushArgument(mb, q, p);
				else
					q = pushNilBat(mb, q);
				if (g)
					q = pushArgument(mb, q, g);
				else
					q = pushNilBat(mb, q);
				q = pushArgument(mb, q, topn);
				q = pushBit(mb, q, dir);
				q = pushBit(mb, q, nullslast);
				q = pushBit(mb, q, distinct != 0);

				l = getArg(q, 0);
				l = getDestVar(q);
				pushInstruction(mb, q);
			} else {
				q = newStmtArgs(mb, algebraRef, groupedfirstnRef, (nr_obe*3)+6);
				if (q == NULL)
					goto bailout;
				q = pushArgument(mb, q, topn);
				q = pushNilBat(mb, q);	/* candidates */
				if (g)					/* grouped case */
					q = pushArgument(mb, q, g);
				else
					q = pushNilBat(mb, q);

				q = pushArgument(mb, q, c);
				q = pushBit(mb, q, dir);
				q = pushBit(mb, q, nullslast);

				l = getArg(q, 0);
				l = getDestVar(q);
				pushInstruction(mb, q);
			}
		}
	} else {
		int len;

		q = newStmt(mb, calcRef, plusRef);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, offset->nr);
		q = pushArgument(mb, q, limit->nr);
		len = getDestVar(q);
		pushInstruction(mb, q);

		/* since both arguments of algebra.subslice are
		   inclusive correct the LIMIT value by
		   subtracting 1 */
		q = newStmt(mb, calcRef, minusRef);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, len);
		q = pushInt(mb, q, 1);
		len = getDestVar(q);
		pushInstruction(mb, q);

		q = newStmt(mb, algebraRef, subsliceRef);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, c);
		q = pushArgument(mb, q, offset->nr);
		q = pushArgument(mb, q, len);
		l = getDestVar(q);
		pushInstruction(mb, q);
	}
	/* retrieve the single values again */
	if (col->nrcols == 0) {
		q = newStmt(mb, algebraRef, findRef);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, l);
		q = pushOid(mb, q, 0);
		l = getDestVar(q);
		pushInstruction(mb, q);
	}

	stmt *ns = stmt_create(be->mvc->sa, piv?st_limit2:st_limit);
	if (ns == NULL) {
		goto bailout;
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

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_sample(backend *be, stmt *s, stmt *sample, stmt *seed)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (s == NULL || sample == NULL || s->nr < 0 || sample->nr < 0)
		goto bailout;
	q = newStmt(mb, sampleRef, subuniformRef);
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, s->nr);
	q = pushArgument(mb, q, sample->nr);

	if (seed) {
		if (seed->nr < 0)
			goto bailout;

		q = pushArgument(mb, q, seed->nr);
	}

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *ns = stmt_create(be->mvc->sa, st_sample);
	be->mvc->sa->eb.enabled = enabled;
	if (ns == NULL) {
		freeInstruction(q);
		goto bailout;
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
	pushInstruction(mb, q);
	return ns;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}


stmt *
stmt_order(backend *be, stmt *s, int direction, int nullslast)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (s == NULL || s->nr < 0)
		goto bailout;
	q = newStmt(mb, algebraRef, sortRef);
	if (q == NULL)
		goto bailout;
	/* both ordered result and oid's order en subgroups */
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, s->nr);
	q = pushBit(mb, q, !direction);
	q = pushBit(mb, q, nullslast);
	q = pushBit(mb, q, FALSE);

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *ns = stmt_create(be->mvc->sa, st_order);
	be->mvc->sa->eb.enabled = enabled;
	if (ns == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	ns->op1 = s;
	ns->flag = direction;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->aggr = s->aggr;
	ns->q = q;
	ns->nr = getDestVar(q);
	pushInstruction(mb, q);
	return ns;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_reorder(backend *be, stmt *s, int direction, int nullslast, stmt *orderby_ids, stmt *orderby_grp)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (s == NULL || orderby_ids == NULL || orderby_grp == NULL || s->nr < 0 || orderby_ids->nr < 0 || orderby_grp->nr < 0)
		goto bailout;
	q = newStmtArgs(mb, algebraRef, sortRef, 9);
	if (q == NULL)
		goto bailout;
	/* both ordered result and oid's order en subgroups */
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, s->nr);
	q = pushArgument(mb, q, orderby_ids->nr);
	q = pushArgument(mb, q, orderby_grp->nr);
	q = pushBit(mb, q, !direction);
	q = pushBit(mb, q, nullslast);
	q = pushBit(mb, q, FALSE);

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *ns = stmt_create(be->mvc->sa, st_reorder);
	be->mvc->sa->eb.enabled = enabled;
	if (ns == NULL) {
		freeInstruction(q);
		goto bailout;
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
	pushInstruction(mb, q);
	return ns;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_atom(backend *be, atom *a)
{
	MalBlkPtr mb = be->mb;

	if (a == NULL)
		goto bailout;

	InstrPtr q = EC_TEMP_FRAC(atom_type(a)->type->eclass) ? newStmt(mb, calcRef, atom_type(a)->type->impl) : newAssignment(mb);

	if (q == NULL)
		goto bailout;
	if (atom_null(a)) {
		q = pushNil(mb, q, atom_type(a)->type->localtype);
	} else {
		int k;
		if ((k = constantAtom(be, mb, a)) == -1) {
			freeInstruction(q);
			goto bailout;
		}
		q = pushArgument(mb, q, k);
	}
	/* digits of the result timestamp/daytime */
	if (EC_TEMP_FRAC(atom_type(a)->type->eclass))
		q = pushInt(mb, q, atom_type(a)->digits);
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_atom);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	s->op4.aval = a;
	s->key = 1;		/* values are also unique */
	s->q = q;
	s->nr = getDestVar(q);
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
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

	if (lops == NULL || rops == NULL)
		goto bailout;

	if (backend_create_subfunc(be, f, NULL) < 0)
		goto bailout;
	op = backend_function_imp(be, f->func);
	mod = sql_func_mod(f->func);

	if (rops->nrcols >= 1) {
		bit need_not = FALSE;

		int narg = 3;
		for (n = lops->op4.lval->h; n; n = n->next)
			narg++;
		for (n = rops->op4.lval->h; n; n = n->next)
			narg++;
		q = newStmtArgs(mb, malRef, multiplexRef, narg);
		if (q == NULL)
			goto bailout;
		setVarType(mb, getArg(q, 0), newBatType(TYPE_bit));
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
		pushInstruction(mb, q);

		q = newStmtArgs(mb, algebraRef, selectRef, 9);
		if (q == NULL)
			goto bailout;
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
		q = newStmtArgs(mb, mod, convertMultiplexFcn(op), 9);
		if (q == NULL)
			goto bailout;
		// push pointer to the SQL structure into the MAL call
		// allows getting argument names for example
		if (LANG_EXT(f->func->lang))
			q = pushPtr(mb, q, f->func); // nothing to see here, please move along
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
			q = pushNilBat(mb, q);

		for (n = rops->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;

			q = pushArgument(mb, q, op->nr);
		}

		q = pushBit(mb, q, anti);
	}

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_uselect);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	s->op1 = lops;
	s->op2 = rops;
	s->op3 = sub;
	s->key = lops->nrcols == 0 && rops->nrcols == 0;
	s->flag = cmp_filter;
	s->nrcols = lops->nrcols;
	s->nr = getDestVar(q);
	s->q = q;
	s->cand = sub;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_uselect(backend *be, stmt *op1, stmt *op2, comp_type cmptype, stmt *sub, int anti, int is_semantics)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	int l, r;
	stmt *sel = sub;

	if (op1 == NULL || op2 == NULL || op1->nr < 0 || op2->nr < 0 || (sub && sub->nr < 0))
		goto bailout;
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
			TRC_ERROR(SQL_EXECUTION, "Unknown operator\n");
		}

		if ((q = multiplex2(mb, mod, convertMultiplexFcn(op), l, r, TYPE_bit)) == NULL)
			goto bailout;
		if (sub && (op1->cand || op2->cand)) {
			if (op1->cand && !op2->cand) {
				if (op1->nrcols > 0)
					q = pushNilBat(mb, q);
				q = pushArgument(mb, q, sub->nr);
			} else if (!op1->cand && op2->cand) {
				q = pushArgument(mb, q, sub->nr);
				if (op2->nrcols > 0)
					q = pushNilBat(mb, q);
			}
			sub = NULL;
		}
		if (is_semantics)
			q = pushBit(mb, q, TRUE);
		k = getDestVar(q);

		q = newStmtArgs(mb, algebraRef, selectRef, 9);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, k);
		if (sub)
			q = pushArgument(mb, q, sub->nr);
		q = pushBit(mb, q, !need_not);
		q = pushBit(mb, q, !need_not);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, anti);
		k = getDestVar(q);
	} else {
		assert (cmptype != cmp_filter);
		q = newStmt(mb, algebraRef, thetaselectRef);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, l);
		if (sub && !op1->cand) {
			q = pushArgument(mb, q, sub->nr);
		} else {
			assert(!sub || op1->cand == sub);
			q = pushNilBat(mb, q);
			sub = NULL;
		}
		q = pushArgument(mb, q, r);
		switch (cmptype) {
		case cmp_equal:
			if (is_semantics)
				q = pushStr(mb, q, anti?"ne":"eq");
			else
				q = pushStr(mb, q, anti?"!=":"==");
			break;
		case cmp_notequal:
			if (is_semantics)
				q = pushStr(mb, q, anti?"eq":"ne");
			else
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
			goto bailout;
		}
	}

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_uselect);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	s->op1 = op1;
	s->op2 = op2;
	s->op3 = sub;
	s->flag = cmptype;
	s->key = op1->nrcols == 0 && op2->nrcols == 0;
	s->nrcols = op1->nrcols;
	s->nr = getDestVar(q);
	s->q = q;
	s->cand = sub;
	pushInstruction(mb, q);
	if (!sub && sel) /* project back the old ids */
		return stmt_project(be, s, sel);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
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
select2_join2(backend *be, stmt *op1, stmt *op2, stmt *op3, int cmp, stmt **Sub, int anti, int symmetric, int swapped, int type, int reduce)
{
	MalBlkPtr mb = be->mb;
	InstrPtr p, q;
	int l;
	const char *cmd = (type == st_uselect2) ? selectRef : rangejoinRef;
	stmt *sub = (Sub)?*Sub:NULL;

	if (op1 == NULL || op2 == NULL || op3 == NULL || op1->nr < 0 || (sub && sub->nr < 0))
		goto bailout;
	l = op1->nr;
	if ((symmetric || op2->nrcols > 0 || op3->nrcols > 0 || !reduce) && (type == st_uselect2)) {
		int k;
		int nrcols = (op1->nrcols || op2->nrcols || op3->nrcols);

		if (op2->nr < 0 || op3->nr < 0)
			goto bailout;

		if (nrcols)
			p = newStmtArgs(mb, batcalcRef, betweenRef, 12);
		else
			p = newStmtArgs(mb, calcRef, betweenRef, 9);
		if (p == NULL)
			goto bailout;
		p = pushArgument(mb, p, l);
		p = pushArgument(mb, p, op2->nr);
		p = pushArgument(mb, p, op3->nr);

		/* cands */
		if ((sub && !reduce) || op1->cand || op2->cand || op3->cand) { /* some already handled the previous selection */
			if (op1->cand && op1->nrcols)
				p = pushNilBat(mb, p);
			else if (op1->nrcols)
				p = pushArgument(mb, p, sub->nr);
			if (op2->nrcols) {
				if (op2->cand)
					p = pushNilBat(mb, p);
				else if (op2->nrcols)
					p = pushArgument(mb, p, sub->nr);
			}
			if (op3->nrcols) {
				if (op3->cand)
					p = pushNilBat(mb, p);
				else if (op3->nrcols)
					p = pushArgument(mb, p, sub->nr);
			}
			sub = NULL;
		}

		p = pushBit(mb, p, (symmetric)?TRUE:FALSE); /* symmetric */
		p = pushBit(mb, p, (cmp & 1) != 0);	    /* lo inclusive */
		p = pushBit(mb, p, (cmp & 2) != 0);	    /* hi inclusive */
		p = pushBit(mb, p, FALSE);		    /* nils_false */
		p = pushBit(mb, p, (anti)?TRUE:FALSE);	    /* anti */
		pushInstruction(mb, p);
		if (!reduce)
			return p;
		k = getDestVar(p);

		q = newStmtArgs(mb, algebraRef, selectRef, 9);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, k);
		if (sub)
			q = pushArgument(mb, q, sub->nr);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, TRUE);
		q = pushBit(mb, q, FALSE);
		pushInstruction(mb, q);
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
		q = newStmtArgs(mb, algebraRef, cmd, 12);
		if (q == NULL)
			goto bailout;
		if (type == st_join2)
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, l);
		if (sub) {
			int cand = op1->cand || op2->cand || op3->cand;
			if (cand) {
				assert(!op1->nrcols || op1->cand);
				assert(!op2->nrcols || op2->cand);
				assert(!op3->nrcols || op3->cand);
				sub = NULL;
			}
		}
		if (sub) /* only for uselect2 */
			q = pushArgument(mb, q, sub->nr);
		if (rs) {
			q = pushArgument(mb, q, rs);
		} else {
			q = pushArgument(mb, q, r1);
			q = pushArgument(mb, q, r2);
		}
		if (type == st_join2) {
			q = pushNilBat(mb, q);
			q = pushNilBat(mb, q);
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
			q = pushBit(mb, q, TRUE); /* all nil's are != */
		} else {
			q = pushBit(mb, q, (symmetric)?TRUE:FALSE);
		}
		if (type == st_join2)
			q = pushNil(mb, q, TYPE_lng); /* estimate */
		pushInstruction(mb, q);
		if (swapped) {
			InstrPtr r = newInstruction(mb,  NULL, NULL);
			if (r == NULL)
				goto bailout;
			getArg(r, 0) = newTmpVariable(mb, TYPE_any);
			r = pushReturn(mb, r, newTmpVariable(mb, TYPE_any));
			r = pushArgument(mb, r, getArg(q,1));
			r = pushArgument(mb, r, getArg(q,0));
			pushInstruction(mb, r);
			q = r;
		}
	}
	if (Sub)
		*Sub = sub;
	return q;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_outerselect(backend *be, stmt *g, stmt *m, stmt *p, bool any)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q;

	q = newStmtArgs(mb, algebraRef, outerselectRef, 6);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, g->nr); /* group ids */
	q = pushArgument(mb, q, m->nr); /* mark flag */
	q = pushArgument(mb, q, p->nr); /* predicate */
	q = pushBit(mb, q, (any)?TRUE:FALSE);
	pushInstruction(mb, q);

	if (!q)
		return NULL;
	stmt *s = stmt_create(be->mvc->sa, st_uselect2);
	if (s == NULL) {
		freeInstruction(q);
		return NULL;
	}

	s->op1 = g;
	s->op2 = m;
	s->flag = MARKJOIN;
	s->key = 0;
	s->nrcols = g->nrcols;
	s->nr = getDestVar(q);
	s->q = q;
	return s;
}

stmt *
stmt_markselect(backend *be, stmt *g, stmt *m, stmt *p, bool any)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q;

	q = newStmtArgs(mb, algebraRef, markselectRef, 6);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, g->nr); /* left ids */
	q = pushArgument(mb, q, m->nr); /* mark info mask */
	q = pushArgument(mb, q, p->nr);	/* predicate */
	q = pushBit(mb, q, (any)?TRUE:FALSE);
	pushInstruction(mb, q);

	if (!q)
		return NULL;
	stmt *s = stmt_create(be->mvc->sa, st_uselect2);
	if (s == NULL) {
		freeInstruction(q);
		return NULL;
	}

	s->op1 = g;
	s->op2 = m;
	s->flag = MARKJOIN;
	s->key = 0;
	s->nrcols = g->nrcols;
	s->nr = getDestVar(q);
	s->q = q;
	return s;
}

stmt *
stmt_markjoin(backend *be, stmt *l, stmt *r, bool final)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q;

	q = newStmtArgs(mb, algebraRef, markjoinRef, 8);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	if (!final)
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, l->nr); /* left ids */
	q = pushArgument(mb, q, r->nr); /* mark info mask */
	q = pushNilBat(mb, q);
	q = pushNilBat(mb, q);
	q = pushNil(mb, q, TYPE_lng);
	pushInstruction(mb, q);

	if (!q)
		return NULL;
	stmt *s = stmt_create(be->mvc->sa, st_join);
	if (s == NULL) {
		freeInstruction(q);
		return NULL;
	}

	s->op1 = l;
	s->op2 = r;
	s->flag = MARKJOIN;
	s->key = 0;
	s->nrcols = l->nrcols;
	s->nr = getDestVar(q);
	s->q = q;
	return s;
}

stmt *
stmt_uselect2(backend *be, stmt *op1, stmt *op2, stmt *op3, int cmp, stmt *sub, int anti, int symmetric, int reduce)
{
	stmt *sel = sub;
	InstrPtr q = select2_join2(be, op1, op2, op3, cmp, &sub, anti, symmetric, 0, st_uselect2, reduce);

	if (q == NULL)
		return NULL;

	stmt *s = stmt_create(be->mvc->sa, st_uselect2);
	if (s == NULL) {
		return NULL;
	}

	s->op1 = op1;
	s->op2 = op2;
	s->op3 = op3;
	s->op4.stval = sub;
	s->flag = cmp;
	s->nrcols = op1->nrcols;
	s->key = op1->nrcols == 0 && op2->nrcols == 0 && op3->nrcols == 0;
	s->nr = getDestVar(q);
	s->q = q;
	s->cand = sub;
	s->reduce = reduce;
	if (!sub && sel) /* project back the old ids */
		return stmt_project(be, s, sel);
	return s;
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

	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_tdiff(backend *be, stmt *op1, stmt *op2, stmt *lcand)
{
	InstrPtr q = NULL;
	MalBlkPtr mb = be->mb;

	if (op1 == NULL || op2 == NULL || op1->nr < 0 || op2->nr < 0)
		goto bailout;
	q = newStmt(mb, algebraRef, differenceRef);
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, op1->nr); /* left */
	q = pushArgument(mb, q, op2->nr); /* right */
	if (lcand)
		q = pushArgument(mb, q, lcand->nr); /* left */
	else
		q = pushNilBat(mb, q); /* left candidate */
	q = pushNilBat(mb, q); /* right candidate */
	q = pushBit(mb, q, FALSE);    /* nil matches */
	q = pushBit(mb, q, FALSE);    /* do not clear nils */
	q = pushNil(mb, q, TYPE_lng); /* estimate */

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_tdiff);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	s->op1 = op1;
	s->op2 = op2;
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->aggr = op1->aggr;
	s->nr = getDestVar(q);
	s->q = q;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_tdiff2(backend *be, stmt *op1, stmt *op2, stmt *lcand)
{
	InstrPtr q = NULL;
	MalBlkPtr mb = be->mb;

	if (op1 == NULL || op2 == NULL || op1->nr < 0 || op2->nr < 0)
		goto bailout;
	q = newStmt(mb, algebraRef, differenceRef);
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, op1->nr); /* left */
	q = pushArgument(mb, q, op2->nr); /* right */
	if (lcand)
		q = pushArgument(mb, q, lcand->nr); /* left */
	else
		q = pushNilBat(mb, q); /* left candidate */
	q = pushNilBat(mb, q); /* right candidate */
	q = pushBit(mb, q, FALSE);     /* nil matches */
	q = pushBit(mb, q, TRUE);     /* not in */
	q = pushNil(mb, q, TYPE_lng); /* estimate */

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_tdiff);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	s->op1 = op1;
	s->op2 = op2;
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->aggr = op1->aggr;
	s->nr = getDestVar(q);
	s->q = q;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_tinter(backend *be, stmt *op1, stmt *op2, bool single)
{
	InstrPtr q = NULL;
	MalBlkPtr mb = be->mb;

	if (op1 == NULL || op2 == NULL || op1->nr < 0 || op2->nr < 0)
		goto bailout;
	q = newStmt(mb, algebraRef, intersectRef);
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, op1->nr); /* left */
	q = pushArgument(mb, q, op2->nr); /* right */
	q = pushNilBat(mb, q); /* left candidate */
	q = pushNilBat(mb, q); /* right candidate */
	q = pushBit(mb, q, FALSE);    /* nil matches */
	q = pushBit(mb, q, single?TRUE:FALSE);    /* max_one */
	q = pushNil(mb, q, TYPE_lng); /* estimate */

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_tinter);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	s->op1 = op1;
	s->op2 = op2;
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->aggr = op1->aggr;
	s->nr = getDestVar(q);
	s->q = q;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_join_cand(backend *be, stmt *op1, stmt *op2, stmt *lcand, stmt *rcand, int anti, comp_type cmptype, int need_left, int is_semantics, bool single, bool inner)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *sjt = inner?joinRef:outerjoinRef;

	(void)anti;
	(void)inner;

	if (need_left) {
		cmptype = cmp_equal;
		sjt = leftjoinRef;
	}
	if (op1 == NULL || op2 == NULL || op1->nr < 0 || op2->nr < 0)
		goto bailout;

	assert (!single || cmptype == cmp_all);

	switch (cmptype) {
	case cmp_equal:
		q = newStmtArgs(mb, algebraRef, sjt, 9);
		if (q == NULL)
			goto bailout;
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		if (!lcand)
			q = pushNilBat(mb, q);
		else
			q = pushArgument(mb, q, lcand->nr);
		if (!rcand)
			q = pushNilBat(mb, q);
		else
			q = pushArgument(mb, q, rcand->nr);
		q = pushBit(mb, q, is_semantics?TRUE:FALSE);
		if (!inner)
			q = pushBit(mb, q, FALSE); /* not match_one */
		q = pushNil(mb, q, TYPE_lng);
		pushInstruction(mb, q);
		break;
	case cmp_notequal:
		if (inner)
			sjt = thetajoinRef;
		q = newStmtArgs(mb, algebraRef, sjt, 9);
		if (q == NULL)
			goto bailout;
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		if (!lcand)
			q = pushNilBat(mb, q);
		else
			q = pushArgument(mb, q, lcand->nr);
		if (!rcand)
			q = pushNilBat(mb, q);
		else
			q = pushArgument(mb, q, rcand->nr);
		if (inner)
			q = pushInt(mb, q, JOIN_NE);
		q = pushBit(mb, q, is_semantics?TRUE:FALSE);
		if (!inner)
			q = pushBit(mb, q, FALSE); /* not match_one */
		q = pushNil(mb, q, TYPE_lng);
		pushInstruction(mb, q);
		break;
	case cmp_lt:
	case cmp_lte:
	case cmp_gt:
	case cmp_gte:
		q = newStmtArgs(mb, algebraRef, thetajoinRef, 9);
		if (q == NULL)
			goto bailout;
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		if (!lcand)
			q = pushNilBat(mb, q);
		else
			q = pushArgument(mb, q, lcand->nr);
		if (!rcand)
			q = pushNilBat(mb, q);
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
		pushInstruction(mb, q);
		break;
	case cmp_all:	/* aka cross table */
		q = newStmt(mb, algebraRef, inner?crossRef:outercrossRef);
		if (q == NULL)
			goto bailout;
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, op1->nr);
		q = pushArgument(mb, q, op2->nr);
		if (!inner) {
			q = pushNilBat(mb, q);
			q = pushNilBat(mb, q);
		}
		q = pushBit(mb, q, single?TRUE:FALSE); /* max_one */
		assert(!lcand && !rcand);
		pushInstruction(mb, q);
		break;
	case cmp_joined:
		q = op1->q;
		if (q == NULL)
			goto bailout;
		break;
	default:
		TRC_ERROR(SQL_EXECUTION, "Impossible action\n");
	}

	stmt *s = stmt_create(be->mvc->sa, st_join);
	if (s == NULL) {
		goto bailout;
	}

	s->op1 = op1;
	s->op2 = op2;
	s->flag = cmptype;
	s->key = 0;
	s->nrcols = 2;
	s->nr = getDestVar(q);
	s->q = q;
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_join(backend *be, stmt *l, stmt *r, int anti, comp_type cmptype, int need_left, int is_semantics, bool single)
{
	return stmt_join_cand(be, l, r, NULL, NULL, anti, cmptype, need_left, is_semantics, single, true);
}

stmt *
stmt_semijoin(backend *be, stmt *op1, stmt *op2, stmt *lcand, stmt *rcand, int is_semantics, bool single)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (op1 == NULL || op2 == NULL || op1->nr < 0 || op2->nr < 0)
		goto bailout;

	if (single) {
		q = newStmtArgs(mb, algebraRef, semijoinRef, 9);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	} else
		q = newStmt(mb, algebraRef, intersectRef);
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, op1->nr);
	q = pushArgument(mb, q, op2->nr);
	if (lcand)
		q = pushArgument(mb, q, lcand->nr);
	else
		q = pushNilBat(mb, q);
	if (rcand)
		q = pushArgument(mb, q, rcand->nr);
	else
		q = pushNilBat(mb, q);
	q = pushBit(mb, q, is_semantics?TRUE:FALSE);
	q = pushBit(mb, q, single?TRUE:FALSE); /* max_one */
	q = pushNil(mb, q, TYPE_lng);

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_semijoin);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
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
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

static InstrPtr
stmt_project_join(backend *be, stmt *op1, stmt *op2, bool delta)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (op1 == NULL || op2 == NULL || op1->nr < 0 || op2->nr < 0)
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
	}
	pushInstruction(mb, q);
	return q;
}

stmt *
stmt_project(backend *be, stmt *op1, stmt *op2)
{
	if (op1 == NULL || op2 == NULL)
		return NULL;
	if (!op2->nrcols)
		return stmt_const(be, op1, op2);
	InstrPtr q = stmt_project_join(be, op1, op2, false);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_join);
		if (s == NULL) {
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
		s->label = op2->label;
		return s;
	}
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : be->mb->errors ? be->mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_project_delta(backend *be, stmt *col, stmt *upd)
{
	InstrPtr q = stmt_project_join(be, col, upd, true);
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_join);
		if (s == NULL) {
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

	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : be->mb->errors ? be->mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_left_project(backend *be, stmt *op1, stmt *op2, stmt *op3)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	if (op1 == NULL || op2 == NULL || op3 == NULL || op1->nr < 0 || op2->nr < 0 || op3->nr < 0)
		goto bailout;

	q = newStmt(mb, sqlRef, projectRef);
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, op1->nr);
	q = pushArgument(mb, q, op2->nr);
	q = pushArgument(mb, q, op3->nr);

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_join);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		goto bailout;
	}

	s->op1 = op1;
	s->op2 = op2;
	s->op3 = op3;
	s->flag = cmp_left_project;
	s->key = 0;
	s->nrcols = 2;
	s->nr = getDestVar(q);
	s->q = q;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_dict(backend *be, stmt *op1, stmt *op2)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (op1 == NULL || op2 == NULL || op1->nr < 0 || op2->nr < 0)
		return NULL;

	q = newStmt(mb, dictRef, decompressRef);
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, op1->nr);
	q = pushArgument(mb, q, op2->nr);

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_join);
	be->mvc->sa->eb.enabled = enabled;
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
	s->tname = op1->tname;
	s->cname = op1->cname;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_for(backend *be, stmt *op1, stmt *min_val)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (op1 == NULL || min_val == NULL || op1->nr < 0)
		return NULL;

	q = newStmt(mb, forRef, decompressRef);
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, op1->nr);
	q = pushArgument(mb, q, min_val->nr);

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_join);
	be->mvc->sa->eb.enabled = enabled;
	if (s == NULL) {
		freeInstruction(q);
		return NULL;
	}

	s->op1 = op1;
	s->op2 = min_val;
	s->flag = cmp_project;
	s->key = 0;
	s->nrcols = op1->nrcols;
	s->nr = getDestVar(q);
	s->q = q;
	s->tname = op1->tname;
	s->cname = op1->cname;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_join2(backend *be, stmt *l, stmt *ra, stmt *rb, int cmp, int anti, int symmetric, int swapped)
{
	InstrPtr q = select2_join2(be, l, ra, rb, cmp, NULL, anti, symmetric, swapped, st_join2, 1/*reduce semantics*/);
	if (q == NULL)
		return NULL;

	stmt *s = stmt_create(be->mvc->sa, st_join2);
	if (s == NULL) {
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

stmt *
stmt_genjoin(backend *be, stmt *l, stmt *r, sql_subfunc *op, int anti, int swapped)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *mod, *fimp;
	node *n;

	if (l == NULL || r == NULL)
		goto bailout;
	if (backend_create_subfunc(be, op, NULL) < 0)
		goto bailout;
	mod = sql_func_mod(op->func);
	fimp = backend_function_imp(be, op->func);
	fimp = sa_strconcat(be->mvc->sa, fimp, "join");

	/* filter qualifying tuples, return oids of h and tail */
	q = newStmtArgs(mb, mod, fimp, list_length(l->op4.lval) + list_length(r->op4.lval) + 7);
	if (q == NULL)
		goto bailout;
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	for (n = l->op4.lval->h; n; n = n->next) {
		stmt *op = n->data;

		q = pushArgument(mb, q, op->nr);
	}

	for (n = r->op4.lval->h; n; n = n->next) {
		stmt *op = n->data;

		q = pushArgument(mb, q, op->nr);
	}
	q = pushNilBat(mb, q); /* candidate lists */
	q = pushNilBat(mb, q); /* candidate lists */
	q = pushBit(mb, q, TRUE);     /* nil_matches */
	q = pushNil(mb, q, TYPE_lng); /* estimate */
	q = pushBit(mb, q, anti?TRUE:FALSE); /* 'not' matching */
	pushInstruction(mb, q);

	if (swapped) {
		InstrPtr r = newInstruction(mb,  NULL, NULL);
		if (r == NULL)
			goto bailout;
		getArg(r, 0) = newTmpVariable(mb, TYPE_any);
		r = pushReturn(mb, r, newTmpVariable(mb, TYPE_any));
		r = pushArgument(mb, r, getArg(q,1));
		r = pushArgument(mb, r, getArg(q,0));
		pushInstruction(mb, r);
		q = r;
	}

	stmt *s = stmt_create(be->mvc->sa, st_joinN);
	if (s == NULL) {
		goto bailout;
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

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_rs_column(backend *be, stmt *rs, int i, sql_subtype *tpe)
{
	InstrPtr q = NULL;

	if (rs == NULL || rs->nr < 0)
		return NULL;
	q = rs->q;
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_rs_column);
		if (s == NULL) {
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
	} else if (rs->type == st_list) {
		list *cols = rs->op4.lval;
		if (i < list_length(cols))
			return list_fetch(cols, i);
	}
	return NULL;
}

/*
 * The dump_header produces a sequence of instructions for
 * the front-end to prepare presentation of a result table.
 *
 * A secondary scheme is added to assemble all information
 * in columns first. Then it can be returned to the environment.
 */
#define NEWRESULTSET

#define meta(P, Id, Tpe, Args)						\
	do {											\
		P = newStmtArgs(mb, batRef, packRef, Args);	\
		if (P) {									\
			Id = getArg(P,0);						\
			setVarType(mb, Id, newBatType(Tpe));	\
			setVarFixed(mb, Id);					\
			list = pushArgument(mb, list, Id);		\
			pushInstruction(mb, P);					\
		}											\
	} while (0)

static int
dump_export_header(mvc *sql, MalBlkPtr mb, list *l, int file, const char * format, const char * sep,const char * rsep,const char * ssep,const char * ns, int onclient)
{
	node *n;
	int ret = -1;
	int args;

	// gather the meta information
	int tblId, nmeId, tpeId, lenId, scaleId;
	InstrPtr list;
	InstrPtr tblPtr, nmePtr, tpePtr, lenPtr, scalePtr;

	args = list_length(l) + 1;

	list = newInstructionArgs(mb, sqlRef, export_tableRef, args + 13);
	if (list == NULL)
		return -1;
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
			if (fqtn == NULL)
				return -1;
			snprintf(fqtn, fqtnl, "%s.%s", nsn, ntn);
			tblPtr = pushStr(mb, tblPtr, fqtn);
			nmePtr = pushStr(mb, nmePtr, cn);
			tpePtr = pushStr(mb, tpePtr, (t->type->localtype == TYPE_void ? "char" : t->type->base.name));
			lenPtr = pushInt(mb, lenPtr, t->digits);
			scalePtr = pushInt(mb, scalePtr, t->scale);
			list = pushArgument(mb, list, c->nr);
		} else
			return -1;
	}
	sa_reset(sql->ta);
	ret = getArg(list,0);
	pushInstruction(mb,list);
	return ret;
}


stmt *
stmt_export(backend *be, stmt *t, const char *sep, const char *rsep, const char *ssep, const char *null_string, int onclient, stmt *file)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	int fnr;
	list *l;

	if (t == NULL || t->nr < 0)
		goto bailout;
	l = t->op4.lval;
	if (file) {
		if (file->nr < 0)
			goto bailout;
		fnr = file->nr;
	} else {
		q = newAssignment(mb);
		if (q == NULL)
			goto bailout;
		q = pushStr(mb,q,"stdout");
		fnr = getArg(q,0);
		pushInstruction(mb, q);
	}
	if (t->type == st_list) {
		if (dump_export_header(be->mvc, mb, l, fnr, "csv", sep, rsep, ssep, null_string, onclient) < 0)
			goto bailout;
	} else {
		q = newStmt(mb, sqlRef, raiseRef);
		if (q == NULL)
			goto bailout;
		q = pushStr(mb, q, "not a valid output list\n");
		pushInstruction(mb, q);
	}
	stmt *s = stmt_create(be->mvc->sa, st_export);
	if(!s) {
		goto bailout;
	}
	s->op1 = t;
	s->op2 = file;
	s->q = q;
	s->nr = 1;
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_export_bin(backend *be, stmt *colstmt, bool byteswap, const char *filename, int on_client)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q;

	if (colstmt == NULL)
		goto bailout;
	q = newStmt(mb, sqlRef, export_bin_columnRef);
	if (q == NULL)
		goto bailout;
	pushArgument(mb, q, colstmt->nr);
	pushBit(mb, q, byteswap);
	pushStr(mb, q, filename);
	pushInt(mb, q, on_client);
	pushInstruction(mb, q);

	stmt *s = stmt_create(be->mvc->sa, st_export);
	if (!s)
		goto bailout;

	s->q = q;
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_trans(backend *be, int type, stmt *chain, stmt *name)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (chain == NULL || chain->nr < 0)
		goto bailout;

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
		goto bailout;
	}
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, chain->nr);
	if (name)
		q = pushArgument(mb, q, name->nr);
	else
		q = pushNil(mb, q, TYPE_str);

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_trans);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = chain;
	s->op2 = name;
	s->flag = type;
	s->q = q;
	s->nr = getDestVar(q);
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_catalog(backend *be, int type, stmt *args)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	node *n;

	if (args == NULL || args->nr < 0)
		goto bailout;

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
		goto bailout;
	}
	q = newStmtArgs(mb, sqlcatalogRef, ref, list_length(args->op4.lval) + 1);
	if (q == NULL)
		goto bailout;
	// pass all arguments as before
	for (n = args->op4.lval->h; n; n = n->next) {
		stmt *c = n->data;

		q = pushArgument(mb, q, c->nr);
	}

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_catalog);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = args;
	s->flag = type;
	s->q = q;
	s->nr = getDestVar(q);
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

void
stmt_set_nrcols(stmt *s)
{
	unsigned nrcols = 0;
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
		s->nr = f->nr;
	}
	s->nrcols = nrcols;
	s->key = key;
}

stmt *
stmt_list(backend *be, list *l)
{
	if (l == NULL)
		return NULL;
	stmt *s = stmt_create(be->mvc->sa, st_list);
	if(!s) {
		return NULL;
	}
	s->op4.lval = l;
	stmt_set_nrcols(s);
	return s;
}

static InstrPtr
dump_header(mvc *sql, MalBlkPtr mb, list *l)
{
	node *n;
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

		if (ntn && nsn && (fqtnl = strlen(ntn) + 1 + strlen(nsn) + 1) ){
			char *fqtn = SA_NEW_ARRAY(sql->ta, char, fqtnl);
			if (fqtn == NULL)
				return NULL;
			snprintf(fqtn, fqtnl, "%s.%s", nsn, ntn);
			tblPtr = pushStr(mb, tblPtr, fqtn);
			nmePtr = pushStr(mb, nmePtr, cn);
			tpePtr = pushStr(mb, tpePtr, (t->type->localtype == TYPE_void ? "char" : t->type->base.name));
			lenPtr = pushInt(mb, lenPtr, t->digits);
			scalePtr = pushInt(mb, scalePtr, t->scale);
			list = pushArgument(mb,list,c->nr);
		} else
			return NULL;
	}
	sa_reset(sql->ta);
	pushInstruction(mb,list);
	return list;
}

int
stmt_output(backend *be, stmt *lst)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	list *l = lst->op4.lval;
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

		if (ntn && nsn) {
			size_t fqtnl = strlen(ntn) + 1 + strlen(nsn) + 1;
			char *fqtn = SA_NEW_ARRAY(be->mvc->ta, char, fqtnl);
			if (fqtn == NULL)
				return -1;
			ok = 1;
			snprintf(fqtn, fqtnl, "%s.%s", nsn, ntn);

			q = newStmt(mb, sqlRef, resultSetRef);
			if (q == NULL)
				return -1;
			getArg(q,0) = newTmpVariable(mb,TYPE_int);
			q = pushStr(mb, q, fqtn);
			q = pushStr(mb, q, cn);
			q = pushStr(mb, q, t->type->localtype == TYPE_void ? "char" : t->type->base.name);
			q = pushInt(mb, q, t->digits);
			q = pushInt(mb, q, t->scale);
			q = pushInt(mb, q, t->type->eclass);
			q = pushArgument(mb, q, c->nr);
			pushInstruction(mb, q);
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
	if (q == NULL)
		return -1;
	q = pushArgument(mb, q, be->mvc_var);
	getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
	q = pushArgument(mb, q, lastnr);
	pushInstruction(mb, q);
	be->mvc_var = getDestVar(q);
	return 0;
}

stmt *
stmt_append(backend *be, stmt *c, stmt *a)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (c == NULL || a == NULL || c->nr < 0 || a->nr < 0)
		goto bailout;
	q = newStmt(mb, batRef, appendRef);
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, c->nr);
	q = pushArgument(mb, q, a->nr);
	q = pushBit(mb, q, TRUE);
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_append);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = c;
	s->op2 = a;
	s->nrcols = c->nrcols;
	s->key = c->key;
	s->nr = getDestVar(q);
	s->q = q;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_append_bulk(backend *be, stmt *c, list *l)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	bool needs_columns = false;

	if (c->nr < 0)
		goto bailout;

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
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, c->nr);
	q = pushBit(mb, q, TRUE);
	for (node *n = l->h ; n ; n = n->next) {
		stmt *a = n->data;
		q = pushArgument(mb, q, a->nr);
	}
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_append_bulk);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = c;
	s->op4.lval = l;
	s->nrcols = c->nrcols;
	s->key = c->key;
	s->nr = getDestVar(q);
	s->q = q;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_pack(backend *be, stmt *c, int n)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (c == NULL || c->nr < 0)
		goto bailout;
	q = newStmtArgs(mb, matRef, packIncrementRef, 3);
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, c->nr);
	q = pushInt(mb, q, n);
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_append);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = c;
	s->nrcols = c->nrcols;
	s->key = c->key;
	s->nr = getDestVar(q);
	s->q = q;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;

}

stmt *
stmt_pack_add(backend *be, stmt *c, stmt *a)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (c == NULL || a == NULL || c->nr < 0 || a->nr < 0)
		goto bailout;
	q = newStmtArgs(mb, matRef, packIncrementRef, 3);
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, c->nr);
	q = pushArgument(mb, q, a->nr);
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_append);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = c;
	s->op2 = a;
	s->nrcols = c->nrcols;
	s->key = c->key;
	s->nr = getDestVar(q);
	s->q = q;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_claim(backend *be, sql_table *t, stmt *cnt)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (!t || cnt->nr < 0)
		goto bailout;
	assert(t->s);				/* declared table */
	q = newStmtArgs(mb, sqlRef, claimRef, 6);
	if (q == NULL)
		goto bailout;
	/* returns offset or offsets */
	q = pushReturn(mb, q, newTmpVariable(mb, newBatType(TYPE_oid)));
	q = pushArgument(mb, q, be->mvc_var);
	q = pushSchema(mb, q, t);
	q = pushStr(mb, q, t->base.name);
	q = pushArgument(mb, q, cnt->nr);
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_claim);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = cnt;
	s->op4.tval = t;
	s->nr = getDestVar(q);
	s->q = q;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

void
stmt_add_dependency_change(backend *be, sql_table *t, stmt *cnt)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (!t || cnt->nr < 0)
		goto bailout;
	q = newStmtArgs(mb, sqlRef, dependRef, 4);
	if (q == NULL)
		goto bailout;
	q = pushSchema(mb, q, t);
	q = pushStr(mb, q, t->base.name);
	q = pushArgument(mb, q, cnt->nr);
	pushInstruction(mb, q);
	return;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : be->mb->errors ? be->mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
}

void
stmt_add_column_predicate(backend *be, sql_column *c)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (!c)
		goto bailout;
	q = newStmtArgs(mb, sqlRef, predicateRef, 4);
	if (q == NULL)
		goto bailout;
	q = pushSchema(mb, q, c->t);
	q = pushStr(mb, q, c->t->base.name);
	q = pushStr(mb, q, c->base.name);
	pushInstruction(mb, q);
	return;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : be->mb->errors ? be->mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
}

stmt *
stmt_replace(backend *be, stmt *r, stmt *id, stmt *val)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (r->nr < 0)
		goto bailout;

	q = newStmt(mb, batRef, replaceRef);
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, r->nr);
	q = pushArgument(mb, q, id->nr);
	q = pushArgument(mb, q, val->nr);
	q = pushBit(mb, q, TRUE); /* forced */
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_replace);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = r;
	s->op2 = id;
	s->op3 = val;
	s->nrcols = r->nrcols;
	s->key = r->key;
	s->nr = getDestVar(q);
	s->q = q;
	s->cand = r->cand;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_table_clear(backend *be, sql_table *t, int restart_sequences)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (!t->s && ATOMIC_PTR_GET(&t->data)) { /* declared table */
		int *l = ATOMIC_PTR_GET(&t->data), cnt = ol_length(t->columns)+1;

		for (int i = 0; i < cnt; i++) {
			q = newStmt(mb, batRef, deleteRef);
			if (q == NULL)
				goto bailout;
			q = pushArgument(mb, q, l[i]);
			pushInstruction(mb, q);
		}
		/* declared tables don't have sequences */
	} else {
		q = newStmt(mb, sqlRef, clear_tableRef);
		if (q == NULL)
			goto bailout;
		q = pushSchema(mb, q, t);
		q = pushStr(mb, q, t->base.name);
		q = pushInt(mb, q, restart_sequences);
		pushInstruction(mb, q);
	}
	stmt *s = stmt_create(be->mvc->sa, st_table_clear);

	if(!s) {
		goto bailout;
	}
	s->op4.tval = t;
	s->nrcols = 0;
	s->nr = getDestVar(q);
	s->q = q;
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
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
	if (q == NULL)
		goto bailout;
	q = pushArgument(mb, q, cond->nr);
	q = pushStr(mb, q, errstr);
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_exception);
	be->mvc->sa->eb.enabled = enabled;
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
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

/* The type setting is not propagated to statements such as st_bat and st_append,
	because they are not considered projections */
static void
tail_set_type(mvc *m, stmt *st, sql_subtype *t)
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
			st->op4.aval = atom_set_type(m->sa, st->op4.aval, t);
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

static stmt *
temporal_convert(backend *be, stmt *v, stmt *sel, sql_subtype *f, sql_subtype *t, bool before)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *convert = t->type->impl, *mod = mtimeRef;
	bool add_tz = false, pushed = (v->cand && v->cand == sel), cand = 0;

	if (before) {
		if (f->type->eclass == EC_TIMESTAMP_TZ && (t->type->eclass == EC_TIMESTAMP || t->type->eclass == EC_TIME)) {
			/* call timestamp+local_timezone */
			convert = "timestamp_add_msec_interval";
			add_tz = true;
		} else if (f->type->eclass == EC_TIMESTAMP_TZ && t->type->eclass == EC_DATE) {
			/* call convert timestamp with tz to date */
			convert = "datetz";
			mod = calcRef;
			add_tz = true;
		} else if (f->type->eclass == EC_TIMESTAMP && t->type->eclass == EC_TIMESTAMP_TZ) {
			/* call timestamp+local_timezone */
			convert = "timestamp_sub_msec_interval";
			add_tz = true;
		} else if (f->type->eclass == EC_TIME_TZ && (t->type->eclass == EC_TIME || t->type->eclass == EC_TIMESTAMP)) {
			/* call times+local_timezone */
			convert = "time_add_msec_interval";
			add_tz = true;
		} else if (f->type->eclass == EC_TIME && t->type->eclass == EC_TIME_TZ) {
			/* call times+local_timezone */
			convert = "time_sub_msec_interval";
			add_tz = true;
		} else if (EC_VARCHAR(f->type->eclass) && EC_TEMP_TZ(t->type->eclass)) {
			if (t->type->eclass == EC_TIME_TZ)
				convert = "daytimetz";
			else
				convert = "timestamptz";
			mod = calcRef;
			add_tz = true;
			cand = 1;
		} else {
			return v;
		}
	} else {
		if (f->type->eclass == EC_DATE && t->type->eclass == EC_TIMESTAMP_TZ) {
			convert = "timestamp_sub_msec_interval";
			add_tz = true;
		} else if (f->type->eclass == EC_DATE && t->type->eclass == EC_TIME_TZ) {
			convert = "time_sub_msec_interval";
			add_tz = true;
		} else {
			return v;
		}
	}

	if (v->nrcols == 0 && (!sel || sel->nrcols == 0)) {	/* simple calc */
		q = newStmtArgs(mb, mod, convert, 13);
		if (q == NULL)
			goto bailout;
	} else {
		if (sel && !pushed && v->nrcols == 0) {
			pushed = 1;
			v = stmt_project(be, sel, v);
			v->cand = sel;
		}
		q = newStmtArgs(mb, mod==calcRef?batcalcRef:batmtimeRef, convert, 13);
		if (q == NULL)
			goto bailout;
	}
	q = pushArgument(mb, q, v->nr);

	if (cand) {
		if (sel && !pushed && !v->cand) {
			q = pushArgument(mb, q, sel->nr);
			pushed = 1;
		} else if (v->nrcols > 0) {
			q = pushNilBat(mb, q);
		}
	}

	if (EC_VARCHAR(f->type->eclass))
		q = pushInt(mb, q, t->digits);

	if (add_tz)
			q = pushLng(mb, q, be->mvc->timezone);

	if (!cand) {
		if (sel && !pushed && !v->cand) {
			q = pushArgument(mb, q, sel->nr);
			pushed = 1;
		} else if (v->nrcols > 0) {
			q = pushNilBat(mb, q);
		}
	}

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_convert);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = v;
	s->nrcols = 0;	/* function without arguments returns single value */
	s->key = v->key;
	s->nrcols = v->nrcols;
	s->aggr = v->aggr;
	s->op4.typeval = *t;
	s->nr = getDestVar(q);
	s->q = q;
	s->cand = pushed ? sel : NULL;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_convert(backend *be, stmt *v, stmt *sel, sql_subtype *f, sql_subtype *t)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *convert = t->type->impl, *mod = calcRef;
	int pushed = (v->cand && v->cand == sel), no_candidates = 0;
	bool add_tz = false;
	/* convert types and make sure they are rounded up correctly */

	if (v->nr < 0)
		goto bailout;

	if (f->type->eclass != EC_EXTERNAL && t->type->eclass != EC_EXTERNAL &&
		/* general cases */
		((t->type->localtype == f->type->localtype && t->type->eclass == f->type->eclass &&
		!EC_INTERVAL(f->type->eclass) && f->type->eclass != EC_DEC && (t->digits == 0 || f->digits == t->digits) && type_has_tz(t) == type_has_tz(f)) ||
		/* trivial decimal cases */
		(f->type->eclass == EC_DEC && t->type->eclass == EC_DEC && f->scale == t->scale && f->type->localtype == t->type->localtype) ||
		/* trivial string cases */
		(EC_VARCHAR(f->type->eclass) && EC_VARCHAR(t->type->eclass) && (t->digits == 0 || (f->digits > 0 && t->digits >= f->digits))))) {
		/* set output type. Despite the MAL code already being generated, the output type may still be checked */
		tail_set_type(be->mvc, v, t);
		return v;
	}

	/* external types have sqlname convert functions,
	   these can generate errors (fromstr cannot) */
	if (t->type->eclass == EC_EXTERNAL)
		convert = t->type->base.name;
	else if (t->type->eclass == EC_MONTH)
		convert = "month_interval";
	else if (t->type->eclass == EC_SEC)
		convert = "second_interval";

	no_candidates = t->type->eclass == EC_EXTERNAL && strcmp(convert, "uuid") != 0; /* uuids conversions support candidate lists */

	if ((type_has_tz(f) && !type_has_tz(t) && !EC_VARCHAR(t->type->eclass)) || (!type_has_tz(f) && type_has_tz(t))) {
		v = temporal_convert(be, v, sel, f, t, true);
		sel = NULL;
		pushed = 0;
		if (EC_VARCHAR(f->type->eclass))
			return v;
	}

	/* Lookup the sql convert function, there is no need
	 * for single value vs bat, this is handled by the
	 * mal function resolution */
	if (v->nrcols == 0 && (!sel || sel->nrcols == 0)) {	/* simple calc */
		q = newStmtArgs(mb, mod, convert, 13);
		if (q == NULL)
			goto bailout;
	} else if ((v->nrcols > 0 || (sel && sel->nrcols > 0)) && no_candidates) {
		int type = t->type->localtype;

		/* with our current implementation, all internal SQL types have candidate list support on their conversions */
		if (sel && !pushed) {
			pushed = 1;
			v = stmt_project(be, sel, v);
			v->cand = sel;
		}
		q = newStmtArgs(mb, malRef, multiplexRef, 15);
		if (q == NULL)
			goto bailout;
		setVarType(mb, getArg(q, 0), newBatType(type));
		q = pushStr(mb, q, convertMultiplexMod(mod, convert));
		q = pushStr(mb, q, convertMultiplexFcn(convert));
	} else {
		if (v->nrcols == 0 && sel && !pushed) {
			pushed = 1;
			v = stmt_project(be, sel, v);
			v->cand = sel;
		}
		q = newStmtArgs(mb, mod==calcRef?batcalcRef:batmtimeRef, convert, 13);
		if (q == NULL)
			goto bailout;
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
	if (add_tz)
			q = pushLng(mb, q, be->mvc->timezone);
	if (sel && !pushed && !v->cand) {
		q = pushArgument(mb, q, sel->nr);
		pushed = 1;
	} else if (v->nrcols > 0 && !no_candidates) {
		q = pushNilBat(mb, q);
	}
	if (!add_tz && (t->type->eclass == EC_DEC || EC_TEMP_FRAC(t->type->eclass) || EC_INTERVAL(t->type->eclass))) {
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
		//q = pushInt(mb, q, type_has_tz(t));
		q = pushLng(mb, q, be->mvc->timezone);
	if (t->type->eclass == EC_GEOM) {
		/* push the type and coordinates of the column */
		q = pushInt(mb, q, t->digits);
		/* push the SRID of the whole columns */
		q = pushInt(mb, q, t->scale);
		/* push the type and coordinates of the inserted value */
		//q = pushInt(mb, q, f->digits);
		/* push the SRID of the inserted value */
		//q = pushInt(mb, q, f->scale);
		/* we decided to create the EWKB type also used by PostGIS and has the SRID provided by the user inside already */
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

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_convert);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = v;
	s->nrcols = 0;	/* function without arguments returns single value */
	s->key = v->key;
	s->nrcols = v->nrcols;
	s->aggr = v->aggr;
	s->op4.typeval = *t;
	s->nr = getDestVar(q);
	s->q = q;
	s->cand = pushed ? sel : NULL;
	pushInstruction(mb, q);
	if ((!type_has_tz(f) && type_has_tz(t)))
		return temporal_convert(be, s, NULL, f, t, false);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_unop(backend *be, stmt *op1, stmt *sel, sql_subfunc *op)
{
	list *ops = sa_list(be->mvc->sa);
	list_append(ops, op1);
	stmt *r = stmt_Nop(be, stmt_list(be, ops), sel, op, NULL);
	if (r && !r->cand)
		r->cand = op1->cand;
	return r;
}

stmt *
stmt_binop(backend *be, stmt *op1, stmt *op2, stmt *sel, sql_subfunc *op)
{
	list *ops = sa_list(be->mvc->sa);
	list_append(ops, op1);
	list_append(ops, op2);
	stmt *r = stmt_Nop(be, stmt_list(be, ops), sel, op, NULL);
	if (r && !r->cand)
		r->cand = op1->cand?op1->cand:op2->cand;
	return r;
}

#define LANG_INT_OR_MAL(l)  ((l)==FUNC_LANG_INT || (l)==FUNC_LANG_MAL)

stmt *
stmt_Nop(backend *be, stmt *ops, stmt *sel, sql_subfunc *f, stmt* rows)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	const char *mod = sql_func_mod(f->func), *fimp = backend_function_imp(be, f->func);
	sql_subtype *tpe = NULL;
	int push_cands = 0, default_nargs;
	stmt *o = NULL, *card = NULL;

	if (ops == NULL)
		goto bailout;

	if (rows) {
		if (sel) /* if there's a candidate list, use it instead of 'rows' */
			rows = sel;
		o = rows;
	} else if (list_length(ops->op4.lval)) {
		o = ops->op4.lval->h->data;
		for (node *n = ops->op4.lval->h; n; n = n->next) {
			stmt *c = n->data;

			if (c && o->nrcols < c->nrcols)
				o = c;
		}
	}

	/* handle nullif */
	if (list_length(ops->op4.lval) == 2 &&
		strcmp(mod, "") == 0 && strcmp(fimp, "") == 0) {
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
			if (q == NULL)
				goto bailout;
			q = pushArgument(mb, q, e1->nr);
			q = pushArgument(mb, q, e2->nr);
			int nr = getDestVar(q);
			pushInstruction(mb, q);

			q = newStmt(mb, mod, ifthenelseRef);
			if (q == NULL)
				goto bailout;
			q = pushArgument(mb, q, nr);
			q = pushNil(mb, q, tt);
			q = pushArgument(mb, q, e1->nr);
			pushInstruction(mb, q);
		}
		push_cands = f->func->type == F_FUNC && can_push_cands(sel, mod, fimp);
	}
	if (q == NULL) {
		if (backend_create_subfunc(be, f, ops->op4.lval) < 0)
			goto bailout;
		mod = sql_func_mod(f->func);
		fimp = convertMultiplexFcn(backend_function_imp(be, f->func));
		push_cands = f->func->type == F_FUNC && can_push_cands(sel, mod, fimp);
		default_nargs = (f->res && list_length(f->res) ? list_length(f->res) : 1) + list_length(ops->op4.lval) + (o && o->nrcols > 0 ? 6 : 4);
		if (rows) {
			card = stmt_aggr(be, rows, NULL, NULL, sql_bind_func(be->mvc, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR, true, true), 1, 0, 1);
			default_nargs++;
		}

		if (o && o->nrcols > 0 && f->func->type != F_LOADER && f->func->type != F_PROC) {
			sql_subtype *res = f->res->h->data;

			q = newStmtArgs(mb, f->func->type == F_UNION ? batmalRef : malRef, multiplexRef, default_nargs);
			if (q == NULL)
				goto bailout;
			if (rows)
				q = pushArgument(mb, q, card->nr);
			q = pushStr(mb, q, mod);
			q = pushStr(mb, q, fimp);
			setVarType(mb, getArg(q, 0), newBatType(res->type->localtype));
		} else {
			q = newStmtArgs(mb, mod, fimp, default_nargs);
			if (q == NULL)
				goto bailout;

			if (rows)
				q = pushArgument(mb, q, card->nr);
			if (f->res && list_length(f->res)) {
				sql_subtype *res = f->res->h->data;

				setVarType(mb, getArg(q, 0), res->type->localtype);
			}
		}
		if (LANG_EXT(f->func->lang)) {
			/* TODO LOADER functions still use information in sql_subfunc struct
			   that won't be visible to other sessions if another function uses them.
			   It has to be cleaned up */
			if (f->func->type == F_LOADER)
				q = pushPtr(mb, q, f);
			else
				q = pushPtr(mb, q, f->func);
		}
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

		for (node *n = ops->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;
			q = pushArgument(mb, q, op->nr);
		}
		/* push candidate lists if that's the case */
		if (push_cands) {
			for (node *n = ops->op4.lval->h; n; n = n->next) {
				stmt *op = n->data;

				if (op->nrcols > 0) {
					if (op->cand && op->cand == sel) {
						q = pushNilBat(mb, q);
					} else {
						q = pushArgument(mb, q, sel->nr);
					}
				}
			}
		}
		/* special case for round function on decimals */
		if (LANG_INT_OR_MAL(f->func->lang) && strcmp(fimp, "round") == 0 && tpe && tpe->type->eclass == EC_DEC && ops->op4.lval->h && ops->op4.lval->h->data) {
			q = pushInt(mb, q, tpe->digits);
			q = pushInt(mb, q, tpe->scale);
		}
		pushInstruction(mb, q);
	}

	stmt *s = stmt_create(be->mvc->sa, st_Nop);
	if(!s) {
		goto bailout;
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
	if (sel && push_cands && s->nrcols)
		s->cand = sel;
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_direct_func(backend *be, InstrPtr q)
{
	if (q) {
		stmt *s = stmt_create(be->mvc->sa, st_func);
		if(!s) {
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
stmt_func(backend *be, stmt *ops, const char *name, sql_rel *rel, int f_union)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;
	prop *p = NULL;

	/* dump args */
	if (ops && ops->nr < 0)
		goto bailout;

	if ((p = find_prop(rel->p, PROP_REMOTE)))
		rel->p = prop_remove(rel->p, p);
	/* sql_processrelation may split projections, so make sure the topmost relation only contains references */
	int opt = rel->opt;
	rel = rel_project(be->mvc->sa, rel, rel_projections(be->mvc, rel, NULL, 1, 1));
	if (!(rel = sql_processrelation(be->mvc, rel, 0, 0, 1, 1)))
		goto bailout;
	if (p) {
		p->p = rel->p;
		rel->p = p;
	}
	rel->opt = opt;

	if (monet5_create_relational_function(be->mvc, sql_private_module_name, name, rel, ops, NULL, 1) < 0)
		goto bailout;

	int nargs;
	sql_rel *r = relational_func_create_result_part1(be->mvc, rel, &nargs);
	if (ops)
		nargs += list_length(ops->op4.lval);
	if (f_union)
		q = newStmt(mb, batmalRef, multiplexRef);
	else
		q = newStmt(mb, sql_private_module_name, name);
	if (q == NULL)
		goto bailout;
	q = relational_func_create_result_part2(mb, q, r);
	if (f_union) {
		q = pushStr(mb, q, sql_private_module_name);
		q = pushStr(mb, q, name);
	}
	if (ops) {
		for (node *n = ops->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;

			q = pushArgument(mb, q, op->nr);
		}
	}

	allocator *sa = be->mvc->sa;
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *o = NULL, *s = stmt_create(sa, st_func);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = ops;
	s->op2 = stmt_atom_string(be, name);
	s->op4.rel = rel;
	s->flag = f_union;
	if (ops && list_length(ops->op4.lval)) {
		node *n;
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
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_aggr(backend *be, stmt *op1, stmt *grp, stmt *ext, sql_subfunc *op, int reduce, int no_nil, int nil_if_empty)
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
		goto bailout;
	if (backend_create_subfunc(be, op, NULL) < 0)
		goto bailout;
	mod = sql_func_mod(op->func);
	aggrfunc = backend_function_imp(be, op->func);

	if (LANG_INT_OR_MAL(op->func->lang)) {
		if (strcmp(aggrfunc, "avg") == 0)
			avg = 1;
		if (avg || strcmp(aggrfunc, "sum") == 0 || strcmp(aggrfunc, "prod") == 0
			|| strcmp(aggrfunc, "str_group_concat") == 0)
			complex_aggr = true;
		if (restype == TYPE_dbl)
			avg = 0;
	}

	int argc = 1
		+ 2 * avg
		+ (LANG_EXT(op->func->lang) != 0)
		+ 2 * (op->func->lang == FUNC_LANG_C || op->func->lang == FUNC_LANG_CPP)
		+ (op->func->lang == FUNC_LANG_PY || op->func->lang == FUNC_LANG_R)
		+ (op1->type != st_list ? 1 : list_length(op1->op4.lval))
		+ (grp ? 4 : avg + 1);

	if (grp) {
		char *aggrF = SA_NEW_ARRAY(be->mvc->sa, char, strlen(aggrfunc) + 4);
		if (!aggrF)
			goto bailout;
		stpcpy(stpcpy(aggrF, "sub"), aggrfunc);
		aggrfunc = aggrF;
		if ((grp && grp->nr < 0) || (ext && ext->nr < 0))
			goto bailout;

		q = newStmtArgs(mb, mod, aggrfunc, argc);
		if (q == NULL)
			goto bailout;
		setVarType(mb, getArg(q, 0), newBatType(restype));
		if (avg) { /* for avg also return rest and count */
			q = pushReturn(mb, q, newTmpVariable(mb, newBatType(TYPE_lng)));
			q = pushReturn(mb, q, newTmpVariable(mb, newBatType(TYPE_lng)));
		}
	} else {
		q = newStmtArgs(mb, mod, aggrfunc, argc);
		if (q == NULL)
			goto bailout;
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
		if (LANG_INT_OR_MAL(op->func->lang)) {
			if (avg) /* push nil candidates */
				q = pushNilBat(mb, q);
			q = pushBit(mb, q, no_nil);
		}
	} else if (LANG_INT_OR_MAL(op->func->lang) && no_nil && strncmp(aggrfunc, "count", 5) == 0) {
		q = pushBit(mb, q, no_nil);
	} else if (LANG_INT_OR_MAL(op->func->lang) && !nil_if_empty && strncmp(aggrfunc, "sum", 3) == 0) {
		q = pushBit(mb, q, FALSE);
	} else if (LANG_INT_OR_MAL(op->func->lang) && avg) { /* push candidates */
		q = pushNilBat(mb, q);
		q = pushBit(mb, q, no_nil);
	}

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_aggr);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
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
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

static stmt *
stmt_alias_(backend *be, stmt *op1, int label, const char *tname, const char *alias)
{
	assert(label);
	stmt *s = stmt_create(be->mvc->sa, st_alias);
	if(!s) {
		return NULL;
	}
	s->label = label;
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
stmt_alias(backend *be, stmt *op1, int label, const char *tname, const char *alias)
{
	/*
	if (((!op1->tname && !tname) ||
	    (op1->tname && tname && strcmp(op1->tname, tname)==0)) &&
	    op1->cname && strcmp(op1->cname, alias)==0)
		return op1;
		*/
	return stmt_alias_(be, op1, label, tname, alias);
}

stmt *
stmt_as(backend *be, stmt *s, stmt *org)
{
	assert(org->type == st_alias);
	return stmt_alias_(be, s, org->label, org->tname, org->cname);
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
		case st_alias:
			if (!st->op1)
				return &st->op4.typeval;
			/* fall through */
		case st_append:
		case st_append_bulk:
		case st_replace:
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
		case st_tid:
		case st_mirror:
			return sql_bind_localtype("oid");
		case st_result:
			return &st->op4.typeval;
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
	case st_semijoin:
	case st_uselect:
	case st_uselect2:
	case st_atom:
		return 0;
	case st_alias:
		return stmt_has_null(s->op1);
	case st_join:
		return stmt_has_null(s->op2);
	case st_bat:
		return s->op4.cval->null;

	default:
		return 1;
	}
}

static const char *
func_name(allocator *sa, const char *n1, const char *n2)
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

static const char *_column_name(allocator *sa, stmt *st);

const char *
column_name(allocator *sa, stmt *st)
{
	if (!st->cname)
		st->cname = _column_name(sa, st);
	return st->cname;
}

static const char *
_column_name(allocator *sa, stmt *st)
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
table_name(allocator *sa, stmt *st)
{
	(void)sa;
	return st->tname;
}

const char *
schema_name(allocator *sa, stmt *st)
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
		/* there are no schema aliases, ie look into the base column */
		if (st->op1)
			return schema_name(sa, st->op1);
		return NULL;
	case st_alias:
		if (!st->op1)
			return NULL;
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
		goto bailout;
	if (anti) {
		sql_subtype *bt = sql_bind_localtype("bit");
		sql_subfunc *not = sql_bind_func(be->mvc, "sys", "not", bt, NULL, F_FUNC, true, true);
		sql_subfunc *or = sql_bind_func(be->mvc, "sys", "or", bt, bt, F_FUNC, true, true);
		sql_subfunc *isnull = sql_bind_func(be->mvc, "sys", "isnull", bt, NULL, F_FUNC, true, true);
		cond = stmt_binop(be,
			stmt_unop(be, cond, NULL, not),
			stmt_unop(be, cond, NULL, isnull), NULL, or);
	}
	if (!loop) {	/* if */
		q = newAssignment(mb);
		if (q == NULL)
			goto bailout;
		q->barrier = BARRIERsymbol;
		q = pushArgument(mb, q, cond->nr);
	} else {	/* while */
		int c;

		if (outer->nr < 0)
			goto bailout;
		/* leave barrier */
		q = newStmt(mb, calcRef, notRef);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, cond->nr);
		c = getArg(q, 0);
		pushInstruction(mb, q);

		q = newAssignment(mb);
		if (q == NULL)
			goto bailout;
		getArg(q, 0) = outer->nr;
		q->barrier = LEAVEsymbol;
		q = pushArgument(mb, q, c);
	}

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_cond);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->flag = be->mvc_var; /* keep the mvc_var of the outer context */
	s->loop = loop;
	s->op1 = cond;
	s->nr = getArg(q, 0);
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_control_end(backend *be, stmt *cond)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (cond->nr < 0)
		goto bailout;

	if (cond->loop) {	/* while */
		/* redo barrier */
		q = newAssignment(mb);
		if (q == NULL)
			goto bailout;
		getArg(q, 0) = cond->nr;
		q->argc = q->retc = 1;
		q->barrier = REDOsymbol;
		q = pushBit(mb, q, TRUE);
	} else {
		q = newAssignment(mb);
		if (q == NULL)
			goto bailout;
		getArg(q, 0) = cond->nr;
		q->argc = q->retc = 1;
		q->barrier = EXITsymbol;
	}
	be->mvc_var = cond->flag; /* restore old mvc_var from before the barrier */
	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_control_end);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = cond;
	s->nr = getArg(q, 0);
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
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
	/* Let's make it a proper assignment */
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
		goto bailout;
	int args = val->type == st_table ? 2 * list_length(val->op1->op4.lval) : 0;
	if (args < MAXARG)
		args = MAXARG;
	q = newInstructionArgs(mb, NULL, NULL, args);
	if (q == NULL)
		goto bailout;
	q->barrier= RETURNsymbol;
	if (val->type == st_table) {
		list *l = val->op1->op4.lval;

		q = dump_cols(mb, l, q);
	} else {
		getArg(q, 0) = getArg(getInstrPtr(mb, 0), 0);
		q = pushArgument(mb, q, val->nr);
	}

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_return);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = val;
	s->flag = nr_declared_tables;
	s->nr = getDestVar(q);
	s->q = q;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_assign(backend *be, const char *sname, const char *varname, stmt *val, int level)
{
	MalBlkPtr mb = be->mb;
	InstrPtr q = NULL;

	if (val && val->nr < 0)
		goto bailout;
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
			goto bailout;
		stpcpy(stpcpy(stpcpy(stpcpy(buf, "A"), levelstr), "%"), varname); /* mangle variable name */
		q = newInstruction(mb, NULL, NULL);
		if (q == NULL) {
			goto bailout;
		}
		q->argc = q->retc = 0;
		q = pushArgumentId(mb, q, buf);
		pushInstruction(mb, q);
		q->retc++;
	} else {
		assert(sname); /* all global variables have a schema */
		q = newStmt(mb, sqlRef, setVariableRef);
		if (q == NULL)
			goto bailout;
		q = pushArgument(mb, q, be->mvc_var);
		q = pushStr(mb, q, sname);
		q = pushStr(mb, q, varname);
		getArg(q, 0) = be->mvc_var = newTmpVariable(mb, TYPE_int);
		pushInstruction(mb, q);
		be->mvc_var = getDestVar(q);
	}
	q = pushArgument(mb, q, val->nr);

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_assign);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		goto bailout;
	}
	s->op2 = val;
	s->flag = (level << 1);
	s->q = q;
	s->nr = 1;
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
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
		goto bailout;
	q = newStmt(mb, batRef, singleRef);
	if (q == NULL)
		goto bailout;
	setVarType(mb, getArg(q, 0), newBatType(tt));
	q = pushArgument(mb, q, val->nr);

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_single);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = val;
	s->op4.typeval = *ct;
	s->nrcols = 1;

	s->tname = val->tname;
	s->cname = val->cname;
	s->nr = getDestVar(q);
	s->q = q;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
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
		goto bailout;
	/* pick from first column on a table case */
	if (val->type == st_table) {
		if (list_length(val->op1->op4.lval) > 1)
			goto bailout;
		val = val->op1->op4.lval->h->data;
	}
	ct = tail_type(val);
	tt = ct->type->localtype;

	q = newStmt(mb, algebraRef, fetchRef);
	if (q == NULL)
		goto bailout;
	setVarType(mb, getArg(q, 0), tt);
	q = pushArgument(mb, q, val->nr);
	q = pushOid(mb, q, 0);

	bool enabled = be->mvc->sa->eb.enabled;
	be->mvc->sa->eb.enabled = false;
	stmt *s = stmt_create(be->mvc->sa, st_single);
	be->mvc->sa->eb.enabled = enabled;
	if(!s) {
		freeInstruction(q);
		goto bailout;
	}
	s->op1 = val;
	s->op4.typeval = *ct;
	s->nrcols = 0;

	s->tname = val->tname;
	s->cname = val->cname;
	s->nr = getDestVar(q);
	s->q = q;
	pushInstruction(mb, q);
	return s;

  bailout:
	if (be->mvc->sa->eb.enabled)
		eb_error(&be->mvc->sa->eb, be->mvc->errstr[0] ? be->mvc->errstr : mb->errors ? mb->errors : *GDKerrbuf ? GDKerrbuf : "out of memory", 1000);
	return NULL;
}

stmt *
stmt_rename(backend *be, sql_exp *exp, stmt *s )
{
	int label = exp_get_label(exp);
	const char *name = exp_name(exp);
	const char *rname = exp_relname(exp);
	stmt *o = s;

	if (!name && exp_is_atom(exp))
		name = sa_strdup(be->mvc->sa, "single_value");
	assert(name);
	s = stmt_alias(be, s, label, rname, name);
	if (o->flag & OUTER_ZERO)
		s->flag |= OUTER_ZERO;
	return s;
}
