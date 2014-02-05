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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f sql_gencode
 * @t SQL to MAL code generation.
 * @a N. Nes, M. Kersten
 * @+ MAL Code generation
 * This module contains the actions to construct a MAL program, ready for
 * optimization and execution by the Monet V5 kernel.
 *
 * The code base is modeled directly after its MIL variant, replacing
 * each IO request by instructions to initialize the corresponding MAL data
 * structure.
 * To speed up the compilation, we may consider keeping a cache of pre-compiled
 * statements.
 *
 * MAL extensions needed. A temporary variable used as an argument
 * should be printed (done). Consider replacing modname/fcnname by
 * an integer constant and a global lookup table. This should
 * reduce the cost to prepare MAL statements significantly.
 *
 * A dummy module is needed to load properly.
 * @-
 */
#include "monetdb_config.h"
#include "sql_gencode.h"
#include "sql_optimizer.h"
#include "sql_scenario.h"
#include "mal_namespace.h"
#include "opt_prelude.h"
#include "querylog.h"
#include "mal_builder.h"

#include <rel_select.h>
#include <rel_optimizer.h>
#include <rel_prop.h>
#include <rel_exp.h>
#include <rel_bin.h>

static int _dumpstmt(backend *sql, MalBlkPtr mb, stmt *s);
static int backend_dumpstmt(backend *be, MalBlkPtr mb, stmt *s, int top);

/*
 * @+ MAL code support
 * To simplify construction of the MAL program use the following
 * macros
 *
 * @+ MAL initialization
 * Many instructions have a more or less fixed structure, therefore
 * they can be assembled in a pre-compiled block. Each time we need it,
 * a copy can be extracted and included in the MAL block
 *
 * The catalog relations should be maintained in a MAL box, which
 * provides the handle for transaction management.
 * @-
 * The atoms produced by the parser should be converted back into
 * MAL constants. Ideally, this should not be necessary when the
 * SQL parser keeps the string representation around.
 * This involves regeneration of their string as well and
 * trimming the enclosing string quotes.
 */
static int
constantAtom(backend *sql, MalBlkPtr mb, atom *a)
{
	int idx;
	ValPtr vr = (ValPtr) &a->data;
	ValRecord cst;

	(void) sql;
	cst.vtype = 0;
	VALcopy(&cst, vr);
	idx = defConstant(mb, vr->vtype, &cst);
	return idx;
}

static int
argumentZero(MalBlkPtr mb, int tpe)
{
	ValRecord cst;

	cst.vtype = TYPE_int;
	cst.val.ival = 0;
	convertConstant(tpe, &cst);
	return defConstant(mb, tpe, &cst);
}

/*
 * @-
 * To speedup code generation we freeze the references to the major modules.
 * This safes table lookups.
 */
static str exportValueRef;
static str exportResultRef;

void
initSQLreferences(void)
{
	optimizerInit();
	if (exportValueRef == NULL) {
		exportValueRef = putName("exportValue", 11);
		exportResultRef = putName("exportResult", 12);
	}
	if (algebraRef == NULL || exportValueRef == NULL || exportResultRef == NULL)
		GDKfatal("error initSQLreferences");
}

/*
 * @-
 * The dump_header produces a sequence of instructions for
 * the front-end to prepare presentation of a result table.
 */
static void
dump_header(mvc *sql, MalBlkPtr mb, stmt *s, list *l)
{
	node *n;
	InstrPtr q;

	for (n = l->h; n; n = n->next) {
		stmt *c = n->data;
		sql_subtype *t = tail_type(c);
		char *tname = table_name(sql->sa, c);
		char *sname = schema_name(sql->sa, c);
		char *_empty = "";
		char *tn = (tname) ? tname : _empty;
		char *sn = (sname) ? sname : _empty;
		char *cn = column_name(sql->sa, c);
		char *ntn = sql_escape_ident(tn);
		char *nsn = sql_escape_ident(sn);
		size_t fqtnl = strlen(ntn) + 1 + strlen(nsn) + 1;
		char *fqtn = NEW_ARRAY(char, fqtnl);

		snprintf(fqtn, fqtnl, "%s.%s", nsn, ntn);

		q = newStmt1(mb, sqlRef, "rsColumn");
		q = pushArgument(mb, q, s->nr);
		q = pushStr(mb, q, fqtn);
		q = pushStr(mb, q, cn);
		q = pushStr(mb, q, t->type->localtype == TYPE_void ? "char" : t->type->sqlname);
		q = pushInt(mb, q, t->digits);
		q = pushInt(mb, q, t->scale);
		(void) pushArgument(mb, q, c->nr);
		_DELETE(ntn);
		_DELETE(nsn);
		_DELETE(fqtn);
	}
}

static int
dump_table(MalBlkPtr mb, sql_table *t)
{
	int nr;
	node *n;
	InstrPtr k = newStmt1(mb, sqlRef, "declaredTable");

	nr = getDestVar(k);
	(void) pushStr(mb, k, t->base.name);
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		char *tname = c->t->base.name;
		char *tn = sql_escape_ident(tname);
		char *cn = c->base.name;
		InstrPtr q = newStmt1(mb, sqlRef, "dtColumn");

		q = pushArgument(mb, q, nr);
		q = pushStr(mb, q, tn);
		q = pushStr(mb, q, cn);
		q = pushStr(mb, q, c->type.type->localtype == TYPE_void ? "char" : c->type.type->sqlname);
		q = pushInt(mb, q, c->type.digits);
		(void) pushInt(mb, q, c->type.scale);
		_DELETE(tn);
	}
	return nr;
}

static int
drop_table(MalBlkPtr mb, str n)
{
	InstrPtr k = newStmt1(mb, sqlRef, "dropDeclaredTable");
	int nr = getDestVar(k);

	(void) pushStr(mb, k, n);
	return nr;
}

static InstrPtr
dump_cols(MalBlkPtr mb, list *l, InstrPtr q)
{
	int i;
	node *n;

	q->retc = q->argc = 0;
	for (i = 0, n = l->h; n; n = n->next, i++) {
		stmt *c = n->data;

		q = pushArgument(mb, q, c->nr);
	}
	q->retc = q->argc;
	/* Lets make it a propper assignment */
	for (i = 0, n = l->h; n; n = n->next, i++) {
		stmt *c = n->data;

		q = pushArgument(mb, q, c->nr);
	}
	return q;
}

static InstrPtr
table_func_create_result(MalBlkPtr mb, InstrPtr q, sql_table *f)
{
	node *n;
	int i;

	for (i = 0, n = f->columns.set->h; n; n = n->next, i++) {
		sql_column *c = n->data;
		int type = c->type.type->localtype;

		type = newBatType(TYPE_oid, type);
		if (i)
			q = pushReturn(mb, q, newTmpVariable(mb, type));
		else
			setVarType(mb, getArg(q, 0), type);
		setVarUDFtype(mb, getArg(q, i));
	}
	return q;
}

static InstrPtr
relational_func_create_result(mvc *sql, MalBlkPtr mb, InstrPtr q, sql_rel *f)
{
	sql_rel *r = f;
	node *n;
	int i;

	if (is_topn(r->op))
		r = r->l;
	if (!is_project(r->op))
		r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
	q->argc = q->retc = 0;
	for (i = 0, n = r->exps->h; n; n = n->next, i++) {
		sql_exp *e = n->data;
		int type = exp_subtype(e)->type->localtype;

		type = newBatType(TYPE_oid, type);
		q = pushReturn(mb, q, newTmpVariable(mb, type));
	}
	return q;
}


static int
_create_relational_function(mvc *m, char *name, sql_rel *rel, stmt *call)
{
	sql_rel *r;
	Client c = MCgetClient(m->clientid);
	backend *be = (backend *) c->sqlcontext;
	MalBlkPtr curBlk = 0;
	InstrPtr curInstr = 0;
	Symbol backup = NULL;
	stmt *s;

	r = rel_optimizer(m, rel);
	s = rel_bin(m, r);

	if (s->type == st_list && s->nrcols == 0 && s->key) {
		/* row to columns */
		node *n;
		list *l = sa_list(m->sa);

		for (n = s->op4.lval->h; n; n = n->next)
			list_append(l, const_column(m->sa, n->data));
		s = stmt_list(m->sa, l);
	}
	s = stmt_table(m->sa, s, 1);
	s = stmt_return(m->sa, s, 0);

	backup = c->curprg;
	c->curprg = newFunction(userRef, putName(name, strlen(name)), FUNCTIONsymbol);

	curBlk = c->curprg->def;
	curInstr = getInstrPtr(curBlk, 0);

	curInstr = relational_func_create_result(m, curBlk, curInstr, r);
	setVarUDFtype(curBlk, 0);

	/* ops */
	if (call->op1->type == st_list) {
		node *n;

		for (n = call->op1->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;
			sql_subtype *t = tail_type(op);
			int type = t->type->localtype;
			int varid = 0;
			char *nme = op->op3->op4.aval->data.val.sval;

			varid = newVariable(curBlk, _STRDUP(nme), type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
			setVarUDFtype(curBlk, varid);
		}
	}

	if (backend_dumpstmt(be, curBlk, s, 0) < 0)
		return -1;
	/* SQL function definitions meant for inlineing should not be optimized before */
	varSetProp(curBlk, getArg(curInstr, 0), sqlfunctionProp, op_eq, NULL);
	addQueryToCache(c);
	if (backup)
		c->curprg = backup;
	return 0;
}

/* stub and remote function */
static int
_create_relational_remote(mvc *m, char *name, sql_rel *rel, stmt *call, prop *prp)
{
	Client c = MCgetClient(m->clientid);
	MalBlkPtr curBlk = 0;
	InstrPtr curInstr = 0, p, o;
	Symbol backup = NULL;
	char *uri = prp->value;
	node *n;
	int i, q, v;
	int *lret, *rret;
	char old = name[0];
	sql_rel *r = rel;

	if (is_topn(r->op))
		r = r->l;
	if (!is_project(r->op))
		r = rel_project(m->sa, r, rel_projections(m, r, NULL, 1, 1));
	lret = SA_NEW_ARRAY(m->sa, int, list_length(r->exps));
	rret = SA_NEW_ARRAY(m->sa, int, list_length(r->exps));
	/* dirty hack, rename (change first char of name) L->l, local
	 * functions name start with 'l'         */
	name[0] = 'l';
	if (_create_relational_function(m, name, rel, call) < 0)
		return -1;

	/* create stub */
	name[0] = old;
	backup = c->curprg;
	c->curprg = newFunction(userRef, putName(name, strlen(name)), FUNCTIONsymbol);
	name[0] = 'l';
	curBlk = c->curprg->def;
	curInstr = getInstrPtr(curBlk, 0);

	curInstr = relational_func_create_result(m, curBlk, curInstr, rel);
	setVarUDFtype(curBlk, 0);

	/* ops */
	if (call->op1->type == st_list) {
		node *n;

		for (n = call->op1->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;
			sql_subtype *t = tail_type(op);
			int type = t->type->localtype;
			int varid = 0;
			char *nme = op->op3->op4.aval->data.val.sval;

			varid = newVariable(curBlk, _STRDUP(nme), type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
			setVarUDFtype(curBlk, varid);
		}
	}

	/* declare return variables */
	for (i = 0, n = r->exps->h; n; n = n->next, i++) {
		sql_exp *e = n->data;
		int type = exp_subtype(e)->type->localtype;

		type = newBatType(TYPE_oid, type);
		p = newFcnCall(curBlk, batRef, newRef);
		p = pushType(curBlk, p, getHeadType(type));
		p = pushType(curBlk, p, getTailType(type));
		setArgType(curBlk, p, 0, type);
		lret[i] = getArg(p, 0);
	}

	/* q := remote.connect("uri", "user", "pass"); */
	p = newStmt(curBlk, remoteRef, connectRef);
	p = pushStr(curBlk, p, uri);
	p = pushStr(curBlk, p, "monetdb");
	p = pushStr(curBlk, p, "monetdb");
	p = pushStr(curBlk, p, "msql");
	q = getArg(p, 0);

	/* remote.register(q, "mod", "fcn"); */
	p = newStmt(curBlk, remoteRef, putName("register", 8));
	p = pushArgument(curBlk, p, q);
	p = pushStr(curBlk, p, userRef);
	p = pushStr(curBlk, p, name);

	/* (x1, x2, ..., xn) := remote.exec(q, "mod", "fcn"); */
	p = newInstruction(curBlk, ASSIGNsymbol);
	setModuleId(p, remoteRef);
	setFunctionId(p, execRef);
	p = pushArgument(curBlk, p, q);
	p = pushStr(curBlk, p, userRef);
	p = pushStr(curBlk, p, name);

	for (i = 0, n = r->exps->h; n; n = n->next, i++) {
		/* x1 := remote.put(q, :type) */
		o = newFcnCall(curBlk, remoteRef, putRef);
		o = pushArgument(curBlk, o, q);
		o = pushArgument(curBlk, o, lret[i]);
		v = getArg(o, 0);
		p = pushReturn(curBlk, p, v);
		rret[i] = v;
	}

	/* send arguments to remote */
	for (i = curInstr->retc; i < curInstr->argc; i++) {
		/* x1 := remote.put(q, A0); */
		o = newStmt(curBlk, remoteRef, putRef);
		o = pushArgument(curBlk, o, q);
		o = pushArgument(curBlk, o, getArg(curInstr, i));
		p = pushArgument(curBlk, p, getArg(o, 0));
	}
	pushInstruction(curBlk, p);

	/* return results */
	for (i = 0; i < curInstr->retc; i++) {
		/* y1 := remote.get(q, x1); */
		p = newFcnCall(curBlk, remoteRef, getRef);
		p = pushArgument(curBlk, p, q);
		p = pushArgument(curBlk, p, rret[i]);
		getArg(p, 0) = lret[i];
	}

	/* remote.disconnect(q); */
	p = newStmt(curBlk, remoteRef, disconnectRef);
	p = pushArgument(curBlk, p, q);

	p = newInstruction(curBlk, RETURNsymbol);
	p->retc = p->argc = 0;
	for (i = 0; i < curInstr->retc; i++)
		p = pushArgument(curBlk, p, lret[i]);
	p->retc = p->argc;
	/* assignment of return */
	for (i = 0; i < curInstr->retc; i++)
		p = pushArgument(curBlk, p, lret[i]);
	pushInstruction(curBlk, p);
	pushEndInstruction(curBlk);

	/* SQL function definitions meant f r inlineing should not be optimized before */
	varSetProp(curBlk, getArg(curInstr, 0), sqlfunctionProp, op_eq, NULL);
	addQueryToCache(c);
	if (backup)
		c->curprg = backup;
	name[0] = old;		/* make sure stub is called */
	return 0;
}

static int
monet5_create_relational_function(mvc *m, char *name, sql_rel *rel, stmt *call)
{
	prop *p = NULL;

	if (rel && (p = find_prop(rel->p, PROP_REMOTE)) != NULL)
		return _create_relational_remote(m, name, rel, call, p);
	else
		return _create_relational_function(m, name, rel, call);
}

/*
 * @-
 * Some utility routines to generate code
 * The equality operator in MAL is '==' instead of '='.
 */
static str
convertMultiplexMod(str mod, str op)
{
	if (strcmp(op, "=") == 0)
		return "calc";
	return mod;
}

static str
convertMultiplexFcn(str op)
{
	if (strcmp(op, "=") == 0)
		return "==";
	return op;
}

static str
convertOperator(str op)
{
	if (strcmp(op, "=") == 0)
		return "==";
	return op;
}

static int
range_join_convertable(stmt *s, stmt **base, stmt **L, stmt **H)
{
	int ls = 0, hs = 0;
	stmt *l = NULL, *h = NULL;
	stmt *bl = s->op2, *bh = s->op3;
	int tt = tail_type(s->op2)->type->localtype;

	if (tt > TYPE_lng)
		return 0;
	if (s->op2->type == st_binop) {
		bl = s->op2->op1;
		l = s->op2->op2;
	} else if (s->op2->type == st_Nop && list_length(s->op2->op1->op4.lval) == 2) {
		bl = s->op2->op1->op4.lval->h->data;
		l = s->op2->op1->op4.lval->t->data;
	}
	if (s->op3->type == st_binop) {
		bh = s->op3->op1;
		h = s->op3->op2;
	} else if (s->op3->type == st_Nop && list_length(s->op3->op1->op4.lval) == 2) {
		bh = s->op3->op1->op4.lval->h->data;
		h = s->op3->op1->op4.lval->t->data;
	}

	if ((ls = (l && strcmp(s->op2->op4.funcval->func->base.name, "sql_sub") == 0 && l->nrcols == 0) || (hs = (h && strcmp(s->op3->op4.funcval->func->base.name, "sql_add") == 0 && h->nrcols == 0))) && (ls || hs) && bl == bh) {
		*base = bl;
		*L = l;
		*H = h;
		return 1;
	}
	return 0;
}

static int
_dump_1(MalBlkPtr mb, char *mod, char *name, int o1)
{
	InstrPtr q;

	q = newStmt2(mb, mod, name);
	q = pushArgument(mb, q, o1);
	return getDestVar(q);
}

static void
dump_1(backend *sql, MalBlkPtr mb, stmt *s, char *mod, char *name)
{
	int o1 = _dumpstmt(sql, mb, s->op1);

	s->nr = _dump_1(mb, mod, name, o1);
}

static int
_dump_2(MalBlkPtr mb, char *mod, char *name, int o1, int o2)
{
	InstrPtr q;

	q = newStmt2(mb, mod, name);
	q = pushArgument(mb, q, o1);
	q = pushArgument(mb, q, o2);
	return getDestVar(q);
}

static void
dump_2(backend *sql, MalBlkPtr mb, stmt *s, char *mod, char *name)
{
	int o1 = _dumpstmt(sql, mb, s->op1);
	int o2 = _dumpstmt(sql, mb, s->op2);

	s->nr = _dump_2(mb, mod, name, o1, o2);
}

static void
dump_2_(backend *sql, MalBlkPtr mb, stmt *s, char *mod, char *name)
{
	InstrPtr q;
	int o1 = _dumpstmt(sql, mb, s->op1);
	int o2 = _dumpstmt(sql, mb, s->op2);

	q = newStmt1(mb, mod, name);
	q = pushArgument(mb, q, o1);
	q = pushArgument(mb, q, o2);
	s->nr = getDestVar(q);
}

static InstrPtr
multiplex2(MalBlkPtr mb, char *mod, char *name /* should be eaten */ , int o1, int o2, int rtype)
{
	InstrPtr q;

	q = newStmt(mb, "mal", "multiplex");
	setVarType(mb, getArg(q, 0), newBatType(TYPE_oid, rtype));
	setVarUDFtype(mb, getArg(q, 0));
	q = pushStr(mb, q, convertMultiplexMod(mod, name));
	q = pushStr(mb, q, convertMultiplexFcn(name));
	q = pushArgument(mb, q, o1);
	q = pushArgument(mb, q, o2);
	return q;
}

static InstrPtr
multiplexN(MalBlkPtr mb, char *mod, char *name)
{
	InstrPtr q = NULL;

	if (strcmp(name, "rotate_xor_hash") == 0 && strcmp(mod, "calc") == 0)
		q = newStmt(mb, "mkey", "bulk_rotate_xor_hash");
	return q;
}

#define SMALLBUFSIZ 64
static int
dump_joinN(backend *sql, MalBlkPtr mb, stmt *s)
{
	char *mod, *fimp;
	InstrPtr q;
	int op1, op2, op3 = 0;
	bit swapped = (s->flag & SWAPPED) ? TRUE : FALSE;

	if (backend_create_func(sql, s->op4.funcval->func) < 0)
		return -1;
	mod = sql_func_mod(s->op4.funcval->func);
	fimp = sql_func_imp(s->op4.funcval->func);

	/* dump left and right operands */
	op1 = _dumpstmt(sql, mb, s->op1);
	op2 = _dumpstmt(sql, mb, s->op2);
	if (s->op3)
		op3 = _dumpstmt(sql, mb, s->op3);

	/* filter qualifying tuples, return oids of h and tail */
	q = newStmt(mb, mod, fimp);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, op1);
	q = pushArgument(mb, q, op2);
	if (s->op3)
		q = pushArgument(mb, q, op3);
	s->nr = getDestVar(q);

	if (swapped) {
		InstrPtr r = newInstruction(mb, ASSIGNsymbol);
		getArg(r, 0) = newTmpVariable(mb, TYPE_any);
		getArg(r, 1) = getArg(q, 1);
		r->retc = 1;
		r->argc = 2;
		pushInstruction(mb, r);
		s->nr = getArg(r, 0);

		r = newInstruction(mb, ASSIGNsymbol);
		getArg(r, 0) = newTmpVariable(mb, TYPE_any);
		getArg(r, 1) = getArg(q, 0);
		r->retc = 1;
		r->argc = 2;
		pushInstruction(mb, r);

		/* rename second result */
		renameVariable(mb, getArg(r, 0), "r1_%d", s->nr);
	} else {
		/* rename second result */
		renameVariable(mb, getArg(q, 1), "r1_%d", s->nr);
	}
	return s->nr;
}

static InstrPtr
pushSchema(MalBlkPtr mb, InstrPtr q, sql_table *t)
{
	if (t->s)
		return pushStr(mb, q, t->s->base.name);
	else
		return pushNil(mb, q, TYPE_str);
}

/*
 * @-
 * The big code generation switch.
 */
static int
_dumpstmt(backend *sql, MalBlkPtr mb, stmt *s)
{
	InstrPtr q = NULL;
	node *n;

	if (s) {
		if (s->nr > 0)
			return s->nr;	/* stmt already handled */

		switch (s->type) {
		case st_none:{
			q = newAssignment(mb);
			s->nr = getDestVar(q);
			(void) pushInt(mb, q, 1);
		} break;
		case st_var:{
			if (s->op1) {
				if (VAR_GLOBAL(s->flag)) {	/* globals */
					int tt = tail_type(s)->type->localtype;

					q = newStmt1(mb, sqlRef, "getVariable");
					q = pushArgument(mb, q, sql->mvc_var);
					q = pushStr(mb, q, s->op1->op4.aval->data.val.sval);
					setVarType(mb, getArg(q, 0), tt);
					setVarUDFtype(mb, getArg(q, 0));
				} else if ((s->flag & VAR_DECLARE) == 0) {
					char *buf = GDKmalloc(MAXIDENTLEN);

					(void) snprintf(buf, MAXIDENTLEN, "A%s", s->op1->op4.aval->data.val.sval);
					q = newAssignment(mb);
					q = pushArgumentId(mb, q, buf);
				} else {
					int tt = tail_type(s)->type->localtype;
					char *buf = GDKmalloc(MAXIDENTLEN);

					if (tt == TYPE_bat) {
						/* declared table */
						s->nr = dump_table(mb, tail_type(s)->comp_type);
						break;
					}
					(void) snprintf(buf, MAXIDENTLEN, "A%s", s->op1->op4.aval->data.val.sval);
					q = newInstruction(mb, ASSIGNsymbol);
					q->argc = q->retc = 0;
					q = pushArgumentId(mb, q, buf);
					q = pushNil(mb, q, tt);
					pushInstruction(mb, q);
					q->retc++;
				}
			} else {
				char *buf = GDKmalloc(SMALLBUFSIZ);
				
				q = newAssignment(mb);
				if (sql->mvc->argc && sql->mvc->args[s->flag]->varid >= 0) {
					q = pushArgument(mb, q, sql->mvc->args[s->flag]->varid);
				} else {
					(void) snprintf(buf, SMALLBUFSIZ, "A%d", s->flag);
					q = pushArgumentId(mb, q, buf);
				}
			}
			s->nr = getDestVar(q);
		} break;
		case st_single:{
			int ht = TYPE_oid;
			int tt = s->op4.typeval.type->localtype;
			int val = _dumpstmt(sql, mb, s->op1);

			q = newStmt1(mb, sqlRef, "single");
			setVarType(mb, getArg(q, 0), newBatType(ht, tt));
			q = pushArgument(mb, q, val);
			s->nr = getDestVar(q);
		} break;
		case st_temp:{
			int ht = TYPE_oid;
			int tt = s->op4.typeval.type->localtype;

			q = newStmt1(mb, batRef, "new");
			setVarType(mb, getArg(q, 0), newBatType(ht, tt));
			setVarUDFtype(mb, getArg(q, 0));
			q = pushType(mb, q, ht);
			q = pushType(mb, q, tt);

			s->nr = getDestVar(q);
		} break;
		case st_tid:{
			int ht = TYPE_oid;
			int tt = TYPE_oid;
			sql_table *t = s->op4.tval;

			q = newStmt1(mb, sqlRef, "tid");
			setVarType(mb, getArg(q, 0), newBatType(ht, tt));
			setVarUDFtype(mb, getArg(q, 0));
			q = pushArgument(mb, q, sql->mvc_var);
			q = pushSchema(mb, q, t);
			q = pushStr(mb, q, t->base.name);
			s->nr = getDestVar(q);
		}
			break;
		case st_bat:{
			int ht = TYPE_oid;
			int tt = s->op4.cval->type.type->localtype;
			sql_table *t = s->op4.cval->t;

			q = newStmt2(mb, sqlRef, bindRef);
			if (s->flag == RD_UPD) {
				q = pushReturn(mb, q, newTmpVariable(mb, newBatType(ht, tt)));
			} else
				setVarType(mb, getArg(q, 0), newBatType(ht, tt));
			q = pushArgument(mb, q, sql->mvc_var);
			q = pushSchema(mb, q, t);
			q = pushStr(mb, q, t->base.name);
			q = pushStr(mb, q, s->op4.cval->base.name);
			q = pushInt(mb, q, s->flag);
			s->nr = getDestVar(q);

			if (s->flag == RD_UPD) {
				/* rename second result */
				renameVariable(mb, getArg(q, 1), "r1_%d", s->nr);
			}
		}
			break;
		case st_idxbat:{
			int ht = TYPE_oid;
			int tt = tail_type(s)->type->localtype;
			sql_table *t = s->op4.idxval->t;

			q = newStmt2(mb, sqlRef, bindidxRef);
			if (s->flag == RD_UPD) {
				q = pushReturn(mb, q, newTmpVariable(mb, newBatType(ht, tt)));
			} else
				setVarType(mb, getArg(q, 0), newBatType(ht, tt));
			q = pushArgument(mb, q, sql->mvc_var);
			q = pushSchema(mb, q, t);
			q = pushStr(mb, q, t->base.name);
			q = pushStr(mb, q, s->op4.idxval->base.name);
			q = pushInt(mb, q, s->flag);
			s->nr = getDestVar(q);

			if (s->flag == RD_UPD) {
				/* rename second result */
				renameVariable(mb, getArg(q, 1), "r1_%d", s->nr);
			}
		}
			break;
		case st_const:{
			if (s->op2)
				dump_2(sql, mb, s, algebraRef, projectRef);
			else
				dump_1(sql, mb, s, algebraRef, projectRef);
		}
			break;
		case st_mark:{
			dump_2(sql, mb, s, algebraRef, markTRef);
		}
			break;
		case st_gen_group:{
			dump_2(sql, mb, s, algebraRef, groupbyRef);
		}
			break;
		case st_reverse:{
			dump_1(sql, mb, s, batRef, reverseRef);
		}
			break;
		case st_mirror:{
			dump_1(sql, mb, s, batRef, mirrorRef);
		}
			break;
		case st_limit2:
		case st_limit:{
			int l = _dumpstmt(sql, mb, s->op1);
			stmt *l1 = (s->type == st_limit2) ? s->op1->op4.lval->h->data : s->op1;
			stmt *l2 = (s->type == st_limit2) ? s->op1->op4.lval->t->data : NULL;
			int offset = _dumpstmt(sql, mb, s->op2);
			int len = _dumpstmt(sql, mb, s->op3);
			int la = (l2) ? l2->nr : 0;

			l = l1->nr;
			/* first insert single value into a bat */
			assert(s->nrcols);
			if (s->nrcols == 0) {
				int k;
				int ht = TYPE_oid;
				int tt = tail_type(s->op1)->type->localtype;

				q = newStmt1(mb, batRef, "new");
				setVarType(mb, getArg(q, 0), newBatType(ht, tt));
				setVarUDFtype(mb, getArg(q, 0));
				q = pushType(mb, q, ht);
				q = pushType(mb, q, tt);
				k = getDestVar(q);

				q = newStmt2(mb, batRef, appendRef);
				q = pushArgument(mb, q, k);
				(void) pushArgument(mb, q, l);
				l = k;
			}
			if (s->flag) {
				int topn = 0, flag = s->flag, utopn = flag & 2;
				char *name = "utopn_min";

				flag >>= 2;
				if (flag)
					name = "utopn_max";

				if (!utopn)
					name = name + 1;
				q = newStmt1(mb, calcRef, "+");
				q = pushArgument(mb, q, offset);
				q = pushArgument(mb, q, len);
				topn = getDestVar(q);

				q = newStmt(mb, "pqueue", name);
				if (la)
					q = pushArgument(mb, q, la);
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, topn);
				l = getDestVar(q);
			} else {
				q = newStmt1(mb, calcRef, "+");
				q = pushArgument(mb, q, offset);
				q = pushArgument(mb, q, len);
				len = getDestVar(q);

				/* since both arguments of algebra.subslice are
				   inclusive correct the LIMIT value by
				   subtracting 1 */
				q = newStmt1(mb, calcRef, "-");
				q = pushArgument(mb, q, len);
				q = pushInt(mb, q, 1);
				len = getDestVar(q);

				q = newStmt1(mb, algebraRef, "subslice");
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, offset);
				q = pushArgument(mb, q, len);
				l = getDestVar(q);
			}
			/* retrieve the single values again */
			if (s->nrcols == 0) {
				q = newStmt1(mb, algebraRef, "find");
				q = pushArgument(mb, q, l);
				q = pushOid(mb, q, 0);
				l = getDestVar(q);
			}
			s->nr = l;
		}
			break;
		case st_sample:{
			int l = _dumpstmt(sql, mb, s->op1);
			int r = _dumpstmt(sql, mb, s->op2);
			q = newStmt(mb, "sample", "subuniform");
			q = pushArgument(mb, q, l);
			q = pushArgument(mb, q, r);
			s->nr = getDestVar(q);
		} break;
		case st_order:{
			int l = _dumpstmt(sql, mb, s->op1);
			int reverse = (s->flag > 0) ? 0 : 1;

			q = newStmt1(mb, algebraRef, "subsort");
			/* both ordered result and oid's order en subgroups */
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, l);
			q = pushBit(mb, q, reverse);
			q = pushBit(mb, q, FALSE);
			s->nr = getDestVar(q);

			renameVariable(mb, getArg(q, 1), "r1_%d", s->nr);
			renameVariable(mb, getArg(q, 2), "r2_%d", s->nr);
		} break;
		case st_reorder:{
			int l = _dumpstmt(sql, mb, s->op1);
			int oids = _dumpstmt(sql, mb, s->op2);
			int ogrp = _dumpstmt(sql, mb, s->op3);
			int reverse = (s->flag > 0) ? 0 : 1;

			q = newStmt1(mb, algebraRef, "subsort");
			/* both ordered result and oid's order en subgroups */
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, l);
			q = pushArgument(mb, q, oids);
			q = pushArgument(mb, q, ogrp);
			q = pushBit(mb, q, reverse);
			q = pushBit(mb, q, FALSE);
			s->nr = getDestVar(q);

			renameVariable(mb, getArg(q, 1), "r1_%d", s->nr);
			renameVariable(mb, getArg(q, 2), "r2_%d", s->nr);
		} break;
		case st_uselect:{
			bit need_not = FALSE;
			int l = _dumpstmt(sql, mb, s->op1);
			int r = s->op2 ? _dumpstmt(sql, mb, s->op2) : -1;
			int sub = -1;
			int anti = is_anti(s);

			if (s->op3)
				sub = _dumpstmt(sql, mb, s->op3);

			if (s->op2->nrcols >= 1) {
				char *mod = calcRef;
				char *op = "=";
				int k;
				int op3 = -1;

				switch (get_cmp(s)) {
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
				case cmp_filter:{
					sql_subfunc *f;
					char *fname = s->op4.funcval->func->base.name;
					stmt *p2 = ((stmt *) s->op2->op4.lval->h->data), *p3 = NULL;

					op = sql_func_imp(s->op4.funcval->func);
					mod = sql_func_mod(s->op4.funcval->func);

					assert(anti == 0);
					r = p2->nr;
					if (s->op2->op4.lval->h->next) {
						p3 = s->op2->op4.lval->h->next->data;

						op3 = p3->nr;
					}
					if ((!p3 && (f = sql_bind_func(sql->mvc->sa, mvc_bind_schema(sql->mvc, "sys"), fname, tail_type(s->op1), tail_type(p2), F_FUNC)) != NULL) ||
					    (p3 && (f = sql_bind_func3(sql->mvc->sa, mvc_bind_schema(sql->mvc, "sys"), fname, tail_type(s->op1), tail_type(p2), tail_type(p3), F_FUNC)) != NULL)) {
						op = sql_func_imp(f->func);
						mod = sql_func_mod(f->func);
					}
				}
					break;
				default:
					showException(GDKout, SQL, "sql", "Unknown operator");
				}

				q = multiplex2(mb, mod, convertOperator(op), l, r, TYPE_bit);
				if (op3 > 0)
					q = pushArgument(mb, q, op3);
				k = getDestVar(q);

				q = newStmt1(mb, algebraRef, "subselect");
				q = pushArgument(mb, q, k);
				if (sub > 0)
					q = pushArgument(mb, q, sub);
				q = pushBit(mb, q, !need_not);
				q = pushBit(mb, q, !need_not);
				q = pushBit(mb, q, TRUE);
				q = pushBit(mb, q, TRUE);
				q = pushBit(mb, q, FALSE);
				k = getDestVar(q);
			} else {
				char *cmd = "subselect";

				if (s->flag != cmp_equal && s->flag != cmp_notequal)
					cmd = "thetasubselect";

				if (get_cmp(s) == cmp_filter) {
					node *n;
					char *mod, *fimp;

					if (backend_create_func(sql, s->op4.funcval->func) < 0)
						return -1;
					mod = sql_func_mod(s->op4.funcval->func);
					fimp = sql_func_imp(s->op4.funcval->func);

					q = newStmt(mb, mod, convertOperator(fimp));
					q = pushArgument(mb, q, l);
					if (sub > 0)
						q = pushArgument(mb, q, sub);

					for (n = s->op2->op4.lval->h; n; n = n->next) {
						stmt *op = n->data;

						q = pushArgument(mb, q, op->nr);
					}
					q = pushBit(mb, q, anti);
					s->nr = getDestVar(q);
					break;
				}

				switch (s->flag) {
				case cmp_equal:{
					q = newStmt1(mb, algebraRef, cmd);
					q = pushArgument(mb, q, l);
					if (sub > 0)
						q = pushArgument(mb, q, sub);
					q = pushArgument(mb, q, r);
					q = pushArgument(mb, q, r);
					q = pushBit(mb, q, TRUE);
					q = pushBit(mb, q, TRUE);
					q = pushBit(mb, q, FALSE);
					break;
				}
				case cmp_notequal:{
					q = newStmt1(mb, algebraRef, cmd);
					q = pushArgument(mb, q, l);
					if (sub > 0)
						q = pushArgument(mb, q, sub);
					q = pushArgument(mb, q, r);
					q = pushArgument(mb, q, r);
					q = pushBit(mb, q, TRUE);
					q = pushBit(mb, q, TRUE);
					q = pushBit(mb, q, TRUE);
					break;
				}
				case cmp_lt:
					q = newStmt1(mb, algebraRef, cmd);
					q = pushArgument(mb, q, l);
					if (sub > 0)
						q = pushArgument(mb, q, sub);
					q = pushArgument(mb, q, r);
					q = pushStr(mb, q, "<");
					break;
				case cmp_lte:
					q = newStmt1(mb, algebraRef, cmd);
					q = pushArgument(mb, q, l);
					if (sub > 0)
						q = pushArgument(mb, q, sub);
					q = pushArgument(mb, q, r);
					q = pushStr(mb, q, "<=");
					break;
				case cmp_gt:
					q = newStmt1(mb, algebraRef, cmd);
					q = pushArgument(mb, q, l);
					if (sub > 0)
						q = pushArgument(mb, q, sub);
					q = pushArgument(mb, q, r);
					q = pushStr(mb, q, ">");
					break;
				case cmp_gte:
					q = newStmt1(mb, algebraRef, cmd);
					q = pushArgument(mb, q, l);
					if (sub > 0)
						q = pushArgument(mb, q, sub);
					q = pushArgument(mb, q, r);
					q = pushStr(mb, q, ">=");
					break;
				default:
					showException(GDKout, SQL, "sql", "SQL2MAL: error impossible subselect compare\n");
				}
			}
			if (q)
				s->nr = getDestVar(q);
			else
				s->nr = newTmpVariable(mb, TYPE_any);
		}
			break;
		case st_uselect2:
		case st_join2:{
			InstrPtr r, p;
			int l = _dumpstmt(sql, mb, s->op1);
			stmt *base, *low = NULL, *high = NULL;
			int r1 = -1, r2 = -1, rs = 0;
			bit anti = (s->flag & ANTI) ? TRUE : FALSE;
			bit swapped = (s->flag & SWAPPED) ? TRUE : FALSE;
			char *cmd = (s->type == st_uselect2) ? "subselect" : "join";
			int sub = -1;

			if (s->op4.stval)
				sub = _dumpstmt(sql, mb, s->op4.stval);

			if ((s->op2->nrcols > 0 || s->op3->nrcols) && (s->type == st_uselect2)) {
				int k;
				char *mod = calcRef;
				char *op1 = "<", *op2 = "<";

				r1 = _dumpstmt(sql, mb, s->op2);
				r2 = _dumpstmt(sql, mb, s->op3);

				if (s->flag & 1)
					op1 = "<=";
				if (s->flag & 2)
					op2 = "<=";

				q = multiplex2(mb, mod, convertOperator(op1), l, r1, TYPE_bit);

				r = multiplex2(mb, mod, convertOperator(op2), l, r2, TYPE_bit);
				p = newStmt1(mb, batcalcRef, "and");
				p = pushArgument(mb, p, getDestVar(q));
				p = pushArgument(mb, p, getDestVar(r));
				k = getDestVar(p);

				q = newStmt1(mb, algebraRef, "subselect");
				q = pushArgument(mb, q, k);
				q = pushBit(mb, q, TRUE);
				q = pushBit(mb, q, TRUE);
				q = pushBit(mb, q, TRUE);
				q = pushBit(mb, q, TRUE);
				q = pushBit(mb, q, FALSE);
				s->nr = getDestVar(q);
				break;
			}
			/* if st_join2 try to convert to bandjoin */
			/* ie check if we subtract/add a constant, to the
			   same column */
			if (s->type == st_join2 && range_join_convertable(s, &base, &low, &high)) {
				int tt = tail_type(base)->type->localtype;
				rs = _dumpstmt(sql, mb, base);
				if (low)
					r1 = _dumpstmt(sql, mb, low);
				else
					r1 = argumentZero(mb, tt);
				if (high)
					r2 = _dumpstmt(sql, mb, high);
				else
					r2 = argumentZero(mb, tt);
				cmd = bandjoinRef;
			}

			if (!rs) {
				r1 = _dumpstmt(sql, mb, s->op2);
				r2 = _dumpstmt(sql, mb, s->op3);
			}
			q = newStmt1(mb, algebraRef, cmd);
			if (s->type == st_join2)
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, l);
			if (sub > 0)
				q = pushArgument(mb, q, sub);
			if (rs)
				q = pushArgument(mb, q, rs);
			q = pushArgument(mb, q, r1);
			q = pushArgument(mb, q, r2);

			switch (s->flag & 3) {
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
			if (s->type == st_uselect2) {
				q = pushBit(mb, q, anti);
				s->nr = getDestVar(q);
				break;
			}
			s->nr = getDestVar(q);

			if (swapped) {
				InstrPtr r = newInstruction(mb, ASSIGNsymbol);
				getArg(r, 0) = newTmpVariable(mb, TYPE_any);
				getArg(r, 1) = getArg(q, 1);
				r->retc = 1;
				r->argc = 2;
				pushInstruction(mb, r);
				s->nr = getArg(r, 0);

				r = newInstruction(mb, ASSIGNsymbol);
				getArg(r, 0) = newTmpVariable(mb, TYPE_any);
				getArg(r, 1) = getArg(q, 0);
				r->retc = 1;
				r->argc = 2;
				pushInstruction(mb, r);

				/* rename second result */
				renameVariable(mb, getArg(r, 0), "r1_%d", s->nr);
			} else {
				/* rename second result */
				renameVariable(mb, getArg(q, 1), "r1_%d", s->nr);
			}
			break;
		}
		case st_joinN:
			s->nr = dump_joinN(sql, mb, s);
			break;
		case st_tunion:{
			dump_2_(sql, mb, s, batRef, "mergecand");
		}
			break;
		case st_tdiff:{
			dump_2_(sql, mb, s, algebraRef, "tdiff");
		}
			break;
		case st_tinter:{
			dump_2_(sql, mb, s, algebraRef, "tinter");
		}
			break;
		case st_diff:{
			dump_2(sql, mb, s, algebraRef, kdifferenceRef);
		}
			break;
		case st_union:{
			dump_2(sql, mb, s, algebraRef, kunionRef);
		}
			break;
		case st_join:{
			int l = _dumpstmt(sql, mb, s->op1);
			int r = _dumpstmt(sql, mb, s->op2);
			char *jt = "join";
			char *sjt = "subjoin";

			assert(l >= 0 && r >= 0);

			if (s->flag == cmp_joined) {
				s->nr = l;
				return s->nr;
			}
			if (s->flag == cmp_project || s->flag == cmp_reorder_project) {
				int ins;

				/* delta bat */
				if (s->op3) {
					char nme[SMALLBUFSIZ];
					int uval = -1;

					snprintf(nme, SMALLBUFSIZ, "r1_%d", r);
					uval = findVariable(mb, nme);
					assert(uval >= 0);

					ins = _dumpstmt(sql, mb, s->op3);
					q = newStmt2(mb, sqlRef, deltaRef);
					q = pushArgument(mb, q, l);
					q = pushArgument(mb, q, r);
					q = pushArgument(mb, q, uval);
					q = pushArgument(mb, q, ins);
					s->nr = getDestVar(q);
					return s->nr;
				}
				/* projections, ie left is void headed */
				if (s->flag == cmp_project)
					q = newStmt1(mb, algebraRef, "leftfetchjoin");
				else
					q = newStmt2(mb, algebraRef, leftjoinRef);
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				s->nr = getDestVar(q);
				return s->nr;
			}


			switch (s->flag) {
			case cmp_equal:
				q = newStmt1(mb, algebraRef, jt);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				break;
			case cmp_equal_nil:
				q = newStmt1(mb, algebraRef, sjt);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				q = pushNil(mb, q, TYPE_bat);
				q = pushNil(mb, q, TYPE_bat);
				q = pushBit(mb, q, TRUE);
				q = pushNil(mb, q, TYPE_lng);
				break;
			case cmp_notequal:
				q = newStmt1(mb, algebraRef, antijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				break;
			case cmp_lt:
				q = newStmt1(mb, algebraRef, thetajoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				q = pushInt(mb, q, -1);
				break;
			case cmp_lte:
				q = newStmt1(mb, algebraRef, thetajoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				q = pushInt(mb, q, -2);
				break;
			case cmp_gt:
				q = newStmt1(mb, algebraRef, thetajoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				q = pushInt(mb, q, 1);
				break;
			case cmp_gte:
				q = newStmt1(mb, algebraRef, thetajoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				q = pushInt(mb, q, 2);
				break;
			case cmp_all:	/* aka cross table */
				q = newStmt2(mb, algebraRef, crossRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				break;
			case cmp_project:
			case cmp_reorder_project:
				assert(0);
				break;
			default:
				showException(GDKout, SQL, "sql", "SQL2MAL: error impossible\n");
			}
			s->nr = getDestVar(q);

			/* rename second result */
			renameVariable(mb, getArg(q, 1), "r1_%d", s->nr);
			break;
		}
		case st_group:{
			int cnt = 0, ext = 0, grp = 0, o1 = _dumpstmt(sql, mb, s->op1);

			if (s->op2) {
				grp = _dumpstmt(sql, mb, s->op2);
				ext = _dumpstmt(sql, mb, s->op3);
				cnt = _dumpstmt(sql, mb, s->op4.stval);
			}

			q = newStmt2(mb, groupRef, s->flag & GRP_DONE ? subgroupdoneRef : subgroupRef);

			/* output variables extend and hist */
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, o1);
			if (grp)
				q = pushArgument(mb, q, grp);

			s->nr = getDestVar(q);

			/* rename second result */
			ext = getArg(q, 1);
			renameVariable(mb, ext, "r1_%d", s->nr);

			/* rename 3rd result */
			cnt = getArg(q, 2);
			renameVariable(mb, cnt, "r2_%d", s->nr);

		} break;
		case st_result:{
			int l = _dumpstmt(sql, mb, s->op1);

			if (s->op1->type == st_join && s->op1->flag == cmp_joined) {
				s->nr = l;
				if (s->flag)
					s->nr = s->op1->op2->nr;
			} else if (s->flag) {
				char nme[SMALLBUFSIZ];
				int v = -1;

				snprintf(nme, SMALLBUFSIZ, "r%d_%d", s->flag, l);
				v = findVariable(mb, nme);
				assert(v >= 0);

				s->nr = v;
			} else {
				s->nr = l;
			}
		}
			break;
		case st_unique:{
			int l = _dumpstmt(sql, mb, s->op1);

			if (s->op2) {
				int grp = _dumpstmt(sql, mb, s->op2);
				int ext = _dumpstmt(sql, mb, s->op3);

				q = newStmt2(mb, groupRef, subgroupRef);
				/* push second result */
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, grp);
				grp = getDestVar(q);
				ext = getArg(q, 1);

				q = newStmt2(mb, algebraRef, leftfetchjoinRef);
				q = pushArgument(mb, q, ext);
				q = pushArgument(mb, q, l);
			} else {
				q = newStmt2(mb, algebraRef, tuniqueRef);
				q = pushArgument(mb, q, l);
			}
			s->nr = getDestVar(q);
			break;
		}
		case st_convert:{
			list *types = s->op4.lval;
			sql_subtype *f = types->h->data;
			sql_subtype *t = types->t->data;
			char *convert = t->type->base.name;
			/* convert types and make sure they are rounded up correctly */
			int l = _dumpstmt(sql, mb, s->op1);

			if (t->type->localtype == f->type->localtype && (t->type->eclass == f->type->eclass || (EC_VARCHAR(f->type->eclass) && EC_VARCHAR(t->type->eclass))) && f->type->eclass != EC_INTERVAL && f->type->eclass != EC_DEC &&
			    (t->digits == 0 || f->digits == t->digits)
				) {
				s->nr = l;
				break;
			}

			/* external types have sqlname convert functions,
			   these can generate errors (fromstr cannot) */
			if (t->type->eclass == EC_EXTERNAL)
				convert = t->type->sqlname;

			if (t->type->eclass == EC_INTERVAL) {
				if (t->type->localtype == TYPE_int)
					convert = "month_interval";
				else
					convert = "second_interval";
			}

			/* Lookup the sql convert function, there is no need
			 * for single value vs bat, this is handled by the
			 * mal function resolution */
			if (s->nrcols == 0) {	/* simple calc */
				q = newStmt1(mb, calcRef, convert);
			} else if (s->nrcols > 0 &&
				   (t->type->localtype > TYPE_str || f->type->eclass == EC_DEC || t->type->eclass == EC_DEC || t->type->eclass == EC_INTERVAL || EC_TEMP(t->type->eclass) ||
				    (EC_VARCHAR(t->type->eclass) && !(f->type->eclass == EC_STRING && t->digits == 0)))) {
				int type = t->type->localtype;

				q = newStmt(mb, "mal", "multiplex");
				setVarType(mb, getArg(q, 0), newBatType(TYPE_oid, type));
				setVarUDFtype(mb, getArg(q, 0));
				q = pushStr(mb, q, convertMultiplexMod("calc", convert));
				q = pushStr(mb, q, convertMultiplexFcn(convert));
			} else
				q = newStmt1(mb, batcalcRef, convert);

			/* convert to string is complex, we need full type info
			   and mvc for the timezone */
			if (EC_VARCHAR(t->type->eclass) && !(f->type->eclass == EC_STRING && t->digits == 0)) {
				q = pushInt(mb, q, f->type->eclass);
				q = pushInt(mb, q, f->digits);
				q = pushInt(mb, q, f->scale);
				q = pushInt(mb, q, type_has_tz(f));
			} else if (f->type->eclass == EC_DEC)
				/* scale of the current decimal */
				q = pushInt(mb, q, f->scale);
			q = pushArgument(mb, q, l);

			if (t->type->eclass == EC_DEC || EC_TEMP_FRAC(t->type->eclass) || t->type->eclass == EC_INTERVAL) {
				/* digits, scale of the result decimal */
				q = pushInt(mb, q, t->digits);
				if (!EC_TEMP_FRAC(t->type->eclass))
					q = pushInt(mb, q, t->scale);
			}
			/* convert to string, give error on to large strings */
			if (EC_VARCHAR(t->type->eclass) && !(f->type->eclass == EC_STRING && t->digits == 0))
				q = pushInt(mb, q, t->digits);
			s->nr = getDestVar(q);
			break;
		}
		case st_unop:{
			char *mod, *fimp;
			int l = _dumpstmt(sql, mb, s->op1);

			if (backend_create_func(sql, s->op4.funcval->func) < 0)
				return -1;
			mod = sql_func_mod(s->op4.funcval->func);
			fimp = sql_func_imp(s->op4.funcval->func);
			if (s->op1->nrcols && strcmp(fimp, "not_uniques") == 0) {
				int rtype = s->op4.funcval->res.type->localtype;

				q = newStmt(mb, mod, fimp);
				setVarType(mb, getArg(q, 0), newBatType(TYPE_oid, rtype));
				setVarUDFtype(mb, getArg(q, 0));
				q = pushArgument(mb, q, l);
			} else if (s->op1->nrcols) {
				int rtype = s->op4.funcval->res.type->localtype;

				q = newStmt(mb, "mal", "multiplex");
				setVarType(mb, getArg(q, 0), newBatType(TYPE_oid, rtype));
				setVarUDFtype(mb, getArg(q, 0));
				q = pushStr(mb, q, convertMultiplexMod(mod, fimp));
				q = pushStr(mb, q, convertMultiplexFcn(fimp));
				q = pushArgument(mb, q, l);
			} else {
				q = newStmt(mb, mod, fimp);
				q = pushArgument(mb, q, l);
			}
			s->nr = getDestVar(q);
		}
			break;
		case st_binop:{
			/* TODO use the rewriter to fix the 'round' function */
			sql_subtype *tpe = tail_type(s->op1);
			int special = 0;
			char *mod, *fimp;
			int l = _dumpstmt(sql, mb, s->op1);
			int r = _dumpstmt(sql, mb, s->op2);

			if (backend_create_func(sql, s->op4.funcval->func) < 0)
				return -1;
			mod = sql_func_mod(s->op4.funcval->func);
			fimp = sql_func_imp(s->op4.funcval->func);

			if (strcmp(fimp, "round") == 0 && tpe->type->eclass == EC_DEC)
				special = 1;

			if (s->op1->nrcols || s->op2->nrcols) {
				if (!special) {
					q = multiplex2(mb, mod, convertOperator(fimp), l, r, s->op4.funcval->res.type->localtype);
				} else {
					mod = convertMultiplexMod(mod, fimp);
					fimp = convertMultiplexFcn(fimp);
					q = newStmt(mb, "mal", "multiplex");
					setVarType(mb, getArg(q, 0), newBatType(TYPE_oid, s->op4.funcval->res.type->localtype));
					setVarUDFtype(mb, getArg(q, 0));
					q = pushStr(mb, q, mod);
					q = pushStr(mb, q, fimp);
					q = pushArgument(mb, q, l);
					q = pushInt(mb, q, tpe->digits);
					q = pushInt(mb, q, tpe->scale);
					q = pushArgument(mb, q, r);
				}
				s->nr = getDestVar(q);
			} else {
				q = newStmt(mb, mod, convertOperator(fimp));
				q = pushArgument(mb, q, l);
				if (special) {
					q = pushInt(mb, q, tpe->digits);
					q = pushInt(mb, q, tpe->scale);
				}
				q = pushArgument(mb, q, r);
			}
			s->nr = getDestVar(q);
		}
			break;
		case st_Nop:{
			char *mod, *fimp;
			sql_subtype *tpe = NULL;
			int special = 0;
			sql_subfunc *f = s->op4.funcval;
			node *n;
			/* dump operands */
			_dumpstmt(sql, mb, s->op1);

			if (backend_create_func(sql, f->func) < 0)
				return -1;
			mod = sql_func_mod(f->func);
			fimp = sql_func_imp(f->func);
			if (s->nrcols) {
				fimp = convertMultiplexFcn(fimp);
				q = multiplexN(mb, mod, fimp);
				if (!q) {
					q = newStmt(mb, "mal", "multiplex");
					setVarType(mb, getArg(q, 0), newBatType(TYPE_oid, f->res.type->localtype));
					setVarUDFtype(mb, getArg(q, 0));
					q = pushStr(mb, q, mod);
					q = pushStr(mb, q, fimp);
				} else {
					setVarType(mb, getArg(q, 0), newBatType(TYPE_any, f->res.type->localtype));
					setVarUDFtype(mb, getArg(q, 0));
				}
			} else {
				fimp = convertOperator(fimp);
				q = newStmt(mb, mod, fimp);
			}
			/* first dynamic output of copy* functions */
			if (f->res.comp_type)
				q = table_func_create_result(mb, q, f->res.comp_type);
			else if (f->func->res.comp_type)
				q = table_func_create_result(mb, q, f->func->res.comp_type);
			if (list_length(s->op1->op4.lval))
				tpe = tail_type(s->op1->op4.lval->h->data);
			if (strcmp(fimp, "round") == 0 && tpe && tpe->type->eclass == EC_DEC)
				special = 1;

			for (n = s->op1->op4.lval->h; n; n = n->next) {
				stmt *op = n->data;

				q = pushArgument(mb, q, op->nr);
				if (special) {
					q = pushInt(mb, q, tpe->digits);
					q = pushInt(mb, q, tpe->scale);
				}
				special = 0;
			}
			s->nr = getDestVar(q);
			/* keep reference to instruction */
			s->rewritten = (void *) q;
		} break;
		case st_func:{
			char *mod = "user";
			char *fimp = s->op2->op4.aval->data.val.sval;
			sql_rel *rel = s->op4.rel;
			node *n;

			/* dump args */
			if (s->op1)
				_dumpstmt(sql, mb, s->op1);
			if (monet5_create_relational_function(sql->mvc, fimp, rel, s) < 0)
				 return -1;

			q = newStmt(mb, mod, fimp);
			q = relational_func_create_result(sql->mvc, mb, q, rel);
			if (s->op1)
				for (n = s->op1->op4.lval->h; n; n = n->next) {
					stmt *op = n->data;

					q = pushArgument(mb, q, op->nr);
				}
			s->nr = getDestVar(q);
			/* keep reference to instruction */
			s->rewritten = (void *) q;
		} break;
		case st_aggr:{
			int no_nil = s->flag;
			int g = 0, e = 0, l = _dumpstmt(sql, mb, s->op1);	/* maybe a list */
			char *mod, *aggrfunc;
			char aggrF[64];
			int restype = s->op4.aggrval->res.type->localtype;
			int complex_aggr = 0;
			int abort_on_error, i, *stmt_nr = NULL;

			if (backend_create_func(sql, s->op4.aggrval->aggr) < 0)
				return -1;
			mod = s->op4.aggrval->aggr->mod;
			aggrfunc = s->op4.aggrval->aggr->imp;

			if (strcmp(aggrfunc, "avg") == 0 || strcmp(aggrfunc, "sum") == 0 || strcmp(aggrfunc, "prod") == 0)
				complex_aggr = 1;
			/* some "sub" aggregates have an extra
			 * argument "abort_on_error" */
			abort_on_error = complex_aggr || strncmp(aggrfunc, "stdev", 5) == 0 || strncmp(aggrfunc, "variance", 8) == 0;

			if (s->op3) {
				snprintf(aggrF, 64, "sub%s", aggrfunc);
				aggrfunc = aggrF;
				g = _dumpstmt(sql, mb, s->op2);
				e = _dumpstmt(sql, mb, s->op3);

				q = newStmt(mb, mod, aggrfunc);
				setVarType(mb, getArg(q, 0), newBatType(TYPE_any, restype));
				setVarUDFtype(mb, getArg(q, 0));
			} else {
				if (no_nil) {
					if (s->op1->type != st_list) {
						q = newStmt2(mb, algebraRef, selectNotNilRef);
						q = pushArgument(mb, q, l);
						l = getDestVar(q);
					} else {
						stmt_nr = SA_NEW_ARRAY(sql->mvc->sa, int, list_length(s->op1->op4.lval));
						for (i=0, n = s->op1->op4.lval->h; n; n = n->next, i++) {
							stmt *op = n->data;

							q = newStmt2(mb, algebraRef, selectNotNilRef);
							q = pushArgument(mb, q, op->nr);
							stmt_nr[i] = getDestVar(q);
						}
					}
				}
				q = newStmt(mb, mod, aggrfunc);
				if (complex_aggr) {
					setVarType(mb, getArg(q, 0), restype);
					setVarUDFtype(mb, getArg(q, 0));
				}
			}
			if (s->op1->type != st_list) {
				q = pushArgument(mb, q, l);
			} else {
				for (i=0, n = s->op1->op4.lval->h; n; n = n->next, i++) {
					stmt *op = n->data;

					if (stmt_nr)
						q = pushArgument(mb, q, stmt_nr[i]);
					else
						q = pushArgument(mb, q, op->nr);
				}
			}
			if (g) {
				q = pushArgument(mb, q, g);
				q = pushArgument(mb, q, e);
				g = getDestVar(q);
				q = pushBit(mb, q, no_nil);
				if (abort_on_error)
					q = pushBit(mb, q, TRUE);
			}
			s->nr = getDestVar(q);
		}
			break;
		case st_atom:{
			atom *a = s->op4.aval;
			q = newStmt1(mb, calcRef, atom_type(a)->type->base.name);
			if (atom_null(a)) {
				q = pushNil(mb, q, atom_type(a)->type->localtype);
			} else {
				int k;
				k = constantAtom(sql, mb, a);
				q = pushArgument(mb, q, k);
			}
			/* digits of the result timestamp/daytime */
			if (EC_TEMP_FRAC(atom_type(a)->type->eclass))
				q = pushInt(mb, q, atom_type(a)->digits);
			s->nr = getDestVar(q);
		}
			break;
		case st_append:{
			int l = 0;
			int r = _dumpstmt(sql, mb, s->op2);

			l = _dumpstmt(sql, mb, s->op1);
			q = newStmt2(mb, batRef, appendRef);
			q = pushArgument(mb, q, l);
			q = pushArgument(mb, q, r);
			q = pushBit(mb, q, TRUE);
			s->nr = getDestVar(q);
		} break;
		case st_update_col:
		case st_append_col:{
			int tids = _dumpstmt(sql, mb, s->op1), upd = 0;
			sql_column *c = s->op4.cval;
			char *n = (s->type == st_append_col) ? appendRef : updateRef;

			if (s->op2)
				upd = _dumpstmt(sql, mb, s->op2);
			if (s->type == st_append_col && s->flag) {	/* fake append */
				s->nr = tids;
			} else {
				q = newStmt2(mb, sqlRef, n);
				q = pushArgument(mb, q, sql->mvc_var);
				getArg(q, 0) = sql->mvc_var = newTmpVariable(mb, TYPE_int);
				q = pushSchema(mb, q, c->t);
				q = pushStr(mb, q, c->t->base.name);
				q = pushStr(mb, q, c->base.name);
				q = pushArgument(mb, q, tids);
				if (s->op2)
					q = pushArgument(mb, q, upd);
				sql->mvc_var = s->nr = getDestVar(q);
			}
		}
			break;

		case st_update_idx:
		case st_append_idx:{
			int tids = _dumpstmt(sql, mb, s->op1), upd = 0;
			sql_idx *i = s->op4.idxval;
			char *n = (s->type == st_append_idx) ? appendRef : updateRef;

			if (s->op2)
				upd = _dumpstmt(sql, mb, s->op2);
			q = newStmt2(mb, sqlRef, n);
			q = pushArgument(mb, q, sql->mvc_var);
			getArg(q, 0) = sql->mvc_var = newTmpVariable(mb, TYPE_int);
			q = pushSchema(mb, q, i->t);
			q = pushStr(mb, q, i->t->base.name);
			q = pushStr(mb, q, sa_strconcat(sql->mvc->sa, "%", i->base.name));
			q = pushArgument(mb, q, tids);
			if (s->op2)
				q = pushArgument(mb, q, upd);
			sql->mvc_var = s->nr = getDestVar(q);
		}
			break;
		case st_delete:{
			int r = _dumpstmt(sql, mb, s->op1);
			sql_table *t = s->op4.tval;
			str mod = sqlRef;

			q = newStmt1(mb, mod, "delete");
			q = pushArgument(mb, q, sql->mvc_var);
			getArg(q, 0) = sql->mvc_var = newTmpVariable(mb, TYPE_int);
			q = pushSchema(mb, q, t);
			q = pushStr(mb, q, t->base.name);
			q = pushArgument(mb, q, r);
			sql->mvc_var = s->nr = getDestVar(q);
		} break;
		case st_table_clear:{
			sql_table *t = s->op4.tval;
			str mod = sqlRef;

			q = newStmt1(mb, mod, "clear_table");
			q = pushSchema(mb, q, t);
			q = pushStr(mb, q, t->base.name);
			s->nr = getDestVar(q);
		} break;
		case st_exception:{
			int l, r;

			l = _dumpstmt(sql, mb, s->op1);
			r = _dumpstmt(sql, mb, s->op2);

			/* if(bit(l)) { error(r);}  ==raising an exception */
			q = newStmt1(mb, sqlRef, "assert");
			q = pushArgument(mb, q, l);
			q = pushArgument(mb, q, r);
			s->nr = getDestVar(q);
			break;
		}
		case st_trans:{
			int l, r = -1;

			l = _dumpstmt(sql, mb, s->op1);
			if (s->op2)
				r = _dumpstmt(sql, mb, s->op2);
			q = newStmt1(mb, sqlRef, "trans");
			q = pushInt(mb, q, s->flag);
			q = pushArgument(mb, q, l);
			if (r > 0)
				q = pushArgument(mb, q, r);
			else
				q = pushNil(mb, q, TYPE_str);
			s->nr = getDestVar(q);
			break;
		}
		case st_catalog:{
			_dumpstmt(sql, mb, s->op1);

			q = newStmt1(mb, sqlRef, "catalog");
			q = pushInt(mb, q, s->flag);
			for (n = s->op1->op4.lval->h; n; n = n->next) {
				stmt *c = n->data;
				q = pushArgument(mb, q, c->nr);
			}
			s->nr = getDestVar(q);
			break;
		}
		case st_alias:
			s->nr = _dumpstmt(sql, mb, s->op1);
			break;
		case st_list:
			for (n = s->op4.lval->h; n; n = n->next) {
				_dumpstmt(sql, mb, n->data);
			}
			s->nr = 1;
			break;
		case st_rs_column:{
			_dumpstmt(sql, mb, s->op1);
			q = (void *) s->op1->rewritten;
			s->nr = getArg(q, s->flag);
		} break;
		case st_affected_rows:{
			InstrPtr q;
			int o1 = _dumpstmt(sql, mb, s->op1);

			q = newStmt1(mb, sqlRef, "affectedRows");
			q = pushArgument(mb, q, sql->mvc_var);
			getArg(q, 0) = sql->mvc_var = newTmpVariable(mb, TYPE_int);
			q = pushArgument(mb, q, o1);
			q = pushStr(mb, q, "");	/* warning */
			sql->mvc_var = s->nr = getDestVar(q);
		} break;
		case st_output:
		case st_export:{
			stmt *lst = s->op1;

			_dumpstmt(sql, mb, lst);

			if (lst->type == st_list) {
				list *l = lst->op4.lval;
				int file, cnt = list_length(l);
				stmt *first;
				InstrPtr k;

				n = l->h;
				first = n->data;

				/* single value result, has a fast exit */
				if (cnt == 1 && first->nrcols <= 0 && s->type != st_export) {
					stmt *c = n->data;
					sql_subtype *t = tail_type(c);
					char *tname = table_name(sql->mvc->sa, c);
					char *sname = schema_name(sql->mvc->sa, c);
					char *_empty = "";
					char *tn = (tname) ? tname : _empty;
					char *sn = (sname) ? sname : _empty;
					char *cn = column_name(sql->mvc->sa, c);
					char *ntn = sql_escape_ident(tn);
					char *nsn = sql_escape_ident(sn);
					size_t fqtnl = strlen(ntn) + 1 + strlen(nsn) + 1;
					char *fqtn = NEW_ARRAY(char, fqtnl);

					snprintf(fqtn, fqtnl, "%s.%s", nsn, ntn);

					q = newStmt2(mb, sqlRef, exportValueRef);
					s->nr = getDestVar(q);
					q = pushInt(mb, q, sql->mvc->type);
					q = pushStr(mb, q, fqtn);
					q = pushStr(mb, q, cn);
					q = pushStr(mb, q, t->type->localtype == TYPE_void ? "char" : t->type->sqlname);
					q = pushInt(mb, q, t->digits);
					q = pushInt(mb, q, t->scale);
					q = pushInt(mb, q, t->type->eclass);
					q = pushArgument(mb, q, c->nr);
					(void) pushStr(mb, q, "");	/* warning */
					_DELETE(ntn);
					_DELETE(nsn);
					_DELETE(fqtn);
					break;
				}
				k = newStmt2(mb, sqlRef, resultSetRef);
				s->nr = getDestVar(k);
				k = pushInt(mb, k, cnt);
				if (s->type == st_export) {
					node *n = s->op4.lval->h;
					char *sep = n->data;
					char *rsep = n->next->data;
					char *ssep = n->next->next->data;
					char *ns = n->next->next->next->data;

					k = pushStr(mb, k, sep);
					k = pushStr(mb, k, rsep);
					k = pushStr(mb, k, ssep);
					k = pushStr(mb, k, ns);
				} else {
					k = pushInt(mb, k, sql->mvc->type);
				}
				(void) pushArgument(mb, k, first->nr);
				dump_header(sql->mvc, mb, s, l);

				if (s->type == st_export && s->op2) {
					int codeset;

					q = newStmt(mb, "str", "codeset");
					codeset = getDestVar(q);
					file = _dumpstmt(sql, mb, s->op2);

					q = newStmt(mb, "str", "iconv");
					q = pushArgument(mb, q, file);
					q = pushStr(mb, q, "UTF-8");
					q = pushArgument(mb, q, codeset);
					file = getDestVar(q);

					q = newStmt(mb, "streams", "openWrite");
					q = pushArgument(mb, q, file);
					file = getDestVar(q);
				} else {
					q = newStmt(mb, "io", "stdout");
					file = getDestVar(q);
				}
				q = newStmt2(mb, sqlRef, exportResultRef);
				q = pushArgument(mb, q, file);
				(void) pushArgument(mb, q, s->nr);
				if (s->type == st_export && s->op2) {
					q = newStmt(mb, "streams", "close");
					(void) pushArgument(mb, q, file);
				}
			} else {
				q = newStmt1(mb, sqlRef, "print");
				(void) pushStr(mb, q, "not a valid output list\n");
				s->nr = 1;
			}
		}
			break;

		case st_table:{
			stmt *lst = s->op1;

			_dumpstmt(sql, mb, lst);

			if (lst->type != st_list) {
				q = newStmt1(mb, sqlRef, "print");
				(void) pushStr(mb, q, "not a valid output list\n");
			}
			s->nr = 1;
		}
			break;


		case st_cond:{
			int c = _dumpstmt(sql, mb, s->op1);

			if (!s->flag) {	/* if */
				q = newAssignment(mb);
				q->barrier = BARRIERsymbol;
				pushArgument(mb, q, c);
				s->nr = getArg(q, 0);
			} else {	/* while */
				int outer = _dumpstmt(sql, mb, s->op2);

				/* leave barrier */
				q = newStmt1(mb, calcRef, "not");
				q = pushArgument(mb, q, c);
				c = getArg(q, 0);

				q = newAssignment(mb);
				getArg(q, 0) = outer;
				q->barrier = LEAVEsymbol;
				pushArgument(mb, q, c);
				s->nr = outer;
			}
		} break;
		case st_control_end:{
			int c = _dumpstmt(sql, mb, s->op1);

			if (s->op1->flag) {	/* while */
				/* redo barrier */
				q = newAssignment(mb);
				getArg(q, 0) = c;
				q->argc = q->retc = 1;
				q->barrier = REDOsymbol;
				(void) pushBit(mb, q, TRUE);
			} else {
				q = newAssignment(mb);
				getArg(q, 0) = c;
				q->argc = q->retc = 1;
				q->barrier = EXITsymbol;
			}
			q = newStmt1(mb, sqlRef, "mvc");
			sql->mvc_var = getDestVar(q);
			s->nr = getArg(q, 0);
		}
			break;
		case st_return:{
			int c = _dumpstmt(sql, mb, s->op1);

			if (s->flag) {	/* drop declared tables */
				InstrPtr k = newStmt1(mb, sqlRef, "dropDeclaredTables");
				(void) pushInt(mb, k, s->flag);
			}
			q = newInstruction(mb, RETURNsymbol);
			if (s->op1->type == st_table) {
				list *l = s->op1->op1->op4.lval;

				q = dump_cols(mb, l, q);
			} else {
				getArg(q, 0) = getArg(getInstrPtr(mb, 0), 0);
				q = pushArgument(mb, q, c);
			}
			pushInstruction(mb, q);
			s->nr = 1;
		}
			break;
		case st_assign:{
			int r = -1;

			if (s->op2)
				r = _dumpstmt(sql, mb, s->op2);
			if (!VAR_GLOBAL(s->flag)) {	/* globals */
				char *buf = GDKmalloc(MAXIDENTLEN);
				char *vn = atom2string(sql->mvc->sa, s->op1->op4.aval);

				if (!s->op2) {
					/* drop declared table */
					s->nr = drop_table(mb, vn);
					break;
				}
				(void) snprintf(buf, MAXIDENTLEN, "A%s", vn);
				q = newInstruction(mb, ASSIGNsymbol);
				q->argc = q->retc = 0;
				q = pushArgumentId(mb, q, buf);
				pushInstruction(mb, q);
				q->retc++;
				s->nr = 1;
			} else {
				int vn = _dumpstmt(sql, mb, s->op1);
				q = newStmt1(mb, sqlRef, "setVariable");
				q = pushArgument(mb, q, sql->mvc_var);
				q = pushArgument(mb, q, vn);
				getArg(q, 0) = sql->mvc_var = newTmpVariable(mb, TYPE_int);
				sql->mvc_var = s->nr = getDestVar(q);
			}
			(void) pushArgument(mb, q, r);
		} break;
		}

		return s->nr;
	}

	return (0);
}

/*
 * @-
 * The kernel uses two calls to procedures defined in SQL.
 * They have to be initialized, which is currently hacked
 * by using the SQLstatment.
 */
static void
setCommitProperty(MalBlkPtr mb)
{
	ValRecord cst;

	if (varGetProp(mb, getArg(mb->stmt[0], 0), PropertyIndex("autoCommit")))
		 return;	/* already set */
	cst.vtype = TYPE_bit;
	cst.val.btval = TRUE;
	varSetProperty(mb, getArg(getInstrPtr(mb, 0), 0), "autoCommit", "=", &cst);
}

static int
backend_dumpstmt(backend *be, MalBlkPtr mb, stmt *s, int top)
{
	mvc *c = be->mvc;
	stmt **stmts = stmt_array(c->sa, s);
	InstrPtr q;
	int old_mv = be->mvc_var, nr = 0;

	/* announce the transaction mode */
	if (top && c->session->auto_commit)
		setCommitProperty(mb);
	q = newStmt1(mb, sqlRef, "mvc");
	be->mvc_var = getDestVar(q);

	/*print_stmts(c->sa, stmts); */
	clear_stmts(stmts);
	while (stmts[nr]) {
		stmt *s = stmts[nr++];
		if (_dumpstmt(be, mb, s) < 0)
			return -1;
	}
	if (_dumpstmt(be, mb, s) < 0)
		return -1;

	be->mvc_var = old_mv;
	if (top && c->caching && (c->type == Q_SCHEMA || c->type == Q_TRANS)) {
		q = newStmt2(mb, sqlRef, exportOperationRef);
		(void) pushStr(mb, q, "");	/* warning */
	}
	/* generate a dummy return assignment for functions */
	if (getArgType(mb, getInstrPtr(mb, 0), 0) != TYPE_void && getInstrPtr(mb, mb->stop - 1)->barrier != RETURNsymbol) {
		q = newAssignment(mb);
		getArg(q, 0) = getArg(getInstrPtr(mb, 0), 0);
		q->barrier = RETURNsymbol;
	}
	pushEndInstruction(mb);
	return 0;
}

int
backend_callinline(backend *be, Client c, stmt *s)
{
	mvc *m = be->mvc;
	InstrPtr curInstr = 0;
	MalBlkPtr curBlk = c->curprg->def;

	curInstr = getInstrPtr(curBlk, 0);

	if (m->argc) {	
		int argc = 0;

		for (; argc < m->argc; argc++) {
			atom *a = m->args[argc];
			int type = atom_type(a)->type->localtype;
			int varid = 0;

			curInstr = newAssignment(curBlk);
			a->varid = varid = getDestVar(curInstr);
			setVarType(curBlk, varid, type);
			setVarUDFtype(curBlk, varid);

			if (atom_null(a)) {
				sql_subtype *t = atom_type(a);
				(void) pushNil(curBlk, curInstr, t->type->localtype);
			} else {
				int _t = constantAtom(be, curBlk, a);
				(void) pushArgument(curBlk, curInstr, _t);
			}
		}
	}
	if (backend_dumpstmt(be, curBlk, s, 1) < 0)
		return -1;
	c->curprg->def = curBlk;
	return 0;
}

Symbol
backend_dumpproc(backend *be, Client c, cq *cq, stmt *s)
{
	mvc *m = be->mvc;
	MalBlkPtr mb = 0;
	Symbol curPrg = 0, backup = NULL;
	InstrPtr curInstr = 0;
	int argc = 0;
	char arg[SMALLBUFSIZ];
	node *n;

	backup = c->curprg;

	/* later we change this to a factory ? */
	if (cq)
		c->curprg = newFunction(userRef, putName(cq->name, strlen(cq->name)), FUNCTIONsymbol);
	else
		c->curprg = newFunction(userRef, "tmp", FUNCTIONsymbol);

	curPrg = c->curprg;
	curPrg->def->keephistory = backup->def->keephistory;
	mb = curPrg->def;
	curInstr = getInstrPtr(mb, 0);
	/* we do not return anything */
	setVarType(mb, 0, TYPE_void);
	setVarUDFtype(mb, 0);
	setModuleId(curInstr, userRef);

	if (m->argc) {
		for (argc = 0; argc < m->argc; argc++) {
			atom *a = m->args[argc];
			int type = atom_type(a)->type->localtype;
			int varid = 0;

			snprintf(arg, SMALLBUFSIZ, "A%d", argc);
			a->varid = varid = newVariable(mb, _STRDUP(arg), type);
			curInstr = pushArgument(mb, curInstr, varid);
			setVarType(mb, varid, type);
			setVarUDFtype(mb, 0);
		}
	} else if (m->params) {	/* needed for prepare statements */

		for (n = m->params->h; n; n = n->next, argc++) {
			sql_arg *a = n->data;
			int type = a->type.type->localtype;
			int varid = 0;

			snprintf(arg, SMALLBUFSIZ, "A%d", argc);
			varid = newVariable(mb, _STRDUP(arg), type);
			curInstr = pushArgument(mb, curInstr, varid);
			setVarType(mb, varid, type);
			setVarUDFtype(mb, varid);
		}
	}

	if (backend_dumpstmt(be, mb, s, 1) < 0)
		return NULL;

	// Always keep the SQL query around for monitoring
	// if (m->history || QLOGisset()) {
	{
		char *t, *tt;
		InstrPtr q;

		if (be->q && be->q->codestring) {
			tt = t = GDKstrdup(be->q->codestring);
			while (t && isspace((int) *t))
				t++;
		} else {
			tt = t = GDKstrdup("-- no query");
		}

		q = newStmt1(mb, "querylog", "define");
		q->token = REMsymbol;	// will be patched
		q = pushStr(mb, q, t);
		GDKfree(tt);
		q = pushStr(mb, q, getSQLoptimizer(be->mvc));
		m->Tparse = 0;
	}
	if (cq)
		addQueryToCache(c);

	curPrg = c->curprg;
	if (backup)
		c->curprg = backup;
	return curPrg;
}

void
backend_call(backend *be, Client c, cq *cq)
{
	mvc *m = be->mvc;
	InstrPtr q;
	MalBlkPtr mb = c->curprg->def;

	q = newStmt1(mb, userRef, cq->name);
	/* cached (factorized queries return bit??) */
	if (cq->code && getInstrPtr(((Symbol)cq->code)->def, 0)->token == FACTORYsymbol) {
		setVarType(mb, getArg(q, 0), TYPE_bit);
		setVarUDFtype(mb, getArg(q, 0));
	} else {
		setVarType(mb, getArg(q, 0), TYPE_void);
		setVarUDFtype(mb, getArg(q, 0));
	}
	if (m->argc) {
		int i;

		for (i = 0; i < m->argc; i++) {
			atom *a = m->args[i];
			sql_subtype *pt = cq->params + i;

			if (!atom_cast(a, pt)) {
				sql_error(m, 003, "wrong type for argument %d of " "function call: %s, expected %s\n", i + 1, atom_type(a)->type->sqlname, pt->type->sqlname);
				break;
			}
			if (atom_null(a)) {
				sql_subtype *t = cq->params + i;
				/* need type from the prepared argument */
				q = pushNil(mb, q, t->type->localtype);
			} else {
				int _t = constantAtom(be, mb, a);
				q = pushArgument(mb, q, _t);
			}
		}
	}
}

int
monet5_resolve_function(ptr M, sql_func *f)
{
	mvc *sql = (mvc *) M;
	Client c = MCgetClient(sql->clientid);
	Module m;

	/*
	   fails to search outer modules!
	   if (!findSymbol(c->nspace, f->mod, f->imp))
	   return 0;
	 */

	for (m = findModule(c->nspace, f->mod); m; m = m->outer) {
		if (strcmp(m->name, f->mod) == 0) {
			Symbol s = m->subscope[(int) (getSubScope(f->imp))];
			for (; s; s = s->peer) {
				InstrPtr sig = getSignature(s);
				int argc = sig->argc - sig->retc;

				if (strcmp(s->name, f->imp) == 0 && ((!f->ops && argc == 0) || list_length(f->ops) == argc || (sig->varargs & VARARGS) == VARARGS))
					return 1;

			}
		}
	}
	return 0;
/*
	node *n;
	newFcnCall(f->mod, f->imp);
	for (n = f->ops->h; n; n = n->next) {
		sql_arg *a = n->data;

		q = push ?type? (mb, q, a->);
	}
*/
}

/* TODO handle aggr */
int
backend_create_func(backend *be, sql_func *f)
{
	mvc *m = be->mvc;
	sql_schema *schema = m->session->schema;
	MalBlkPtr curBlk = 0;
	InstrPtr curInstr = 0;
	Client c = be->client;
	Symbol backup = NULL;
	stmt *s;
	int i, retseen = 0, sideeffects = 0;
	sql_allocator *sa, *osa = m->sa;

	/* nothing to do for internal and ready (not recompiling) functions */
	if (!f->sql || f->sql > 1)
		return 0;
	f->sql++;
	sa = sa_create();
	m->session->schema = f->s;
	s = sql_parse(m, sa, f->query, m_instantiate);
	m->sa = osa;
	m->session->schema = schema;
	if (s && !f->sql) {	/* native function */
		sa_destroy(sa);
		return 0;
	}

	if (!s) {
		f->sql--;
		sa_destroy(sa);
		return -1;
	}
	assert(s);

	backup = c->curprg;
	c->curprg = newFunction(userRef, putName(f->base.name, strlen(f->base.name)), FUNCTIONsymbol);

	curBlk = c->curprg->def;
	curInstr = getInstrPtr(curBlk, 0);

	if (f->res.type) {
		if (f->res.comp_type)
			curInstr = table_func_create_result(curBlk, curInstr, f->res.comp_type);
		else
			setVarType(curBlk, 0, f->res.type->localtype);
	} else {
		setVarType(curBlk, 0, TYPE_void);
	}
	setVarUDFtype(curBlk, 0);

	if (f->ops) {
		int argc = 0;
		node *n;

		for (n = f->ops->h; n; n = n->next, argc++) {
			sql_arg *a = n->data;
			int type = a->type.type->localtype;
			int varid = 0;
			char *buf = GDKmalloc(MAXIDENTLEN);

			if (a->name)
				(void) snprintf(buf, MAXIDENTLEN, "A%s", a->name);
			else
				(void) snprintf(buf, MAXIDENTLEN, "A%d", argc);
			varid = newVariable(curBlk, buf, type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
			setVarUDFtype(curBlk, varid);
		}
	}
	/* announce the transaction mode */
	if (m->session->auto_commit)
		setCommitProperty(curBlk);

	if (backend_dumpstmt(be, curBlk, s, 0) < 0)
		return -1;
	/* selectively make functions available for inlineing */
	/* for the time being we only inline scalar functions */
	/* and only if we see a single return value */
	/* check the function for side effects and make that explicit */
	sideeffects = 0;
	for (i = 1; i < curBlk->stop; i++) {
		InstrPtr p = getInstrPtr(curBlk, i);
		if (getFunctionId(p) == bindRef || getFunctionId(p) == bindidxRef)
			continue;
		sideeffects = sideeffects || hasSideEffects(p, FALSE) || (getModuleId(p) != sqlRef && isUpdateInstruction(p));
		if (p->token == RETURNsymbol || p->token == YIELDsymbol || p->barrier == RETURNsymbol || p->barrier == YIELDsymbol)
			retseen++;
	}
	if (i == curBlk->stop && retseen == 1 && !f->res.comp_type)
		varSetProp(curBlk, getArg(curInstr, 0), inlineProp, op_eq, NULL);
	if (sideeffects)
		varSetProp(curBlk, getArg(curInstr, 0), unsafeProp, op_eq, NULL);
	/* SQL function definitions meant for inlineing should not be optimized before */
	varSetProp(curBlk, getArg(curInstr, 0), sqlfunctionProp, op_eq, NULL);
	f->sa = sa;
	m->sa = osa;
	addQueryToCache(c);
	if (backup)
		c->curprg = backup;
	return 0;
}
