/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
 */
#include "monetdb_config.h"
#include "sql_gencode.h"
#include "sql_optimizer.h"
#include "sql_scenario.h"
#include "sql_mvc.h"
#include "sql_qc.h"
#include "sql_optimizer.h"
#include "mal_namespace.h"
#include "opt_prelude.h"
#include "querylog.h"
#include "mal_builder.h"
#include "mal_debugger.h"

#include <rel_select.h>
#include <rel_optimizer.h>
#include <rel_prop.h>
#include <rel_rel.h>
#include <rel_exp.h>
#include <rel_bin.h>
#include <rel_dump.h>
#include <rel_remote.h>

static int _dumpstmt(backend *sql, MalBlkPtr mb, stmt *s);

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
 *
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

static int
argumentZero(MalBlkPtr mb, int tpe)
{
	ValRecord cst;
	str msg;

	cst.vtype = TYPE_int;
	cst.val.ival = 0;
	msg = convertConstant(tpe, &cst);
	if( msg)
		GDKfree(msg); // will not be called
	return defConstant(mb, tpe, &cst);
}

/*
 * To speedup code generation we freeze the references to the major module names.
 */

void
initSQLreferences(void)
{
	if (algebraRef == NULL)
		GDKfatal("error initSQLreferences");
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
dump_header(mvc *sql, MalBlkPtr mb, stmt *s, list *l)
{
	node *n;
	InstrPtr q;
	int ret = -1;
	// gather the meta information
	int tblId, nmeId, tpeId, lenId, scaleId, k;
	InstrPtr p = NULL, list;

	list = newInstruction(mb,ASSIGNsymbol);
	getArg(list,0) = newTmpVariable(mb,TYPE_int);
	setModuleId(list, sqlRef);
	setFunctionId(list, resultSetRef);
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
		char *fqtn;

		if (ntn && nsn && (fqtnl = strlen(ntn) + 1 + strlen(nsn) + 1) ){
			fqtn = NEW_ARRAY(char, fqtnl);
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

static int
dump_export_header(mvc *sql, MalBlkPtr mb, list *l, int file, str format, str sep,str rsep,str ssep,str ns)
{
	node *n;
	InstrPtr q;
	int ret = -1;
	// gather the meta information
	int tblId, nmeId, tpeId, lenId, scaleId, k;
	InstrPtr p= NULL, list;

	list = newInstruction(mb,ASSIGNsymbol);
	getArg(list,0) = newTmpVariable(mb,TYPE_int);
	setModuleId(list, sqlRef);
	setFunctionId(list,export_tableRef);
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
		char *fqtn;

		if (ntn && nsn && (fqtnl = strlen(ntn) + 1 + strlen(nsn) + 1) ){
			fqtn = NEW_ARRAY(char, fqtnl);
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

static int
dump_table(MalBlkPtr mb, sql_table *t)
{
	int nr;
	node *n;
	InstrPtr k = newStmt(mb, sqlRef, putName("declaredTable"));

	nr = getDestVar(k);
	k = pushStr(mb, k, t->base.name);
	if (k == NULL)
		return -1;
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		const char *tname = c->t->base.name;
		const char *tn = sql_escape_ident(tname);
		const char *cn = c->base.name;
		InstrPtr q;

		if (tn == NULL)
			return -1;
		q = newStmt(mb, sqlRef, putName("dtColumn"));
		q = pushArgument(mb, q, nr);
		q = pushStr(mb, q, tn);
		q = pushStr(mb, q, cn);
		q = pushStr(mb, q, c->type.type->localtype == TYPE_void ? "char" : c->type.type->sqlname);
		q = pushInt(mb, q, c->type.digits);
		q = pushInt(mb, q, c->type.scale);
		c_delete(tn);
		if (q == NULL)
			return -1;
	}
	return nr;
}

static int
drop_table(MalBlkPtr mb, str n)
{
	InstrPtr k = newStmt(mb, sqlRef, putName("dropDeclaredTable"));
	int nr = getDestVar(k);

	k = pushStr(mb, k, n);
	if (k == NULL)
		return -1;
	return nr;
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

static InstrPtr
table_func_create_result(MalBlkPtr mb, InstrPtr q, sql_func *f, list *restypes)
{
	node *n;
	int i;

	if (q == NULL)
		return NULL;
	if (f->varres) {
		for (i = 0, n = restypes->h; n; n = n->next, i++) {
			sql_subtype *st = n->data;
			int type = st->type->localtype;

			type = newBatType(type);
			if (i) {
				if ((q = pushReturn(mb, q, newTmpVariable(mb, type))) == NULL)
					return NULL;
			} else
				setVarType(mb, getArg(q, 0), type);
			setVarUDFtype(mb, getArg(q, i));
		}
	} else {
		for (i = 0, n = f->res->h; n; n = n->next, i++) {
			sql_arg *a = n->data;
			int type = a->type.type->localtype;

			type = newBatType(type);
			if (i) {
				if ((q = pushReturn(mb, q, newTmpVariable(mb, type))) == NULL)
					return NULL;
			} else
				setVarType(mb, getArg(q, 0), type);
			setVarUDFtype(mb, getArg(q, i));
		}
	}
	return q;
}

static InstrPtr
relational_func_create_result(mvc *sql, MalBlkPtr mb, InstrPtr q, sql_rel *f)
{
	sql_rel *r = f;
	node *n;
	int i;

	if (q == NULL)
		return NULL;
	if (is_topn(r->op))
		r = r->l;
	if (!is_project(r->op))
		r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
	q->argc = q->retc = 0;
	for (i = 0, n = r->exps->h; n; n = n->next, i++) {
		sql_exp *e = n->data;
		int type = exp_subtype(e)->type->localtype;

		type = newBatType(type);
		q = pushReturn(mb, q, newTmpVariable(mb, type));
	}
	return q;
}


static int
_create_relational_function(mvc *m, char *mod, char *name, sql_rel *rel, stmt *call, int inline_func)
{
	sql_rel *r;
	Client c = MCgetClient(m->clientid);
	backend *be = (backend *) c->sqlcontext;
	MalBlkPtr curBlk = 0;
	InstrPtr curInstr = 0;
	Symbol backup = NULL, curPrg = NULL;
	stmt *s;
	int old_argc = be->mvc->argc;

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
	curPrg = c->curprg = newFunction(putName(mod), putName(name), FUNCTIONsymbol);
	if( curPrg == NULL)
		return -1;

	curBlk = c->curprg->def;
	curInstr = getInstrPtr(curBlk, 0);

	curInstr = relational_func_create_result(m, curBlk, curInstr, r);
	setVarUDFtype(curBlk, 0);

	/* ops */
	if (call && (call->type == st_list || call->op1->type == st_list)) {
		node *n;
		list *ops = NULL;

		if (call->type == st_list)
			ops = call->op4.lval;
		else
			ops = call->op1->op4.lval;
		for (n = ops->h; n; n = n->next) {
			stmt *op = n->data;
			sql_subtype *t = tail_type(op);
			int type = t->type->localtype;
			int varid = 0;
			const char *nme = (op->op3)?op->op3->op4.aval->data.val.sval:op->cname;
			char buf[64];

			if (nme[0] != 'A')
				snprintf(buf,64,"A%s",nme);
			else
				snprintf(buf,64,"%s",nme);
			varid = newVariable(curBlk, buf, strlen(buf), type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
			setVarUDFtype(curBlk, varid);
		}
	}

	be->mvc->argc = 0;
	if (backend_dumpstmt(be, curBlk, s, 0, 1, NULL) < 0) {
		freeSymbol(curPrg);
		if (backup)
			c->curprg = backup;
		return -1;
	}
	be->mvc->argc = old_argc;
	/* SQL function definitions meant for inlineing should not be optimized before */
	if (inline_func)
		curBlk->inlineProp = 1;
	/* optimize the code */
	SQLaddQueryToCache(c);
	if( curBlk->inlineProp == 0)
		SQLoptimizeQuery(c, c->curprg->def);
	else{
		chkProgram(c->fdout, c->nspace, c->curprg->def);
		SQLoptimizeFunction(c,c->curprg->def);
	}
	if (backup)
		c->curprg = backup;
	return 0;
}

static str
rel2str( mvc *sql, sql_rel *rel)
{
	buffer *b;
	stream *s = buffer_wastream(b = buffer_create(1024), "rel_dump");
	list *refs = sa_list(sql->sa);
	char *res = NULL; 

	rel_print_refs(sql, s, rel, 0, refs, 0);
	rel_print_(sql, s, rel, 0, refs, 0);
	mnstr_printf(s, "\n");
	res = buffer_get_buf(b);
	buffer_destroy(b);
	mnstr_destroy(s);
	return res;
}


/* stub and remote function */
static int
_create_relational_remote(mvc *m, char *mod, char *name, sql_rel *rel, stmt *call, prop *prp)
{
	Client c = MCgetClient(m->clientid);
	MalBlkPtr curBlk = 0;
	InstrPtr curInstr = 0, p, o;
	Symbol backup = NULL;
	const char *uri = mapiuri_uri(prp->value, m->sa);
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
	if (_create_relational_function(m, mod, name, rel, call, 0) < 0)
		return -1;

	/* create stub */
	name[0] = old;
	backup = c->curprg;
	c->curprg = newFunction(putName(mod), putName(name), FUNCTIONsymbol);
	if( c->curprg == NULL)
		return -1;
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
			const char *nme = (op->op3)?op->op3->op4.aval->data.val.sval:op->cname;
			char buf[64];

			snprintf(buf,64,"A%s",nme);
			varid = newVariable(curBlk, buf,strlen(buf), type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
			setVarUDFtype(curBlk, varid);
		}
	}

	/* declare return variables */
	for (i = 0, n = r->exps->h; n; n = n->next, i++) {
		sql_exp *e = n->data;
		int type = exp_subtype(e)->type->localtype;

		type = newBatType(type);
		p = newFcnCall(curBlk, batRef, newRef);
		p = pushType(curBlk, p, getBatType(type));
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

#define REL
#ifndef REL
	/* remote.register(q, "mod", "fcn"); */
	p = newStmt(curBlk, remoteRef, registerRef);
	p = pushArgument(curBlk, p, q);
	p = pushStr(curBlk, p, mod);
	p = pushStr(curBlk, p, name);
#else
	/* remote.exec(q, "sql", "register", "mod", "name", "relational_plan", "signature"); */
	p = newInstruction(curBlk, ASSIGNsymbol);
	setModuleId(p, remoteRef);
	setFunctionId(p, execRef);
	p = pushArgument(curBlk, p, q);
	p = pushStr(curBlk, p, sqlRef);
	p = pushStr(curBlk, p, registerRef);

	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushInt(curBlk, o, TYPE_str); /* dummy result type */
	p = pushReturn(curBlk, p, getArg(o, 0));

	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushStr(curBlk, o, mod);
	p = pushArgument(curBlk, p, getArg(o,0));

	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushStr(curBlk, o, name);
	p = pushArgument(curBlk, p, getArg(o,0));

	{ 
	int len = 1024, nr = 0;
	char *s, *buf = GDKmalloc(len);
	s = rel2str(m, rel);
	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushStr(curBlk, o, s);	/* relational plan */
	p = pushArgument(curBlk, p, getArg(o,0));
	free(s); 

	s = "";
	if (call->op1->type == st_list) {
		node *n;

		buf[0] = 0;
		for (n = call->op1->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;
			sql_subtype *t = tail_type(op);
			const char *nme = (op->op3)?op->op3->op4.aval->data.val.sval:op->cname;

			nr += snprintf(buf+nr, len-nr, "%s %s(%u,%u)%c", nme, t->type->sqlname, t->digits, t->scale, n->next?',':' ');
		}
		s = buf;
	}
	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushStr(curBlk, o, s);	/* signature */
	p = pushArgument(curBlk, p, getArg(o,0));
	GDKfree(buf);
	}
	pushInstruction(curBlk, p);
#endif

	/* (x1, x2, ..., xn) := remote.exec(q, "mod", "fcn"); */
	p = newInstruction(curBlk, ASSIGNsymbol);
	setModuleId(p, remoteRef);
	setFunctionId(p, execRef);
	p = pushArgument(curBlk, p, q);
	p = pushStr(curBlk, p, mod);
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

	/* SQL function definitions meant for inlineing should not be optimized before */
	//for now no inline of the remote function, this gives garbage collection problems
	//curBlk->inlineProp = 1;

	SQLaddQueryToCache(c);
	//chkProgram(c->fdout, c->nspace, c->curprg->def);
	SQLoptimizeFunction(c, c->curprg->def);
	if (backup)
		c->curprg = backup;
	name[0] = old;		/* make sure stub is called */
	return 0;
}

int
monet5_create_relational_function(mvc *m, char *mod, char *name, sql_rel *rel, stmt *call, int inline_func)
{
	prop *p = NULL;

	if (rel && (p = find_prop(rel->p, PROP_REMOTE)) != NULL)
		return _create_relational_remote(m, mod, name, rel, call, p);
	else
		return _create_relational_function(m, mod, name, rel, call, inline_func);
}

/*
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
_dump_1(MalBlkPtr mb, char *mod, char *name, int o1)
{
	InstrPtr q;

	q = newStmt(mb, mod, name);
	q = pushArgument(mb, q, o1);
	if (q == NULL)
		return -1;
	return getDestVar(q);
}

static int
dump_1(backend *sql, MalBlkPtr mb, stmt *s, char *mod, char *name)
{
	int o1 = _dumpstmt(sql, mb, s->op1);

	if (o1 < 0)
		return -1;
	s->nr = _dump_1(mb, mod, name, o1);
	return s->nr;
}

static int
_dump_2(MalBlkPtr mb, char *mod, char *name, int o1, int o2)
{
	InstrPtr q;

	q = newStmt(mb, mod, name);
	q = pushArgument(mb, q, o1);
	q = pushArgument(mb, q, o2);
	if (q == NULL)
		return -1;
	return getDestVar(q);
}

static int
dump_2(backend *sql, MalBlkPtr mb, stmt *s, char *mod, char *name)
{
	int o1, o2;

	if ((o1 = _dumpstmt(sql, mb, s->op1)) < 0)
		return -1;
	if ((o2 = _dumpstmt(sql, mb, s->op2)) < 0)
		return -1;
	s->nr = _dump_2(mb, mod, name, o1, o2);
	return s->nr;
}

static int
dump_2_(backend *sql, MalBlkPtr mb, stmt *s, char *mod, char *name)
{
	InstrPtr q;
	int o1, o2;

	if ((o1 = _dumpstmt(sql, mb, s->op1)) < 0)
		return -1;
	if ((o2 = _dumpstmt(sql, mb, s->op2)) < 0)
		return -1;

	q = newStmt(mb, mod, name);
	q = pushArgument(mb, q, o1);
	q = pushArgument(mb, q, o2);
	if (q == NULL)
		return -1;
	s->nr = getDestVar(q);
	return 0;
}

static InstrPtr
multiplex2(MalBlkPtr mb, char *mod, char *name /* should be eaten */ , int o1, int o2, int rtype)
{
	InstrPtr q;

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

static int backend_create_subfunc(backend *be, sql_subfunc *f, list *ops);
static int backend_create_subaggr(backend *be, sql_subaggr *f);

static int
dump_joinN(backend *sql, MalBlkPtr mb, stmt *s)
{
	char *mod, *fimp;
	InstrPtr q;
	bit swapped = (s->flag & SWAPPED) ? TRUE : FALSE;
	node *n;

	if (backend_create_subfunc(sql, s->op4.funcval, NULL) < 0)
		return -1;
	mod = sql_func_mod(s->op4.funcval->func);
	fimp = sql_func_imp(s->op4.funcval->func);
	fimp = sa_strconcat(sql->mvc->sa, fimp, "subjoin");

	/* dump left and right operands */
	_dumpstmt(sql, mb, s->op1);
	_dumpstmt(sql, mb, s->op2);

	/* filter qualifying tuples, return oids of h and tail */
	q = newStmt(mb, mod, fimp);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	for (n = s->op1->op4.lval->h; n; n = n->next) {
		stmt *op = n->data;

		q = pushArgument(mb, q, op->nr);
	}

	for (n = s->op2->op4.lval->h; n; n = n->next) {
		stmt *op = n->data;

		q = pushArgument(mb, q, op->nr);
	}
	q = pushNil(mb, q, TYPE_bat); /* candidate lists */
	q = pushNil(mb, q, TYPE_bat); /* candidate lists */
	q = pushBit(mb, q, TRUE);     /* nil_matches */
	q = pushNil(mb, q, TYPE_lng); /* estimate */
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
		return pushArgument(mb, q, getStrConstant(mb,t->s->base.name));
	else
		return pushNil(mb, q, TYPE_str);
}

/*
 * The big code generation switch.
 */
static int
_dumpstmt(backend *sql, MalBlkPtr mb, stmt *s)
{
	InstrPtr q = NULL;
	node *n;

	if (THRhighwater())
		return -1;
	if (s) {
		if (s->nr > 0)
			return s->nr;	/* stmt already handled */

		switch (s->type) {
		case st_none:{
			q = newAssignment(mb);
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);
			(void) pushInt(mb, q, 1);
		} break;
		case st_var:{
			if (s->op1) {
				if (VAR_GLOBAL(s->flag)) {	/* globals */
					int tt = tail_type(s)->type->localtype;

					q = newStmt(mb, sqlRef, putName("getVariable"));
					q = pushArgument(mb, q, sql->mvc_var);
					q = pushStr(mb, q, s->op1->op4.aval->data.val.sval);
					if (q == NULL)
						return -1;
					setVarType(mb, getArg(q, 0), tt);
					setVarUDFtype(mb, getArg(q, 0));
				} else if ((s->flag & VAR_DECLARE) == 0) {
					char *buf = GDKmalloc(MAXIDENTLEN);

					if (buf == NULL)
						return -1;
					(void) snprintf(buf, MAXIDENTLEN, "A%s", s->op1->op4.aval->data.val.sval);
					q = newAssignment(mb);
					q = pushArgumentId(mb, q, buf);
					GDKfree(buf);
					if (q == NULL)
						return -1;
				} else {
					sql_subtype *st = tail_type(s);
					char *buf;
					int tt;

					if (s->op3) {
						/* declared table */
						s->nr = dump_table(mb, (sql_table*)s->op3);
						if (s->nr < 0)
							return -1;
						break;
					}
					tt = st->type->localtype;
				       	buf = GDKmalloc(MAXIDENTLEN);
					if (buf == NULL)
						return -1;
					(void) snprintf(buf, MAXIDENTLEN, "A%s", s->op1->op4.aval->data.val.sval);
					q = newInstruction(mb, ASSIGNsymbol);
					if (q == NULL) {
						GDKfree(buf);
						return -1;
					}
					q->argc = q->retc = 0;
					q = pushArgumentId(mb, q, buf);
					GDKfree(buf);
					q = pushNil(mb, q, tt);
					pushInstruction(mb, q);
					if (q == NULL)
						return -1;
					q->retc++;
				}
			} else {
				q = newAssignment(mb);
				if (sql->mvc->argc && sql->mvc->args[s->flag]->varid >= 0) {
					q = pushArgument(mb, q, sql->mvc->args[s->flag]->varid);
				} else {
					char *buf = GDKmalloc(IDLENGTH);
					if (buf == NULL)
						return -1;
					(void) snprintf(buf, IDLENGTH, "A%d", s->flag);
					q = pushArgumentId(mb, q, buf);
					GDKfree(buf);
				}
				if (q == NULL)
					return -1;
			}
			s->nr = getDestVar(q);
		} break;
		case st_single:{
			int tt = s->op4.typeval.type->localtype;
			int val = _dumpstmt(sql, mb, s->op1);

			if (val < 0)
				return -1;
			q = newStmt(mb, sqlRef, singleRef);
			if (q == NULL)
				return -1;
			setVarType(mb, getArg(q, 0), newBatType(tt));
			q = pushArgument(mb, q, val);
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);
		} break;
		case st_temp:{
			int tt = s->op4.typeval.type->localtype;

			q = newStmt(mb, batRef, newRef);
			if (q == NULL)
				return -1;
			setVarType(mb, getArg(q, 0), newBatType(tt));
			setVarUDFtype(mb, getArg(q, 0));
			q = pushType(mb, q, tt);
			if (q == NULL)
				return -1;

			s->nr = getDestVar(q);
		} break;
		case st_tid:{
			int tt = TYPE_oid;
			sql_table *t = s->op4.tval;

			q = newStmt(mb, sqlRef, tidRef);
			if (q == NULL)
				return -1;
			setVarType(mb, getArg(q, 0), newBatType(tt));
			setVarUDFtype(mb, getArg(q, 0));
			q = pushArgument(mb, q, sql->mvc_var);
			q = pushSchema(mb, q, t);
			q = pushStr(mb, q, t->base.name);
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);
			if (t && (!isRemote(t) && !isMergeTable(t)) && s->partition) {
				sql_trans *tr = sql->mvc->session->tr;
				BUN rows = (BUN) store_funcs.count_col(tr, t->columns.set->h->data, 1);
				setRowCnt(mb,getArg(q,0),rows);
				if (t->p && 0)
					setMitosisPartition(q, t->p->base.id);
			}
		}
			break;
		case st_bat:{
			int tt = s->op4.cval->type.type->localtype;
			sql_column *c = s->op4.cval;
			sql_table *t = c->t;

			q = newStmt(mb, sqlRef, bindRef);
			if (q == NULL)
				return -1;
			if (s->flag == RD_UPD_ID) {
				q = pushReturn(mb, q, newTmpVariable(mb, newBatType(tt)));
				setVarUDFtype(mb, getArg(q, 0));
				setVarUDFtype(mb, getArg(q, 1));
			} else {
				setVarType(mb, getArg(q, 0), newBatType(tt));
				setVarUDFtype(mb, getArg(q, 0));
			}
			q = pushArgument(mb, q, sql->mvc_var);
			q = pushSchema(mb, q, t);
			q = pushArgument(mb, q, getStrConstant(mb,t->base.name));
			q = pushArgument(mb, q, getStrConstant(mb,c->base.name));
			q = pushArgument(mb, q, getIntConstant(mb,s->flag));
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);

			if (s->flag == RD_UPD_ID) {
				/* rename second result */
				renameVariable(mb, getArg(q, 1), "r1_%d", s->nr);
				setVarType(mb, getArg(q, 1), newBatType(tt));
				setVarUDFtype(mb, getArg(q, 1));
			}
			if (s->flag != RD_INS && s->partition) {
				sql_trans *tr = sql->mvc->session->tr;

				if (c && (!isRemote(c->t) && !isMergeTable(c->t))) {
					BUN rows = (BUN) store_funcs.count_col(tr, c, 1);
					setRowCnt(mb,getArg(q,0),rows);
					if (t->p && 0)
						setMitosisPartition(q, t->p->base.id);
				}
			}
		}
			break;
		case st_idxbat:{
			int tt = tail_type(s)->type->localtype;
			sql_idx *i = s->op4.idxval;
			sql_table *t = i->t;

			q = newStmt(mb, sqlRef, bindidxRef);
			if (q == NULL)
				return -1;
			if (s->flag == RD_UPD_ID) {
				q = pushReturn(mb, q, newTmpVariable(mb, newBatType(tt)));
			} else {
				setVarType(mb, getArg(q, 0), newBatType(tt));
				setVarUDFtype(mb, getArg(q, 0));
			}

			q = pushArgument(mb, q, sql->mvc_var);
			q = pushSchema(mb, q, t);
			q = pushArgument(mb, q, getStrConstant(mb,t->base.name));
			q = pushArgument(mb, q, getStrConstant(mb,i->base.name));
			q = pushArgument(mb, q, getIntConstant(mb,s->flag));
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);

			if (s->flag == RD_UPD_ID) {
				/* rename second result */
				renameVariable(mb, getArg(q, 1), "r1_%d", s->nr);
			}
			if (s->flag != RD_INS && s->partition) {
				sql_trans *tr = sql->mvc->session->tr;

				if (i && (!isRemote(i->t) && !isMergeTable(i->t))) {
					BUN rows = (BUN) store_funcs.count_idx(tr, i, 1);
					setRowCnt(mb,getArg(q,0),rows);
					if (t->p && 0)
						setMitosisPartition(q, t->p->base.id);
				}
			}
		}
			break;
		case st_const:{
			if (s->op2) {
				if (dump_2(sql, mb, s, algebraRef, projectRef) < 0)
					return -1;
			} else {
				if (dump_1(sql, mb, s, algebraRef, projectRef) < 0)
					return -1;
			}
		}
			break;
		case st_gen_group:{
			if (dump_2(sql, mb, s, algebraRef, groupbyRef) < 0)
				return -1;
		}
			break;
		case st_mirror:{
			if (dump_1(sql, mb, s, batRef, mirrorRef) < 0)
				return -1;
		}
			break;
		case st_limit2:
		case st_limit:{
			stmt *piv, *gid, *col;
			int l, offset, len, p, g, c;

			if ((l = _dumpstmt(sql, mb, s->op1)) < 0)
				return -1;
			col = (s->type == st_limit2) ? s->op1->op4.lval->h->data : s->op1;
			piv = (s->type == st_limit2) ? s->op1->op4.lval->h->next->data : NULL;
			gid = (s->type == st_limit2) ? s->op1->op4.lval->t->data : NULL;
			if ((offset = _dumpstmt(sql, mb, s->op2)) < 0)
				return -1;
			if ((len = _dumpstmt(sql, mb, s->op3)) < 0)
				return -1;
			c = (col) ? col->nr : 0;
			p = (piv) ? piv->nr : 0;
			g = (gid) ? gid->nr : 0;

			/* first insert single value into a bat */
			assert(s->nrcols);
			if (s->nrcols == 0) {
				int k;
				int tt = tail_type(s->op1)->type->localtype;

				assert(0);
				q = newStmt(mb, batRef, newRef);
				if (q == NULL)
					return -1;
				setVarType(mb, getArg(q, 0), newBatType(tt));
				setVarUDFtype(mb, getArg(q, 0));
				q = pushType(mb, q, tt);
				if (q == NULL)
					return -1;
				k = getDestVar(q);

				q = newStmt(mb, batRef, appendRef);
				q = pushArgument(mb, q, k);
				q = pushArgument(mb, q, c);
				if (q == NULL)
					return -1;
				c = k;
			}
			if (s->flag&1) {
				int topn = 0, flag = s->flag;
				int last = (flag & 2);
				int dir = (flag & 4);
				int distinct = (flag & 8);

				q = newStmt(mb, calcRef, "+");
				q = pushArgument(mb, q, offset);
				q = pushArgument(mb, q, len);
				if (q == NULL)
					return -1;
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
					return -1;
				s->nr = getArg(q, 0);
				if (!last)
					renameVariable(mb, getArg(q, 1), "r1_%d", s->nr);
				l = getDestVar(q);
			} else {
				q = newStmt(mb, calcRef, "+");
				q = pushArgument(mb, q, offset);
				q = pushArgument(mb, q, len);
				if (q == NULL)
					return -1;
				len = getDestVar(q);

				/* since both arguments of algebra.subslice are
				   inclusive correct the LIMIT value by
				   subtracting 1 */
				q = newStmt(mb, calcRef, "-");
				q = pushArgument(mb, q, len);
				q = pushInt(mb, q, 1);
				if (q == NULL)
					return -1;
				len = getDestVar(q);

				q = newStmt(mb, algebraRef, subsliceRef);
				q = pushArgument(mb, q, c);
				q = pushArgument(mb, q, offset);
				q = pushArgument(mb, q, len);
				if (q == NULL)
					return -1;
				l = getDestVar(q);
			}
			/* retrieve the single values again */
			if (s->nrcols == 0) {
				q = newStmt(mb, algebraRef, findRef);
				q = pushArgument(mb, q, l);
				q = pushOid(mb, q, 0);
				if (q == NULL)
					return -1;
				l = getDestVar(q);
			}
			s->nr = l;
		}
			break;
		case st_sample:{
			int l, r;

			if ((l = _dumpstmt(sql, mb, s->op1)) < 0)
				return -1;
			if ((r = _dumpstmt(sql, mb, s->op2)) < 0)
				return -1;
			q = newStmt(mb, sampleRef, subuniformRef);
			q = pushArgument(mb, q, l);
			q = pushArgument(mb, q, r);
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);
		} break;
		case st_order:{
			int l = _dumpstmt(sql, mb, s->op1);
			int reverse = (s->flag <= 0);

			if (l < 0)
				return -1;
			q = newStmt(mb, algebraRef, subsortRef);
			/* both ordered result and oid's order en subgroups */
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, l);
			q = pushBit(mb, q, reverse);
			q = pushBit(mb, q, FALSE);
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);

			renameVariable(mb, getArg(q, 1), "r1_%d", s->nr);
			renameVariable(mb, getArg(q, 2), "r2_%d", s->nr);
		} break;
		case st_reorder:{
			int l;
			int oids;
			int ogrp;
			int reverse;

			if ((l = _dumpstmt(sql, mb, s->op1)) < 0)
				return -1;
			if ((oids = _dumpstmt(sql, mb, s->op2)) < 0)
				return -1;
			if ((ogrp = _dumpstmt(sql, mb, s->op3)) < 0)
				return -1;
			reverse = (s->flag <= 0);

			q = newStmt(mb, algebraRef, subsortRef);
			/* both ordered result and oid's order en subgroups */
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, l);
			q = pushArgument(mb, q, oids);
			q = pushArgument(mb, q, ogrp);
			q = pushBit(mb, q, reverse);
			q = pushBit(mb, q, FALSE);
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);

			renameVariable(mb, getArg(q, 1), "r1_%d", s->nr);
			renameVariable(mb, getArg(q, 2), "r2_%d", s->nr);
		} break;
		case st_uselect:{
			bit need_not = FALSE;
			int l, r, sub, anti;
			node *n;

			if ((l = _dumpstmt(sql, mb, s->op1)) < 0)
				return -1;
			if ((r = _dumpstmt(sql, mb, s->op2)) < 0)
				return -1;
			sub = -1;
			anti = is_anti(s);

			if (s->op3 && (sub = _dumpstmt(sql, mb, s->op3)) < 0)
				return -1;

			if (s->op2->nrcols >= 1) {
				char *mod = calcRef;
				char *op = "=";
				int k, done = 0;

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
				case cmp_filter:
					done = 1;

					if (backend_create_subfunc(sql, s->op4.funcval, NULL) < 0)
						return -1;
					op = sql_func_imp(s->op4.funcval->func);
					mod = sql_func_mod(s->op4.funcval->func);

					q = newStmt(mb, malRef, multiplexRef);
					setVarType(mb, getArg(q, 0), newBatType(TYPE_bit));
					setVarUDFtype(mb, getArg(q, 0));
					q = pushStr(mb, q, convertMultiplexMod(mod, op));
					q = pushStr(mb, q, convertMultiplexFcn(op));
					for (n = s->op1->op4.lval->h; n; n = n->next) {
						stmt *op = n->data;
						q = pushArgument(mb, q, op->nr);
					}
					for (n = s->op2->op4.lval->h; n; n = n->next) {
						stmt *op = n->data;
						q = pushArgument(mb, q, op->nr);
					}
					if (q == NULL)
						return -1;
					break;
				default:
					showException(GDKout, SQL, "sql", "Unknown operator");
				}

				if (!done && (q = multiplex2(mb, mod, convertOperator(op), l, r, TYPE_bit)) == NULL) 
					return -1;
				k = getDestVar(q);

				q = newStmt(mb, algebraRef, subselectRef);
				q = pushArgument(mb, q, k);
				if (sub > 0)
					q = pushArgument(mb, q, sub);
				q = pushBit(mb, q, !need_not);
				q = pushBit(mb, q, !need_not);
				q = pushBit(mb, q, TRUE);
				q = pushBit(mb, q, TRUE);
				q = pushBit(mb, q, anti);
				if (q == NULL)
					return -1;
				k = getDestVar(q);
			} else {
				if (get_cmp(s) == cmp_filter) {
					node *n;
					char *mod, *fimp;
					sql_func *f = s->op4.funcval->func;

					if (backend_create_subfunc(sql, s->op4.funcval, NULL) < 0)
						return -1;

					mod = sql_func_mod(f);
					fimp = sql_func_imp(f);
					fimp = sa_strconcat(sql->mvc->sa, fimp, subselectRef);
					q = newStmt(mb, mod, convertOperator(fimp));
					// push pointer to the SQL structure into the MAL call
					// allows getting argument names for example
					if (LANG_EXT(f->lang))
						q = pushPtr(mb, q, s->op4.funcval); // nothing to see here, please move along
					// f->query contains the R code to be run
					if (f->lang == FUNC_LANG_R || f->lang == FUNC_LANG_PY || f->lang == FUNC_LANG_MAP_PY)
						q = pushStr(mb, q, f->query);

					for (n = s->op1->op4.lval->h; n; n = n->next) {
						stmt *op = n->data;

						q = pushArgument(mb, q, op->nr);
					}
					/* candidate lists */
					if (sub > 0)
						q = pushArgument(mb, q, sub);
					else
						q = pushNil(mb, q, TYPE_bat); 

					for (n = s->op2->op4.lval->h; n; n = n->next) {
						stmt *op = n->data;

						q = pushArgument(mb, q, op->nr);
					}

					q = pushBit(mb, q, anti);
					if (q == NULL)
						return -1;
					s->nr = getDestVar(q);
					break;
				}

				q = newStmt(mb, algebraRef, thetasubselectRef);
				q = pushArgument(mb, q, l);
				if (sub > 0)
					q = pushArgument(mb, q, sub);
				q = pushArgument(mb, q, r);
				switch (s->flag) {
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
					showException(GDKout, SQL, "sql", "SQL2MAL: error impossible subselect compare\n");
					if (q)
						freeInstruction(q);
					q = NULL;
				}
				if (q == NULL)
					return -1;
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
			char *cmd = (s->type == st_uselect2) ? subselectRef : subrangejoinRef;
			int sub = -1;

			if (l < 0)
				return -1;
			if (s->op4.stval &&
			    (sub = _dumpstmt(sql, mb, s->op4.stval)) < 0)
				return -1;

			if ((s->op2->nrcols > 0 || s->op3->nrcols) && (s->type == st_uselect2)) {
				int k, symmetric = s->flag&CMP_SYMMETRIC;
				char *mod = calcRef;
				char *op1 = "<", *op2 = "<";

				if ((r1 = _dumpstmt(sql, mb, s->op2)) < 0)
					return -1;
				if ((r2 = _dumpstmt(sql, mb, s->op3)) < 0)
					return -1;

				if (s->flag & 1)
					op1 = "<=";
				if (s->flag & 2)
					op2 = "<=";

				if (s->flag&1 && s->flag&2) {
					if (symmetric)
						p = newStmt(mb, batcalcRef, betweensymmetricRef);
					else
						p = newStmt(mb, batcalcRef, betweenRef);
					p = pushArgument(mb, p, l);
					p = pushArgument(mb, p, r1);
					p = pushArgument(mb, p, r2);
					k = getDestVar(p);
				} else {
					if ((q = multiplex2(mb, mod, convertOperator(op1), l, r1, TYPE_bit)) == NULL)
						return -1;

					if ((r = multiplex2(mb, mod, convertOperator(op2), l, r2, TYPE_bit)) == NULL)
						return -1;
					p = newStmt(mb, batcalcRef, andRef);
					p = pushArgument(mb, p, getDestVar(q));
					p = pushArgument(mb, p, getDestVar(r));
					if (p == NULL)
						return -1;
					k = getDestVar(p);
				}

				q = newStmt(mb, algebraRef, subselectRef);
				q = pushArgument(mb, q, k);
				if (sub > 0)
					q = pushArgument(mb, q, sub);
				q = pushBit(mb, q, TRUE);
				q = pushBit(mb, q, TRUE);
				q = pushBit(mb, q, TRUE);
				q = pushBit(mb, q, TRUE);
				q = pushBit(mb, q, FALSE);
				if (q == NULL)
					return -1;
				s->nr = getDestVar(q);
				break;
			}
			/* if st_join2 try to convert to bandjoin */
			/* ie check if we subtract/add a constant, to the
			   same column */
			if (s->type == st_join2 && range_join_convertable(s, &base, &low, &high)) {
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
				cmd = subbandjoinRef;
			}

			if (!rs) {
				if ((r1 = _dumpstmt(sql, mb, s->op2)) < 0)
					return -1;
				if ((r2 = _dumpstmt(sql, mb, s->op3)) < 0)
					return -1;
			}
			q = newStmt(mb, algebraRef, cmd);
			if (s->type == st_join2)
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, l);
			if (sub > 0) /* only for uselect2 */
				q = pushArgument(mb, q, sub);
			if (rs) {
				q = pushArgument(mb, q, rs);
			} else {
				q = pushArgument(mb, q, r1);
				q = pushArgument(mb, q, r2);
			}
			if (s->type == st_join2) {
				q = pushNil(mb, q, TYPE_bat);
				q = pushNil(mb, q, TYPE_bat);
			}

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
			if (s->type == st_join2)
				q = pushNil(mb, q, TYPE_lng); /* estimate */
			if (s->type == st_uselect2) {
				q = pushBit(mb, q, anti);
				if (q == NULL)
					return -1;
				s->nr = getDestVar(q);
				break;
			}
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);

			if (swapped) {
				InstrPtr r = newInstruction(mb, ASSIGNsymbol);
				if (r == NULL)
					return -1;
				getArg(r, 0) = newTmpVariable(mb, TYPE_any);
				getArg(r, 1) = getArg(q, 1);
				r->retc = 1;
				r->argc = 2;
				pushInstruction(mb, r);
				s->nr = getArg(r, 0);

				r = newInstruction(mb, ASSIGNsymbol);
				if (r == NULL)
					return -1;
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
			if (dump_2_(sql, mb, s, batRef, mergecandRef) < 0)
				return -1;
		}
			break;
		case st_tdiff:
		case st_tinter:{
			int o1, o2;
			if ((o1 = _dumpstmt(sql, mb, s->op1)) < 0)
				return -1;
			if ((o2 = _dumpstmt(sql, mb, s->op2)) < 0)
				return -1;

			q = newStmt(mb, algebraRef, s->type == st_tdiff ? subdiffRef : subinterRef);
			q = pushArgument(mb, q, o1); /* left */
			q = pushArgument(mb, q, o2); /* right */
			q = pushNil(mb, q, TYPE_bat); /* left candidate */
			q = pushNil(mb, q, TYPE_bat); /* right candidate */
			q = pushBit(mb, q, FALSE);    /* nil matches */
			q = pushNil(mb, q, TYPE_lng); /* estimate */
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);
		}
			break;
		case st_join:{
			int l;
			int r;
			int cmp = s->flag;
			int left = (cmp == cmp_left);
			char *sjt = "subjoin";

			if (left) {
				cmp = cmp_equal;
				sjt = "subleftjoin";
			}
			if ((l = _dumpstmt(sql, mb, s->op1)) < 0)
				return -1;
			if ((r = _dumpstmt(sql, mb, s->op2)) < 0)
				return -1;
			assert(l >= 0 && r >= 0);

			if (cmp == cmp_joined) {
				s->nr = l;
				return s->nr;
			}
			if (cmp == cmp_left_project) {
				int op3;
				if ((op3 = _dumpstmt(sql, mb, s->op3)) < 0)
					return -1;
				q = newStmt(mb, sqlRef, projectRef);
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				q = pushArgument(mb, q, op3);
				if (q == NULL)
					return -1;
				s->nr = getDestVar(q);
				return s->nr;
			}
			if (cmp == cmp_project) {
				int ins;

				/* delta bat */
				if (s->op3) {
					char nme[IDLENGTH];
					int uval = -1;

					snprintf(nme, IDLENGTH, "r1_%d", r);
					uval = findVariable(mb, nme);
					assert(uval >= 0);

					if ((ins = _dumpstmt(sql, mb, s->op3)) < 0)
						return -1;
					q = newStmt(mb, sqlRef, deltaRef);
					q = pushArgument(mb, q, l);
					q = pushArgument(mb, q, r);
					q = pushArgument(mb, q, uval);
					q = pushArgument(mb, q, ins);
					if (q == NULL)
						return -1;
					s->nr = getDestVar(q);
					return s->nr;
				}
				/* projections, ie left is void headed */
				q = newStmt(mb, algebraRef, projectionRef);
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				if (q == NULL)
					return -1;
				s->nr = getDestVar(q);
				if (cmp == cmp_project && s->key) {
					q = newStmt(mb, batRef, putName("setKey"));
					q = pushArgument(mb, q, s->nr);
					q = pushBit(mb, q, TRUE);
					s->nr = getDestVar(q);
				}
				return s->nr;
			}


			switch (cmp) {
			case cmp_equal:
				q = newStmt(mb, algebraRef, sjt);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				q = pushNil(mb, q, TYPE_bat);
				q = pushNil(mb, q, TYPE_bat);
				q = pushBit(mb, q, FALSE);
				q = pushNil(mb, q, TYPE_lng);
				if (q == NULL)
					return -1;
				break;
			case cmp_equal_nil: /* nil == nil */
				q = newStmt(mb, algebraRef, sjt);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				q = pushNil(mb, q, TYPE_bat);
				q = pushNil(mb, q, TYPE_bat);
				q = pushBit(mb, q, TRUE);
				q = pushNil(mb, q, TYPE_lng);
				if (q == NULL)
					return -1;
				break;
			case cmp_notequal:
				q = newStmt(mb, algebraRef, subantijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				q = pushNil(mb, q, TYPE_bat);
				q = pushNil(mb, q, TYPE_bat);
				q = pushBit(mb, q, FALSE);
				q = pushNil(mb, q, TYPE_lng);
				if (q == NULL)
					return -1;
				break;
			case cmp_lt:
			case cmp_lte:
			case cmp_gt:
			case cmp_gte:
				q = newStmt(mb, algebraRef, subthetajoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				q = pushNil(mb, q, TYPE_bat);
				q = pushNil(mb, q, TYPE_bat);
				if (cmp == cmp_lt)
					q = pushInt(mb, q, -1);
				else if (cmp == cmp_lte)
					q = pushInt(mb, q, -2);
				else if (cmp == cmp_gt)
					q = pushInt(mb, q, 1);
				else if (cmp == cmp_gte)
					q = pushInt(mb, q, 2);
				q = pushBit(mb, q, TRUE);
				q = pushNil(mb, q, TYPE_lng);
				if (q == NULL)
					return -1;
				break;
			case cmp_all:	/* aka cross table */
				q = newStmt(mb, algebraRef, crossRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				if (q == NULL)
					return -1;
				break;
			case cmp_project:
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
			int cnt = 0, ext = 0, grp = 0, o1;

			if ((o1 = _dumpstmt(sql, mb, s->op1)) < 0)
				return -1;
			if (s->op2) {
				if ((grp = _dumpstmt(sql, mb, s->op2)) < 0)
					return -1;
				if ((ext = _dumpstmt(sql, mb, s->op3)) < 0)
					return -1;
				if ((cnt = _dumpstmt(sql, mb, s->op4.stval)) < 0)
					return -1;
			}

			q = newStmt(mb, groupRef, s->flag & GRP_DONE ? subgroupdoneRef : subgroupRef);

			/* output variables extend and hist */
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, o1);
			if (grp)
				q = pushArgument(mb, q, grp);
			if (q == NULL)
				return -1;

			s->nr = getDestVar(q);

			/* rename second result */
			ext = getArg(q, 1);
			renameVariable(mb, ext, "r1_%d", s->nr);

			/* rename 3rd result */
			cnt = getArg(q, 2);
			renameVariable(mb, cnt, "r2_%d", s->nr);

		} break;
		case st_result:{
			int l;

			if ((l = _dumpstmt(sql, mb, s->op1)) < 0)
				return -1;

			if (s->op1->type == st_join && s->op1->flag == cmp_joined) {
				s->nr = l;
				if (s->flag)
					s->nr = s->op1->op2->nr;
			} else if (s->flag) {
				char nme[IDLENGTH];
				int v = -1;

				snprintf(nme, IDLENGTH, "r%d_%d", s->flag, l);
				v = findVariable(mb, nme);
				assert(v >= 0);

				s->nr = v;
			} else {
				s->nr = l;
			}
		}
			break;
		case st_convert:{
			list *types = s->op4.lval;
			sql_subtype *f = types->h->data;
			sql_subtype *t = types->t->data;
			char *convert = t->type->base.name;
			/* convert types and make sure they are rounded up correctly */
			int l;

			if ((l = _dumpstmt(sql, mb, s->op1)) < 0)
				return -1;

			if (t->type->localtype == f->type->localtype && (t->type->eclass == f->type->eclass || (EC_VARCHAR(f->type->eclass) && EC_VARCHAR(t->type->eclass))) && !EC_INTERVAL(f->type->eclass) && f->type->eclass != EC_DEC &&
			    (t->digits == 0 || f->digits == t->digits)
				) {
				s->nr = l;
				break;
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
			if (s->nrcols == 0) {	/* simple calc */
				q = newStmt(mb, calcRef, convert);
			} else if (s->nrcols > 0 &&
				   (t->type->localtype > TYPE_str || f->type->eclass == EC_DEC || t->type->eclass == EC_DEC || EC_INTERVAL(t->type->eclass) || EC_TEMP(t->type->eclass) ||
				    (EC_VARCHAR(t->type->eclass) && !(f->type->eclass == EC_STRING && t->digits == 0)))) {
				int type = t->type->localtype;

				q = newStmt(mb, malRef, multiplexRef);
				if (q == NULL)
					return -1;
				setVarType(mb, getArg(q, 0), newBatType(type));
				setVarUDFtype(mb, getArg(q, 0));
				q = pushStr(mb, q, convertMultiplexMod(calcRef, convert));
				q = pushStr(mb, q, convertMultiplexFcn(convert));
			} else
				q = newStmt(mb, batcalcRef, convert);

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
/*				if(sql->mvc->argc > 1)
					f->scale = ((ValRecord)((atom*)((mvc*)sql->mvc)->args[1])->data).val.ival;
				
				q = pushInt(mb, q, f->digits);
				q = pushInt(mb, q, f->scale);
*/				//q = pushInt(mb, q, ((ValRecord)((atom*)((mvc*)sql->mvc)->args[1])->data).val.ival);
			}
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);
			break;
		}
		case st_Nop:{
			char *mod, *fimp;
			sql_subtype *tpe = NULL;
			int special = 0;
			sql_subfunc *f = s->op4.funcval;
			node *n;
			/* dump operands */
			if (_dumpstmt(sql, mb, s->op1) < 0)
				return -1;

			if (backend_create_subfunc(sql, f, s->op1->op4.lval) < 0)
				return -1;
			mod = sql_func_mod(f->func);
			fimp = sql_func_imp(f->func);
			if (s->nrcols) {
				sql_subtype *res = f->res->h->data;
				fimp = convertMultiplexFcn(fimp);
				q = NULL;
				if (strcmp(fimp, "rotate_xor_hash") == 0 &&
				    strcmp(mod, calcRef) == 0 &&
				    (q = newStmt(mb, mkeyRef, putName("bulk_rotate_xor_hash"))) == NULL)
					return -1;
				if (!q) {
					if (f->func->type == F_UNION)
						q = newStmt(mb, batmalRef, multiplexRef);
					else
						q = newStmt(mb, malRef, multiplexRef);
					if (q == NULL)
						return -1;
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
			if (f->func->lang == FUNC_LANG_R || f->func->lang == FUNC_LANG_PY || f->func->lang == FUNC_LANG_MAP_PY)
				q = pushStr(mb, q, f->func->query);
			/* first dynamic output of copy* functions */
			if (f->func->type == F_UNION || (f->func->type == F_LOADER && f->res != NULL))
				q = table_func_create_result(mb, q, f->func, f->res);
			if (list_length(s->op1->op4.lval))
				tpe = tail_type(s->op1->op4.lval->h->data);
			if (strcmp(fimp, "round") == 0 && tpe && tpe->type->eclass == EC_DEC)
				special = 1;

			for (n = s->op1->op4.lval->h; n; n = n->next) {
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
			if (q == NULL)
				return -1;
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
			if (s->op1 && _dumpstmt(sql, mb, s->op1) < 0)
				return -1;
			if (monet5_create_relational_function(sql->mvc, mod, fimp, rel, s, 1) < 0)
				 return -1;

			if (s->flag) 
				q = newStmt(mb, batmalRef, multiplexRef);
			else
				q = newStmt(mb, mod, fimp);
			q = relational_func_create_result(sql->mvc, mb, q, rel);
			if (s->flag) {
				q = pushStr(mb, q, mod);
				q = pushStr(mb, q, fimp);
			}
			if (s->op1)
				for (n = s->op1->op4.lval->h; n; n = n->next) {
					stmt *op = n->data;

					q = pushArgument(mb, q, op->nr);
				}
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);
			/* keep reference to instruction */
			s->rewritten = (void *) q;
		} break;
		case st_aggr:{
			int no_nil = s->flag;
			int g = 0, e = 0, l = _dumpstmt(sql, mb, s->op1);	/* maybe a list */
			char *mod, *aggrfunc;
			char aggrF[64];
			sql_subtype *res = s->op4.aggrval->res->h->data;
			int restype = res->type->localtype;
			int complex_aggr = 0;
			int abort_on_error, i, *stmt_nr = NULL;

			if (l < 0)
				return -1;
			if (backend_create_subaggr(sql, s->op4.aggrval) < 0)
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
				if ((g = _dumpstmt(sql, mb, s->op2)) < 0)
					return -1;
				if ((e = _dumpstmt(sql, mb, s->op3)) < 0)
					return -1;

				q = newStmt(mb, mod, aggrfunc);
				if (q == NULL)
					return -1;
				setVarType(mb, getArg(q, 0), newBatType(restype));
				setVarUDFtype(mb, getArg(q, 0));
			} else {
				q = newStmt(mb, mod, aggrfunc);
				if (q == NULL)
					return -1;
				if (complex_aggr) {
					setVarType(mb, getArg(q, 0), restype);
					setVarUDFtype(mb, getArg(q, 0));
				}
			}

			if (LANG_EXT(s->op4.aggrval->aggr->lang))
				q = pushPtr(mb, q, s->op4.aggrval->aggr);
			if (s->op4.aggrval->aggr->lang == FUNC_LANG_R || s->op4.aggrval->aggr->lang == FUNC_LANG_PY || s->op4.aggrval->aggr->lang == FUNC_LANG_MAP_PY){
				if (!g) {
					setVarType(mb, getArg(q, 0), restype);
					setVarUDFtype(mb, getArg(q, 0));
				}
				q = pushStr(mb, q, s->op4.aggrval->aggr->query);
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
				if (q == NULL)
					return -1;
				g = getDestVar(q);
				q = pushBit(mb, q, no_nil);
				if (abort_on_error)
					q = pushBit(mb, q, TRUE);
			} else if (no_nil && strncmp(aggrfunc, "count", 5) == 0) 
				q = pushBit(mb, q, no_nil);
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);
		}
			break;
		case st_atom:{
			atom *a = s->op4.aval;
			q = newStmt(mb, calcRef, atom_type(a)->type->base.name);
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
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);
		}
			break;
		case st_append:{
			int l = 0;
			int r;

			if ((r = _dumpstmt(sql, mb, s->op2)) < 0)
				return -1;
			if ((l = _dumpstmt(sql, mb, s->op1)) < 0)
				return -1;
			q = newStmt(mb, batRef, appendRef);
			q = pushArgument(mb, q, l);
			q = pushArgument(mb, q, r);
			q = pushBit(mb, q, TRUE);
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);
		} break;
		case st_update_col:
		case st_append_col:{
			int tids = _dumpstmt(sql, mb, s->op1), upd = 0;
			sql_column *c = s->op4.cval;
			char *n = (s->type == st_append_col) ? appendRef : updateRef;

			if (tids < 0)
				return -1;
			if (s->op2 && (upd = _dumpstmt(sql, mb, s->op2)) < 0)
				return -1;
			if (s->type == st_append_col && s->flag) {	/* fake append */
				s->nr = tids;
			} else {
				q = newStmt(mb, sqlRef, n);
				q = pushArgument(mb, q, sql->mvc_var);
				if (q == NULL)
					return -1;
				getArg(q, 0) = sql->mvc_var = newTmpVariable(mb, TYPE_int);
				q = pushSchema(mb, q, c->t);
				q = pushStr(mb, q, c->t->base.name);
				q = pushStr(mb, q, c->base.name);
				q = pushArgument(mb, q, tids);
				if (s->op2)
					q = pushArgument(mb, q, upd);
				if (q == NULL)
					return -1;
				sql->mvc_var = s->nr = getDestVar(q);
			}
		}
			break;

		case st_update_idx:
		case st_append_idx:{
			int tids = _dumpstmt(sql, mb, s->op1), upd = 0;
			sql_idx *i = s->op4.idxval;
			char *n = (s->type == st_append_idx) ? appendRef : updateRef;

			if (tids < 0)
				return -1;
			if (s->op2 && (upd = _dumpstmt(sql, mb, s->op2)) < 0)
				return -1;
			q = newStmt(mb, sqlRef, n);
			q = pushArgument(mb, q, sql->mvc_var);
			if (q == NULL)
				return -1;
			getArg(q, 0) = sql->mvc_var = newTmpVariable(mb, TYPE_int);
			q = pushSchema(mb, q, i->t);
			q = pushStr(mb, q, i->t->base.name);
			q = pushStr(mb, q, sa_strconcat(sql->mvc->sa, "%", i->base.name));
			q = pushArgument(mb, q, tids);
			if (s->op2)
				q = pushArgument(mb, q, upd);
			if (q == NULL)
				return -1;
			sql->mvc_var = s->nr = getDestVar(q);
		}
			break;
		case st_delete:{
			int r = _dumpstmt(sql, mb, s->op1);
			sql_table *t = s->op4.tval;
			str mod = sqlRef;

			if (r < 0)
				return -1;
			q = newStmt(mb, mod, deleteRef);
			q = pushArgument(mb, q, sql->mvc_var);
			if (q == NULL)
				return -1;
			getArg(q, 0) = sql->mvc_var = newTmpVariable(mb, TYPE_int);
			q = pushSchema(mb, q, t);
			q = pushStr(mb, q, t->base.name);
			q = pushArgument(mb, q, r);
			if (q == NULL)
				return -1;
			sql->mvc_var = s->nr = getDestVar(q);
		} break;
		case st_table_clear:{
			sql_table *t = s->op4.tval;
			str mod = sqlRef;

			q = newStmt(mb, mod, clear_tableRef);
			q = pushSchema(mb, q, t);
			q = pushStr(mb, q, t->base.name);
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);
		} break;
		case st_exception:{
			int l, r;

			if ((l = _dumpstmt(sql, mb, s->op1)) < 0)
				return -1;
			if ((r = _dumpstmt(sql, mb, s->op2)) < 0)
				return -1;

			/* if(bit(l)) { error(r);}  ==raising an exception */
			q = newStmt(mb, sqlRef, assertRef);
			q = pushArgument(mb, q, l);
			q = pushArgument(mb, q, r);
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);
			break;
		}
		case st_trans:{
			int l, r = -1;

			if ((l = _dumpstmt(sql, mb, s->op1)) < 0)
				return -1;
			if (s->op2 && (r = _dumpstmt(sql, mb, s->op2)) < 0)
				return -1;
			q = newStmt(mb, sqlRef, "trans");
			q = pushInt(mb, q, s->flag);
			q = pushArgument(mb, q, l);
			if (r > 0)
				q = pushArgument(mb, q, r);
			else
				q = pushNil(mb, q, TYPE_str);
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);
			break;
		}
		case st_catalog:{
			if (_dumpstmt(sql, mb, s->op1) < 0)
				return -1;

			q = newStmt(mb, sqlRef, catalogRef);
			q = pushInt(mb, q, s->flag);
			for (n = s->op1->op4.lval->h; n; n = n->next) {
				stmt *c = n->data;
				q = pushArgument(mb, q, c->nr);
			}
			if (q == NULL)
				return -1;
			s->nr = getDestVar(q);
			break;
		}
		case st_alias:
			if ((s->nr = _dumpstmt(sql, mb, s->op1)) < 0)
				return -1;
			break;
		case st_list:
			for (n = s->op4.lval->h; n; n = n->next) {
				if (_dumpstmt(sql, mb, n->data) < 0)
					return -1;
			}
			s->nr = 1;
			break;
		case st_rs_column:{
			if (_dumpstmt(sql, mb, s->op1) < 0)
				return -1;
			q = (void *) s->op1->rewritten;
			s->nr = getArg(q, s->flag);
		} break;
		case st_affected_rows:{
			InstrPtr q;
			int o1 = _dumpstmt(sql, mb, s->op1);

			if (o1 < 0)
				return -1;
			q = newStmt(mb, sqlRef, affectedRowsRef);
			q = pushArgument(mb, q, sql->mvc_var);
			if (q == NULL)
				return -1;
			getArg(q, 0) = sql->mvc_var = newTmpVariable(mb, TYPE_int);
			q = pushArgument(mb, q, o1);
			if (q == NULL)
				return -1;
			sql->mvc_var = s->nr = getDestVar(q);
		} break;
		case st_output:{
			stmt *lst = s->op1;

			if (_dumpstmt(sql, mb, lst) < 0)
				return -1;

			if (lst->type == st_list) {
				list *l = lst->op4.lval;
				int cnt = list_length(l);
				stmt *first;

				n = l->h;
				first = n->data;

				/* single value result, has a fast exit */
				if (cnt == 1 && first->nrcols <= 0 ){
					stmt *c = n->data;
					sql_subtype *t = tail_type(c);
					const char *tname = table_name(sql->mvc->sa, c);
					const char *sname = schema_name(sql->mvc->sa, c);
					const char *_empty = "";
					const char *tn = (tname) ? tname : _empty;
					const char *sn = (sname) ? sname : _empty;
					const char *cn = column_name(sql->mvc->sa, c);
					const char *ntn = sql_escape_ident(tn);
					const char *nsn = sql_escape_ident(sn);
					size_t fqtnl = strlen(ntn) + 1 + strlen(nsn) + 1;
					char *fqtn = NEW_ARRAY(char, fqtnl);

					snprintf(fqtn, fqtnl, "%s.%s", nsn, ntn);

					q = newStmt(mb, sqlRef, resultSetRef);
					if (q) {
						s->nr = getDestVar(q);
						q = pushStr(mb, q, fqtn);
						q = pushStr(mb, q, cn);
						q = pushStr(mb, q, t->type->localtype == TYPE_void ? "char" : t->type->sqlname);
						q = pushInt(mb, q, t->digits);
						q = pushInt(mb, q, t->scale);
						q = pushInt(mb, q, t->type->eclass);
						q = pushArgument(mb, q, c->nr);
					}

					c_delete(ntn);
					c_delete(nsn);
					_DELETE(fqtn);
					if (q == NULL)
						return -1;
					break;
				}
				if ( (s->nr =dump_header(sql->mvc, mb, s, l)) < 0)
					return -1;

			} else {
				q = newStmt(mb, sqlRef, raiseRef);
				q = pushStr(mb, q, "not a valid output list\n");
				if (q == NULL)
					return -1;
				s->nr = 1;
			}
		}
		break;
		case st_export:{
			stmt *lst = s->op1;
			char *sep = NULL;
			char *rsep = NULL;
			char *ssep = NULL;
			char *ns = NULL;

			if (_dumpstmt(sql, mb, lst) < 0)
				return -1;

			if (lst->type == st_list) {
				list *l = lst->op4.lval;
				int file = -1 ;

				n = s->op4.lval->h;
				sep = n->data;
				rsep = n->next->data;
				ssep = n->next->next->data;
				ns = n->next->next->next->data;

				if (s->type == st_export && s->op2) {
					if ((file = _dumpstmt(sql, mb, s->op2)) < 0)
						return -1;
				}  else {
					q= newAssignment(mb);
					q = pushStr(mb,q,"stdout");
					file = getArg(q,0);
				}
				if ( (s->nr =dump_export_header(sql->mvc, mb, l, file, "csv", sep,rsep,ssep,ns)) < 0)
					return -1;
			} else {
				q = newStmt(mb, sqlRef, raiseRef);
				q = pushStr(mb, q, "not a valid output list\n");
				if (q == NULL)
					return -1;
				s->nr = 1;
			}
		}
		break;

		case st_table:{
			stmt *lst = s->op1;

			if (_dumpstmt(sql, mb, lst) < 0)
				return -1;

			if (lst->type != st_list) {
				q = newStmt(mb, sqlRef, printRef);
				q = pushStr(mb, q, "not a valid output list\n");
				if (q == NULL)
					return -1;
			}
			s->nr = 1;
		}
			break;


		case st_cond:{
			int c = _dumpstmt(sql, mb, s->op1);

			if (c < 0)
				return -1;
			if (!s->flag) {	/* if */
				q = newAssignment(mb);
				if (q == NULL)
					return -1;
				q->barrier = BARRIERsymbol;
				q = pushArgument(mb, q, c);
				if (q == NULL)
					return -1;
				s->nr = getArg(q, 0);
			} else {	/* while */
				int outer = _dumpstmt(sql, mb, s->op2);

				if (outer < 0)
					return -1;
				/* leave barrier */
				q = newStmt(mb, calcRef, notRef);
				q = pushArgument(mb, q, c);
				if (q == NULL)
					return -1;
				c = getArg(q, 0);

				q = newAssignment(mb);
				if (q == NULL)
					return -1;
				getArg(q, 0) = outer;
				q->barrier = LEAVEsymbol;
				q = pushArgument(mb, q, c);
				if (q == NULL)
					return -1;
				s->nr = outer;
			}
		} break;
		case st_control_end:{
			int c = _dumpstmt(sql, mb, s->op1);

			if (c < 0)
				return -1;
			if (s->op1->flag) {	/* while */
				/* redo barrier */
				q = newAssignment(mb);
				if (q == NULL)
					return -1;
				getArg(q, 0) = c;
				q->argc = q->retc = 1;
				q->barrier = REDOsymbol;
				q = pushBit(mb, q, TRUE);
				if (q == NULL)
					return -1;
			} else {
				q = newAssignment(mb);
				if (q == NULL)
					return -1;
				getArg(q, 0) = c;
				q->argc = q->retc = 1;
				q->barrier = EXITsymbol;
			}
			q = newStmt(mb, sqlRef, mvcRef);
			if (q == NULL)
				return -1;
			sql->mvc_var = getDestVar(q);
			s->nr = getArg(q, 0);
		}
			break;
		case st_return:{
			int c = _dumpstmt(sql, mb, s->op1);

			if (c < 0)
				return -1;
			if (s->flag) {	/* drop declared tables */
				InstrPtr k = newStmt(mb, sqlRef, "dropDeclaredTables");
				(void) pushInt(mb, k, s->flag);
			}
			q = newInstruction(mb, RETURNsymbol);
			if (q == NULL)
				return -1;
			if (s->op1->type == st_table) {
				list *l = s->op1->op1->op4.lval;

				q = dump_cols(mb, l, q);
			} else {
				getArg(q, 0) = getArg(getInstrPtr(mb, 0), 0);
				q = pushArgument(mb, q, c);
			}
			if (q == NULL)
				return -1;
			pushInstruction(mb, q);
			s->nr = 1;
		}
			break;
		case st_assign:{
			int r = -1;

			if (s->op2 && (r = _dumpstmt(sql, mb, s->op2)) < 0)
				return -1;
			if (!VAR_GLOBAL(s->flag)) {	/* globals */
				char *buf;
				char *vn = atom2string(sql->mvc->sa, s->op1->op4.aval);

				if (!s->op2) {
					/* drop declared table */
					s->nr = drop_table(mb, vn);
					if (s->nr < 0)
						return -1;
					break;
				}
				buf = GDKmalloc(MAXIDENTLEN);
				if (buf == NULL)
					return -1;
				(void) snprintf(buf, MAXIDENTLEN, "A%s", vn);
				q = newInstruction(mb, ASSIGNsymbol);
				if (q == NULL) {
					GDKfree(buf);
					return -1;
				}
				q->argc = q->retc = 0;
				q = pushArgumentId(mb, q, buf);
				GDKfree(buf);
				if (q == NULL)
					return -1;
				pushInstruction(mb, q);
				if (mb->errors)
					return -1;
				q->retc++;
				s->nr = 1;
			} else {
				int vn = _dumpstmt(sql, mb, s->op1);
				if (vn < 0)
					return -1;
				q = newStmt(mb, sqlRef, setVariableRef);
				q = pushArgument(mb, q, sql->mvc_var);
				q = pushArgument(mb, q, vn);
				if (q == NULL)
					return -1;
				getArg(q, 0) = sql->mvc_var = newTmpVariable(mb, TYPE_int);
				sql->mvc_var = s->nr = getDestVar(q);
			}
			q = pushArgument(mb, q, r);
			if (q == NULL)
				return -1;
		} break;
		}
		if (mb->errors)
			return -1;

		return s->nr;
	}

	return (0);
}

/*
 * The kernel uses two calls to procedures defined in SQL.
 * They have to be initialized, which is currently hacked
 * by using the SQLstatment.
 */

int
backend_dumpstmt(backend *be, MalBlkPtr mb, stmt *s, int top, int add_end, char *query)
{
	mvc *c = be->mvc;
	stmt **stmts = stmt_array(c->sa, s);
	InstrPtr q;
	int old_mv = be->mvc_var, nr = 0;

	// Always keep the SQL query around for monitoring
	if (query) {
		char *t, *tt;
		InstrPtr q;

		tt = t = GDKstrdup(query);
		while (t && isspace((int) *t))
			t++;

		q = newStmt(mb, querylogRef, defineRef);
		if (q == NULL) {
			GDKfree(tt);
			return -1;
		}
		q->token = REMsymbol;	// will be patched
		setVarType(mb, getArg(q, 0), TYPE_void);
		setVarUDFtype(mb, getArg(q, 0));
		q = pushStr(mb, q, t);
		GDKfree(tt);
		q = pushStr(mb, q, getSQLoptimizer(be->mvc));
	}

	/* announce the transaction mode */
	q = newStmt(mb, sqlRef, "mvc");
	if (q == NULL)
		return -1;
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
		q = newStmt(mb, sqlRef, exportOperationRef);
		if (q == NULL)
			return -1;
	}
	/* generate a dummy return assignment for functions */
	if (getArgType(mb, getInstrPtr(mb, 0), 0) != TYPE_void && getInstrPtr(mb, mb->stop - 1)->barrier != RETURNsymbol) {
		q = newAssignment(mb);
		if (q == NULL)
			return -1;
		getArg(q, 0) = getArg(getInstrPtr(mb, 0), 0);
		q->barrier = RETURNsymbol;
	}
	if (add_end)
		pushEndInstruction(mb);
	return 0;
}

/* Generate the assignments of the query arguments to the query template*/
int
backend_callinline(backend *be, Client c)
{
	mvc *m = be->mvc;
	InstrPtr curInstr = 0;
	MalBlkPtr curBlk = c->curprg->def;

	if (m->argc) {	
		int argc = 0;

		for (; argc < m->argc; argc++) {
			atom *a = m->args[argc];
			int type = atom_type(a)->type->localtype;
			int varid = 0;

			curInstr = newAssignment(curBlk);
			if (curInstr == NULL)
				return -1;
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
	c->curprg->def = curBlk;
	return 0;
}

/* SQL procedures, functions and PREPARE statements are compiled into a parameterised plan */
Symbol
backend_dumpproc(backend *be, Client c, cq *cq, stmt *s)
{
	mvc *m = be->mvc;
	MalBlkPtr mb = 0;
	Symbol curPrg = 0, backup = NULL;
	InstrPtr curInstr = 0;
	int argc = 0;
	char arg[IDLENGTH];
	node *n;

	backup = c->curprg;
	if (cq)
		c->curprg = newFunction(userRef, putName(cq->name), FUNCTIONsymbol);
	else
		c->curprg = newFunction(userRef, "tmp", FUNCTIONsymbol);
	if (c->curprg == NULL)
		return NULL;

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

			snprintf(arg, IDLENGTH, "A%d", argc);
			a->varid = varid = newVariable(mb, arg,strlen(arg), type);
			curInstr = pushArgument(mb, curInstr, varid);
			assert(curInstr);
			if (curInstr == NULL) 
				goto cleanup;
			setVarType(mb, varid, type);
			setVarUDFtype(mb, 0);
		}
	} else if (m->params) {	/* needed for prepare statements */

		for (n = m->params->h; n; n = n->next, argc++) {
			sql_arg *a = n->data;
			int type = a->type.type->localtype;
			int varid = 0;

			snprintf(arg, IDLENGTH, "A%d", argc);
			varid = newVariable(mb, arg,strlen(arg), type);
			curInstr = pushArgument(mb, curInstr, varid);
			assert(curInstr);
			if (curInstr == NULL) 
				goto cleanup;
			setVarType(mb, varid, type);
			setVarUDFtype(mb, varid);
		}
	}

	if (backend_dumpstmt(be, mb, s, 1, 1, be->q?be->q->codestring:NULL) < 0) 
		goto cleanup;

	if (cq){
		SQLaddQueryToCache(c);
		// optimize this code the 'old' way
		if ( m->emode == m_prepare || !qc_isaquerytemplate(getFunctionId(getInstrPtr(c->curprg->def,0))) )
			SQLoptimizeFunction(c,c->curprg->def);
	}

	// restore the context for the wrapper code
	curPrg = c->curprg;
	if (backup)
		c->curprg = backup;
	return curPrg;

cleanup:
	freeSymbol(curPrg);
	if (backup)
		c->curprg = backup;
	return NULL;
}

void
backend_call(backend *be, Client c, cq *cq)
{
	mvc *m = be->mvc;
	InstrPtr q;
	MalBlkPtr mb = c->curprg->def;

	q = newStmt(mb, userRef, cq->name);
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

			if (!atom_cast(m->sa, a, pt)) {
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

	for (m = findModule(c->nspace, f->mod); m; m = m->link) {
		if (strcmp(m->name, f->mod) == 0) {
			Symbol s = m->space[(int) (getSymbolIndex(f->imp))];
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

static int
backend_create_r_func(backend *be, sql_func *f)
{
	(void)be;
	switch(f->type) {
	case  F_AGGR:
		f->mod = "rapi";
		f->imp = "eval_aggr";
		break;
	case  F_PROC: /* no output */
	case  F_FUNC:
	default: /* ie also F_FILT and F_UNION for now */
		f->mod = "rapi";
		f->imp = "eval";
		break;
	}
	return 0;
}

/* Create the MAL block for a registered function and optimize it */
static int
backend_create_py_func(backend *be, sql_func *f)
{
	(void)be;
	switch(f->type) {
	case  F_AGGR:
		f->mod = "pyapi";
		f->imp = "eval_aggr";
		break;
	case F_LOADER:
		f->mod = "pyapi";
		f->imp = "eval_loader";
		break;
	case  F_PROC: /* no output */
	case  F_FUNC:
	default: /* ie also F_FILT and F_UNION for now */
		f->mod = "pyapi";
		f->imp = "eval";
		break;
	}
	return 0;
}

static int
backend_create_map_py_func(backend *be, sql_func *f)
{
	(void)be;
	switch(f->type) {
	case  F_AGGR:
		f->mod = "pyapimap";
		f->imp = "eval_aggr";
		break;
	case  F_PROC: /* no output */
	case  F_FUNC:
	default: /* ie also F_FILT and F_UNION for now */
		f->mod = "pyapimap";
		f->imp = "eval";
		break;
	}
	return 0;
}

static int
backend_create_sql_func(backend *be, sql_func *f, list *restypes, list *ops)
{
	mvc *m = be->mvc;
	sql_schema *schema = m->session->schema;
	MalBlkPtr curBlk = NULL;
	InstrPtr curInstr = NULL;
	Client c = be->client;
	Symbol backup = NULL, curPrg = NULL;
	stmt *s;
	int i, retseen = 0, sideeffects = 0, vararg = (f->varres || f->vararg), no_inline = 0;
	sql_allocator *sa;

	/* nothing to do for internal and ready (not recompiling) functions */
	if (!f->sql || (!vararg && f->sql > 1))
		return 0;
	if (!vararg)
		f->sql++;
	sa = sa_create();
	m->session->schema = f->s;
	s = sql_parse(m, sa, f->query, m_instantiate);
	m->session->schema = schema;
	if (s && !f->sql) {	/* native function */
		sa_destroy(sa);
		return 0;
	}

	if (!s) {
		if (!vararg)
			f->sql--;
		sa_destroy(sa);
		return -1;
	}
	assert(s);

	backup = c->curprg;
	curPrg = c->curprg = newFunction(userRef, putName(f->base.name), FUNCTIONsymbol);
	if( curPrg == NULL)
		goto cleanup;

	curBlk = c->curprg->def;
	curInstr = getInstrPtr(curBlk, 0);

	if (f->res) {
		sql_arg *res = f->res->h->data;
		if (f->type == F_UNION)
			curInstr = table_func_create_result(curBlk, curInstr, f, restypes);
		else
			setVarType(curBlk, 0, res->type.type->localtype);
	} else {
		setVarType(curBlk, 0, TYPE_void);
	}
	setVarUDFtype(curBlk, 0);

	if (f->vararg && ops) {
		int argc = 0;
		node *n;

		for (n = ops->h; n; n = n->next, argc++) {
			stmt *s = n->data;
			int type = tail_type(s)->type->localtype;
			int varid = 0;
			char buf[IDLENGTH];

			(void) snprintf(buf, IDLENGTH, "A%d", argc);
			varid = newVariable(curBlk, buf, strlen(buf), type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
			setVarUDFtype(curBlk, varid);
		}
	} else if (f->ops) {
		int argc = 0;
		node *n;

		for (n = f->ops->h; n; n = n->next, argc++) {
			sql_arg *a = n->data;
			int type = a->type.type->localtype;
			int varid = 0;
			char buf[IDLENGTH];

			if (a->name)
				(void) snprintf(buf, IDLENGTH, "A%s", a->name);
			else
				(void) snprintf(buf, IDLENGTH, "A%d", argc);
			varid = newVariable(curBlk, buf, strlen(buf), type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
			setVarUDFtype(curBlk, varid);
		}
	}
	/* announce the transaction mode */

	if (backend_dumpstmt(be, curBlk, s, 0, 1, NULL) < 0) 
		goto cleanup;
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
		no_inline |= (getModuleId(p) == malRef && getFunctionId(p) == multiplexRef);
		if (p->token == RETURNsymbol || p->token == YIELDsymbol || p->barrier == RETURNsymbol || p->barrier == YIELDsymbol)
			retseen++;
	}
	if (i == curBlk->stop && retseen == 1 && f->type != F_UNION && !no_inline)
		curBlk->inlineProp =1;
	if (sideeffects)
		curBlk->unsafeProp = 1;
	f->sa = sa;
	sa_register(sa);
	/* optimize the code */
	SQLaddQueryToCache(c);
	if( curBlk->inlineProp == 0)
		SQLoptimizeFunction(c, c->curprg->def);
	else{
		chkProgram(c->fdout, c->nspace, c->curprg->def);
		SQLoptimizeFunction(c,c->curprg->def);
	}
	if (backup)
		c->curprg = backup;
	return 0;
cleanup:
	freeSymbol(curPrg);
	sa_destroy(sa);
	if (backup)
		c->curprg = backup;
	return -1;

}

/* TODO handle aggr */
int
backend_create_func(backend *be, sql_func *f, list *restypes, list *ops)
{
	switch(f->lang) {
	case FUNC_LANG_INT:
	case FUNC_LANG_MAL:
	case FUNC_LANG_SQL:
		return backend_create_sql_func(be, f, restypes, ops);
	case FUNC_LANG_R:
		return backend_create_r_func(be, f);
	case FUNC_LANG_PY:
		return backend_create_py_func(be, f);
	case FUNC_LANG_MAP_PY:
		return backend_create_map_py_func(be, f);
	case FUNC_LANG_C:
	case FUNC_LANG_J:
	default:
		return -1;
	}
}

static int
backend_create_subfunc(backend *be, sql_subfunc *f, list *ops)
{
	return backend_create_func(be, f->func, f->res, ops);
}

static int
backend_create_subaggr(backend *be, sql_subaggr *f)
{
	return backend_create_func(be, f->aggr, f->res, NULL);
}

void
_rel_print(mvc *sql, sql_rel *rel) 
{
	list *refs = sa_list(sql->sa);
	rel_print_refs(sql, GDKstdout, rel, 0, refs, 1);
	rel_print_(sql, GDKstdout, rel, 0, refs, 1);
	mnstr_printf(GDKstdout, "\n");
}

void
rel_print(mvc *sql, sql_rel *rel, int depth) 
{
	list *refs = sa_list(sql->sa);
	size_t pos;
	size_t nl = 0;
	size_t len = 0, lastpos = 0;
	stream *fd = sql->scanner.ws;
	stream *s;
	buffer *b = buffer_create(16364); /* hopefully enough */
	if (!b)
		return; /* signal somehow? */
	s = buffer_wastream(b, "SQL Plan");
	if (!s) {
		buffer_destroy(b);
		return; /* signal somehow? */
	}

	rel_print_refs(sql, s, rel, depth, refs, 1);
	rel_print_(sql, s, rel, depth, refs, 1);
	mnstr_printf(s, "\n");

	/* count the number of lines in the output, skip the leading \n */
	for (pos = 1; pos < b->pos; pos++) {
		if (b->buf[pos] == '\n') {
			nl++;
			if (len < pos - lastpos)
				len = pos - lastpos;
			lastpos = pos + 1;
		}
	}
	b->buf[b->pos - 1] = '\0';  /* should always end with a \n, can overwrite */

	/* craft a semi-professional header */
	mnstr_printf(fd, "&1 0 " SZFMT " 1 " SZFMT "\n", /* type id rows columns tuples */
			nl, nl);
	mnstr_printf(fd, "%% .plan # table_name\n");
	mnstr_printf(fd, "%% rel # name\n");
	mnstr_printf(fd, "%% clob # type\n");
	mnstr_printf(fd, "%% " SZFMT " # length\n", len - 1 /* remove = */);

	/* output the data */
	mnstr_printf(fd, "%s\n", b->buf + 1 /* omit starting \n */);

	mnstr_close(s);
	mnstr_destroy(s);
	buffer_destroy(b);
}

