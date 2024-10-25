/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 * (author)  Author M. Kersten
 * For documentation see website
 */
#include "monetdb_config.h"
#include "mal_instruction.h"
#include "mal_function.h"		/* for getPC() */
#include "mal_utils.h"
#include "mal_exception.h"
#include "mal_private.h"

/* to avoid memory fragmentation stmt and var blocks are allocated in chunks */
#define MALCHUNK 256

/* If we encounter an error it can be left behind in the MalBlk
 * for the upper layers to abandon the track
 */
void
addMalException(MalBlkPtr mb, str msg)
{
	if (msg == NULL)
		return;
	if (mb->errors) {
		mb->errors = concatErrors(mb->errors, msg);
	} else {
		mb->errors = dupError(msg);
	}
}

Symbol
newSymbol(const char *nme, int kind)
{
	Symbol cur;

	assert(kind == COMMANDsymbol || kind == PATTERNsymbol || kind == FUNCTIONsymbol);
	if (nme == NULL)
		return NULL;
	cur = (Symbol) GDKzalloc(sizeof(SymRecord));
	if (cur == NULL)
		return NULL;
	cur->name = putName(nme);
	if (cur->name == NULL) {
		GDKfree(cur);
		return NULL;
	}
	cur->kind = kind;
	cur->peer = NULL;
	if (kind == FUNCTIONsymbol) {
		cur->def = newMalBlk(STMT_INCREMENT);
		if (cur->def == NULL) {
			GDKfree(cur);
			return NULL;
		}
	}
	return cur;
}

void
freeSymbol(Symbol s)
{
	if (s == NULL)
		return;
	if (s->def) {
		freeMalBlk(s->def);
		s->def = NULL;
	} else if (s->allocated && s->func) {
		GDKfree(s->func->comment);
		GDKfree((char*)s->func->cname);
		GDKfree(s->func->args);
		GDKfree(s->func);
	}
	GDKfree(s);
}

void
freeSymbolList(Symbol s)
{
	Symbol t = s;

	while (s) {
		t = s->peer;
		s->peer = NULL;
		freeSymbol(s);
		s = t;
	}
}

int
newMalBlkStmt(MalBlkPtr mb, int maxstmts)
{
	InstrPtr *p;
	maxstmts = maxstmts % MALCHUNK == 0 ? maxstmts : ((maxstmts / MALCHUNK) + 1) * MALCHUNK;

	p = (InstrPtr *) GDKzalloc(sizeof(InstrPtr) * maxstmts);
	if (p == NULL)
		return -1;
	mb->stmt = p;
	mb->stop = 0;
	mb->ssize = maxstmts;
	return 0;
}

MalBlkPtr
newMalBlk(int elements)
{
	MalBlkPtr mb;
	VarRecord *v;

	mb = (MalBlkPtr) GDKmalloc(sizeof(MalBlkRecord));
	if (mb == NULL)
		return NULL;

	/* each MAL instruction implies at least one variable
	 * we reserve some extra for constants */
	assert(elements >= 0);
	elements += 8;
	if (elements % MALCHUNK != 0)
		elements = (elements / MALCHUNK + 1) * MALCHUNK;
	v = (VarRecord *) GDKzalloc(sizeof(VarRecord) * elements);
	if (v == NULL) {
		GDKfree(mb);
		return NULL;
	}
	*mb = (MalBlkRecord) {
		.var = v,
		.vsize = elements,
		.maxarg = MAXARG,		/* the minimum for each instruction */
		.workers = ATOMIC_VAR_INIT(1),
	};
	if (newMalBlkStmt(mb, elements) < 0) {
		GDKfree(mb->var);
		GDKfree(mb);
		return NULL;
	}
	return mb;
}

int
resizeMalBlk(MalBlkPtr mb, int elements)
{
	int i;
	assert(elements >= 0);
	if (elements % MALCHUNK != 0)
		elements = (elements / MALCHUNK + 1) * MALCHUNK;

	if (elements > mb->ssize) {
		InstrPtr *ostmt = mb->stmt;
		mb->stmt = GDKrealloc(mb->stmt, elements * sizeof(InstrPtr));
		if (mb->stmt) {
			for (i = mb->ssize; i < elements; i++)
				mb->stmt[i] = 0;
			mb->ssize = elements;
		} else {
			mb->stmt = ostmt;	/* reinstate old pointer */
			mb->errors = createMalException(mb, 0, TYPE,
											SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
	}
	return 0;
}

/* For a MAL session we have to keep the variables around
 * and only need to reset the instruction pointer
 */
void
resetMalTypes(MalBlkPtr mb, int stop)
{
	int i;

	for (i = 0; i < stop; i++)
		mb->stmt[i]->typeresolved = false;
	mb->stop = stop;
	mb->errors = NULL;
}

/* For SQL operations we have to cleanup variables and trim the space
 * A portion is retained for the next query */
void
resetMalBlk(MalBlkPtr mb)
{
	int i;
	InstrPtr *new;
	VarRecord *vnew;

	for (i = 1/*MALCHUNK*/; i < mb->ssize; i++) {
		freeInstruction(mb->stmt[i]);
		mb->stmt[i] = NULL;
	}
	if (mb->ssize != MALCHUNK) {
		new = GDKrealloc(mb->stmt, sizeof(InstrPtr) * MALCHUNK);
		if (new == NULL) {
			/* the only place to return an error signal at this stage. */
			/* The Client context should be passed around more deeply */
			mb->errors = createMalException(mb, 0, TYPE,
											SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return;
		}
		mb->stmt = new;
		mb->ssize = MALCHUNK;
	}
	/* Reuse the initial function statement */
	mb->stop = 1;

	for (i = 0; i < mb->vtop; i++) {
		if (mb->var[i].name)
			GDKfree(mb->var[i].name);
		mb->var[i].name = NULL;
		if (isVarConstant(mb, i))
			VALclear(&getVarConstant(mb, i));
	}

	if (mb->vsize != MALCHUNK) {
		vnew = GDKrealloc(mb->var, sizeof(VarRecord) * MALCHUNK);
		if (vnew == NULL) {
			/* the only place to return an error signal at this stage. */
			/* The Client context should be passed around more deeply */
			mb->errors = createMalException(mb, 0, TYPE,
											SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return;
		}
		mb->var = vnew;
		mb->vsize = MALCHUNK;
	}
	mb->vtop = 0;
}


/* The freeMalBlk code is quite defensive. It is used to localize an
 * illegal reuse of a MAL blk. */
void
freeMalBlk(MalBlkPtr mb)
{
	int i;

	for (i = 0; i < mb->ssize; i++)
		if (mb->stmt[i]) {
			freeInstruction(mb->stmt[i]);
			mb->stmt[i] = NULL;
		}
	mb->stop = 0;
	for (i = 0; i < mb->vtop; i++) {
		if (mb->var[i].name)
			GDKfree(mb->var[i].name);
		mb->var[i].name = NULL;
		if (isVarConstant(mb, i))
			VALclear(&getVarConstant(mb, i));
	}
	mb->vtop = 0;
	GDKfree(mb->stmt);
	mb->stmt = 0;
	GDKfree(mb->var);
	mb->var = 0;

	mb->binding[0] = 0;
	mb->tag = 0;
	mb->memory = 0;
	if (mb->help)
		GDKfree(mb->help);
	mb->help = 0;
	mb->inlineProp = 0;
	mb->unsafeProp = 0;
	freeException(mb->errors);
	GDKfree(mb);
}

/* The routine below should assure that all referenced structures are
 * private. The copying is memory conservative. */
MalBlkPtr
copyMalBlk(MalBlkPtr old)
{
	MalBlkPtr mb;
	int i;

	mb = (MalBlkPtr) GDKzalloc(sizeof(MalBlkRecord));
	if (mb == NULL)
		return NULL;

	mb->var = (VarRecord *) GDKzalloc(sizeof(VarRecord) * old->vsize);
	if (mb->var == NULL) {
		GDKfree(mb);
		return NULL;
	}

	mb->vsize = old->vsize;

	/* copy all variable records */
	for (i = 0; i < old->vtop; i++) {
		mb->var[i] = old->var[i];
		if (mb->var[i].name) {
			mb->var[i].name = GDKstrdup(mb->var[i].name);
			if (!mb->var[i].name)
				goto bailout;
		}
		if (VALcopy(&(mb->var[i].value), &(old->var[i].value)) == NULL) {
			mb->vtop = i;
			goto bailout;
		}
	}
	mb->vtop = old->vtop;

	mb->stmt = (InstrPtr *) GDKzalloc(sizeof(InstrPtr) * old->ssize);
	if (mb->stmt == NULL) {
		goto bailout;
	}

	mb->ssize = old->ssize;
	assert(old->stop < old->ssize);
	for (i = 0; i < old->stop; i++) {
		mb->stmt[i] = copyInstruction(old->stmt[i]);
		if (mb->stmt[i] == NULL) {
			mb->stop = i;
			goto bailout;
		}
	}
	mb->stop = old->stop;
	if (old->help && (mb->help = GDKstrdup(old->help)) == NULL) {
		goto bailout;
	}

	strcpy_len(mb->binding, old->binding, sizeof(mb->binding));
	mb->errors = old->errors ? GDKstrdup(old->errors) : 0;
	mb->tag = old->tag;
	mb->runtime = old->runtime;
	mb->calls = old->calls;
	mb->optimize = old->optimize;
	mb->maxarg = old->maxarg;
	mb->inlineProp = old->inlineProp;
	mb->unsafeProp = old->unsafeProp;
	return mb;

  bailout:
	for (i = 0; i < old->stop; i++)
		freeInstruction(mb->stmt[i]);
	for (i = 0; i < old->vtop; i++) {
		if (mb->var[i].name)
			GDKfree(mb->var[i].name);
		VALclear(&mb->var[i].value);
	}
	GDKfree(mb->var);
	GDKfree(mb->stmt);
	GDKfree(mb);
	return NULL;
}

/* The MAL records should be managed from a pool to
 * avoid repeated alloc/free and reduce probability of
 * memory fragmentation. (todo)
 * The complicating factor is their variable size,
 * which leads to growing records as a result of pushArguments
 * Allocation of an instruction should always succeed.
 */
InstrPtr
newInstructionArgs(MalBlkPtr mb, const char *modnme, const char *fcnnme,
				   int args)
{
	InstrPtr p;

	if (mb && mb->errors)
		return NULL;
	if (args <= 0)
		args = 1;
	p = GDKmalloc(args * sizeof(p->argv[0]) + offsetof(InstrRecord, argv));
	if (p == NULL) {
		if (mb)
			mb->errors = createMalException(mb, 0, TYPE,
											SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return NULL;
	}
	*p = (InstrRecord) {
		.maxarg = args,
		.typeresolved = false,
		.modname = modnme,
		.fcnname = fcnnme,
		.argc = 1,
		.retc = 1,
		/* Flow of control instructions are always marked as an assignment
		 * with modifier */
		.token = ASSIGNsymbol,
	};
	memset(p->argv, 0, args * sizeof(p->argv[0]));
	p->argv[0] = -1;
	return p;
}

InstrPtr
newInstruction(MalBlkPtr mb, const char *modnme, const char *fcnnme)
{
	return newInstructionArgs(mb, modnme, fcnnme, MAXARG);
}

InstrPtr
copyInstructionArgs(const InstrRecord *p, int args)
{
	if (args < p->maxarg)
		args = p->maxarg;
	InstrPtr new = (InstrPtr) GDKmalloc(offsetof(InstrRecord, argv) +
										args * sizeof(p->argv[0]));
	if (new == NULL)
		return new;
	memcpy(new, p,
		   offsetof(InstrRecord, argv) + p->maxarg * sizeof(p->argv[0]));
	if (args > p->maxarg)
		memset(new->argv + p->maxarg, 0,
			   (args - p->maxarg) * sizeof(new->argv[0]));
	new->typeresolved = false;
	new->maxarg = args;
	return new;
}

InstrPtr
copyInstruction(const InstrRecord *p)
{
	return copyInstructionArgs(p, p->maxarg);
}

void
clrFunction(InstrPtr p)
{
	p->token = ASSIGNsymbol;
	p->fcn = 0;
	p->blk = 0;
	p->typeresolved = false;
	setModuleId(p, NULL);
	setFunctionId(p, NULL);
}

void
clrInstruction(InstrPtr p)
{
	clrFunction(p);
	memset(p, 0, offsetof(InstrRecord, argv) + p->maxarg * sizeof(p->argv[0]));
}

void
freeInstruction(InstrPtr p)
{
	GDKfree(p);
}

/* Query optimizers walk their way through a MAL program block. They
 * require some primitives to move instructions around and to remove
 * superfluous instructions. The removal is based on the assumption
 * that indeed the instruction belonged to the block. */
void
removeInstruction(MalBlkPtr mb, InstrPtr p)
{
	int i;
	for (i = 0; i < mb->stop - 1; i++)
		if (mb->stmt[i] == p)
			break;
	if (i == mb->stop)
		return;
	for (; i < mb->stop - 1; i++)
		mb->stmt[i] = mb->stmt[i + 1];
	mb->stmt[i] = 0;
	mb->stop--;
	assert(i == mb->stop);		/* move statement after stop */
	mb->stmt[i] = p;
}

void
removeInstructionBlock(MalBlkPtr mb, int pc, int cnt)
{
	int i;
	InstrPtr p;
	for (i = pc; i < pc + cnt; i++) {
		p = getInstrPtr(mb, i);
		freeInstruction(p);
		mb->stmt[i] = NULL;
	} for (i = pc; i < mb->stop - cnt; i++)
		mb->stmt[i] = mb->stmt[i + cnt];
	mb->stop -= cnt;
	for (; i < mb->stop; i++)
		mb->stmt[i] = 0;
}

void
moveInstruction(MalBlkPtr mb, int pc, int target)
{
	InstrPtr p;
	int i;
	p = getInstrPtr(mb, pc);
	if (pc > target) {
		for (i = pc; i > target; i--)
			mb->stmt[i] = mb->stmt[i - 1];
		mb->stmt[i] = p;
	} else {
		for (i = target; i > pc; i--)
			mb->stmt[i] = mb->stmt[i - 1];
		mb->stmt[i] = p;
	}
}

/* Beware that the first argument of a signature is reserved for the
 * function return type , which should be equal to the destination
 * variable type.
 */
int
findVariable(MalBlkPtr mb, const char *name)
{
	int i;
	if (name == NULL)
		return -1;
	for (i = mb->vtop - 1; i >= 0; i--)
		if (mb->var[i].name && idcmp(name, mb->var[i].name) == 0)
			return i;
	return -1;
}

/* The second version of findVariable assumes you have not yet
 * allocated a private structure. This is particularly useful during
 * parsing, because most variables are already defined. This way we
 * safe GDKmalloc/GDKfree. */
int
findVariableLength(MalBlkPtr mb, const char *name, int len)
{
	int i;
	for (i = mb->vtop - 1; i >= 0; i--) {
		const char *s = mb->var[i].name;
		if (s && strncmp(name, s, len) == 0 && s[len] == 0)
			return i;
	}
	return -1;
}

str
getArgDefault(MalBlkPtr mb, InstrPtr p, int idx)
{
	ValPtr v = &getVarConstant(mb, getArg(p, idx));
	if (v->vtype == TYPE_str)
		return v->val.sval;
	return NULL;
}

/* Beware, the symbol table structure assumes that it is relatively
 * cheap to perform a linear search to a variable or constant. */
static int
makeVarSpace(MalBlkPtr mb)
{
	if (mb->vtop >= mb->vsize) {
		VarRecord *new;
		int s = (mb->vtop / MALCHUNK + 1) * MALCHUNK;
		new = (VarRecord *) GDKrealloc(mb->var, s * sizeof(VarRecord));
		if (new == NULL) {
			/* the only place to return an error signal at this stage. */
			/* The Client context should be passed around more deeply */
			mb->errors = createMalException(mb, 0, TYPE, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
		memset(new + mb->vsize, 0, (s - mb->vsize) * sizeof(VarRecord));
		mb->vsize = s;
		mb->var = new;
	}
	return 0;
}

/* create and initialize a variable record*/
void
setVariableType(MalBlkPtr mb, const int n, malType type)
{
	assert(n >= 0 && n < mb->vtop);
	setVarType(mb, n, type);
	setRowCnt(mb, n, 0);
	clrVarFixed(mb, n);
	clrVarUsed(mb, n);
	clrVarInit(mb, n);
	clrVarDisabled(mb, n);
	clrVarConstant(mb, n);
	clrVarCleanup(mb, n);
}

char *
getVarNameIntoBuffer(MalBlkPtr mb, int idx, char *buf)
{
	char *s = mb->var[idx].name;
	if (getVarKind(mb, idx) == 0)
		setVarKind(mb, idx, REFMARKER);
	if (s == NULL) {
		(void) snprintf(buf, IDLENGTH, "%c_%d", getVarKind(mb, idx), idx);
	} else {
		(void) snprintf(buf, IDLENGTH, "%s", s);
	}
	return buf;
}

int
newVariable(MalBlkPtr mb, const char *name, size_t len, malType type)
{
	int n;
	int kind = REFMARKER;
	if (mb->errors)
		return -1;
	if (len >= IDLENGTH) {
		mb->errors = createMalException(mb, 0, TYPE, "newVariable: id too long");
		return -1;
	}
	if (makeVarSpace(mb)) {		/* no space for a new variable */
		return -1;
	}
	n = mb->vtop;
	mb->var[n].name = NULL;
	if (name && len > 0) {
		char *nme = GDKmalloc(len+1);
		if (!nme) {
			mb->errors = createMalException(mb, 0, TYPE, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
		mb->var[n].name = nme;
		for (size_t i = 0; i < len; i++)
			nme[i] = name[i];
		nme[len] = 0;
		kind = nme[0];
	}
	mb->vtop++;
	setVarKind(mb, n, kind);
	setVariableType(mb, n, type);
	return n;
}

/* Simplified cloning. */
int
cloneVariable(MalBlkPtr tm, MalBlkPtr mb, int x)
{
	int res;
	if (isVarConstant(mb, x))
		res = cpyConstant(tm, getVar(mb, x));
	else {
		res = newTmpVariable(tm, getVarType(mb, x));
		if (mb->var[x].name)
			tm->var[x].name = GDKstrdup(mb->var[x].name);
	}
	if (res < 0)
		return res;
	if (isVarFixed(mb, x))
		setVarFixed(tm, res);
	if (isVarUsed(mb, x))
		setVarUsed(tm, res);
	if (isVarInit(mb, x))
		setVarInit(tm, res);
	if (isVarDisabled(mb, x))
		setVarDisabled(tm, res);
	if (isVarCleanup(mb, x))
		setVarCleanup(tm, res);
	getVarSTC(tm, x) = getVarSTC(mb, x);
	setVarKind(tm, x, getVarKind(mb, x));
	return res;
}

int
newTmpVariable(MalBlkPtr mb, malType type)
{
	return newVariable(mb, 0, 0, type);
}

int
newTypeVariable(MalBlkPtr mb, malType type)
{
	int n, i;
	for (i = 0; i < mb->vtop; i++)
		if (isVarTypedef(mb, i) && getVarType(mb, i) == type)
			break;
	if (i < mb->vtop)
		return i;
	n = newTmpVariable(mb, type);
	if (n >= 0)
		setVarTypedef(mb, n);
	return n;
}

void
clearVariable(MalBlkPtr mb, int varid)
{
	VarPtr v;
	v = getVar(mb, varid);
	if (isVarConstant(mb, varid) || isVarDisabled(mb, varid))
		VALclear(&v->value);
	if (v->name)
		GDKfree(v->name);
	v->name = NULL;
	v->type = 0;
	v->constant = 0;
	v->typevar = 0;
	v->fixedtype = 0;
	v->cleanup = 0;
	v->initialized = 0;
	v->used = 0;
	v->rowcnt = 0;
	v->eolife = 0;
	v->stc = 0;
}

void
freeVariable(MalBlkPtr mb, int varid)
{
	clearVariable(mb, varid);
}

/* A special action is to reduce the variable space by removing all
 * that do not contribute.
 * All temporary variables are renamed in the process to trim the varid.
 */
void
trimMalVariables_(MalBlkPtr mb, MalStkPtr glb)
{
	int *alias, cnt = 0, i, j;
	InstrPtr q;
	if (mb->vtop == 0)
		return;
	alias = (int *) GDKzalloc(mb->vtop * sizeof(int));
	if (alias == NULL)
		return;					/* forget it if we run out of memory *//* build the alias table */
	for (i = 0; i < mb->vtop; i++) {
		if (isVarUsed(mb, i) == 0) {
			if (glb && i < glb->stktop && isVarConstant(mb, i))
				VALclear(&glb->stk[i]);
			freeVariable(mb, i);
			continue;
		}
		if (i > cnt) {			/* remap temporary variables */
			VarRecord t = mb->var[cnt];
			mb->var[cnt] = mb->var[i];
			mb->var[i] = t;
		}						/* valgrind finds a leak when we move these variable record * pointers around. */
		alias[i] = cnt;
		if (glb && i < glb->stktop && i != cnt) {
			glb->stk[cnt] = glb->stk[i];
			VALempty(&glb->stk[i]);
		}
		cnt++;
	}							/* remap all variable references to their new position. */
	if (cnt < mb->vtop) {
		for (i = 0; i < mb->stop; i++) {
			q = getInstrPtr(mb, i);
			for (j = 0; j < q->argc; j++) {
				getArg(q, j) = alias[getArg(q, j)];
			}
		}
		mb->vtop = cnt;
	}
	GDKfree(alias);
}

void
trimMalVariables(MalBlkPtr mb, MalStkPtr stk)
{
	int i, j;
	InstrPtr q;					/* reset the use bit for all non-signature arguments */
	for (i = 0; i < mb->vtop; i++)
		clrVarUsed(mb, i);		/* build the use table */
	for (i = 0; i < mb->stop; i++) {
		q = getInstrPtr(mb, i);
		for (j = 0; j < q->argc; j++)
			setVarUsed(mb, getArg(q, j));
	}
	trimMalVariables_(mb, stk);
}

/* MAL constants
 * Constants are stored in the symbol table and referenced by a
 * variable identifier. This means that per MAL instruction, we may
 * end up with MAXARG entries in the symbol table. This may lead to
 * long searches for variables. An optimization strategy deployed in
 * the current implementation is to look around for a similar
 * (constant) definition and to reuse its identifier. This avoids an
 * exploding symbol table with a lot of temporary variables (as in
 * tst400cHuge)
 *
 * But then the question becomes how far to search? Searching through
 * all variables is only useful when the list remains short or when
 * the constant-variable-name is easily derivable from its literal
 * value and a hash-based index leads you quickly to it.
 *
 * For the time being, we use a MAL system parameter, MAL_VAR_WINDOW,
 * to indicate the number of symbol table entries to consider. Setting
 * it to >= MAXARG will at least capture repeated use of a constant
 * within a single function call or repeated use within a small block
 * of code.
 *
 * The final step is to prepare a GDK value record, from which the
 * internal representation can be obtained during MAL interpretation.
 *
 * The constant values are linked together to improve searching
 * them. This start of the constant list is kept in the MalBlk.
 *
 * Conversion of a constant to another type is limited to well-known
 * coercion rules. Errors are reported and the nil value is set. */

/* Converts the constant in vr to the MAL type type.  Conversion is
 * done in the vr struct. */
str
convertConstant(int type, ValPtr vr)
{
	if (type > GDKatomcnt)
		throw(SYNTAX, "convertConstant", "type index out of bound");
	if (vr->vtype == type)
		return MAL_SUCCEED;
	if (isaBatType(type)) {	/* BAT variables can only be set to nil */
		if (vr->vtype != TYPE_void)
			throw(SYNTAX, "convertConstant", "BAT conversion error");
		VALclear(vr);
		vr->vtype = getBatType(type);
		vr->bat = true;
		vr->val.bval = bat_nil;
		return MAL_SUCCEED;
	}
	if (type == TYPE_ptr) {		/* all coercions should be avoided to protect against memory probing */
		if (vr->vtype == TYPE_void) {
			VALclear(vr);
			vr->vtype = type;
			vr->val.pval = NULL;
			return MAL_SUCCEED;
		}
		if (vr->vtype != type)
			throw(SYNTAX, "convertConstant", "pointer conversion error");
		return MAL_SUCCEED;
	}
	if (type == TYPE_any) {
#ifndef DEBUG_MAL_INSTR
		assert(0);
#endif
		throw(SYNTAX, "convertConstant", "missing type");
	}
	if (VALconvert(type, vr) == NULL) {
		if (vr->vtype == TYPE_str)
			throw(SYNTAX, "convertConstant", "parse error in '%s'", vr->val.sval);
		throw(SYNTAX, "convertConstant", "coercion failed");
	}
	return MAL_SUCCEED;
}

int
fndConstant(MalBlkPtr mb, const ValRecord *cst, int depth)
{
	int i, k;
	const void *p;				/* pointers never match */
	if (ATOMstorage(cst->vtype) == TYPE_ptr)
		return -1;
	p = VALptr(cst);
	k = mb->vtop - depth;
	if (k < 0)
		k = 0;
	for (i = k; i < mb->vtop - 1; i++) {
		VarPtr v = getVar(mb, i);
		if (v->constant) {
			if (v && v->type == cst->vtype &&
					v->value.len == cst->len &&
					isaBatType(v->type) == cst->bat &&
					ATOMcmp(cst->vtype, VALptr(&v->value), p) == 0)
				return i;
		}
	}
	return -1;
}

int
cpyConstant(MalBlkPtr mb, VarPtr vr)
{
	int i;
	ValRecord cst;
	if (VALcopy(&cst, &vr->value) == NULL)
		return -1;
	i = defConstant(mb, vr->type, &cst);
	if (i < 0)
		return -1;
	return i;
}

int
defConstant(MalBlkPtr mb, int type, ValPtr cst)
{
	int k;
	str msg;

	assert(!isaBatType(type) || cst->bat);
	cst->bat = false;
	if (isaBatType(type)) {
		if (cst->vtype == TYPE_void) {
			cst->vtype = getBatType(type);
			cst->bat = true;
			cst->val.bval = bat_nil;
		} else {
			mb->errors = createMalException(mb, 0, TYPE, "BAT coercion error");
			VALclear(cst);	// it could contain allocated space
			return -1;
		}
	} else if (cst->vtype != type && !isPolyType(type)) {
		int otype = cst->vtype;
		assert(type != TYPE_any);	/* help Coverity */
		msg = convertConstant(getBatType(type), cst);
		if (msg) {
			str ft, tt;			/* free old value */
			ft = getTypeName(otype);
			tt = getTypeName(type);
			if (ft && tt)
				mb->errors = createMalException(mb, 0, TYPE,
												"constant coercion error from %s to %s",
												ft, tt);
			else
				mb->errors = createMalException(mb, 0, TYPE,
												"constant coercion error");
			GDKfree(ft);
			GDKfree(tt);
			freeException(msg);
			VALclear(cst);		/* it could contain allocated space */
			return -1;
		} else {
			assert(cst->vtype == type);
		}
	}
	if (cst->vtype != TYPE_any) {
		k = fndConstant(mb, cst, MAL_VAR_WINDOW);
		if (k >= 0) {				/* protect against leaks coming from constant reuse */
			VALclear(cst);
			return k;
		}
	}
	k = newTmpVariable(mb, type);
	if (k < 0) {
		VALclear(cst);
		return -1;
	}
	setVarConstant(mb, k);
	setVarFixed(mb, k);
	if (type >= 0 && type < GDKatomcnt && ATOMextern(type))
		setVarCleanup(mb, k);
	else
		clrVarCleanup(mb, k);	/* if cst is external, we give its allocated buffer away, so clear * it to avoid confusion */
	getVarConstant(mb, k) = *cst;
	VALempty(cst);
	return k;
}

/* Argument handling
 * The number of arguments for procedures is currently
 * limited. Furthermore, we should assure that no variable is
 * referenced before being assigned. Failure to obey should mark the
 * instruction as type-error. */
static InstrPtr
extendInstruction(MalBlkPtr mb, InstrPtr p)
{
	InstrPtr pn = p;
	if (p->argc == p->maxarg) {
		int space = p->maxarg * sizeof(p->argv[0]) + offsetof(InstrRecord, argv);
		pn = (InstrPtr) GDKrealloc(p, space + MAXARG * sizeof(p->argv[0]));
		if (pn == NULL) {		/* In the exceptional case we can not allocate more space * then we show an exception, mark the block as erroneous * and leave the instruction as is. */
			mb->errors = createMalException(mb, 0, TYPE,
											SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return p;
		}
		memset(((char *) pn) + space, 0, MAXARG * sizeof(pn->argv[0]));
		pn->maxarg += MAXARG;
	}
	return pn;
}

InstrPtr
pushArgument(MalBlkPtr mb, InstrPtr p, int varid)
{
	if (p == NULL || mb->errors)
		return p;
	if (varid < 0) {			/* leave everything as is in this exceptional programming error */
		mb->errors = createMalException(mb, 0, TYPE, "improper variable id");
		return p;
	}
	if (p->argc == p->maxarg) {
#ifndef NDEBUG
		for (int i = 0; i < mb->stop; i++)
			assert(mb->stmt[i] != p);
#endif
		p = extendInstruction(mb, p);
		if (mb->errors)
			return p;
	}							/* protect against the case that the instruction is malloced in isolation */
	if (mb->maxarg < p->maxarg)
		mb->maxarg = p->maxarg;
	p->argv[p->argc++] = varid;
	return p;
}

InstrPtr
setArgument(MalBlkPtr mb, InstrPtr p, int idx, int varid)
{
	int i;
	if (p == NULL || mb->errors)
		return p;
	p = pushArgument(mb, p, varid);	/* make space */
	for (i = p->argc - 1; i > idx; i--)
		getArg(p, i) = getArg(p, i - 1);
	getArg(p, i) = varid;
	return p;
}

InstrPtr
pushReturn(MalBlkPtr mb, InstrPtr p, int varid)
{
	if (p == NULL || mb->errors)
		return p;
	if (p->retc == 1 && p->argv[0] == -1) {
		p->argv[0] = varid;
		return p;
	}
	p = setArgument(mb, p, p->retc, varid);
	p->retc++;
	return p;
}

/* Store the information of a destination variable in the signature
 * structure of each instruction. This code is largely equivalent to
 * pushArgument, but it is more efficient in searching and collecting
 * the information.
 * TODO */
/* swallows name argument */
InstrPtr
pushArgumentId(MalBlkPtr mb, InstrPtr p, const char *name)
{
	int v;
	if (p == NULL || mb->errors)
		return p;
	v = findVariable(mb, name);
	if (v < 0) {
		size_t namelen = strlen(name);
		if ((v = newVariable(mb, name, namelen, getAtomIndex(name, namelen, TYPE_any))) < 0) {
			/* set the MAL block to erroneous and simply return without
			 * doing anything */
			/* mb->errors already set */
			return p;
		}
	}
	return pushArgument(mb, p, v);
}

/* The alternative is to remove arguments from an instruction
 * record. This is typically part of instruction constructions. */
void
delArgument(InstrPtr p, int idx)
{
	int i;
	for (i = idx; i < p->argc - 1; i++)
		p->argv[i] = p->argv[i + 1];
	p->argc--;
	if (idx < p->retc)
		p->retc--;
}

void
setArgType(MalBlkPtr mb, InstrPtr p, int i, int tpe)
{
	assert(p->argv[i] < mb->vsize);
	setVarType(mb, getArg(p, i), tpe);
}

void
setReturnArgument(InstrPtr p, int i)
{
	setDestVar(p, i);
}

malType
destinationType(MalBlkPtr mb, InstrPtr p)
{
	if (p->argc > 0)
		return getVarType(mb, getDestVar(p));
	return TYPE_any;
}

/* For polymorphic instructions we should keep around the maximal
 * index to later allocate sufficient space for type resolutions maps.
 * Beware, that we should only consider the instruction polymorphic if
 * it has a positive index or belongs to the signature.
 * BATs can only have a polymorphic type at the tail.
 */
inline void
setPolymorphic(InstrPtr p, int tpe, int force /* just any isn't polymorphic */)
{
	int any = isAnyExpression(tpe) || tpe == TYPE_any, index = 0;
	if ((force == FALSE && tpe == TYPE_any) || !any)
		return;
	if (getTypeIndex(tpe) > 0)
		index = getTypeIndex(tpe);
	if (any && (index + 1) >= p->polymorphic)
		p->polymorphic = index + 1;
}

/* Instructions are simply appended to a MAL block. It should always succeed.
 * The assumption is to push it when you are completely done with its preparation.
 */
void
pushInstruction(MalBlkPtr mb, InstrPtr p)
{
	int i;
	InstrPtr q;
	if (p == NULL)
		return;
	if (mb->stop + 1 >= mb->ssize) {
		int s = (mb->ssize / MALCHUNK + 1) * MALCHUNK;
		if (resizeMalBlk(mb, s) < 0) {
			/* we are now left with the situation that the new
			 * instruction is dangling.  The hack is to take an
			 * instruction out of the block that is likely not
			 * referenced independently.  The last resort is to take the
			 * first, which should always be there.  This assumes that
			 * no references are kept elsewhere to the statement. */
			assert(mb->errors != NULL);
			for (i = 1; i < mb->stop; i++) {
				q = getInstrPtr(mb, i);
				if (q->token == REMsymbol) {
					freeInstruction(q);
					mb->stmt[i] = p;
					return;
				}
			}
			freeInstruction(getInstrPtr(mb, 0));
			mb->stmt[0] = p;
			return;
		}
	}
	if (mb->stmt[mb->stop])
		freeInstruction(mb->stmt[mb->stop]);
	p->pc = mb->stop;
	mb->stmt[mb->stop++] = p;
}
