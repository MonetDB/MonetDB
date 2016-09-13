/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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

#define MAXSYMBOLS 12000 /* enough for the startup and some queries */
static SymRecord symbolpool[MAXSYMBOLS];
static int symboltop;

Symbol
newSymbol(str nme, int kind)
{
	Symbol cur;

	if (nme == NULL) {
		GDKerror("newSymbol:unexpected name (=null)\n");
		return NULL;
	}
	if( symboltop < MAXSYMBOLS){
		cur = symbolpool + symboltop;
		symboltop++;
	} else
		cur = (Symbol) GDKzalloc(sizeof(SymRecord));

	if (cur == NULL) {
		GDKerror("newSymbol:" MAL_MALLOC_FAIL);
		return NULL;
	}
	cur->name = putName(nme);
	cur->kind = kind;
	cur->peer = NULL;
	cur->def = newMalBlk(kind == FUNCTIONsymbol?MAXVARS : MAXARG, kind == FUNCTIONsymbol? STMT_INCREMENT : 2);
	if ( cur->def == NULL){
		GDKfree(cur);
		return NULL;
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
	}
	if( !( s >= symbolpool && s < symbolpool + MAXSYMBOLS))
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

	p = (InstrPtr *) GDKzalloc(sizeof(InstrPtr) * maxstmts);
	if (p == NULL) {
		GDKerror("newMalBlk:" MAL_MALLOC_FAIL);
		return -1;
	}
	mb->stmt = p;
	mb->stop = 0;
	mb->ssize = maxstmts;
	return 0;
}

MalBlkPtr
newMalBlk(int maxvars, int maxstmts)
{
	MalBlkPtr mb;
	VarPtr *v;

	/* each MAL instruction implies at least on variable */
	// TODO: this check/assignment makes little sense
	/*
	if (maxvars < maxstmts)
		maxvars = maxvars;
	*/
	v = (VarPtr *) GDKzalloc(sizeof(VarPtr) * maxvars);
	if (v == NULL) {
		GDKerror("newMalBlk:" MAL_MALLOC_FAIL);
		return NULL;
	}
	mb = (MalBlkPtr) GDKmalloc(sizeof(MalBlkRecord));
	if (mb == NULL) {
		GDKfree(v);
		GDKerror("newMalBlk:" MAL_MALLOC_FAIL);
		return NULL;
	}

	mb->var = v;
	mb->vtop = 0;
	mb->vid = 0;
	mb->vsize = maxvars;
	mb->help = NULL;
	mb->binding[0] = 0;
	mb->tag = 0;
	mb->errors = 0;
	mb->alternative = NULL;
	mb->history = NULL;
	mb->keephistory = 0;
	mb->dotfile = 0;
	mb->maxarg = MAXARG;		/* the minimum for each instruction */
	mb->typefixed = 0;
	mb->flowfixed = 0;
	mb->inlineProp = 0;
	mb->unsafeProp = 0;
	mb->ptop = mb->psize = 0;
	mb->replica = NULL;
	mb->trap = 0;
	mb->runtime = 0;
	mb->calls = 0;
	mb->optimize = 0;
	mb->stmt = NULL;
	mb->activeClients = 1;
	if (newMalBlkStmt(mb, maxstmts) < 0) {
		GDKfree(mb->var);
		GDKfree(mb->stmt);
		GDKfree(mb);
		return NULL;
	}
	return mb;
}

void
resizeMalBlk(MalBlkPtr mb, int maxstmt, int maxvar)
{
	int i;

	if ( maxvar < maxstmt)
		maxvar = maxstmt;
	if ( mb->ssize > maxstmt && mb->vsize > maxvar)
		return ;

	mb->stmt = (InstrPtr *) GDKrealloc(mb->stmt, maxstmt * sizeof(InstrPtr));
	if ( mb->stmt == NULL)
		goto wrapup;
	for ( i = mb->ssize; i < maxstmt; i++)
		mb->stmt[i] = 0;
	mb->ssize = maxstmt;

	mb->var = (VarPtr*) GDKrealloc(mb->var, maxvar * sizeof (VarPtr));
	if ( mb->var == NULL)
		goto wrapup;
	for( i = mb->vsize; i < maxvar; i++)
		mb->var[i] = 0;
	mb->vsize = maxvar;
	return;
wrapup:
	GDKerror("resizeMalBlk:" MAL_MALLOC_FAIL);
}
/* The resetMalBlk code removes instructions, but without freeing the
 * space. This way the structure is prepared for re-use */
void
resetMalBlk(MalBlkPtr mb, int stop)
{
	int i;

	for(i=0; i<stop; i++) 
		mb->stmt[i] ->typechk = TYPE_UNKNOWN;
	mb->stop = stop;
}

/* The freeMalBlk code is quite defensive. It is used to localize an
 * illegal re-use of a MAL blk. */
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
	for (i = 0; i < mb->vsize; i++)
		if (mb->var[i]) {
			freeVariable(mb, i);
			mb->var[i] = 0;
		}
	mb->vtop = 0;
	mb->vid = 0;
	GDKfree(mb->stmt);
	mb->stmt = 0;
	GDKfree(mb->var);
	mb->var = 0;

	if (mb->history)
		freeMalBlk(mb->history);
	mb->binding[0] = 0;
	mb->tag = 0;
	if (mb->help)
		GDKfree(mb->help);
	mb->help = 0;
	mb->inlineProp = 0;
	mb->unsafeProp = 0;
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
	if (mb == NULL) {
		GDKerror("newMalBlk:" MAL_MALLOC_FAIL);
		return NULL;
	}
	mb->alternative = old->alternative;
	mb->history = NULL;
	mb->keephistory = old->keephistory;
	mb->dotfile = old->dotfile;
	mb->var = (VarPtr *) GDKzalloc(sizeof(VarPtr) * old->vsize);
	mb->activeClients = 1;

	if (mb->var == NULL) {
		GDKfree(mb);
		GDKerror("newMalBlk:" MAL_MALLOC_FAIL);
		return NULL;
	}
	mb->vsize = old->vsize;

	mb->vtop = 0;
	mb->vid = old->vid;
	for (i = 0; i < old->vtop; i++) {
		if( copyVariable(mb, getVar(old, i)) == -1){
			GDKfree(mb->var);
			GDKfree(mb);
			GDKerror("copyVariable" MAL_MALLOC_FAIL);
			return NULL;
		}
		mb->vtop++;
	}

	mb->stmt = (InstrPtr *) GDKzalloc(sizeof(InstrPtr) * old->ssize);

	if (mb->stmt == NULL) {
		GDKfree(mb->var);
		GDKfree(mb);
		GDKerror("newMalBlk:" MAL_MALLOC_FAIL);
		return NULL;
	}

	mb->stop = old->stop;
	mb->ssize = old->ssize;
	assert(old->stop < old->ssize);
	for (i = 0; i < old->stop; i++)
		mb->stmt[i] = copyInstruction(old->stmt[i]);

	mb->help = old->help ? GDKstrdup(old->help) : NULL;
	strncpy(mb->binding,  old->binding, IDLENGTH);
	mb->errors = old->errors;
	mb->tag = old->tag;
	mb->typefixed = old->typefixed;
	mb->flowfixed = old->flowfixed;
	mb->trap = old->trap;
	mb->runtime = old->runtime;
	mb->calls = old->calls;
	mb->optimize = old->optimize;
	mb->replica = old->replica;
	mb->maxarg = old->maxarg;
	mb->inlineProp = old->inlineProp;
	mb->unsafeProp = old->unsafeProp;
	return mb;
}

void
addtoMalBlkHistory(MalBlkPtr mb)
{
	MalBlkPtr cpy, h;
	if (mb->keephistory) {
		cpy = copyMalBlk(mb);
		if (cpy == NULL)
			return;				/* ignore history */
		cpy->history = NULL;
		if (mb->history == NULL)
			mb->history = cpy;
		else {
			for (h = mb; h->history; h = h->history)
				;
			h->history = cpy;
		}
	}
}

MalBlkPtr
getMalBlkHistory(MalBlkPtr mb, int idx)
{
	MalBlkPtr h = mb;
	while (h && idx-- >= 0)
		h = h->history;
	return h ? h : mb;
}


/* The MalBlk structures potentially consume a lot a of space, because
 * it is not possible to precisely estimate the default sizes of the
 * var and stmt components. The routines below provide a mechanism to
 * handle the issue. The expandMalBlk routine takes the number of
 * new-lines as a parameter and guesses the size of variable and
 * statement table.
 *
 * Experience shows that trimming leads to memory fragmentation (140K
 * lost after server init) and is therefore turned off. */
static void
trimexpand(MalBlkPtr mb, int varsize, int stmtsize)
{
	VarRecord **v;
	InstrPtr *stmt;
	int len, i;

	assert(varsize > 0 && stmtsize > 0);
	len = sizeof(ValPtr) * (mb->vtop + varsize);
	v = (VarRecord **) GDKzalloc(len);
	if (v == NULL)
		return;
	len = sizeof(InstrPtr) * (mb->ssize + stmtsize);
	stmt = (InstrPtr *) GDKzalloc(len);
	if (stmt == NULL){
		GDKfree(v);
		return;
	}

	memcpy((str) v, (str) mb->var, sizeof(ValPtr) * mb->vtop);

	for (i = mb->vtop; i < mb->vsize; i++)
		if (mb->var[i])
			freeVariable(mb, i);
	if (mb->var)
		GDKfree(mb->var);
	mb->var = v;
	mb->vsize = mb->vtop + varsize;

	memcpy((str) stmt, (str) mb->stmt, sizeof(InstrPtr) * mb->stop);
	for (i = mb->stop; i < mb->ssize; i++) {
		if (mb->stmt[i]) {
			freeInstruction(mb->stmt[i]);
			mb->stmt[i] = NULL;
		}
	}
	GDKfree(mb->stmt);
	mb->stmt = stmt;

	mb->ssize = mb->ssize + stmtsize;
}

/* Before compiling a large string, it makes sense to allocate
 * approximately enough space to keep the intermediate
 * code. Otherwise, we end up with a repeated extend on the MAL block,
 * which really consumes a lot of memcpy resources. The average MAL
 * string length could been derived from the test cases. An error in
 * the estimate is more expensive than just counting the lines.
 *
 * The MAL blocks act as instruction pools. Using a resetMALblock
 * makes the instructions available. */
void
prepareMalBlk(MalBlkPtr mb, str s)
{
	int cnt = STMT_INCREMENT;

	while (s) {
		s = strchr(s, '\n');
		if (s) {
			s++;
			cnt++;
		}
	}
	cnt = (int) (cnt * 1.1);
	if (cnt > mb->ssize || cnt > mb->vsize)
		trimexpand(mb, cnt, cnt);
}

InstrPtr
newInstruction(MalBlkPtr mb, int kind)
{
	InstrPtr p = NULL;

	if (mb && mb->stop < mb->ssize) {
		p = mb->stmt[mb->stop];

		if (p && p->maxarg < MAXARG) {
			assert(0);
			p = NULL;
		}
		mb->stmt[mb->stop] = NULL;
	}
	if (p == NULL) {
		p = GDKzalloc(MAXARG * sizeof(p->argv[0]) + offsetof(InstrRecord, argv));
		if (p == NULL) {
			showException(GDKout, MAL, "pushEndInstruction", "memory allocation failure");
			return NULL;
		}
		p->maxarg = MAXARG;
	}
	p->typechk = TYPE_UNKNOWN;
	setModuleId(p, NULL);
	setFunctionId(p, NULL);
	p->fcn = NULL;
	p->blk = NULL;
	p->polymorphic = 0;
	p->varargs = 0;
	p->argc = 1;
	p->retc = 1;
	p->mitosis = -1;
	p->argv[0] = -1;			/* watch out for direct use in variable table */
	/* Flow of control instructions are always marked as an assignment
	 * with modifier */
	switch (kind) {
	case BARRIERsymbol:
	case REDOsymbol:
	case LEAVEsymbol:
	case EXITsymbol:
	case RETURNsymbol:
	case YIELDsymbol:
	case CATCHsymbol:
	case RAISEsymbol:
		p->token = ASSIGNsymbol;
		p->barrier = kind;
		break;
	default:
		p->token = kind;
		p->barrier = 0;
	}
	p->gc = 0;
	p->jump = 0;
	return p;
}

/* Copying an instruction is space conservative. */
InstrPtr
copyInstruction(InstrPtr p)
{
	InstrPtr new;
	new = (InstrPtr) GDKmalloc(offsetof(InstrRecord, argv) + p->maxarg * sizeof(p->maxarg));
	if( new == NULL) {
		GDKerror("copyInstruction:failed to allocated space");
		return new;
	}
	oldmoveInstruction(new, p);
	return new;
}

void
clrFunction(InstrPtr p)
{
	p->token = ASSIGNsymbol;
	p->fcn = 0;
	p->blk = 0;
	p->typechk = TYPE_UNKNOWN;
	setModuleId(p, NULL);
	setFunctionId(p, NULL);
}

void
clrInstruction(InstrPtr p)
{
	clrFunction(p);
	memset((char *) p, 0, offsetof(InstrRecord, argv) + p->maxarg * sizeof(p->argv[0]));
}

void
freeInstruction(InstrPtr p)
{
	assert(p != 0);
	GDKfree(p);
}

/* Moving instructions around calls for care, because all dependent
 * information should also be updated. */
void
oldmoveInstruction(InstrPtr new, InstrPtr p)
{
	int space;

	space = offsetof(InstrRecord, argv) + p->maxarg * sizeof(p->argv[0]);
	memcpy((char *) new, (char *) p, space);
	setFunctionId(new, getFunctionId(p));
	setModuleId(new, getModuleId(p));
	new->typechk = TYPE_UNKNOWN;
}

/* Query optimizers walk their way through a MAL program block. They
 * require some primitives to move instructions around and to remove
 * superflous instructions. The removal is based on the assumption
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
	assert(i == mb->stop);

	/* move statement after stop */
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
	}

	for (i = pc; i < mb->stop - cnt; i++)
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

void
insertInstruction(MalBlkPtr mb, InstrPtr p, int pc)
{
	pushInstruction(mb, p);		/* to ensure room */
	moveInstruction(mb, mb->stop - 1, pc);
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
		if (idcmp(name, getVarName(mb, i)) == 0)
			return i;
	return -1;
}

/* The second version of findVariable assumes you have not yet
 * allocated a private structure. This is particularly usefull during
 * parsing, because most variables are already defined. This way we
 * safe GDKmalloc/GDKfree. */
int
findVariableLength(MalBlkPtr mb, str name, int len)
{
	int i;
	int j;

	for (i = mb->vtop - 1; i >= 0; i--)
		if (mb->var[i]) { /* mb->var[i]->id will always evaluate to true */
			str s = mb->var[i]->id;

			j = 0;
			if (s)
				for (j = 0; j < len; j++)
					if (name[j] != s[j])
						break;
			if (j == len && s && s[j] == 0)
				return i;
		}
	return -1;
}

/* Note that getType also checks for type names directly. They have
 * preference over variable names. */
malType
getType(MalBlkPtr mb, str nme)
{
	int i;

	i = findVariable(mb, nme);
	if (i < 0)
		return getAtomIndex(nme, -1, TYPE_any);
	return getVarType(mb, i);
}

str
getArgDefault(MalBlkPtr mb, InstrPtr p, int idx)
{
	ValPtr v = &getVarConstant(mb, getArg(p, idx));

	if (v->vtype == TYPE_str)
		return v->val.sval;
	return NULL;
}

/* All variables are implicitly declared upon their first assignment.
 *
 * Lexical constants require some care. They typically appear as
 * arguments in operator/function calls. To simplify program analysis
 * later on, we stick to the situation that function/operator
 * arguments are always references to by variables.
 *
 * Reserved words
 * Although MAL has been designed as a minimal language, several
 * identifiers are not eligible as variables. The encoding below is
 * geared at simple and speed. */
#if 0
int
isReserved(str nme)
{
	switch (*nme) {
	case 'A':
	case 'a':
		if (idcmp("atom", nme) == 0)
			return 1;
		break;
	case 'B':
	case 'b':
		if (idcmp("barrier", nme) == 0)
			return 1;
		break;
	case 'C':
	case 'c':
		if (idcmp("command", nme) == 0)
			return 1;
		break;
	case 'E':
	case 'e':
		if (idcmp("exit", nme) == 0)
			return 1;
		if (idcmp("end", nme) == 0)
			return 1;
		break;
	case 'F':
	case 'f':
		if (idcmp("false", nme) == 0)
			return 1;
		if (idcmp("function", nme) == 0)
			return 1;
		if (idcmp("factory", nme) == 0)
			return 1;
		break;
	case 'I':
	case 'i':
		if (idcmp("include", nme) == 0)
			return 1;
		break;
	case 'M':
	case 'm':
		if (idcmp("module", nme) == 0)
			return 1;
		if (idcmp("macro", nme) == 0)
			return 1;
		break;
	case 'O':
	case 'o':
		if (idcmp("orcam", nme) == 0)
			return 1;
		break;
	case 'P':
	case 'p':
		if (idcmp("pattern", nme) == 0)
			return 1;
		break;
	case 'T':
	case 't':
		if (idcmp("thread", nme) == 0)
			return 1;
		if (idcmp("true", nme) == 0)
			return 1;
		break;
	}
	return 0;
}
#endif

/* Beware, the symbol table structure assumes that it is relatively
 * cheap to perform a linear search to a variable or constant. */
static int
makeVarSpace(MalBlkPtr mb)
{
	if (mb->vtop >= mb->vsize) {
		VarPtr *new;
		int s = mb->vsize * 2;

		new = (VarPtr *) GDKzalloc(s * sizeof(VarPtr));
		if (new == NULL) {
			mb->errors++;
			showScriptException(GDKout, mb, 0, MAL, "newMalBlk:no storage left\n");
			assert(0);
			return -1;
		}
		memcpy((char *) new, (char *) mb->var, sizeof(VarPtr) * mb->vtop);
		GDKfree(mb->var);
		mb->vsize = s;
		mb->var = new;
	}
	return 0;
}

/* create and initialize a variable record*/
int
newVariable(MalBlkPtr mb, str name, size_t len, malType type)
{
	int n;

	if( len >= IDLENGTH)
		return -1;
	if (makeVarSpace(mb)) 
		return -1;
	n = mb->vtop;
	if (getVar(mb, n) == NULL){
		getVar(mb, n) = (VarPtr) GDKzalloc(sizeof(VarRecord) );
		if ( getVar(mb,n) == NULL) {
			mb->errors++;
			GDKerror("newVariable:" MAL_MALLOC_FAIL);
			return -1;
		}
	}
	if( name == 0 || len == 0)
		(void) snprintf(mb->var[n]->id, IDLENGTH,"%c%c%d", REFMARKER, TMPMARKER,mb->vid++);
	else{
		(void) strncpy( mb->var[n]->id, name,len);
		mb->var[n]->id[len]=0;
	}

	setRowCnt(mb,n,0);
	setVarType(mb, n, type);
	clrVarFixed(mb, n);
	clrVarUsed(mb, n);
	clrVarInit(mb, n);
	clrVarDisabled(mb, n);
	clrVarUDFtype(mb, n);
	clrVarConstant(mb, n);
	clrVarCleanup(mb, n);
	mb->vtop++;
	return n;
}

/* Simplified cloning. */
int
cloneVariable(MalBlkPtr tm, MalBlkPtr mb, int x)
{
	int res;
	if (isVarConstant(mb, x))
		res = cpyConstant(tm, getVar(mb, x));
	else
		res = newVariable(tm, getVarName(mb, x), strlen(getVarName(mb,x)), getVarType(mb, x));
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
	if (isVarUDFtype(mb, x))
		setVarUDFtype(tm, res);
	if (isVarCleanup(mb, x))
		setVarCleanup(tm, res);
	strncpy(getSTC(tm,x),getSTC(mb,x), 2 *IDLENGTH);
	return res;
}

/* generate a new variable name based on a pattern with 1 %d argument*/
void
renameVariable(MalBlkPtr mb, int id, str pattern, int newid)
{
	assert(id >=0 && id <mb->vtop);
	snprintf(mb->var[id]->id,IDLENGTH,pattern,newid);
}

int
newTmpVariable(MalBlkPtr mb, malType type)
{
	return newVariable(mb,0,0,type);
}

int
newTypeVariable(MalBlkPtr mb, malType type)
{
	int n, i;
	for (i = 0; i < mb->vtop; i++)
		if (isTmpVar(mb, i) && getVarType(mb, i) == type)
			break;

	if( i < mb->vtop && isVarTypedef(mb,i))
		return i;
	n = newTmpVariable(mb, type);
	setVarTypedef(mb, n);
	return n;
}

int
copyVariable(MalBlkPtr dst, VarPtr v)
{
	VarPtr w;

	w = (VarPtr) GDKzalloc(sizeof(VarRecord));
	if( w == NULL)
		return -1;
	strcpy(w->id,v->id);
	w->type = v->type;
	w->flags = v->flags;
	w->rowcnt = v->rowcnt;
	VALcopy(&w->value, &v->value);
	dst->var[dst->vtop] = w;
	return 0;
}


void
clearVariable(MalBlkPtr mb, int varid)
{
	VarPtr v;

	v = getVar(mb, varid);
	if (v == 0)
		return;
	if (isVarConstant(mb, varid) || isVarDisabled(mb, varid))
		VALclear(&v->value);
	v->type = 0;
	v->flags = 0;
	v->rowcnt = 0;
	v->eolife = 0;
	v->stc[0] = 0;
}

void
freeVariable(MalBlkPtr mb, int varid)
{
	VarPtr v;

	v = getVar(mb, varid);
	clearVariable(mb, varid);
	GDKfree(v);
	getVar(mb, varid) = NULL;
}

/* A special action is to reduce the variable space by removing all
 * that do not contribute.
 */
void
trimMalVariables_(MalBlkPtr mb, MalStkPtr glb)
{
	int *vars, cnt = 0, i, j;
	int maxid = 0,m;
	InstrPtr q;

	vars = (int *) GDKzalloc(mb->vtop * sizeof(int));
	if (vars == NULL)
		return;					/* forget it if we run out of memory */

	/* build the alias table */
	for (i = 0; i < mb->vtop; i++) {
		if ( isVarUsed(mb,i) == 0) {
			if (glb && isVarConstant(mb, i))
				VALclear(&glb->stk[i]);
			freeVariable(mb, i);
			continue;
		}
		if( isTmpVar(mb,i) ){
			m = atoi(getVarName(mb,i)+2);
			if( m > maxid)
				maxid = m;
		}
        if (i > cnt) {
            /* remap temporary variables */
            VarRecord *t = mb->var[cnt];
            mb->var[cnt] = mb->var[i];
            mb->var[i] = t;
        }

		/* valgrind finds a leak when we move these variable record
		 * pointers around. */
		vars[i] = cnt;
		if (glb && i != cnt) {
			glb->stk[cnt] = glb->stk[i];
			VALempty(&glb->stk[i]);
		}
		cnt++;
	}
#ifdef DEBUG_REDUCE
	mnstr_printf(GDKout, "Variable reduction %d -> %d\n", mb->vtop, cnt);
	for (i = 0; i < mb->vtop; i++)
		mnstr_printf(GDKout, "map %d->%d\n", i, vars[i]);
#endif

	/* remap all variable references to their new position. */
	if (cnt < mb->vtop) {
		for (i = 0; i < mb->stop; i++) {
			q = getInstrPtr(mb, i);
			for (j = 0; j < q->argc; j++)
				getArg(q, j) = vars[getArg(q, j)];
		}
	}
	/* reset the variable counter */
	mb->vid= maxid + 1;
#ifdef DEBUG_REDUCE
	mnstr_printf(GDKout, "After reduction \n");
	printFunction(GDKout, mb, 0, 0);
#endif
	GDKfree(vars);
	mb->vtop = cnt;
}

void
trimMalVariables(MalBlkPtr mb, MalStkPtr stk)
{
	int i, j;
	InstrPtr q;

	/* reset the use bit */
	for (i = 0; i < mb->vtop; i++) 
		clrVarUsed(mb,i);
	/* build the use table */
	for (i = 0; i < mb->stop; i++) {
		q = getInstrPtr(mb, i);

		for (j = 0; j < q->argc; j++)
			setVarUsed(mb,getArg(q,j));
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
	if( type > GDKatomcnt )
		throw(SYNTAX, "convertConstant", "type index out of bound");
	if (vr->vtype == type)
		return MAL_SUCCEED;
	if (vr->vtype == TYPE_str) {
		int ll = 0;
		ptr d = NULL;
		char *s = vr->val.sval;

		if (ATOMfromstr(type, &d, &ll, vr->val.sval) < 0 || d == NULL) {
			VALinit(vr, type, ATOMnilptr(type));
			throw(SYNTAX, "convertConstant", "parse error in '%s'", s);
		}
		if (strncmp(vr->val.sval, "nil", 3) != 0 && ATOMcmp(type, d, ATOMnilptr(type)) == 0) {
			GDKfree(d);
			VALinit(vr, type, ATOMnilptr(type));
			throw(SYNTAX, "convertConstant", "parse error in '%s'", s);
		}
		VALset(vr, type, d);
		if (ATOMextern(type) == 0)
			GDKfree(d);
		if (vr->vtype != type)
			throw(SYNTAX, "convertConstant", "coercion failed in '%s'", s);
	}

	if (type == TYPE_bat || isaBatType(type)) {
		/* BAT variables can only be set to nil */
		vr->vtype = type;
		vr->val.bval = bat_nil;
		return MAL_SUCCEED;
	}
	switch (ATOMstorage(type)) {
	case TYPE_any:
		/* In case *DEBUG*_MAL_INSTR is not defined and assertions are
		 * disabled, this will fall-through to the type cases below,
		 * rather than triggering an exception.
		 * Is this correct/intended like this??
		 */
#ifdef DEBUG_MAL_INSTR
		throw(SYNTAX, "convertConstant", "missing type");
#else
		assert(0);
#endif
	case TYPE_bit:
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_void:
	case TYPE_oid:
	case TYPE_flt:
	case TYPE_dbl:
	case TYPE_lng:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
		if (VALconvert(type, vr) == NULL)
			throw(SYNTAX, "convertConstant", "coercion failed");
		return MAL_SUCCEED;
	case TYPE_str:
	{
		str w = 0;
		if (vr->vtype == TYPE_void || ATOMcmp(vr->vtype, ATOMnilptr(vr->vtype), VALptr(vr)) == 0) {
			vr->vtype = type;
			vr->val.sval = GDKstrdup(str_nil);
			vr->len = (int) strlen(vr->val.sval);
			return MAL_SUCCEED;
		}
		ATOMformat(vr->vtype, VALptr(vr), &w);
		assert(w != NULL);
		vr->vtype = TYPE_str;
		vr->len = (int) strlen(w);
		vr->val.sval = w;
		/* VALset(vr, type, w); does not use TYPE-str */
		if (vr->vtype != type)
			throw(SYNTAX, "convertConstant", "coercion failed");
		return MAL_SUCCEED;
	}

	case TYPE_bat:
		/* BAT variables can only be set to nil */
		vr->vtype = type;
		vr->val.bval = bat_nil;
		return MAL_SUCCEED;
	case TYPE_ptr:
		/* all coercions should be avoided to protect against memory probing */
		if (vr->vtype == TYPE_void) {
			vr->vtype = type;
			vr->val.pval = 0;
			return MAL_SUCCEED;
		}
		/*
		   if (ATOMcmp(vr->vtype, ATOMnilptr(vr->vtype), VALptr(vr)) == 0) {
		   vr->vtype = type;
		   vr->val.pval = 0;
		   return MAL_SUCCEED;
		   }
		   if (vr->vtype == TYPE_int) {
		   char buf[BUFSIZ];
		   int ll = 0;
		   ptr d = NULL;

		   snprintf(buf, BUFSIZ, "%d", vr->val.ival);
		   (*BATatoms[type].atomFromStr) (buf, &ll, &d);
		   if( d==0 ){
		   VALinit(vr, type, ATOMnilptr(type));
		   throw(SYNTAX, "convertConstant", "conversion error");
		   }
		   VALset(vr, type, d);
		   if (ATOMextern(type) == 0 )
		   GDKfree(d);
		   }
		 */
		if (vr->vtype != type)
			throw(SYNTAX, "convertConstant", "pointer conversion error");
		return MAL_SUCCEED;
		/* Extended types are always represented as string literals
		 * and converted to the internal storage structure. Beware
		 * that the typeFromStr routines generate storage space for
		 * the new value. This should be garbage collected at the
		 * end. */
	default:{
		int ll = 0;
		ptr d = NULL;

		if (isaBatType(type)) {
			VALinit(vr, TYPE_bat, ATOMnilptr(TYPE_bat));
			break;
		}
		/* see if an atomFromStr() function is available */
		if (BATatoms[type].atomFromStr == 0)
			throw(SYNTAX, "convertConstant", "no conversion operator defined");

		/* if the value we're converting from is nil, the to
		 * convert to value will also be nil */
		if (ATOMcmp(vr->vtype, ATOMnilptr(vr->vtype), VALptr(vr)) == 0) {
			if (VALinit(vr, type, ATOMnilptr(type)) == NULL)
				throw(MAL, "convertConstant", MAL_MALLOC_FAIL);
			break;
		}

		/* if what we're converting from is not a string */
		if (vr->vtype != TYPE_str) {
			/* an extern type */
			str w = 0;

			/* dump the non-string atom as string in w */
			ATOMformat(vr->vtype, VALptr(vr), &w);
			/* and try to parse it from string as the desired type */
			if (ATOMfromstr(type, &d, &ll, w) < 0 || d == 0) {
				VALinit(vr, type, ATOMnilptr(type));
				GDKfree(w);
				throw(SYNTAX, "convertConstant", "conversion error");
			}
			memset((char *) vr, 0, sizeof(*vr));
			VALset(vr, type, d);
			if (ATOMextern(type) == 0)
				GDKfree(d);
			GDKfree(w);
		} else {				/* what we're converting from is a string */
			if (ATOMfromstr(type, &d, &ll, vr->val.sval) < 0 || d == NULL) {
				VALinit(vr, type, ATOMnilptr(type));
				throw(SYNTAX, "convertConstant", "conversion error");
			}
			VALset(vr, type, d);
			if (ATOMextern(type) == 0)
				GDKfree(d);
		}
	}
	}
	if (vr->vtype != type)
		throw(SYNTAX, "convertConstant", "conversion error");
	return MAL_SUCCEED;
}

int
fndConstant(MalBlkPtr mb, const ValRecord *cst, int depth)
{
	int i, k;
	const void *p;

	/* pointers never match */
	if (ATOMstorage(cst->vtype) == TYPE_ptr)
		return -1;

	p = VALptr(cst);
	k = mb->vtop - depth;
	if (k < 0)
		k = 0;
	for (i=k; i < mb->vtop - 1; i++) 
	if( isVarConstant(mb,i)){
		VarPtr v = getVar(mb, i);
		if (v && v->type == cst->vtype && ATOMcmp(cst->vtype, VALptr(&v->value), p) == 0)
			return i;
	}
	return -1;
}

int
cpyConstant(MalBlkPtr mb, VarPtr vr)
{
	int i;
	ValRecord cst;

	VALcopy(&cst, &vr->value);

	i = defConstant(mb, vr->type, &cst);
	return i;
}

int
defConstant(MalBlkPtr mb, int type, ValPtr cst)
{
	int k;
	ValPtr vr;
	str msg;

	if (isaBatType(type) && cst->vtype == TYPE_void) {
		cst->vtype = TYPE_bat;
		cst->val.bval = bat_nil;
	} else if (cst->vtype != type && !isaBatType(type) && !isPolyType(type)) {
		ValRecord vr = *cst;
		int otype = cst->vtype;
		assert(type != TYPE_any);	/* help Coverity */
		msg = convertConstant(type, cst);
		if (msg) {
			str ft, tt;

			/* free old value */
			ft = getTypeName(otype);
			tt = getTypeName(type);
			showException(GDKout, SYNTAX, "defConstant", "constant coercion error from %s to %s", ft, tt);
			GDKfree(ft);
			GDKfree(tt);
			mb->errors++;
			GDKfree(msg);
		} else {
			assert(cst->vtype == type);
		}
		VALclear(&vr);
	}
	k = fndConstant(mb, cst, MAL_VAR_WINDOW);
	if (k >= 0) {
		/* protect against leaks coming from constant reuse */
		if (ATOMextern(type) && cst->val.pval)
			VALclear(cst);
		return k;
	}
	k = newTmpVariable(mb, type);
	setVarConstant(mb, k);
	setVarFixed(mb, k);
	if (type >= 0 && type < GDKatomcnt && ATOMextern(type))
		setVarCleanup(mb, k);
	else
		clrVarCleanup(mb, k);
	vr = &getVarConstant(mb, k);
	*vr = *cst;
	return k;
}

/* Argument handling
 * The number of arguments for procedures is currently
 * limited. Furthermore, we should assure that no variable is
 * referenced before being assigned. Failure to obey should mark the
 * instruction as type-error. */
InstrPtr
pushArgument(MalBlkPtr mb, InstrPtr p, int varid)
{
	if (p == NULL)
		return NULL;
	if (varid < 0) {
		freeInstruction(p);
		return NULL;
	}
	assert(varid >= 0);
	if (p->argc + 1 == p->maxarg) {
		InstrPtr pn;
		int pc = 0, pclimit;
		int space = p->maxarg * sizeof(p->argv[0]) + offsetof(InstrRecord, argv);

		/* instructions are either created in isolation or are stored
		 * on the program instruction stack already. In the latter
		 * case, we may have to adjust their reference. It does not
		 * make sense to locate it on the complete stack, because this
		 * would jeopardise long MAL program.
		 *
		 * The alternative to this hack is to change the code in many
		 * places and educate the programmer to not forget updating
		 * the stmtblock after pushing the arguments. In sql_gencode
		 * this alone would be >100 places and in the optimizers >
		 * 30. In almost all cases the instructions have few
		 * parameters. */
		pclimit = mb->stop - 8;
		pclimit = pclimit < 0 ? 0 : pclimit;
		for (pc = mb->stop - 1; pc >= pclimit; pc--)
			if (mb->stmt[pc] == p) 
				break;

		pn = GDKmalloc(space + MAXARG * sizeof(p->maxarg));
		if (pn == NULL) {
			freeInstruction(p);
			return NULL;
		}
		memcpy(pn, p, space);
		GDKfree(p);
		pn->maxarg += MAXARG;
		/* we have to keep track on the maximal arguments/block
		 * because it is needed by the interpreter */
		if (mb->maxarg < pn->maxarg)
			mb->maxarg = pn->maxarg;
		if( pc >= pclimit)
			mb->stmt[pc] = pn;
		//else 
			// Keep it referenced from the block, assert(0);
		p = pn;
	}
	p->argv[p->argc++] = varid;
	return p;
}

InstrPtr
setArgument(MalBlkPtr mb, InstrPtr p, int idx, int varid)
{
	int i;

	if (p == NULL)
		return NULL;
	p = pushArgument(mb, p, varid);	/* make space */
	if (p == NULL)
		return NULL;
	for (i = p->argc - 1; i > idx; i--)
		getArg(p, i) = getArg(p, i - 1);
	getArg(p, i) = varid;
	return p;
}

InstrPtr
pushReturn(MalBlkPtr mb, InstrPtr p, int varid)
{
	if (p->retc == 1 && p->argv[0] == -1) {
		p->argv[0] = varid;
		return p;
	}
	if ((p = setArgument(mb, p, p->retc, varid)) == NULL)
		return NULL;
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
pushArgumentId(MalBlkPtr mb, InstrPtr p, str name)
{
	int v;

	if (p == NULL) {
		GDKfree(name);
		return NULL;
	}
	v = findVariable(mb, name);
	if (v < 0) {
		if ((v = newVariable(mb, name, strlen(name), getAtomIndex(name, -1, TYPE_any))) < 0) {
			freeInstruction(p);
			return NULL;
		}
	} else
		GDKfree(name);
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
setVarType(MalBlkPtr mb, int i, int tpe)
{
	VarPtr v;
	v = mb->var[i];

	v->type = tpe;
}

/* Cleaning a variable type by setting it to TYPE_any possibly
 * invalidates all other type derivations in the program. Beware of
 * the exception variables. They are globally known. */
void
clrAllTypes(MalBlkPtr mb)
{
	int i;
	InstrPtr p;

	p = getInstrPtr(mb, 0);

	for (i = p->argc; i < mb->vtop; i++)
		if (!isVarUDFtype(mb, i) && isVarUsed(mb, i) && !isVarTypedef(mb, i) && !isVarConstant(mb, i) && !isExceptionVariable(mb->var[i]->id)) {
			setVarType(mb, i, TYPE_any);
			clrVarCleanup(mb, i);
			clrVarFixed(mb, i);
		}
	for (i = 1; i < mb->stop - 1; i++) {
		p = getInstrPtr(mb, i);
		p->typechk = TYPE_UNKNOWN;
		p->fcn = 0;
		p->blk = NULL;

		switch (p->token) {
		default:
			p->token = ASSIGNsymbol;
		case RAISEsymbol:
		case CATCHsymbol:
		case RETURNsymbol:
		case LEAVEsymbol:
		case YIELDsymbol:
		case EXITsymbol:
		case NOOPsymbol:
			break;
		case ENDsymbol:
			return;
		}
	}
}

void
setArgType(MalBlkPtr mb, InstrPtr p, int i, int tpe)
{
	assert(p->argv[i] < mb->vsize);
	mb->var[getArg(p, i)]->type = tpe;
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
 * index to later allocate sufficient space for type resolutions
 * maps. Beware, that we only consider the instruction polymorphic if
 * it has an index or belongs to the signature. In other cases it
 * merely has to be filled. */
inline void
setPolymorphic(InstrPtr p, int tpe, int force)
{
	int c1 = 0, c2 = 0;

	if (force == FALSE && tpe == TYPE_any)
		return;
	if (isaBatType(tpe)) 
		c1= TYPE_oid;
	if (getTypeIndex(tpe) > 0)
		c2 = getTypeIndex(tpe);
	else if (getBatType(tpe) == TYPE_any)
		c2 = 1;
	c1 = c1 > c2 ? c1 : c2;
	if (c1 > 0 && c1 >= p->polymorphic)
		p->polymorphic = c1 + 1;

}

/* Instructions are simply appended to a MAL block. It is also the
 * place to collect information to speed-up use later on. */
void
pushInstruction(MalBlkPtr mb, InstrPtr p)
{
	int i;

	if (p == NULL)
		return;

	i = mb->stop;
	if (i + 1 >= mb->ssize) {
		int space = (mb->ssize + STMT_INCREMENT) * sizeof(InstrPtr);
		InstrPtr *newblk = (InstrPtr *) GDKzalloc(space);

		if (newblk == NULL) {
			mb->errors++;
			showException(GDKout, MAL, "pushInstruction", "out of memory (requested: %d bytes)", space);
			return;
		}
		memcpy(newblk, mb->stmt, mb->ssize * sizeof(InstrPtr));
		mb->ssize += STMT_INCREMENT;
		GDKfree(mb->stmt);
		mb->stmt = newblk;
	}
	/* A destination variable should be set */
	if(p->argc > 0 && p->argv[0] < 0){
		mb->errors++;
		showException(GDKout, MAL, "pushInstruction", "Illegal instruction (missing target variable)");
		freeInstruction(p);
		return;
	}
	if (mb->stmt[i]) {
		/* if( getModuleId(mb->stmt[i] ) )
		   printf("Garbage collect statement %s.%s\n",
		   getModuleId(mb->stmt[i]), getFunctionId(mb->stmt[i])); */
		freeInstruction(mb->stmt[i]);
	}
	mb->stmt[i] = p;

	mb->stop++;
}

/* The END instruction has an optional name, which is only checked
 * during parsing; */
void
pushEndInstruction(MalBlkPtr mb)
{
	InstrPtr p;

	p = newInstruction(mb, ENDsymbol);
	if (!p) {
		mb->errors++;
		showException(GDKout, MAL, "pushEndInstruction", "failed to create instruction (out of memory?)");
		return;
	}
	p->argc = 0;
	p->retc = 0;
	p->argv[0] = 0;
	pushInstruction(mb, p);
}

