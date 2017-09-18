/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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

void
addMalException(MalBlkPtr mb, str msg)
{
	str new;

	if( mb->errors){
		new = GDKzalloc(strlen(mb->errors) + strlen(msg) + 4);
		if (new == NULL)
			return ; // just stick to one error message, ignore rest
		strcpy(new, mb->errors);
		strcat(new, msg);
		GDKfree(mb->errors);
		mb->errors = new;
	} else {
		new = GDKstrdup(msg);
		if( new == NULL)
			return ; // just stick to one error message, ignore rest
		mb->errors = new;
	}
}

Symbol
newSymbol(str nme, int kind)
{
	Symbol cur;

	if (nme == NULL)
		return NULL;
	cur = (Symbol) GDKzalloc(sizeof(SymRecord));
	if (cur == NULL)
		return NULL;
	cur->name = putName(nme);
	cur->kind = kind;
	cur->peer = NULL;
	cur->def = newMalBlk(kind == FUNCTIONsymbol? STMT_INCREMENT : 2);
	if (cur->def == NULL){
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

	/* each MAL instruction implies at least on variable 
 	 * we reserve some extra for constants */
	v = (VarRecord *) GDKzalloc(sizeof(VarRecord) * (elements + 8) );
	if (v == NULL) {
		GDKfree(mb);
		return NULL;
	}
	mb->var = v;
	mb->vtop = 0;
	mb->vid = 0;
	mb->vsize = elements;
	mb->help = NULL;
	mb->binding[0] = 0;
	mb->tag = 0;
	mb->errors = NULL;
	mb->alternative = NULL;
	mb->history = NULL;
	mb->keephistory = 0;
	mb->maxarg = MAXARG;		/* the minimum for each instruction */
	mb->inlineProp = 0;
	mb->unsafeProp = 0;
	mb->sealedProp = 0;
	mb->replica = NULL;
	mb->trap = 0;
	mb->runtime = 0;
	mb->calls = 0;
	mb->optimize = 0;
	mb->stmt = NULL;
	mb->activeClients = 1;
	if (newMalBlkStmt(mb, elements) < 0) {
		GDKfree(mb->var);
		GDKfree(mb->stmt);
		GDKfree(mb);
		return NULL;
	}
	return mb;
}

/* We only grow until the MAL block can be used */
static int growBlk(int elm)
{
	int steps =1 ;
	int old = elm;

	while( old / 2 > 1){
		old /= 2;
		steps++;
	}
	return elm + steps * STMT_INCREMENT;
}

int
resizeMalBlk(MalBlkPtr mb, int elements)
{
	int i;

	if( elements > mb->ssize){
		InstrPtr *ostmt = mb->stmt;
		mb->stmt = (InstrPtr *) GDKrealloc(mb->stmt, elements * sizeof(InstrPtr));
		if ( mb->stmt ){
			for ( i = mb->ssize; i < elements; i++)
				mb->stmt[i] = 0;
			mb->ssize = elements;
		} else {
			mb->stmt = ostmt;	/* reinstate old pointer */
			mb->errors = createMalException(mb,0, TYPE, "out of memory (requested: "LLFMT" bytes)", (lng) elements * sizeof(InstrPtr));
			return -1;
		}
	}


	if( elements > mb->vsize){
		VarRecord *ovar = mb->var;
		mb->var = (VarRecord*) GDKrealloc(mb->var, elements * sizeof (VarRecord));
		if ( mb->var ){
			memset( ((char*) mb->var) + sizeof(VarRecord) * mb->vsize, 0, (elements - mb->vsize) * sizeof(VarRecord));
			mb->vsize = elements;
		} else{
			mb->var = ovar;
			mb->errors = createMalException(mb,0, TYPE, "out of memory (requested: "LLFMT" bytes)", (lng) elements * sizeof(InstrPtr));
			return -1;
		}
	}
	return 0;
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
	mb->errors = NULL;
}

void
resetMalBlkAndFreeInstructions(MalBlkPtr mb, int stop)
{
	int i;

	for(i=stop; i<mb->stop; i++) {
		freeInstruction(mb->stmt[i]);
		mb->stmt[i] = NULL;
	}
	resetMalBlk(mb, stop);
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
	for(i=0; i< mb->vtop; i++)
		VALclear(&getVarConstant(mb,i));
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
	mb->sealedProp = 0;
	GDKfree(mb->errors);
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
	mb->alternative = old->alternative;
	mb->history = NULL;
	mb->keephistory = old->keephistory;

	mb->var = (VarRecord *) GDKzalloc(sizeof(VarRecord) * old->vsize);
	if (mb->var == NULL) {
		GDKfree(mb);
		return NULL;
	}

	mb->activeClients = 1;
	mb->vsize = old->vsize;
	mb->vtop = old->vtop;
	mb->vid = old->vid;

	// copy all variable records
	for (i = 0; i < old->vtop; i++) {
		mb->var[i]=  old->var[i];
		if (!VALcopy(&(mb->var[i].value), &(old->var[i].value))) {
			while (--i >= 0)
				VALclear(&mb->var[i].value);
			GDKfree(mb->var);
			GDKfree(mb);
			return NULL;
		}
	}

	mb->stmt = (InstrPtr *) GDKzalloc(sizeof(InstrPtr) * old->ssize);

	if (mb->stmt == NULL) {
		for (i = 0; i < old->vtop; i++)
			VALclear(&mb->var[i].value);
		GDKfree(mb->var);
		GDKfree(mb);
		return NULL;
	}

	mb->stop = old->stop;
	mb->ssize = old->ssize;
	assert(old->stop < old->ssize);
	for (i = 0; i < old->stop; i++) {
		mb->stmt[i] = copyInstruction(old->stmt[i]);
		if(!mb->stmt[i]) {
			while (--i >= 0)
				freeInstruction(mb->stmt[i]);
			for (i = 0; i < old->vtop; i++)
				VALclear(&mb->var[i].value);
			GDKfree(mb->var);
			GDKfree(mb->stmt);
			GDKfree(mb);
			return NULL;
		}
	}
	mb->help = old->help ? GDKstrdup(old->help) : NULL;
	if (old->help && !mb->help) {
		for (i = 0; i < old->stop; i++)
			freeInstruction(mb->stmt[i]);
		for (i = 0; i < old->vtop; i++)
			VALclear(&mb->var[i].value);
		GDKfree(mb->var);
		GDKfree(mb->stmt);
		GDKfree(mb);
		return NULL;
	}
	strncpy(mb->binding,  old->binding, IDLENGTH);
	mb->errors = old->errors? GDKstrdup(old->errors):0;
	mb->tag = old->tag;
	mb->trap = old->trap;
	mb->runtime = old->runtime;
	mb->calls = old->calls;
	mb->optimize = old->optimize;
	mb->replica = old->replica;
	mb->maxarg = old->maxarg;
	mb->inlineProp = old->inlineProp;
	mb->unsafeProp = old->unsafeProp;
	mb->sealedProp = old->sealedProp;
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

// Localize the plan using the optimizer name
MalBlkPtr
getMalBlkOptimized(MalBlkPtr mb, str name)
{
	MalBlkPtr h = mb->history;
	InstrPtr p;
	int i= 0;
	char buf[IDLENGTH]= {0}, *n;

	if( name == 0)
		return mb;
	strncpy(buf,name, IDLENGTH);
	n = strchr(buf,']');
	if( n) *n = 0;
	
	while (h ){
		for( i = 1; i< h->stop; i++){
			p = getInstrPtr(h,i);
			if( p->token == REMsymbol && strstr(getVarConstant(h, getArg(p,0)).val.sval, buf)  )
				return h;
		}
		h = h->history;
	}
	return 0;
}


/* Before compiling a large string, it makes sense to allocate
 * approximately enough space to keep the intermediate
 * code. Otherwise, we end up with a repeated extend on the MAL block,
 * which really consumes a lot of memcpy resources. The average MAL
 * string length could been derived from the test cases. An error in
 * the estimate is more expensive than just counting the lines.
 */
int
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
	return resizeMalBlk(mb, cnt);
}

/* The MAL records should be managed from a pool to
 * avoid repeated alloc/free and reduce probability of
 * memory fragmentation. (todo)
 * The complicating factor is their variable size,
 * which leads to growing records as a result of pushArguments
 * Allocation of an instruction should always succeed.
 */
InstrPtr
newInstruction(MalBlkPtr mb, str modnme, str fcnnme)
{
	InstrPtr p = NULL;

	p = GDKzalloc(MAXARG * sizeof(p->argv[0]) + offsetof(InstrRecord, argv));
	if (p == NULL) {
		/* We are facing an hard problem.
		 * The hack is to re-use an already allocated instruction.
		 * The marking of the block as containing errors should protect further actions.
		 */
		if( mb){
			mb->errors = createMalException(mb,0, TYPE, SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		return NULL;
	}
	p->maxarg = MAXARG;
	p->typechk = TYPE_UNKNOWN;
	setModuleId(p, modnme);
	setFunctionId(p, fcnnme);
	p->argc = 1;
	p->retc = 1;
	p->mitosis = -1;
	p->argv[0] = -1;			/* watch out for direct use in variable table */
	/* Flow of control instructions are always marked as an assignment
	 * with modifier */
	p->token = ASSIGNsymbol;
	return p;
}

/* Copying an instruction is space conservative. */
InstrPtr
copyInstruction(InstrPtr p)
{
	InstrPtr new = (InstrPtr) GDKmalloc(offsetof(InstrRecord, argv) + p->maxarg * sizeof(p->maxarg));
	if(new == NULL) 
		return new;
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
	{
			str s = mb->var[i].id;

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
		VarRecord *new;
		int s = growBlk(mb->vsize);

		new = (VarRecord*) GDKrealloc(mb->var, s * sizeof(VarRecord));
		if (new == NULL) {
			// the only place to return an error signal at this stage.
			// The Client context should be passed around more deeply
			mb->errors = createMalException(mb,0,TYPE, SQLSTATE(HY001) MAL_MALLOC_FAIL);
			return -1;
		}
		memset( ((char*) new) + mb->vsize * sizeof(VarRecord), 0, (s- mb->vsize) * sizeof(VarRecord));
		mb->vsize = s;
		mb->var = new;
	}
	return 0;
}

/* create and initialize a variable record*/
int
newVariable(MalBlkPtr mb, const char *name, size_t len, malType type)
{
	int n;

	if( len >= IDLENGTH)
		return -1;
	if (makeVarSpace(mb)) 
		/* no space for a new variable */
		return -1;
	n = mb->vtop;
	if( name == 0 || len == 0)
		(void) snprintf(getVarName(mb,n), IDLENGTH,"%c%c%d", REFMARKER, TMPMARKER,mb->vid++);
	else{
		(void) strncpy( getVarName(mb,n), name,len);
		getVarName(mb,n)[len]=0;
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
	getVarSTC(tm,x) = getVarSTC(mb,x);
	return res;
}

/* generate a new variable name based on a pattern with 1 %d argument*/
void
renameVariable(MalBlkPtr mb, int id, str pattern, int newid)
{
	assert(id >=0 && id <mb->vtop);
	snprintf(getVarName(mb,id),IDLENGTH,pattern,newid);
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
		if (isVarTypedef(mb, i) && getVarType(mb, i) == type)
			break;

	if( i < mb->vtop )
		return i;
	n = newTmpVariable(mb, type);
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
	v->type = 0;
	v->constant= 0;
	v->typevar= 0;		
	v->fixedtype= 0;
	v->udftype= 0;
	v->cleanup= 0;
	v->initialized= 0;
	v->used= 0;
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

	if( mb->vtop == 0)
		return;
	alias = (int *) GDKzalloc(mb->vtop * sizeof(int));
	if (alias == NULL)
		return;					/* forget it if we run out of memory */

	/* build the alias table */
	for (i = 0; i < mb->vtop; i++) {
#ifdef DEBUG_REDUCE
		fprintf(stderr,"used %s %d\n", getVarName(mb,i), isVarUsed(mb,i));
#endif
		if ( isVarUsed(mb,i) == 0) {
			if (glb && isVarConstant(mb, i))
				VALclear(&glb->stk[i]);
			freeVariable(mb, i);
			continue;
		}
        if (i > cnt) {
            /* remap temporary variables */
            VarRecord t = mb->var[cnt];
            mb->var[cnt] = mb->var[i];
            mb->var[i] = t;
        }

		/* valgrind finds a leak when we move these variable record
		 * pointers around. */
		alias[i] = cnt;
		if (glb && i != cnt) {
			glb->stk[cnt] = glb->stk[i];
			VALempty(&glb->stk[i]);
		}
		cnt++;
	}
#ifdef DEBUG_REDUCE
	fprintf(stderr, "Variable reduction %d -> %d\n", mb->vtop, cnt);
	for (i = 0; i < mb->vtop; i++)
		fprintf(stderr, "map %d->%d\n", i, alias[i]);
#endif

	/* remap all variable references to their new position. */
	if (cnt < mb->vtop) {
		for (i = 0; i < mb->stop; i++) {
			q = getInstrPtr(mb, i);
			for (j = 0; j < q->argc; j++){
#ifdef DEBUG_REDUCE
				fprintf(stderr, "map %d->%d\n", getArg(q,j), alias[getArg(q,j)]);
#endif
				getArg(q, j) = alias[getArg(q, j)];
			}
		}
	}
	/* rename the temporary variable */
	mb->vid = 0;
	for( i =0; i< cnt; i++)
	if( isTmpVar(mb,i))
        (void) snprintf(mb->var[i].id, IDLENGTH,"%c%c%d", REFMARKER, TMPMARKER,mb->vid++);
	
#ifdef DEBUG_REDUCE
	fprintf(stderr, "After reduction \n");
	fprintFunction(stderr, mb, 0, 0);
#endif
	GDKfree(alias);
	mb->vtop = cnt;
}

void
trimMalVariables(MalBlkPtr mb, MalStkPtr stk)
{
	int i, j;
	InstrPtr q;

	/* reset the use bit for all non-signature arguments */
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
		size_t ll = 0;
		ptr d = NULL;
		char *s = vr->val.sval;

		if (ATOMfromstr(type, &d, &ll, vr->val.sval) < 0 || d == NULL) {
			GDKfree(d);
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
		str w;
		if (vr->vtype == TYPE_void || ATOMcmp(vr->vtype, ATOMnilptr(vr->vtype), VALptr(vr)) == 0) {
			vr->vtype = type;
			vr->val.sval = GDKstrdup(str_nil);
			vr->len = (int) strlen(vr->val.sval);
			return MAL_SUCCEED;
		}
		w = ATOMformat(vr->vtype, VALptr(vr));
		if (w == NULL)
			throw(SYNTAX, "convertConstant", GDK_EXCEPTION);
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
		   size_t ll = 0;
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
		size_t ll = 0;
		ptr d = NULL;

		if (isaBatType(type)) {
			if (VALinit(vr, TYPE_bat, ATOMnilptr(TYPE_bat)) == NULL)
				throw(MAL, "convertConstant", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			break;
		}
		/* see if an atomFromStr() function is available */
		if (BATatoms[type].atomFromStr == 0)
			throw(SYNTAX, "convertConstant", "no conversion operator defined");

		/* if the value we're converting from is nil, the to
		 * convert to value will also be nil */
		if (ATOMcmp(vr->vtype, ATOMnilptr(vr->vtype), VALptr(vr)) == 0) {
			if (VALinit(vr, type, ATOMnilptr(type)) == NULL)
				throw(MAL, "convertConstant", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			break;
		}

		/* if what we're converting from is not a string */
		if (vr->vtype != TYPE_str) {
			/* an extern type */
			str w;

			/* dump the non-string atom as string in w */
			if ((w = ATOMformat(vr->vtype, VALptr(vr))) == NULL ||
				/* and try to parse it from string as the desired type */
				ATOMfromstr(type, &d, &ll, w) < 0 ||
				d == NULL) {
				GDKfree(d);
				GDKfree(w);
				VALinit(vr, type, ATOMnilptr(type));
				throw(SYNTAX, "convertConstant", "conversion error");
			}
			GDKfree(w);
			memset((char *) vr, 0, sizeof(*vr));
			VALset(vr, type, d);
			if (ATOMextern(type) == 0)
				GDKfree(d);
		} else {				/* what we're converting from is a string */
			if (ATOMfromstr(type, &d, &ll, vr->val.sval) < 0 || d == NULL) {
				GDKfree(d);
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
	if (getVar(mb,i) && isVarConstant(mb,i)){
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

	if (VALcopy(&cst, &vr->value) == NULL)
		return -1;

	i = defConstant(mb, vr->type, &cst);
	return i;
}

int
defConstant(MalBlkPtr mb, int type, ValPtr cst)
{
	int k;
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
			mb->errors = createMalException(mb, 0, TYPE, "constant coercion error from %s to %s", ft, tt);
			GDKfree(ft);
			GDKfree(tt);
			freeException(msg);
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
	VALcopy( &getVarConstant(mb, k),cst);
	if (ATOMextern(cst->vtype) && cst->val.pval)
		VALclear(cst);
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
		/* leave everything as is in this exceptional programming error */
		mb->errors = createMalException(mb, 0, TYPE,"improper variable id");
		return p;
	}

	if (p->argc + 1 == p->maxarg) {
		int i = 0;
		int space = p->maxarg * sizeof(p->argv[0]) + offsetof(InstrRecord, argv);
		InstrPtr pn = (InstrPtr) GDKrealloc(p,space + MAXARG * sizeof(p->argv[0]));

		if (pn == NULL) {
			/* In the exceptional case we can not allocate more space
			 * then we show an exception, mark the block as erroneous
			 * and leave the instruction as is.
			*/
			mb->errors = createMalException(mb,0, TYPE, SQLSTATE(HY001) MAL_MALLOC_FAIL);
			return p;
		}
		memset( ((char*)pn) + space, 0, MAXARG * sizeof(pn->argv[0]));
		pn->maxarg += MAXARG;

		/* if the instruction is already stored in the MAL block
		 * it should be replaced by an extended version.
		 */
		if( p != pn)
			for (i = mb->stop - 1; i >= 0; i--)
				if (mb->stmt[i] == p) {
					mb->stmt[i] =  pn;
					break;
				}

		p = pn;
		/* we have to keep track on the maximal arguments/block
		 * because it is needed by the interpreter */
		if (mb->maxarg < pn->maxarg)
			mb->maxarg = pn->maxarg;
	}
	if( mb->maxarg < p->maxarg)
		mb->maxarg= p->maxarg;

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
pushArgumentId(MalBlkPtr mb, InstrPtr p, const char *name)
{
	int v;

	if (p == NULL)
		return NULL;
	v = findVariable(mb, name);
	if (v < 0) {
		if ((v = newVariable(mb, name, strlen(name), getAtomIndex(name, -1, TYPE_any))) < 0) {
			freeInstruction(p);
			return NULL;
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
	setVarType(mb,getArg(p, i),tpe);
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

/* Instructions are simply appended to a MAL block. It should always succeed.
 * The assumption is to push it when you are completely done with its preparation.
 */

void
pushInstruction(MalBlkPtr mb, InstrPtr p)
{
	int i;
	int extra;
	InstrPtr q;

	if (p == NULL)
		return;

	extra = mb->vsize - mb->vtop; // the extra variables already known
	if (mb->stop + 1 >= mb->ssize) {
		if( resizeMalBlk(mb, growBlk(mb->ssize) + extra) ){
			/* perhaps we can continue with a smaller increment.
			 * But the block remains marked as faulty.
			 */
			if( resizeMalBlk(mb,mb->ssize + 1)){
				/* we are now left with the situation that the new instruction is dangling .
				 * The hack is to take an instruction out of the block that is likely not referenced independently
				 * The last resort is to take the first, which should always be there
				 * This assumes that no references are kept elsewhere to the statement
				 */
				for( i = 1; i < mb->stop; i++){
					q= getInstrPtr(mb,i);
					if( q->token == REMsymbol){
						freeInstruction(q);
						mb->stmt[i] = p;
						return;
					}		
				}
				freeInstruction(getInstrPtr(mb,0));
				mb->stmt[0] = p;
				return;
			}
		}
	}
	if (mb->stmt[mb->stop])
		freeInstruction(mb->stmt[mb->stop]);
	mb->stmt[mb->stop++] = p;
}
