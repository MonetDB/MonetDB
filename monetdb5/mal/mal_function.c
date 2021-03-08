/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * (author) M. Kersten
 * For documentation see website
 */
#include "monetdb_config.h"
#include "mal_function.h"
#include "mal_resolve.h"	/* for isPolymorphic() & chkProgram() */
#include "mal_interpreter.h"	/* for showErrors() */
#include "mal_listing.h"
#include "mal_namespace.h"
#include "mal_private.h"

Symbol newFunctionArgs(const char *mod, const char *nme, int kind, int args)
{
	Symbol s;
	InstrPtr p;
	int varid;

	if(mod == NULL || nme == NULL)
		return NULL;

	s = newSymbol(nme,kind);
	if (s == NULL)
		return NULL;

	varid = newVariable(s->def,nme,strlen(nme),TYPE_any);
	if( varid < 0){
		freeSymbol(s);
		return NULL;
	}

	p = newInstructionArgs(NULL, mod, nme, args);
	if (p == NULL) {
		freeSymbol(s);
		return NULL;
	}
	p->token = kind;
	p->barrier = 0;
	setDestVar(p, varid);
	pushInstruction(s->def,p);
	return s;
}

Symbol newFunction(const char *mod, const char *nme, int kind)
{
	return newFunctionArgs(mod, nme, kind, MAXARG);
}

/*
 * Optimizers may be interested in the function definition
 * for obtaining properties. Rather than polution of the
 * instruction record with a scope reference, we use a lookup function until it
 * becomes a performance hindrance.
 */
Symbol  getFunctionSymbol(Module scope, InstrPtr p){
	Module m;
	Symbol s;

	for(m= findModule(scope,getModuleId(p)); m; m= m->link)
		if(idcmp(m->name, getModuleId(p))==0 ) {
			s= m->space[getSymbolIndex(getFunctionId(p))];
			for(; s; s= s->peer)
				if( getSignature(s)->fcn == p->fcn)
					return s;
		}
	return 0;
}

int getPC(MalBlkPtr mb, InstrPtr p)
{   int i;
	for( i=0;i<mb->stop; i++)
	if( getInstrPtr(mb,i)==p) return i;
	return -1;
}
/*
 * Checking the control flow structure is done by a single pass over the
 * MAL program after the program has been type-checked.
 * It should inspect all BARRIER and CATCH blocks for proper structure.
 * If the flow is correct and not dependent on an undefined typed instruction
 * we avoid doing this check any further.
 */
#define DEPTH 128

str
chkFlow(MalBlkPtr mb)
{   int i,j,k, v,lastInstruction;
	int  pc[DEPTH];
	int  var[DEPTH];
	InstrPtr stmt[DEPTH];
	int btop=0;
	int endseen=0, retseen=0, yieldseen=0;
	InstrPtr p, sig;
	str msg = MAL_SUCCEED;

	if ( mb->errors != MAL_SUCCEED)
		return mb->errors ;
	sig = getInstrPtr(mb, 0);
	lastInstruction = mb->stop-1;
	for(i= 0; i<mb->stop; i++){
		p= getInstrPtr(mb,i);
		/* we have to keep track on the maximal arguments/block
		  because it is needed by the interpreter */
		switch( p->barrier){
		case BARRIERsymbol:
		case CATCHsymbol:
			if(btop== DEPTH)
			    throw(MAL,"chkFlow", "%s.%s Too many nested MAL blocks", getModuleId(sig), getFunctionId(sig));
			pc[btop]= i;
			v= var[btop]= getDestVar(p);
			stmt[btop]=p;

			for(j=btop-1;j>=0;j--)
			if( v==var[j])
			    throw(MAL,"chkFlow", "%s.%s recursive %s[%d] shields %s[%d]", getModuleId(sig), getFunctionId(sig), getVarName(mb,v), pc[j], getFcnName(mb),pc[i]);

			btop++;
			break;
		case EXITsymbol:
			v= getDestVar(p);
			if( btop>0 && var[btop-1] != v)
			    throw(MAL,"chkFlow",  "%s.%s exit-label '%s' doesnot match '%s'", getModuleId(sig), getFunctionId(sig), getVarName(mb,v), getVarName(mb,var[btop-1]));
			if(btop==0)
			    throw(MAL,"chkFlow", "%s.%s exit-label '%s' without begin-label",  getModuleId(sig), getFunctionId(sig), getVarName(mb,v));
			/* search the matching block */
			for(j=btop-1;j>=0;j--)
			if( var[j]==v) break;
			if(j>=0) btop= j; else btop--;

			/* retrofit LEAVE/REDO instructions */
			stmt[btop]->jump= i;
			for(k=pc[btop]; k<i; k++){
			    InstrPtr p1= getInstrPtr(mb,k);
			    if( getDestVar(p1)==v ) {
			        /* handle assignments with leave/redo option*/
			        if(p1->barrier== LEAVEsymbol )
			            p1->jump= i;
			        if( p1->barrier==REDOsymbol )
			            p1->jump= pc[btop]+1;
			    }
			}
			break;
		case LEAVEsymbol:
		case REDOsymbol:
			v= getDestVar(p);
			for(j=btop-1;j>=0;j--)
			if( var[j]==v) break;
			if(j<0){
				str nme=getVarName(mb,v);
			    throw(MAL,"chkFlow",  "%s.%s label '%s' not in guarded block", getModuleId(sig), getFunctionId(sig), nme);
			}
			break;
		case YIELDsymbol:
			{ InstrPtr ps= getInstrPtr(mb,0);
			if( ps->token != FACTORYsymbol){
			    throw(MAL,"chkFlow",  "%s.%s yield misplaced!",  getModuleId(sig), getFunctionId(sig));
			}
			yieldseen= TRUE;
			 }
			/* fall through */
		case RETURNsymbol:
			{
				InstrPtr ps = getInstrPtr(mb, 0);
				int e;
				if (p->barrier == RETURNsymbol)
					yieldseen = FALSE;    /* always end with a return */
				if (ps->retc != p->retc) {
					throw(MAL,"chkFlow",  "%s.%s invalid return target!",  getModuleId(sig), getFunctionId(sig));
				} else
				if (ps->typechk == TYPE_RESOLVED)
					for (e = 0; e < p->retc; e++) {
						if (resolveType(getArgType(mb, ps, e), getArgType(mb, p, e)) < 0) {
							str tpname = getTypeName(getArgType(mb, p, e));
							msg = createException(MAL, "%s.%s %s type mismatch at type '%s'\n", getModuleId(p), getFunctionId(p),
									(p->barrier == RETURNsymbol ? "RETURN" : "YIELD"), tpname);
							GDKfree(tpname);
							return msg;
						}
					}
			}
			//if (btop == 0)
				retseen = 1;
			break;
	    case RAISEsymbol:
			endseen = 1;
	        break;
	    case ENDsymbol:
			endseen =1;
	        break;
		default:
			if( isaSignature(p) ){
				if( p->token == REMsymbol){
					/* do nothing */
				} else if( i) {
					str l = instruction2str(mb,0,p,TRUE);
					msg = createException( MAL, "%s.%s signature misplaced\n!%s", getModuleId(p), getFunctionId(p),l);
					GDKfree(l);
					return  msg;
				}
			}
		}
	}

	if(lastInstruction < mb->stop-1 )
		throw(MAL,"chkFlow",  "%s.%s instructions after END", getModuleId(sig), getFunctionId(sig));

	if( endseen && btop  > 0)
			throw(MAL,"chkFlow",  "barrier '%s' without exit in %s[%d]", getVarName(mb,var[btop - 1]), getFcnName(mb), i);
	p= getInstrPtr(mb,0);
	if( !isaSignature(p))
		throw( MAL,"chkFlow",  "%s.%s signature missing",  getModuleId(sig), getFunctionId(sig));
	if( retseen == 0){
		if( getArgType(mb,p,0)!= TYPE_void &&
			(p->token==FUNCTIONsymbol || p->token==FACTORYsymbol))
				throw(MAL,"chkFlow",  "%s.%s RETURN missing", getModuleId(sig), getFunctionId(sig));
	}
	if ( yieldseen && getArgType(mb,p,0)!= TYPE_void)
			throw( MAL,"chkFlow",  "%s.%s RETURN missing", getModuleId(sig), getFunctionId(sig));
	return MAL_SUCCEED;
}

/*
 * A code may contain temporary names for marking barrier blocks.
 * Since they are introduced by the compiler, the parser should locate
 * them itself when encountering the LEAVE,EXIT,REDO.
 * The starting position is mostly the last statement entered.
 * Purposely, the nameless envelops searches the name of the last
 * unclosed block. All others are ignored.
 */
int getBarrierEnvelop(MalBlkPtr mb){
	int pc;
	InstrPtr p;
	for(pc= mb->stop-2 ; pc>=0; pc--){
		p= getInstrPtr(mb,pc);
		if( blockExit(p)){
			int l= p->argv[0];
			for(; pc>=0;pc--){
			    p= getInstrPtr(mb,pc);
			    if( blockStart(p) && p->argv[0]==l) break;
			}
			continue;
		}
		if( blockStart(p) ) return p->argv[0];
	}
	return newTmpVariable(mb,TYPE_any);
}

static void replaceTypeVar(MalBlkPtr mb, InstrPtr p, int v, malType t){
	int j,i,x,y;

	for(j=0; j<mb->stop; j++){
	    p= getInstrPtr(mb,j);
	if( p->polymorphic)
	for(i=0;i<p->argc; i++)
	if( isPolymorphic(x= getArgType(mb,p,i))) {
		if( isaBatType(x)){
			int tail;
			int tx;
			tail = getBatType(x);
			tx = getTypeIndex(x);
			if(v && tx == v && tail == TYPE_any){
			    tx= 0;
			    tail = t;
			}
			y= newBatType(tail);
			setTypeIndex(y,tx);
			setArgType(mb,p,i,y);
		} else
		if(getTypeIndex(x) == v){
			setArgType(mb,p,i,t);
		}
		else {
		}
	}
	}
}

/* insert a symbol into the symbol table just before the symbol
 * "before". */
static void
insertSymbolBefore(Module scope, Symbol prg, Symbol before)
{
	InstrPtr sig;
	int t;
	Symbol s;

	assert(strcmp(prg->name, before->name) == 0);
	sig = getSignature(prg);
	if (getModuleId(sig) && getModuleId(sig) != scope->name) {
		Module c = findModule(scope, getModuleId(sig));
		if (c)
			scope = c;
	}
	t = getSymbolIndex(getFunctionId(sig));
	assert(scope->space != NULL);
	assert(scope->space[t] != NULL);
	s = scope->space[t];
	prg->skip = before->skip;
	prg->peer = before;
	if (s == before) {
		scope->space[t] = prg;
	} else {
		for (;;) {
			assert(s != NULL);
			if (s->skip == before) {
				s->skip = prg;
			}
			if (s->peer == before) {
				s->peer = prg;
				break;
			}
			s = s->peer;
		}
	}
}

/*
 * Upon cloning a function we should remove all the polymorphic flags.
 * Otherwise we may end up with a recursive clone.
 */
Symbol
cloneFunction(Module scope, Symbol proc, MalBlkPtr mb, InstrPtr p)
{
	Symbol new;
	int i,v;
	InstrPtr pp;
	str msg = MAL_SUCCEED;

	new = newFunction(scope->name, proc->name, getSignature(proc)->token);
	if( new == NULL){
		return NULL;
	}
	freeMalBlk(new->def);
	if((new->def = copyMalBlk(proc->def)) == NULL) {
		freeSymbol(new);
		return NULL;
	}
	/* now change the definition of the original proc */
	/* check for errors after fixation , TODO*/
	pp = getSignature(new);
	for (i = 0; i < pp->argc; i++)
		if (isPolymorphic(v = getArgType(new->def,pp, i))) {
			int t = getArgType(mb, p, i);

			if (v == TYPE_any)
				replaceTypeVar(new->def, pp, v, t);
			if (isaBatType(v)) {
				if (getTypeIndex(v))
					replaceTypeVar(new->def, pp, getTypeIndex(v), getBatType(t));
			} else
				replaceTypeVar(new->def, pp, getTypeIndex(v), t);
		}
	/* include the function at the proper place in the scope */
	insertSymbolBefore(scope, new, proc);
	/* clear polymorphic and type to force analysis*/
	for (i = 0; i < new->def->stop; i++) {
		pp = getInstrPtr(new->def, i);
	    pp->typechk = TYPE_UNKNOWN;
		pp->polymorphic = 0;
	}
	/* clear type fixations */
	for (i = 0; i < new->def->vtop; i++)
		clrVarFixed(new->def, i);


	/* check for errors after fixation , TODO*/
	/* beware, we should now ignore any cloning */
	if (proc->def->errors == 0) {
		msg = chkProgram(scope,new->def);
		if( msg)
			mb->errors = msg;
		else
		if(new->def->errors){
			assert(mb->errors == NULL);
			mb->errors = new->def->errors;
			mb->errors = createMalException(mb,0,TYPE,"Error in cloned function");
			new->def->errors = 0;
		}
	}

	return new;
}

/*
 * For commands we do not have to clone the routine. We merely have to
 * assure that the type-constraints are obeyed. The resulting type
 * is returned.
 */
void
debugFunction(stream *fd, MalBlkPtr mb, MalStkPtr stk, int flg, int first, int step)
{
	int i,j;
	str ps;
	InstrPtr p;

	if (mb == NULL) {
		mnstr_printf(fd, "# function definition missing\n");
		return;
	}
	if ( flg == 0 || step < 0  || first < 0 )
		return;

	for (i = first; i < first +step && i < mb->stop; i++){
		ps = instruction2str(mb, stk, (p=getInstrPtr(mb, i)), flg);
		if (ps) {
			if (p->token == REMsymbol)
				mnstr_printf(fd,"%-40s\n",ps);
			else {
				mnstr_printf(fd,"%-40s\t#[%d] ("BUNFMT") %s ",ps, i, getRowCnt(mb,getArg(p,0)), (p->blk? p->blk->binding:""));
				if( flg & LIST_MAL_FLOW){
					for(j =0; j < p->retc; j++)
						mnstr_printf(fd,"%d ",getArg(p,j));
					if( p->argc - p->retc > 0)
						mnstr_printf(fd,"<- ");
					for(; j < p->argc; j++)
						mnstr_printf(fd,"%d ",getArg(p,j));
				}
				// also show type check property
				if( p->typechk == TYPE_UNKNOWN)
					mnstr_printf(fd," type check needed ");
				mnstr_printf(fd,"\n");
			}
			GDKfree(ps);
		} else mnstr_printf(fd,"#failed instruction2str()\n");
	}
}

void
listFunction(stream *fd, MalBlkPtr mb, MalStkPtr stk, int flg, int first, int size)
{
	int i;
	int sample = 256;

	if (mb == NULL) {
		mnstr_printf(fd, "# function definition missing\n");
		return;
	}
	if ( flg == 0)
		return;

	assert(size>=0);
	assert(first>=0 && first <mb->stop);
	renameVariables(mb);
	if (flg & LIST_MAL_MAPI) {
		size_t len = 0;
		str ps;
		mnstr_printf(fd, "&1 0 %d 1 %d\n", /* type id rows columns tuples */
				mb->stop, mb->stop);
		mnstr_printf(fd, "%% .explain # table_name\n");
		mnstr_printf(fd, "%% mal # name\n");
		mnstr_printf(fd, "%% clob # type\n");
		for (i = first; i < first +size && i < mb->stop && sample-- > 0; i++) {
			ps = instruction2str(mb, stk, getInstrPtr(mb, i), flg);
			if (ps) {
				size_t l = strlen(ps);
				if (l > len)
					len = l;
				GDKfree(ps);
			} else mnstr_printf(fd,"#failed instruction2str()\n");
		}
		mnstr_printf(fd, "%% %zu # length\n", len);
	}
	for (i = first; i < first +size && i < mb->stop; i++)
		printInstruction(fd, mb, stk, getInstrPtr(mb, i), flg);
}


void
renameVariables(MalBlkPtr mb)
{
	int i;
	char *s;

	/* Temporary variables get their name from the position in the symbol table */
	/* However, also MAL input may contain temporary names. At some point you need to clean it up to avoid clashes */
	/* Certainly when you are about to print the MAL function */
	/* During optimization they may be copied around, which means there name should be re-establised */
	/* rename all temporaries for ease of variable table interpretation */
	/* this code should not be necessary is variables always keep their position */
	for( i = 0; i < mb->vtop; i++) {
		s = getVarName(mb, i);
		if( s[1] == '_' && (*s == 'C' || *s == 'X'))
			snprintf(s + 2, IDLENGTH-2, "%d", i);
	}
}

void printFunction(stream *fd, MalBlkPtr mb, MalStkPtr stk, int flg)
{
	int i,j;
	InstrPtr p;


	// Set the used bits properly
	for(i=0; i< mb->vtop; i++)
		clrVarUsed(mb,i);


	for(i=0; i< mb->stop; i++){
		p= getInstrPtr(mb,i);
		for(j= p->retc; j<p->argc; j++)
			setVarUsed(mb, getArg(p,j));
		if( p->barrier)
			for(j= 0; j< p->retc; j++)
				setVarUsed(mb, getArg(p,j));
	}
	listFunction(fd,mb,stk,flg,0,mb->stop);
}

void traceFunction(component_t comp, MalBlkPtr mb, MalStkPtr stk, int flg)
{
	int i,j;
	InstrPtr p;
	// Set the used bits properly
	for(i=0; i< mb->vtop; i++)
		clrVarUsed(mb,i);
	for(i=0; i< mb->stop; i++){
		p= getInstrPtr(mb,i);
		for(j= p->retc; j<p->argc; j++)
			setVarUsed(mb, getArg(p,j));
		if( p->barrier)
			for(j= 0; j< p->retc; j++)
				setVarUsed(mb, getArg(p,j));
	}
	for (i = 0; i < mb->stop; i++)
		traceInstruction(comp, mb, stk, getInstrPtr(mb, i), flg);
}

/* initialize the static scope boundaries for all variables */
void
setVariableScope(MalBlkPtr mb)
{
	int pc, k, depth=0, dflow= -1;
	InstrPtr p;

	/* reset the scope admin */
	for (k = 0; k < mb->vtop; k++)
	if( isVarConstant(mb,k)){
		setVarScope(mb,k,0);
		setVarDeclared(mb,k,0);
		setVarUpdated(mb,k,0);
		setVarEolife(mb,k,mb->stop);
	} else {
		setVarScope(mb,k,0);
		setVarDeclared(mb,k,0);
		setVarUpdated(mb,k,0);
		setVarEolife(mb,k,0);
	}

	for (pc = 0; pc < mb->stop; pc++) {
		p = getInstrPtr(mb, pc);
		if( p->token == NOOPsymbol)
			continue;

		if( blockStart(p)){
			if (getModuleId(p) && getFunctionId(p) && strcmp(getModuleId(p),"language")==0 && strcmp(getFunctionId(p),"dataflow")==0){
				if( dflow != -1)
					addMalException(mb,"setLifeSpan nested dataflow blocks not allowed" );
				dflow= depth;
			} else
				depth++;
		}

		for (k = 0; k < p->argc; k++) {
			int v = getArg(p,k);
			if( isVarConstant(mb,v) && getVarUpdated(mb,v) == 0)
				setVarUpdated(mb,v, pc);

			if ( getVarDeclared(mb,v) == 0 ){
				setVarDeclared(mb,v, pc);
				setVarScope(mb,v,depth);
			}
			if (k < p->retc )
				setVarUpdated(mb,v, pc);
			if ( getVarScope(mb,v) == depth )
				setVarEolife(mb,v,pc);

			if ( k >= p->retc && getVarScope(mb,v) < depth )
				setVarEolife(mb,v,-1);
		}
		/*
		 * At a block exit we can finalize all variables defined within that block.
		 * This does not hold for dataflow blocks. They merely direct the execution
		 * thread, not the syntactic scope.
		 */
		if( blockExit(p) ){
			for (k = 0; k < mb->vtop; k++)
			if ( getVarEolife(mb,k) == 0 && getVarScope(mb,k) ==depth )
				setVarEolife(mb,k,pc);
			else if ( getVarEolife(mb,k) == -1 )
				setVarEolife(mb,k,pc);

			if( dflow == depth)
				dflow= -1;
			else depth--;
		}
		if( blockReturn(p)){
			for (k = 0; k < p->argc; k++)
				setVarEolife(mb,getArg(p,k),pc);
		}
	}
	for (k = 0; k < mb->vtop; k++)
		if( getVarEolife(mb,k) == 0)
			setVarEolife(mb,k, mb->stop-1);
}

int
isLoopBarrier(MalBlkPtr mb, int pc){
	InstrPtr p;
	int varid;
	p= getInstrPtr(mb,pc);
	if( p->barrier != BARRIERsymbol)
		return 0;
	varid= getDestVar(p);
	for(pc++; pc< mb->stop; pc++){
		p= getInstrPtr(mb,pc);
		if( p->barrier == REDOsymbol && getDestVar(p)== varid)
			return 1;
		if( p->barrier == EXITsymbol && getDestVar(p)== varid)
			break;
	}
	return 0;
}
int
getBlockBegin(MalBlkPtr mb,int pc){
	InstrPtr p;
	int varid=0,i;

	for(i= pc; i< mb->stop; i++){
		p= getInstrPtr(mb,i);
		if( p->barrier == EXITsymbol ){
			varid= getDestVar(p);
			break;
		}
	}
	if( i==mb->stop) return 0;

	for(; pc> 0; pc--){
		p= getInstrPtr(mb,pc);
		if( (p->barrier == BARRIERsymbol || p->barrier == CATCHsymbol) &&
		    getDestVar(p)== varid)
			return pc;
	}
	return 0;
}
int
getBlockExit(MalBlkPtr mb,int pc){
	InstrPtr p;
	int varid;
	p= getInstrPtr(mb,pc);
	if( p->barrier != BARRIERsymbol && p->barrier != CATCHsymbol)
		return 0;
	varid= getDestVar(p);
	for(pc++; pc< mb->stop; pc++){
		p= getInstrPtr(mb,pc);
		if( p->barrier == EXITsymbol && getDestVar(p)== varid)
			return pc;
	}
	return 0;
}
/*
 * Variable declaration
 * Variables are implicitly declared upon first use.
 * This feature may become a source of runtime errors and
 * complicates the analyse during optimization.
 * Therefore, in line with the flow of control check,
 * we make sure that all variables are properly initialized
 * before being used. Since barrier blocks may be skipped at
 * runtime, they actually introduce a separate scope.
 * Variables declared within a block may not be used outside it.
 * Variables can only be declared once.
 *
 * In many situation chkFlow and chkDeclarations should be called
 * together. Moreover, an erroneous chkFlow most likely implies
 * errors in the declarations as well.
 *
 * Since in interactive mode each statement is handled separately,
 * we have to remember the scope assigned to a variable.
 */
void clrDeclarations(MalBlkPtr mb){
	int i;
	for(i=0;i<mb->vtop; i++){
		clrVarInit(mb,i);
		clrVarUsed(mb,i);
		clrVarDisabled(mb,i);
	}
}

str
chkDeclarations(MalBlkPtr mb){
	int pc,i, k,l;
	InstrPtr p, sig;
	short blks[MAXDEPTH], top= 0, blkId=1;
	int dflow = -1;
	str msg = MAL_SUCCEED;

	if( mb->errors)
		return GDKstrdup(mb->errors);
	blks[top] = blkId;

	/* initialize the scope */
	for(i=0; i< mb->vtop; i++)
		setVarScope(mb,i,0);

	/* all signature variables are declared at outer level */
	sig= getInstrPtr(mb,0);
	for(k=0; k<sig->argc; k++)
		setVarScope(mb, getArg(sig, k), blkId);

	for(pc=1;pc<mb->stop; pc++){
		p= getInstrPtr(mb,pc);
		if ( p->token == REMsymbol || p->token == NOOPsymbol)
			continue;
		/* check correct use of the arguments*/
		for(k=p->retc;k<p->argc; k++) {
			l=getArg(p,k);
			if ( l < 0)
					throw(MAL,"chkFlow",  "%s.%s Non-declared variable: pc=%d, var= %d",  getModuleId(sig), getFunctionId(sig), pc, k);
			setVarUsed(mb,l);
			if( getVarScope(mb,l) == 0){
				/*
				 * The problem created here is that only variables are
				 * recognized that are declared through instructions.
				 * For interactive code, and code that is based on a global
				 * stack this is insufficient. In those cases, the variable
				 * can be defined in a previous execution.
				 * We have to recognize if the declaration takes place
				 * in the context of a global stack.
				 */
				if( p->barrier == CATCHsymbol){
					setVarScope(mb, l, blks[0]);
				} else if( !( isVarConstant(mb, l) || isVarTypedef(mb,l)) &&
					!isVarInit(mb,l) ) {
					throw(MAL,"chkFlow",  "%s.%s '%s' may not be used before being initialized",  getModuleId(sig), getFunctionId(sig), getVarName(mb,l));
				}
			} else if( !isVarInit(mb,l) ){
			    /* is the block still active ? */
			    for( i=0; i<= top; i++)
				if( blks[i] == getVarScope(mb,l) )
					break;
			    if( i> top || blks[i]!= getVarScope(mb,l) )
			        throw( MAL,"chkFlow",  "%s.%s '%s' used outside scope", getModuleId(sig), getFunctionId(sig), getVarName(mb,l));
			}
			if( blockCntrl(p) || blockStart(p) )
				setVarInit(mb, l);
		}
		/* define variables */
		for(k=0; k<p->retc; k++){
			l= getArg(p,k);
			if (isVarInit(mb, l) && getVarScope(mb,l) == 0) {
				/* first time we see this variable and it is already
				 * initialized: assume it exists globally */
				setVarScope(mb, l, blks[0]);
			}
			setVarInit(mb,l);
			if( getVarScope(mb,l) == 0){
				/* variable has not been defined yet */
				/* exceptions are always declared at level 1 */
				if( p->barrier == CATCHsymbol)
					setVarScope(mb, l, blks[0]);
				else
					setVarScope(mb, l, blks[top]);
			}
			if( blockCntrl(p) || blockStart(p) )
				setVarUsed(mb, l);
		}
		if( p->barrier && msg == MAL_SUCCEED){
			if ( blockStart(p)){
				if( top == MAXDEPTH-2)
					throw(MAL,"chkFlow",  "%s.%s too deeply nested  MAL program",  getModuleId(sig), getFunctionId(sig));
				blkId++;
				if (getModuleId(p) && getFunctionId(p) && strcmp(getModuleId(p),"language")==0 && strcmp(getFunctionId(p),"dataflow")== 0){
					if( dflow != -1)
						throw(MAL,"chkFlow",  "%s.%s setLifeSpan nested dataflow blocks not allowed", getModuleId(sig), getFunctionId(sig));
					dflow= blkId;
				}
				blks[++top]= blkId;
			}
			if( blockExit(p) && top > 0 ){
				if( dflow == blkId){
					dflow = -1;
				} else
				/*
				 * At the end of the block we should reset the status of all variables
				 * defined within the block. For, the block could have been skipped
				 * leading to uninitialized variables.
				 */
				for (l = 0; l < mb->vtop; l++)
				if( getVarScope(mb,l) == blks[top]){
					setVarScope(mb,l, 0);
					clrVarInit(mb,l);
				}
			    top--;
			}
		}
	}
	return msg;
}
