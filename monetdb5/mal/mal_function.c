/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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

Symbol newFunction(str mod, str nme,int kind){
	Symbol s;
	InstrPtr p;
	int varid;

	s = newSymbol(nme,kind);
	if (s == NULL)
		return NULL;

	varid = newVariable(s->def,nme,strlen(nme),TYPE_any);
	if( varid < 0){
		freeSymbol(s);
		return NULL;
	}

	p = newInstruction(NULL,kind);
	if (p == NULL) {
		freeSymbol(s);
		return NULL;
	}
	setModuleId(p, mod);
	setFunctionId(p, nme);
	setDestVar(p, varid);
	pushInstruction(s->def,p);
	return s;
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

void chkFlow(stream *out, MalBlkPtr mb)
{   int i,j,k, v,lastInstruction;
	int  pc[DEPTH];
	int  var[DEPTH];
	InstrPtr stmt[DEPTH];
	int btop=0;
	int endseen=0, retseen=0, yieldseen=0;
	int fixed=1;
	InstrPtr p;

	if ( mb->errors)
		return;
	lastInstruction = mb->stop-1;
	for(i= 0; i<mb->stop; i++){
		p= getInstrPtr(mb,i);
		/* we have to keep track on the maximal arguments/block
		  because it is needed by the interpreter */
		if( mb->maxarg < p->maxarg)
			mb->maxarg= p->maxarg;
		switch( p->barrier){
		case BARRIERsymbol:
		case CATCHsymbol:
			if(btop== DEPTH){
			    showScriptException(out, mb,i,SYNTAX,
					"Too many nested MAL blocks");
			    mb->errors++;
			    return;
			}
			pc[btop]= i;
			v= var[btop]= getDestVar(p);
			stmt[btop]=p;

			for(j=btop-1;j>=0;j--)
			if( v==var[j]){
			    showScriptException(out, mb,i,SYNTAX,
					"recursive %s[%d] shields %s[%d]",
						getVarName(mb,v), pc[j],
						getFcnName(mb),pc[i]);
			    mb->errors++;
			    return;
			}

			btop++;
			if( p->typechk != TYPE_RESOLVED) fixed =0;
			break;
		case EXITsymbol:
			v= getDestVar(p);
			if( btop>0 && var[btop-1] != v){
			    mb->errors++;
			    showScriptException(out, mb,i,SYNTAX,
					"exit-label '%s' doesnot match '%s'",
					getVarName(mb,v), getVarName(mb,var[btop-1]));
			}
			if(btop==0){
			    showScriptException(out, mb,i,SYNTAX,
					"exit-label '%s' without begin-label",
					getVarName(mb,v));
			    mb->errors++;
			    continue;
			}
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
			if( p->typechk != TYPE_RESOLVED) fixed =0;
			break;
		case LEAVEsymbol:
		case REDOsymbol:
			v= getDestVar(p);
			for(j=btop-1;j>=0;j--)
			if( var[j]==v) break;
			if(j<0){
				str nme= getVarName(mb,v);
			    showScriptException(out, mb,i,SYNTAX,
					"label '%s' not in guarded block",nme);
			    mb->errors++;
			} else
			if( p->typechk != TYPE_RESOLVED) fixed =0;
			break;
		case YIELDsymbol:
			{ InstrPtr ps= getInstrPtr(mb,0);
			if( ps->token != FACTORYsymbol){
			    showScriptException(out, mb,i,SYNTAX,"yield misplaced!");
			    mb->errors++;
			}
			yieldseen= TRUE;
			 }
		case RETURNsymbol:
			{
				InstrPtr ps = getInstrPtr(mb, 0);
				int e;
				if (p->barrier == RETURNsymbol)
					yieldseen = FALSE;    /* always end with a return */
				if (ps->retc != p->retc) {
					showScriptException(out, mb, i, SYNTAX,
							"invalid return target!");
					mb->errors++;
				} else 
				if (ps->typechk == TYPE_RESOLVED)
					for (e = 0; e < p->retc; e++) {
						if (resolveType(getArgType(mb, ps, e), getArgType(mb, p, e)) < 0) {
							str tpname = getTypeName(getArgType(mb, p, e));
							showScriptException(out, mb, i, TYPE,
									"%s type mismatch at type '%s'",
									(p->barrier == RETURNsymbol ? "RETURN" : "YIELD"), tpname);
							GDKfree(tpname);
							mb->errors++;
						}
					}
				if (ps->typechk != TYPE_RESOLVED) fixed = 0;
			}
			if (btop == 0)
				retseen = 1;
			break;
	    case RAISEsymbol:
	        break;
	    case ENDsymbol:
			endseen =1;
	        break;
		default:
			if( isaSignature(p) ){
				if( p->token == REMsymbol){
					/* do nothing */
				} else if( i) {
					str msg=instruction2str(mb,0,p,TRUE);
					showScriptException(out, mb,i,SYNTAX,"signature misplaced\n!%s",msg);
					GDKfree(msg);
					mb->errors++;
				}
			}
		}
	}
	if( lastInstruction < mb->stop-1 ){
		showScriptException(out, mb,lastInstruction,SYNTAX,
			"instructions after END");
#ifdef DEBUG_MAL_FCN
		printFunction(out, mb, 0, LIST_MAL_ALL);
#endif
		mb->errors++;
	}
	if( endseen)
	for(btop--; btop>=0;btop--){
		showScriptException(out, mb,lastInstruction, SYNTAX,
			"barrier '%s' without exit in %s[%d]",
				getVarName(mb,var[btop]),getFcnName(mb),i);
		mb->errors++;
	}
	p= getInstrPtr(mb,0);
	if( !isaSignature(p)){
		showScriptException(out, mb,0,SYNTAX,"signature missing");
		mb->errors++;
	}
	if( retseen == 0){
		if( getArgType(mb,p,0)!= TYPE_void &&
			(p->token==FUNCTIONsymbol || p->token==FACTORYsymbol)){
			showScriptException(out, mb,0,SYNTAX,"RETURN missing");
			mb->errors++;
		}
	}
	if ( yieldseen && getArgType(mb,p,0)!= TYPE_void){
			showScriptException(out, mb,0,SYNTAX,"RETURN missing");
		mb->errors++;
	}
	if( mb->errors == 0 )
		mb->flowfixed = fixed; /* we might not have to come back here */
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
#ifdef DEBUG_MAL_FCN
	char *tpenme = getTypeName(t);
	mnstr_printf(GDKout,"replace type _%d by type %s\n",v, tpenme);
	GDKfree(tpenme);
#endif
	for(j=0; j<mb->stop; j++){
	    p= getInstrPtr(mb,j);
#ifdef DEBUG_MAL_FCN
		printInstruction(GDKout,mb,0,p,LIST_MAL_ALL);
#endif
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
#ifdef DEBUG_MAL_FCN
			{
				char *xnme = getTypeName(x), *ynme = getTypeName(y);
				mnstr_printf(GDKout," %d replaced %s->%s \n",i,xnme,ynme);
				GDKfree(xnme);
				GDKfree(ynme);
			}
#endif
		} else
		if(getTypeIndex(x) == v){
#ifdef DEBUG_MAL_FCN
			char *xnme = getTypeName(x);
			mnstr_printf(GDKout," replace x= %s polymorphic\n",xnme);
			GDKfree(xnme);
#endif
			setArgType(mb,p,i,t);
		}
#ifdef DEBUG_MAL_FCN
		else {
			char *xnme = getTypeName(x);
			mnstr_printf(GDKout," non x= %s %d\n",xnme,getTypeIndex(x));
			GDKfree(xnme);
		}
#endif
	}
#ifdef DEBUG_MAL_FCN
		printInstruction(GDKout,mb,0,p,LIST_MAL_ALL);
#endif
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
cloneFunction(stream *out, Module scope, Symbol proc, MalBlkPtr mb, InstrPtr p)
{
	Symbol new;
	int i,v;
	InstrPtr pp;

#ifdef DEBUG_CLONE
	mnstr_printf(out,"clone the function %s to scope %s\n",
				 proc->name,scope->name);
	printInstruction(out,mb,0,p,LIST_MAL_ALL);
#endif
	new = newFunction(scope->name, proc->name, getSignature(proc)->token);
	if( new == NULL){
		fprintf(stderr,"cloneFunction() failed");
		return NULL;
	}
	freeMalBlk(new->def);
	new->def = copyMalBlk(proc->def);
	/* now change the definition of the original proc */
#ifdef DEBUG_CLONE
	mnstr_printf(out, "CLONED VERSION\n");
	printFunction(out, new->def, 0, LIST_MAL_ALL);
#endif
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
#ifdef DEBUG_MAL_FCN
		else {
			char *tpenme = getTypeName(v);
			mnstr_printf(out,"%d remains %s\n", i, tpenme);
			GDKfree(tpenme);
		}
#endif
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

#ifdef DEBUG_MAL_FCN
	mnstr_printf(out, "FUNCTION TO BE CHECKED\n");
	printFunction(out, new->def, 0, LIST_MAL_ALL);
#endif

	/* check for errors after fixation , TODO*/
	/* beware, we should now ignore any cloning */
	if (proc->def->errors == 0) {
		chkProgram(out, scope,new->def);
		if (new->def->errors) {
			showScriptException(out, new->def, 0, MAL,
								"Error in cloned function");
#ifdef DEBUG_MAL_FCN
			printFunction(out, new->def, 0, LIST_MAL_ALL);
#endif
		}
	}
#ifdef DEBUG_CLONE
	mnstr_printf(out, "newly cloned function added to %s %d \n",
				 scope->name, i);
	printFunction(out, new->def, 0, LIST_MAL_ALL);
#endif
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
				for(j =0; j < p->retc; j++)
					mnstr_printf(fd,"%d ",getArg(p,j));
				if( p->argc - p->retc > 0)
					mnstr_printf(fd,"<- ");
				for(; j < p->argc; j++)
					mnstr_printf(fd,"%d ",getArg(p,j));
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
	if (mb == NULL) {
		mnstr_printf(fd, "# function definition missing\n");
		return;
	}
	if ( flg == 0)
		return;
	assert(size>=0);
	assert(first>=0 && first <mb->stop);
	if (flg & LIST_MAL_MAPI) {
		size_t len = 0;
		str ps;
		mnstr_printf(fd, "&1 0 %d 1 %d\n", /* type id rows columns tuples */
				mb->stop, mb->stop);
		mnstr_printf(fd, "%% .explain # table_name\n");
		mnstr_printf(fd, "%% mal # name\n");
		mnstr_printf(fd, "%% clob # type\n");
		for (i = first; i < first +size && i < mb->stop; i++) {
			ps = instruction2str(mb, stk, getInstrPtr(mb, i), flg);
			if (ps) {
				size_t l = strlen(ps);
				if (l > len)
					len = l;
				GDKfree(ps);
			} else mnstr_printf(fd,"#failed instruction2str()\n");
		}
		mnstr_printf(fd, "%% " SZFMT " # length\n", len);
	}
	for (i = first; i < first +size && i < mb->stop; i++)
		printInstruction(fd, mb, stk, getInstrPtr(mb, i), flg);
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
		mb->var[k]->declared = 0;
		mb->var[k]->updated = 0;
		mb->var[k]->eolife = mb->stop;
	} else {
		setVarScope(mb,k,0);
		mb->var[k]->declared = 0;
		mb->var[k]->updated = 0;
		mb->var[k]->eolife = 0;
	}

	for (pc = 0; pc < mb->stop; pc++) {
		p = getInstrPtr(mb, pc);
		if( p->token == NOOPsymbol)
			continue;

		if( blockStart(p)){
			if (getModuleId(p) && getFunctionId(p) && strcmp(getModuleId(p),"language")==0 && strcmp(getFunctionId(p),"dataflow")==0){
				if( dflow != -1){
					GDKerror("setLifeSpan nested dataflow blocks not allowed" );
					mb->errors++;
				}
				dflow= depth;
			} else
				depth++;
		}

		for (k = 0; k < p->argc; k++) {
			int v = getArg(p,k);
			if( isVarConstant(mb,v) && mb->var[v]->updated == 0)
				mb->var[v]->updated= pc;

			if (mb->var[v]->declared == 0 ){
				mb->var[v]->declared = pc;
				setVarScope(mb,v,depth);
			}
			if (k < p->retc )
				mb->var[v]->updated= pc;
			if ( getVarScope(mb,v) == depth )
				mb->var[v]->eolife = pc;

			if ( k >= p->retc && getVarScope(mb,v) < depth )
				mb->var[v]->eolife = -1;
		}
		/*
		 * At a block exit we can finalize all variables defined within that block.
		 * This does not hold for dataflow blocks. They merely direct the execution
		 * thread, not the syntactic scope.
		 */
		if( blockExit(p) ){
			for (k = 0; k < mb->vtop; k++)
			if ( mb->var[k]->eolife == 0 && getVarScope(mb,k) ==depth )
				mb->var[k]->eolife = pc;
			else if ( mb->var[k]->eolife == -1 )
				mb->var[k]->eolife = pc;
			
			if( dflow == depth)
				dflow= -1;
			else depth--;
		}
	}
	for (k = 0; k < mb->vtop; k++)
		if( mb->var[k]->eolife == 0)
			mb->var[k]->eolife = mb->stop-1;
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
 * Garbage collection
 * Variables are marked with their last line of use. At that point they
 * can be garbage collected by the interpreter. Care should be taken
 * for variables defined within a barrier block, they can be garbage collected
 * at the end only due to possible redo-statements.
 */
void
malGarbageCollector(MalBlkPtr mb)
{
	int i;

	setVariableScope(mb);

	for (i = 0; i < mb->vtop; i++)
		if( isVarCleanup(mb,i) && getEndScope(mb,i) >= 0) {
			mb->var[i]->eolife = getEndScope(mb,i);
			mb->stmt[mb->var[i]->eolife]->gc |= GARBAGECONTROL;
		}
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

void chkDeclarations(stream *out, MalBlkPtr mb){
	int pc,i, k,l;
	InstrPtr p;
	short blks[MAXDEPTH], top= 0, blkId=1;
	int dflow = -1;

	blks[top] = blkId;

	/* initialize the scope */
	for(i=0; i< mb->vtop; i++)
		setVarScope(mb,i,0);

	/* all signature variables are declared at outer level */
	p= getInstrPtr(mb,0);
	for(k=0;k<p->argc; k++)
		setVarScope(mb, getArg(p,k), blkId);

	for(pc=1;pc<mb->stop; pc++){
		p= getInstrPtr(mb,pc);
		if ( p->token == REMsymbol || p->token == NOOPsymbol)
			continue;
		/* check correct use of the arguments*/
		for(k=p->retc;k<p->argc; k++) {
			l=getArg(p,k);
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
				} else
				if( !( isVarConstant(mb, l) || isVarTypedef(mb,l)) &&
					!isVarInit(mb,l) ) {
					showScriptException(out, mb,pc,TYPE,
						"'%s' may not be used before being initialized",
						getVarName(mb,l));
					mb->errors++;
				}
			} else
			if( !isVarInit(mb,l) ){
			    /* is the block still active ? */
			    for( i=0; i<= top; i++)
					if( blks[i] == getVarScope(mb,l) )
						break;
			    if( i> top || blks[i]!= getVarScope(mb,l) ){
			            showScriptException(out, mb,pc,TYPE,
							"'%s' used outside scope",
							getVarName(mb,l));
			        mb->errors++;
			    }
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
#ifdef DEBUG_MAL_FCN
				mnstr_printf(out,"defined %s in block %d\n",
					getVarName(mb,l), getVarScope(mb,l));
#endif
			}
			if( blockCntrl(p) || blockStart(p) )
				setVarUsed(mb, l);
		}
		if( p->barrier){
			if ( blockStart(p)){
				if( top == MAXDEPTH-2){
					showScriptException(out, mb,pc,SYNTAX, "too deeply nested  MAL program");
					mb->errors++;
					return;
				}
				blkId++;
				if (getModuleId(p) && getFunctionId(p) && strcmp(getModuleId(p),"language")==0 && strcmp(getFunctionId(p),"dataflow")== 0){
					if( dflow != -1){
						GDKerror("setLifeSpan nested dataflow blocks not allowed" );
						mb->errors++;
					}
					dflow= blkId;
				} 
				blks[++top]= blkId;
#ifdef DEBUG_MAL_FCN
				mnstr_printf(out,"new block %d at top %d\n",blks[top], top);
#endif
			}
			if( blockExit(p) && top > 0 ){
#ifdef DEBUG_MAL_FCN
				mnstr_printf(out,"leave block %d at top %d\n",blks[top], top);
#endif
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
}

/*
 * Data flow analysis.
 * Flow graph display is handy for debugging and analysis.
 * A better flow analysis is needed, which takes into account barrier blocks 
 */
static void
showOutFlow(MalBlkPtr mb, int pc, int varid, stream *f)
{
	InstrPtr p;
	int i, k, found;

	for (i = pc + 1; i < mb->stop - 1; i++) {
		p = getInstrPtr(mb, i);
		found = 0;
		for (k = 0; k < p->argc; k++) {
			if (p->argv[k] == varid) {
				mnstr_printf(f, "n%d -> n%d\n", pc, i);
				found++;
			}
		}
		/* stop as soon you find a re-assignment */
		for (k = 0; k < p->retc; k++) {
			if (getArg(p, k) == varid)
				i = mb->stop;
		}
		/* or a side-effect usage */
		if (found &&
			(p->retc == 0 || getArgType(mb, p, 0) == TYPE_void))
			i = mb->stop;
	}
}

static void
showInFlow(MalBlkPtr mb, int pc, int varid, stream *f)
{
	InstrPtr p;
	int i, k;

	/* find last use, needed for operations with side effects */
	for (i = pc - 1; i >= 0; i--) {
		p = getInstrPtr(mb, i);
		for (k = 0; k < p->argc; k++) {
			if (p->argv[k] == varid ) {
				mnstr_printf(f, "n%d -> n%d\n", i, pc);
				return;
			}
		}
	}
}

/*
 * We only display the minimal debugging information. The remainder
 * can be obtained through the profiler.
 */
static void
showFlowDetails(MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int pc, stream *f)
{
	(void) mb;     /* fool the compiler */
	(void) stk;     /* fool the compiler */
	mnstr_printf(f, "n%d [fontsize=8, shape=box, label=\"%s\"]\n", pc, getFunctionId(p));
}

/* the stethoscope needs dot files for its graphical interface.
 * They are produced whenever a main() is called.
 * In all cases a single dot file is produced.
 */
#define MAXFLOWGRAPHS 128

int getFlowGraphs(MalBlkPtr mb, MalStkPtr stk, MalBlkPtr *mblist, MalStkPtr *stklist,int top);
int getFlowGraphs(MalBlkPtr mb, MalStkPtr stk, MalBlkPtr *mblist, MalStkPtr *stklist,int top){
	int i;
	InstrPtr p;

	for ( i=0; i<top; i++)
	if ( mblist[i] == mb)
		return top;

	if ( top == MAXFLOWGRAPHS)
		return top; /* just bail out */
	mblist[top] = mb;
	stklist[top++] = stk;
	for (i=1; i < mb->stop; i++){
		p = getInstrPtr(mb,i);
		if ( p->token == FCNcall || p->token == FACcall )
			top =getFlowGraphs(p->blk, 0,mblist, stklist, top);
	}
	return top;
}

void
showFlowGraph(MalBlkPtr mb, MalStkPtr stk, str fname)
{
	stream *f;
	InstrPtr p;
	int i, j,k, stethoscope=0;
	char mapimode = 0;
	buffer *bufstr = NULL;
	MalBlkPtr mblist[MAXFLOWGRAPHS];
	MalStkPtr stklist[MAXFLOWGRAPHS];
	int top =0;

	(void) stk;     /* fool the compiler */

	memset(mblist, 0, sizeof(mblist));
	memset(stklist, 0, sizeof(stklist));

	if (idcmp(fname, "stdout") == 0) {
		f = GDKout;
	} else if (idcmp(fname, "stdout-mapi") == 0) {
		bufstr = buffer_create(8096);
		f = buffer_wastream(bufstr, "bufstr_write");
		mapimode = 1;
	} else {
		f = open_wastream(fname);
	}
	if ( f == NULL)
		return;

	top = getFlowGraphs(mb,stk,mblist,stklist,0);
	if ( stethoscope == 0)
		top =1;
	for( j=0; j< top; j++){
		mb = mblist[j];
		stk = stklist[j];
		if (mb == 0 || (mb->dotfile && stethoscope))
			continue; /* already sent */
		p = getInstrPtr(mb, 0);
		mnstr_printf(f, "digraph %s {\n", getFunctionId(p));
		p = getInstrPtr(mb, 0);
		showFlowDetails(mb, stk, p, 0, f);
		for (k = p->retc; k < p->argc; k++) {
			showOutFlow(mb, 0, p->argv[k], f);
		}
		for (i = 1; i < mb->stop; i++) {
			p = getInstrPtr(mb, i);

			showFlowDetails(mb, stk, p, i, f);

			for (k = 0; k < p->retc; k++)
				showOutFlow(mb, i, p->argv[k], f);

			if (p->retc == 0 || getArgType(mb, p, 0) == TYPE_void) /* assume side effects */
				for (k = p->retc; k < p->argc; k++)
					if (getArgType(mb, p, k) != TYPE_void &&
						!isVarConstant(mb, getArg(p, k)))
						showOutFlow(mb, i, p->argv[k], f);

			if (getFunctionId(p) == 0)
				for (k = 0; k < p->retc; k++)
					if (getArgType(mb, p, k) != TYPE_void)
						showInFlow(mb, i, p->argv[k], f);
			if (p->token == ENDsymbol)
				break;
		}
		mnstr_printf(f, "}\n");
		mb->dotfile++;
	}

	if (mapimode == 1) {
		size_t maxlen = 0;
		size_t rows = 0;
		str buf = buffer_get_buf(bufstr);
		str line, oline;

		/* calculate width of column, and the number of tuples */
		oline = buf;
		while ((line = strchr(oline, '\n')) != NULL) {
			if ((size_t) (line - oline) > maxlen)
				maxlen = line - oline;
			rows++;
			oline = line + 1;
		} /* see printf before this mapimode if, last line ends with \n */

		/* write mapi header */
		if ( f == GDKout) {
			mnstr_printf(f, "&1 0 " SZFMT " 1 " SZFMT "\n",
					/* type id rows columns tuples */ rows, rows);
			mnstr_printf(f, "%% .dot # table_name\n");
			mnstr_printf(f, "%% dot # name\n");
			mnstr_printf(f, "%% clob # type\n");
			mnstr_printf(f, "%% " SZFMT " # length\n", maxlen);
		}
		oline = buf;
		while ((line = strchr(oline, '\n')) != NULL) {
			*line++ = '\0';
			mnstr_printf(GDKout, "=%s\n", oline);
			oline = line;
		}
		free(buf);
	}
	if (f != GDKout) {
		if (!stethoscope ) {
			MT_sleep_ms(4000); /* delay for stethoscope */
			close_stream(f);
		}
	}
}

