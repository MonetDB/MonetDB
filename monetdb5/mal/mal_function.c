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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @a M. Kersten
 * @v 0.0
 * @+ Functions
 * MAL comes with a standard functional abstraction scheme.
 * Functions are represented by MAL instruction lists, enclosed
 * by a @sc{function} signature
 * and @sc{end} statement. The @sc{function}
 * signature lists the arguments and their types.
 * The @sc{end} statement marks the end of this sequence.
 * Its argument is the function name.
 *
 * An illustrative example is:
 * @example
 * function user.helloWorld(msg:str):str;
 *     io.print(msg);
 *     msg:= "done";
 *     return msg;
 * end user.helloWorld;
 * @end example
 *
 * The module name 'user' designates the collection to which this function
 * belongs.
 * A missing module name is considered a reference to the current module,
 * i.e. the last module or atom context openend.
 * All user defined functions are assembled in the module @sc{user}
 * by default.
 *
 * The functional abstraction scheme comes with several variations:
 *  @sc{commands}, @sc{patterns}, and @sc{factories}.
 * They are discussed shortly.
 * @menu
 * * Polymorphic Functions ::
 * * C Functions::
 * @end menu
 * @-
 * The information maintained for each MAL function should both
 * be geared towards fast execution and to ease symbolic debugging.
 */
/*
 * @-
 * The MAL function blocks are constructed incrementally while parsing the source.
 * The function kind determines its semantics. It is taken from the list
 * FUNCTION, FACTORY, COMMAND, and PATTERN.
 */
#include "monetdb_config.h"
#include "mal_function.h"
#include "mal_resolve.h"	/* for isPolymorphic() & chkProgram() */
#include "mal_interpreter.h"	/* for showErrors() */
#include "mal_namespace.h"

Symbol newFunction(str mod, str nme,int kind){
	Symbol s;
	InstrPtr p;

	s = newSymbol(nme,kind);
	if (s == NULL)
		return NULL;
	p = newInstruction(NULL,kind);
	if (p == NULL)
		return NULL;
	setModuleId(p, mod);
	setFunctionId(p, nme);
	setDestVar(p, newVariable(s->def,GDKstrdup(nme),TYPE_any));
	pushInstruction(s->def,p);
	return s;
}
InstrPtr newCall(Module scope, str fcnname, int kind){
	InstrPtr p;
	p= newInstruction(NULL,kind);
	if (p == NULL)
		return NULL;
	setModuleScope(p, scope);
	setFunctionId(p, putName(fcnname,strlen(fcnname)));
	return p;
}
/*
 * @-
 * Optimizers may be interested in the function definition
 * for obtaining properties. Rather then polution of the
 * instruction record with a scope reference, we use a lookup function until it
 * becomes a performance hindrance.
 */
Symbol  getFunctionSymbol(Module scope, InstrPtr p){
	Module m;
	Symbol s;

	for(m= findModule(scope,getModuleId(p)); m; m= m->outer)
		if(idcmp(m->name, getModuleId(p))==0 ) {
				s= m->subscope[(int)(getSubScope(getFunctionId(p)))];
				for(; s; s= s->peer)
				if( getSignature(s)->fcn == p->fcn) return s;
		}
	return 0;
}
/*
 * @- Flow of control
 * The nesting of (BARRIER, CATCH) and EXIT statements with their associated
 * flow of control primitives LEAVE, REDO and RAISE should form a valid
 * hierarchy. Failure to comply is considered a structural error
 * and leads to flagging the function as erroneous.
 *
 * Check barrier should ensure that both exit-points of a block for the
 * variable referenced in 'pp' exists. In addition, we should ensure
 * proper weaveing of the begin-end pairs. This can simply be checked by
 * counting the begin/end pairs. It should balance for every block.
 * Currently, the barrier control variables should be of type bit,
 * sht, int, or lng. A number zero is interpreted as end of the barrier
 * block.
 *
 * To speed-up interpretation of the control statements, we could also
 * include the program-counter in the instruction record. However, this implies
 * that any subsequent change to a program, i.e. by the optimizers,
 * should be followed by a call to recalculate the PC.
 * For the time being it will be a linear scan.
 *
 */
int getPC(MalBlkPtr mb, InstrPtr p)
{   int i;
	for( i=0;i<mb->stop; i++)
	if( getInstrPtr(mb,i)==p) return i;
	return -1;
}
/*
 * @-
 * Checking the control flow structure is done by a single pass over the
 * MAL program after the program has been type-checked.
 * It should inspect all BARRIER and CATCH blocks for proper structure.
 * If the flow is correct and not dependent on an undefined typed instruction
 * we avoid doing this check any further.
 */
#define DEPTH 128

void chkFlow(MalBlkPtr mb)
{   int i,j,k, v,lastInstruction;
	int  pc[DEPTH];
	int  var[DEPTH];
	InstrPtr stmt[DEPTH];
	int btop=0;
	int retseen=0, yieldseen=0;
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
			    showScriptException(mb,i,SYNTAX,
					"Too many nested MAL blocks");
			    mb->errors++;
			    return;
			}
			pc[btop]= i;
			v= var[btop]= getDestVar(p);
			stmt[btop]=p;

			for(j=btop-1;j>=0;j--)
			if( v==var[j]){
			    showScriptException(mb,i,SYNTAX,
					"recursive %s[%d] shields %s[%d]",
						getVarName(mb,v), pc[j],
						getFcnName(mb),pc[i]);
			    mb->errors++;
			    return;
			}

			if( getVarType(mb,v) != TYPE_bit &&
			    getVarType(mb,v) != TYPE_int &&
			    getVarType(mb,v) != TYPE_str &&
			    getVarType(mb,v) != TYPE_lng &&
			    getVarType(mb,v) != TYPE_oid &&
			    getVarType(mb,v) != TYPE_sht &&
			    !isaBatType(getVarType(mb,v)) &&
			    getVarType(mb,v) != TYPE_chr &&
			    getVarType(mb,v) != TYPE_bte &&
			    getVarType(mb,v) != TYPE_wrd
				){
			    showScriptException(mb,i,TYPE,
					"barrier '%s' should be of type bit, str or number",
					getVarName(mb, v));
					mb->errors++;
			}
			btop++;
			if( p->typechk != TYPE_RESOLVED) fixed =0;
			break;
		case EXITsymbol:
			v= getDestVar(p);
			if( btop>0 && var[btop-1] != v){
			    mb->errors++;
			    showScriptException(mb,i,SYNTAX,
					"exit-label '%s' doesnot match '%s'",
					getVarName(mb,v), getVarName(mb,var[btop-1]));
			}
			if(btop==0){
			    showScriptException(mb,i,SYNTAX,
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
			    showScriptException(mb,i,SYNTAX,
					"label '%s' not in guarded block",nme);
			    mb->errors++;
			} else
			if( p->typechk != TYPE_RESOLVED) fixed =0;
			break;
		case YIELDsymbol:
			{ InstrPtr ps= getInstrPtr(mb,0);
			if( ps->token != FACTORYsymbol){
			    showScriptException(mb,i,SYNTAX,"yield misplaced!");
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
					showScriptException(mb, i, SYNTAX,
							"invalid return target!");
					mb->errors++;
				} else if (ps->typechk == TYPE_RESOLVED)
					for (e = 0; e < p->retc; e++) {
						if (resolveType(getArgType(mb, ps, e), getArgType(mb, p, e)) < 0) {
							str tpname = getTypeName(getArgType(mb, p, e));
							showScriptException(mb, i, TYPE,
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
		default:
			if( isaSignature(p) ){
				if( p->token == REMsymbol){
					/* do nothing */
				} else if( i) {
					str msg=instruction2str(mb,0,p,TRUE);
					showScriptException(mb,i,SYNTAX,"signature misplaced\n!%s",msg);
					GDKfree(msg);
					mb->errors++;
				}
			}
		}
	}
	if( lastInstruction < mb->stop-1 ){
		showScriptException(mb,lastInstruction,SYNTAX,
			"instructions after END");
#ifdef DEBUG_MAL_FCN
		printFunction(GDKout, mb, 0, LIST_MAL_ALL);
#endif
		mb->errors++;
	}
	for(btop--; btop>=0;btop--){
		showScriptException(mb,lastInstruction, SYNTAX,
			"barrier '%s' without exit in %s[%d]",
				getVarName(mb,var[btop]),getFcnName(mb),i);
		mb->errors++;
	}
	p= getInstrPtr(mb,0);
	if( !isaSignature(p)){
		showScriptException(mb,0,SYNTAX,"signature missing");
		mb->errors++;
	}
	if( retseen == 0){
		if( getArgType(mb,p,0)!= TYPE_void &&
			(p->token==FUNCTIONsymbol || p->token==FACTORYsymbol)){
			showScriptException(mb,0,SYNTAX,"RETURN missing");
			mb->errors++;
		}
	}
	if ( yieldseen && getArgType(mb,p,0)!= TYPE_void){
			showScriptException(mb,0,SYNTAX,"RETURN missing");
		mb->errors++;
	}
	if( mb->errors == 0 )
		mb->flowfixed = fixed; /* we might not have to come back here */
}
/*
 * @-
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
/*
 * @-
 * @node Polymorphic Functions, C Functions, MAL Functions, MAL Functions
 * @- Polymorphic Functions
 * Polymorphic functions are characterised by type variables
 * denoted by @sc{:any} and an optional index.
 * Each time a polymorphic MAL function is called, the
 * symbol table is first inspected for the matching strongly typed
 * version.  If it does not exists, a copy of the MAL program
 * is generated, whereafter the type
 * variables are replaced with their concrete types.
 * The new MAL program is immediately type checked and, if
 * no errors occured, added to the symbol table.
 *
 * The generic type variable @sc{:any} designates an unknown type, which may
 * be filled at type resolution time. Unlike indexed polymorphic type
 * arguments, @sc{:any} type arguments match possibly with different
 * concrete types.
 *
 * An example of a parameterised function is shown below:
 * @example
 * function user.helloWorld(msg:any_1):any_1;
 *     io.print(msg);
 *     return user.helloWorld;
 * end helloWorld;
 * @end example
 * The type variables ensure that the return type equals the
 * argument type. Type variables can be used at any place
 * where a type name is permitted.
 * Beware that polymorphic typed variables are propagated
 * throughout the function body. This may invalidate
 * type resolutions decisions taken  earlier (See @ref{MAL Type System}).
 *
 * This version of @sc{helloWorld} can also be used for
 * other arguments types, i.e. @sc{bit,sht,lng,flt,dbl,...}.
 * For example, calling @sc{helloWorld(3.14:flt)} echoes
 * a float value.
 *
 * @node C Functions, MAL Factories, Polymorphic Functions, MAL Functions
 * @- C functions
 * The MAL function body can also be implemented with a C-function.
 * They are introduced to the MAL type checker by providing their
 * signature and an @sc{address} qualifier for linkage.
 *
 * We distinguish both @sc{command} and @sc{pattern} C-function blocks.
 * They differ in the information accessible at run time. The @sc{command}
 * variant calls the underlying C-function, passing pointers to the arguments
 * on the MAL runtime stack. The @sc{pattern} command is passed pointers
 * to the MAL definition block, the runtime stack, and the instruction itself.
 * It can be used to analyse the types of the arguments directly.
 *
 * For example, the definitions below link the kernel routine @sc{BKCinsert_bun}
 * with the function @sc{bat.insert()}.
 * It does not fully specify the result type.
 * The @sc{io.print()} pattern applies to any BAT argument list,
 * provided they match on the head column type.
 * Such a polymorphic type list may only be used in the context of
 * a pattern.
 * @example
 * command bat.insert(b:bat[:any_1,:any_2], ht:any_1, tt:any_2)
 * 	:bat[:any_1,:any_2]
 * address BKCinsert_bun;
 *
 * pattern io.print(b1:bat[:any_1,:any]...):int
 * address IOtable;
 * @end example
 *
 * The internal representation of the MAL functions is rather traditional,
 * using C-structure to collect the necessary information.
 * Moreover, we assume that MAL functions are relatively small, up to
 * a few hundred of instructions. This assumption makes us to rely on
 * linear scans as it comes to locating information of interest.
 *
 * Patterns should not be cloned, because the alternative interpretations
 * are handled by the underlying code fragments.
 */
static void replaceTypeVar(MalBlkPtr mb, InstrPtr p, int v, malType t){
	int j,i,x,y;
#ifdef DEBUG_MAL_FCN
	mnstr_printf(GDKout,"replace type _%d by type %s\n",v,
		getTypeName(t));
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
			int head,tail;
			int hx,tx;
			head = getHeadType(x);
			tail = getTailType(x);
			hx = getHeadIndex(x);
			tx = getTailIndex(x);
			if(v && hx == v && head == TYPE_any){
			    hx =0;
			    head =t;
			}
			if(v && tx == v && tail == TYPE_any){
			    tx= 0;
			    tail = t;
			}
			y= newBatType(head,tail);
			setAnyHeadIndex(y,hx);
			setAnyTailIndex(y,tx);
			setArgType(mb,p,i,y);
#ifdef DEBUG_MAL_FCN
		mnstr_printf(GDKout," %d replaced %s->%s \n",i,getTypeName(x),getTypeName(y));
#endif
		} else
		if(getTailIndex(x) == v){
#ifdef DEBUG_MAL_FCN
		mnstr_printf(GDKout," replace x= %s polymorphic\n",getTypeName(x));
#endif
			setArgType(mb,p,i,t);
		}
#ifdef DEBUG_MAL_FCN
		else
		mnstr_printf(GDKout," non x= %s %d\n",getTypeName(x),getTailIndex(x));
#endif
	}
#ifdef DEBUG_MAL_FCN
		printInstruction(GDKout,mb,0,p,LIST_MAL_ALL);
#endif
	}
}
/*
 * @-
 * Upon cloning a function we should remove all the polymorphic flags.
 * Otherwise we may end up with a recursive clone.
 */
Symbol  cloneFunction(Module scope, Symbol proc, MalBlkPtr mb, InstrPtr p){
	Symbol new;
	int i,v;
	InstrPtr pp;

#ifdef DEBUG_CLONE
	mnstr_printf(GDKout,"clone the function %s to scope %s\n",
			proc->name,scope->name);
	printInstruction(GDKout,mb,0,p,LIST_MAL_ALL);
#endif
	new= newFunction(scope->name, proc->name, getSignature(proc)->token );
	freeMalBlk(new->def);
	new->def = copyMalBlk(proc->def);
	/* now change the definition of the original proc */
#ifdef DEBUG_CLONE
	mnstr_printf(GDKout,"CLONED VERSION\n");
	printFunction(GDKout, new->def, 0, LIST_MAL_ALL);
#endif
	/* check for errors after fixation , TODO*/
	pp = getSignature(new);
	for(i=0;i<pp->argc;i++)
	if( isPolymorphic(v= getArgType(new->def,pp,i)) ){
		int t = getArgType(mb,p,i);

		if ( v== TYPE_any)
			replaceTypeVar(new->def, pp, v, t);
		if( isaBatType(v) ){
			if( getHeadIndex(v) )
				replaceTypeVar(new->def, pp, getHeadIndex(v), getHeadType(t));
			if( getTailIndex(v) )
				replaceTypeVar(new->def, pp, getTailIndex(v), getTailType(t));
		} else
			replaceTypeVar(new->def, pp, getTailIndex(v), t);
	}
#ifdef DEBUG_MAL_FCN
	else mnstr_printf(GDKout,"%d remains %s\n",i, getTypeName(v));
#endif
	/* include the function at the proper place in the scope */
	insertSymbol(scope,new);
	/* clear polymorphic and type to force analysis*/
	for(i=0;i<new->def->stop;i++) {
		pp= getInstrPtr(new->def,i);
	    pp->typechk= TYPE_UNKNOWN;
		pp->polymorphic= 0;
	}
	/* clear type fixations */
	for(i=0;i< new->def->vtop; i++)
		clrVarFixed(new->def,i);

#ifdef DEBUG_MAL_FCN
	mnstr_printf(GDKout,"FUNCTION TO BE CHECKED\n");
	printFunction(GDKout, new->def, 0, LIST_MAL_ALL);
#endif

	/* check for errors after fixation , TODO*/
	/* beware, we should now ignore any cloning */
	if(proc->def->errors == 0) {
		chkProgram(scope,new->def);
		if( new->def->errors){
			showScriptException(new->def,0,MAL,"Error in cloned function");
#ifdef DEBUG_MAL_FCN
			printFunction(GDKout,new->def, 0, LIST_MAL_ALL);
#endif
		}
	}
#ifdef DEBUG_CLONE
	mnstr_printf(GDKout,"newly cloned function added to %s %d \n",scope->name,i);
	printFunction(GDKout,new->def, 0, LIST_MAL_ALL);
#endif
	return new;
}
/*
 * @-
 * For commands we do not have to clone the routine. We merely have to
 * assure that the type-constraints are obeyed. The resulting type
 * is returned.
 */
void
listFunction(stream *fd, MalBlkPtr mb, MalStkPtr stk, int flg, int first, int size)
{
	int i;
	if ( flg == 0)
		return;
	if (mb == NULL) {
		mnstr_printf(fd, "# function definition missing\n");
		return;
	}
	if (flg & LIST_MAPI) {
		/* a bit dirty, but only here we have the number of lines */
		mnstr_printf(fd, "&1 0 %d 1 %d\n", /* type id rows columns tuples */
				mb->stop, mb->stop);
		mnstr_printf(fd, "%% .explain # table_name\n");
		mnstr_printf(fd, "%% mal # name\n");
		mnstr_printf(fd, "%% clob # type\n");
		mnstr_printf(fd, "%% 0 # length\n");	/* unknown */
	}
	first = first<0?0:first;
	size = size < 0?-size:size;
	for (i = first; i < first +size && i < mb->stop; i++)
		printInstruction(fd, mb, stk, getInstrPtr(mb, i), flg);
}
void printFunction(stream *fd, MalBlkPtr mb, MalStkPtr stk, int flg)
{
	if (mb == NULL) {
		mnstr_printf(fd, "# function definition missing\n");
		return;
	}
	listFunction(fd,mb,stk,flg,0,mb->stop);
}

/*
 * @- Lifespan analysis
 * Optimizers may be interested in the characteristic of the
 * barrier blocks for making a decision.
 * The variables have a lifespan in the code blocks, denoted by properties
 * beginLifespan,endLifespan. The beginLifespan denotes the intruction where
 * it receives its first value, the endLifespan the last instruction in which
 * it was used as operand or target.
 *
 * If, however, the last use lies within a BARRIER block and the
 * variable is defined outside the block, we can not be sure
 * about its end of life status, because a block redo may implictly
 * revive it. For these situations we associate the endLifespan with
 * the block exit.
 *
 * In many cases, we have to determine if the lifespan interferes with
 * a optimization decision being prepared.
 * The lifespan is calculated once at the beginning of the optimizer sequence.
 * It should either be maintained to reflect the most accurate situation while
 * optimizing the code base. In particular it means that any move/remove/addition
 * of an instruction calls for either a recalculation or delta propagation.
 * Unclear what will be the best strategy. For the time being we just recalc.
 *
 * Also take care of the nested block structure. Because the span should
 * fall within a single block. This is handled by the chkflow already.
 *
 * The lifespan assumes that any potential conflict of variable
 * declaration within blocks have been detected already by chkDeclarations().
 * A variable may be introduced only once.
 * @-
 * The scope of a variable should respect the guarded blocks.
 * Variables defined outside, can not be freed insight the
 * block, due to possible REDO actions.
 * Variables defined within the block can be finished at
 * any time, but at EXIT at the latest.
 */
Lifespan
setLifespan(MalBlkPtr mb)
{
	int pc, k, depth=0, prop;
	InstrPtr p;
	int *blk;
	Lifespan span= newLifespan(mb);

	memset((char*) span,0, sizeof(LifespanRecord)* mb->vtop);
	prop = PropertyIndex("transparent");

	blk= (int *) GDKzalloc(sizeof(int)*mb->vtop);

	for (pc = 0; pc < mb->stop; pc++) {
		p = getInstrPtr(mb, pc);
		if( p->token == NOOPsymbol)
			continue;

		if( blockStart(p) && varGetProp(mb, getArg(p,0), prop) == NULL)
			depth++;

		for (k = 0; k < p->argc; k++) {
			int v = getArg(p,k);

			if (span[v].beginLifespan == 0 ){
				span[v].beginLifespan = pc;
				blk[v]= depth;
			}
			if (k < p->retc )
				span[v].lastUpdate= pc;
			if ( blk[v] == depth )
				span[v].endLifespan = pc;

			if ( k >= p->retc && blk[v] < depth )
				span[v].endLifespan = -1;	/* declared in outer scope*/
		}
		/*
		 * @-
		 * At a block exit we can finalize all variables defined within that block.
		 * This does not hold for dataflow blocks. They merely direct the execution
		 * thread, not the syntactic scope.
		 */
		if( blockExit(p) ){
			for (k = 0; k < mb->vtop; k++)
			if ( span[k].endLifespan == -1)
				span[k].endLifespan = pc;
			else
			if ( span[k].endLifespan == 0 && blk[k]==depth)
				span[k].endLifespan = pc;
			if (varGetProp(mb, getArg(p,0), prop) == NULL )
				depth--;
		}
	}
	for (k = 0; k < mb->vtop; k++)
	if ( span[k].endLifespan == 0 )
		span[k].endLifespan = pc-2;/* generate them before the end */
	GDKfree(blk);
	return span;
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
/*
 * @-
 * Searching the beginning or end of an instruction block.
 */
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
 * @- Garbage collection
 * Variables are marked with their last line of use. At that point they
 * can be garbage collected by the interpreter. Care should be taken
 * for variables defined within a barrier block, they can be garbage collected
 * at the end only due to possible redo-statements.
 */
void
malGarbageCollector(MalBlkPtr mb)
{
	int i;
	Lifespan span;

	span = setLifespan(mb);
	if ( span == NULL)
		return ;

	for (i = 0; i < mb->vtop; i++)
	if( isVarCleanup(mb,i) && getEndLifespan(span,i) >= 0) {
		mb->var[i]->eolife = getEndLifespan(span,i);
		mb->stmt[mb->var[i]->eolife]->gc |= GARBAGECONTROL;
	}
	GDKfree(span);
}
/*
 * @- Variable declaration
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

void chkDeclarations(MalBlkPtr mb){
	int pc,i, k,l;
	InstrPtr p;
	short blks[MAXDEPTH], top= 0, blkId=1;
	int *decl;

	decl = (int*) GDKzalloc(sizeof(int) * mb->vtop);
	if ( decl == NULL) {
		showScriptException(mb,0,SYNTAX, MAL_MALLOC_FAIL);
		mb->errors = 1;
		return;
	}
	blks[top] = blkId;

	/* all signature variables are declared at outer level */
	p= getInstrPtr(mb,0);
	for(k=0;k<p->argc; k++)
		decl[getArg(p,k)]= blkId;

	for(pc=1;pc<mb->stop; pc++){
		p= getInstrPtr(mb,pc);
		if ( p->token == REMsymbol || p->token == NOOPsymbol)
			continue;
		/* check correct use of the arguments*/
		for(k=p->retc;k<p->argc; k++) {
			l=getArg(p,k);
			setVarUsed(mb,l);
			if( decl[l] == 0){
				/*
				 * @-
				 * The problem created here is that only variables are
				 * recognized that are declared through instructions.
				 * For interactive code, and code that is based on a global
				 * stack this is insufficient. In those cases, the variable
				 * can be defined in a previous execution.
				 * We have to recognize if the declaration takes place
				 * in the context of a global stack.
				 */
				if( p->barrier == CATCHsymbol){
					decl[l] = blks[0];
				} else
				if( !( isVarConstant(mb, l) || isVarTypedef(mb,l)) &&
					!isVarInit(mb,l) ) {
					showScriptException(mb,pc,TYPE,
						"'%s' may not be used before being initialized",
						getVarName(mb,l));
					mb->errors++;
				}
			} else
			if( !isVarInit(mb,l) ){
			    /* is the block still active ? */
			    for( i=0; i<= top; i++)
					if( blks[i] == decl[l] )
						break;
			    if( i> top || blks[i]!= decl[l] ){
			            showScriptException(mb,pc,TYPE,
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
			if (isVarInit(mb, l) && decl[l] == 0) {
				/* first time we see this variable and it is already
				 * initialized: assume it exists globally */
				decl[l] = blks[0];
			}
			setVarInit(mb,l);
			if( decl[l] == 0){
				/* variable has not been defined yet */
				/* exceptions are always declared at level 1 */
				if( p->barrier == CATCHsymbol)
					decl[l] = blks[0];
				else
					decl[l] = blks[top];
#ifdef DEBUG_MAL_FCN
				mnstr_printf(GDKout,"defined %s in block %d\n",
					getVarName(mb,l),decl[l]);
#endif
			}
			if( blockCntrl(p) || blockStart(p) )
				setVarUsed(mb, l);
		}
		if( p->barrier){
			if ( blockStart(p) &&
			varGetProp(mb, getArg(p,0), PropertyIndex("transparent")) == NULL){
				if( top <MAXDEPTH-2){
					blks[++top]= ++blkId;
#ifdef DEBUG_MAL_FCN
				mnstr_printf(GDKout,"new block %d at top %d\n",blks[top], top);
#endif
				} else {
					showScriptException(mb,pc,SYNTAX, "too deeply nested  MAL program");
					mb->errors++;
					GDKfree(decl);
					return;
				}
			}
			if( blockExit(p) && top > 0 &&
			varGetProp(mb, getArg(p,0), PropertyIndex("transparent")) == NULL){
#ifdef DEBUG_MAL_FCN
				mnstr_printf(GDKout,"leave block %d at top %d\n",blks[top], top);
#endif
				/*
				 * @-
				 * At the end of the block we should reset the status of all variables
				 * defined within the block. For, the block could have been skipped
				 * leading to uninitialized variables.
				 */
				for (l = 0; l < mb->vtop; l++)
				if( decl[l] == blks[top]){
					decl[l] =0;
					clrVarInit(mb,l);
				}
			    top--;
			}
		}
	}
	GDKfree(decl);
}
/*
 * @-
 * Data flow analysis.
 * Flow graph display is handy for debugging and analysis.
 * A better flow analysis is needed, which takes into account
 * loops and side-effect functions.
 */
static void
showOutFlow(MalBlkPtr mb, int pc, int varid, stream *f)
{
	InstrPtr p;
	int i, k,found;

	for (i = pc + 1; i < mb->stop - 1; i++) {
		p = getInstrPtr(mb, i);
		found=0;
		for (k = p->retc; k < p->argc; k++) {
			if (p->argv[k] == varid ) {
				mnstr_printf(f, "n%d -> n%d\n", pc, i);
				found++;
			}
		}
		/* stop as soon you find a re-assignment */
		for (k = 0; k < p->retc; k++) {
			if (getArg(p,k) == varid)
				i = mb->stop;
		}
		/* or a side-effect usage */
		if( found &&
			(p->retc== 0 || getArgType(mb,p,0)== TYPE_void) )
				i = mb->stop;
	}
}
static void
showInFlow(MalBlkPtr mb, int pc, int varid, stream *f)
{
	InstrPtr p;
	int i, k;
	/* find last use, needed for operations with side effects */
	for (i = pc -1; i >= 0; i-- ){
		p = getInstrPtr(mb, i);
		for (k = 0; k < p->argc; k++)
			if (p->argv[k] == varid  ){
				mnstr_printf(f, "n%d -> n%d\n",i, pc);
				return;
			}
	}
}

/*
 * @-
 * We only display the minimal debugging information. The remainder
 * can be obtained through the profiler.
 */
static void
showFlowDetails(MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int pc, stream *f)
{
	str s, msg;

	(void) stk;		/* fool the compiler */
	msg = instruction2str(mb,stk, p, LIST_MAL_DEBUG);
	mnstr_printf(f, "n%d [fontsize=8, shape=box, label=\"", pc);
	for (s = msg; *s; s++)
		if (*s == '"')
			mnstr_printf(f, "\\\"");
		else
			mnstr_printf(f, "%c", *s);
	GDKfree(msg);
	mnstr_printf(f, "\"];\n");
}

void
showFlowGraph(MalBlkPtr mb, MalStkPtr stk, str fname)
{
	stream *f;
	InstrPtr p;
	int i, k;

	(void) stk;		/* fool the compiler */

	if (idcmp(fname, "stdout") == 0)
		f = GDKout;
	else
		f = open_wastream(fname);
	p = getInstrPtr(mb, 0);
	mnstr_printf(f, "digraph %s{\n", getFunctionId(p));
	p = getInstrPtr(mb, 0);
	showFlowDetails(mb, stk, p, 0, f);
	for (k = p->retc; k < p->argc; k++) {
		showOutFlow(mb, 0, p->argv[k], f);
	}
	for (i = 1; i < mb->stop ; i++) {
		p = getInstrPtr(mb, i);

		showFlowDetails(mb, stk, p, i, f);

		for (k = 0; k < p->retc; k++)
				showOutFlow(mb, i, p->argv[k], f);

		if( p->retc== 0 || getArgType(mb,p,0)== TYPE_void) /* assume side effects */
		for (k = p->retc; k < p->argc; k++)
			if (getArgType(mb,p,k) != TYPE_void &&
				!isVarConstant(mb,getArg(p,k)))
				showOutFlow(mb, i, p->argv[k], f);

		if( getFunctionId(p)== 0)
			for (k =0; k< p->retc; k++)
				if( getArgType(mb,p,k) != TYPE_void)
					showInFlow(mb, i, p->argv[k], f);
		if ( p->token == ENDsymbol)
			break;
	}
	mnstr_printf(f, "}\n");
	if (f != GDKout)
		mnstr_close(f);
}

