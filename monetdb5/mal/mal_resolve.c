/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * (author) M. Kersten
 * 
 * Search the first definition of the operator in the current module
 * and check the parameter types.
 * For a polymorphic MAL function we make a fully instantiated clone.
 * It will be prepended to the symbol list as it is more restrictive.
 * This effectively overloads the MAL procedure.
 */
#include "monetdb_config.h"
#include "mal_resolve.h"
#include "mal_namespace.h"
#include "mal_private.h"
#include "mal_linker.h"

static malType getPolyType(malType t, int *polytype);
static int updateTypeMap(int formal, int actual, int polytype[MAXTYPEVAR]);
static int typeKind(MalBlkPtr mb, InstrPtr p, int i);

/* #define DEBUG_MAL_RESOLVE*/
#define MAXMALARG 256

str traceFcnName = "____";
int tracefcn;
int polyVector[MAXTYPEVAR];

/*
 * We found the proper function. Copy some properties. In particular,
 * determine the calling strategy, i.e. FCNcall, CMDcall, FACcall, PATcall
 * Beware that polymorphic functions may produce type-incorrect clones.
 * This piece of code may be shared by the separate binder
 */
#define bindFunction(s, p, mb)									\
	do {															\
		if (p->token == ASSIGNsymbol) {								\
			switch (getSignature(s)->token) {						\
			case COMMANDsymbol:										\
				p->token = CMDcall;									\
				p->fcn = getSignature(s)->fcn;      /* C implementation mandatory */ \
				if (p->fcn == NULL) {								\
					if(!silent)  mb->errors = createMalException(mb, getPC(mb, p), TYPE, \
										"object code for command %s.%s missing", \
										p->modname, p->fcnname);	\
					p->typechk = TYPE_UNKNOWN;						\
					goto wrapup;									\
				}													\
				break;												\
			case PATTERNsymbol:										\
				p->token = PATcall;									\
				p->fcn = getSignature(s)->fcn;      /* C implementation optional */	\
				break;												\
			case FACTORYsymbol:										\
				p->token = FACcall;									\
				p->fcn = getSignature(s)->fcn;      /* C implementation optional */	\
				break;												\
			case FUNCTIONsymbol:									\
				p->token = FCNcall;									\
				if (getSignature(s)->fcn)							\
					p->fcn = getSignature(s)->fcn;     /* C implementation optional */ \
				break;												\
			default: {												\
					if(!silent) mb->errors = createMalException(mb, getPC(mb, p), MAL, \
										"MALresolve: unexpected token type"); \
				goto wrapup;										\
			}														\
			}														\
			p->blk = s->def;										\
		}															\
	} while (0)

/*
 * Since we now know the storage type of the receiving variable, we can
 * set the garbage collection flag.
 */
#define prepostProcess(tp, p, b, mb)					\
	do {												\
		if( isaBatType(tp) ||							\
			ATOMtype(tp) == TYPE_str ||				\
			(!isPolyType(tp) && tp < TYPE_any &&		\
			 tp >= 0 && ATOMextern(tp))) {				\
			getInstrPtr(mb, 0)->gc |= GARBAGECONTROL;	\
			setVarCleanup(mb, getArg(p, b));			\
			p->gc |= GARBAGECONTROL;					\
		}												\
	} while (0)

static malType
findFunctionType(Module scope, MalBlkPtr mb, InstrPtr p, int silent)
{
	Module m;
	Symbol s;
	InstrPtr sig;
	int i, k, unmatched = 0, s1;
	int polytype[MAXTYPEVAR];
	int returns[256];
	int *returntype = NULL;
	/*
	 * Within a module find the element in its list
	 * of symbols. A skiplist is used to speed up the search for the
	 * definition of the function.
	 *
	 * For the implementation we should be aware that over 90% of the
	 * functions in the kernel have just a few arguments and a single
	 * return value.
	 * A point of concern is that polymorphic arithmetic operations
	 * lead to an explosion in the symbol table. This increase the
	 * loop to find a candidate.
	 *
	 * Consider to collect the argument type into a separate structure, because
	 * it will be looked up multiple types to resolve the instruction.[todo]
	 * Simplify polytype using a map into the concrete argument table.
	 */
#ifdef DEBUG_MAL_RESOLVE
		fprintf(stderr,"#findFunction %s.%s\n", getModuleId(p), getFunctionId(p));
#endif
	m = scope;
	s = m->space[(int) (getSymbolIndex(getFunctionId(p)))];
	if (s == 0)
		return -1;

	if ( p->retc < 256){
		for (i=0; i< p->retc; i++)
			returns[i] = 0;
		returntype = returns;
	} else {
		returntype = (int *) GDKzalloc(p->retc * sizeof(int));
		if (returntype == 0)
			return -1;
	}

	while (s != NULL) {			/* single scope element check */
		if (getFunctionId(p) != s->name) {
			s = s->skip;
			continue;
		}
		/*
		 * Perform a strong type-check on the actual arguments. If it
		 * turns out to be a polymorphic MAL function, we have to
		 * clone it.  Provided the actual/formal parameters are
		 * compliant throughout the function call.
		 *
		 * Also look out for variable argument lists. This means that
		 * we have to keep two iterators, one for the caller (i) and
		 * one for the callee (k). Since a variable argument only
		 * occurs as the last one, we simple avoid an increment when
		 * running out of formal arguments.
		 *
		 * A call of the form (X1,..., Xi) := f(Y1,....,Yn) can be
		 * matched against the function signature (B1,...,Bk):=
		 * f(A1,...,Am) where i==k , n<=m and
		 * type(Ai)=type(Yi). Furthermore, the variables Xi obtain
		 * their type from Bi (or type(Bi)==type(Xi)).
		 */
		sig = getSignature(s);
		unmatched = 0;

		/*
		 * The simple case could be taken care of separately to
		 * speedup processing
		 * However, it turned out not to make a big difference.  The
		 * first time we encounter a polymorphic argument in the
		 * signature.
		 * Subsequently, the polymorphic arguments update this table
		 * and check for any type mismatches that might occur.  There
		 * are at most 2 type variables involved per argument due to
		 * the limited type nesting permitted.  Note, each function
		 * returns at least one value.
		 */
		if (sig->polymorphic) {
			int limit = sig->polymorphic;
			if (!(sig->argc == p->argc ||
				  (sig->argc < p->argc && sig->varargs & (VARARGS | VARRETS)))
				) {
				s = s->peer;
				continue;
			}
			if (sig->retc != p->retc && !(sig->varargs & VARRETS)) {
				s = s->peer;
				continue;
			}
/*  if(polyVector[0]==0) polyInit();
    memcpy(polytype,polyVector, 2*sig->argc*sizeof(int)); */

#ifdef DEBUG_MAL_RESOLVE
		if (tracefcn && (sig->polymorphic || sig->retc == p->retc)) {
			fprintf(stderr, "#resolving: ");
			fprintInstruction(stderr, mb, 0, p, LIST_MAL_ALL);
			fprintf(stderr, "#against:");
			fprintInstruction(stderr, s->def, 0, getSignature(s), LIST_MAL_ALL);
		}
#endif
			for (k = 0; k < limit; k++)
				polytype[k] = TYPE_any;
			/*
			 * Most polymorphic functions don't have a variable argument
			 * list. So we save some instructions factoring this caise out.
			 * Be careful, the variable number of return arguments should
			 * be considered as well.
			 */
			i = p->retc;
			/* first handle the variable argument list */
			for (k = sig->retc; i < p->argc; k++, i++) {
				int actual = getArgType(mb, p, i);
				int formal = getArgType(s->def, sig, k);
				if (k == sig->argc - 1 && sig->varargs & VARARGS)
					k--;
				/*
				 * Take care of variable argument lists.
				 * They are allowed as the last in the signature only.
				 * Furthermore, for patterns if the formal type is
				 * 'any' then all remaining arguments are acceptable
				 * and detailed type analysis becomes part of the
				 * pattern implementation.
				 * In all other cases the type should apply to all
				 * remaining arguments.
				 */
				if (formal == actual)
					continue;
				if (updateTypeMap(formal, actual, polytype)) {
					unmatched = i;
					break;
				}
				formal = getPolyType(formal, polytype);
				/*
				 * Collect the polymorphic types and resolve them.
				 * If it fails, we know this isn't the function we are
				 * looking for.
				 */
				if (resolveType(formal, actual) == -1) {
					unmatched = i;
					break;
				}
			}
			/*
			 * The last argument/result type could be a polymorphic
			 * variable list.  It should only be allowed for patterns,
			 * where it can deal with the stack.  If the type is
			 * specified as :any then any mix of arguments is allowed.
			 * If the type is a new numbered type variable then the
			 * first element in the list determines the required type
			 * of all.
			 */
			if (sig->varargs) {
				if (sig->token != PATTERNsymbol)
					unmatched = i;
				else {
					/* resolve the arguments */
					for (; i < p->argc; i++) {
						/* the type of the last one has already been set */
						int actual = getArgType(mb, p, i);
						int formal = getArgType(s->def, sig, k);
						if (k == sig->argc - 1 && sig->varargs & VARARGS)
							k--;

						formal = getPolyType(formal, polytype);
						if (formal == actual || formal == TYPE_any)
							continue;
						if (resolveType(formal, actual) == -1) {
							unmatched = i;
							break;
						}
					}
				}
			}
		} else {
			/*
			 * We have to check the argument types to determine a
			 * possible match for the non-polymorphic case.
			 */
			if (sig->argc != p->argc || sig->retc != p->retc) {
				s = s->peer;
				continue;
			}
#ifdef DEBUG_MAL_RESOLVE
		if (tracefcn && (sig->polymorphic || sig->retc == p->retc)) {
			fprintf(stderr, "#resolving: ");
			fprintInstruction(stderr, mb, 0, p, LIST_MAL_ALL);
			fprintf(stderr, "#against:");
			fprintInstruction(stderr, s->def, 0, getSignature(s), LIST_MAL_ALL);
		}
#endif
			for (i = p->retc; i < p->argc; i++) {
				int actual = getArgType(mb, p, i);
				int formal = getArgType(s->def, sig, i);
				if (resolveType(formal, actual) == -1) {
#ifdef DEBUG_MAL_RESOLVE
					char *ftpe = getTypeName(formal);
					char *atpe = getTypeName(actual);
					fprintf(stderr, "#unmatched %d formal %s actual %s\n",
								 i, ftpe, atpe);
					GDKfree(ftpe);
					GDKfree(atpe);
#endif
					unmatched = i;
					break;
				}
			}
		}
		/*
		 * It is possible that you may have to coerce the value to
		 * another type.  We assume that coercions are explicit at the
		 * MAL level. (e.g. var2:= var0:int). This avoids repeated
		 * type analysis just before you execute a function.
		 * An optimizer may at a later stage automatically insert such
		 * coercion requests.
		 */
#ifdef DEBUG_MAL_RESOLVE
		if (tracefcn) {
			char *tpe, *tpe2;
			fprintf(stderr, "#finished %s.%s unmatched=%d polymorphic=%d %d",
						 getModuleId(sig), getFunctionId(sig), unmatched,
						 sig->polymorphic, p == sig);
			if (sig->polymorphic) {
				int l;
				fprintf(stderr,"poly ");
				for (l = 0; l < 2 * p->argc; l++)
					if (polytype[l] != TYPE_any) {
						tpe = getTypeName(polytype[l]);
						fprintf(stderr, " %d %s", l, tpe);
						GDKfree(tpe);
					}
				fprintf(stderr,"\n");
			}
			fprintf(stderr, "#resolving:");
			fprintInstruction(stderr, mb, 0, p, LIST_MAL_ALL);
			fprintf(stderr, "#against :");
			fprintInstruction(stderr, s->def, 0, getSignature(s), LIST_MAL_ALL);
			tpe = getTypeName(getArgType(mb, p, unmatched));
			tpe2 = getTypeName(getArgType(s->def, sig, unmatched));
			if( unmatched)
				fprintf(stderr, "#unmatched %d test %s poly %s\n",
							 unmatched, tpe, tpe2);
			GDKfree(tpe);
			GDKfree(tpe2);
		}
#endif
		if (unmatched) {
			s = s->peer;
			continue;
		}
		/*
		 * At this stage we know all arguments are type compatible
		 * with the signature.
		 * We should assure that also the target variables have the
		 * proper types or can inherit them from the signature. The
		 * result type vector should be build separately first,
		 * because we may encounter an error later on.
		 *
		 * If any of the arguments refer to a constraint type, any_x,
		 * then the resulting type can not be determined.
		 */
		s1 = 0;
		if (sig->polymorphic)
			for (k = i = 0; i < p->retc; k++, i++) {
				int actual = getArgType(mb, p, i);
				int formal = getArgType(s->def, sig, k);

				if (k == sig->retc - 1 && sig->varargs & VARRETS)
					k--;

				s1 = getPolyType(formal, polytype);

				returntype[i] = resolveType(s1, actual);
				if (returntype[i] == -1) {
					s1 = -1;
					break;
				}
			}
		else
			/* check for non-polymorphic return */
			for (k = i = 0; i < p->retc; i++) {
				int actual = getArgType(mb, p, i);
				int formal = getArgType(s->def, sig, i);

				if (k == sig->retc - 1 && sig->varargs & VARRETS)
					k--;

				if (actual == formal)
					returntype[i] = actual;
				else {
					returntype[i] = resolveType(formal, actual);
					if (returntype[i] == -1) {
						s1 = -1;
						break;
					}
				}
			}
		if (s1 < 0) {
			s = s->peer;
			continue;
		}
		/*
		 * If the return types are correct, copy them in place.
		 * Beware that signatures should be left untouched, which
		 * means that we may not overwrite any formal argument.
		 * Using the knowledge dat the arguments occupy the header
		 * of the symbol stack, it is easy to filter such errors.
		 * Also mark all variables that are subject to garbage control.
		 * Beware, this is not yet effectuated in the interpreter.
		 */
#ifdef DEBUG_MAL_RESOLVE
		fprintf(stderr,"#TYPE RESOLVED:");
		fprintInstruction(stderr, mb, 0, p, LIST_MAL_DEBUG);
#endif
		p->typechk = TYPE_RESOLVED;
		for (i = 0; i < p->retc; i++) {
			int ts = returntype[i];
			if (isVarConstant(mb, getArg(p, i))) {
					if(!silent) { mb->errors = createMalException(mb, getPC(mb, p), TYPE, "Assignment to constant"); }
				p->typechk = TYPE_UNKNOWN;
				goto wrapup;
			}
			if (!isVarFixed(mb, getArg(p, i)) && ts >= 0) {
				setVarType(mb, getArg(p, i), ts);
				setVarFixed(mb, getArg(p, i));
			}
			prepostProcess(ts, p, i, mb);
		}
		/*
		 * Also the arguments may contain constants
		 * to be garbage collected.
		 */
		for (i = p->retc; i < p->argc; i++)
			if (ATOMtype(getArgType(mb, p, i)) == TYPE_str ||
				getArgType(mb, p, i) == TYPE_bat ||
				isaBatType(getArgType(mb, p, i)) ||
				(!isPolyType(getArgType(mb, p, i)) &&
				 getArgType(mb, p, i) < TYPE_any &&
				 getArgType(mb, p, i) >= 0 &&
				 ATOMstorage(getArgType(mb, p, i)) == TYPE_str)) {
				getInstrPtr(mb, 0)->gc |= GARBAGECONTROL;
				p->gc |= GARBAGECONTROL;
			}
		/*
		 * It may happen that an argument was still untyped and as a
		 * result of the polymorphism matching became strongly
		 * typed. This should be reflected in the symbol table.
		 */
		s1 = returntype[0];		/* for those interested */
		/*
		 * If the call refers to a polymorphic function, we clone it
		 * to arrive at a bounded instance. Polymorphic patterns and
		 * commands are responsible for type resolution themselves.
		 * Note that cloning pre-supposes that the function being
		 * cloned does not contain errors detected earlier in the
		 * process, nor does it contain polymorphic actual arguments.
		 */
		if (sig->polymorphic) {
			int cnt = 0;
			for (k = i = p->retc; i < p->argc; i++) {
				int actual = getArgType(mb, p, i);
				if (isAnyExpression(actual))
					cnt++;
			}
			if (cnt == 0 && s->kind != COMMANDsymbol && s->kind != PATTERNsymbol) {
				s = cloneFunction(scope, s, mb, p);
				if (mb->errors)
					goto wrapup;
			}
		}
		/* Any previousely found error in the block
		 * turns the complete block into erroneous.
		if (mb->errors) {
			p->typechk = TYPE_UNKNOWN;
			goto wrapup;
		}															\
		 */
		bindFunction(s, p, mb);

		if (returntype != returns)
			GDKfree(returntype);
		return s1;
	} /* while */
	/*
	 * We haven't found the correct function.  To ease debugging, we
	 * may reveal that we found an instruction with the proper
	 * arguments, but that clashes with one of the target variables.
	 */
  wrapup:
#ifdef DEBUG_MAL_RESOLVE
		if (tracefcn) {
			fprintf(stderr, "#Wrapup matching returntype %d returns %d:",*returntype,*returns);
			fprintInstruction(stderr, mb, 0, p, LIST_MAL_ALL);
		}
#endif
	if (returntype != returns)
		GDKfree(returntype);
	return -3;
}

int
resolveType(int dsttype, int srctype)
{
#ifdef DEBUG_MAL_RESOLVE
	if (tracefcn) {
		char *dtpe = getTypeName(dsttype);
		char *stpe = getTypeName(srctype);
		fprintf(stderr, "#resolveType dst %s (%d) %s(%d)\n",
					 dtpe, dsttype, stpe, srctype);
		GDKfree(dtpe);
		GDKfree(stpe);
	}
#endif
	if (dsttype == srctype)
		return dsttype;
	if (dsttype == TYPE_any)
		return srctype;
	if (srctype == TYPE_any)
		return dsttype;
	/*
	 * A bat reference can be coerced to bat type.
	 */
	if (isaBatType(srctype) && dsttype == TYPE_bat)
		return srctype;
	if (isaBatType(dsttype) && srctype == TYPE_bat)
		return dsttype;
	if (isaBatType(dsttype) && isaBatType(srctype)) {
		int t1, t2, t3;
		t1 = getBatType(dsttype);
		t2 = getBatType(srctype);
		if (t1 == t2)
			t3 = t1;
		else if (t1 == TYPE_any)
			t3 = t2;
		else if (t2 == TYPE_any)
			t3 = t1;
		else {
#ifdef DEBUG_MAL_RESOLVE
			if (tracefcn)
				fprintf(stderr, "#Tail can not be resolved \n");
#endif
			return -1;
		}
#ifdef DEBUG_MAL_RESOLVE
		if (tracefcn) {
			int i2 = getTypeIndex(dsttype);
			char *tpe1, *tpe2, *tpe3; 
			tpe1 = getTypeName(t1);
			tpe2 = getTypeName(t2);
			tpe3 = getTypeName(t3);
			fprintf(stderr, "#resolved to bat[:oid,:%s] bat[:oid,:%s]->bat[:oid,%s:%d]\n",
						 tpe1, tpe2, tpe3, i2);
			GDKfree(tpe1);
			GDKfree(tpe2);
			GDKfree(tpe3);
		}
#endif
		return newBatType(t3);
	}
#ifdef DEBUG_MAL_RESOLVE
	if (tracefcn)
		fprintf(stderr, "#Can not be resolved \n");
#endif
	return -1;
}

/*
 * We try to clear the type check flag by looking up the
 * functions. Errors are simply ignored at this point of the game,
 * because they may be resolved as part of the calling sequence.
 */
static void
typeMismatch(MalBlkPtr mb, InstrPtr p, int lhs, int rhs, int silent)
{
	str n1;
	str n2;

	if (!silent) {
		n1 = getTypeName(lhs);
		n2 = getTypeName(rhs);
		mb->errors = createMalException(mb, getPC(mb, p), TYPE, "type mismatch %s := %s", n1, n2);
		GDKfree(n1);
		GDKfree(n2);
	}
	p->typechk = TYPE_UNKNOWN;
}

/*
 * A function search should inspect all modules unless a specific module
 * is given. Preference is given to the lower scopes.
 * The type check is set to TYPE_UNKNOWN first to enforce a proper
 * analysis. This way it forms a cheap mechanism to resolve
 * the type after a change by an optimizer.
 * If we can not find the function, the type check returns unsuccessfully.
 * In this case we should issue an error message to the user.
 *
 * A re-check after the optimizer call should reset the token
 * to assignment.
 */
void
typeChecker(Module scope, MalBlkPtr mb, InstrPtr p, int silent)
{
	int s1 = -1, i, k;
	Module m = 0;

	p->typechk = TYPE_UNKNOWN;
	if ((p->fcn || p->blk) && p->token >= FCNcall && p->token <= PATcall) {
		p->token = ASSIGNsymbol;
		p->fcn = NULL;
		p->blk = NULL;
	}

	if (isaSignature(p)) {
		for (k = 0; k < p->argc; k++)
			setVarFixed(mb, getArg(p, k));
		for (k = p->retc; k < p->argc; k++) {
			prepostProcess(getArgType(mb, p, k), p, k, mb);
		}
		p->typechk = TYPE_RESOLVED;
		for (k = 0; k < p->retc; k++)
			p->typechk = MIN(p->typechk, typeKind(mb, p, 0));
		return;
	}
	if (getFunctionId(p) && getModuleId(p)) {
#ifdef DEBUG_MAL_RESOLVE
		//tracefcn = idcmp(getFunctionId(p), traceFcnName) == 0;
#endif
		m = findModule(scope, getModuleId(p));
		s1 = findFunctionType(m, mb, p, silent);
#ifdef DEBUG_MAL_RESOLVE
		fprintf(stderr,"#typeChecker matched %d\n",s1);
#endif
		if (s1 >= 0)
			return;
		/*
		 * Could not find a function that statisfies the constraints.
		 * If the instruction is just a function header we may
		 * continue.  Likewise, the function and module may refer to
		 * string variables known only at runtime.
		 *
		 * In all other cases we should generate a message, but only
		 * if we know that the error was not caused by checking the
		 * definition of a polymorphic function or the module or
		 * function name are variables, In those cases, the detailed
		 * analysis is performed upon an actual call.
		 */
		if (!isaSignature(p) && !getInstrPtr(mb, 0)->polymorphic) {
			if (!silent) {
				char *errsig;
				if (!malLibraryEnabled(p->modname)) {
					mb->errors = createMalException(mb, getPC(mb, p), TYPE,
										"'%s%s%s' library error in: %s",
										(getModuleId(p) ? getModuleId(p) : ""),
										(getModuleId(p) ? "." : ""),
										getFunctionId(p), malLibraryHowToEnable(p->modname));
				} else {
					errsig = instruction2str(mb,0,p,(LIST_MAL_NAME | LIST_MAL_TYPE | LIST_MAL_VALUE));
					mb->errors = createMalException(mb, getPC(mb, p), TYPE,
										"'%s%s%s' undefined in: %s",
										(getModuleId(p) ? getModuleId(p) : ""),
										(getModuleId(p) ? "." : ""),
										getFunctionId(p), errsig?errsig:"failed instruction2str()");
					GDKfree(errsig);
				}
			} 
			p->typechk = TYPE_UNKNOWN;
		} else
			p->typechk = TYPE_RESOLVED;
#ifdef DEBUG_MAL_RESOLVE
		fprintf(stderr,"#typeChecker  no-sig and no-oly could not find it %d\n",p->typechk);
#endif
		return;
	}
	/*
	 * When we arrive here the operator is an assignment.
	 * The language should also recognize (a,b):=(1,2);
	 * This is achieved by propagation of the rhs types to the lhs
	 * variables.
	 */
	if (getFunctionId(p)){
#ifdef DEBUG_MAL_RESOLVE
		fprintf(stderr,"#typeChecker function call break %s\n", getFunctionId(p));
#endif
		return;
	}
	if (p->retc >= 1 && p->argc > p->retc && p->argc != 2 * p->retc) {
		if (!silent){
			mb->errors  = createMalException(mb, getPC(mb, p), TYPE, "Multiple assignment mismatch");
		}
		p->typechk = TYPE_RESOLVED;
	} else
		p->typechk = TYPE_RESOLVED;
	for (k = 0, i = p->retc; k < p->retc && i < p->argc; i++, k++) {
		int rhs = getArgType(mb, p, i);
		int lhs = getArgType(mb, p, k);

		if (rhs != TYPE_void) {
			s1 = resolveType(lhs, rhs);
			if (s1 == -1) {
				typeMismatch(mb, p, lhs, rhs, silent);
#ifdef DEBUG_MAL_RESOLVE
				fprintf(stderr,"#typeChecker function mismatch %s\n", getFunctionId(p));
#endif
				return;
			}
		} else {
			/*
			 * The language permits assignment of 'nil' to any variable,
			 * using the target type.
			 */
			if (lhs != TYPE_void && lhs != TYPE_any) {
				ValRecord cst;
				cst.vtype = TYPE_void;
				cst.val.oval = void_nil;
				cst.len = 0;

				rhs = isaBatType(lhs) ? TYPE_bat : lhs;
				p->argv[i] = defConstant(mb, rhs, &cst);
				rhs = lhs;
			}
		}

		if (!isVarFixed(mb, getArg(p, k))) {
			setVarType(mb, getArg(p, k), rhs);
			setVarFixed(mb, getArg(p, k));
		}
		prepostProcess(s1, p, i, mb);
		prepostProcess(s1, p, k, mb);
	}
	/* the case where we have no rhs */
	if (p->barrier && p->retc == p->argc)
		for (k = 0; k < p->retc; k++) {
			int tpe = getArgType(mb, p, k);
			if (isaBatType(tpe)  ||
				ATOMtype(tpe) == TYPE_str ||
				(!isPolyType(tpe) && tpe < MAXATOMS && ATOMextern(tpe)))
				setVarCleanup(mb, getArg(p, k));
		}
}

/*
 * After the parser finishes, we have to look for semantic errors,
 * such as flow of control problems and possible typeing conflicts.
 * The nesting of BARRIER and CATCH statements with their associated
 * flow of control primitives LEAVE and RETRY should form a valid
 * hierarchy. Failure to comply is considered a structural error
 * and leads to flagging the function as erroneous.
 * Also check general conformaty of the ML block structure.
 * It should start with a signature and finish with and ENDsymbol
 *
 * Type checking a program is limited to those instructions that are
 * not resolved yet. Once the program is completely checked, further calls
 * should be ignored. This should be separately administered for the flow
 * as well, because a dynamically typed instruction should later on not
 * lead to a re-check when it was already fully analyzed.
 */
void
chkTypes(Module s, MalBlkPtr mb, int silent)
{
	InstrPtr p = 0;
	int i;

	for (i = 0; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		assert (p != NULL);
		if (p->typechk != TYPE_RESOLVED)
			typeChecker(s, mb, p, silent);
		if (mb->errors)
			return;
	}
}

/*
 * Type checking an individual instruction is dangerous,
 * because it ignores data flow and variable declarations.
 */
int
chkInstruction(Module s, MalBlkPtr mb, InstrPtr p)
{
	if( mb->errors == MAL_SUCCEED){
		p->typechk = TYPE_UNKNOWN;
		typeChecker(s, mb, p, TRUE);
	}
	return mb->errors != MAL_SUCCEED;
}

/*
 * Perform silent check on the program, merely setting the error flag.
 */
void
chkProgram(Module s, MalBlkPtr mb)
{
/* it is not ready yet, too fragile
		mb->typefixed = mb->stop == chk; ignored END */
/*	if( mb->flowfixed == 0)*/

	chkTypes(s, mb, FALSE);
	if (mb->errors)
		return;
	chkFlow(mb);
	if (mb->errors)
		return;
	chkDeclarations(mb);
	/* malGarbageCollector(mb); */
}

/*
 * Polymorphic type analysis
 * MAL provides for type variables of the form any$N. This feature
 * supports polymorphic types, but also complicates the subsequent
 * analysis. A variable typed with any$N not occuring in the function
 * header leads to a dynamic typed statement. In principle we have
 * to type check the function upon each call.
 */
static int
typeKind(MalBlkPtr mb, InstrPtr p, int i)
{
	malType t = getArgType(mb, p, i);
	if (t == TYPE_any || isAnyExpression(t)) {
		return TYPE_UNKNOWN;
	}
	return TYPE_RESOLVED;
}

/*
 * For a polymorphic commands we do not generate a cloned version.
 * It suffices to determine the actual return value taking into
 * account the type variable constraints.
 */
static malType
getPolyType(malType t, int *polytype)
{
	int ti;
	int tail;

	ti = getTypeIndex(t);
	if (!isaBatType(t) && ti > 0)
		return polytype[ti];

	tail = ti == 0 ? getBatType(t) : polytype[ti];
	if (isaBatType(t)) 
		return newBatType(tail);
	return tail;
}

/*
 * Each argument is checked for binding of polymorphic arguments.
 * This routine assumes that the type index is indeed smaller than maxarg.
 * (The parser currently enforces a single digit from 1-9 )
 * The polymorphic type 'any', i.e. any_0, does never constraint an operation
 * it can match with all polymorphic types.
 * The routine returns the instanciated formal type for subsequent
 * type resolution.
 */
static int
updateTypeMap(int formal, int actual, int polytype[MAXTYPEVAR])
{
	int h, t, ret = 0;

	if (formal == TYPE_bat && isaBatType(actual))
		return 0;
#ifdef DEBUG_MAL_RESOLVE
	if(tracefcn){
		char *tpe1 = getTypeName(formal), *tpe2 = getTypeName(actual);
		fprintf(stderr, "#updateTypeMap:formal %s actual %s\n", tpe1, tpe2);
		GDKfree(tpe1);
		GDKfree(tpe2);
	}
#endif

	if ((h = getTypeIndex(formal))) {
		if (isaBatType(actual) && !isaBatType(formal) &&
			(polytype[h] == TYPE_any || polytype[h] == actual)) {
			polytype[h] = actual;
			ret = 0;
			goto updLabel;
		}
		t = getBatType(actual);
		if (t != polytype[h]) {
			if (polytype[h] == TYPE_bat && isaBatType(actual))
				ret = 0;
			else if (polytype[h] == TYPE_any)
				polytype[h] = t;
			else {
				ret = -1;
				goto updLabel;
			}
		}
	}
	if (isaBatType(formal)) {
		if (!isaBatType(actual) && actual != TYPE_bat)
			return -1;
	}
  updLabel:
#ifdef DEBUG_MAL_RESOLVE
	fprintf(stderr, "#updateTypeMap returns: %d\n", ret);
#endif
	return ret;
}
