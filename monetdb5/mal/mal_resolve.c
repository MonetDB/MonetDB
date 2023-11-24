/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
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

#define MAXTYPEVAR  4

static malType getPolyType(malType t, int *polytype);
static int updateTypeMap(int formal, int actual, int polytype[MAXTYPEVAR]);
static bool typeResolved(MalBlkPtr mb, InstrPtr p, int i);

int
resolvedType(int dsttype, int srctype)
{
	if (dsttype == srctype || dsttype == TYPE_any || srctype == TYPE_any)
		return 0;

	if (getOptBat(dsttype) && isaBatType(srctype)) {
		int t1 = getBatType(dsttype);
		int t2 = getBatType(srctype);
		if (t1 == t2 || t1 == TYPE_any || t2 == TYPE_any)
			return 0;
	}
	if (getOptBat(dsttype) && !isaBatType(srctype)) {
		int t1 = getBatType(dsttype);
		int t2 = srctype;
		if (t1 == t2 || t1 == TYPE_any || t2 == TYPE_any)
			return 0;
	}

	if (isaBatType(dsttype) && isaBatType(srctype)) {
		int t1 = getBatType(dsttype);
		int t2 = getBatType(srctype);
		if (t1 == t2 || t1 == TYPE_any || t2 == TYPE_any)
			return 0;
	}
	return -1;
}

static int
resolveType(int *rtype, int dsttype, int srctype)
{
	if (dsttype == srctype) {
		*rtype = dsttype;
		return 0;
	}
	if (dsttype == TYPE_any) {
		*rtype = srctype;
		return 0;
	}
	if (srctype == TYPE_any) {
		*rtype = dsttype;
		return 0;
	}
	/*
	 * A bat reference can be coerced to bat type.
	 */
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
			return -1;
		}
		*rtype = newBatType(t3);
		return 0;
	}
	return -1;
}


/*
 * Since we now know the storage type of the receiving variable, we can
 * set the garbage collection flag.
 */
#define prepostProcess(tp, p, b, mb)					\
	do {												\
		if( isaBatType(tp) ||							\
			ATOMtype(tp) == TYPE_str ||					\
			(!isPolyType(tp) && tp < TYPE_any &&		\
			 tp >= 0 && ATOMextern(tp))) {				\
			getInstrPtr(mb, 0)->gc = true;				\
			setVarCleanup(mb, getArg(p, b));			\
			p->gc = true;								\
		}												\
	} while (0)

static malType
getFormalArgType( Symbol s, int arg)
{
	if (s->kind == FUNCTIONsymbol)
		return getArgType(s->def, getSignature(s), arg);
	mel_arg *a = s->func->args+arg;
	malType tpe = TYPE_any;
	if (a->nr || !a->type[0]) {
		if (a->isbat)
			tpe = newBatType(TYPE_any);
		else
			tpe = a->typeid;
		setTypeIndex(tpe, a->nr);
	} else {
		if (a->isbat)
			tpe = newBatType(a->typeid);
		else
			tpe = a->typeid;
	}
	if (a->opt == 1)
		setOptBat(tpe);
	return tpe;
}

static malType
findFunctionType(Module scope, MalBlkPtr mb, InstrPtr p, int idx, int silent)
{
	Module m;
	Symbol s;
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

	m = scope;
	s = m->space[(int) (getSymbolIndex(getFunctionId(p)))];
	if (s == 0)
		return -1;

	if (p->retc < 256) {
		for (i = 0; i < p->retc; i++)
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
		int argc = 0, argcc = 0, retc = 0, varargs = 0, varrets = 0, unsafe = 0, inlineprop = 0, polymorphic = 0;
		if (s->kind == FUNCTIONsymbol) {
			InstrPtr sig = getSignature(s);
			retc = sig->retc;
			argc = sig->argc;
			varargs = (sig->varargs & (VARARGS | VARRETS));
			varrets = (sig->varargs & VARRETS);
			unsafe = s->def->unsafeProp;
			inlineprop = s->def->inlineProp;
			polymorphic = sig->polymorphic;
			argcc = argc;
		} else {
			retc = s->func->retc;
			argc = s->func->argc;
			varargs = /*retc == 0 ||*/ s->func->vargs || s->func->vrets;
			varrets = retc == 0 || s->func->vrets;
			unsafe = s->func->unsafe;
			inlineprop = 0;
			polymorphic = s->func->poly;
			if (!retc && !polymorphic)
				polymorphic = 1;
			argcc = argc + ((retc == 0)?1:0);
		}
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
		if (polymorphic) {
			int limit = polymorphic;
			if (!(argcc == p->argc || (argc < p->argc && varargs))) {
				s = s->peer;
				continue;
			}
			if (retc != p->retc && !varrets) {
				s = s->peer;
				continue;
			}

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
			for (k = retc; i < p->argc; k++, i++) {
				int actual = getArgType(mb, p, i);
				int formal = getFormalArgType(s, k);
				if (k == argc - 1 && varargs)
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
				if (getOptBat(formal) && !isAnyExpression(formal) && getBatType(actual) == getBatType(formal))
					formal = actual;
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
				if (resolvedType(formal, actual) < 0) {
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
			if (varargs) {
				if (s->kind != PATTERNsymbol && retc)
					unmatched = i;
				else {
					/* resolve the arguments */
					for (; i < p->argc; i++) {
						/* the type of the last one has already been set */
						int actual = getArgType(mb, p, i);
						int formal = getFormalArgType(s, k);
						if (k == argc - 1 && varargs)
							k--;

						formal = getPolyType(formal, polytype);
						if (getOptBat(formal) && !isAnyExpression(formal) && getBatType(actual) == getBatType(formal))
							formal = actual;
						if (formal == actual || formal == TYPE_any)
							continue;
						if (resolvedType(formal, actual) < 0) {
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
			if (argc != p->argc || retc != p->retc) {
				s = s->peer;
				continue;
			}


			for (i = p->retc; i < p->argc; i++) {
				int actual = getArgType(mb, p, i);
				int formal = getFormalArgType(s, i);
				if (resolvedType(formal, actual) < 0) {
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
		if (polymorphic) {
			for (k = i = 0; i < p->retc && k < retc; k++, i++) {
				int actual = getArgType(mb, p, i);
				int formal = getFormalArgType(s, k);

				if (k == retc - 1 && varrets)
					k--;

				s1 = getPolyType(formal, polytype);

				if (getOptBat(formal) && !isAnyExpression(formal) && getBatType(actual) == getBatType(formal))
					s1 = actual;
				if (resolveType(returntype+i, s1, actual) < 0) {
					s1 = -1;
					break;
				}
			}
		} else {
			/* check for non-polymorphic return */
			for (k = i = 0; i < p->retc; i++) {
				int actual = getArgType(mb, p, i);
				int formal = getFormalArgType(s, i);

				if (k == retc - 1 && varrets)
					k--;

				if (actual == formal) {
					returntype[i] = actual;
				} else {
					if (resolveType(returntype+i, formal, actual) < 0) {
						s1 = -1;
						break;
					}
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

		p->typeresolved = true;
		p->inlineProp = inlineprop;
		p->unsafeProp = unsafe;
		for (i = 0; i < p->retc; i++) {
			int ts = returntype[i];
			if (isVarConstant(mb, getArg(p, i))) {
				if (!silent) {
					mb->errors = createMalException(mb, idx, TYPE,
													"Assignment to constant");
				}
				p->typeresolved = false;
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
				isaBatType(getArgType(mb, p, i)) ||
				(!isPolyType(getArgType(mb, p, i)) &&
				 getArgType(mb, p, i) < TYPE_any &&
				 getArgType(mb, p, i) >= 0 &&
				 ATOMstorage(getArgType(mb, p, i)) == TYPE_str)) {
				getInstrPtr(mb, 0)->gc = true;
				p->gc = true;
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
		if (polymorphic) {
			int cnt = 0;
			for (k = i = p->retc; i < p->argc; i++) {
				int actual = getArgType(mb, p, i);
				if (isAnyExpression(actual))
					cnt++;
			}
			if (cnt == 0 && s->kind != COMMANDsymbol
				&& s->kind != PATTERNsymbol) {
				assert(s->kind == FUNCTIONsymbol);
				s = cloneFunction(scope, s, mb, p);
				if (mb->errors)
					goto wrapup;
			}
		}
		/* Any previousely found error in the block
		 * turns the complete block into erroneous.
		 if (mb->errors) {
		 p->typeresolved = false;
		 goto wrapup;
		 }
		 */

		/*
		 * We found the proper function. Copy some properties. In
		 * particular, determine the calling strategy, i.e. FCNcall,
		 * CMDcall, PATcall Beware that polymorphic functions
		 * may produce type-incorrect clones.  This piece of code may be
		 * shared by the separate binder
		 */
		if (p->token == ASSIGNsymbol) {
			switch (s->kind) {
			case COMMANDsymbol:
				p->token = CMDcall;
				p->fcn = s->func->imp;				/* C implementation mandatory */
				if (p->fcn == NULL) {
					if (!silent)
						mb->errors = createMalException(mb, idx, TYPE,
														"object code for command %s.%s missing",
														p->modname, p->fcnname);
					p->typeresolved = false;
					goto wrapup;
				}
				break;
			case PATTERNsymbol:
				p->token = PATcall;
				p->fcn = s->func->imp;				/* C implementation optional */
				break;
			case FUNCTIONsymbol:
				p->token = FCNcall;
				if (getSignature(s)->fcn)
					p->fcn = getSignature(s)->fcn;	/* C implementation optional */
				break;
			default:
				if (!silent)
					mb->errors = createMalException(mb, idx, MAL,
													"MALresolve: unexpected token type");
				goto wrapup;
			}
			p->blk = s->def;
		}

		if (returntype != returns)
			GDKfree(returntype);
		return s1;
	}							/* while */
	/*
	 * We haven't found the correct function.  To ease debugging, we
	 * may reveal that we found an instruction with the proper
	 * arguments, but that clashes with one of the target variables.
	 */
  wrapup:
	if (returntype != returns)
		GDKfree(returntype);
	return -3;
}

/*
 * We try to clear the type check flag by looking up the
 * functions. Errors are simply ignored at this point of the game,
 * because they may be resolved as part of the calling sequence.
 */
static void
typeMismatch(MalBlkPtr mb, InstrPtr p, int idx, int lhs, int rhs, int silent)
{
	str n1;
	str n2;

	if (!silent) {
		n1 = getTypeName(lhs);
		n2 = getTypeName(rhs);
		mb->errors = createMalException(mb, idx, TYPE, "type mismatch %s := %s", n1,
										n2);
		GDKfree(n1);
		GDKfree(n2);
	}
	p->typeresolved = false;
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
typeChecker(Module scope, MalBlkPtr mb, InstrPtr p, int idx, int silent)
{
	int s1 = -1, i, k;
	Module m = 0;

	p->typeresolved = false;
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
		p->typeresolved = true;
		for (k = 0; k < p->retc; k++)
			p->typeresolved &= typeResolved(mb, p, 0);
		return;
	}
	if (getFunctionId(p) && getModuleId(p)) {
		m = findModule(scope, getModuleId(p));

		if (!m || strcmp(m->name, getModuleId(p)) != 0) {
			if (!silent)
				mb->errors = createMalException(mb, idx, TYPE, "'%s%s%s' undefined",
					(getModuleId(p) ?  getModuleId(p) : ""),
					(getModuleId(p) ? "." : ""),
					getFunctionId(p));
			return;
		}
		s1 = findFunctionType(m, mb, p, idx, silent);

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
				char *errsig = NULL;
				if (!malLibraryEnabled(p->modname)) {
					mb->errors = createMalException(mb, idx, TYPE,
													"'%s%s%s' library error in: %s",
													(getModuleId(p) ?
													 getModuleId(p) : ""),
													(getModuleId(p) ? "." : ""),
													getFunctionId(p),
													malLibraryHowToEnable(p->
																		  modname));
				} else {
					bool free_errsig = false, special_undefined = false;
					errsig = malLibraryHowToEnable(p->modname);
					if (!strcmp(errsig, "")) {
						errsig = instruction2str(mb, 0, p,
												 (LIST_MAL_NAME | LIST_MAL_TYPE
												  | LIST_MAL_VALUE));
						free_errsig = true;
					} else {
						special_undefined = true;
					}
					mb->errors = createMalException(mb, idx, TYPE,
													"'%s%s%s' undefined%s: %s",
													(getModuleId(p) ?
													 getModuleId(p) : ""),
													(getModuleId(p) ? "." : ""),
													getFunctionId(p),
													special_undefined ? "" :
													" in",
													errsig ? errsig :
													"failed instruction2str()");
					if (free_errsig)
						GDKfree(errsig);
				}
			}
			p->typeresolved = false;
		} else
			p->typeresolved = true;
		return;
	}
	/*
	 * When we arrive here the operator is an assignment.
	 * The language should also recognize (a,b):=(1,2);
	 * This is achieved by propagation of the rhs types to the lhs
	 * variables.
	 */
	if (getFunctionId(p)) {
		return;
	}
	if (p->retc >= 1 && p->argc > p->retc && p->argc != 2 * p->retc) {
		if (!silent) {
			mb->errors = createMalException(mb, idx, TYPE,
											"Multiple assignment mismatch");
		}
		p->typeresolved = true;
	} else
		p->typeresolved = true;
	for (k = 0, i = p->retc; k < p->retc && i < p->argc; i++, k++) {
		int rhs = getArgType(mb, p, i);
		int lhs = getArgType(mb, p, k);

		if (rhs != TYPE_void) {
			if (resolveType(&s1, lhs, rhs) < 0) {
				typeMismatch(mb, p, idx, lhs, rhs, silent);
				return;
			}
		} else {
			/*
			 * The language permits assignment of 'nil' to any variable,
			 * using the target type.
			 */
			if (lhs != TYPE_void && lhs != TYPE_any) {
				ValRecord cst = { .vtype = TYPE_void, .val.oval = void_nil, .bat = isaBatType(lhs) };
				int k;

				k = defConstant(mb, lhs, &cst);
				if (k >= 0)
					p->argv[i] = k;
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
			if (isaBatType(tpe) ||
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
str
chkTypes(Module s, MalBlkPtr mb, int silent)
{
	InstrPtr p = 0;
	int i;
	str msg = MAL_SUCCEED;

	for (i = 0; mb->errors == NULL && i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		assert(p != NULL);
		if (!p->typeresolved)
			typeChecker(s, mb, p, i, silent);
	}
	if (mb->errors) {
		msg = mb->errors;
		mb->errors = NULL;
	}
	return msg;
}

/*
 * Type checking an individual instruction is dangerous,
 * because it ignores data flow and variable declarations.
 */
int
chkInstruction(Module s, MalBlkPtr mb, InstrPtr p)
{
	if (mb->errors == MAL_SUCCEED) {
		p->typeresolved = false;
		typeChecker(s, mb, p, getPC(mb, p), TRUE);
	}
	return mb->errors != MAL_SUCCEED;
}

/*
 * Perform silent check on the program, merely setting the error flag.
 */
str
chkProgram(Module s, MalBlkPtr mb)
{
	str msg;
/* it is not ready yet, too fragile
		mb->typefixed = mb->stop == chk; ignored END */
/*	if( mb->flowfixed == 0)*/

	if (mb->errors) {
		msg = mb->errors;
		mb->errors = NULL;
		return msg;
	}
	msg = chkTypes(s, mb, FALSE);
	if (msg == MAL_SUCCEED)
		msg = chkFlow(mb);
	if (msg == MAL_SUCCEED)
		msg = chkDeclarations(mb);
	return msg;
}

/*
 * Polymorphic type analysis
 * MAL provides for type variables of the form any$N. This feature
 * supports polymorphic types, but also complicates the subsequent
 * analysis. A variable typed with any$N not occuring in the function
 * header leads to a dynamic typed statement. In principle we have
 * to type check the function upon each call.
 */
static bool
typeResolved(MalBlkPtr mb, InstrPtr p, int i)
{
	malType t = getArgType(mb, p, i);
	if (t == TYPE_any || isAnyExpression(t)) {
		return false;
	}
	return true;
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
	malType tail;

	ti = getTypeIndex(t);
	if (!isaBatType(t) && ti > 0) {
		tail = polytype[ti];
		if (getOptBat(t))
			setOptBat(tail);
		return tail;
	}

	tail = ti == 0 ? getBatType(t) : polytype[ti];
	if (isaBatType(t)) {
		tail = newBatType(tail);
	}
	if (getOptBat(t))
		setOptBat(tail);
	return tail;
}

/*
 * Each argument is checked for binding of polymorphic arguments.
 * This routine assumes that the type index is indeed smaller than maxarg.
 * (The parser currently enforces a single digit from 1-2 )
 * The polymorphic type 'any', i.e. any_0, does never constraint an operation
 * it can match with all polymorphic types.
 * The routine returns the instanciated formal type for subsequent
 * type resolution.
 */
static int
updateTypeMap(int formal, int actual, int polytype[MAXTYPEVAR])
{
	int h, t, ret = 0;

	if (!isAnyExpression(formal) && isaBatType(formal) && isaBatType(actual))
		return 0;

	if ((h = getTypeIndex(formal))) {
		if (isaBatType(actual) && !isaBatType(formal) && !getOptBat(formal) &&
			(polytype[h] == TYPE_any || polytype[h] == actual)) {
			polytype[h] = actual;
			return 0;
		}
		t = getBatType(actual);
		if (t != polytype[h]) {
			if (isaBatType(polytype[h]) && isaBatType(actual))
				ret = 0;
			else if (polytype[h] == TYPE_any)
				polytype[h] = t;
			else {
				return -1;
			}
		}
	}
	if (isaBatType(formal)) {
		if (!isaBatType(actual))
			return -1;
	}
	return ret;
}
