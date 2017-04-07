/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (author) M. L. Kersten
 * @node Stack Management, The MAL Optimizer, Garbage Collection, The MAL Interpreter
 * @+ MAL runtime stack
 * The runtime context of a MAL procedure is allocated on the runtime stack
 * of the corresponding interpreter.
 * Access to the elements in the stack are through index offsets,
 * determined during MAL procedure parsing.
 *
 * The scope administration for MAL procedures is
 * decoupled from their actual runtime behavior. This means we are
 * more relaxed on space allocation, because the size is determined
 * by the number of MAL procedure definitions instead of the runtime
 * calling behavior. (See mal_interpreter for details on value stack
 * management)
 *
 * The variable names and types are kept in the stack to ease debugging.
 * The underlying string value need not be garbage collected.
 * Runtime storage for variables are allocated on the stack of the
 * interpreter thread. The physical stack is often limited in size,
 * which calls for safeguarding their value and garbage collection before returning.
 * A malicious procedure or implementation will lead to memory leakage.
 *
 * A system command (linked C-routine) may be interested in extending the
 * stack. This is precluded, because it could interfere with the recursive
 * calling sequence of procedures. To accommodate the (rare) case, the routine
 * should issue an exception to be handled by the interpreter before retrying.
 * All other errors are turned into an exception, followed by continuing
 * at the exception handling block of the MAL procedure.
 *
 * The interpreter should be protected against physical stack overflow.
 * The solution chosen is to maintain an incremental depth size.
 * Once it exceeds a threshold, we call upon the kernel to
 * ensure we are still within safe bounds.
 */
/*
 * The clearStack operation throws away any space occupied by variables
 * Freeing the stack itself is automatic upon return from the interpreter
 * context. Since the stack is allocated and zeroed on the calling stack,
 * it may happen that entries are never set to a real value.
 * This can be recognized by the vtype component
 */
#include "monetdb_config.h"
#include "mal_stack.h"
#include "mal_exception.h"

/* #define DEBUG_MAL_STACK*/

MalStkPtr
newGlobalStack(int size)
{
	MalStkPtr s;

	s = (MalStkPtr) GDKzalloc(stackSize(size) + offsetof(MalStack, stk));
	if (!s) {
		GDKerror("newGlobalStack:"MAL_MALLOC_FAIL);
		return NULL;
	}
	s->stksize = size;
	return s;
}

MalStkPtr
reallocGlobalStack(MalStkPtr old, int cnt)
{
	int k;
	MalStkPtr s;

	if (old->stksize > cnt)
		return old;
	k = ((cnt / STACKINCR) + 1) * STACKINCR;
	s = newGlobalStack(k);
	if (!s) {
		return NULL;
	}
	memcpy(s, old, stackSize(old->stksize));
	s->stksize = k;
	GDKfree(old);
	return s;
}

/*
 * When you add a value to the stack, you should ensure that
 * there is space left. It should only be used for global
 * stack frames, because the others are allocated in the
 * runtime stack.
 */
void
freeStack(MalStkPtr stk)
{
	clearStack(stk);
	GDKfree(stk);
}

void
clearStack(MalStkPtr s)
{
	ValPtr v;
	int i;

	if (!s) return;
	
	i = s->stktop;

	for (v = s->stk; i >= 0; i--, v++)
		if (ATOMextern(v->vtype) && v->val.pval) {
			GDKfree(v->val.pval);
			v->vtype = 0;
			v->val.pval = NULL;
		}
	s->stkbot = 0;
}

