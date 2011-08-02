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

#ifndef _MAL_STACK_H_
#define _MAL_STACK_H_
#include "mal.h"
#include "gdk.h"

#ifdef HAVE_SYS_TIMES_H
#include <sys/times.h>
#endif

#define STACKINCR   128
#define MAXGLOBALS  4 * STACKINCR
#define MAXSHARES   8

typedef str (*MALfcn) ();
typedef int (*DFhook) (void *, void *, void *, void *);

typedef struct MALSTK {
	int stksize;
	int stktop;
	int stkbot;			/* the first variable to be initialized */
	int stkdepth;		/* to protect against runtime stack overflow */
	int calldepth;		/* to protect against runtime stack overflow */
	short keepAlive;	/* do not garbage collect when set */
	short garbageCollect; /* stack needs garbage collection */
	/*
	 * @-
	 * Parallel processing is mostly driven by dataflow, but within this context
	 * there may be different schemes to take instructions into execution.
	 * The admission scheme (and wrapup) are the necessary scheduler hooks.
	 */
	DFhook admit;
	DFhook wrapup;
	MT_Lock stklock;	/* used for parallel processing */
/*
 * @-
 * It is handy to administer the timing in the stack frame
 * for use in profiling and recylcing instructions.
 */
#ifdef HAVE_TIMES
    struct tms timer;   /* timing information */
#endif
	struct timeval clock;		/* seconds + microsecs since epoch */
	lng clk;			/* micro seconds */
	char cmd;		/* debugger communication */
	int pcup;		/* saved pc upon a recursive all */
	struct MALSTK *up;	/* stack trace list */
	struct MALBLK *blk;	/* associated definition */
	ValRecord stk[1];
} MalStack, *MalStkPtr;

#define stackSize(CNT) (sizeof(ValRecord)*(CNT) + sizeof(MalStack))
#define newStack(S,CNT) S= (MalStkPtr) GDKzalloc(stackSize(CNT));\
		(S)->stksize=CNT;


mal_export MalStkPtr newGlobalStack(int size);
mal_export MalStkPtr reallocStack(MalStkPtr s, int cnt);
mal_export MalStkPtr reallocGlobalStack(MalStkPtr s, int cnt);
mal_export void freeStack(MalStkPtr stk);
mal_export void clearStack(MalStkPtr s);
mal_export void chkStack(MalStkPtr stk, int i);	/* used in src/mal/mal_box.c */

#define VARfreeze(X)    if(X){X->frozen=TRUE;}
#define VARfixate(X)    if(X){X->constant=TRUE;}

#define getStkRecord(S,P,I) &(S)->stk[(P)->argv[I]]
#define getStkValue(S,P,I)  ( getStkType(S,P,I)== TYPE_str? \
					getStkRecord(S,P,I)->val.sval :\
					getStkRecord(S,P,I)->val.pval )
#define getStkType(S,P,I)   (S)->stk[(P)->argv[I]].vtype
#define setStkType(S,P,I,T) (S)->stk[(P)->argv[I]].vtype = T
#endif /* _MAL_STACK_H_ */
