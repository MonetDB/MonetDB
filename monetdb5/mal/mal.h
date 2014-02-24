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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (c) Martin Kersten
 *  MonetDB Basic Definitions
 * Definitions that need to included in every file of the Monet system, as well as in user defined module implementations.
 */
#ifndef _MAL_H
#define _MAL_H

#include <gdk.h>

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define mal_export extern __declspec(dllimport)
#else
#define mal_export extern __declspec(dllexport)
#endif
#else
#define mal_export extern
#endif

#ifdef HAVE_SYS_TIMES_H
# include <sys/times.h>
#endif

#include <setjmp.h>
/*
 * MonetDB Calling Options
 * The number of invocation arguments is kept to a minimum.
 * See `man mserver5` or tools/mserver/mserver5.1
 * for additional system variable settings.
 */
#define MAXSCRIPT 64
#define MEMORY_THRESHOLD  0.8

mal_export char     monet_cwd[PATHLENGTH];
mal_export size_t	monet_memory;
mal_export lng 		memorypool;      /* memory claimed by concurrent threads */
mal_export int 		memoryclaims;    /* number of threads active with expensive operations */
mal_export char		*mal_trace;		/* enable profile events on console */

/*
   See gdk/gdk.mx for the definition of all debug masks.
   See `man mserver5` or tools/mserver/mserver5.1
   for a documentation of all debug options.
*/
#define GRPthreads (THRDMASK | PARMASK)
#define GRPmemory (MEMMASK | ALLOCMASK )
#define GRPproperties (CHECKMASK | PROPMASK | BATMASK )
#define GRPio (IOMASK | PERFMASK )
#define GRPheaps (HEAPMASK)
#define GRPtransactions (TMMASK | DELTAMASK | TEMMASK)
#define GRPmodules (LOADMASK)
#define GRPalgorithms (ALGOMASK | ESTIMASK)
#define GRPperformance (JOINPROPMASK | DEADBEEFMASK)
#define GRPoptimizers  (OPTMASK)
#define GRPforcemito (FORCEMITOMASK)
#define GRPrecycler (1<<30)

mal_export MT_Lock  mal_contextLock;
mal_export MT_Lock  mal_remoteLock;
mal_export MT_Lock  mal_profileLock ;
mal_export MT_Lock  mal_copyLock ;
mal_export MT_Lock  mal_delayLock ;


mal_export int mal_init(void);
mal_export void mal_exit(void);

/* This should be here, but cannot, as "Client" isn't known, yet ... |-(
 * For now, we move the prototype declaration to src/mal/mal_client.c,
 * the only place where it is currently used. Maybe, we should concider
 * also moving the implementation there...
 */


/* Listing modes are globally known */
#define LIST_INPUT      1       /* echo original input */
#define LIST_MAL_STMT  2       /* show mal instruction */
#define LIST_MAL_TYPE   4       /* show type resolutoin */
#define LIST_MAL_UDF    8       /* show type resolutoin */
#define LIST_MAL_PROPS    16       /* show line numbers */
#define LIST_MAL_DETAIL 32		/* type details */
#define LIST_MAL_VALUE  64		/* list bat tuple count */
#define LIST_MAPI       128       /* output Mapi compatible output */
#define LIST_MAL_ARG 256		/* show the formal argument name */
#define LIST_MAL_LNR    512       /* show line numbers */
#define LIST_MAL_CALL  (LIST_MAL_STMT | LIST_MAL_UDF | LIST_MAL_VALUE )
#define LIST_MAL_DEBUG  (LIST_MAL_STMT | LIST_MAL_UDF | LIST_MAL_VALUE | LIST_MAL_ARG)
#define LIST_MAL_EXPLAIN  (LIST_MAL_STMT | LIST_MAL_UDF | LIST_MAL_ARG)
#define LIST_MAL_ALL   (LIST_MAL_STMT | LIST_MAL_TYPE | LIST_MAL_UDF | LIST_MAL_PROPS | LIST_MAL_DETAIL  | LIST_MAL_ARG | LIST_MAL_LNR | LIST_MAPI)

#ifndef WORDS_BIGENDIAN
#define STRUCT_ALIGNED
#endif

#ifndef MAXPATHLEN
#define MAXPATHLEN 1024
#endif

/* The MAL instruction block type definitions */
/* Variable properties */
#define VAR_CONSTANT 	1
#define VAR_TYPEVAR	2
#define VAR_FIXTYPE	4
#define VAR_UDFTYPE	8
#define VAR_CLEANUP	16
#define VAR_INIT	32
#define VAR_USED	64
#define VAR_DISABLED	128		/* used for comments and scheduler */

/* type check status is kept around to improve type checking efficiency */
#define TYPE_ERROR      -1
#define TYPE_UNKNOWN     0
#define TYPE_RESOLVED    2
#define GARBAGECONTROL   3

#define VARARGS 1				/* deal with variable arguments */
#define VARRETS 2

#define SERVERSHUTDOWNDELAY 5 /* seconds */

typedef int malType;
typedef str (*MALfcn) ();

typedef struct MalProp {
	bte idx;
	bte op;
	int var;
} *MalPropPtr, MalProp;

typedef struct SYMDEF {
	struct SYMDEF *peer;		/* where to look next */
	struct SYMDEF *skip;		/* skip to next different symbol */
	str name;
	int kind;
	struct MALBLK *def;			/* the details of the MAL fcn */
} *Symbol, SymRecord;

typedef struct VARRECORD {
	str name;					/* argname or lexical value repr */
	malType type;				/* internal type signature */
	int flags;					/* see below, reserve some space */
	int tmpindex;				/* temporary variable */
	ValRecord value;
	int eolife;					/* pc index when it should be garbage collected */
	int propc, maxprop;			/* proc count and max number of properties */
	int prps[];					/* property array */
} *VarPtr, VarRecord;

typedef struct {
	bit token;					/* instruction type */
	bit barrier;				/* flow of control modifier takes:
								   BARRIER, LEAVE, REDO, EXIT, CATCH, RAISE */
	bit typechk;				/* type check status */
	bit gc;						/* garbage control flags */
	bit polymorphic;			/* complex type analysis */
	bit varargs;				/* variable number of arguments */
	int recycle;				/* <0 or index into recycle cache */
	int jump;					/* controlflow program counter */
	MALfcn fcn;					/* resolved function address */
	struct MALBLK *blk;			/* resolved MAL function address */
	str modname;				/* module context */
	str fcnname;				/* function name */
	int argc, retc, maxarg;		/* total and result argument count */
	int argv[];					/* at least a few entries */
} *InstrPtr, InstrRecord;

/* For performance analysis we keep track of the number of calls and
 * the total time spent while executing the instruction. (See
 * mal_profiler.mx) The performance structures are separately
 * administered, because they are only used in limited
 * curcumstances. */

typedef struct PERF {
#ifdef HAVE_TIMES
	struct tms timer;			/* timing information */
#endif
	struct timeval clock;		/* clock */
	lng clk;					/* time when instruction started */
	lng ticks;					/* micro seconds spent on last call */
	lng totalticks;				/* accumulate micro seconds send on this instruction */
	int calls;					/* number of calls seen */
	bit trace;					/* facilitate filter-based profiling */
	lng rbytes;					/* bytes read by an instruction */
	lng wbytes;					/* bytes written by an instruction */
} *ProfPtr, ProfRecord;

typedef struct MALBLK {
	str binding;				/* related C-function */
	str help;					/* supportive commentary */
	oid tag;					/* unique block tag */
	struct MALBLK *alternative;
	int vtop;					/* next free slot */
	int vsize;					/* size of variable arena */
	VarRecord **var;			/* Variable table */
	int stop;					/* next free slot */
	int ssize;					/* byte size of arena */
	InstrPtr *stmt;				/* Instruction location */
	int ptop;					/* next free slot */
	int psize;					/* byte size of arena */
	MalProp *prps;				/* property table */
	int errors;					/* left over errors */
	int typefixed;				/* no undetermined instruction */
	int flowfixed;				/* all flow instructions are fixed */
	ProfPtr profiler;
	struct MALBLK *history;		/* of optimizer actions */
	short keephistory;			/* do we need the history at all */
	short dotfile;				/* send dot file to stethoscope? */
	str marker;					/* history points are marked for backtracking */
	int maxarg;					/* keep track on the maximal arguments used */
	ptr replica;				/* for the replicator tests */
	sht recycle;				/* execution subject to recycler control */
	lng recid;					/* Recycler identifier */
	lng legid;					/* Octopus control */
	sht trap;					/* call debugger when called */
	lng starttime;				/* track when the query started, for resource management */
	lng runtime;				/* average execution time of block in ticks */
	int calls;					/* number of calls */
	lng optimize;				/* total optimizer time */
} *MalBlkPtr, MalBlkRecord;

#define STACKINCR   128
#define MAXGLOBALS  (4 * STACKINCR)
#define MAXSHARES   8

typedef int (*DFhook) (void *, void *, void *, void *);

typedef struct MALSTK {
	int stksize;
	int stktop;
	int stkbot;			/* the first variable to be initialized */
	int stkdepth;		/* to protect against runtime stack overflow */
	int calldepth;		/* to protect against runtime stack overflow */
	short keepAlive;	/* do not garbage collect when set */
	short garbageCollect; /* stack needs garbage collection */
	lng tmpspace;		/* amount of temporary space produced */
	/*
	 * Parallel processing is mostly driven by dataflow, but within this context
	 * there may be different schemes to take instructions into execution.
	 * The admission scheme (and wrapup) are the necessary scheduler hooks.
	 */
	DFhook admit;
	DFhook wrapup;
	MT_Lock stklock;	/* used for parallel processing */

/*
 * It is handy to administer the timing in the stack frame
 * for use in profiling and recylcing instructions.
 */
	struct timeval clock;		/* time this stack was created */
	char cmd;		/* debugger and runtime communication */
	char status;	/* srunning 'R' uspended 'S', quiting 'Q' */
	int pcup;		/* saved pc upon a recursive all */
	struct MALSTK *up;	/* stack trace list */
	struct MALBLK *blk;	/* associated definition */
	ValRecord stk[1];
} MalStack, *MalStkPtr;

#endif /*  _MAL_H*/
