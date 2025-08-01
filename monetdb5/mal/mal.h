/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 * (c) Martin Kersten
 *  MonetDB Basic Definitions
 * Definitions that need to included in every file of the Monet system, as well as in user defined module implementations.
 */
#ifndef _MAL_H
#define _MAL_H

#include "gdk.h"

#ifdef WIN32
#ifndef LIBMONETDB5
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

/*
 * MonetDB Calling Options
 * The number of invocation arguments is kept to a minimum.
 * See `man mserver5` or tools/mserver/mserver5.1
 * for additional system variable settings.
 */
#define MAXSCRIPT 64
mal_export lng MALdebug;

/*
 * MonetDB assumes it can use most of the machines memory,
 * leaving a small portion for other programs.
 */
#define GB (((lng)1024)*1024*1024)
#define MEMORY_THRESHOLD  (0.2 * GDK_mem_maxsize > 8 * GB?  GDK_mem_maxsize - 8 * GB: 0.8 * GDK_mem_maxsize)

mal_export char monet_cwd[FILENAME_MAX];
mal_export char monet_characteristics[4096];
mal_export stream *maleventstream;

/*
   See gdk/gdk.h for the definition of all debug masks.
   See `man mserver5` or tools/mserver/mserver5.1
   for a documentation of all debug options.
*/
#define GRPthreads (THRDMASK | PARMASK)
#define GRPmemory (ALLOCMASK )
#define GRPproperties (CHECKMASK )
#define GRPio (IOMASK | PERFMASK )
#define GRPheaps (HEAPMASK)
#define GRPtransactions (TMMASK | DELTAMASK | TEMMASK)
#define GRPmodules (LOADMASK)
#define GRPalgorithms (ALGOMASK)
#define GRPperformance (DEADBEEFMASK)
#define GRPforcemito (FORCEMITOMASK | NOSYNCMASK)

mal_export MT_Lock mal_contextLock;

mal_export int mal_init(char *modules[], bool embedded, const char *initpasswd,
						const char *caller_revision);
mal_export _Noreturn void mal_exit(int status);
mal_export void mal_reset(void);
mal_export const char *mal_version(void);

/* This should be here, but cannot, as "Client" isn't known, yet ... |-(
 * For now, we move the prototype declaration to src/mal/mal_client.c,
 * the only place where it is currently used. Maybe, we should consider
 * also moving the implementation there...
 */

/* Listing modes are globally known */
#define LIST_INPUT      1		/* echo original input */
#define LIST_MAL_NAME   2		/* show variable name */
#define LIST_MAL_TYPE   4		/* show type resolutoin */
#define LIST_MAL_VALUE  8		/* list bat tuple count */
#define LIST_MAL_PROPS 16		/* show variable properties */
#define LIST_MAL_MAPI  32		/* output Mapi compatible output */
#define LIST_MAL_REMOTE  64		/* output MAL for remote execution */
#define LIST_MAL_FLOW   128		/* output MAL dataflow dependencies */
#define LIST_MAL_ALGO	256		/* output algorithm used */
#define LIST_MAL_NOCFUNC	512		/* skip C function */
#define LIST_MAL_CALL  (LIST_MAL_NAME | LIST_MAL_VALUE )
#define LIST_MAL_DEBUG (LIST_MAL_NAME | LIST_MAL_VALUE | LIST_MAL_TYPE | LIST_MAL_PROPS | LIST_MAL_FLOW)
#define LIST_MAL_ALL   (LIST_MAL_NAME | LIST_MAL_VALUE | LIST_MAL_TYPE | LIST_MAL_MAPI)

#define VARARGS 1				/* deal with variable arguments */
#define VARRETS 2

typedef int malType;
typedef void (*MALfcn)(void);

#include "mel.h"

typedef struct SYMDEF {
	struct SYMDEF *peer;		/* where to look next */
	struct SYMDEF *skip;		/* skip to next different symbol */
	const char *name;
	int kind;					/* what kind of symbol */
	bool allocated;				/* allocated using mallocs or compiled inside the binary */
	struct MALBLK *def;			/* the details of the MAL fcn */
	mel_func *func;
} *Symbol, SymRecord;


typedef struct VARRECORD {
	char *name;					/* use the space for the full name */
	malType type;				/* internal type signature */
	char kind;					/* Could be either _, X or C to stamp the variable type */
	bool constant:1,
		typevar:1,
		fixedtype:1,
		cleanup:1,
		initialized:1,
		used:1,
		disabled:1;
	short depth;				/* scope block depth, set to -1 if not used */
	ValRecord value;
	int declared;				/* pc index when it was first assigned */
	int updated;				/* pc index when it was first updated */
	int eolife;					/* pc index when it should be garbage collected */
	int stc;					/* pc index for rendering schema.table.column  */
	BUN rowcnt;					/* estimated row count */
} *VarPtr, VarRecord;

/* For performance analysis we keep track of the number of calls and
 * the total time spent while executing the instruction. (See
 * mal_profiler.c)
 */

typedef struct INSTR {
	bte token;					/* instruction type */
	bte barrier;				/* flow of control modifier takes:
								   BARRIER, LEAVE, REDO, EXIT, CATCH, RAISE */
	uint16_t polymorphic:3,		/* complex type analysis */
		varargs:2,				/* variable number of arguments */
		inlineProp:1,			/* inline property */
		unsafeProp:1,			/* unsafe property */
		gc:1,					/* garbage control flags */
		typeresolved:1;			/* true if type is resolved */
	int jump;					/* controlflow program counter */
	int pc;						/* location in MAL plan for profiler */
	MALfcn fcn;					/* resolved function address */
	struct MALBLK *blk;			/* resolved MAL function address */
	/* inline statistics */
	lng wbytes;					/* number of bytes produced in last instruction */
	/* the core admin */
	const char *modname;		/* module context, reference into namespace */
	const char *fcnname;		/* function name, reference into namespace */
	int argc, retc, maxarg;		/* total and result argument count */
	int argv[];					/* at least a few entries */
} *InstrPtr, InstrRecord;

typedef struct MALBLK {
	char binding[IDLENGTH];		/* related C-function */
	str help;					/* supportive commentary */
	oid tag;					/* unique block tag */
	int vtop;					/* next free slot */
	int vsize;					/* size of variable arena */
	VarRecord *var;				/* Variable table */
	int stop;					/* next free slot */
	int ssize;					/* byte size of arena */
	InstrPtr *stmt;				/* Instruction location */

	bool inlineProp:1,			/* inline property */
	 unsafeProp:1;				/* unsafe property */

	str errors;					/* left over errors */
	int maxarg;					/* keep track on the maximal arguments used */

	/* During the run we keep track on the maximum number of concurrent threads and memory claim */
	ATOMIC_TYPE workers;
	lng memory;
	lng runtime;				/* average execution time of block in ticks */
	int calls;					/* number of calls */
	lng optimize;				/* total optimizer time */
} *MalBlkPtr, MalBlkRecord;

#define STACKINCR   128
#define MAXGLOBALS  (4 * STACKINCR)

typedef int (*DFhook)(void *, void *, void *, void *);

typedef struct MALSTK {
	int stksize;
	int stktop;
	int stkbot;					/* the first variable to be initialized */
	int stkdepth;				/* to protect against runtime stack overflow */
	int calldepth;				/* to protect against runtime stack overflow */
	bool keepAlive:1,			/* do not garbage collect when set */
	 keepTmps:1;				/* also do not garbage collect tmps (needed for interactive debugging only) */
	/*
	 * Parallel processing is mostly driven by dataflow, but within this context
	 * there may be different schemes to take instructions into execution.
	 * The admission scheme (and wrapup) are the necessary scheduler hooks.
	 */
	DFhook admit;
	DFhook wrapup;

/*
 * It is handy to administer the timing in the stack frame
 * for use in profiling instructions.
 */
	char status;				/* running 'R' suspended 'S', quitting 'Q' */
	int pcup;					/* saved pc upon a recursive all */
	oid tag;					/* unique invocation call tag */
	lng memory;					/* Actual memory claims for highwater mark */

	struct MALSTK *up;			/* stack trace list */
	struct MALBLK *blk;			/* associated definition */
	ValRecord stk[];
} MalStack, *MalStkPtr;

#endif /*  _MAL_H */
