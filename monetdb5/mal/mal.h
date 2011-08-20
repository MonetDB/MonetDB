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
 * @+ Monet Basic Definitions
 * Definitions that need to included in every file of the Monet system,
 * as well as in user defined module implementations.
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

/*
 * @+ Monet Calling Options
 * The number of invocation arguments is kept to a minimum.
 * See `man mserver5` or tools/mserver/mserver5.1
 * for additional system variable settings.
 * @
 */
#define MAXSCRIPT 64

mal_export char            monet_cwd[PATHLENGTH];
mal_export int             monet_welcome;
mal_export str             *monet_script;
mal_export int 				monet_daemon;
mal_export size_t			monet_memory;
mal_export int				nrservers;

#define mal_set_lock(X,Y) if(GDKprotected) MT_lock_set(&X,Y)
#define mal_unset_lock(X,Y) if(GDKprotected) MT_lock_unset(&X,Y)
#define mal_up_sema(X,Y) if(GDKprotected) MT_sema_up(&X,Y)
#define mal_down_sema(X,Y) if(GDKprotected) MT_sema_down(&X,Y)

/*
   See gdk/gdk.mx for the definition of all debug masks.
   See `man mserver5` or tools/mserver/mserver5.1
   for a documentation of all debug options.
*/
#define GRPthreads ( 1 | PARMASK)
#define GRPmemory (MEMMASK | ALLOCMASK )
#define GRPproperties (CHECKMASK | PROPMASK | BATMASK )
#define GRPio (IOMASK | PERFMASK )
#define GRPtransactions ( TMMASK | DELTAMASK | TEMMASK)
#define GRPmodules (DLMASK | LOADMASK)
#define GRPalgorithms (ALGOMASK | ESTIMASK)
#define GRPxproperties (XPROPMASK )
#define GRPperformance (JOINPROPMASK | DEADBEEFMASK)
#define GRPoptimizers  (1<<27)	/* == OPTMASK; cf., gdk/gdk.mx */
#define GRPforcemito (FORCEMITOMASK)
/*
 * @-
 * @node Execution Engine, Session Scenarios, MAL Synopsis , Design  Overview
 * @+ Execution Engine
 * The execution engine comes in several flavors. The default is a
 * simple, sequential MAL interpreter. For each MAL function call it creates
 * a stack frame, which is initialized with all constants found in the
 * function body. During interpretation the garbage collector
 * ensures freeing of space consumptive tables (BATs) and strings.
 * Furthermore, all temporary structures are garbage collected before
 * the funtion returns the result.
 *
 * This simple approach leads to an accumulation of temporary variables.
 * They can be freed earlier in the process using an explicit garbage collection
 * command, but the general intend is to leave such decisions to an optimizer
 * or scheduler.
 *
 * The execution engine is only called when all MAL instructions
 * can be resolved against the available libraries.
 * Most modules are loaded when the server starts using a
 * bootstrap script @sc{mal_init.mx}
 * Failure to find the startup-file terminates the session.
 * It most likely points to an error in the MonetDB configuration file.
 *
 * During the boot phase, the global symbol table is initialized
 * with MAL function and factory definitions, and
 * loading the pre-compiled commands and patterns.
 * The libraries are dynamically loaded by default.
 * Expect tens of modules and hundreds of operations to become readily available.
 *
 * Modules can not be dropped without restarting the server.
 * The rational behind this design decision is that a dynamic load/drop feature
 * is often hardly used and severely complicates the code base.
 * In particular, upon each access to the global symbol table we have to be
 * prepared that concurrent threads may be actively changing its structure.
 * Especially, dropping modules may cause severe problems by not being
 * able to detect all references kept around.
 * This danger required all accesses to global information to be packaged
 * in a critical section, which is known to be a severe performance hindrance.
 *
 */


mal_export MT_Lock  mal_contextLock;
mal_export MT_Lock  mal_remoteLock;
mal_export MT_Lock  mal_profileLock ;
mal_export MT_Lock  mal_copyLock ;


mal_export int mal_init(void);
mal_export void mal_exit(void);
mal_export int moreClients(int reruns);

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

#endif /*  _MAL_H*/
