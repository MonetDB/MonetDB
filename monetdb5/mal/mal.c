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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @+ Design Considerations
 * Redesign of the MonetDB software stack was driven by the need to
 * reduce the effort to extend the system into novel directions
 * and to reduce the Total Execution Cost (TEC).
 * The TEC is what an end-user or application program will notice.
 * The TEC is composed on several cost factors:
 * @itemize
 * @item  A)
 * API message handling
 * @item  P)
 * Parsing and semantic analysis
 * @item  O)
 * Optimization and plan generation
 * @item  D)
 * Data access to the persistent store
 * @item  E)
 * Execution of the query terms
 * @item R)
 * Result delivery to the application
 * @end itemize
 *
 * Choosing an architecture for processing database operations pre-supposes an
 * intuition on how the cost will be distributed. In an OLTP
 * setting you expect most of the cost to be in (P,O), while in OLAP it will
 * be (D,E,R). In a distributed setting the components (O,D,E) are dominant.
 * Web-applications would focus on (A,E,R).
 *
 * Such a simple characterization ignores the wide-spread
 * differences that can be experienced at each level. To illustrate,
 * in D) and R) it makes a big difference whether the data is already in the
 * cache or still on disk. With E) it makes a big difference whether you
 * are comparing two integers, evaluation of a mathematical function,
 * e.g., Gaussian, or a regular expression evaluation on a string.
 * As a result, intense optimization in one area may become completely invisible
 * due to being overshadowed by other cost factors.
 *
 * The Version 5 infrastructure is designed to ease addressing each
 * of these cost factors in a well-defined way, while retaining the
 * flexibility to combine the components needed for a particular situation.
 * It results in an architecture where you assemble the components
 * for a particular application domain and hardware platform.
 *
 * The primary interface to the database kernel is still based on
 * the exchange of text in the form of queries and simply formatted results.
 * This interface is designed for ease of interpretation, versatility and
 * is flexible to accommodate system debugging and application tool development.
 * Although a textual interface potentially leads to a performance degradation,
 * our experience with earlier system versions
 * showed that the overhead can be kept within acceptable bounds.
 * Moreover, a textual interface reduces the programming
 * effort otherwise needed to develop test and application programs.
 * The XML trend as the language for tool interaction supports our decision.
 * 
 * @node Architecture Overview, MAL Synopsis, Design Considerations, Design  Overview
 * @+ Architecture Overview
 * The architecture is built around a few independent components:
 * the MonetDB server, the merovigian, and the client application.
 * The MonetDB server is the heart of the system, it manages a single
 * physical database on one machine for all (concurrent) applications.
 * The merovigian program works along side a single server, keeping
 * an eye on its behavior. If the server accidently crashes, it is this program
 * that will attempt an automatic restart.
 *
 * The top layer consists of applications written in your favorite
 * language.
 * They provide both specific functionality
 * for a particular product, e.g., @url{http://kdl.cs.umass.edu/software,Proximity},
 * and generic functionality, e.g.,
 * the @url{http://www.aquafold.com,Aquabrowser} or @url{http://www.minq.se,Dbvisualizer}.
 * The applications communicate with the server
 * using de-facto standard interface packaged,
 * i.e., JDBC, ODBC, Perl, PHP, etc.
 *
 * The middle layer consists of query language processors such as
 * SQL and XQuery. The former supports the core functionality
 * of SQL'99 and extends into SQL'03. The latter is based on
 * the W3C standard and includes the XUpdate functionality.
 * The query language processors each manage their own private catalog structure.
 * Software bridges, e.g., import/export routines, are used to
 * share data between language paradigms.
 *
 * @iftex
 * @image{base00,,,,.pdf}
 * @emph{Figure 2.1}
 * @end iftex
 * 
 * @node MAL Synopsis, Execution Engine, Architecture Overview,  Design  Overview
 * @+ MonetDB Assembly Language (MAL)
 * The target language for a query compiler is
 * the MonetDB Assembly Language (MAL).
 * It was designed to ease code generation
 * and fast interpretation by the server.
 * The compiler produces algebraic query plans, which
 * are turned into  physical execution
 * plans by the MAL optimizers.
 *
 * The output of a compiler is either an @sc{ascii} representation
 * of the MAL program or the compiler is tightly coupled with
 * the server to save parsing and communication overhead.
 *
 * A snippet of the MAL code produced by the SQL compiler
 * for the query @sc{select count(*) from tables}
 * is shown below. It illustrates a sequences of relational
 * operations against a table column and producing a
 * partial result.
 * @example
 * 	...
 *     _22:bat[:oid,:oid]  := sql.bind_dbat("tmp","_tables",0);
 *     _23 := bat.reverse(_22);
 *     _24 := algebra.kdifference(_20,_23);
 *     _25 := algebra.markT(_24,0:oid);
 *     _26 := bat.reverse(_25);
 *     _27 := algebra.join(_26,_20);
 *     _28 := bat.setWriteMode(_19);
 *     bat.append(_28,_27,true);
 * 	...
 * @end example
 *
 * MAL supports the full breadth of computational paradigms
 * deployed in a database setting. It is language framework
 * where the execution semantics is determined by the
 * code transformations and the final engine choosen.
 *
 * The design and implementation of MAL takes the functionality offered
 * previously a significant step further. To name a few:
 * @itemize @bullet
 * @item All instructions are strongly typed before being executed.
 * @item It supports polymorphic functions.
 * They act as templates that produce strongly typed instantiations when needed.
 * @item Function style expressions where
 * each assignment instruction can receive multiple target results;
 * it forms a point in the dataflow graph.
 * @item It supports co-routines (Factories) to build streaming applications.
 * @item Properties are associated with the program code for
 * ease of optimization and scheduling.
 * @item It can be readily extended with user defined types and
 * function modules.
 * @end itemize
 *
 * @+ Critical sections and semaphores
 * MonetDB Version 5 is implemented as a collection of threads.
 * This calls for extreme
 * care in coding. At several places locks and semaphores are necessary
 * to achieve predictable results. In particular, after they are created
 * and when they are inspected or being modified to take decisions.
 *
 * In the current implementation the following list of locks and semaphores
 * is used in the Monet layer:
 *
 */
#include <monetdb_config.h>
#include <mal.h>

char monet_cwd[PATHLENGTH] = { 0 };
size_t monet_memory;
char *mal_trace;		/* enable profile events on console */

#include "mal_stack.h"
#include "mal_linker.h"
#include "mal_session.h"
#include "mal_parser.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"  /* for initNamespace() */
#include "mal_client.h"
#include "mal_sabaoth.h"
#include "mal_recycle.h"
#include "mal_dataflow.h"
#include "mal_profiler.h"
#include "mal_private.h"

MT_Lock     mal_contextLock MT_LOCK_INITIALIZER("mal_contextLock");
MT_Lock     mal_namespaceLock MT_LOCK_INITIALIZER("mal_namespaceLock");
MT_Lock     mal_remoteLock MT_LOCK_INITIALIZER("mal_remoteLock");
MT_Lock  	mal_profileLock MT_LOCK_INITIALIZER("mal_profileLock");
MT_Lock     mal_copyLock MT_LOCK_INITIALIZER("mal_copyLock");
MT_Lock     mal_delayLock MT_LOCK_INITIALIZER("mal_delayLock");
MT_Sema		mal_parallelism;
/*
 * Initialization of the MAL context
 * The compiler directive STRUCT_ALIGNED tells that the
 * fields in the VALrecord all start at the same offset.
 * This knowledge avoids low-level type decodings, but should
 * be assured at least once for each platform.
 */

static
void tstAligned(void)
{
#ifdef STRUCT_ALIGNED
	int allAligned=0;
	ValRecord v;
	ptr val, base;
	base = (ptr) & v.val.ival;
	val= (ptr) & v.val.bval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.btval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.shval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.bval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.ival; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.oval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.pval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.fval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.dval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.lval; if(val != base){ allAligned = -1; }
	val= (ptr) & v.val.sval; if(val != base){ allAligned = -1; }
	if(allAligned<0)
	    GDKfatal("Recompile with STRUCT_ALIGNED flag disabled\n");
#endif
}
int mal_init(void){
#ifdef NEED_MT_LOCK_INIT
	MT_lock_init( &mal_contextLock, "mal_contextLock");
	MT_lock_init( &mal_namespaceLock, "mal_namespaceLock");
	MT_lock_init( &mal_remoteLock, "mal_remoteLock");
	MT_lock_init( &mal_profileLock, "mal_profileLock");
	MT_lock_init( &mal_copyLock, "mal_copyLock");
	MT_lock_init( &mal_delayLock, "mal_delayLock");
#endif
	/* "/2" is arbitrarily used / chosen, as on systems with
	 * hyper-threading enabled, using all hardware threads rather than
	 * "only" all physical cores does not necessarily yield a linear
	 * performance benefit */
	MT_sema_init( &mal_parallelism, (GDKnr_threads > 1 ? GDKnr_threads/2: 1), "mal_parallelism");

	tstAligned();
	MCinit();
	if (mdbInit()) 
		return -1;
	if (monet_memory == 0)
		monet_memory = MT_npages() * MT_pagesize();
	initNamespace();
	initParser();
	initHeartbeat();
	initResource();
	RECYCLEinit();
	if( malBootstrap() == 0)
		return -1;
	/* set up the profiler if needed, output sent to console */
	/* Use the same shortcuts as stethoscope */
	if ( mal_trace && *mal_trace) {
		char *s;
		setFilterAll();
		openProfilerStream(mal_clients[0].fdout);
		for ( s= mal_trace; *s; s++)
		switch(*s){
		case 'a': activateCounter("aggregate");break;
		case 'b': activateCounter("rbytes");
				activateCounter("wbytes");break;
		case 'c': activateCounter("cpu");break;
		case 'e': activateCounter("event");break;
		case 'f': activateCounter("function");break;
		case 'i': activateCounter("pc");break;
		case 'm': activateCounter("memory");break;
		case 'p': activateCounter("process");break;
		case 'r': activateCounter("reads");break;
		case 's': activateCounter("stmt");break;
		case 't': activateCounter("ticks");break;
		case 'u': activateCounter("user");break;
		case 'w': activateCounter("writes");break;
		case 'y': activateCounter("type");break;
		case 'D': activateCounter("dot");break;
		case 'I': activateCounter("thread");break; 
		case 'T': activateCounter("time");break;
		case 'S': activateCounter("start");
		}
		startProfiling();
	} else mal_trace =0;
	return 0;
}
/*
 * Upon exit we should attempt to remove all allocated memory explicitly.
 * This seemingly superflous action is necessary to simplify analyis of
 * memory leakage problems later on.
 */

/* stopping clients should be done with care, as they may be in the mids of
 * transactions. One safe place is between MAL instructions, which would
 * abort the transaction by raising an exception. All non-console sessions are
 * terminate this way.
 * We should also ensure that no new client enters the scene while shutting down.
 * For this we mark the client records as BLOCKCLIENT.
 *
 * Beware, mal_exit is also called during a SIGTERM from the monetdb tool
 */

void mal_exit(void){
	str err;

	/*
	 * Before continuing we should make sure that all clients
	 * (except the console) have left the scene.
	 */
	MCstopClients(0);
#if 0
{
	int reruns=0, go_on;
	do{
		if ( (go_on = MCactiveClients()) )
			MT_sleep_ms(1000);
		mnstr_printf(mal_clients->fdout,"#MALexit: %d clients still active\n", go_on);
	} while (++reruns < SERVERSHUTDOWNDELAY && go_on > 1);
}
#endif
	stopHeartbeat();
	stopMALdataflow();
	stopProfiling();
	RECYCLEdrop(mal_clients); /* remove any left over intermediates */
	unloadLibraries();
#if 0
	/* skip this to solve random crashes, needs work */
	freeModuleList(mal_clients->nspace);

	finishNamespace();
	if( mal_clients->prompt)
		GDKfree(mal_clients->prompt);
	if( mal_clients->errbuf)
		GDKfree(mal_clients->errbuf);
	if( mal_clients->bak)
		GDKfree(mal_clients->bak);
	if( mal_clients->fdin){
		/* missing protection against closing stdin stream */
		(void) mnstr_close(mal_clients->fdin->s);
		(void) bstream_destroy(mal_clients->fdin);
	}
	if( mal_clients->fdout && mal_clients->fdout != GDKstdout) {
		(void) mnstr_close(mal_clients->fdout);
		(void) mnstr_destroy(mal_clients->fdout);
	}
#endif
	/* deregister everything that was registered, ignore errors */
	if ((err = msab_wildRetreat()) != NULL) {
		fprintf(stderr, "!%s", err);
		free(err);
	}
	/* the server will now be shut down */
	if ((err = msab_registerStop()) != NULL) {
		fprintf(stderr, "!%s", err);
		free(err);
	}
	GDKexit(0); 	/* properly end GDK */
}
