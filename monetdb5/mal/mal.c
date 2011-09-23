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
 * @f mal
 * @-
 * @node  Design Considerations, Architecture Overview, Design Overview, Design Overview
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
 * @-
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
 * @-
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
 * @-
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
int monet_welcome = 1;
str *monet_script;
int monet_daemon=0;
size_t monet_memory;
int nrservers = 0;

#include "mal_stack.h"
#include "mal_linker.h"
#include "mal_session.h"
#include "mal_parser.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"  /* for initNamespace() */
#include "mal_debugger.h" /* for mdbInit() */
#include "mal_client.h"
#include "mal_sabaoth.h"
#include "mal_recycle.h"

MT_Lock     mal_contextLock;
MT_Lock     mal_remoteLock;
MT_Lock  	mal_profileLock ;
MT_Lock     mal_copyLock;
/*
 * @-
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
	val= (ptr) & v.val.cval[0]; if(val != base){ allAligned = -1; }
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
	MT_lock_init( &mal_contextLock, "mal_contextLock");
	MT_lock_init( &mal_remoteLock, "mal_remoteLock");
	MT_lock_init( &mal_profileLock, "mal_profileLock");
	MT_lock_init( &mal_copyLock, "mal_copyLock");

	GDKprotect();
	tstAligned();
	MCinit();
	mdbInit();
	if (monet_memory == 0)
		monet_memory = MT_npages() * MT_pagesize();
	initNamespace();
	initParser();
	RECYCLEinit();
	if( malBootstrap() == 0)
		return -1;
	return 0;
}
/*
 * @-
 * Upon exit we should attempt to remove all allocated memory explicitly.
 * This seemingly superflous action is necessary to simplify analyis of
 * memory leakage problems later on.
 */
int
moreClients(int reruns)
{
	int freeclient=0, finishing=0, claimed=0, awaiting=0;
	Client cntxt = mal_clients;

	freeclient=0; finishing=0; claimed=0; awaiting=0;
	for(cntxt= mal_clients+1;  cntxt<mal_clients+MAL_MAXCLIENTS; cntxt++){
		freeclient += (cntxt->mode == FREECLIENT);
		finishing += (cntxt->mode == FINISHING);
		claimed += (cntxt->mode == CLAIMED);
		awaiting += (cntxt->mode == AWAITING);
		if( cntxt->mode & FINISHING)
			printf("#Client %d %d\n",(int)(cntxt - mal_clients), cntxt->idx);
	}
	if( reruns == 3){
		mnstr_printf(mal_clients->fdout,"#MALexit: server forced exit"
			" %d finishing %d claimed %d waiting\n",
				finishing,claimed,awaiting);
		return 0;
	}
	return finishing+claimed+awaiting;
}
void mal_exit(void){
 	int t = 0;
	str err;

	/*
	 * @-
	 * Before continuing we should make sure that all clients
	 * (except the console) have left the scene.
	 */
	RECYCLEshutdown(mal_clients); /* remove any left over intermediates */
	stopProfiling();
#if 0
{
	int reruns=0, goon;
	do{
		if ( (goon=moreClients(reruns)) )
			MT_sleep_ms(1000);
		if(reruns)
			mnstr_printf(mal_clients->fdout,"#MALexit: clients still active\n");
	} while (++reruns<3 && goon);
}
#endif
	unloadLibraries();
#if 0
	/* skip this to solve random crashes, needs work */
	freeBoxes(mal_clients);
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
		(void) mnstr_destroy(mal_clients->fdin->s);
		(void) bstream_destroy(mal_clients->fdin);
	}
	if( mal_clients->fdout && mal_clients->fdout != GDKstdout) {
		(void) mnstr_close(mal_clients->fdout);
		(void) mnstr_destroy(mal_clients->fdout);
	}
#endif
	/* deregister everything that was registered, ignore errors */
	if ((err = SABAOTHwildRetreat(&t)) != MAL_SUCCEED) {
		fprintf(stderr, "!%s", err);
		if (err != M5OutOfMemory)
			GDKfree(err);
	}
	/* the server will now be shut down */
	if ((err = SABAOTHregisterStop(&t)) != MAL_SUCCEED) {
		fprintf(stderr, "!%s", err);
		if (err != M5OutOfMemory)
			GDKfree(err);
	}
	GDKexit(0); 	/* properly end GDK */
}
