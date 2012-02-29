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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f opt_pipes
 * @a M.L. Kersten
 * @-
 * The default SQL optimizer pipeline can be set per server.  See the
 * optpipe setting in monetdb(1) when using merovingian.  During SQL
 * initialization, the optimizer pipeline is checked against the
 * dependency information maintained in the optimizer library to ensure
 * there are no conflicts and at least the pre-requisite optimizers are
 * used.  The setting of sql_optimizer can be either the list of
 * optimizers to run, or one or more variables containing the optimizer
 * pipeline to run.  The latter is provided for readability purposes
 * only.
 */
#include "monetdb_config.h"
#include "opt_pipes.h"
#include "mal_client.h"
#include "mal_instruction.h"
#include "mal_function.h"
#include "mal_listing.h"
#include "mal_linker.h"

#define MAXOPTPIPES 64

struct PIPELINES{
	char *name;
	char *def;
	char *status;
	char *prerequisite;
	MalBlkPtr mb;
} pipes[MAXOPTPIPES] ={
/* The minimal pipeline necessary by the server to operate correctly*/
{ "minimal_pipe",	
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.deadcode();"
	"optimizer.multiplex();"
	"optimizer.garbageCollector();", 
	"stable",0,0},

/*
 * The default pipe line contains as of Feb2010 mitosis-mergetable-reorder,
 * aimed at large tables and improved access locality
*/
{ "default_pipe",
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.evaluate();"
	"optimizer.costModel();"
	"optimizer.coercions();"
	"optimizer.emptySet();"
	"optimizer.aliases();"
	"optimizer.mitosis();"
	"optimizer.mergetable();"
	"optimizer.deadcode();"
	"optimizer.commonTerms();"
	"optimizer.joinPath();"
	"optimizer.reorder();"
	"optimizer.deadcode();"
	"optimizer.reduce();"
	"optimizer.dataflow();"
	"optimizer.history();"
	"optimizer.multiplex();"
	"optimizer.accumulators();"
	"optimizer.garbageCollector();",
	 "stable",0,0},

/*
 * The no_mitosis pipe line is (and should be kept!) identical to the default pipeline,
 * except that optimizer mitosis is omitted.  It is used mainly to make some tests work
 * deterministically, and to check / debug whether "unexpected" problems are related to
 * mitosis (and/or mergetable).
*/
{ "no_mitosis_pipe",
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.evaluate();"
	"optimizer.costModel();"
	"optimizer.coercions();"
	"optimizer.emptySet();"
	"optimizer.aliases();"
	"optimizer.mergetable();"
	"optimizer.deadcode();"
	"optimizer.commonTerms();"
	"optimizer.joinPath();"
	"optimizer.reorder();"
	"optimizer.deadcode();"
	"optimizer.reduce();"
	"optimizer.dataflow();"
	"optimizer.history();"
	"optimizer.multiplex();"
	"optimizer.accumulators();"
	"optimizer.garbageCollector();",
	"stable",0,0},

/* The sequential pipe line is (and should be kept!) identical to the default pipeline,
 * except that optimizers mitosis & dataflow are omitted.  It is use mainly to make some
 * tests work deterministically, i.e., avoid ambigious output, by avoiding parallelism.
*/
{ "sequential_pipe",	
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.evaluate();"
	"optimizer.costModel();"
	"optimizer.coercions();"
	"optimizer.emptySet();"
	"optimizer.aliases();"
	"optimizer.mergetable();"
	"optimizer.deadcode();"
	"optimizer.commonTerms();"
	"optimizer.joinPath();"
	"optimizer.reorder();"
	"optimizer.deadcode();"
	"optimizer.reduce();"
	"optimizer.history();"
	"optimizer.multiplex();"
	"optimizer.accumulators();"
	"optimizer.garbageCollector();",
	"stable", 0,0 },

/* The default pipeline used in the November 2009 release
{ "nov2009_pipe",	
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.evaluate();"
	"optimizer.costModel();"
	"optimizer.coercions();"
	"optimizer.emptySet();"
	"optimizer.aliases();"
	"optimizer.mergetable();"
	"optimizer.deadcode();"
	"optimizer.constants();"
	"optimizer.commonTerms();"
	"optimizer.joinPath();"
	"optimizer.deadcode();"
	"optimizer.reduce();"
	"optimizer.dataflow();"
	"optimizer.history();"
	"optimizer.multiplex();"
	"optimizer.garbageCollector();",
	"stable",0,0},
*/

/*
 * Experimental pipelines stressing various components under development
 * Do not use any of these pipelines in production settings!
*/
/* Not yet compiled
{"replication_pipe",	
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.evaluate();"
	"optimizer.costModel();"
	"optimizer.coercions();"
	"optimizer.emptySet();"
	"optimizer.aliases();"
	"optimizer.mergetable();"
	"optimizer.deadcode();"
	"optimizer.constants();"
	"optimizer.commonTerms();"
	"optimizer.joinPath();"
	"optimizer.deadcode();"
	"optimizer.reduce();"
	"optimizer.dataflow();"
	"optimizer.history();"
	"optimizer.replication();"
	"optimizer.multiplex();"
	"optimizer.garbageCollector();",
	"experimental",0,0 },
*/

{"accumulator_pipe",	
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.evaluate();"
	"optimizer.costModel();"
	"optimizer.coercions();"
	"optimizer.emptySet();"
	"optimizer.aliases();"
	"optimizer.mergetable();"
	"optimizer.deadcode();"
	"optimizer.constants();"
	"optimizer.commonTerms();"
	"optimizer.joinPath();"
	"optimizer.deadcode();"
	"optimizer.reduce();"
	"optimizer.dataflow();"
	"optimizer.history();"
	"optimizer.multiplex();"
	"optimizer.accumulators();"
	"optimizer.garbageCollector();",
     "stable",0,0},

{"recycler_pipe",	
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.evaluate();"
	"optimizer.costModel();"
	"optimizer.coercions();"
	"optimizer.emptySet();"
	"optimizer.aliases();"
	"optimizer.deadcode();"
	"optimizer.commonTerms();"
	"optimizer.joinPath();"
	"optimizer.deadcode();"
	"optimizer.recycle();"
	"optimizer.reduce();"
	"optimizer.history();"
	"optimizer.multiplex();"
	"optimizer.garbageCollector();",
	"experimental",0,0},

{"cracker_pipe",	
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.evaluate();"
	"optimizer.costModel();"
	"optimizer.coercions();"
	"optimizer.emptySet();"
	"optimizer.aliases();"
	"optimizer.selcrack();"
	"optimizer.deadcode();"
	"optimizer.commonTerms();"
	"optimizer.joinPath();"
	"optimizer.reorder();"
	"optimizer.deadcode();"
	"optimizer.reduce();"
	"optimizer.dataflow();"
	"optimizer.history();"
	"optimizer.multiplex();"
	"optimizer.garbageCollector();",
	"experimental","OPTselcrack",0},

{"sidcrack_pipe",	
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.evaluate();"
	"optimizer.costModel();"
	"optimizer.coercions();"
	"optimizer.emptySet();"
	"optimizer.aliases();"
	"optimizer.sidcrack();"
	"optimizer.deadcode();"
	"optimizer.commonTerms();"
	"optimizer.joinPath();"
	"optimizer.reorder();"
	"optimizer.deadcode();"
	"optimizer.reduce();"
	"optimizer.dataflow();"
	"optimizer.history();"
	"optimizer.multiplex();"
	"optimizer.garbageCollector();",
	"experimental","OPTsidcrack",0},

/*
 * The Octopus pipeline for distributed processing (Merovingian enabled platforms only)
*/
#ifndef WIN32
{"octopus_pipe",	
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.evaluate();"
	"optimizer.costModel();"
	"optimizer.coercions();"
	"optimizer.emptySet();"
	"optimizer.aliases();"
	"optimizer.mitosis();"
	"optimizer.mergetable();"
	"optimizer.deadcode();"
	"optimizer.commonTerms();"
	"optimizer.joinPath();"
	"optimizer.reorder();"
	"optimizer.deadcode();"
	"optimizer.costModel();"
	"optimizer.octopus();"
	"optimizer.reduce();"
	"optimizer.dataflow();"
	"optimizer.history();"
	"optimizer.multiplex();"
	"optimizer.garbageCollector();",
	"experimental", "OPToctopus",0 },
/* 
 * The centipede pipe line aims at a map-reduce style of query processing
*/
{ "centipede",	
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.evaluate();"
	"optimizer.costModel();"
	"optimizer.coercions();"
	"optimizer.emptySet();"
	"optimizer.aliases();"
	"optimizer.centipede();"
	"optimizer.mergetable();"
	"optimizer.deadcode();"
	"optimizer.commonTerms();"
	"optimizer.joinPath();"
	"optimizer.reorder();"
	"optimizer.deadcode();"
	"optimizer.reduce();"
	"optimizer.dataflow();"
	"optimizer.history();"
	"optimizer.multiplex();"
	"optimizer.accumulators();"
	"optimizer.garbageCollector();",
	"experimental", "OPTcentipede",0 },
#endif

{"datacell_pipe",
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.datacell();"
	"optimizer.garbageCollector();"
	"optimizer.evaluate();"
	"optimizer.costModel();"
	"optimizer.coercions();"
	"optimizer.emptySet();"
	"optimizer.aliases();"
	"optimizer.mitosis();"
	"optimizer.mergetable();"
	"optimizer.deadcode();"
	"optimizer.commonTerms();"
	"optimizer.joinPath();"
	"optimizer.reorder();"
	"optimizer.deadcode();"
	"optimizer.reduce();"
	"optimizer.dataflow();"
	"optimizer.history();"
	"optimizer.multiplex();"
	"optimizer.accumulators();"
	"optimizer.garbageCollector();",
	"experimental", "OPTdatacell",0},

/* The default + datacyclotron*/
{"datacyclotron_pipe",	
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.evaluate();"
	"optimizer.costModel();"
	"optimizer.coercions();"
	"optimizer.emptySet();"
	"optimizer.aliases();"
	"optimizer.datacyclotron();"
	"optimizer.mergetable();"
	"optimizer.deadcode();"
	"optimizer.commonTerms();"
	"optimizer.joinPath();"
	"optimizer.reorder();"
	"optimizer.deadcode();"
	"optimizer.reduce();"
	"optimizer.dataflow();"
	"optimizer.history();"
	/* "optimizer.replication();" not used */
	"optimizer.multiplex();"
	"optimizer.garbageCollector();",
	"experimental", "OPTdatacyclotron",0},

/* The default + dictionary*/
{"dictionary_pipe",	
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.dictionary();"
	"optimizer.evaluate();"
	"optimizer.costModel();"
	"optimizer.coercions();"
	"optimizer.emptySet();"
	"optimizer.aliases();"
	"optimizer.mergetable();"
	"optimizer.deadcode();"
	"optimizer.constants();"
	"optimizer.commonTerms();"
	"optimizer.joinPath();"
	"optimizer.deadcode();"
	"optimizer.reduce();"
	"optimizer.dataflow();"
	"optimizer.history();"
	"optimizer.multiplex();"
	"optimizer.garbageCollector();",
	"experimental","OPTdictionary",0},

/* The default + compression */
{"compression_pipe",	
	"optimizer.inline();"
	"optimizer.remap();"
	"optimizer.evaluate();"
	"optimizer.costModel();"
	"optimizer.coercions();"
	"optimizer.emptySet();"
	"optimizer.aliases();"
	"optimizer.mergetable();"
	"optimizer.deadcode();"
	"optimizer.constants();"
	"optimizer.commonTerms();"
	"optimizer.joinPath();"
	"optimizer.deadcode();"
	"optimizer.reduce();"
	"optimizer.dataflow();"
	"optimizer.compression();"
	"optimizer.dataflow();"
	"optimizer.history();"
	"optimizer.multiplex();"
	"optimizer.garbageCollector();",
	"experimental","OPTcompress",0}

};
#ifdef WIN32
static int builtinoptimizers = 8;
#else
static int builtinoptimizers = 11;
#endif
/*
 * @-
 * Debugging the optimizer pipeline",
 * The best way is to use mdb and inspect the information gathered",
 * during the optimization phase.  Several optimizers produce more",
 * intermediate information, which may shed light on the details.  The",
 * opt_debug bitvector controls their output. It can be set to a",
 * pipeline or a comma separated list of optimizers you would like to",
 * trace. It is a server wide property and can not be set dynamically,",
 * as it is intended for internal use.",
 */
#include "opt_pipes.h"

/* the session_pipe is the one defined by the user */
str
addPipeDefinition(Client cntxt, str name, str pipe)
{
	int i;
	str msg;

	for( i =0; i< MAXOPTPIPES && pipes[i].name; i++)
	if ( pipes[i].name && strcmp(name,pipes[i].name)==0)
		break;

	if ( i < builtinoptimizers)
		throw(MAL, "optimizer.addPipeDefinition", "No overwrite of built in allowed" );
	if ( i == MAXOPTPIPES )
		throw(MAL, "optimizer.addPipeDefinition", "Out of slots" );
	if ( pipes[i].name )
		GDKfree(pipes[i].name);
	if ( pipes[i].def )
		GDKfree(pipes[i].def);
	if ( pipes[i].mb)
		freeMalBlk(pipes[i].mb);
	if ( pipes[i].status )
		GDKfree(pipes[i].status);
	pipes[i].name = GDKstrdup(name);
	pipes[i].def = GDKstrdup(pipe);
	pipes[i].status = GDKstrdup("experimental");
	pipes[i].mb = NULL;
	msg = compileOptimizer(cntxt, name);
	if ( msg){
		GDKfree(pipes[i].status);
		pipes[i].status = GDKstrdup(msg);
	}
	return msg;
}

int
isOptimizerPipe(str name)
{
	int i;

	for( i=0; i < MAXOPTPIPES && pipes[i].name; i++)
		if ( strcmp(name, pipes[i].name) == 0)
			return TRUE;
	return FALSE;
}

str
getPipeDefinition(str name)
{
	int i;

	for( i=0; i < MAXOPTPIPES && pipes[i].name; i++)
		if ( strcmp(name, pipes[i].name) == 0)
			return GDKstrdup(pipes[i].def);
	return NULL;
}

str
getPipeCatalog(int *nme, int *def, int *stat)
{
	BAT *b,*bn, *bs;
	int i;
	b= BATnew(TYPE_void, TYPE_str, 20);
	if ( b == NULL)
		throw(MAL, "optimizer.getpipeDefinition", MAL_MALLOC_FAIL );

	bn= BATnew(TYPE_void, TYPE_str, 20);
	if ( bn == NULL){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "optimizer.getpipeDefinition", MAL_MALLOC_FAIL );
	}

	bs= BATnew(TYPE_void, TYPE_str, 20);
	if ( bs == NULL){
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(bn->batCacheid);
		throw(MAL, "optimizer.getpipeDefinition", MAL_MALLOC_FAIL );
	}

	BATseqbase(b,0);
	BATseqbase(bn,0);
	BATseqbase(bs,0);
	for( i=0; i < MAXOPTPIPES && pipes[i].name; i++){
		if ( pipes[i].prerequisite &&
			getAddress(GDKout, NULL, optimizerRef, pipes[i].prerequisite, TRUE) == NULL)
		continue;
		BUNappend(b,pipes[i].name, FALSE);
		BUNappend(bn,pipes[i].def, FALSE);
		BUNappend(bs,pipes[i].status, FALSE);
	}

	BBPkeepref(*nme= b->batCacheid);
	BBPkeepref(*def= bn->batCacheid);
	BBPkeepref(*stat= bs->batCacheid);
	return MAL_SUCCEED;
}

static str
validatePipe(MalBlkPtr mb){
	int mitosis= FALSE, deadcode= FALSE, mergetable= FALSE, multiplex=FALSE, garbage=FALSE;
	int i;

	if ( getFunctionId(getInstrPtr(mb,1)) == NULL || idcmp(getFunctionId( getInstrPtr(mb,1)), "inline" ) )
		throw(MAL,"optimizer.validate","'inline' should be the first\n");

	/* deadcode should be used */
	for ( i=1; i < mb->stop -1; i++)
	if ( getFunctionId(getInstrPtr(mb,i)) != NULL){
		if (strcmp(getFunctionId(getInstrPtr(mb,i)),"deadcode") == 0)
			deadcode= TRUE;
		else
		if (strcmp(getFunctionId(getInstrPtr(mb,i)),"mitosis") == 0)
			mitosis= TRUE;
		else
		if (strcmp(getFunctionId(getInstrPtr(mb,i)),"mergetable") == 0)
			mergetable= TRUE;
		else
		if (strcmp(getFunctionId(getInstrPtr(mb,i)),"multiplex") == 0)
			multiplex= TRUE;
		else
		if (strcmp(getFunctionId(getInstrPtr(mb,i)),"garbageCollector") == 0 && i == mb->stop - 2)
			garbage= TRUE;

#ifdef WIN32
		else
		if (strcmp(getFunctionId(getInstrPtr(mb,i)),"octopus") == 0)
			throw(MAL,"optimizer.validate","'octopus' needs monetdbd\n");
		else
		if (strcmp(getFunctionId(getInstrPtr(mb,i)),"centipede") == 0)
			throw(MAL,"optimizer.validate","'octopus' needs monetdbd\n");
#endif
	} else
			throw(MAL,"optimizer.validate","Missing optimizer call\n");

	if (mitosis == TRUE && mergetable == FALSE) 
		throw(MAL,"optimizer.validate","'mitosis' needs 'mergetable'\n");

	if (multiplex == 0)
		throw(MAL,"optimizer.validate","'multiplex' should be used\n");
	if (deadcode == FALSE )
		throw(MAL,"optimizeri.validate","'deadcode' should be used at least once\n");
	if (garbage == FALSE )
		throw(MAL,"optimizer.validate","'garbageCollector' should be used as the last one\n");
	
	return MAL_SUCCEED;
}

static str
validateOptimizerPipes(void){
	int i;
	str msg = MAL_SUCCEED;

	mal_set_lock(mal_contextLock,"optimizer validation");
	for ( i = 0; i < MAXOPTPIPES && pipes[i].def; i++)
	if ( pipes[i].mb) {
		msg = validatePipe(pipes[i].mb);
		if ( msg != MAL_SUCCEED)
			break;
	}
	mal_unset_lock(mal_contextLock,"optimizer validation");
	return msg;
}

/*
 * Compile (the first time) an optimizer pipe string
 * then copy the statements to the end of the MAL plan
*/
str
compileOptimizer(Client cntxt, str name){
	int i, j;
	Symbol sym;
	str msg = MAL_SUCCEED;
	
	(void) cntxt;

	for( i=0; i < MAXOPTPIPES && pipes[i].name; i++)
	if ( strcmp(pipes[i].name, name) == 0 && pipes[i].mb == 0){
		/* precompile the pipeline as MAL string */
		Client c = MCinitClient((oid)1,0,0);
		assert(c != NULL);
		c->nspace = newModule(NULL, putName("user", 4));
		c->father = cntxt;	/* to avoid conflicts on GDKin */
		c->fdout = cntxt->fdout;
		if (setScenario(c,"mal")) 
			throw(MAL,"optimizer.addOptimizerPipe","failed to set scenario");
		(void)MCinitClientThread(c);
		for ( j =0; j < MAXOPTPIPES && pipes[j].def; j++)
		if (pipes[j].mb == NULL) {

			if (pipes[j].prerequisite && getAddress(c->fdout, NULL, optimizerRef, pipes[j].prerequisite, TRUE) == NULL)
				continue;
			MSinitClientPrg(c, "user", pipes[j].name);
			msg = compileString(&sym, c, pipes[j].def);
			if ( msg != MAL_SUCCEED){
				MCcloseClient(c); 
				return msg;
			}
			pipes[j].mb = copyMalBlk(sym->def);
		}
		MCcloseClient(c); 
		msg = validateOptimizerPipes();
		if ( msg != MAL_SUCCEED)
			return msg;
	}
	return MAL_SUCCEED;
}

str
addOptimizerPipe(Client cntxt, MalBlkPtr mb, str name){
	int i, j, k;
	InstrPtr p;
	
	(void) cntxt;

	for( i=0; i < MAXOPTPIPES && pipes[i].name; i++)
	if ( strcmp(pipes[i].name, name) == 0)
		break;

	if ( pipes[i].mb == NULL) 
		(void) compileOptimizer(cntxt,name);
	
	if ( pipes[i].mb) {
		for ( j =1; j < pipes[i].mb->stop-1; j++) {
			p =  copyInstruction(pipes[i].mb->stmt[j]);
			for ( k =0; k < p->argc; k++) 
				getArg(p,k) = cloneVariable(mb, pipes[i].mb, getArg(p,k));
			typeChecker(cntxt->fdout, cntxt->nspace, mb, p, FALSE);
			pushInstruction(mb,p);
		}
	}
	return MAL_SUCCEED;
}
