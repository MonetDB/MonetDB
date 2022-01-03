/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

/*
 * author M.L. Kersten
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
#include "opt_support.h"
#include "mal_client.h"
#include "mal_instruction.h"
#include "mal_function.h"
#include "mal_listing.h"
#include "mal_linker.h"

#define MAXOPTPIPES 64

static struct PIPELINES {
	char *name;
	char *def;
	char *status;
	MalBlkPtr mb;
	char builtin;
} pipes[MAXOPTPIPES] = {
/* The minimal pipeline necessary by the server to operate correctly
 *
 * NOTE:
 * If you change the minimal pipe, please also update the man page
 * (see tools/mserver/mserver5.1) accordingly!
 */
	{"minimal_pipe",
	 "optimizer.inline();"
	 "optimizer.remap();"
	 "optimizer.bincopyfrom();"
	 "optimizer.emptybind();"
	 "optimizer.deadcode();"
	 "optimizer.for();"
	 "optimizer.dict();"
	 "optimizer.multiplex();"
	 "optimizer.generator();"
	 //"optimizer.candidates();" only for decoration in explain
	 //"optimizer.mask();"
	 "optimizer.garbageCollector();"
	 "optimizer.profiler();",
	 "stable", NULL, 1},
#ifdef USE_STRIMPS_OPTIMIZERS
	{"minimal_strimps_pipe",
	 "optimizer.inline();"
	 "optimizer.remap();"
	 "optimizer.bincopyfrom();"
	 "optimizer.aliases();"
	 "optimizer.constants();"
	 "optimizer.deadcode();"
	 "optimizer.multiplex();"
	 "optimizer.strimps();"
	 "optimizer.generator();"
	 //"optimizer.candidates();" only for decoration in explain
	 //"optimizer.mask();"
	 "optimizer.garbageCollector();"
	 "optimizer.profiler();",
	 "stable", NULL, 1},
#endif  // USE_STRIMPS_OPTIMIZERS
	{"minimal_fast",
	 "optimizer.minimalfast()",
	 "stable", NULL, 1},
/* The default pipe line contains as of Feb2010
 * mitosis-mergetable-reorder, aimed at large tables and improved
 * access locality.
 *
 * NOTE:
 * If you change the default pipe, please also update the no_mitosis pipe
 * and sequential pipe (see below, as well as the man page (see
 * tools/mserver/mserver5.1) accordingly!
 */
	{"default_pipe",
	 "optimizer.inline();"
	 "optimizer.remap();"
	 "optimizer.costModel();"
	 "optimizer.coercions();"
	 "optimizer.aliases();"
	 "optimizer.evaluate();"
	 "optimizer.emptybind();"
	 "optimizer.deadcode();" /* Feb2021 update, I pushed deadcode optimizer earlier in the pipeline so it runs before mitosis, thus removing less instructions */
	 "optimizer.pushselect();"
	 "optimizer.aliases();"
	 "optimizer.for();"
	 "optimizer.dict();"
	 "optimizer.mitosis();"
	 "optimizer.mergetable();"
	 "optimizer.bincopyfrom();"
	 "optimizer.aliases();"
	 "optimizer.constants();"
	 "optimizer.commonTerms();"
	 "optimizer.projectionpath();"
	 "optimizer.deadcode();"
	 "optimizer.matpack();"
	 "optimizer.reorder();"
	 "optimizer.dataflow();"
	 "optimizer.querylog();"
	 "optimizer.multiplex();"
	 "optimizer.generator();"
	 "optimizer.candidates();"
	 //"optimizer.mask();"
	 "optimizer.deadcode();"
	 "optimizer.postfix();"
//	 "optimizer.jit();" awaiting the new batcalc api
	 "optimizer.wlc();"
	 "optimizer.garbageCollector();"
	 "optimizer.profiler();",
	 "stable", NULL, 1},
#ifdef USE_STRIMPS_OPTIMIZERS
	{"strimps_pipe",
	 "optimizer.inline();"
	 "optimizer.remap();"
	 "optimizer.costModel();"
	 "optimizer.coercions();"
	 "optimizer.aliases();"
	 "optimizer.evaluate();"
	 "optimizer.emptybind();"
	 "optimizer.deadcode();" /* Feb2021 update, I pushed deadcode optimizer earlier in the pipeline so it runs before mitosis, thus removing less instructions */
	 "optimizer.pushselect();"
	 "optimizer.aliases();"
	 "optimizer.mitosis();"
	 "optimizer.mergetable();"
	 "optimizer.bincopyfrom();"
	 "optimizer.aliases();"
	 "optimizer.constants();"
	 "optimizer.commonTerms();"
	 "optimizer.projectionpath();"
	 "optimizer.deadcode();"
	 "optimizer.matpack();"
	 "optimizer.reorder();"
	 "optimizer.dataflow();"
	 "optimizer.querylog();"
	 "optimizer.multiplex();"
	 "optimizer.strimps();"
	 "optimizer.generator();"
	 "optimizer.candidates();"
	 //"optimizer.mask();"
	 "optimizer.deadcode();"
	 "optimizer.postfix();"
//	 "optimizer.jit();" awaiting the new batcalc api
	 "optimizer.wlc();"
	 "optimizer.garbageCollector();"
	 "optimizer.profiler();",
	 "stable", NULL, 1},
#endif  // USE_STRIMPS_OPTIMIZERS
	{"default_fast",
	 "optimizer.defaultfast()",
	 "stable", NULL, 1},
/*
 * Volcano style execution produces a sequence of blocks from the source relation
 */
	{"volcano_pipe",
	 "optimizer.inline();"
	 "optimizer.remap();"
	 "optimizer.costModel();"
	 "optimizer.coercions();"
	 "optimizer.aliases();"
	 "optimizer.evaluate();"
	 "optimizer.emptybind();"
	 "optimizer.deadcode();" /* Feb2021 update, I pushed deadcode optimizer earlier in the pipeline so it runs before mitosis, thus removing less instructions */
	 "optimizer.pushselect();"
	 "optimizer.aliases();"
	 "optimizer.mitosis();"
	 "optimizer.mergetable();"
	 "optimizer.bincopyfrom();"
	 "optimizer.aliases();"
	 "optimizer.constants();"
	 "optimizer.commonTerms();"
	 "optimizer.projectionpath();"
	 "optimizer.deadcode();"
	 "optimizer.matpack();"
	 "optimizer.reorder();"
	 "optimizer.dataflow();"
	 "optimizer.querylog();"
	 "optimizer.multiplex();"
	 "optimizer.generator();"
	 "optimizer.volcano();"
	 "optimizer.candidates();"
	 //"optimizer.mask();"
	 "optimizer.deadcode();"
	 "optimizer.postfix();"
//	 "optimizer.jit();" awaiting the new batcalc api
	 "optimizer.wlc();"
	 "optimizer.garbageCollector();"
	 "optimizer.profiler();",
	 "stable", NULL, 1},
/* The no_mitosis pipe line is (and should be kept!) identical to the
 * default pipeline, except that optimizer mitosis is omitted.  It is
 * used mainly to make some tests work deterministically, and to check
 * / debug whether "unexpected" problems are related to mitosis
 * (and/or mergetable).
 *
 * NOTE:
 * If you change the no_mitosis pipe, please also update the man page
 * (see tools/mserver/mserver5.1) accordingly!
 */
	{"no_mitosis_pipe",
	 "optimizer.inline();"
	 "optimizer.remap();"
	 "optimizer.costModel();"
	 "optimizer.coercions();"
	 "optimizer.aliases();"
	 "optimizer.evaluate();"
	 "optimizer.emptybind();"
	 "optimizer.deadcode();" /* Feb2021 update, I pushed deadcode optimizer earlier in the pipeline so it runs before mitosis, thus removing less instructions */
	 "optimizer.pushselect();"
	 "optimizer.aliases();"
	 "optimizer.mergetable();"
	 "optimizer.bincopyfrom();"
	 "optimizer.aliases();"
	 "optimizer.constants();"
	 "optimizer.commonTerms();"
	 "optimizer.projectionpath();"
	 "optimizer.deadcode();"
	 "optimizer.matpack();"
	 "optimizer.reorder();"
	 "optimizer.dataflow();"
	 "optimizer.querylog();"
	 "optimizer.multiplex();"
	 "optimizer.generator();"
	 "optimizer.candidates();"
	 //"optimizer.mask();"
	 "optimizer.deadcode();"
	 "optimizer.postfix();"
//	 "optimizer.jit();" awaiting the new batcalc api
	 "optimizer.wlc();"
	 "optimizer.garbageCollector();"
	 "optimizer.profiler();",
	 "stable", NULL, 1},
/* The sequential pipe line is (and should be kept!) identical to the
 * default pipeline, except that optimizers mitosis & dataflow are
 * omitted.  It is use mainly to make some tests work
 * deterministically, i.e., avoid ambigious output, by avoiding
 * parallelism.
 *
 * NOTE:
 * If you change the sequential pipe, please also update the man page
 * (see tools/mserver/mserver5.1) accordingly!
 */
	{"sequential_pipe",
	 "optimizer.inline();"
	 "optimizer.remap();"
	 "optimizer.costModel();"
	 "optimizer.coercions();"
	 "optimizer.aliases();"
	 "optimizer.evaluate();"
	 "optimizer.emptybind();"
	 "optimizer.deadcode();" /* Feb2021 update, I pushed deadcode optimizer earlier in the pipeline so it runs before mitosis, thus removing less instructions */
	 "optimizer.pushselect();"
	 "optimizer.aliases();"
	 "optimizer.for();"
	 "optimizer.dict();"
	 "optimizer.mergetable();"
	 "optimizer.bincopyfrom();"
	 "optimizer.aliases();"
	 "optimizer.constants();"
	 "optimizer.commonTerms();"
	 "optimizer.projectionpath();"
	 "optimizer.deadcode();"
	 "optimizer.matpack();"
	 "optimizer.reorder();"
	 "optimizer.querylog();"
	 "optimizer.multiplex();"
	 "optimizer.generator();"
	 "optimizer.candidates();"
	 //"optimizer.mask();"
	 "optimizer.deadcode();"
	 "optimizer.postfix();"
//	 "optimizer.jit();" awaiting the new batcalc api
	 "optimizer.wlc();"
	 "optimizer.garbageCollector();"
	 "optimizer.profiler();",
	 "stable", NULL, 1},
/* Experimental pipelines stressing various components under
 * development.  Do not use any of these pipelines in production
 * settings!
 */
/* sentinel */
	{NULL, NULL, NULL, NULL, 0}
};

/*
 * Debugging the optimizer pipeline",
 * The best way is to use mdb and inspect the information gathered",
 * during the optimization phase.  Several optimizers produce more",
 * intermediate information, which may shed light on the details.  The",
 * opt_debug bitvector controls their output. It can be set to a",
 * pipeline or a comma separated list of optimizers you would like to",
 * trace. It is a server wide property and can not be set dynamically,",
 * as it is intended for internal use.",
 */
#include "optimizer_private.h"

static MT_Lock pipeLock = MT_LOCK_INITIALIZER(pipeLock);

/* the session_pipe is the one defined by the user */
str
addPipeDefinition(Client cntxt, const char *name, const char *pipe)
{
	int i;
	str msg;
	struct PIPELINES oldpipe;

	MT_lock_set(&pipeLock);
	for (i = 0; i < MAXOPTPIPES && pipes[i].name; i++)
		if (strcmp(name, pipes[i].name) == 0)
			break;

	if (i == MAXOPTPIPES) {
		MT_lock_unset(&pipeLock);
		throw(MAL, "optimizer.addPipeDefinition", SQLSTATE(HY013) "Out of slots");
	}
	if (pipes[i].name && pipes[i].builtin) {
		MT_lock_unset(&pipeLock);
		throw(MAL, "optimizer.addPipeDefinition", SQLSTATE(42000) "No overwrite of built in allowed");
	}

	/* save old value */
	oldpipe = pipes[i];
	pipes[i].name = GDKstrdup(name);
	pipes[i].def = GDKstrdup(pipe);
	pipes[i].status = GDKstrdup("experimental");
	if(pipes[i].name == NULL || pipes[i].def == NULL || pipes[i].status == NULL) {
		GDKfree(pipes[i].name);
		GDKfree(pipes[i].def);
		GDKfree(pipes[i].status);
		pipes[i].name = oldpipe.name;
		pipes[i].def = oldpipe.def;
		pipes[i].status = oldpipe.status;
		MT_lock_unset(&pipeLock);
		throw(MAL, "optimizer.addPipeDefinition", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	pipes[i].mb = NULL;
	MT_lock_unset(&pipeLock);
	msg = compileOptimizer(cntxt, name);
	if (msg) {
		/* failed: restore old value */
		MT_lock_set(&pipeLock);
		GDKfree(pipes[i].name);
		GDKfree(pipes[i].def);
		GDKfree(pipes[i].status);
		pipes[i] = oldpipe;
		MT_lock_unset(&pipeLock);
	} else {
		/* succeeded: destroy old value */
		if (oldpipe.name)
			GDKfree(oldpipe.name);
		if (oldpipe.def)
			GDKfree(oldpipe.def);
		if (oldpipe.mb)
			freeMalBlk(oldpipe.mb);
		if (oldpipe.status)
			GDKfree(oldpipe.status);
	}
	return msg;
}

int
isOptimizerPipe(const char *name)
{
	int i;

	for (i = 0; i < MAXOPTPIPES && pipes[i].name; i++)
		if (strcmp(name, pipes[i].name) == 0)
			return TRUE;
	return FALSE;
}

str
getPipeDefinition(str name)
{
	int i;

	for (i = 0; i < MAXOPTPIPES && pipes[i].name; i++)
		if (strcmp(name, pipes[i].name) == 0)
			return GDKstrdup(pipes[i].def);
	return NULL;
}

str
getPipeCatalog(bat *nme, bat *def, bat *stat)
{
	BAT *b, *bn, *bs;
	int i;

	b = COLnew(0, TYPE_str, 20, TRANSIENT);
	bn = COLnew(0, TYPE_str, 20, TRANSIENT);
	bs = COLnew(0, TYPE_str, 20, TRANSIENT);
	if (b == NULL || bn == NULL || bs == NULL) {
		BBPreclaim(b);
		BBPreclaim(bn);
		BBPreclaim(bs);
		throw(MAL, "optimizer.getpipeDefinition", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for (i = 0; i < MAXOPTPIPES && pipes[i].name; i++) {
		if (BUNappend(b, pipes[i].name, false) != GDK_SUCCEED ||
			BUNappend(bn, pipes[i].def, false) != GDK_SUCCEED ||
			BUNappend(bs, pipes[i].status, false) != GDK_SUCCEED) {
			BBPreclaim(b);
			BBPreclaim(bn);
			BBPreclaim(bs);
			throw(MAL, "optimizer.getpipeDefinition", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}

	BBPkeepref(*nme = b->batCacheid);
	BBPkeepref(*def = bn->batCacheid);
	BBPkeepref(*stat = bs->batCacheid);
	return MAL_SUCCEED;
}

static str
validatePipe(MalBlkPtr mb)
{
	int mitosis = FALSE, deadcode = FALSE, mergetable = FALSE, multiplex = FALSE;
	int bincopyfrom = FALSE, garbage = FALSE, generator = FALSE, remap =  FALSE;
	int i;
	InstrPtr p;

	if (mb == NULL )
		throw(MAL, "optimizer.validate", SQLSTATE(42000) "missing optimizer mal block\n");
	p = getInstrPtr(mb,1);

	if (getFunctionId(p) && (idcmp(getFunctionId(p), "defaultfast") == 0 || (idcmp(getFunctionId(p), "minimalfast") == 0)))
		return MAL_SUCCEED;

	if (getFunctionId(p) == NULL || idcmp(getFunctionId(p), "inline") != 0)
		throw(MAL, "optimizer.validate", SQLSTATE(42000) "'inline' should be the first\n");

	for (i = 1; i < mb->stop - 1; i++){
		p = getInstrPtr(mb, i);
		const char *fname = getFunctionId(p);
		if (fname != NULL) {
			if (strcmp(fname, "deadcode") == 0)
				deadcode = TRUE;
			else if (strcmp(fname, "remap") == 0)
				remap = TRUE;
			else if (strcmp(fname, "mitosis") == 0)
				mitosis = TRUE;
			else if (strcmp(fname, "bincopyfrom") == 0)
				bincopyfrom = TRUE;
			else if (strcmp(fname, "mergetable") == 0)
				mergetable = TRUE;
			else if (strcmp(fname, "multiplex") == 0)
				multiplex = TRUE;
			else if (strcmp(fname, "generator") == 0)
				generator = TRUE;
			else if (strcmp(fname, "garbageCollector") == 0)
				garbage = TRUE;
		} else
			throw(MAL, "optimizer.validate", SQLSTATE(42000) "Missing optimizer call\n");
	}

	if (mitosis == TRUE && mergetable == FALSE)
		throw(MAL, "optimizer.validate", SQLSTATE(42000) "'mitosis' needs 'mergetable'\n");

	/* several optimizer should be used */
	if (multiplex == 0)
		throw(MAL, "optimizer.validate", SQLSTATE(42000) "'multiplex' should be used\n");
	if (deadcode == FALSE)
		throw(MAL, "optimizer.validate", SQLSTATE(42000) "'deadcode' should be used at least once\n");
	if (garbage == FALSE)
		throw(MAL, "optimizer.validate", SQLSTATE(42000) "'garbageCollector' should be used as the last one\n");
	if (remap == FALSE)
		throw(MAL, "optimizer.validate", SQLSTATE(42000) "'remap' should be used\n");
	if (generator == FALSE)
		throw(MAL, "optimizer.validate", SQLSTATE(42000) "'generator' should be used\n");
	if (bincopyfrom == FALSE)
		throw(MAL, "optimizer.validate", SQLSTATE(42000) "'bincopyfrom' should be used\n");

	return MAL_SUCCEED;
}

static str
validateOptimizerPipes(void)
{
	int i;
	str msg = MAL_SUCCEED;

	for (i = 0; i < MAXOPTPIPES && pipes[i].def; i++)
		if (pipes[i].mb) {
			msg = validatePipe(pipes[i].mb);
			if (msg != MAL_SUCCEED)
				break;
		}
	return msg;
}

/*
 * Compile (the first time) an optimizer pipe string
 * then copy the statements to the end of the MAL plan
*/
str
compileOptimizer(Client cntxt, const char *name)
{
	int i, j;
	char buf[2048];
	str msg = MAL_SUCCEED;
	Symbol fcn, compiled;

	MT_lock_set(&pipeLock);
	for (i = 0; i < MAXOPTPIPES && pipes[i].name; i++)
		if (strcmp(pipes[i].name, name) == 0 && pipes[i].mb == 0) {
			/* precompile a pipeline as MAL string */
			for (j = 0; j < MAXOPTPIPES && pipes[j].def; j++) {
				if (pipes[j].mb == NULL) {
					snprintf(buf,2048,"function optimizer.%s(); %s;end %s;", pipes[j].name,pipes[j].def,pipes[j].name);
					msg = compileString(&fcn,cntxt, buf);
					if( msg == MAL_SUCCEED){
						compiled = findSymbol(cntxt->usermodule,getName("optimizer"), getName(pipes[j].name));
						if( compiled)
							pipes[j].mb = compiled->def;
					} else break;
				}
			}
			if (msg != MAL_SUCCEED ||
				(msg = validateOptimizerPipes()) != MAL_SUCCEED)
				break;
		}
	MT_lock_unset(&pipeLock);
	return msg;
}

str
compileAllOptimizers(Client cntxt)
{
	int i;
	str msg = MAL_SUCCEED;

	for(i=0;pipes[i].def && msg == MAL_SUCCEED; i++){
		msg =compileOptimizer(cntxt,pipes[i].name);
	}
	return msg;
}

/*
 * Add a new components of the optimizer pipe to the plan
 */
str
addOptimizerPipe(Client cntxt, MalBlkPtr mb, const char *name)
{
	int i, j, k;
	InstrPtr p,q;
	str msg = MAL_SUCCEED;

	if (strcmp(name, "default_fast") == 0 && isSimpleSQL(mb)){
		for (i = 0; i < MAXOPTPIPES && pipes[i].name; i++)
			if (strcmp(pipes[i].name, "minimal_fast") == 0)
				break;
	} else {
		for (i = 0; i < MAXOPTPIPES && pipes[i].name; i++)
			if (strcmp(pipes[i].name, name) == 0)
				break;
	}

	if (i == MAXOPTPIPES)
		throw(MAL, "optimizer.addOptimizerPipe", SQLSTATE(HY013) "Out of slots");

	if (pipes[i].mb == NULL){
		if (strcmp(name, "default_fast") == 0  && isSimpleSQL(mb))
			msg = compileOptimizer(cntxt, name);
		else
			msg = compileOptimizer(cntxt, "minimal_fast");
	}

	if (pipes[i].mb && pipes[i].mb->stop) {
		for (j = 1; j < pipes[i].mb->stop - 1; j++) {
			q= getInstrPtr(pipes[i].mb,j);
			if( getModuleId(q) != optimizerRef)
				continue;
			p = copyInstruction(q);
			if (!p)
				throw(MAL, "optimizer.addOptimizerPipe", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			for (k = 0; k < p->argc; k++)
				getArg(p, k) = cloneVariable(mb, pipes[i].mb, getArg(p, k));
			// typecheck to get its internal address typeChecker(cntxt->usermodule, mb, p, j, FALSE);
			pushInstruction(mb, p);
		}
	}
	return msg;
}

void
opt_pipes_reset(void)
{
	int i;

	for (i = 0; i < MAXOPTPIPES; i++)
		pipes[i].mb = NULL;
}
