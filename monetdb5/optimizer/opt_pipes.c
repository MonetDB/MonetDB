/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
#include "mal_import.h"
#include "opt_support.h"
#include "mal_client.h"
#include "mal_instruction.h"
#include "mal_function.h"
#include "mal_listing.h"
#include "mal_linker.h"

#define MAXOPTPIPES 64

static struct pipeline {
	char *name;
	char **def;					/* NULL terminated list of optimizers */
	bool builtin;
} pipes[MAXOPTPIPES] = {
/* The minimal pipeline necessary by the server to operate correctly
 *
 * NOTE:
 * If you change the minimal pipe, please also update the man page
 * (see tools/mserver/mserver5.1) accordingly!
 */
	{"minimal_pipe",
	 (char *[]) {
		 "inline",
		 "remap",
		 "emptybind",
		 "deadcode",
		 "for",
		 "dict",
		 "multiplex",
		 "generator",
		 "profiler",
		 "garbageCollector",
		 NULL,
	 },
	 true,
	},
	{"minimal_fast",
	 (char *[]) {
		 "minimalfast",
		 NULL,
	 },
	 true,
	},
/* NOTE:
 * If you change the default pipe, please also update the no_mitosis
 * pipe and sequential pipe (see below, as well as the man page (see
 * tools/mserver/mserver5.1) accordingly!
 */
	{"default_pipe",
	 (char *[]) {
		 "inline",
		 "remap",
		 "costModel",
		 "coercions",
		 "aliases",
		 "evaluate",
		 "emptybind",
		 "deadcode",
		 "pushselect",
		 "aliases",
		 "for",
		 "dict",
		 "mitosis",
		 "mergetable",
		 "aliases",
		 "constants",
		 "commonTerms",
		 "projectionpath",
		 "deadcode",
		 "matpack",
		 "reorder",
		 "dataflow",
		 "querylog",
		 "multiplex",
		 "generator",
		 "candidates",
		 "deadcode",
		 "postfix",
		 "profiler",
		 "garbageCollector",
		 NULL,
	 },
	 true,
	},
	{"default_fast",
	 (char *[]) {
		 "defaultfast",
		 NULL,
	 },
	 true,
	},
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
	 (char *[]) {
		 "inline",
		 "remap",
		 "costModel",
		 "coercions",
		 "aliases",
		 "evaluate",
		 "emptybind",
		 "deadcode",
		 "pushselect",
		 "aliases",
		 "mergetable",
		 "aliases",
		 "constants",
		 "commonTerms",
		 "projectionpath",
		 "deadcode",
		 "matpack",
		 "reorder",
		 "dataflow",
		 "querylog",
		 "multiplex",
		 "generator",
		 "candidates",
		 "deadcode",
		 "postfix",
		 "profiler",
		 "garbageCollector",
		 NULL,
	 },
	 true,
	},
/* The sequential pipe line is (and should be kept!) identical to the
 * default pipeline, except that optimizers mitosis & dataflow are
 * omitted.  It is used mainly to make some tests work
 * deterministically, i.e., avoid ambiguous output, by avoiding
 * parallelism.
 *
 * NOTE:
 * If you change the sequential pipe, please also update the man page
 * (see tools/mserver/mserver5.1) accordingly!
 */
	{"sequential_pipe",
	 (char *[]) {
		 "inline",
		 "remap",
		 "costModel",
		 "coercions",
		 "aliases",
		 "evaluate",
		 "emptybind",
		 "deadcode",
		 "pushselect",
		 "aliases",
		 "for",
		 "dict",
		 "mergetable",
		 "aliases",
		 "constants",
		 "commonTerms",
		 "projectionpath",
		 "deadcode",
		 "matpack",
		 "reorder",
		 "querylog",
		 "multiplex",
		 "generator",
		 "candidates",
		 "deadcode",
		 "postfix",
		 "profiler",
		 "garbageCollector",
		 NULL,
	 },
	 true,
	},
/* Experimental pipelines stressing various components under
 * development.  Do not use any of these pipelines in production
 * settings!
 */
/* sentinel */
	{NULL, NULL, false,},
};

#include "optimizer_private.h"

static MT_Lock pipeLock = MT_LOCK_INITIALIZER(pipeLock);

static str
validatePipe(struct pipeline *pipe)
{
	bool mitosis = false, deadcode = false, mergetable = false;
	bool multiplex = false, garbage = false, generator = false, remap = false;
	int i;

	if (pipe->def == NULL || pipe->def[0] == NULL)
		throw(MAL, "optimizer.validate", SQLSTATE(42000) "missing optimizers");

	if (strcmp(pipe->def[0], "defaultfast") == 0
		|| strcmp(pipe->def[0], "minimalfast") == 0)
		return MAL_SUCCEED;

	if (strcmp(pipe->def[0], "inline") != 0)
		throw(MAL, "optimizer.validate",
			  SQLSTATE(42000) "'inline' should be the first\n");

	for (i = 0; pipe->def[i]; i++) {
		const char *fname = pipe->def[i];
		if (garbage)
			throw(MAL, "optimizer.validate",
				  SQLSTATE(42000)
				  "'garbageCollector' should be used as the last one\n");
		if (strcmp(fname, "deadcode") == 0)
			deadcode = true;
		else if (strcmp(fname, "remap") == 0)
			remap = true;
		else if (strcmp(fname, "mitosis") == 0)
			mitosis = true;
		else if (strcmp(fname, "mergetable") == 0)
			mergetable = true;
		else if (strcmp(fname, "multiplex") == 0)
			multiplex = true;
		else if (strcmp(fname, "generator") == 0)
			generator = true;
		else if (strcmp(fname, "garbageCollector") == 0)
			garbage = true;
	}

	if (mitosis && !mergetable)
		throw(MAL, "optimizer.validate",
			  SQLSTATE(42000) "'mitosis' needs 'mergetable'\n");

	/* several optimizer should be used */
	if (!multiplex)
		throw(MAL, "optimizer.validate",
			  SQLSTATE(42000) "'multiplex' should be used\n");
	if (!deadcode)
		throw(MAL, "optimizer.validate",
			  SQLSTATE(42000) "'deadcode' should be used at least once\n");
	if (!garbage)
		throw(MAL, "optimizer.validate",
			  SQLSTATE(42000)
			  "'garbageCollector' should be used as the last one\n");
	if (!remap)
		throw(MAL, "optimizer.validate",
			  SQLSTATE(42000) "'remap' should be used\n");
	if (!generator)
		throw(MAL, "optimizer.validate",
			  SQLSTATE(42000) "'generator' should be used\n");

	return MAL_SUCCEED;
}

/* the session_pipe is the one defined by the user */
str
addPipeDefinition(Client cntxt, const char *name, const char *pipe)
{
	int i, n;
	str msg = MAL_SUCCEED;
	struct pipeline oldpipe;
	const char *p;

	(void) cntxt;
	MT_lock_set(&pipeLock);
	for (i = 0; i < MAXOPTPIPES && pipes[i].name; i++)
		if (strcmp(name, pipes[i].name) == 0)
			break;

	if (i == MAXOPTPIPES) {
		MT_lock_unset(&pipeLock);
		throw(MAL, "optimizer.addPipeDefinition",
			  SQLSTATE(HY013) "Out of slots");
	}
	if (pipes[i].name && pipes[i].builtin) {
		MT_lock_unset(&pipeLock);
		throw(MAL, "optimizer.addPipeDefinition",
			  SQLSTATE(42000) "No overwrite of built in allowed");
	}

	/* save old value */
	oldpipe = pipes[i];
	pipes[i] = (struct pipeline) {
		.name = GDKstrdup(name),
	};
	if (pipes[i].name == NULL)
		goto bailout;
	n = 1;
	for (p = pipe; p; p = strchr(p, ';')) {
		p++;
		n++;
	}
	if ((pipes[i].def = GDKmalloc(n * sizeof(char *))) == NULL)
		goto bailout;
	n = 0;
	while ((p = strchr(pipe, ';')) != NULL) {
		if (strncmp(pipe, "optimizer.", 10) == 0)
			pipe += 10;
		const char *q = pipe;
		while (q < p && *q != '(' && *q != '.' && !GDKisspace(*q))
			q++;
		if (*q == '.') {
			msg = createException(MAL, "optimizer.addPipeDefinition",
								  SQLSTATE(42000) "Bad pipeline definition");
			goto bailout;
		}
		if (q > pipe) {
			if ((pipes[i].def[n++] = GDKstrndup(pipe, q - pipe)) == NULL)
				goto bailout;
		}
		pipe = p + 1;
		while (*pipe && GDKisspace(*pipe))
			pipe++;
	}
	pipes[i].def[n] = NULL;
	msg = validatePipe(&pipes[i]);
	if (msg != MAL_SUCCEED) {
		/* failed: restore old value */
		goto bailout;
	}
	MT_lock_unset(&pipeLock);
	/* succeeded: destroy old value */
	GDKfree(oldpipe.name);
	if (oldpipe.def)
		for (n = 0; oldpipe.def[n]; n++)
			GDKfree(oldpipe.def[n]);
	GDKfree(oldpipe.def);
	return msg;

  bailout:
	GDKfree(pipes[i].name);
	if (pipes[i].def)
		for (n = 0; pipes[i].def[n]; n++)
			GDKfree(pipes[i].def[n]);
	GDKfree(pipes[i].def);
	pipes[i] = oldpipe;
	MT_lock_unset(&pipeLock);
	if (msg)
		return msg;
	throw(MAL, "optimizer.addPipeDefinition", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

bool
isOptimizerPipe(const char *name)
{
	int i;

	for (i = 0; i < MAXOPTPIPES && pipes[i].name; i++)
		if (strcmp(name, pipes[i].name) == 0)
			return true;
	return false;
}

str
getPipeCatalog(bat *nme, bat *def, bat *stat)
{
	BAT *b, *bn, *bs;
	int i;
	size_t l = 2048;
	char *buf = GDKmalloc(l);

	b = COLnew(0, TYPE_str, 20, TRANSIENT);
	bn = COLnew(0, TYPE_str, 20, TRANSIENT);
	bs = COLnew(0, TYPE_str, 20, TRANSIENT);
	if (buf == NULL || b == NULL || bn == NULL || bs == NULL) {
		BBPreclaim(b);
		BBPreclaim(bn);
		BBPreclaim(bs);
		GDKfree(buf);
		throw(MAL, "optimizer.getpipeDefinition",
			  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for (i = 0; i < MAXOPTPIPES && pipes[i].name; i++) {
		size_t n = 1;
		for (int j = 0; pipes[i].def[j]; j++)
			n += strlen(pipes[i].def[j]) + 13;
		if (n > l) {
			GDKfree(buf);
			buf = GDKmalloc(n);
			l = n;
			if (buf == NULL) {
				BBPreclaim(b);
				BBPreclaim(bn);
				BBPreclaim(bs);
				GDKfree(buf);
				throw(MAL, "optimizer.getpipeDefinition",
					  SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
		char *p = buf;
		for (int j = 0; pipes[i].def[j]; j++) {
			p = stpcpy(p, "optimizer.");
			p = stpcpy(p, pipes[i].def[j]);
			p = stpcpy(p, "();");
		}
		if (BUNappend(b, pipes[i].name, false) != GDK_SUCCEED
			|| BUNappend(bn, buf, false) != GDK_SUCCEED
			|| BUNappend(bs, pipes[i].builtin ? "stable" : "experimental",
						 false) != GDK_SUCCEED) {
			BBPreclaim(b);
			BBPreclaim(bn);
			BBPreclaim(bs);
			GDKfree(buf);
			throw(MAL, "optimizer.getpipeDefinition",
				  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	GDKfree(buf);

	*nme = b->batCacheid;
	BBPkeepref(b);
	*def = bn->batCacheid;
	BBPkeepref(bn);
	*stat = bs->batCacheid;
	BBPkeepref(bs);
	return MAL_SUCCEED;
}

/*
 * Add a new components of the optimizer pipe to the plan
 */
str
addOptimizerPipe(Client cntxt, MalBlkPtr mb, const char *name)
{
	int i, j;
	InstrPtr p;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	if (strcmp(name, "default_fast") == 0 && isSimpleSQL(mb)) {
		for (i = 0; i < MAXOPTPIPES && pipes[i].name; i++)
			if (strcmp(pipes[i].name, "minimal_fast") == 0)
				break;
	} else {
		for (i = 0; i < MAXOPTPIPES && pipes[i].name; i++)
			if (strcmp(pipes[i].name, name) == 0)
				break;
	}

	if (i == MAXOPTPIPES || pipes[i].name == NULL)
		throw(MAL, "optimizer.addOptimizerPipe",
			  SQLSTATE(22023) "Unknown optimizer");

	for (j = 0; pipes[i].def[j]; j++) {
		p = newFcnCall(mb, optimizerRef, pipes[i].def[j]);
		if (p == NULL)
			throw(MAL, "optimizer.addOptimizerPipe",
				  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		p->fcn = (MALfcn) OPTwrapper;
		p->token = PATcall;
		pushInstruction(mb, p);
	}
	return msg;
}

void
opt_pipes_reset(void)
{
	for (int i = 0; i < MAXOPTPIPES; i++)
		if (pipes[i].name && !pipes[i].builtin) {
			GDKfree(pipes[i].name);
			if (pipes[i].def)
				for (int n = 0; pipes[i].def[n]; n++)
					GDKfree(pipes[i].def[n]);
			GDKfree(pipes[i].def);
			pipes[i] = (struct pipeline) {
				.name = NULL,
			};
		}
}
