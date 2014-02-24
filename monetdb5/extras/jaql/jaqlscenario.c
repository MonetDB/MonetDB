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

#include "monetdb_config.h"
#include "jaqlscenario.h"
#include "jaql.h"
#include "jaqlgencode.h"
#include "msabaoth.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_exception.h"
#include "mal_scenario.h"
#include "mal_instruction.h"
#include "mal_debugger.h"
#include "optimizer.h"
#include "opt_pipes.h"

extern int jaqlparse(jc *j);
extern int jaqllex_init_extra(jc *user_defined, void **scanner);
extern int jaqllex_destroy(void *yyscanner);

str
JAQLprelude(void)
{
	Scenario s = getFreeScenario();

	if (!s)
		throw(MAL, "jaql.start", "out of scenario slots");

	s->name = "jaql";
	s->language = "jaql";
	s->initSystem = NULL;
	s->exitSystem = "JAQLexit";
	s->initClient = "JAQLinitClient";
	s->exitClient = "JAQLexitClient";
	s->reader = "JAQLreader";
	s->parser = "JAQLparser";
	s->engine = "JAQLengine";

	fprintf(stdout, "# MonetDB/JAQL module loaded\n");
	fflush(stdout); /* make merovingian see this *now* */

	return msab_marchScenario(s->name);
}

str
JAQLepilogue(void)
{
	/* this function is never called, but for the idea of it, we clean
	 * up our own mess */
	return msab_retreatScenario("jaql");
}

str
JAQLexit(Client c)
{
	(void) c;		/* not used */
	return MAL_SUCCEED;
}

str
JAQLinitClient(Client c)
{
	jc *j = NULL;
	str msg = MAL_SUCCEED;

	j = GDKzalloc(sizeof(jc));
	jaqllex_init_extra(j, &j->scanner);

	optimizerInit();  /* for all xxxRef vars in dumpcode */

	/* Set state, this indicates an initialized client scenario */
	c->state[MAL_SCENARIO_READER] = c;
	c->state[MAL_SCENARIO_PARSER] = c;
	c->state[MAL_SCENARIO_OPTIMIZE] = c;
	c->jaqlcontext = j;

	return msg;
}

str
JAQLexitClient(Client c)
{
	if (c->jaqlcontext != NULL) {
		jc *j = (jc *) c->jaqlcontext;

		jaqllex_destroy(j->scanner);
		j->scanner = NULL;
		freevars(j->vars);
		GDKfree(j);

		c->state[MAL_SCENARIO_READER] = NULL;
		c->state[MAL_SCENARIO_PARSER] = NULL;
		c->state[MAL_SCENARIO_OPTIMIZE] = NULL;
		c->jaqlcontext = NULL;
	}

	return MAL_SUCCEED;
}


static void
freeVariables(Client c, MalBlkPtr mb, MalStkPtr glb, int start)
{
	int i, j;

	for (i = start; i < mb->vtop;) {
		if (glb) {
			if (isVarCleanup(mb,i))
				garbageElement(c,&glb->stk[i]);
			/* clean stack entry */
			glb->stk[i].vtype = TYPE_int;
			glb->stk[i].val.ival = 0;
			glb->stk[i].len = 0;
		}
		clearVariable(mb, i);
		i++;
	}
	mb->vtop = start;
	for (i = j = 0; i < mb->ptop; i++) {
		if (mb->prps[i].var < start) {
			if (i > j)
				mb->prps[j] = mb->prps[i];
			j++;
		}
	}
	mb->ptop = j;
}


str
JAQLreader(Client c)
{
	/* dummy stub, the scanner reads for us
	 * TODO: pre-fill the buf if we have single line mode */

	if (c->fdin == NULL)
		throw(MAL, "jaql.reader", RUNTIME_IO_EOF);

	/* "activate" the stream by sending a prompt (client sync) */
	if (c->fdin->eof != 0) {
		if (mnstr_flush(c->fdout) < 0) {
			c->mode = FINISHCLIENT;
		} else {
			c->fdin->eof = 0;
		}
	}

	return MAL_SUCCEED;
}

str
JAQLparser(Client c)
{
	bstream *in = c->fdin;
	stream *out = c->fdout;
	jc *j;
	int oldvtop, oldstop;
	str errmsg;

	if ((errmsg = getJAQLContext(c, &j)) != MAL_SUCCEED) {
		/* tell the client */
		mnstr_printf(out, "!%s, aborting\n", errmsg);
		/* leave a message in the log */
		fprintf(stderr, "%s, cannot handle client!\n", errmsg);
		/* stop here, instead of printing the exception below to the
		 * client in an endless loop */
		c->mode = FINISHCLIENT;
		return errmsg;
	}

	c->curprg->def->errors = 0;
	oldvtop = c->curprg->def->vtop;
	oldstop = c->curprg->def->stop;
	j->vtop = oldvtop;
	j->explain = j->plan = j->planf = j->debug = j->trace = j->mapimode = 0;
	j->buf = NULL;
	j->scanstreamin = in;
	j->scanstreamout = out;
	j->scanstreameof = 0;
	j->pos = 0;
	j->p = NULL;
	j->time = 0;
	j->timing.parse = j->timing.optimise = j->timing.gencode = 0L;

	j->timing.parse = GDKusec();
	jaqlparse(j);
	j->timing.parse = GDKusec() - j->timing.parse;

	/* stop if it seems nothing is going to come any more */
	if (j->scanstreameof == 1) {
		c->mode = FINISHCLIENT;
		freetree(j->p);
		j->p = NULL;
		return MAL_SUCCEED;
	}

	/* parsing is done */
	in->pos = j->pos;
	c->yycur = 0;

	if (j->err[0] != '\0') {
		/* tell the client */
		mnstr_printf(out, "!%s\n", j->err);
		j->err[0] = '\0';
		return MAL_SUCCEED;
	}

	if (j->p == NULL)  /* there was nothing to parse, EOF */
		return MAL_SUCCEED;

	if (!j->plan && !j->planf) {
		Symbol prg = c->curprg;
		j->mapimode = 1;  /* request dumping in MAPI mode */
		(void)dumptree(j, c, prg->def, j->p);
		j->mapimode = 0;
		pushEndInstruction(prg->def);
		/* codegen could report an error */
		if (j->err[0] != '\0') {
			MSresetInstructions(prg->def, oldstop);
			freeVariables(c, prg->def, c->glb, oldvtop);
			prg->def->errors = 0;
			mnstr_printf(out, "!%s\n", j->err);
			freetree(j->p);
			throw(PARSE, "JAQLparse", "%s", j->err);
		}

		j->timing.optimise = GDKusec();
		chkTypes(out, c->nspace, prg->def, FALSE);
		/* TODO: use a configured pipe */
		addOptimizerPipe(c, prg->def, "minimal_pipe");
		if ((errmsg = optimizeMALBlock(c, prg->def)) != MAL_SUCCEED) {
			MSresetInstructions(prg->def, oldstop);
			freeVariables(c, prg->def, c->glb, oldvtop);
			prg->def->errors = 0;
			mnstr_printf(out, "!%s\n", errmsg);
			freetree(j->p);
			return errmsg;
		}
		j->timing.optimise = GDKusec() - j->timing.optimise;
		if (prg->def->errors) {
			/* this is bad already, so let's try to make it debuggable */
			mnstr_printf(out, "!jaqlgencode: generated program contains errors\n");
			printFunction(out, prg->def, 0, LIST_MAPI);
			
			/* restore the state */
			MSresetInstructions(prg->def, oldstop);
			freeVariables(c, prg->def, c->glb, oldvtop);
			prg->def->errors = 0;
			freetree(j->p);
			throw(SYNTAX, "JAQLparser", "typing errors");
		}
	}

	return MAL_SUCCEED;
}

str
JAQLengine(Client c)
{
	str msg = MAL_SUCCEED;
	MalStkPtr oldglb = c->glb;
	jc *j;

	if ((msg = getJAQLContext(c, &j)) != MAL_SUCCEED)
		return msg;

	/* FIXME: if we don't run this, any barrier will cause an endless loop
	 * (program jumps back to first frame), so this is kind of a
	 * workaround */
	chkProgram(c->fdout, c->nspace, c->curprg->def);

	assert(c->glb == 0 || c->glb == oldglb); /* detect leak */
	c->glb = 0;
	if (j->explain) {
		printFunction(c->fdout, c->curprg->def, 0, LIST_MAL_STMT | LIST_MAPI);
	} else if (j->plan || j->planf) {
		mnstr_printf(c->fdout, "=");
		printtree(c->fdout, j->p, 0, j->planf);
		mnstr_printf(c->fdout, "\n");
		freetree(j->p);
		c->glb = oldglb;
		return MAL_SUCCEED;  /* don't have a plan generated */
	} else if (j->debug) {
		msg = runMALDebugger(c, c->curprg);
	} else if (MALcommentsOnly(c->curprg->def)) {
		msg = MAL_SUCCEED;
	} else {
		msg = runMAL(c, c->curprg->def, 0, 0);
	}

	if (msg) {
		/* don't print exception decoration, just the message */
		char *n = NULL;
		char *o = msg;
		while ((n = strchr(o, '\n')) != NULL) {
			*n = '\0';
			mnstr_printf(c->fdout, "!%s\n", getExceptionMessage(o));
			*n++ = '\n';
			o = n;
		}
		if (*o != 0)
			mnstr_printf(c->fdout, "!%s\n", getExceptionMessage(o));
	}

	MSresetInstructions(c->curprg->def, 1);
	freeVariables(c, c->curprg->def, NULL, j->vtop);
	assert(c->glb == 0 || c->glb == oldglb); /* detect leak */
	c->glb = oldglb;

	freetree(j->p);

	return msg;
}
