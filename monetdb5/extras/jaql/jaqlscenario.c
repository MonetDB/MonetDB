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

#include "monetdb_config.h"
/*#include "jaql_scenario.h"*/
#include "jaql.h"
#include "jaqlgencode.h"
#include "msabaoth.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_exception.h"
#include "mal_scenario.h"
#include "mal_instruction.h"
#include "optimizer.h"

#include "parser/jaql.tab.h"
#include "parser/jaql.yy.h"

extern int jaqlparse(jc *j);

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
	c->state[MAL_SCENARIO_OPTIMIZE] = j;

	return msg;
}

str
JAQLexitClient(Client c)
{
	if (c->state[MAL_SCENARIO_OPTIMIZE] != NULL) {
		jc *j = (jc *) c->state[MAL_SCENARIO_OPTIMIZE];

		jaqllex_destroy(j->scanner);
		j->scanner = NULL;
		freevars(j->vars);

		c->state[MAL_SCENARIO_READER] = NULL;
		c->state[MAL_SCENARIO_PARSER] = NULL;
		c->state[MAL_SCENARIO_OPTIMIZE] = NULL;
	}

	return MAL_SUCCEED;
}


void
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
	if (MCreadClient(c) > 0)
		return MAL_SUCCEED;

	c->mode = FINISHING;
	if (c->fdin) {
		c->fdin->buf[c->fdin->pos] = 0;
	} else {
		throw(MAL, "jaql.reader", RUNTIME_IO_EOF);
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

	if ((errmsg = getContext(c, &j)) != MAL_SUCCEED) {
		/* tell the client */
		mnstr_printf(out, "!%s, aborting\n", errmsg);
		/* leave a message in the log */
		fprintf(stderr, "%s, cannot handle client!\n", errmsg);
		/* stop here, instead of printing the exception below to the
		 * client in an endless loop */
		c->mode = FINISHING;
		return errmsg;
	}

	c->curprg->def->errors = 0;
	oldvtop = c->curprg->def->vtop;
	oldstop = c->curprg->def->stop;
	j->vtop = oldvtop;
	j->explain = 0;
	j->buf = in->buf + in->pos;

	jaqlparse(j);
	
	/* now the parsing is done we should advance the stream */
	in->pos = in->len;
	c->yycur = 0;

	if (j->err[0] != '\0') {
		/* tell the client */
		mnstr_printf(out, "!%s\n", j->err);
		j->err[0] = '\0';
		return MAL_SUCCEED;
	}

	if (j->p == NULL) /* there was nothing to parse, EOF */
		return MAL_SUCCEED;

	if (j->explain < 2 || j->explain == 4) {
		Symbol prg = c->curprg;
		j->explain |= 64;  /* request dumping in MAPI mode */
		(void)dumptree(j, c, prg->def, j->p);
		j->explain &= ~64;
		pushEndInstruction(prg->def);
		/* codegen could report an error */
		if (j->err[0] != '\0') {
			MSresetInstructions(prg->def, oldstop);
			freeVariables(c, prg->def, c->glb, oldvtop);
			prg->def->errors = 0;
			mnstr_printf(out, "!%s\n", j->err);
			throw(PARSE, "JAQLparse", "%s", j->err);
		}

		chkTypes(out, c->nspace, prg->def, FALSE);
		if (prg->def->errors) {
			/* this is bad already, so let's try to make it debuggable */
			mnstr_printf(out, "!jaqlgencode: generated program contains errors\n");
			printFunction(out, c->curprg->def, 0, LIST_MAPI);
			
			/* restore the state */
			MSresetInstructions(prg->def, oldstop);
			freeVariables(c, prg->def, c->glb, oldvtop);
			prg->def->errors = 0;
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

	if ((msg = getContext(c, &j)) != MAL_SUCCEED)
		return msg;

	c->glb = 0;
	if (j->explain == 1) {
		chkProgram(c->fdout, c->nspace, c->curprg->def);
		printFunction(c->fdout, c->curprg->def, 0, LIST_MAL_STMT | LIST_MAPI);
	} else if (j->explain == 2 || j->explain == 3) {
		printtree(j->p, 0, j->explain == 3);
		printf("\n");
		return MAL_SUCCEED;  /* don't have a plan generated */
	} else if (j->explain == 4) {
		msg = runMALDebugger(c, c->curprg);
	} else if (MALcommentsOnly(c->curprg->def)) {
		msg = MAL_SUCCEED;
	} else {
		msg = runMAL(c, c->curprg->def, 1, 0, 0, 0);
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
	freeVariables(c, c->curprg->def, c->glb, j->vtop);
	c->glb = oldglb;

	return msg;
}
