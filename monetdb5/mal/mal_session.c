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
 * @a M. Kersten
 * @v 0.0
 * @- Server Bootstrapping
 *
 * The MonetDB server uses a startup script to boot the system.
 * This script is an ordinary MAL program, but will mostly
 * consist of include statements to load modules of general interest.
 * The startup script is ran as user Admin.
 * Its location is described in Monet configuration file.
 * The default location is:  !!!%%% TODO %%%!!! <-- FIXME
 *
 * It may overwritten using a command line argument.
 *
 */
#include "monetdb_config.h"
#include "mal_session.h"
#include "mal_instruction.h" /* for pushEndInstruction() */
#include "mal_interpreter.h" /* for showErrors(), runMAL(), garbageElement() */
#include "mal_linker.h"	     /* for initLibraries() */
#include "mal_parser.h"	     /* for parseMAL() */
#include "mal_namespace.h"
#include "mal_readline.h"
#include "mal_authorize.h"
#include "mal_sabaoth.h"
#include <gdk.h>	/* for opendir and friends */

int
malBootstrap(void)
{
	Client c;
	str bootfile = "mal_init", s;

	c = MCinitClient((oid)0, 0, 0);
	assert(c != NULL);
	c->nspace = newModule(NULL, putName("user", 4));
	initLibraries();
	if (defaultScenario(c)) {
		GDKerror("Failed to initialise default scenario");
		return 0;
	}
	MSinitClientPrg(c, "user", "main");
	(void)MCinitClientThread(c);
	s = malInclude(c, bootfile, 0);
	if (s != NULL) {
		mnstr_printf(GDKout, "!%s\n", s);
		GDKfree(s);
		return 0;
	}
	pushEndInstruction(c->curprg->def);
	chkProgram(c->nspace, c->curprg->def);
	if (c->curprg->def->errors)
		showErrors(c);
	s = MALengine(c);
	if (s)
		GDKfree(s);
	return 1;
}

/*
 * @+ Client main routine
 * Every client has a 'main' function to collect the statements.
 * Once the END instruction has been found, it is added to the
 * symbol table and a fresh container is being constructed.
 * Note, this scheme makes testing for recursive function calls a
 * little more difficult. Therefore, type checking should be performed
 * afterwards.
 *
 * In interactive mode,  the closing statement is never reached.
 * The 'main' procedure is typically cleaned between successive external
 * messages except for its variables, which are considerd global.
 * This storage container is re-used when during the previous call
 * nothing was added.
 * At the end of the session we have to garbage collect the BATs
 * introduced.
 */

void
MSinitClientPrg(Client cntxt, str mod, str nme)
{
	InstrPtr p;
	MalBlkPtr mb;

	if (cntxt->curprg && idcmp(nme, cntxt->curprg->name) == 0) {
		MSresetClientPrg(cntxt);
		return;
/*
	int i, cnt = 1;
		mb = cntxt->curprg->def;
		cntxt->itrace = 0;
		getInstrPtr(mb,0)->gc = 0;
		for (i = 1; i < mb->stop; i++)
			if (mb->stmt[i]->token == REMsymbol)
				cnt++;
		if (mb->stop <= cnt + 1) {
			mb->typefixed = 0;
			mb->flowfixed = 0;
			mb->stop = cnt;
			return;
		}
		if( mb->history){
			freeMalBlk(mb->history);
			mb->history=0;
		}
*/
	}
	cntxt->curprg = newFunction(putName("user",4), putName(nme, strlen(nme)), FUNCTIONsymbol);
	mb = cntxt->curprg->def;
	p = getSignature(cntxt->curprg);
	if( mod )
		setModuleId(p,mod);
	else
		setModuleScope(p, cntxt->nspace);
	setVarType(mb, findVariable(mb, nme), TYPE_void);
	insertSymbol(cntxt->nspace, cntxt->curprg);
	cntxt->glb = 0;
	assert(cntxt->curprg->def != NULL);
}

void
MSresetClientPrg(Client cntxt)
{
	MalBlkPtr mb;
	InstrPtr p;

	cntxt->itrace = 0;	/* turn off any debugging */
	mb = cntxt->curprg->def;
	mb->typefixed = 0;
	mb->flowfixed = 0;
	mb->stop = 1;
	mb->errors=0;
	p= mb->stmt[0];
	setModuleId(p,putName("user",4));
	setFunctionId(p,putName("main",4));
	p->gc = 0;
	p->retc=1;
	p->argc=1;
	/* remove any MAL history */
	if( mb->history){
		freeMalBlk(mb->history);
		mb->history=0;
	}
}

/*
 * @+ Client authorization
 * The default method to interact with the database server is to
 * connect using a port number. The first line received should contain
 * authorization information, such as user name.
 *
 * The scheduleClient receives a challenge response consisting of
 * endian:user:password:lang:database:
 */
void
MSscheduleClient(str command, str challenge, bstream *fin, stream *fout)
{
	char *user = command, *algo = NULL, *passwd = NULL, *lang = NULL;
	char *database = NULL, *s;
	Client c;
	MT_Id p;

	/* decode BIG/LIT:user:{cypher}passwordchal:lang:database: line */

	/* byte order */
	s = strchr(user, ':');
	if (s) {
		*s = 0;
		mnstr_set_byteorder(fin->s, strcmp(user, "BIG") == 0);
		user = s + 1;
	} else {
		mnstr_printf(fout, "!incomplete challenge '%s'\n", user);
		mnstr_flush(fout);
		GDKfree(command);
		return;
	}

	/* passwd */
	s = strchr(user, ':');
	if (s) {
		*s = 0;
		passwd = s + 1;
		/* decode algorithm, i.e. {plain}mypasswordchallenge */
		if (*passwd != '{') {
			mnstr_printf(fout, "!invalid password entry\n");
			mnstr_flush(fout);
			GDKfree(command);
			return;
		}
		algo = passwd + 1;
		s = strchr(algo, '}');
		if (!s) {
			mnstr_printf(fout, "!invalid password entry\n");
			mnstr_flush(fout);
			GDKfree(command);
			return;
		}
		*s = 0;
		passwd = s + 1;
	} else {
		mnstr_printf(fout, "!incomplete challenge '%s'\n", user);
		mnstr_flush(fout);
		GDKfree(command);
		return;
	}

	/* lang */
	s = strchr(passwd, ':');
	if (s) {
		*s = 0;
		lang = s + 1;
	} else {
		mnstr_printf(fout, "!incomplete challenge, missing language\n");
		mnstr_flush(fout);
		GDKfree(command);
		return;
	}

	/* database */
	s = strchr(lang, ':');
	if (s) {
		*s = 0;
		database = s + 1;
		/* we can have stuff following, make it void */
		s = strchr(database, ':');
		if (s)
			*s = 0;
	}

	if (!GDKembedded && database != NULL && database[0] != '\0' &&
			strcmp(database, GDKgetenv("gdk_dbname")) != 0)
	{
		mnstr_printf(fout, "!request for database '%s', "
				"but this is database '%s', "
				"did you mean to connect to monetdbd instead?\n",
				database, GDKgetenv("gdk_dbname"));
		/* flush the error to the client, and abort further execution */
		mnstr_flush(fout);
		GDKfree(command);
		return;
	} else {
		str err;
		oid uid;
		sabdb *stats;
		Client root = &mal_clients[0];

		/* access control: verify the credentials supplied by the user,
		 * no need to check for database stuff, because that is done per
		 * database itself (one gets a redirect) */
		err = AUTHcheckCredentials(&uid, &root, &user, &passwd, &challenge, &algo, &lang);
		assert(lang);			/* we expect it's still not NULL */
		if (err != MAL_SUCCEED) {
			mnstr_printf(fout, "!%s\n", err);
			mnstr_flush(fout);
			GDKfree(command);
			return;
		}

		if (!GDKembedded) {
			err = SABAOTHgetMyStatus(&stats);
			if (err != MAL_SUCCEED) {
				/* this is kind of awful, but we need to get rid of this
			 	* message */
				fprintf(stderr, "!SABAOTHgetMyStatus: %s\n", err);
				if (err != M5OutOfMemory)
					GDKfree(err);
				mnstr_printf(fout, "!internal server error, "
						"please try again later\n");
				mnstr_flush(fout);
				GDKfree(command);
				return;
			}
			if (stats->locked == 1) {
				if (uid == 0) {
					mnstr_printf(fout, "#server is running in "
							"maintenance mode\n");
				} else {
					mnstr_printf(fout, "!server is running in "
							"maintenance mode, please try again later\n");
					mnstr_flush(fout);
					SABAOTHfreeStatus(&stats);
					GDKfree(command);
					return;
				}
			}
			SABAOTHfreeStatus(&stats);
		}

		c = MCinitClient(uid, fin, fout);
		if (c == NULL) {
			mnstr_printf(fout, "!internal server error (out of client slots), "
					"please try again later\n");
			mnstr_flush(fout);
			GDKfree(command);
			return;
		}
		/* move this back !! */
		if (c->nspace == 0) {
			c->nspace = newModule(NULL, putName("user", 4));
			c->nspace->outer = mal_clients[0].nspace->outer;
		}

		if ((s = setScenario(c, lang)) != NULL) {
			mnstr_printf(c->fdout, "!%s\n", s);
			mnstr_flush(c->fdout);
			GDKfree(s);
			c->mode = FINISHING;
		}
	}

	MSinitClientPrg(c,"user", "main");

	GDKfree(command);
	if (MT_create_thread(&p, MSserveClient, (void *) c, MT_THR_DETACHED) != 0) {
		mnstr_printf(fout, "!internal server error (cannot fork new "
				"client thread), please try again later\n");
		mnstr_flush(fout);
		showException(MAL, "initClient", "cannot fork new client thread");
		return;
	}
}

/*
 * @+ Client services
 * After the client initialization has been finished, we
 * can start the interaction protocol. This involves parsing the
 * input in the context of an already defined procedure and upon
 * success, its execution.
 *
 * In essence, this calls for an incremental parsing operation,
 * because we should wait until a complete basic block has been detected.
 * Test, first collect the instructions before we take them all.
 * @-
 * In interactive mode, we should remove the instructions before
 * accepting new ones. The function signature remains the same
 * and the symbol table should also not be affected.
 * Aside from removing instruction, we should also condense the
 * variable stack, i.e. removing at least the temporary variables,
 * but maybe everything beyond a previous defined pont.
 *
 * Beware that we have to cleanup the global stack as well. This to avoid
 * subsequent calls to find garbage information.
 * However, this action is only required after a successful execution.
 * Otherwise, garbage collection is not needed.
 */
void
MSresetInstructions(MalBlkPtr mb, int start)
{
	int i;
	InstrPtr p;

	for (i = start; i < mb->ssize; i++) {
		p = getInstrPtr(mb, i);
		if (p)
			freeInstruction(p);
		mb->stmt[i] = NULL;
	}
	mb->stop = start;
}

/*
 * @-
 * Determine the variable being used and clear non-used onces.
 */
void
MSresetVariables(Client cntxt, MalBlkPtr mb, MalStkPtr glb, int start)
{
	int i, k;
	bit *used= GDKzalloc(mb->vtop * sizeof(bit));

	for (i=0; i<start && start<mb->vtop; i++)
		used[i]=1;
	if (mb->errors==0)
	for (i = start; i < mb->vtop; i++) {
		if (used[i] || !isTmpVar(mb,i) ){
			VarPtr v = getVar(mb,i);
			assert(!mb->var[i]->value.vtype || isVarConstant(mb,i) );

			/* keep all properties as well */
			for (k=0; k< v->propc; k++)
				used[mb->prps[k].var]=1;
			used[i]= 1;
		}
		if (glb && !used[i]) {
			if (isVarConstant(mb,i))
				garbageElement(cntxt, &glb->stk[i]);
			/* clean stack entry */
			glb->stk[i].vtype = TYPE_int;
			glb->stk[i].len = 0;
			glb->stk[i].val.pval = 0;
		}
	}
	if (mb->errors==0)
		trimMalVariables_(mb,used,glb);
	GDKfree(used);
}

/*
 * @-
 * Here we start the first client. We need to initialize
 * the corresponding thread and allocate space for the
 * global variables. Thereafter it is up to the scenario
 * interpreter to process input.
 */
void
MSserveClient(void *dummy)
{
	MalBlkPtr mb;
	Client c = (Client) dummy;
	str msg = 0;

	if (!isAdministrator(c) && MCinitClientThread(c) < 0){
		MCcloseClient(c);
		return;
	}
	/*
	 * @-
	 * A stack frame is initialized to keep track of global variables.
	 * The scenarios are run until we finally close the last one.
	 */
	mb = c->curprg->def;
	if ( c->glb == NULL)
		c->glb = newGlobalStack(MAXGLOBALS + mb->vsize);
	if ( c->glb == NULL){
		showException(MAL, "serveClient", MAL_MALLOC_FAIL);
		c->mode = FINISHING + 1; /* == CLAIMED */
	} else {
		c->glb->stktop = mb->vtop;
		c->glb->blk = mb;
	}

	if (c->scenario == 0)
		msg = defaultScenario(c);
	if (msg) {
		showException(MAL, "serveClient", "could not initialize default scenario");
		c->mode = FINISHING + 1; /* == CLAIMED */
	} else {
		do {
			do {
				runScenario(c);
				if (c->mode == FINISHING)
					break;
				resetScenario(c);
			} while (c->scenario);
		} while(c->scenario && c->mode != FINISHING);
	}
	/*
	 * @-
	 * At this stage we should clean out the MAL block
	 */
	freeMalBlk(c->curprg->def);
	c->curprg->def = 0;

	if (c->mode > FINISHING) {
		if (isAdministrator(c) /* && moreClients(0)==0 */) {
			if (c->scenario) {
				exitScenario(c);
			}
		}
	}
	if (!isAdministrator(c))
		MCcloseClient(c);
}

/*
 * @+ MAL scenario components
 * The stages of processing user requests are controlled by a
 * scenario. The routines below are the default implementation.
 * The main issues to deal after parsing it to clean out the
 * Admin.main function from any information added erroneously.
 *
 * Ideally this involves resetting the state of the client
 * 'main' function, i.e. the symbol table is reset and any
 * instruction added should be cleaned. Beware that the instruction
 * table may have grown in size.
 *
 */
str
MALinitClient(Client c)
{
	assert (c->state[0] == NULL);
	c->state[0] = c;
	return NULL;
}

str
MALexitClient(Client c)
{
	if (c->glb && c->curprg->def->errors == 0)
		garbageCollector(c, c->curprg->def, c->glb,TRUE);
	if ( c-> bak)
		return NULL;
	c->mode = FINISHING;
	return NULL;
}

str
MALreader(Client c)
{
	int r= 1;

	if (c == mal_clients) {
		r = readConsole(c);
		if (r < 0 && c->fdin->eof == 0 )
			r = MCreadClient(c);
		if (r > 0)
			return MAL_SUCCEED;
	} else if (MCreadClient(c) > 0)
		return MAL_SUCCEED;
	c->mode = FINISHING;
	if (c->fdin)
		c->fdin->buf[c->fdin->pos] = 0;
	else
		throw(MAL, "mal.reader", RUNTIME_IO_EOF);
	return MAL_SUCCEED;
}

str
MALparser(Client c)
{
	InstrPtr p;
	MalBlkRecord oldstate;

	c->curprg->def->errors = 0;
	oldstate = *c->curprg->def;

	prepareMalBlk(c->curprg->def, CURRENT(c));
	if (parseMAL(c, c->curprg) || c->curprg->def->errors) {
		/* just complete it for visibility */
		pushEndInstruction(c->curprg->def);
		/* caught errors */
		showErrors(c);
		if( c->listing)
			printFunction(c->fdout,c->curprg->def, 0, c->listing);
		MSresetVariables(c,c->curprg->def, c->glb, oldstate.vtop);
		resetMalBlk(c->curprg->def,1);
		/* now the parsing is done we should advance the stream */
		c->fdin->pos += c->yycur;
		c->yycur = 0;
		throw(SYNTAX, "mal.parser", SYNTAX_GENERAL MANUAL_HELP);
	}

	/* now the parsing is done we should advance the stream */
	c->fdin->pos += c->yycur;
	c->yycur = 0;

	/* check for unfinished blocks */
	if (c->blkmode)
		return MAL_SUCCEED;
	/* empty files should be skipped as well */
	if (c->curprg->def->stop == 1)
		return MAL_SUCCEED;

	p = getInstrPtr(c->curprg->def, 0);
	if (p->token != FUNCTIONsymbol) {
		if( c->listing)
			printFunction(c->fdout,c->curprg->def, 0, c->listing);
		MSresetVariables(c,c->curprg->def, c->glb, oldstate.vtop);
		resetMalBlk(c->curprg->def,1);
		throw(SYNTAX, "mal.parser", SYNTAX_SIGNATURE);
	}
	pushEndInstruction(c->curprg->def);
	chkProgram(c->nspace, c->curprg->def);
	if (c->curprg->def->errors) {
		showErrors(c);
		if( c->listing)
			printFunction(c->fdout,c->curprg->def, 0, c->listing);
		MSresetVariables(c,c->curprg->def, c->glb, oldstate.vtop);
		resetMalBlk(c->curprg->def,1);
		throw(MAL, "MAL.parser", SEMANTIC_GENERAL);
	}
	return MAL_SUCCEED;
}

int
MALcommentsOnly(MalBlkPtr mb)
{
	int i;

	for (i = 1; i < mb->stop; i++)
		if (mb->stmt[i]->token != REMsymbol)
			return 0;
	return 1;
}

str
MALengine(Client c)
{
	Symbol prg;
	str msg = MAL_SUCCEED;
	MalBlkRecord oldstate = *c->curprg->def;
	oldstate.stop = 0;

	if( c->blkmode)
		return MAL_SUCCEED;
	prg = c->curprg;
	if (prg == NULL)
		throw(SYNTAX, "mal.engine", SYNTAX_SIGNATURE);
	if (prg->def == NULL)
		throw(SYNTAX, "mal.engine", SYNTAX_SIGNATURE);

	if (prg->def->errors > 0) {
		showErrors(c);
		if (c->listing )
			printFunction(c->fdout, c->curprg->def, 0, c->listing);
		MSresetVariables(c,c->curprg->def, c->glb, oldstate.vtop);
		resetMalBlk(c->curprg->def,1);
		throw(MAL, "mal.engine", PROGRAM_GENERAL);
	}
	if (prg->def->stop == 1 || MALcommentsOnly(prg->def))
		return 0;	/* empty block */
	if (c->glb) {
		if (prg->def && c->glb->stksize < prg->def->vsize)
			c->glb = reallocGlobalStack(c->glb, prg->def->vsize);
		c->glb->stktop = prg->def->vtop;
		c->glb->blk = prg->def;
		c->glb->cmd = (c->itrace && c->itrace != 'C') ? 'n' : 0;
	}
	if (c->listing > 1 )
		printFunction(c->fdout, c->curprg->def, 0, c->listing);

	/*
	 * @-
	 * In interactive mode we should avoid early garbage collection of values.
	 * This can be controlled by the clean up control at the instruction level
	 * and marking all non-temporary variables as being (potentially) used.
	 */
	if (c->glb){
		c->glb->pcup = 0;
		c->glb->keepAlive= TRUE; /* no garbage collection */
	}
	if (prg->def->errors == 0)
		msg = (str) runMAL(c, prg->def, 1, 0, c->glb, 0);
	if (msg) {
		str place = getExceptionPlace(msg);
		showException(getExceptionType(msg), place, "%s", getExceptionMessage(msg));
		GDKfree(place);
		if (!c->listing)
			printFunction(c->fdout, c->curprg->def, 0, c->listing);
		showErrors(c);
	}
	MSresetVariables(c,prg->def, c->glb, 0);
	resetMalBlk(prg->def,1);
	if (c->glb){
		/* for global stacks avoid reinitialization from this point */
		c->glb->stkbot = prg->def->vtop;
	}
	if( prg->def->profiler){
		GDKfree(prg->def->profiler);
		prg->def->profiler= NULL;
	}
	prg->def->errors = 0;
	if( c->itrace)
		mnstr_printf(c->fdout,"mdb>#EOD\n");
	return msg;
}

