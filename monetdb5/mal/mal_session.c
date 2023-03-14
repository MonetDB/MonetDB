/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/* (author) M.L. Kersten
 */
#include "monetdb_config.h"
#include "mal_session.h"
#include "mal_instruction.h" /* for pushEndInstruction() */
#include "mal_interpreter.h" /* for runMAL(), garbageElement() */
#include "mal_parser.h"	     /* for parseMAL() */
#include "mal_namespace.h"
#include "mal_authorize.h"
#include "mal_builder.h"
#include "msabaoth.h"
#include "mal_private.h"
#include "gdk.h"	/* for opendir and friends */

/*
 * The MonetDB server uses a startup script to boot the system.
 * This script is an ordinary MAL program, but will mostly
 * consist of include statements to load modules of general interest.
 * The startup script is run as user Admin.
 */
str
malBootstrap(char *modules[], bool embedded, const char *initpasswd)
{
	Client c;
	str msg = MAL_SUCCEED;

	c = MCinitClient(MAL_ADMIN, NULL, NULL);
	if(c == NULL) {
		throw(MAL, "malBootstrap", "Failed to initialize client");
	}
	assert(c != NULL);
	c->curmodule = c->usermodule = userModule();
	if(c->usermodule == NULL) {
		MCfreeClient(c);
		throw(MAL, "malBootstrap", "Failed to initialize client MAL module");
	}
	if ( (msg = defaultScenario(c)) ) {
		MCfreeClient(c);
		return msg;
	}
	if((msg = MSinitClientPrg(c, "user", "main")) != MAL_SUCCEED) {
		MCfreeClient(c);
		return msg;
	}

	if( MCinitClientThread(c) < 0){
		MCfreeClient(c);
		throw(MAL, "malBootstrap", "Failed to create client thread");
	}
	if ((msg = malIncludeModules(c, modules, 0, embedded, initpasswd)) != MAL_SUCCEED) {
		MCfreeClient(c);
		return msg;
	}
	pushEndInstruction(c->curprg->def);
	msg = chkProgram(c->usermodule, c->curprg->def);
	if ( msg != MAL_SUCCEED || (msg= c->curprg->def->errors) != MAL_SUCCEED ) {
		MCfreeClient(c);
		return msg;
	}
	msg = MALengine(c);
	MCfreeClient(c);
	return msg;
}

/*
 * Every client has a 'main' function to collect the statements.  Once
 * the END instruction has been found, it is added to the symbol table
 * and a fresh container is being constructed.  Note, this scheme makes
 * testing for recursive function calls a little more difficult.
 * Therefore, type checking should be performed afterwards.
 *
 * In interactive mode,  the closing statement is never reached.  The
 * 'main' procedure is typically cleaned between successive external
 * messages except for its variables, which are considerd global.  This
 * storage container is re-used when during the previous call nothing
 * was added.  At the end of the session we have to garbage collect the
 * BATs introduced.
 */
static str
MSresetClientPrg(Client cntxt, const char *mod, const char *fcn)
{
	MalBlkPtr mb;
	InstrPtr p;

	cntxt->itrace = 0;  /* turn off any debugging */
	mb = cntxt->curprg->def;
	mb->stop = 1;
	mb->errors = MAL_SUCCEED;
	p = mb->stmt[0];

	p->gc = 0;
	p->retc = 1;
	p->argc = 1;
	p->argv[0] = 0;

	setModuleId(p, mod);
	setFunctionId(p, fcn);
	if( findVariable(mb,fcn) < 0)
		if ((p->argv[0] = newVariable(mb, fcn, strlen(fcn), TYPE_void)) < 0)
			throw(MAL, "resetClientPrg", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	setVarType(mb, findVariable(mb, fcn), TYPE_void);
	/* remove any MAL history */
	if (mb->history) {
		freeMalBlk(mb->history);
		mb->history = 0;
	}
	return MAL_SUCCEED;
}

/*
 * Create a new container block
 */

str
MSinitClientPrg(Client cntxt, const char *mod, const char *nme)
{
	int idx;

	if (cntxt->curprg  && idcmp(nme, cntxt->curprg->name) == 0)
		return MSresetClientPrg(cntxt, putName(mod), putName(nme));
	cntxt->curprg = newFunction(putName(mod), putName(nme), FUNCTIONsymbol);
	if( cntxt->curprg == 0)
		throw(MAL, "initClientPrg", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if( (idx= findVariable(cntxt->curprg->def,"main")) >=0)
		setVarType(cntxt->curprg->def, idx, TYPE_void);
	insertSymbol(cntxt->usermodule,cntxt->curprg);

	if (cntxt->glb == NULL )
		cntxt->glb = newGlobalStack(MAXGLOBALS + cntxt->curprg->def->vsize);
	if( cntxt->glb == NULL)
		throw(MAL,"initClientPrg", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	assert(cntxt->curprg->def != NULL);
	assert(cntxt->curprg->def->vtop >0);
	return MAL_SUCCEED;
}

/*
 * The default method to interact with the database server is to connect
 * using a port number. The first line received should contain
 * authorization information, such as user name.
 *
 * The scheduleClient receives a challenge response consisting of
 * endian:user:password:lang:database:
 */
static void
exit_streams( bstream *fin, stream *fout )
{
	if (fout && fout != GDKstdout) {
		mnstr_flush(fout, MNSTR_FLUSH_DATA);
		close_stream(fout);
	}
	if (fin)
		bstream_destroy(fin);
}

const char* mal_enableflag = "mal_for_all";

static bool
is_exiting(void *data)
{
	(void) data;
	return GDKexiting();
}

static str MSserveClient(Client cntxt);


static inline void
cleanUpScheduleClient(Client c, Scenario s, bstream *fin, stream *fout, str *command, str *err)
{
	if(c) {
		if (s) {
			str msg = NULL;
			if((msg = s->exitClientCmd(c)) != MAL_SUCCEED) {
				mnstr_printf(fout, "!%s\n", msg);
				freeException(msg);
			}
		}
		MCfreeClient(c);
	}
	exit_streams(fin, fout);
	if (command) {
		GDKfree(*command);
		*command = NULL;
	}
	if (err) {
		freeException(*err);
		*err = NULL;
	}
}


void
MSscheduleClient(str command, str challenge, bstream *fin, stream *fout, protocol_version protocol, size_t blocksize)
{
	char *user = command, *algo = NULL, *passwd = NULL, *lang = NULL, *handshake_opts = NULL;
	char *database = NULL, *s;
	const char *dbname;
	str msg = MAL_SUCCEED;
	bool filetrans = false;
	Client c;

	/* decode BIG/LIT:user:{cypher}passwordchal:lang:database: line */

	/* byte order */
	s = strchr(user, ':');
	if (s) {
		*s = 0;
		mnstr_set_bigendian(fin->s, strcmp(user, "BIG") == 0);
		user = s + 1;
	} else {
		mnstr_printf(fout, "!incomplete challenge '%s'\n", user);
		exit_streams(fin, fout);
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
			exit_streams(fin, fout);
			GDKfree(command);
			return;
		}
		algo = passwd + 1;
		s = strchr(algo, '}');
		if (!s) {
			mnstr_printf(fout, "!invalid password entry\n");
			exit_streams(fin, fout);
			GDKfree(command);
			return;
		}
		*s = 0;
		passwd = s + 1;
	} else {
		mnstr_printf(fout, "!incomplete challenge '%s'\n", user);
		exit_streams(fin, fout);
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
		exit_streams(fin, fout);
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
			*s++ = 0;
	}

	if (s && strncmp(s, "FILETRANS:", 10) == 0) {
		s += 10;
		filetrans = true;
	} else if (s && s[0] == ':') {
		s += 1;
		filetrans = false;
	}

	if (s && strchr(s, ':') != NULL) {
		handshake_opts = s;
		s = strchr(s, ':');
		*s++ = '\0';
	}
	dbname = GDKgetenv("gdk_dbname");
	if (database != NULL && database[0] != '\0' &&
		strcmp(database, dbname) != 0)
	{
		mnstr_printf(fout, "!request for database '%s', "
						   "but this is database '%s', "
						   "did you mean to connect to monetdbd instead?\n",
				database, dbname);
		/* flush the error to the client, and abort further execution */
		exit_streams(fin, fout);
		GDKfree(command);
		return;
	} else {
		str err;
		oid uid = 0;
		sabdb *stats = NULL;

		if (!GDKembedded()) {
			if ((c = MCinitClient(MAL_ADMIN, NULL, NULL)) == NULL) {
				if ( MCshutdowninprogress())
					mnstr_printf(fout, "!system shutdown in progress, please try again later\n");
				else
					mnstr_printf(fout, "!maximum concurrent client limit reached "
									   "(%d), please try again later\n", MAL_MAXCLIENTS);
				cleanUpScheduleClient(NULL, NULL, fin, fout, &command, NULL);
				return;
			}
			Scenario scenario = findScenario("sql");
			if ((msg = scenario->initClientCmd(c)) != MAL_SUCCEED) {
				mnstr_printf(fout, "!%s\n", msg);
				cleanUpScheduleClient(c, scenario, fin, fout, &command, &msg);
				return;
			}
			/* access control: verify the credentials supplied by the user,
			 * no need to check for database stuff, because that is done per
			 * database itself (one gets a redirect) */
			if ((msg = AUTHcheckCredentials(&uid, c, user, passwd, challenge, algo)) != MAL_SUCCEED) {
				mnstr_printf(fout, "!%s\n", msg);
				cleanUpScheduleClient(c, scenario, fin, fout, &command, &msg);
				return;
			}
			cleanUpScheduleClient(c, scenario, NULL, NULL, NULL, NULL);
		}


		if (!GDKinmemory(0) && !GDKembedded()) {
			err = msab_getMyStatus(&stats);
			if (err != NULL) {
				/* this is kind of awful, but we need to get rid of this
				 * message */
				free(err);
				mnstr_printf(fout, "!internal server error, "
							 "please try again later\n");
				exit_streams(fin, fout);
				GDKfree(command);
				return;
			}
			if (stats->locked) {
				if (uid == 0) {
					mnstr_printf(fout, "#server is running in "
								 "maintenance mode\n");
				} else {
					mnstr_printf(fout, "!server is running in "
								 "maintenance mode, please try again later\n");
					exit_streams(fin, fout);
					msab_freeStatus(&stats);
					GDKfree(command);
					return;
				}
			}
			msab_freeStatus(&stats);
		}

		c = MCinitClient(uid, fin, fout);
		if (c == NULL) {
			if ( MCshutdowninprogress())
				mnstr_printf(fout, "!system shutdown in progress, please try again later\n");
			else
				mnstr_printf(fout, "!maximum concurrent client limit reached "
								   "(%d), please try again later\n", MAL_MAXCLIENTS);
			exit_streams(fin, fout);
			GDKfree(command);
			return;
		}
		c->filetrans = filetrans;
		c->handshake_options = handshake_opts ? strdup(handshake_opts) : NULL;
		/* move this back !! */
		if (c->usermodule == 0) {
			c->curmodule = c->usermodule = userModule();
			if(c->curmodule  == NULL) {
				mnstr_printf(fout, "!could not allocate space\n");
				exit_streams(fin, fout);
				GDKfree(command);
				MCcloseClient(c);
				return;
			}
		}

		if ((s = setScenario(c, lang)) != NULL) {
			mnstr_printf(c->fdout, "!%s\n", s);
			mnstr_flush(c->fdout, MNSTR_FLUSH_DATA);
			GDKfree(s);
			exit_streams(fin, fout);
			GDKfree(command);
			MCcloseClient(c);
			return;
		}
		if (!GDKgetenv_isyes(mal_enableflag) &&
				(strncasecmp("sql", lang, 3) != 0 && uid != 0)) {

			mnstr_printf(fout, "!only the 'monetdb' user can use non-sql languages. "
					           "run mserver5 with --set %s=yes to change this.\n", mal_enableflag);
			exit_streams(fin, fout);
			GDKfree(command);
			MCcloseClient(c);
			return;
		}
	}

	if((msg = MSinitClientPrg(c, "user", "main")) != MAL_SUCCEED) {
		mnstr_printf(fout, "!could not allocate space\n");
		exit_streams(fin, fout);
		freeException(msg);
		GDKfree(command);
		MCcloseClient(c);
		return;
	}

	// at this point username should have being verified
	c->username = GDKstrdup(user);

	GDKfree(command);

	/* NOTE ABOUT STARTING NEW THREADS
	 * At this point we have conducted experiments (Jun 2012) with
	 * reusing threads.  The implementation used was a lockless array of
	 * semaphores to wake up threads to do work.  Experimentation on
	 * Linux, Solaris and Darwin showed no significant improvements, in
	 * most cases no improvements at all.  Hence the following
	 * conclusion: thread reuse doesn't save up on the costs of just
	 * forking new threads.  Since the latter means no difficulties of
	 * properly maintaining a pool of threads and picking the workers
	 * out of them, it is favourable just to start new threads on
	 * demand. */

	/* fork a new thread to handle this client */

	c->protocol = protocol;
	c->blocksize = blocksize;

	mnstr_settimeout(c->fdin->s, 50, is_exiting, NULL);
	msg = MSserveClient(c);
	if (msg != MAL_SUCCEED) {
		mnstr_printf(fout, "!could not serve client\n");
		exit_streams(fin, fout);
		freeException(msg);
	}
}

/*
 * After the client initialization has been finished, we can start the
 * interaction protocol. This involves parsing the input in the context
 * of an already defined procedure and upon success, its execution.
 *
 * In essence, this calls for an incremental parsing operation, because
 * we should wait until a complete basic block has been detected.  Test,
 * first collect the instructions before we take them all.
 *
 * In interactive mode, we should remove the instructions before
 * accepting new ones. The function signature remains the same and the
 * symbol table should also not be affected.  Aside from removing
 * instruction, we should also condense the variable stack, i.e.
 * removing at least the temporary variables, but maybe everything
 * beyond a previous defined point.
 *
 * Beware that we have to cleanup the global stack as well. This to
 * avoid subsequent calls to find garbage information.  However, this
 * action is only required after a successful execution.  Otherwise,
 * garbage collection is not needed.
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
 * MAL instructions generate variables.
 * The values of temporary variables should be cleaned at the end of a call
 * The values of global variables are retained.
 * Global variables should not start with C_ or X_
 */
void
MSresetStack(Client cntxt, MalBlkPtr mb, MalStkPtr glb)
{
	InstrPtr sig = getInstrPtr(mb, 0);
	int i, k = sig->argc;

	if (mb->errors == MAL_SUCCEED){
		for (i = sig->argc; i < mb->vtop; i++) {
			if (glb && i < glb->stktop && isTmpVar(mb,i) && !glb->keepTmps) {
				/* clean stack entry */
				garbageElement(cntxt, &glb->stk[i]);
				glb->stk[i].vtype = TYPE_int;
				glb->stk[i].len = 0;
				glb->stk[i].val.pval = 0;
				if (isVarConstant(mb, i))
					garbageElement(cntxt, &mb->var[i].value);
			} else {
				/* compress the global variable list and stack */
				mb->var[k] = mb->var[i];
				glb->stk[k] = glb->stk[i];
				setVarUsed(mb, k);
				setVarInit(mb, k);
				if( i != k){
					glb->stk[i].vtype = TYPE_int;
					glb->stk[i].len = 0;
					glb->stk[i].val.pval = 0;
					clrVarConstant(mb,i);
					clrVarCleanup(mb,i);
				}
				k++;
			}
		}
	}
	assert(k <= mb->vsize);
	mb->vtop = k;
}

/* The symbol table be become filled with constant values to be garbage collected
* The signature is always left behind.
*/

void
MSresetVariables(MalBlkPtr mb)
{
	InstrPtr sig = getInstrPtr(mb, 0);
	int i;

	if (mb->errors == MAL_SUCCEED)
		for (i = sig->argc; i < mb->vtop; i++)
			if( isVarConstant(mb,i)){
				VALclear(&getVarConstant(mb,i));
				clrVarConstant(mb, i);
			}
}

/*
 * Here we start the client.  We need to initialize and allocate space
 * for the global variables.  Thereafter it is up to the scenario
 * interpreter to process input.
 */
static str
MSserveClient(Client c)
{
	MalBlkPtr mb;
	str msg = 0;

	if (MCinitClientThread(c) < 0) {
		MCcloseClient(c);
		return MAL_SUCCEED;
	}
	/*
	 * A stack frame is initialized to keep track of global variables.
	 * The scenarios are run until we finally close the last one.
	 */
	mb = c->curprg->def;
	if (c->glb == NULL)
		c->glb = newGlobalStack(MAXGLOBALS + mb->vsize);
	if (c->glb == NULL) {
		MCcloseClient(c);
		throw(MAL, "serveClient", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else {
		c->glb->stktop = mb->vtop;
		c->glb->blk = mb;
	}

	if (c->scenario == 0)
		msg = defaultScenario(c);
	if (msg) {
		MCcloseClient(c);
		return msg;
	} else {
		do {
			do {
				MT_thread_setworking("running scenario");
				msg = runScenario(c,0);
				freeException(msg);
				if (c->mode == FINISHCLIENT)
					break;
				resetScenario(c);
			} while (c->scenario && !GDKexiting());
		} while (c->scenario && c->mode != FINISHCLIENT && !GDKexiting());
	}
	MT_thread_setworking("exiting");
	/* pre announce our exiting: cleaning up may take a while and we
	 * don't want to get killed during that time for fear of
	 * deadlocks */
	MT_exiting_thread();
	/*
	 * At this stage we should clean out the MAL block
	 */
	if (c->backup) {
		assert(0);
		freeSymbol(c->backup);
		c->backup = 0;
	}

	if( c->curprg && c->curprg->def)
		resetMalBlk(c->curprg->def);
	/*
	if (c->curprg) {
		freeSymbol(c->curprg);
		c->curprg = 0;
	}
	*/

	MCcloseClient(c);
	return MAL_SUCCEED;
}

/*
 * The stages of processing user requests are controlled by a scenario.
 * The routines below are the default implementation.  The main issues
 * to deal after parsing it to clean out the Admin.main function from
 * any information added erroneously.
 *
 * Ideally this involves resetting the state of the client 'main'
 * function, i.e. the symbol table is reset and any instruction added
 * should be cleaned. Beware that the instruction table may have grown
 * in size.
 */
str
MALinitClient(Client c)
{
	assert(c->state[0] == NULL);
	c->state[0] = c;
	return NULL;
}

str
MALexitClient(Client c)
{
	if (c->glb && c->curprg->def->errors == MAL_SUCCEED)
		garbageCollector(c, c->curprg->def, c->glb, TRUE);
	c->mode = FINISHCLIENT;
	if (c->backup) {
		assert(0);
		freeSymbol(c->backup);
		c->backup = NULL;
	}
	/* should be in the usermodule */
	c->curprg = NULL;
	if (c->usermodule){
		freeModule(c->usermodule);
		c->usermodule = NULL;
	}
	return NULL;
}

str
MALreader(Client c)
{
	if (MCreadClient(c) > 0)
		return MAL_SUCCEED;
	MT_lock_set(&mal_contextLock);
	c->mode = FINISHCLIENT;
	MT_lock_unset(&mal_contextLock);
	if (c->fdin)
		c->fdin->buf[c->fdin->pos] = 0;
	return MAL_SUCCEED;
}

/* Before compiling a large string, it makes sense to allocate
 * approximately enough space to keep the intermediate
 * code. Otherwise, we end up with a repeated extend on the MAL block,
 * which really consumes a lot of memcpy resources. The average MAL
 * string length could been derived from the test cases. An error in
 * the estimate is more expensive than just counting the lines.
 */
static int
prepareMalBlk(MalBlkPtr mb, str s)
{
	int cnt = STMT_INCREMENT;

	while (s) {
		s = strchr(s, '\n');
		if (s) {
			s++;
			cnt++;
		}
	}
	cnt = (int) (cnt * 1.1);
	return resizeMalBlk(mb, cnt);
}

str
MALparser(Client c)
{
	InstrPtr p;
	str msg= MAL_SUCCEED;

	assert(c->curprg->def->errors == NULL);
	c->curprg->def->errors = 0;

	if( prepareMalBlk(c->curprg->def, CURRENT(c)) < 0)
		throw(MAL, "mal.parser", "Failed to prepare");
	parseMAL(c, c->curprg, 0, INT_MAX, 0);

	/* now the parsing is done we should advance the stream */
	c->fdin->pos += c->yycur;
	c->yycur = 0;

	/* check for unfinished blocks */
	if(!c->curprg->def->errors && c->blkmode)
		return MAL_SUCCEED;
	/* empty files should be skipped as well */
	if (c->curprg->def->stop == 1){
		if ( (msg =c->curprg->def->errors) )
			c->curprg->def->errors = 0;
		return msg;
	}

	p = getInstrPtr(c->curprg->def, 0);
	if (p->token != FUNCTIONsymbol) {
		msg =c->curprg->def->errors;
		c->curprg->def->errors = 0;
		MSresetStack(c, c->curprg->def, c->glb);
		resetMalTypes(c->curprg->def, 1);
		return msg;
	}
	pushEndInstruction(c->curprg->def);
	msg = chkProgram(c->usermodule, c->curprg->def);
	if (msg !=MAL_SUCCEED || (msg =c->curprg->def->errors) ){
		c->curprg->def->errors = 0;
		MSresetStack(c, c->curprg->def, c->glb);
		resetMalTypes(c->curprg->def, 1);
		return msg;
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
MALcallback(Client c, str msg)
{
	if (msg) {
		/* don't print exception decoration, just the message */
		char *n = NULL;
		char *o = msg;
		while ((n = strchr(o, '\n')) != NULL) {
			if (*o == '!')
				o++;
			mnstr_printf(c->fdout, "!%.*s\n", (int) (n - o), o);
			o = ++n;
		}
		if (*o != 0) {
			if (*o == '!')
				o++;
			mnstr_printf(c->fdout, "!%s\n", o);
		}
		freeException(msg);
	}
	return MAL_SUCCEED;
}

str
MALengine(Client c)
{
	Symbol prg;
	str msg = MAL_SUCCEED;

	if (c->blkmode)
		return MAL_SUCCEED;
	prg = c->curprg;
	if (prg == NULL)
		throw(SYNTAX, "mal.engine", SYNTAX_SIGNATURE);
	if (prg->def == NULL)
		throw(SYNTAX, "mal.engine", SYNTAX_SIGNATURE);

	if (prg->def->errors != MAL_SUCCEED) {
		msg = prg->def->errors;
		prg->def->errors = NULL;
		MSresetStack(c, c->curprg->def, c->glb);
		resetMalTypes(c->curprg->def, 1);
		return msg;
	}
	if (prg->def->stop == 1 || MALcommentsOnly(prg->def))
		return 0;   /* empty block */
	if (c->glb) {
		if (prg->def && c->glb->stksize < prg->def->vsize){
			c->glb = reallocGlobalStack(c->glb, prg->def->vsize);
			if( c->glb == NULL)
				throw(MAL, "mal.engine", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		c->glb->stktop = prg->def->vtop;
		c->glb->blk = prg->def;
		c->glb->cmd = (c->itrace && c->itrace != 'C') ? 'n' : 0;
	}

	/*
	 * In interactive mode we should avoid early garbage collection of values.
	 * This can be controlled by the clean up control at the instruction level
	 * and marking all non-temporary variables as being (potentially) used.
	 */
	if (c->glb) {
		c->glb->pcup = 0;
		c->glb->keepAlive = TRUE; /* no garbage collection */
	}
	if (prg->def->errors == MAL_SUCCEED)
		msg = (str) runMAL(c, prg->def, 0, c->glb);
	if (msg) {
		/* ignore "internal" exceptions */
		if (strstr(msg, "client.quit") ) {
			freeException(msg);
			msg = MAL_SUCCEED;
		}
	}
	MSresetStack(c, prg->def, c->glb);
	resetMalTypes(prg->def, 1);
	if (c->glb) {
		/* for global stacks avoid reinitialization from this point */
		c->glb->stkbot = prg->def->vtop;
	}

	if (prg->def->errors)
		freeException(prg->def->errors);
	prg->def->errors = NULL;
	if (c->itrace)
		mnstr_printf(c->fdout, "mdb>#EOD\n");
	return msg;
}
