/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (author) M. Kersten
 * @+ Session Scenarios
 * In MonetDB multiple languages, optimizers, and execution engines can
 * be combined at run time to satisfy a wide user-community.
 * Such an assemblage of components is called a @emph{scenario}
 * and consists of a @emph{reader}, @emph{parser}, @emph{optimizer},
 * @emph{tactic scheduler} and @emph{engine}. These hooks allow
 * for both linked-in and external components.
 *
 * The languages supported are SQL, the Monet Assembly Language (MAL), and profiler.
 * The default scenario handles MAL instructions, which is used
 * to illustrate the behavior of the scenario steps.
 *
 * The MAL reader component handles interaction with
 * a front-end to obtain a string for subsequent compilation and
 * execution. The reader uses the common stream package to read
 * data in large chunks, if possible. In interactive mode the lines
 * are processed one at a time.
 *
 * The MAL parser component turns the string into
 * an internal representation of the MAL program.
 * During this phase semantic checks are performed, such that
 * we end up with a type correct program.
 *
 * The code block is subsequently sent to an MAL optimizer.
 * In the default case the program is left untouched. For other languages,
 * the optimizer deploys language specific code transformations,
 * e.g., foreign-key optimizations in joins and remote query execution.
 * All optimization information is statically derived from the
 * code blocks and possible catalogues maintained for the query language
 * at hand. Optimizers leave advice and their findings in properties
 * in the symbol table, see @ref{Property Management}.
 *
 * Once the program has thus been refined, the
 * MAL scheduler prepares for execution using tactical optimizations.
 * For example, it may parallelize the code, generate an ad-hoc
 * user-defined function, or prepare for efficient replication management.
 * In the default case, the program is handed over to the MAL interpreter
 * without any further modification.
 *
 * The final stage is to choose an execution paradigm,
 * i.e. interpretative (default), compilation of an ad-hoc user
 * defined function, dataflow driven interpretation,
 * or vectorized pipe-line execution by a dedicated engine.
 *
 * A failure encountered in any of the steps terminates the scenario
 * cycle. It returns to the user for a new command.
 *
 * @+ Scenario management
 * Scenarios are captured in modules; they can be dynamically loaded
 * and remain active until the system is brought to a halt.
 * The first time a scenario @sc{xyz} is used, the system looks for a scenario
 * initialization routine @sc{xyzinitSystem()} and executes it.
 * It is typically used to prepare the server for language specific interactions.
 * Thereafter its components are set to those required by
 * the scenario and the client initialization takes place.
 *
 * When the last user interested in a particular scenario leaves the
 * scene, we activate its finalization routine calling @sc{xyzexitSystem()}.
 * It typically perform cleanup, backup and monitoring functions.
 *
 * A scenario is interpreted in a strictly linear fashion,
 * i.e. performing a symbolic optimization before scheduling decisions
 * are taken.
 * The routines associated with each state in
 * the scenario may patch the code so as to assure that subsequent
 * execution can use a different scenario, e.g., to handle dynamic
 * code fragments.
 *
 * The state of execution is maintained in the scenario record for
 * each individual client. Sharing this information between clients
 * should be dealt with in the implementation of the scenario managers.
 * Upon need, the client can postpone a session scenario by
 * pushing a new one(language, optimize, tactic,
 * processor). Propagation of the state information is
 * encapsulated a scenario2scenario() call. Not all transformations
 * may be legal.
 *
 * @+ Scenario administration
 * Administration of scenarios follows the access rules
 * defined for code modules in general.
 *
 */
/*
 * @-
 */
#include "monetdb_config.h"
#include "mal_scenario.h"
#include "mal_linker.h"		/* for getAddress() */
#include "mal_client.h"
#include "mal_authorize.h"
#include "mal_exception.h"
#include "mal_profiler.h"
#include "mal_private.h"

#ifdef HAVE_SYS_TIMES_H
# include <sys/times.h>
#endif

static struct SCENARIO scenarioRec[MAXSCEN] = {
	{"mal", "mal",
	 0, 0,			/* hardwired MALinit*/
	 0, 0,			/* implicit */
	 "MALinitClient", (MALfcn) &MALinitClient,
	 "MALexitClient", (MALfcn) &MALexitClient,
	 "MALreader", (MALfcn) &MALreader, 0,
	 "MALparser", (MALfcn) &MALparser, 0,
	 "MALoptimizer", 0, 0,
	 0, 0, 0,
	 "MALengine", (MALfcn) &MALengine, 0, 0},
	{"profiler","profiler",			/* name */
	 0, 0,			/* initClient */
	 0, 0,			/* exitClient */
	 "PROFinitClient", (MALfcn) &PROFinitClient,			/* initClient */
	 "PROFexitClient", (MALfcn) &PROFexitClient,			/* exitClient */
	 "MALreader", (MALfcn) &MALreader, 0,		/* reader */
	 "MALparser", (MALfcn) &MALparser, 0,		/* parser */
	 0, 0, 0,		/* optimizer */
	 0, 0, 0,		/* scheduler */
	 0, 0, 0, 0		/* engine */
	 },
	{0,0,			/* name */
	 0, 0,			/* init */
	 0, 0,			/* exit */
	 0, 0,			/* initClient */
	 0, 0,			/* exitClient */
	 0, 0, 0,		/* reader */
	 0, 0, 0,		/* parser */
	 0, 0, 0,		/* optimizer */
	 0, 0, 0,		/* scheduler */
	 0, 0, 0, 0		/* engine */
	 }
};

static str fillScenario(Client c, Scenario scen);
static MT_Lock scenarioLock MT_LOCK_INITIALIZER("scenarioLock");


/*
 * Currently each user can define a new scenario, provided we have a free slot.
 * Scenarios not hardwired can always be dropped.
 */
Scenario
getFreeScenario(void)
{
	int i;
	Scenario scen = NULL;

	MT_lock_set(&scenarioLock);
	for (i = 0; i < MAXSCEN && scenarioRec[i].name; i++)
		;

	if (i == MAXSCEN) {
		showException(GDKout, MAL,"freeScenario", "no scenario space left (%d); adjust MAXSCEN and recompile", MAXSCEN);
	} else {
		scen = scenarioRec + i;
	}
	MT_lock_unset(&scenarioLock);

	return scen;
}

/*
 * A scenario is initialized only once per session.
 * All other requests are silently ignored.
 * After initialization, all state functions should have been set.
 * Initialization includes searching for the scenario startup file in
 * the etc/MonetDB directory. This creates a dependency, because the
 * malInclude also needs a scenario. To break this cycle, the system should
 * call once the routine default scenario for each client first.
 */
static str
initScenario(Client c, Scenario s)
{
	str l = s->language;
	str msg = MAL_SUCCEED;

	if (s->initSystemCmd)
		return(fillScenario(c, s));
	/* prepare for conclicts */
	MT_lock_set(&mal_contextLock);
	if (s->initSystem && s->initSystemCmd == 0) {
		s->initSystemCmd = (MALfcn) getAddress(c->fdout, l, s->initSystem,0);
		if (s->initSystemCmd) {
			msg = (*s->initSystemCmd) (c);
		} else {
			char buf[BUFSIZ];
			snprintf(buf,BUFSIZ,"%s.init", l);
			msg = createException(MAL, buf, "Scenario not initialized");
		}
	}
	if (msg) {
		MT_lock_unset(&mal_contextLock);
		return msg;
	}

	if (s->exitSystem && s->exitSystemCmd == 0)
		s->exitSystemCmd = (MALfcn) getAddress(c->fdout, l, s->exitSystem,0);
	if (s->initClient && s->initClientCmd == 0)
		s->initClientCmd = (MALfcn) getAddress(c->fdout, l, s->initClient,0);
	if (s->exitClient && s->exitClientCmd == 0)
		s->exitClientCmd = (MALfcn) getAddress(c->fdout, l, s->exitClient,0);
	if (s->reader && s->readerCmd == 0)
		s->readerCmd = (MALfcn) getAddress(c->fdout, l, s->reader,0);
	if (s->parser && s->parserCmd == 0)
		s->parserCmd = (MALfcn) getAddress(c->fdout, l, s->parser,0);
	if (s->optimizer && s->optimizerCmd == 0)
		s->optimizerCmd = (MALfcn) getAddress(c->fdout, l, s->optimizer,0);
	if (s->tactics && s->tacticsCmd == 0)
		s->tacticsCmd = (MALfcn) getAddress(c->fdout, l, s->tactics,0);
	if (s->engine && s->engineCmd == 0)
		s->engineCmd = (MALfcn) getAddress(c->fdout, l, s->engine,0);
	MT_lock_unset(&mal_contextLock);
	return(fillScenario(c, s));
}

str
defaultScenario(Client c)
{
#ifdef NEED_MT_LOCK_INIT
	if (c == mal_clients)
		MT_lock_init(&scenarioLock, "scenarioLock");
#endif
	return initScenario(c, scenarioRec);
}

/*
 * The Monet debugger provides an option to inspect the scenarios currently
 * defined.
 *
 */
static void
print_scenarioCommand(stream *f, str cmd, MALfcn funcptr)
{
    if (cmd)
	mnstr_printf(f," \"%s%s\",", cmd, (funcptr?"":"?"));
    else
	mnstr_printf(f," nil,");
}

void
showScenario(stream *f, Scenario scen)
{
	mnstr_printf(f, "[ \"%s\",", scen->name);
	print_scenarioCommand(f, scen->initSystem, scen->initSystemCmd);
	print_scenarioCommand(f, scen->exitSystem, scen->exitSystemCmd);
	print_scenarioCommand(f, scen->initClient, scen->initClientCmd);
	print_scenarioCommand(f, scen->exitClient, scen->exitClientCmd);
	print_scenarioCommand(f, scen->parser, scen->parserCmd);
	print_scenarioCommand(f, scen->optimizer, scen->optimizerCmd);
	print_scenarioCommand(f, scen->tactics, scen->tacticsCmd);
	print_scenarioCommand(f, scen->engine, scen->engineCmd);
	mnstr_printf(f, "]\n");
}

Scenario
findScenario(str nme)
{
	int i;
	Scenario scen = scenarioRec;

	for (i = 0; i < MAXSCEN && scen->name; i++, scen++)
		if (strcmp(scen->name, nme) == 0)
			return scen;
	return NULL;
}

/*
 * Functions may become resolved only after the corresponding module
 * has been loaded. This should be announced as part of the module
 * prelude code.
 * Beware that after the update, we also have to adjust the client records.
 * They contain a copy of the functions addresses.
 */
void
updateScenario(str nme, str fnme, MALfcn fcn)
{
	int phase = -1;
	Scenario scen = findScenario(nme);

	if (scen->initSystem && strcmp(scen->initSystem, fnme) == 0)
		scen->initSystemCmd = fcn;
	if (scen->exitSystem && strcmp(scen->exitSystem, fnme) == 0)
		scen->exitSystemCmd = fcn;
	if (scen->initClient && strcmp(scen->initClient, fnme) == 0) {
		scen->initClientCmd = fcn;
		phase = MAL_SCENARIO_INITCLIENT;
	}
	if (scen->exitClient && strcmp(scen->exitClient, fnme) == 0) {
		scen->exitClientCmd = fcn;
		phase = MAL_SCENARIO_EXITCLIENT;
	}
	if (scen->reader && strcmp(scen->reader, fnme) == 0) {
		scen->readerCmd = fcn;
		phase = MAL_SCENARIO_READER;
	}
	if (scen->parser && strcmp(scen->parser, fnme) == 0) {
		scen->parserCmd = fcn;
		phase = MAL_SCENARIO_PARSER;
	}
	if (scen->optimizer && strcmp(scen->optimizer, fnme) == 0) {
		scen->optimizerCmd = fcn;
		phase = MAL_SCENARIO_OPTIMIZE;
	}
	if (scen->tactics && strcmp(scen->tactics, fnme) == 0) {
		scen->tacticsCmd = fcn;
		phase = MAL_SCENARIO_SCHEDULER;
	}
	if (scen->engine && strcmp(scen->engine, fnme) == 0) {
		scen->engineCmd = fcn;
		phase = MAL_SCENARIO_ENGINE;
	}
	if (phase != -1) {
		Client c1;

		for (c1 = mal_clients; c1 < mal_clients + MAL_MAXCLIENTS; c1++) {
			if (c1->scenario &&
			    strcmp(c1->scenario, scen->name) == 0)
				c1->phase[phase] = fcn;
			if (c1->oldscenario &&
			    strcmp(c1->oldscenario, scen->name) == 0)
				c1->oldphase[phase] = fcn;
		}
	}
}

void
showScenarioByName(stream *f, str nme)
{
	Scenario scen = findScenario(nme);

	if (scen)
		showScenario(f, scen);
}

void
showAllScenarios(stream *f)
{
	int i;
	Scenario scen = scenarioRec;

	for (i = 0; i < MAXSCEN && scen->name; i++, scen++)
		showScenario(f, scen);
}

str getScenarioLanguage(Client c){
	Scenario scen= findScenario(c->scenario);
	if( scen) return scen->language;
	return "mal";
}
/*
 * Changing the scenario for a particular client invalidates the
 * state maintained for the previous scenario. The old scenario is
 * retained in the client record to facilitate propagation of
 * state information, or to simply switch back to the previous one.
 * Before we initialize a scenario the client scenario is reset to
 * the MAL scenario. This implies that all scenarios are initialized
 * using the same scenario. After the scenario initialization file
 * has been processed, the scenario phases are replaced with the
 * proper ones.
 *
 * All client records should be initialized with a default
 * scenario, i.e. the first described in the scenario table.
 */
static str
fillScenario(Client c, Scenario scen)
{
	c->scenario = scen->name;

	c->phase[MAL_SCENARIO_READER] = scen->readerCmd;
	c->phase[MAL_SCENARIO_PARSER] = scen->parserCmd;
	c->phase[MAL_SCENARIO_OPTIMIZE] = scen->optimizerCmd;
	c->phase[MAL_SCENARIO_SCHEDULER] = scen->tacticsCmd;
	c->phase[MAL_SCENARIO_ENGINE] = scen->engineCmd;
	c->phase[MAL_SCENARIO_INITCLIENT] = scen->initClientCmd;
	c->phase[MAL_SCENARIO_EXITCLIENT] = scen->exitClientCmd;
	c->state[MAL_SCENARIO_READER] = 0;
	c->state[MAL_SCENARIO_PARSER] = 0;
	c->state[MAL_SCENARIO_OPTIMIZE] = 0;
	c->state[MAL_SCENARIO_SCHEDULER] = 0;
	c->state[MAL_SCENARIO_ENGINE] = 0;
	c->state[MAL_SCENARIO_INITCLIENT] = 0;
	c->state[MAL_SCENARIO_EXITCLIENT] = 0;
	return(MAL_SUCCEED);
}

/*
 * Setting a new scenario calls for saving the previous state
 * and execution of the initClientScenario routine.
 */
str
setScenario(Client c, str nme)
{
	int i;
	str msg;
	Scenario scen = scenarioRec;

	scen = findScenario(nme);
	if (scen == NULL)
		throw(MAL, "setScenario", SCENARIO_NOT_FOUND " '%s'", nme);

	if (c->scenario) {
		c->oldscenario = c->scenario;
		for (i = 0; i < 7; i++) {
			c->oldstate[i] = c->state[i];
			c->oldphase[i] = c->phase[i];
		}
	}
	for (i = 0; i < 7; i++)
		c->state[i] = 0;

	msg = initScenario(c, scen);
	if (msg) {
		/* error occurred, reset the scenario , assume default always works */
		c->scenario = c->oldscenario;
		for (i = 0; i < 7; i++) {
			c->state[i] = c->oldstate[i];
			c->phase[i] = c->oldphase[i];
			c->oldstate[i] = NULL;
			c->oldphase[i] = NULL;
		}
		c->oldscenario = NULL;
		return msg;
	}
	return MAL_SUCCEED;
}

/*
 * After finishing a session in a scenario, we should reset the
 * state of the previous one. But also call the exitClient
 * to garbage collect any scenario specific structures.
 */
#if 0
str
getCurrentScenario(Client c)
{
	return c->scenario;
}
#endif

void
resetScenario(Client c)
{
	int i;
	Scenario scen = scenarioRec;

	if (c->scenario == 0)
		return;

	scen = findScenario(c->scenario);
	if (scen != NULL && scen->exitClientCmd)
		(*scen->exitClientCmd) (c);

	c->scenario = c->oldscenario;
	for (i = 0; i < 7; i++) {
		c->state[i] = c->oldstate[i];
		c->phase[i] = c->oldphase[i];
	}
	c->oldscenario = 0;
}

void
exitScenario(Client c)
{
	Scenario scen = scenarioRec;

	if (c->scenario == 0)
		return;
	scen = findScenario(c->scenario);
	if (scen->exitSystemCmd)
		(*scen->exitSystemCmd) (c);
	c->scenario = NULL;
}

/*
 * The building blocks of scenarios are routines obeying a strict
 * name signature. They require exclusive access to the client
 * record. Any specific information should be accessible from
 * there, e.g., access to a scenario specific state descriptor.
 * The client scenario initialization and finalization brackets
 * are  @sc{xyzinitClient()} and @sc{xyzexitClient()}.
 *
 * The @sc{xyzparser(Client c)} contains the parser for language XYZ
 * and should fill the MAL program block associated with the client record.
 * The latter may have been initialized with variables.
 * Each language parser may require a catalog with information
 * on the translation of language specific datastructures into their BAT
 * equivalent.
 *
 * The @sc{xyzoptimizer(Client c)} contains language specific optimizations
 * using the MAL intermediate code as a starting point.
 *
 * The @sc{xyztactics(Client c)} synchronizes the program execution with the
 * state of the machine, e.g., claiming resources, the history of the client
 * or alignment of the request with concurrent actions (e.g., transaction
 * coordination).
 *
 * The @sc{xyzengine(Client c)} contains the applicable back-end engine.
 * The default is the MAL interpreter, which provides good balance
 * between speed and ability to analysis its behavior.
 *
 */
static str
runPhase(Client c, int phase)
{
	str msg = MAL_SUCCEED;
	if (c->phase[c->stage = phase])
	    return msg = (str) (*c->phase[phase])(c);
	return msg;
}

/*
 * Access control enforcement. Except for the server owner
 * running a scenario should be explicitly permitted.
 */
static str
runScenarioBody(Client c)
{
	str msg= MAL_SUCCEED;
	lng start;

	c->exception_buf_initialized = 1;
	if (setjmp( c->exception_buf) < 0)
		c->mode = FINISHCLIENT;
	while ((c->mode > FINISHCLIENT || msg != MAL_SUCCEED) && !GDKexiting()) {
		if (msg != MAL_SUCCEED){
/* we should actually show it [postponed]
			mnstr_printf(c->fdout,"!%s\n",msg);
*/
			freeException(msg);
			msg = MAL_SUCCEED;
		}
		if (!c->state[0] &&
		    (msg = runPhase(c, MAL_SCENARIO_INITCLIENT)) != MAL_SUCCEED)
			continue;
		if (c->mode <= FINISHCLIENT ||
		    (msg = runPhase(c, MAL_SCENARIO_READER)) != MAL_SUCCEED)
			continue;
		c->lastcmd= time(0);
		start= GDKusec();
		if (c->mode <= FINISHCLIENT ||
		    (msg = runPhase(c, MAL_SCENARIO_PARSER)) != MAL_SUCCEED)
			continue;
		if (c->mode <= FINISHCLIENT ||
		    (msg = runPhase(c, MAL_SCENARIO_OPTIMIZE)) != MAL_SUCCEED)
			continue;
		if (c->mode <= FINISHCLIENT ||
                    (msg = runPhase(c, MAL_SCENARIO_SCHEDULER)) != MAL_SUCCEED)
			continue;
		if (c->mode <= FINISHCLIENT ||
		    (msg = runPhase(c, MAL_SCENARIO_ENGINE)) != MAL_SUCCEED)
			continue;
		c->actions++;
		start = GDKusec()-start;
		c->totaltime += start;
	}
	if (c->phase[MAL_SCENARIO_EXITCLIENT])
		(*c->phase[MAL_SCENARIO_EXITCLIENT]) (c);
	c->exception_buf_initialized = 0;
	return msg;
}

str
runScenario(Client c)
{
	str msg = MAL_SUCCEED;

	if (c == 0 || c->phase[MAL_SCENARIO_READER] == 0)
		return msg;
	msg = runScenarioBody(c);
	if (msg != MAL_SUCCEED &&
			strcmp(msg,"MALException:client.quit:Server stopped."))
		mnstr_printf(c->fdout,"!%s\n",msg);
	return msg;
}

