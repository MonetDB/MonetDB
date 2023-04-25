/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * (author) M. Kersten
 * @+ Session Scenarios
 * In MonetDB multiple languages and execution engines can
 * be combined at run time to satisfy a wide user-community.
 * Such an assemblage of components is called a @emph{scenario}
 * and consists of a @emph{engine}. These hooks allow
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
 * Thereafter its components are set to those required by
 * the scenario and the client initialization takes place.
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
 * pushing a new one(language, optimize,
 * processor).
 *
 * @+ Scenario administration
 * Administration of scenarios follows the access rules
 * defined for code modules in general.
 *
 */
#include "monetdb_config.h"
#include "mal_scenario.h"
#include "mal_client.h"
#include "mal_authorize.h"
#include "mal_exception.h"
#include "mal_profiler.h"
#include "mal_private.h"
#include "mal_session.h"

#ifdef HAVE_SYS_TIMES_H
# include <sys/times.h>
#endif

static struct SCENARIO scenarioRec[MAXSCEN] = {
	{	.name = "mal",
		.language = "mal",
		.initClient = "MALinitClient",
		.initClientCmd = (init_client) MALinitClient,
		.exitClient = "MALexitClient",
		.exitClientCmd = (MALfcn) MALexitClient,
		.engine = "MALengine",
		.engineCmd = (engine_fptr)MALengine,
	},
	{
		.name = NULL,
	}
};

static str fillScenario(Client c, Scenario scen);
static MT_Lock scenarioLock = MT_LOCK_INITIALIZER(scenarioLock);


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
	if (i < MAXSCEN)
		scen = scenarioRec + i;
	MT_lock_unset(&scenarioLock);

	return scen;
}

str
defaultScenario(Client c)
{
	return fillScenario(c, scenarioRec);
}

/*
 * The Monet debugger provides an option to inspect the scenarios currently
 * defined.
 *
 */
static void
print_scenarioCommand(stream *f, str cmd)
{
	mnstr_printf(f," \"%s\",", cmd);
}

void
showScenario(stream *f, Scenario scen)
{
	mnstr_printf(f, "[ \"%s\",", scen->name);
	print_scenarioCommand(f, scen->initClient);
	print_scenarioCommand(f, scen->exitClient);
	print_scenarioCommand(f, scen->engine);
	mnstr_printf(f, "]\n");
}

Scenario
findScenario(const char *nme)
{
	int i;
	Scenario scen = scenarioRec;

	for (i = 0; i < MAXSCEN; i++, scen++)
		if (scen->name && strcmp(scen->name, nme) == 0)
			return scen;
	return NULL;
}

void
showScenarioByName(stream *f, const char *nme)
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
 * state maintained for the previous scenario.
 * Before we initialize a scenario the client scenario is reset to
 * the MAL scenario. This implies that all scenarios are initialized
 * using the same scenario. After the scenario initialization file
 * has been processed, the scenario functions are replaced with the
 * proper ones.
 *
 * All client records should be initialized with a default
 * scenario, i.e. the first described in the scenario table.
 */
static str
fillScenario(Client c, Scenario scen)
{
	c->scenario = scen->name;

	c->engine = scen->engineCmd;
	c->initClient = scen->initClientCmd;
	c->exitClient = scen->exitClientCmd;
	return(MAL_SUCCEED);
}

/*
 * Setting a new scenario calls for saving the previous state
 * and execution of the initClientScenario routine.
 */
str
setScenario(Client c, const char *nme)
{
	str msg;
	Scenario scen;

	scen = findScenario(nme);
	if (scen == NULL)
		throw(MAL, "setScenario", SCENARIO_NOT_FOUND " '%s'", nme);

	msg = fillScenario(c, scen);
	if (msg) {
		/* error occurred, reset the scenario , assume default always works */
		c->scenario = NULL;
		c->initClient = NULL;
		c->exitClient = NULL;
		c->engine = NULL;
		return msg;
	}
	return MAL_SUCCEED;
}

/*
 * After finishing a session in a scenario, we should reset the
 * state of the previous one. But also call the exitClient
 * to garbage collect any scenario specific structures.
 */

void
resetScenario(Client c)
{
	Scenario scen = scenarioRec;

	if (c->scenario == 0)
		return;

	scen = findScenario(c->scenario);
	if (scen != NULL && scen->exitClientCmd) {
		str msg = (*scen->exitClientCmd) (c);
		freeException(msg);
	}

	c->scenario = NULL;
	c->initClient = NULL;
	c->exitClient = NULL;
	c->engine = NULL;
}

/*
 * The building blocks of scenarios are routines obeying a strict
 * name signature. They require exclusive access to the client
 * record. Any specific information should be accessible from
 * there, e.g., access to a scenario specific descriptor.
 * The client scenario initialization and finalization brackets
 * are  @sc{xyzinitClient()} and @sc{xyzexitClient()}.
 *
 * The @sc{xyzengine(Client c)} contains the applicable back-end engine.
 * The default is the MAL interpreter, which provides good balance
 * between speed and ability to analysis its behavior.
 *
 */

/*
 * Access control enforcement. Except for the server owner
 * running a scenario should be explicitly permitted.
 */
static str
runScenarioBody(Client c)
{
	MT_thread_setworking("engine");
	while (c->mode > FINISHCLIENT && !GDKexiting()) {
		c->engine(c);
		assert(c->curprg->def->errors == NULL);
	}
	if (!GDKexiting() && GDKerrbuf && GDKerrbuf[0])
		mnstr_printf(c->fdout,"!GDKerror: %s\n",GDKerrbuf);
	return c->exitClient(c);
}

str
runScenario(Client c)
{
	str msg = MAL_SUCCEED;

	if (c == 0)
		return msg;
	msg = runScenarioBody(c);
	if (msg != MAL_SUCCEED &&
			strcmp(msg,"MALException:client.quit:Server stopped."))
		mnstr_printf(c->fdout,"!%s\n",msg);
	return msg;
}
