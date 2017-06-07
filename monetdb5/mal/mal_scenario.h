/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _MAL_SCENARIO_H
#define _MAL_SCENARIO_H

#include "mal_import.h"

#define MAL_SCENARIO_READER 0
#define MAL_SCENARIO_PARSER  1
#define MAL_SCENARIO_OPTIMIZE 2
#define MAL_SCENARIO_SCHEDULER 3
#define MAL_SCENARIO_ENGINE 4
#define MAL_SCENARIO_INITCLIENT 5
#define MAL_SCENARIO_EXITCLIENT 6
#define MAL_SCENARIO_CALLBACK 7

/*#define MAL_SCENARIO_DEBUG*/
/*
 * @-
 * The scenario descriptions contains all information to
 * implement the scenario. Each client gets a copy.
 * An exception or error detected while parsing is turned
 * into an exception and aborts the scenario.
 */
#define MAXSCEN 128

typedef struct SCENARIO {
	str name, language;
	str initSystem;
	MALfcn initSystemCmd;
	str exitSystem;
	MALfcn exitSystemCmd;
	str initClient;
	MALfcn initClientCmd;
	str exitClient;
	MALfcn exitClientCmd;
	str reader;
	MALfcn readerCmd;
	void *readerState;
	str parser;
	MALfcn parserCmd;
	void *parserState;
	str optimizer;
	MALfcn optimizerCmd;
	void *optimizerState;
	str tactics;
	MALfcn tacticsCmd;
	void *tacticsState;
	str engine;
	MALfcn engineCmd;
	void *engineState;
	str callback;
	MALfcn callbackCmd;
	void *callbackState;
	struct SCENARIO *next;
} *Scenario;

mal_export str setScenario(Client c, str nme);
mal_export str runScenario(Client c, int once);
mal_export str getScenarioLanguage(Client c);
mal_export Scenario getFreeScenario(void);

mal_export void showCurrentScenario(void);
mal_export void showScenarioByName(stream *f, str s);
mal_export void showScenario(stream *f, Scenario s);
mal_export void showAllScenarios(stream *f);
mal_export void resetScenario(Client c);

mal_export Scenario findScenario(str nme);
mal_export void updateScenario(str scen, str nme, MALfcn fcn);

#endif /* _MAL_SCENARIO_H */
