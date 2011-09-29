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
	struct SCENARIO *next;
} *Scenario;

mal_export str setScenario(Client c, str nme);
mal_export str runScenario(Client c);
mal_export str getScenarioLanguage(Client c);
mal_export Scenario getFreeScenario(void);

mal_export str defaultScenario(Client c);	/* used in src/mal/mal_session.c */
mal_export void exitScenario(Client c);	/* used in src/mal/mal_session.c */

mal_export void showCurrentScenario(void);
mal_export void showScenarioByName(stream *f, str s);
mal_export void showScenario(stream *f, Scenario s);
mal_export void showAllScenarios(stream *f);
mal_export void resetScenario(Client c);

mal_export Scenario findScenario(str nme);
mal_export void updateScenario(str scen, str nme, MALfcn fcn);

#endif /* _MAL_SCENARIO_H */
