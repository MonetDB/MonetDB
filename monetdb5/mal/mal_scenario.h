/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _MAL_SCENARIO_H
#define _MAL_SCENARIO_H

#include "mal_import.h"

/*#define MAL_SCENARIO_DEBUG*/
/*
 * @-
 * The scenario descriptions contains all information to
 * implement the scenario. Each client gets a copy.
 * An exception or error detected while parsing is turned
 * into an exception and aborts the scenario.
 */
#define MAXSCEN 4

typedef struct SCENARIO {
	str name, language;
	str initSystem;
	MALfcn initSystemCmd;
	str exitSystem;
	MALfcn exitSystemCmd;
	str initClient;
	init_client initClientCmd;
	str exitClient;
	MALfcn exitClientCmd;
	str engine;
	MALfcn engineCmd;
	str callback;
	MALfcn callbackCmd;
} *Scenario;

mal_export Scenario getFreeScenario(void);
mal_export Scenario findScenario(str nme);

#ifdef LIBMONETDB5
extern str setScenario(Client c, str nme);
extern str runScenario(Client c);
extern str getScenarioLanguage(Client c);

extern void showCurrentScenario(void);
extern void showScenarioByName(stream *f, str s);
extern void showScenario(stream *f, Scenario s);
extern void showAllScenarios(stream *f);
extern void resetScenario(Client c);
#endif

#endif /* _MAL_SCENARIO_H */
