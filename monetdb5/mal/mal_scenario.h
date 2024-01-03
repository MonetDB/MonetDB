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
	str initClient;
	init_client initClientCmd;
	str exitClient;
	exit_client exitClientCmd;
	str engine;
	engine_fptr engineCmd;
} *Scenario;

mal_export Scenario getFreeScenario(void);
mal_export Scenario findScenario(const char *nme);

#ifdef LIBMONETDB5
extern str setScenario(Client c, const char *nme);
extern str runScenario(Client c);
extern str getScenarioLanguage(Client c);

extern void showCurrentScenario(void);
extern void showScenarioByName(stream *f, const char *s);
extern void showScenario(stream *f, Scenario s);
extern void showAllScenarios(stream *f);
extern void resetScenario(Client c);
#endif

#endif /* _MAL_SCENARIO_H */
