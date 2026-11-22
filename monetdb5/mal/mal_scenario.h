/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
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
mal_export str getScenarioLanguage(Client c);

#ifdef LIBMONETDB5
extern str setScenario(Client c, const char *nme)
	ATTR_VISIBILITY_HIDDEN;
extern str runScenario(Client c)
	ATTR_VISIBILITY_HIDDEN;

extern void showCurrentScenario(void)
	ATTR_VISIBILITY_HIDDEN;
extern void showScenarioByName(stream *f, const char *s)
	ATTR_VISIBILITY_HIDDEN;
extern void showScenario(stream *f, Scenario s)
	ATTR_VISIBILITY_HIDDEN;
extern void showAllScenarios(stream *f)
	ATTR_VISIBILITY_HIDDEN;
extern void resetScenario(Client c)
	ATTR_VISIBILITY_HIDDEN;
extern str defaultScenario(Client c)
	ATTR_VISIBILITY_HIDDEN;
#endif

#endif /* _MAL_SCENARIO_H */
