/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "sql_scripts_hge.h"
#include "gdk.h"
#include "mal_client.h"

#include "39_analytics_hge.sql.h"
#include "40_json_hge.sql.h"

#include <assert.h>

#define LOAD_SQL_SCRIPT(script) \
	if ((err = script(c)) != MAL_SUCCEED) \
		return err;

str
install_sql_scripts_hge(Client c)
{
	str err;
	LOAD_SQL_SCRIPT(sql_install_39_analytics_hge)
	LOAD_SQL_SCRIPT(sql_install_40_json_hge)
	return err;
}
