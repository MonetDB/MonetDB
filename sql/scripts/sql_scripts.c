/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "sql_scripts.h"
#include "gdk.h"
#include "mal_client.h"

#include "09_like.sql.h"
#include "10_math.sql.h"
#include "11_times.sql.h"
#include "12_url.sql.h"
#include "13_date.sql.h"
#include "14_inet.sql.h"
#include "15_querylog.sql.h"
#include "16_tracelog.sql.h"
#include "17_temporal.sql.h"
#include "18_index.sql.h"
#include "20_vacuum.sql.h"
#include "21_dependency_views.sql.h"
#include "22_clients.sql.h"
#include "23_skyserver.sql.h"
#include "25_debug.sql.h"
#include "26_sysmon.sql.h"
#include "27_rejects.sql.h"
#include "39_analytics.sql.h"
#include "40_json.sql.h"
#include "41_md5sum.sql.h"
#include "45_uuid.sql.h"
#include "46_profiler.sql.h"
#include "51_sys_schema_extension.sql.h"
#include "60_wlcr.sql.h"
#include "75_storagemodel.sql.h"
#include "80_statistics.sql.h"
#include "99_system.sql.h"

#include <assert.h>

#define LOAD_SQL_SCRIPT(script) \
	if ((err = script(c)) != MAL_SUCCEED) \
		return err;

str
install_sql_scripts(Client c)
{
	str err;
LOAD_SQL_SCRIPT(sql_install_09_like)
LOAD_SQL_SCRIPT(sql_install_10_math)
LOAD_SQL_SCRIPT(sql_install_11_times)
LOAD_SQL_SCRIPT(sql_install_12_url)
LOAD_SQL_SCRIPT(sql_install_13_date)
LOAD_SQL_SCRIPT(sql_install_14_inet)
LOAD_SQL_SCRIPT(sql_install_15_querylog)
LOAD_SQL_SCRIPT(sql_install_16_tracelog)
LOAD_SQL_SCRIPT(sql_install_17_temporal)
LOAD_SQL_SCRIPT(sql_install_18_index)
LOAD_SQL_SCRIPT(sql_install_20_vacuum)
LOAD_SQL_SCRIPT(sql_install_21_dependency_views)
LOAD_SQL_SCRIPT(sql_install_22_clients)
LOAD_SQL_SCRIPT(sql_install_23_skyserver)
LOAD_SQL_SCRIPT(sql_install_25_debug)
LOAD_SQL_SCRIPT(sql_install_26_sysmon)
LOAD_SQL_SCRIPT(sql_install_27_rejects)
LOAD_SQL_SCRIPT(sql_install_39_analytics)
LOAD_SQL_SCRIPT(sql_install_40_json)
LOAD_SQL_SCRIPT(sql_install_41_md5sum)
LOAD_SQL_SCRIPT(sql_install_45_uuid)
LOAD_SQL_SCRIPT(sql_install_46_profiler)
LOAD_SQL_SCRIPT(sql_install_51_sys_schema_extension)
LOAD_SQL_SCRIPT(sql_install_60_wlcr)
LOAD_SQL_SCRIPT(sql_install_75_storagemodel)
LOAD_SQL_SCRIPT(sql_install_80_statistics)
LOAD_SQL_SCRIPT(sql_install_99_system)
	return err;
}
