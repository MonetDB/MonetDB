#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.
#]]

#[[
install(FILES
  10_sys_schema_extension.sql
  12_url.sql
  13_date.sql
  14_inet.sql
  15_querylog.sql
  16_tracelog.sql
  17_temporal.sql
  18_index.sql
  21_dependency_views.sql
  22_clients.sql
  23_skyserver.sql
  25_debug.sql
  26_sysmon.sql
  27_rejects.sql
  39_analytics.sql
  40_json.sql
  42_md5sum.sql
  45_uuid.sql
  46_profiler.sql
  47_regexp.sql
  48_txtsim.sql
  49_strings.sql
  52_describe.sql
  58_hot_snapshot.sql
  75_storagemodel.sql
  76_dump.sql
  77_storage.sql
  80_statistics.sql
  81_tracer.sql
  91_information_schema.sql
  99_system.sql
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5/createdb)

if(HAVE_HGE)
  install(FILES
    39_analytics_hge.sql
    41_json_hge.sql DESTINATION
    ${CMAKE_INSTALL_LIBDIR}/monetdb5/createdb)
endif()
#]]
