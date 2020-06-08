#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

include(os_release_info)

if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  get_os_release_info(LINUX_DISTRO LINUX_DISTRO_VERSION)
endif()

set(CPACK_RPM_COMPONENT_INSTALL ON)
#set(CPACK_RPM_FILENAME RPM-DEFAULT)
set(CPACK_RPM_PACKAGE_SUMMARY "MonetDB - Monet Database Management System")
set(CPACK_RPM_PACKAGE_VENDOR "MonetDB BV <info@monetdb.org>")
set(CPACK_RPM_PACKAGE_LICENSE "MPLv2.0")
set(CPACK_RPM_PACKAGE_GROUP "Applications/Databases")
set(CPACK_RPM_PACKAGE_URL "https://www.monetdb.org/")
# set(CPACK_RPM_CHANGELOG_FILE"")

set(CPACK_RPM_monetdb_PACKAGE_NAME "${CMAKE_PROJECT_NAME}")
set(CPACK_RPM_monetdb_FILE_NAME "${CMAKE_PROJECT_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_monetdb_PACKAGE_SUGGESTS "%{name}-client%{?_isa} = %{version}-%{release}")

set(CPACK_RPM_monetdbdev_PACKAGE_NAME "devel")
set(CPACK_RPM_monetdbdev_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_monetdbdev_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_monetdbdev_PACKAGE_SUMMARY "MonetDB development files")
set(CPACK_RPM_monetdbdev_PACKAGE_REQUIRES
  "%{name}%{?_isa} = %{version}-%{release}, %{name}-stream-devel%{?_isa} = %{version}-%{release}")

set(CPACK_RPM_stream_PACKAGE_NAME "stream")
set(CPACK_RPM_stream_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_stream_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_stream_SUMMARY "MonetDB stream library")

set(CPACK_RPM_streamdev_PACKAGE_NAME "stream-devel")
set(CPACK_RPM_streamdev_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_streamdev_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_streamdev_SUMMARY "MonetDB stream library")
set(CPACK_RPM_streamdev_REQUIRES "%{name}-stream%{?_isa} = %{version}-%{release}, bzip2-devel, libcurl-devel, zlib-devel")

set(CPACK_RPM_client_PACKAGE_NAME "client")
set(CPACK_RPM_client_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_client_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_client_PACKAGE_SUMMARY "MonetDB - Monet Database Management System Client Programs")
set(CPACK_RPM_client_RECCOMMENDS "%{name}-SQL-server5%{?_isa} = %{version}-%{release}")

set(CPACK_RPM_clientdev_PACKAGE_NAME "client-devel")
set(CPACK_RPM_clientdev_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_clientdev_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_clientdev_PACKAGE_SUMMARY "MonetDB - Monet Database Management System Client Programs")
set(CPACK_RPM_clientdev_PACKAGE_REQUIRES "%{name}-client%{?_isa} = %{version}-%{release}, %{name}-stream-devel%{?_isa} = %{version}-%{release}, openssl-devel")

set(CPACK_RPM_odbc_PACKAGE_NAME "client-odbc")
set(CPACK_RPM_odbc_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_odbc_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_odbc_PACKAGE_SUMMARY "MonetDB ODBC driver")
set(CPACK_RPM_odbc_PACKAGE_REQUIRES "%{name}-client%{?_isa} = %{version}-%{release}")
set(CPACK_RPM_odbc_PACKAGE_REQUIRES_POST "unixODBC")
set(CPACK_RPM_odbc_PACKAGE_REQUIRES_POSTUN "unixODBC")

set(CPACK_RPM_clienttest_PACKAGE_NAME "client-test")
set(CPACK_RPM_clienttest_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_clienttest_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_clienttest_PACKAGE_SUMMARY "MonetDB Client tests package")
set(CPACK_RPM_clienttest_PACKAGE_REQUIRES "MonetDB5-server%{?_isa} = %{version}-%{release}, %{name}-client%{?_isa} = %{version}-%{release}, %{name}-client-odbc%{?_isa} = %{version}-%{release}, %{name}-SQL-server5%{?_isa} = %{version}-%{release}, python3-pymonetdb >= 1.0.6")
set(CPACK_RPM_clienttest_PACKAGE_RECOMMENDS "perl-DBD-monetdb >= 1.0, php-monetdb >= 1.0")

set(CPACK_RPM_geom_PACKAGE_NAME "geom-MonetDB5")
set(CPACK_RPM_geom_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_geom_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_geom_PACKAGE_SUMMARY "MonetDB5 SQL GIS support module")
set(CPACK_RPM_geom_PACKAGE_REQUIRES "MonetDB5-server%{?_isa} = %{version}-%{release}")

set(CPACK_RPM_lidar_PACKAGE_NAME "lidar")
set(CPACK_RPM_lidar_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_lidar_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_lidar_PACKAGE_SUMMARY "MonetDB5 SQL support for working with LiDAR data")
set(CPACK_RPM_lidar_PACKAGE_REQUIRES "MonetDB5-server%{?_isa} = %{version}-%{release}")

set(CPACK_RPM_rapi_PACKAGE_NAME "R")
set(CPACK_RPM_rapi_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_rapi_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_rapi_PACKAGE_SUMMARY "Integration of MonetDB and R, allowing use of R from within SQL")
set(CPACK_RPM_rapi_PACKAGE_REQUIRES "MonetDB-SQL-server5%{?_isa} = %{version}-%{release}")

set(CPACK_RPM_pyapi3_PACKAGE_NAME "python3")
set(CPACK_RPM_pyapi3_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_pyapi3_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_pyapi3_PACKAGE_SUMMARY "Integration of MonetDB and Python, allowing use of Python from within SQL")
set(CPACK_RPM_pyapi3_PACKAGE_REQUIRES "MonetDB-SQL-server5%{?_isa} = %{version}-%{release}")

set(CPACK_RPM_fits_PACKAGE_NAME "cfitsio")
set(CPACK_RPM_fits_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_fits_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_fits_PACKAGE_SUMMARY "MonetDB: Add on module that provides support for FITS files")
set(CPACK_RPM_fits_PACKAGE_REQUIRES "MonetDB-SQL-server5%{?_isa} = %{version}-%{release}")

set(CPACK_RPM_server_PACKAGE_NAME "MonetDB5-server")
set(CPACK_RPM_server_FILE_NAME "${CPACK_RPM_server_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_server_PACKAGE_SUMMARY "MonetDB - Monet Database Management System")
set(CPACK_RPM_server_PACKAGE_REQUIRES "%{name}-client%{?_isa} = %{version}-%{release}")
# TODO: check for hugeint
set(CPACK_RPM_server_PACKAGE_RECOMMENDS "%{name}-SQL-server5%{?_isa} = %{version}-%{release}, MonetDB5-server-hugeint%{?_isa} = %{version}-%{release}")
set(CPACK_RPM_server_PACKAGE_SUGGESTS "%{name}-client%{?_isa} = %{version}-%{release}")
set(CPACK_RPM_server_PACKAGE_CONFLICTS "python-pymonetdb < 1.0.6")
# TODO: check for rhel
set(CPACK_RPM_server_PACKAGE_REQUIRES_PRE "shadow-utils, systemd")

set(CPACK_RPM_hugeint_PACKAGE_NAME "MonetDB5-server-hugeint")
set(CPACK_RPM_hugeint_FILE_NAME "${CPACK_RPM_hugeint_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_hugeint_PACKAGE_SUMMARY "MonetDB - 128-bit integer support for MonetDB5-server")
set(CPACK_RPM_hugeint_PACKAGE_REQUIRES "MonetDB5-server%{?_isa}")

set(CPACK_RPM_serverdev_PACKAGE_NAME "MonetDB5-server-devel")
set(CPACK_RPM_serverdev_FILE_NAME "${CPACK_RPM_serverdev_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_serverdev_PACKAGE_SUMMARY "MonetDB development files")
set(CPACK_RPM_serverdev_PACKAGE_REQUIRES "MonetDB5-server%{?_isa} = %{version}-%{release}, %{name}-devel%{?_isa} = %{version}-%{release}")

set(CPACK_RPM_sql_PACKAGE_NAME "SQL-server5")
set(CPACK_RPM_sql_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_sql_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_sql_PACKAGE_SUMMARY "MonetDB5 SQL server modules")
set(CPACK_RPM_sql_PACKAGE_REQUIRES_PRE "MonetDB5-server%{?_isa} = %{version}-%{release}")
# TODO: check hugeint
set(CPACK_RPM_sql_PACKAGE_RECOMMENDS "%{name}-SQL-server5-hugeint%{?_isa} = %{version}-%{release}")
set(CPACK_RPM_sql_PACKAGE_SUGGESTS "%{name}-client%{?_isa} = %{version}-%{release}")
# TODO: systemd_requires?

set(CPACK_RPM_sqlint128_PACKAGE_NAME "SQL-server5-hugeint")
set(CPACK_RPM_sqlint128_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_sqlint128_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_sqlint128_PACKAGE_SUMMARY "MonetDB5 128 bit integer (hugeint) support for SQL")
set(CPACK_RPM_sqlint128_PACKAGE_REQUIRES "MonetDB5-server-hugeint%{?_isa} = %{version}-%{release}, MonetDB-SQL-server5%{?_isa} = %{version}-%{release}")

set(CPACK_RPM_testing_PACKAGE_NAME "testing")
set(CPACK_RPM_testing_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_testing_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_TESTING_PACKAGE_SUMMARY "MonetDB - Monet Database Management System")

set(CPACK_RPM_pytesting_PACKAGE_NAME "testing-python")
set(CPACK_RPM_pytesting_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_pytesting_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_pytesting_PACKAGE_SUMMARY "MonetDB - Monet Database Management System")
set(CPACK_RPM_pytesting_PACKAGE_REQUIRES "%{name}-testing = %{version}-%{release}, %{name}-client-tests = %{version}-%{release}, /usr/bin/python3")
set(CPACK_RPM_pytesting_PACKAGE_ARCHITECTURE "noarch")

# TODO: detect rhel
set(CPACK_RPM_selinux_PACKAGE_NAME "selinux")
set(CPACK_RPM_selinux_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_selinux_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_selinux_PACKAGE_SUMMARY "SELinux policy files for MonetDB")
set(CPACK_RPM_selinux_PACKAGE_ARCHITECTURE "noarch")
set(CPACK_RPM_selinux_PACKAGE_REQUIRES_POST "MonetDB5-server%{?_isa} = %{version}-%{release}, %{name}-SQL-server5%{?_isa} = %{version}-%{release}, /usr/sbin/semodule, /sbin/restorecon, /sbin/fixfiles")
set(CPACK_RPM_selinux_PACKAGE_REQUIRES_POSTUN "MonetDB5-server%{?_isa} = %{version}-%{release}, %{name}-SQL-server5%{?_isa} = %{version}-%{release}, /usr/sbin/semodule, /sbin/restorecon, /sbin/fixfiles")
