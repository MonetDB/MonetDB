#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
set(CPACK_RPM_CHANGELOG_FILE "${CMAKE_SOURCE_DIR}/misc/packages/rpm/changelog")
set(CPACK_RPM_PACKAGE_RELOCATABLE OFF)

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
set(CPACK_RPM_server_PACKAGE_RECOMMENDS "%{name}-SQL-server5%{?_isa} = %{version}-%{release}")

set(CPACK_RPM_server_PACKAGE_SUGGESTS "%{name}-client%{?_isa} = %{version}-%{release}")
set(CPACK_RPM_server_PACKAGE_CONFLICTS "python-pymonetdb < 1.0.6")
# TODO: check for rhel
set(CPACK_RPM_server_PACKAGE_REQUIRES_PRE "shadow-utils, systemd")

set(CPACK_RPM_serverdev_PACKAGE_NAME "MonetDB5-server-devel")
set(CPACK_RPM_serverdev_FILE_NAME "${CPACK_RPM_serverdev_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_serverdev_PACKAGE_SUMMARY "MonetDB development files")
set(CPACK_RPM_serverdev_PACKAGE_REQUIRES "MonetDB5-server%{?_isa} = %{version}-%{release}, %{name}-devel%{?_isa} = %{version}-%{release}")

set(CPACK_RPM_sql_PACKAGE_NAME "SQL-server5")
set(CPACK_RPM_sql_FILE_NAME "${CMAKE_PROJECT_NAME}-${CPACK_RPM_sql_PACKAGE_NAME}-${MONETDB_VERSION}.rpm")
set(CPACK_RPM_sql_PACKAGE_SUMMARY "MonetDB5 SQL server modules")
set(CPACK_RPM_sql_PACKAGE_REQUIRES_PRE "MonetDB5-server%{?_isa} = %{version}-%{release}")
set(CPACK_RPM_sql_PACKAGE_SUGGESTS "%{name}-client%{?_isa} = %{version}-%{release}")
# TODO: systemd_requires?

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
set(CPACK_RPM_selinux_POST_INSTALL_SCRIPT_FILE "${CMAKE_BINARY_DIR}/misc/selinux/post_install_script_file")
set(CPACK_RPM_selinux_POST_UNINSTALL_SCRIPT_FILE "${CMAKE_BINARY_DIR}/misc/selinux/post_uninstall_script_file")

# Determine the build requires settings for the source build
# This add buildsrequirement based on the packages that are
# found on the machine that generates the packages. This might
# not be what we want when distributing the source rpm, so we
# might change this, or add the option to generate one based
# on the cmake options.
set(buildrequireslist
  "gcc"
  "bison"
  "/usr/bin/python3")

# RHEL >= 7, and all current Fedora
LIST(APPEND buildrequireslist
  "/usr/lib/rpm/macros.d/macros.systemd"
  "checkpolicy"
  "selinux-policy-devel"
  "hardlink")

if(BZIP2_FOUND)
  LIST(APPEND buildrequireslist "bzip2-devel")
endif()

if(CFITSIO_FOUND)
  LIST(APPEND buildrequireslist "pkgconfig(cfitsio)")
endif()

if(GEOS_FOUND)
  LIST(APPEND buildrequireslist "geos-devel > 3.4.0")
endif()

if(CURL_FOUND)
  LIST(APPEND buildrequireslist "pkgconfig(libcurl)")
endif()

if(LIBLZMA_FOUND)
  LIST(APPEND buildrequireslist "pkgconfig(liblzma)")
endif()

if(READLINE_FOUND)
  LIST(APPEND buildrequireslist "readline-devel")
endif()

if(ODBC_FOUND)
  LIST(APPEND buildrequireslist "unixODBC-devel")
endif()

if(ZLIB_FOUND)
  LIST(APPEND buildrequireslist "pkgconfig(zlib)")
endif()

if(PY3INTEGRATION)
  LIST(APPEND buildrequireslist "python3-devel >= 3.5")
  LIST(APPEND buildrequireslist "python3-numpy")
endif()

if(LIBR_FOUND)
  LIST(APPEND buildrequireslist "R-core-devel")
endif()

LIST(JOIN buildrequireslist ", " buildrequires)

set(CPACK_RPM_BUILDREQUIRES ${buildrequires})
