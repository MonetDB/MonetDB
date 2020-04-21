#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

set(CPACK_DEBIAN_PACKAGE_MAINTAINER "unknown")
set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_DEBIAN_PACKAGE_GENERATE_SHLIBS ON)

# Use 3 backslashes to make the variable a literal string
#set(CPACK_DEBIAN_PACKAGE_DEPENDS "\\\${misc:Depends}")
#set(CPACK_DEBIAN_PACKAGE_SECTION "misc")
#set(CPACK_COMPONENTS_IGNORE_GROUPS 1)

set(CPACK_DEBIAN_PACKAGE_monetdb_NAME "${CMAKE_PROJECT_NAME}-18")
set(CPACK_DEBIAN_PACKAGE_monetdb_ARCH "any")
set(CPACK_DEBIAN_PACKAGE_monetdb_VERSION "0.4.12")
set(CPACK_DEBIAN_PACKAGE_monetdb_CONFLICTS
  "libmonetdb5-server-geom (<< \\\${source:Version})")
set(CPACK_DEBIAN_PACKAGE_monetdb_DESRCIPTION "MonetDB core library")
set(CPACK_DEBIAN_PACKAGE_monetdb_MAINTAINER "${CPACK_DEBIAN_PACKAGE_MAINTAINER}")

set(CPACK_DEBIAN_PACKAGE_monetdbdev_NAME "${CMAKE_PROJECT_NAME}-monetdb-dev")
set(CPACK_DEBIAN_PACKAGE_monetdbev_ARCH "any")
set(CPACK_DEBIAN_PACKAGE_monetdbdev_VERSION "0.4.12")
set(CPACK_DEBIAN_PACKAGE_monetdbdev_DEPENDS "libmonetdb18, libmonetdb-stream-dev")
set(CPACK_DEBIAN_PACKAGE_monetdbdev_DESRCIPTION "MonetDB development files")
set(CPACK_DEBIAN_PACKAGE_streamdev_MAINTAINER "${CPACK_DEBIAN_PACKAGE_MAINTAINER}")

set(CPACK_DEBIAN_PACKAGE_stream_NAME "${CMAKE_PROJECT_NAME}-stream13")
set(CPACK_DEBIAN_PACKAGE_stream_ARCH "any")
set(CPACK_DEBIAN_PACKAGE_stream_VERSION "0.4.12")
set(CPACK_DEBIAN_PACKAGE_stream_DESRCIPTION "MonetDB stream library")
set(CPACK_DEBIAN_PACKAGE_stream_MAINTAINER "${CPACK_DEBIAN_PACKAGE_MAINTAINER}")

set(CPACK_DEBIAN_PACKAGE_streamdev_NAME "${CMAKE_PROJECT_NAME}-stream-dev")
set(CPACK_DEBIAN_PACKAGE_streamdev_ARCH "any")
set(CPACK_DEBIAN_PACKAGE_streamdev_VERSION "0.4.12")
set(CPACK_DEBIAN_PACKAGE_streamdev_DESRCIPTION "MonetDB stream library development files")
set(CPACK_DEBIAN_PACKAGE_streamdev_MAINTAINER "${CPACK_DEBIAN_PACKAGE_MAINTAINER}")

set(CPACK_DEBIAN_PACKAGE_client_NAME "${CMAKE_PROJECT_NAME}-client${MAPI_VERSION_MAJOR}")
set(CPACK_DEBIAN_PACKAGE_client_ARCH "any")
set(CPACK_DEBIAN_PACKAGE_client_VERSION "0.4.12")
set(CPACK_DEBIAN_PACKAGE_client_DESRCIPTION "MonetDB client/server interface library")
set(CPACK_DEBIAN_PACKAGE_client_MAINTAINER "${CPACK_DEBIAN_PACKAGE_MAINTAINER}")

set(CPACK_DEBIAN_PACKAGE_clientdev_NAME "${CMAKE_PROJECT_NAME}-client-dev")
set(CPACK_DEBIAN_PACKAGE_clientdev_ARCH "any")
set(CPACK_DEBIAN_PACKAGE_clientdev_VERSION "0.4.12")
set(CPACK_DEBIAN_PACKAGE_clientdev_DEPENDS "${CMAKE_PROJECT_NAME}-client${MAPI_VERSION_MAJOR} (= \\\${source:Version}")
set(CPACK_DEBIAN_PACKAGE_clientdev_DESRCIPTION "MonetDB client/server interface library development files")
set(CPACK_DEBIAN_PACKAGE_clientdev_MAINTAINER "${CPACK_DEBIAN_PACKAGE_MAINTAINER}")

set(CPACK_DEBIAN_PACKAGE_mclient_NAME "${CMAKE_PROJECT_NAME}-monetdb-client")
set(CPACK_DEBIAN_PACKAGE_mclient_ARCH "any")
set(CPACK_DEBIAN_PACKAGE_mclient_VERSION "0.4.12")
set(CPACK_DEBIAN_PACKAGE_mclient_RECOMMENDS "monetdb5-sql (= \\\${source:Version}")
set(CPACK_DEBIAN_PACKAGE_mclient_DESRCIPTION "MonetDB database client")
set(CPACK_DEBIAN_PACKAGE_mclient_MAINTAINER "${CPACK_DEBIAN_PACKAGE_MAINTAINER}")

set(CPACK_DEBIAN_TOOLS_PACKAGE_NAME "${CMAKE_PROJECT_NAME}-monetdb-client-tools")
set(CPACK_DEBIAN_TOOLS_PACKAGE_SECTION "misc")
set(CPACK_COMPONENT_TOOLS_DESCRIPTION "MonetDB database client")
set(CPACK_DEBIAN_PACKAGE_TOOLS_MAINTAINER "${CPACK_DEBIAN_PACKAGE_MAINTAINER}")

set(CPACK_DEBIAN_SERVER_PACKAGE_NAME "${CMAKE_PROJECT_NAME}-monetdb5-server")
set(CPACK_DEBIAN_SERVER_PACKAGE_SECTION "misc")
set(CPACK_DEBIAN_SERVER_PACKAGE_DEPENDS "\\\${misc:Depends}, adduser")
set(CPACK_DEBIAN_SERVER_PACKAGE_SUGGESTS "monetdb-client (= \\\${source:Version})")
set(CPACK_DEBIAN_SERVER_PACKAGE_CONFLICTS "python-pymonetdb (<< 1.0.6)")
set(CPACK_COMPONENT_SERVER_DESCRIPTION "MonetDB database server version 5")
set(CPACK_DEBIAN_SERVER_PACKAGE_MAINTAINER "${CPACK_DEBIAN_PACKAGE_MAINTAINER}")
set(CPACK_DEBIAN_SERVER_PACKAGE_CONTROL_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-server.dirs
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-server.docs
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-server.manpages
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-server.postinst
)

#set(CPACK_DEBIAN_SERVERDEV_PACKAGE_NAME "${CMAKE_PROJECT_NAME}-monetdb5-server")
#set(CPACK_DEBIAN_SERVERDEV_PACKAGE_SECTION "misc")
#set(CPACK_DEBIAN_SERVERDEV_PACKAGE_DEPENDS
#  "${shlibs:Depends}"
#  "${misc:Depends}"
#  "monetdb5-server (= ${source:Version})"
#  "libmonetdb-dev (= ${source:Version})")
#set(CPACK_COMPONENT_SERVERDEV_DESCRIPTION "MonetDB database server version 5")
#set(CPACK_DEBIAN_SERVERDEV_PACKAGE_MAINTAINER
#  "${CPACK_DEBIAN_PACKAGE_MAINTAINER}")

set(CPACK_DEBIAN_SQL_PACKAGE_NAME "${CMAKE_PROJECT_NAME}-monetdb5-sql")
set(CPACK_DEBIAN_SQL_PACKAGE_SECTION "misc")
set(CPACK_DEBIAN_SQL_PACKAGE_DEPENDS
  "\\\${shlibs:Depends}"
  "\\\${misc:Depends}"
  "monetdb5-server (= \\\${source:Version})")
set(CPACK_DEBIAN_SQL_PACKAGE_RECOMMENDS
  "monetdb5-sql-hugeint (= \\\${source:Version}) [amd64]")
set(CPACK_COMPONENT_SQL_DESCRIPTION "MonetDB SQL support for monetdb5")
set(CPACK_DEBIAN_SQL_PACKAGE_MAINTAINER
  "${CPACK_DEBIAN_PACKAGE_MAINTAINER}")
set(CPACK_DEBIAN_SQL_PACKAGE_CONTROL_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-sql.default
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-sql.dirs
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-sql.doc
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-sql.manpages
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-sql.postinst
)

