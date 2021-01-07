#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

set(CPACK_DEBIAN_PACKAGE_MAINTAINER "MonetDB BV <info@monetdb.org>")
set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_DEBIAN_PACKAGE_GENERATE_SHLIBS OFF)
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_DEBIAN_PACKAGE_SECTION "misc")
set(CPACK_DEBIAN_PACKAGE_SOURCE "monetdb")
# set(CPACK_DEBIAN_PACKAGE_DEPENDS "")

set(CPACK_DEBIAN_MONETDB_PACKAGE_NAME "libmonetdb${GDK_VERSION_MAJOR}")
set(CPACK_DEBIAN_MONETDB_PACKAGE_DEPENDS "\\\${shlibs:Depends}, \\\${misc:Depends}")
set(CPACK_DEBIAN_MONETDB_PACKAGE_CONFLICTS
  "libmonetdb5-server-geom (<< \\\${source:Version})")
set(CPACK_COMPONENT_MONETDB_DESCRIPTION "MonetDB core library
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains the core components of MonetDB in the form of a
 single shared library.  If you want to use MonetDB, you will certainly
 need this package, but you will also need at least the monetdb5-server
 package, and most likely also monetdb5-sql, as well as one or
 more client packages.")

set(CPACK_DEBIAN_MONETDBDEV_PACKAGE_NAME "libmonetdb-dev")
set(CPACK_DEBIAN_MONETDBDEV_PACKAGE_DEPENDS "\\\${shlibs:Depends}, \\\${misc:Depends}, libmonetdb${GDK_VERSION_MAJOR}, libmonetdb-stream-dev")
set(CPACK_COMPONENT_MONETDBDEV_DESCRIPTION "MonetDB development files
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains files needed to develop extensions to the core
 functionality of MonetDB.")

set(CPACK_DEBIAN_STREAM_PACKAGE_NAME "libmonetdb-stream${STREAM_VERSION_MAJOR}")
set(CPACK_DEBIAN_STREAM_PACKAGE_DEPENDS "\\\${shlibs:Depends}, \\\${misc:Depends}")
set(CPACK_COMPONENT_STREAM_DESCRIPTION "MonetDB stream library
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains a shared library (libstream) which is needed by
 various other components.")

set(CPACK_DEBIAN_STREAMDEV_PACKAGE_NAME "libmonetdb-stream-dev")
set(CPACK_DEBIAN_STREAMDEV_PACKAGE_DEPENDS "\\\${shlibs:Depends}, \\\${misc:Depends}, libmonetdb-stream${STREAM_VERSION_MAJOR} (= \\\${source:Version}")
set(CPACK_COMPONENT_STREAMDEV_DESCRIPTION "MonetDB stream library development files
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains the files to develop with the
 libmonetdb-stream${STREAM_VERSION_MAJOR} library.")

set(CPACK_DEBIAN_CLIENT_PACKAGE_NAME "libmonetdb-client${MAPI_VERSION_MAJOR}")
set(CPACK_DEBIAN_CLIENT_PACKAGE_DEPENDS "\\\${shlibs:Depends}, \\\${misc:Depends}")
set(CPACK_COMPONENT_CLIENT_DESCRIPTION "MonetDB client/server interface library
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains a shared library (libmapi) which is needed by
 various other components.")

set(CPACK_DEBIAN_CLIENTDEV_PACKAGE_NAME "libmonetdb-client-dev")
set(CPACK_DEBIAN_CLIENTDEV_PACKAGE_DEPENDS "\\\${shlibs:Depends}, \\\${misc:Depends}, libmonetdb-client${MAPI_VERSION_MAJOR} (= \\\${source:Version}")
set(CPACK_COMPONENT_CLIENTDEV_DESCRIPTION "MonetDB client/server interface library development files
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains the files to develop with the libmonetdb-client${MAPI_VERSION_MAJOR}
 library.")

set(CPACK_DEBIAN_MCLIENT_PACKAGE_NAME "monetdb-client")
set(CPACK_DEBIAN_MCLIENT_PACKAGE_RECOMMENDS "monetdb5-sql (= \\\${source:Version}")
set(CPACK_DEBIAN_MCLIENT_PACKAGE_DEPENDS "\\\${shlibs:Depends}, \\\${misc:Depends}")
set(CPACK_COMPONENT_MCLIENT_DESCRIPTION "MonetDB database client
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains mclient, the main client program to communicate
 with the MonetDB database server, and msqldump, a program to dump the
 SQL database so that it can be loaded back later.  If you want to use
 MonetDB, you will very likely need this package.")

set(CPACK_DEBIAN_ODBC_PACKAGE_NAME "libmonetdb-client-odbc")
set(CPACK_DEBIAN_ODBC_PACKAGE_DEPENDS "\\\${shlibs:Depends}\\\${misc:Depends}")
set(CPACK_COMPONENT_ODBC_DESCRIPTION "MonetDB ODBC driver
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains the MonetDB ODBC driver.")

set(CPACK_DEBIAN_CLIENTTEST_PACKAGE_NAME "monetdb-client-testing")
set(CPACK_DEBIAN_CLIENTTEST_PACKAGE_DEPENDS "\\\${shlibs:Depends} ,libmonetdb-client-odbc (= \\\${source:Version}),
 monetdb5-server (= \\\${source:Version}),
 libdbd-monetdb-perl (>= 1.0),
 php-monetdb (>= 1.0),
 python3-pymonetdb (>= 1.0.6),
 monetdb5-sql (= \\\${source:Version})")
set(CPACK_COMPONENT_CLIENTTEST_DESCRIPTION "MonetDB client testing tools
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains the sample MAPI programs used for testing other
 MonetDB packages.  You probably don't need this, unless you are a
 developer.")

set(CPACK_DEBIAN_GEOM_PACKAGE_NAME "libmonetdb5-server-geom")
set(CPACK_DEBIAN_GEOM_PACKAGE_DEPENDS "\\\${shlibs:Depends}\\\${misc:Depends}, monetdb5-sql (= \\\${source:Version}")
set(CPACK_DEBIAN_GEOM_PACKAGE_SECTION "libs")
set(CPACK_COMPONENT_GEOM_DESCRIPTION "MonetDB5 SQL GIS support module
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains the GIS (Geographic Information System)
 extensions for MonetDB-SQL-server5.")

set(CPACK_DEBIAN_FITS_PACKAGE_NAME "libmonetdb5-server-cfitsio")
set(CPACK_DEBIAN_FITS_PACKAGE_DEPENDS "\\\${shlibs:Depends}, \\\${misc:Depends}, monetdb5-sql (= \\\${source:Version}")
set(CPACK_DEBIAN_FITS_PACKAGE_SECTION "libs")
set(CPACK_COMPONENT_FITS_DESCRIPTION "MonetDB5 SQL GIS support module
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains a module for accessing data in the FITS file
 format.")

set(CPACK_DEBIAN_SERVER_PACKAGE_NAME "monetdb5-server")
#set(CPACK_DEBIAN_SERVER_PACKAGE_DEPENDS "\\\${misc:Depends}, adduser")
set(CPACK_DEBIAN_SERVER_PACKAGE_DEPENDS "\\\${shlibs:Depends}, \\\${misc:Depends}, adduser")
set(CPACK_DEBIAN_SERVER_PACKAGE_SUGGESTS "monetdb-client (= \\\${source:Version})")
set(CPACK_DEBIAN_SERVER_PACKAGE_CONFLICTS "python-pymonetdb (<< 1.0.6)")
set(CPACK_COMPONENT_SERVER_DESCRIPTION "MonetDB database server version 5
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains the MonetDB server component.  You need this
 package if you want to use the MonetDB database system.  If you want
 to use the SQL front end, you also need monetdb5-sql.")
set(CPACK_DEBIAN_SERVER_PACKAGE_CONTROL_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-server.dirs
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-server.docs
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-server.manpages
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-server.postinst
)

set(CPACK_DEBIAN_SERVERDEV_PACKAGE_NAME "monetdb5-server-dev")
set(CPACK_DEBIAN_SERVERDEV_PACKAGE_DEPENDS "\\\${shlibs:Depends}, \\\${misc:Depends},
 monetdb5-server (= \\\${source:Version}), libmonetdb-dev (= \\\${source:Version})")
set(CPACK_COMPONENT_SERVERDEV_DESCRIPTION "MonetDB database server version 5
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains files needed to develop extensions that can be
 used from the MAL level.")

set(CPACK_DEBIAN_SQL_PACKAGE_NAME "monetdb5-sql")
set(CPACK_DEBIAN_SQL_PACKAGE_DEPENDS
  "\\\${shlibs:Depends}"
  "\\\${misc:Depends}"
  "monetdb5-server (= \\\${source:Version})")
set(CPACK_DEBIAN_SQL_PACKAGE_SUGGEST "monetdb-client (= \\\${source:Version})")
set(CPACK_COMPONENT_SQL_DESCRIPTION "MonetDB SQL support for monetdb5
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains the SQL front end for MonetDB.")
set(CPACK_DEBIAN_SQL_PACKAGE_CONTROL_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-sql.default
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-sql.dirs
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-sql.doc
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-sql.manpages
  ${CMAKE_CURRENT_SOURCE_DIR}/debian/monetdb5-sql.postinst
  )

set(CPACK_DEBIAN_PYAPI3_PACKAGE_NAME "monetdb-python3")
set(CPACK_DEBIAN_PYAPI3_PACKAGE_DEPENDS
  "\\\${shlibs:Depends}"
  "\\\${misc:Depends}"
  "monetdb5-sql (= \\\${source:Version})"
  "python3-numpy")
set(CPACK_COMPONENT_PYAPI3_DESCRIPTION "Integration of MonetDB and Python, allowing use of Python from within SQL
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains the interface to use the Python language from
 within SQL queries.  This package is for Python 3.
 .
 NOTE: INSTALLING THIS PACKAGE OPENS UP SECURITY ISSUES.  If you don't
 know how this package affects the security of your system, do not
 install it.")

set(CPACK_DEBIAN_RAPI_PACKAGE_NAME "monetdb-r")
set(CPACK_DEBIAN_RAPI_PACKAGE_DEPENDS
  "\\\${shlibs:Depends}"
  "\\\${misc:Depends}"
  "monetdb5-sql (= \\\${source:Version})"
  "r-base-core")
set(CPACK_COMPONENT_RAPI_DESCRIPTION "Integration of MonetDB and R, allowing use of R from within SQL
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains the interface to use the R language from within
 SQL queries.
 .
 NOTE: INSTALLING THIS PACKAGE OPENS UP SECURITY ISSUES.  If you don't
 know how this package affects the security of your system, do not
 install it.")

set(CPACK_DEBIAN_TESTING_PACKAGE_NAME "monetdb-testing")
set(CPACK_DEBIAN_TESTING_PACKAGE_DEPENDS
  "\\\${shlibs:Depends}"
  "\\\${misc:Depends}")
set(CPACK_COMPONENT_TESTING_DESCRIPTION "MonetDB testing programs
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains the programs and files needed for testing the
 MonetDB packages.  You probably don't need this, unless you are a
 developer.  If you do want to test, install monetdb-testing-python.")

set(CPACK_DEBIAN_PYTESTING_PACKAGE_NAME "monetdb-testing-python")
set(CPACK_DEBIAN_PYTESTING_PACKAGE_DEPENDS
  "\\\${shlibs:Depends}"
  "\\\${misc:Depends}"
  "python3"
  "monetdb-testing (= \\\${source:Version})"
  "monetdb-client-testing (= \\\${source:Version}")
set(CPACK_COMPONENT_PYTESTING_DESCRIPTION "MonetDB testing Python programs
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains the Python programs and files needed for
 testing the MonetDB packages.  You probably don't need this, unless
 you are a developer, but if you do want to test, this is the package
 you need.")

set(CPACK_DEBIAN_DEBUG_PACKAGE_NAME "monetdb-dbg")
set(CPACK_DEBIAN_DEBUG_PACKAGE_SECTION "debug")
set(CPACK_DEBIAN_DEBUG_PACKAGE_PRIORITY "optional")
set(CPACK_DEBIAN_DEBUG_PACKAGE_DEPENDS "\\\${misc:Depends}")
set(CPACK_COMPONENT_PYTESTING_DESCRIPTION " Debugging symbols for monetdb packages
 MonetDB is a database management system that is developed from a
 main-memory perspective with use of a fully decomposed storage model,
 automatic index management, extensibility of data types and search
 accelerators.  It also has an SQL front end.
 .
 This package contains the debugging symbols for all monetdb binary
 packages.")
