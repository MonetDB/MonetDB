# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.

%global version 11.55.2

%bcond_with compat

%global name MonetDB%{?with_compat:%version}

%{!?buildno: %global buildno %(date +%Y%m%d)}

# Use bcond_with to add a --with option; i.e., "without" is default.
# Use bcond_without to add a --without option; i.e., "with" is default.
# The --with OPTION and --without OPTION arguments can be passed on
# the commandline of both rpmbuild and mock.

# On 64 bit architectures compile with 128 bit integer support.
%if "%{?_lib}" == "lib64"
%bcond_without hugeint
%endif

%global release %{buildno}%{?dist}

# This package contains monetdbd which is a (long running) daemon, so
# we need to harden:
%global _hardened_build 1

# On RedHat Enterprise Linux and derivatives, if the Extra Packages
# for Enterprise Linux (EPEL) repository is not available, you can
# disable its use by providing rpmbuild or mock with the "--without
# epel" option.
# If the EPEL repository is available, or if building for Fedora, most
# optional sub packages can be built.  We indicate that here by
# setting the macro fedpkgs to 1.  If the EPEL repository is not
# available and we are not building for Fedora, we set fedpkgs to 0.
%if %{?rhel:1}%{!?rhel:0}
# RedHat Enterprise Linux (or CentOS or Scientific Linux)
%bcond_without epel
%if %{with epel}
# EPEL is enabled through the command line
%global fedpkgs 1
%else
# EPEL is not enabled
%global fedpkgs 0
%endif
%else
# Not RHEL (so presumably Fedora)
%global fedpkgs 1
%endif

# On Fedora, the geos library is available, and so we can require it
# and build the geom modules.  On RedHat Enterprise Linux and
# derivatives (CentOS, Scientific Linux), the geos library is not
# available.  However, the geos library is available in the Extra
# Packages for Enterprise Linux (EPEL).
%if %{fedpkgs} && (0%{?rhel} != 7) && (0%{?rhel} != 8) && (0%{?rhel} != 9)
# By default create the MonetDB-geom package on Fedora and RHEL 7
%bcond_without geos
%endif

# By default use PCRE for the implementation of the SQL LIKE and ILIKE
# operators.  Otherwise the POSIX regex functions are used.
%bcond_without pcre

# By default, include C integration
%bcond_without cintegration

%if %{fedpkgs}
# By default, create the MonetDB-R package.
%bcond_without rintegration
%endif

# By default, include Python 3 integration.
%bcond_without py3integration

%if %{fedpkgs}
# By default, create the MonetDB-cfitsio package.
%bcond_without fits
%endif

Name: %{name}
Version: %{version}
Release: %{release}
Summary: Monet Database Management System
Vendor: MonetDB Foundation <info@monetdb.org>

Group: Applications/Databases
License: MPL-2.0
URL: https://www.monetdb.org/
BugURL: https://github.com/MonetDB/MonetDB/issues
Source: https://www.monetdb.org/downloads/sources/Dec2025/MonetDB-%{version}.tar.bz2

# The Fedora packaging document says we need systemd-rpm-macros for
# the _unitdir and _tmpfilesdir macros to exist; however on RHEL 7
# that doesn't exist and we need systemd, so instead we just require
# the macro file that contains the definitions.
# We need checkpolicy and selinux-policy-devel for the SELinux policy.
%if 0%{?rhel} != 7
BuildRequires: systemd-rpm-macros
%else
BuildRequires: systemd
%endif
BuildRequires: checkpolicy
BuildRequires: selinux-policy-devel
BuildRequires: hardlink
BuildRequires: cmake3 >= 3.12
BuildRequires: gcc
BuildRequires: bison
BuildRequires: python3-devel
%if %{?rhel:1}%{!?rhel:0}
# RH 7 (and for readline also 8)
BuildRequires: bzip2-devel
BuildRequires: unixODBC-devel
BuildRequires: readline-devel
%else
BuildRequires: pkgconfig(bzip2)
%if %{without compat}
BuildRequires: pkgconfig(odbc)
%endif
BuildRequires: pkgconfig(readline)
%endif
%if %{with fits}
BuildRequires: pkgconfig(cfitsio)
%endif
%if %{with geos}
BuildRequires: geos-devel >= 3.10.0
%endif
BuildRequires: pkgconfig(libcurl)
BuildRequires: pkgconfig(liblzma)
BuildRequires: pkgconfig(libxml-2.0)
%if 0%{?rhel} != 7
BuildRequires: pkgconfig(openssl) >= 1.1.1
%global with_openssl 1
%endif
%if %{with pcre}
BuildRequires: pkgconfig(libpcre2-8)
%endif
BuildRequires: pkgconfig(zlib)
BuildRequires: pkgconfig(liblz4) >= 1.8
%if %{with py3integration}
BuildRequires: pkgconfig(python3) >= 3.5
# cannot use python3dist(numpy) because of CentOS 7
BuildRequires: python3-numpy
%endif
%if %{with rintegration}
BuildRequires: pkgconfig(libR)
%endif
# optional packages:
# BuildRequires: pkgconfig(cmocka)      # -DWITH_CMOCKA=ON
# BuildRequires: pkgconfig(gdal)        # -DSHP=ON
# BuildRequires: pkgconfig(netcdf)      # -DNETCDF=ON
# BuildRequires: pkgconfig(proj)        # -DWITH_PROJ=ON
# BuildRequires: pkgconfig(valgrind)    # -DWITH_VALGRIND=ON

%if (0%{?fedora} >= 22)
Recommends: %{name}-SQL%{?_isa} = %{version}-%{release}
Recommends: %{name}-server%{?_isa} = %{version}-%{release}
Suggests: %{name}-client%{?_isa} = %{version}-%{release}
%endif

%description
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the core components of MonetDB in the form of a
single shared library.  If you want to use MonetDB, you will certainly
need this package, but you will also need at least the %{name}-server
package, and most likely also %{name}-SQL, as well as one or
more client packages.

%ldconfig_scriptlets

%files
%license COPYING
%defattr(-,root,root)
%{_libdir}/libbat*.so.*

%if %{without compat}
%package devel
Summary: MonetDB development files
Group: Applications/Databases
Requires: %{name}%{?_isa} = %{version}-%{release}
Requires: %{name}-stream-devel%{?_isa} = %{version}-%{release}

%description devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains files needed to develop extensions to the core
functionality of MonetDB.

%files devel
%defattr(-,root,root)
%dir %{_includedir}/monetdb
%{_includedir}/monetdb/copybinary.h
%{_includedir}/monetdb/gdk*.h
%{_includedir}/monetdb/matomic.h
%{_includedir}/monetdb/mstring.h
%exclude %{_includedir}/monetdb/monetdbe.h
%{_includedir}/monetdb/monet*.h
%{_libdir}/libbat*.so
%{_libdir}/pkgconfig/monetdb-gdk.pc
%dir %{_datadir}/monetdb
%dir %{_datadir}/monetdb/cmake
%{_datadir}/monetdb/cmake/gdkTargets*.cmake
%{_datadir}/monetdb/cmake/matomicTargets.cmake
%{_datadir}/monetdb/cmake/mstringTargets.cmake
%{_datadir}/monetdb/cmake/monetdb_config_headerTargets.cmake
%endif

%package mutils
Summary: MonetDB mutils library
Group: Applications/Databases

%description mutils
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains a shared library (libmutils) which is needed by
various other components.

%ldconfig_scriptlets mutils

%files mutils
%license COPYING
%defattr(-,root,root)
%{_libdir}/libmutils*.so.*

%if %{without compat}
%package mutils-devel
Summary: MonetDB mutils library
Group: Applications/Databases
Requires: %{name}-mutils%{?_isa} = %{version}-%{release}

%description mutils-devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the files to develop with the %{name}-mutils
library.

%files mutils-devel
%defattr(-,root,root)
%dir %{_includedir}/monetdb
%{_libdir}/libmutils*.so
%{_libdir}/pkgconfig/monetdb-mutils.pc
%{_datadir}/monetdb/cmake/mutilsTargets*.cmake
%endif

%package stream
Summary: MonetDB stream library
Group: Applications/Databases

%description stream
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains a shared library (libstream) which is needed by
various other components.

%ldconfig_scriptlets stream

%files stream
%license COPYING
%defattr(-,root,root)
%{_libdir}/libstream*.so.*

%if %{without compat}
%package stream-devel
Summary: MonetDB stream library
Group: Applications/Databases
Requires: %{name}-stream%{?_isa} = %{version}-%{release}
Requires: %{name}-mutils-devel%{?_isa} = %{version}-%{release}
Requires: bzip2-devel
Requires: libcurl-devel
Requires: zlib-devel

%description stream-devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the files to develop with the %{name}-stream
library.

%files stream-devel
%defattr(-,root,root)
%dir %{_includedir}/monetdb
%{_libdir}/libstream*.so
%{_includedir}/monetdb/stream.h
%{_includedir}/monetdb/stream_socket.h
%{_libdir}/pkgconfig/monetdb-stream.pc
%{_datadir}/monetdb/cmake/streamTargets*.cmake
%endif

%package client-lib
Summary: MonetDB - Monet Database Management System Client Programs
Group: Applications/Databases
%if (0%{?fedora} >= 22)
Recommends: %{name}-SQL%{?_isa} = %{version}-%{release}
Recommends: %{name}-server%{?_isa} = %{version}-%{release}
%endif

%description client-lib
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains libmapi.so, the main client library used by both
mclient, msqldump and by the ODBC driver.  If you want to use MonetDB,
you will very likely need this package.

%ldconfig_scriptlets client-lib

%files client-lib
%license COPYING
%defattr(-,root,root)
%{_libdir}/libmapi*.so.*

%package client
Summary: MonetDB - Monet Database Management System Client Programs
Group: Applications/Databases
Requires: %{name}-client-lib%{?_isa} = %{version}-%{release}
%if (0%{?fedora} >= 22)
Recommends: %{name}-SQL%{?_isa} = %{version}-%{release}
Recommends: %{name}-server%{?_isa} = %{version}-%{release}
%endif

%description client
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains mclient, the main client program to communicate
with the MonetDB database server, and msqldump, a program to dump the
SQL database so that it can be loaded back later.  If you want to use
MonetDB, you will very likely need this package.

%files client
%license COPYING
%defattr(-,root,root)
%{_bindir}/mclient*
%{_bindir}/msqldump*
%if %{without compat}
%{_mandir}/man1/mclient.1*
%{_mandir}/man1/msqldump.1*
%endif

%if %{without compat}
%package client-devel
Summary: MonetDB - Monet Database Management System Client Programs
Group: Applications/Databases
Requires: %{name}-client-lib%{?_isa} = %{version}-%{release}
Requires: %{name}-stream-devel%{?_isa} = %{version}-%{release}

%description client-devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the files needed to develop with the
%{name}-client package.

%files client-devel
%defattr(-,root,root)
%dir %{_includedir}/monetdb
%{_libdir}/libmapi*.so
%{_includedir}/monetdb/mapi*.h
%{_includedir}/monetdb/msettings.h
%{_libdir}/pkgconfig/monetdb-mapi.pc
%{_datadir}/monetdb/cmake/mapiTargets*.cmake
%endif

%if %{without compat}
%package client-odbc
Summary: MonetDB ODBC driver
Group: Applications/Databases
Requires: %{name}-client-lib%{?_isa} = %{version}-%{release}
Requires(post): %{_bindir}/odbcinst
Requires(postun): %{_bindir}/odbcinst

%description client-odbc
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the MonetDB ODBC driver.

%post client-odbc
# install driver if first install of package or if driver not installed yet
if [ "$1" -eq 1 ] || ! odbcinst -d -q -n MonetDB >& /dev/null; then
odbcinst -i -d -r <<EOF
[MonetDB]
Description = ODBC for MonetDB
Driver = %{_exec_prefix}/lib/libMonetODBC.so
Setup = %{_exec_prefix}/lib/libMonetODBCs.so
Driver64 = %{_exec_prefix}/lib64/libMonetODBC.so
Setup64 = %{_exec_prefix}/lib64/libMonetODBCs.so
EOF
fi

%postun client-odbc
if [ "$1" -eq 0 ]; then
odbcinst -u -d -n MonetDB
fi

%files client-odbc
%license COPYING
%defattr(-,root,root)
%{_libdir}/libMonetODBC.so
%{_libdir}/libMonetODBCs.so
%endif

%if %{without compat}
%package client-tests
Summary: MonetDB Client tests package
Group: Applications/Databases
Requires: %{name}-server%{?_isa} = %{version}-%{release}
Requires: %{name}-client%{?_isa} = %{version}-%{release}
Requires: %{name}-client-odbc%{?_isa} = %{version}-%{release}
%if (0%{?fedora} >= 22)
Recommends: perl-DBD-monetdb >= 1.0
Recommends: php-monetdb >= 1.0
%endif
Requires: %{name}-server%{?_isa} = %{version}-%{release}
%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} > 7
Recommends: python3dist(lz4)
Recommends: python3dist(scipy)
%endif

%description client-tests
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the sample MAPI programs used for testing other
MonetDB packages.  You probably don't need this, unless you are a
developer.

%files client-tests
%defattr(-,root,root)
%{_bindir}/arraytest
%{_bindir}/backrefencode
%{_bindir}/bincopydata
%{_bindir}/malsample.pl
%{_bindir}/murltest
%{_bindir}/odbcconnect
%{_bindir}/ODBCgetInfo
%{_bindir}/ODBCmetadata
%{_bindir}/odbcsample1
%{_bindir}/ODBCStmtAttr
%{_bindir}/ODBCtester
%{_bindir}/sample0
%{_bindir}/sample1
%{_bindir}/sample4
%{_bindir}/shutdowntest
%{_bindir}/smack00
%{_bindir}/smack01
%{_bindir}/sqlsample.php
%{_bindir}/sqlsample.pl
%{_bindir}/streamcat
%{_bindir}/testcondvar
%endif

%if %{with geos}
%package geom
Summary: SQL GIS support module for MonetDB
Group: Applications/Databases
Requires: %{name}-server%{?_isa} = %{version}-%{release}
Obsoletes: MonetDB-geom-MonetDB5 < 11.50.0
Provides: %{name}-geom-MonetDB5 = %{version}-%{release}
Provides: %{name}-geom-MonetDB5%{?_isa} = %{version}-%{release}

%description geom
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the GIS (Geographic Information System)
extensions for %{name}-server.

%files geom
%defattr(-,root,root)
%{_libdir}/monetdb5*/lib_geom.so
%endif

%if %{with rintegration}
%package R
Summary: Integration of MonetDB and R, allowing use of R from within SQL
Group: Applications/Databases
Requires: %{name}-server%{?_isa} = %{version}-%{release}

%description R
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the interface to use the R language from within
SQL queries.

NOTE: INSTALLING THIS PACKAGE OPENS UP SECURITY ISSUES.  If you don't
know how this package affects the security of your system, do not
install it.

%files R
%defattr(-,root,root)
%{_libdir}/monetdb5*/rapi.R
%{_libdir}/monetdb5*/lib_rapi.so
%endif

%if %{with py3integration}
%package python3
Summary: Integration of MonetDB and Python, allowing use of Python from within SQL
Group: Applications/Databases
Requires: %{name}-server%{?_isa} = %{version}-%{release}
Requires: python3-numpy

%description python3
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the interface to use the Python language from
within SQL queries.  This package is for Python 3.

NOTE: INSTALLING THIS PACKAGE OPENS UP SECURITY ISSUES.  If you don't
know how this package affects the security of your system, do not
install it.

%files python3
%defattr(-,root,root)
%{_libdir}/monetdb5*/lib_pyapi3.so
%endif

%if %{with fits}
%package cfitsio
Summary: MonetDB: Add on module that provides support for FITS files
Group: Applications/Databases
Requires: %{name}-server%{?_isa} = %{version}-%{release}

%description cfitsio
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains a module for accessing data in the FITS file
format.

%files cfitsio
%defattr(-,root,root)
%{_libdir}/monetdb5*/lib_fits.so
%endif

%package libs
Summary: MonetDB - Monet Database Main Libraries
Group: Applications/Databases
Obsoletes: MonetDB5-libs < 11.50.0
Provides: MonetDB5-libs = %{version}-%{release}
Provides: MonetDB5-libs%{?_isa} = %{version}-%{release}

%description libs
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the MonetDB server component in the form of a set
of libraries.  You need this package if you want to use the MonetDB
database system, either as independent program (%{name}-server) or as
embedded library (%{name}-embedded).

%ldconfig_scriptlets libs

%files libs
%defattr(-,root,root)
%{_libdir}/libmonetdb5*.so.*
%{_libdir}/libmonetdbsql*.so*
%dir %{_libdir}/monetdb5-%{version}
%if %{with cintegration}
%{_libdir}/monetdb5*/lib_capi.so
%endif
%{_libdir}/monetdb5*/lib_csv.so
%{_libdir}/monetdb5*/lib_generator.so
%{_libdir}/monetdb5*/lib_monetdb_loader.so

%package odbc-loader
Summary: MonetDB ODBC loader module
Group: Applications/Databases
Requires: %{name}-server%{?_isa} = %{version}-%{release}

%description odbc-loader
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package provides an interface to the MonetDB server through which
data from remote databases can be loaded through an ODBC interface.  In
order to use this module, mserver5 needs to be run with the option
--loadmodule odbc_loader.

%files odbc-loader
%defattr(-,root,root)
%{_libdir}/monetdb5*/lib_odbc_loader.so

%package server
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Requires(pre): shadow-utils
Requires: %{name}-client%{?_isa} = %{version}-%{release}
Requires: %{name}-libs%{?_isa} = %{version}-%{release}
Obsoletes: MonetDB5-server < 11.50.0
Provides: MonetDB5-server = %{version}-%{release}
Provides: MonetDB5-server%{?_isa} = %{version}-%{release}
%if (0%{?fedora} >= 22)
Recommends: %{name}-SQL%{?_isa} = %{version}-%{release}
Suggests: %{name}-client%{?_isa} = %{version}-%{release}
%endif
Requires(pre): systemd

%description server
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the MonetDB server component.  You need this
package if you want to use the MonetDB database system.  If you want to
use the monetdb and monetdbd programs to manage your databases
(recommended), you also need %{name}-SQL.

%pre server
%{?sysusers_create_package:echo 'u monetdb - "MonetDB Server" /var/lib/monetdb' | systemd-sysusers --replace=%_sysusersdir/monetdb.conf -}

getent group monetdb >/dev/null || groupadd --system monetdb
if getent passwd monetdb >/dev/null; then
    case $(getent passwd monetdb | cut -d: -f6) in
    %{_localstatedir}/MonetDB) # old value
        # change home directory, but not using usermod
        # usermod requires there to not be any running processes owned by the user
        EDITOR='sed -i "/^monetdb:/s|:%{_localstatedir}/MonetDB:|:%{_localstatedir}/lib/monetdb:|"'
        unset VISUAL
        export EDITOR
        /sbin/vipw > /dev/null
        ;;
    esac
else
    useradd --system --gid monetdb --home-dir %{_localstatedir}/lib/monetdb \
        --shell /sbin/nologin --comment "MonetDB Server" monetdb
fi
exit 0

%files server
%defattr(-,root,root)
%if %{without compat}
%{_sysusersdir}/monetdb.conf
%attr(2750,monetdb,monetdb) %dir %{_localstatedir}/lib/monetdb
%attr(2770,monetdb,monetdb) %dir %{_localstatedir}/monetdb5
%attr(2770,monetdb,monetdb) %dir %{_localstatedir}/monetdb5/dbfarm
%endif
%{_bindir}/mserver5*
%if %{without compat}
%{_mandir}/man1/mserver5.1*
%dir %{_datadir}/doc/MonetDB
%docdir %{_datadir}/doc/MonetDB
%{_datadir}/doc/MonetDB/*
%endif

%if %{without compat}
%package server-devel
Summary: MonetDB development files
Group: Applications/Databases
Requires: %{name}-libs%{?_isa} = %{version}-%{release}
Requires: %{name}-devel%{?_isa} = %{version}-%{release}
Obsoletes: MonetDB5-server-devel < 11.50.0
Provides: MonetDB5-server-devel = %{version}-%{release}
Provides: MonetDB5-server-devel%{?_isa} = %{version}-%{release}

%description server-devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains files needed to develop extensions that can be
used from the MAL level.

%files server-devel
%defattr(-,root,root)
%{_includedir}/monetdb/mal*.h
%{_includedir}/monetdb/mel.h
%{_libdir}/libmonetdb5*.so
%{_libdir}/pkgconfig/monetdb5.pc
%{_datadir}/monetdb/cmake/monetdb5Targets*.cmake
%endif

%package SQL
Summary: MonetDB SQL server modules
Group: Applications/Databases
Requires(pre): %{name}-server%{?_isa} = %{version}-%{release}
Obsoletes: MonetDB-SQL-server5 < 11.50.0
Provides: %{name}-SQL-server5 = %{version}-%{release}
Provides: %{name}-SQL-server5%{?_isa} = %{version}-%{release}
%if (0%{?fedora} >= 22)
Suggests: %{name}-client%{?_isa} = %{version}-%{release}
%endif
%{?systemd_requires}

%description SQL
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the monetdb and monetdbd programs and the systemd
configuration.

%post SQL
%systemd_post monetdbd.service

%preun SQL
%systemd_preun monetdbd.service

%postun SQL
%systemd_postun_with_restart monetdbd.service

%files SQL
%defattr(-,root,root)
%{_bindir}/monetdb*
%if %{without compat}
%dir %attr(775,monetdb,monetdb) %{_localstatedir}/log/monetdb
%dir %attr(775,monetdb,monetdb) %{_rundir}/monetdb
# RHEL >= 7, and all current Fedora
%{_tmpfilesdir}/monetdbd.conf
%{_unitdir}/monetdbd.service
%config(noreplace) %attr(664,monetdb,monetdb) %{_localstatedir}/monetdb5/dbfarm/.merovingian_properties
%verify(not mtime) %attr(664,monetdb,monetdb) %{_localstatedir}/monetdb5/dbfarm/.merovingian_lock
%config(noreplace) %attr(644,root,root) %{_sysconfdir}/logrotate.d/monetdbd
%{_mandir}/man1/monetdb.1*
%{_mandir}/man1/monetdbd.1*
%dir %{_datadir}/doc/MonetDB-SQL
%docdir %{_datadir}/doc/MonetDB-SQL
%{_datadir}/doc/MonetDB-SQL/*
%endif

%if %{without compat}
%package SQL-devel
Summary: MonetDB SQL server modules development files
Group: Applications/Databases
Requires: %{name}-SQL%{?_isa} = %{version}-%{release}
Requires: %{name}-server-devel%{?_isa} = %{version}-%{release}
Requires: %{name}-embedded-devel%{?_isa} = %{version}-%{release}
Obsoletes: %{name}-SQL-server5-devel < 11.50.0
Provides: %{name}-SQL-server5-devel = %{version}-%{release}
Provides: %{name}-SQL-server5-devel%{?_isa} = %{version}-%{release}

%description SQL-devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains files needed to develop SQL extensions.

%files SQL-devel
%defattr(-,root,root)
%{_includedir}/monetdb/opt_backend.h
%{_includedir}/monetdb/rel_*.h
%{_includedir}/monetdb/sql*.h
%{_includedir}/monetdb/store_*.h
%{_datadir}/monetdb/cmake/MonetDBConfig*.cmake
%{_datadir}/monetdb/cmake/sqlTargets*.cmake
%endif

%if %{without compat}
%package embedded
Summary: MonetDB as an embedded library
Group: Applications/Databases
Requires: %{name}-libs%{?_isa} = %{version}-%{release}

%description embedded
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the library to turn MonetDB into an embeddable
library, also known as MonetDBe.  Also see %{name}-embedded-devel to
use this in a program.

%ldconfig_scriptlets embedded

%files embedded
%{_libdir}/libmonetdbe.so.*

%package embedded-devel
Summary: MonetDB as an embedded library development files
Group: Applications/Databases
Requires: %{name}-embedded%{?_isa} = %{version}-%{release}
Requires: %{name}-devel%{?_isa} = %{version}-%{release}

%description embedded-devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the library and include files to create a
program that uses MonetDB as an embeddable library.

%files embedded-devel
%defattr(-,root,root)
%{_libdir}/libmonetdbe.so
%{_includedir}/monetdb/monetdbe.h
%{_libdir}/pkgconfig/monetdbe.pc
%{_datadir}/monetdb/cmake/monetdbeTargets*.cmake

%package embedded-tests
Summary: MonetDBe tests package
Group: Applications/Databases
Requires: %{name}-embedded%{?_isa} = %{version}-%{release}

%description embedded-tests
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains some test programs using the %{name}-embedded
package.  You probably don't need this, unless you are a developer.

%files embedded-tests
%defattr(-,root,root)
%{_bindir}/example1
%{_bindir}/example2
%{_bindir}/example_append
%{_bindir}/example_append_raw
%{_bindir}/example_backup
%{_bindir}/example_blob
%{_bindir}/example_connections
%{_bindir}/example_copy
%{_bindir}/example_decimals
%{_bindir}/example_proxy
%{_bindir}/example_sessions
%{_bindir}/example_temporal
%{_bindir}/demo_oob_read
%{_bindir}/demo_oob_write

%package testing-python
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Requires: %{name}-client-tests = %{version}-%{release}
Requires: python3dist(pymonetdb) >= 1.9
BuildArch: noarch
%endif

%if %{without compat}
%description testing-python
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the Python programs and files needed for testing
the MonetDB packages.  You probably don't need this, unless you are a
developer, but if you do want to test, this is the package you need.

%files testing-python
%defattr(-,root,root)
%{_bindir}/Mtest.py
%{_bindir}/Mz.py
%{_bindir}/mktest.py
%{_bindir}/sqllogictest.py
%dir %{python3_sitelib}/MonetDBtesting
%{python3_sitelib}/MonetDBtesting/*
%endif

%if %{without compat}
%package selinux
Summary: SELinux policy files for MonetDB
Group: Applications/Databases
%if "%{?_selinux_policy_version}" != ""
Requires:       selinux-policy >= %{?_selinux_policy_version}
%endif
Requires(post):   %{name}-server%{?_isa} = %{version}-%{release}
Requires(postun): %{name}-server%{?_isa} = %{version}-%{release}
Requires(post):   %{name}-SQL%{?_isa} = %{version}-%{release}
Requires(postun): %{name}-SQL%{?_isa} = %{version}-%{release}
# we need /usr/sbin/semodule, /sbin/restorecon, /sbin/fixfiles which are in
# policycoreutils
Requires(post):   policycoreutils
Requires(postun): policycoreutils
BuildArch: noarch

%global selinux_types %(awk '/^#[[:space:]]*SELINUXTYPE=/,/^[^#]/ { if ($3 == "-") printf "%s ", $2 }' /etc/selinux/config 2>/dev/null)
%global selinux_variants %([ -z "%{selinux_types}" ] && echo mls targeted || echo %{selinux_types})

%description selinux
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the SELinux policy for running MonetDB under
control of systemd.  There is one tunable parameter, mserver5_can_read_home,
which can be set using "setsebool -P mserver5_can_read_home=true" to allow
an mserver5 process started by monetdbd under the control of systemd to
read files in users' home directories.

%post selinux
for selinuxvariant in %{selinux_variants}
do
  /usr/sbin/semodule -s ${selinuxvariant} -i \
    %{_datadir}/selinux/${selinuxvariant}/monetdb.pp &> /dev/null || :
done
/sbin/restorecon -R %{_localstatedir}/monetdb5 %{_localstatedir}/log/monetdb %{_rundir}/monetdb %{_bindir}/monetdbd* %{_bindir}/mserver5* %{_unitdir}/monetdbd.service &> /dev/null || :
/usr/bin/systemctl try-restart monetdbd.service

%postun selinux
if [ $1 -eq 0 ] ; then
  active=`/usr/bin/systemctl is-active monetdbd.service`
  if [ $active = active ]; then
    /usr/bin/systemctl stop monetdbd.service
  fi
  for selinuxvariant in %{selinux_variants}
  do
    /usr/sbin/semodule -s ${selinuxvariant} -r monetdb &> /dev/null || :
  done
  /sbin/restorecon -R %{_localstatedir}/monetdb5 %{_localstatedir}/log/monetdb %{_rundir}/monetdb %{_bindir}/monetdbd* %{_bindir}/mserver5* %{_unitdir}/monetdbd.service &> /dev/null || :
  if [ $active = active ]; then
    /usr/bin/systemctl start monetdbd.service
  fi
fi

%files selinux
%defattr(-,root,root,0755)
%docdir %{_datadir}/doc/MonetDB-selinux
%{_datadir}/doc/MonetDB-selinux/*
%{_datadir}/selinux/*/monetdb.pp
%endif

%prep
%setup -q -n MonetDB-%{version}

%build
# from Fedora 40, selinux uses /run where before it used /var/run
# the code is now for Fedora 40 but needs a patch for older versions
%if (0%{?fedora} < 40)
sed -i 's;@CMAKE_INSTALL_FULL_RUNSTATEDIR@/monetdb;@CMAKE_INSTALL_FULL_LOCALSTATEDIR@/run/monetdb;' misc/selinux/monetdb.fc.in
sed -i 's/1\.2/1.1/' misc/selinux/monetdb.te
%endif

%cmake3 \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_RUNSTATEDIR=/run \
        -DRELEASE_VERSION=ON \
        -DASSERT=OFF \
        -DCINTEGRATION=%{?with_cintegration:ON}%{!?with_cintegration:OFF} \
        -DFITS=%{?with_fits:ON}%{!?with_fits:OFF} \
        -DGEOM=%{?with_geos:ON}%{!?with_geos:OFF} \
        -DINT128=%{?with_hugeint:ON}%{!?with_hugeint:OFF} \
        -DNETCDF=OFF \
        -DODBC=%{!?with_compat:ON}%{?with_compat:OFF} \
        -DPY3INTEGRATION=%{?with_py3integration:ON}%{!?with_py3integration:OFF} \
        -DRINTEGRATION=%{?with_rintegration:ON}%{!?with_rintegration:OFF} \
        -DSANITIZER=OFF \
        -DSHP=OFF \
        -DSTRICT=OFF \
        -DTESTING=ON \
        -DWITH_BZ2=ON \
        -DWITH_CMOCKA=OFF \
        -DWITH_CURL=ON \
        -DWITH_LZ4=ON \
        -DWITH_LZMA=ON \
        -DWITH_OPENSSL=%{?with_openssl:ON}%{!?with_openssl:OFF} \
        -DWITH_PCRE=ON \
        -DWITH_PROJ=OFF \
        -DWITH_READLINE=ON \
        -DWITH_RTREE=OFF \
        -DWITH_SQLPARSE=OFF \
        -DWITH_VALGRIND=OFF \
        -DWITH_XML2=ON \
        -DWITH_ZLIB=ON

%cmake3_build

%install
mkdir -p "${RPM_BUILD_ROOT}"/usr
for d in etc var; do mkdir "${RPM_BUILD_ROOT}"/$d; ln -s ../$d "${RPM_BUILD_ROOT}"/usr/$d; done
%cmake3_install
rm "${RPM_BUILD_ROOT}"/usr/var "${RPM_BUILD_ROOT}"/usr/etc

# move file to correct location
mkdir -p "${RPM_BUILD_ROOT}"%{_tmpfilesdir} "${RPM_BUILD_ROOT}"%{_sysusersdir}
mv "${RPM_BUILD_ROOT}"%{_sysconfdir}/tmpfiles.d/monetdbd.conf "${RPM_BUILD_ROOT}"%{_tmpfilesdir}
cat > "${RPM_BUILD_ROOT}"%{_sysusersdir}/monetdb.conf << EOF
u monetdb - "MonetDB Server" /var/lib/monetdb
EOF
rmdir "${RPM_BUILD_ROOT}"%{_sysconfdir}/tmpfiles.d

install -d -m 0750 "${RPM_BUILD_ROOT}"%{_localstatedir}/lib/monetdb
install -d -m 0770 "${RPM_BUILD_ROOT}"%{_localstatedir}/monetdb5/dbfarm
install -d -m 0775 "${RPM_BUILD_ROOT}"%{_localstatedir}/log/monetdb
install -d -m 0775 "${RPM_BUILD_ROOT}"%{_rundir}/monetdb

# remove unwanted stuff
rm -f "${RPM_BUILD_ROOT}"%{_libdir}/monetdb5*/lib_microbenchmark*.so
rm -f "${RPM_BUILD_ROOT}"%{_libdir}/monetdb5*/lib_udf*.so
rm -f "${RPM_BUILD_ROOT}"%{_bindir}/monetdb_mtest.sh

if [ -x /usr/sbin/hardlink ]; then
    /usr/sbin/hardlink -cv "${RPM_BUILD_ROOT}"%{_datadir}/selinux
else
    # Fedora 31
    /usr/bin/hardlink -cv "${RPM_BUILD_ROOT}"%{_datadir}/selinux
fi

# update shebang lines for Python scripts
%if %{?py3_shebang_fix:1}%{!?py3_shebang_fix:0}
    # Fedora has py3_shebang_fix macro
    %{py3_shebang_fix} "${RPM_BUILD_ROOT}"%{_bindir}/*.py
%else
    # EPEL does not, but we can use the script directly
    /usr/bin/pathfix.py -pni "%{__python3} -s" "${RPM_BUILD_ROOT}"%{_bindir}/*.py
%endif

%if %{with compat}
# delete files that are not going to be installed in compat packages
rm "${RPM_BUILD_ROOT}"%{_bindir}/{M{convert.py,test.py,z.py},bincopydata,example_proxy,m{alsample.pl,client,ktest.py,onetdb{,d},s{erver5,qldump},urltest},s{ample{0,1,4},hutdowntest,mack0{0,1},ql{logictest.py,sample.p{hp,l}},treamcat},testcondvar}
rm -r "${RPM_BUILD_ROOT}"%{_datadir}/doc/MonetDB*
rm "${RPM_BUILD_ROOT}"%{_datadir}/selinux/*/monetdb.pp
rm -r "${RPM_BUILD_ROOT}"%{_datadir}/monetdb
rm -r "${RPM_BUILD_ROOT}"%{_includedir}/monetdb
rm "${RPM_BUILD_ROOT}"%{_libdir}/*.so "${RPM_BUILD_ROOT}"%{_libdir}/libmonetdbe.so.*
rm -r "${RPM_BUILD_ROOT}"%{_libdir}/pkgconfig
rm -r "${RPM_BUILD_ROOT}"%{_localstatedir}/lib/monetdb "${RPM_BUILD_ROOT}"%{_localstatedir}/monetdb5
rm -r "${RPM_BUILD_ROOT}"%{_mandir}/man1
rm -r "${RPM_BUILD_ROOT}"%{python3_sitelib}/MonetDBtesting
rm "${RPM_BUILD_ROOT}"%{_sysconfdir}/logrotate.d/monetdbd
rm "${RPM_BUILD_ROOT}"%{_sysusersdir}/monetdb.conf
rm "${RPM_BUILD_ROOT}"%{_tmpfilesdir}/monetdbd.conf
rm "${RPM_BUILD_ROOT}"%{_unitdir}/monetdbd.service
%endif

%changelog
* Tue Dec 16 2025 Lucas Pereira <lucas.pereira@monetdbsolutions.com> - 11.55.1-20251209
- sql: New implementation for the CONTAINS filter function for string data
  types using a much faster algorithm based on the knowledge of the
  bigram occurrences of the to-be-filtered column.

* Tue Dec 16 2025 svetlin <svetlin.stalinov@monetdbsolutions.com> - 11.55.1-20251209
- Extended MonetDB’s memory allocator framework from the SQL layer to all
  layers of the database server.  The main features of the allocator framework
  include i) efficient processing of large numbers of memory allocation calls,
  and ii) efficient management of memory to avoid fragmentation. This update
  also enables fine-grained configuration and monitoring of memory usage per
  thread, query, etc.

* Tue Dec 09 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.55.1-20251209
- Rebuilt.
- GH#7635: Unexpected Inner Join Crash
- GH#7645: Unexpected Internal Error in Inner Join
- GH#7651: Incorrect Anti Join Result
- GH#7652: Incorrect Anti Join Result related to optimization
- GH#7653: Incorrect Inner Join Result
- GH#7667: MonetDB Mar2025-SP1 crashes at `rel_selects` with a circular
  view
- GH#7677: Misleading error message "Could not allocate space"
- GH#7694: Unexpected execution result
- GH#7695: Unexpected execution result
- GH#7696: Unexpected execution result
- GH#7697: Unexpected execution result
- GH#7698: Unexpected Right Join Result
- GH#7701: Add possibility to set MAX_WORKERS to NO MAX_WORKERS in ALTER
  USER
- GH#7703: Unexpected Left Join Result
- GH#7705: Unexpected Anti Join Result
- GH#7707: Unexpected Right Join Result
- GH#7708: Unexpected Join Result
- GH#7709: Monetdb crash when using field fcuntion
- GH#7711: Unexpected Crash
- GH#7712: Unexpected Out of Memory
- GH#7713: Monetdb crashes when using group by
- GH#7714: Monetdb crash when creating table using window function
- GH#7715: Unexpected Anti Join Result
- GH#7716: Unexpected Anti Join Result
- GH#7717: a crash when executing sql
- GH#7719: a crash using select
- GH#7720: MonetDB server (Mar2025-SP2-release) crashes at `stmt_cond`
- GH#7722: MonetDB server (Mar2025-SP2-release) crashes at `rel_with_query`
- GH#7725: MonetDB server (Mar2025-SP2-release) crashes at
  `bin_find_smallest_column`
- GH#7727: MonetDB server (Mar2025-SP2-release) crashes at `exp_ref`
- GH#7739: Unexpected Crash in Left Join
- GH#7741: crash in MonetDB
- GH#7745: Unexpected Execution Results
- GH#7748: MonetDB server crashes with "unexpected end of file" on UPDATE
  ... RETURNING with EXISTS subquery
- GH#7751: Continuation of Bug #7737
- GH#7752: Internal error when executing a simple SQL query:
  TypeException:user.main[29]:'mat.packIncrement' undefined in:
  X_39:any := mat.packIncrement(X_37:bat[:lng], X_38:bat[:bte]);
- GH#7753: mserver5.exe crashes when executing a simple query
- GH#7763: MALexception throw in query with EXISTS and NULL

* Mon Nov 10 2025 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.55.1-20251209
- sql: Add functions to_hex(int) and to_hex(bigint). They return the
  unsigned hexadecimal string representation of their argument.

* Wed Nov  5 2025 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.55.1-20251209
- sql: COPY BINARY has been optimized to be much faster when many string columns
  are involved.
- sql: The performance of COPY BINARY of blob column has also been improved.
- sql: The performance of COPY BINARY ON CLIENT has been much improved. It used
  to be much slower than ON SERVER, now it's only a little slower.
- sql: The file format for COPY BINARY strings has been extended. If the same
  string occurs multiple times, later occurrences can refer back to
  earlier occurrences instead of including another copy of the string.
  This improves both file size and processing time.
- sql: A new example tool 'backrefencode' has been added that can introduce these
  back references or remove them again.
- sql: Experimental support for compressed ON CLIENT transfers has been added.
  If you write ON 'algo' CLIENT, with algo=lz4/gz/xz/b2, the server compresses
  downloaded data and decompresses uploaded data server-side using the given
  algorithm. It is up to the user to ensure that the uploaded data has indeed
  been compressed. This is not always easy because many clients automatically
  compress or decompress data when the file name contains .gz, .lz4, etc.,
  which would lead to double compression and decompression.

* Tue Nov  4 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.55.1-20251209
- sql: Implemented aggregates sha1, sha224, sha256, sha384, sha512, and
  ripemd160 which calculate a checksum (digest) over a column of strings.
  It only makes sense to use these with an ordering, as `SELECT sha256(name
  ORDER BY id) FROM table`, since the ordering in which the values are
  fed into the digest algorithm is important and cannot otherwise be
  guaranteed.

* Tue Nov  4 2025 Lucas Pereira <lucas.pereira@monetdbsolutions.com> - 11.55.1-20251209
- sql: EXPLAIN now supports a BEFORE/AFTER LOGICAL UNNEST/LOGICAL REWRITE/PHYSICAL
  clause to indicate which phase of query compilation to show.  A plain
  EXPLAIN is equivalent to EXPLAIN AFTER REWRITE, which is what PLAN
  used to do. The old EXPLAIN is now EXPLAIN PHYSICAL. LOGICAL REWRITE also
  supports specifying two positive values, rewriter index number and
  optimizer loop cycle stop counter. SHOW DETAILS includes more information
  about properties, rewriters number of changes and time spent.
- sql: The PLAN keyword has been removed.

* Tue Nov  4 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.55.1-20251209
- monetdb5: We no longer persist querylog results.  If you want to keep the data,
  make a copy into an SQL table.

* Tue Nov  4 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.55.1-20251209
- MonetDB: This server is no longer compatible with the MonetDB Stethoscope.

* Tue Nov  4 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.55.1-20251209
- sql: The TRACE prefix for SQL queries now no longer produces two result sets.
  Before, the first result set was the result of the query, and the
  second result set was timing information of the query execution.
  This second result set is no longer produced.  Instead, use the
  (already existing) function sys.tracelog() or the view sys.tracelog to
  retrieve this information.  As before, the table that is produced by the
  function/view is reset whenever a new TRACE prefixed query is executed.

* Tue Nov  4 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.55.1-20251209
- monetdb5: The default_fast and minimal_fast optimizer pipelines have been
  removed.  The default_pipe and minimal_pipe optimizers now use the
  "fast" path always.

* Tue Nov  4 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.55.1-20251209
- sql: Removed table returning function sys.optimizer_stats().

* Tue Nov  4 2025 Lucas Pereira <lucas.pereira@monetdbsolutions.com> - 11.55.1-20251209
- gdk: log_tflush function, when flushnow flag is true, now passes bitmap array
  that indicates which bat id's need to be sync'ed to disk, instead
  of passing NULL which would trigger a sync of the full catalog. If
  new bats are added to the catalog, they will be sync'ed always (see
  maxupdated flag).

* Tue Nov  4 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.55.1-20251209
- sql: There are new types "inet4" and "inet6" which can hold respectively
  IPv4 and IPv6 internet addresses without CIDR network mask
  information.  Netmask information can be held in a separate column if
  they are needed.  The 0 address (0.0.0.0 and ::) are used as NULL
  value and can therefore not be used as addresses.  Bitwise operations
  (AND, OR, XOR, NOT) are supported on the addresses, and there are
  various functions to check whether an address is contained in a
  (sub)net (where an extra CIDR netmask column is also needed).

* Tue Nov  4 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.55.1-20251209
- monetdb5: The type "pcre" has been removed.  There was no way to create a value of
  the type, and there was only one function that used a value of the type.
  That function (pcre.index) has also been removed.

* Tue Nov  4 2025 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.55.1-20251209
- sql: Added the possibility to specify IF NOT EXISTS for the following
  CREATE statements:
    CREATE SEQUENCE IF NOT EXISTS seq_name ...
    CREATE USER IF NOT EXISTS user_name ...
    CREATE ROLE IF NOT EXISTS role_name ...
    CREATE TYPE IF NOT EXISTS type_name ...
    CREATE INDEX IF NOT EXISTS index_name ON ...
    CREATE IMPRINTS INDEX IF NOT EXISTS index_name ON ...
    CREATE ORDERED INDEX IF NOT EXISTS index_name ON ...
  With IF NOT EXISTS specified these CREATE statements will not return
  an error when an object with the same name already exists.  See doc:
  https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/
- sql: Added the possibility to specify IF EXISTS for the following DROP statements:
    DROP SEQUENCE IF EXISTS seq_name ...
    DROP USER IF EXISTS user_name
    DROP ROLE IF EXISTS role_name
    DROP TYPE IF EXISTS type_name ...
    DROP INDEX IF EXISTS index_name
  With IF EXISTS specified these statements will not return an error
  when the object does not exists.  See doc:
  https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/drop-statement/
- sql: Added the possibility to specify IF EXISTS for two ALTER statements:
    ALTER SEQUENCE IF EXISTS seq_name ...
    ALTER USER IF EXISTS user_name ...
  With IF EXISTS specified these statements will not return an error
  when the object does not exists.  See doc:
  https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/sequence-definition/
  and
  https://www.monetdb.org/documentation/user-guide/sql-manual/data-definition/privileges/

* Tue Nov  4 2025 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.55.1-20251209
- odbc: Corrected SQLColAttribute() and SQLGetDescField() for when
  FieldIdentifier is SQL_DESC_LITERAL_PREFIX or SQL_DESC_LITERAL_SUFFIX.
  They will now return the correct literal prefix or suffix string
  instead of an empty string depending on the datatype of the column.

* Tue Nov  4 2025 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.55.1-20251209
- sql: With COPY INTO, the USING DELIMITERS, DECIMAL, ESCAPE, NULL, BEST EFFORT and
  FWF clauses can now be given in any order. If a clause occurs multiple times,
  the last instance wins.

* Tue Nov  4 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.55.1-20251209
- sql: It is now possible to change the type of a column by using a statement
  like ALTER TABLE t ALTER COLUMN c type;.  The content of the column
  is converted to the new type using code similar to CAST(c AS type).
  If the conversion fails, the column type isn't changed.

* Tue Nov  4 2025 Niels Nes <niels@cwi.nl> - 11.55.1-20251209
- MonetDB: Changed the way complex AND and OR expressions are handled. The new
  expression tree uses 2 new cmp flag (cmp_con/cmp_dis), both expressions
  hold lists of expressions. This structure reduces the need for stack
  space, allowing way larger expressions trees to be handled.

* Tue Nov 04 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.15-20251104
- Rebuilt.
- GH#7662: Privilege Issue: MonetDB Mar2025-SP1 does not check the
  permission of 'DROP ROLE' and 'DROP USER' statements
- GH#7663: Privilege Issue: the `ALTER USER ... DEFAULT ROLE` statement
  misses permission checks, which can cause privilege escalation to get
  other users' privileges
- GH#7664: Privilege Issue: the `SET SESSION AUTHORIZATION` statement will
  enable any user to alter other users' MAX_WORKERS
- GH#7665: MonetDB dev-builds crashes at `sql_trans_drop_trigger()`
- GH#7666: MonetDB Mar2025-SP1 unexpectly shutdown with crafted `GLOBAL
  TEMPORARY TABLE` and `ALTER TABLE` statements
- GH#7668: MonetDB Mar2025-SP1 crashes at `key_dup()`
- GH#7669: MonetDB Mar2025-SP1 crashes at `AUTHdecypherValue()`
- GH#7670: MonetDB Mar2025-SP1 crashes at `exp_subtype()`
- GH#7672: MonetDB Mar2025-SP1 crashes at `find_name()`
- GH#7673: MonetDB Mar2025-SP1 crashes at `rel_value_exp2()`
- GH#7674: MonetDB Mar2025-SP1 crashes at `rel_schemas()`
- GH#7689: Empty SQL result (no rows, no columns)
- GH#7699: The OPTIMIZER string value in CREATE USER statement is not
  checked on validity.
- GH#7702: Invalid handling of WHERE conditions
- GH#7706: Role (bob) missing
- GH#7710: Monetdb crash when using char datatype
- GH#7730: Incorrect arithmetic in generate_series with month-based
  intervals
- GH#7732: Missing column name in select expands to all columns in table
- GH#7733: mserver5 assertion failure when started with -d2 --in-memory
- GH#7734: 'epoch' function doesn't handle fractions with leading zeros
  correctly
- GH#7735: crash in Monetdb
- GH#7736: crash in  MonetDB
- GH#7737: SQL Query Optimizer / Performance Regression with Merge Tables
  in MonetDB 11.53
- GH#7742: crash in MonetDB

* Thu Sep 11 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.15-20251104
- clients: Changed the --describe (-D) option of msqldump to really mean (as it
  says in the manual) do a dump without the data.  Before, the output
  looked like a dump, but could not necessarily be fed back into an
  mserver5, i.e. it wasn't really a dump without data.

* Mon Sep 01 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.13-20250901
- Rebuilt.
- GH#7692: Illegal argument on range select with equality
- GH#7693: Obscure failure when running mserver5 on an older monetdbd
  release

* Thu Aug 28 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.13-20250901
- clients: We now try to figure out the size of the terminal window on Windows.
  This means that mclient will, by default, format tabular output to
  not wrap long lines, like is already done on Unix/Linux.

* Thu Aug 21 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.11-20250821
- Rebuilt.
- GH#7643: Unable to process field with split_part(). Facing Issue
  "client4681: exp_bin: CRITICAL: Could not find (null).rate"
- GH#7647: 'mat.packIncrement' undefined
- GH#7648: Unexpected Right Join Assertion Error
- GH#7649: Unexpected Inner Join Crash
- GH#7650: Unexpected Right Join Crash
- GH#7655: slow concurrent insert
- GH#7656: Primary key reported as being a foreign key to itself
- GH#7657: MonetDB Mar2025-SP1 crashes at `sqlparse()` with a crafted
  MERGE statement
- GH#7659: MonetDB Mar2025-SP1 crashes at `rel_select_add_exp()` with a
  crafted CREATE TRIGGER statement
- GH#7660: MonetDB Mar2025-SP1 crashes at `subrel_bin()` with a COPY
  statement
- GH#7661: MonetDB Mar2025-SP1 crashes at `dlist_length()` with a crafted
  CREATE TRIGGER statement
- GH#7671: MonetDB Mar2025-SP1 crashes at `BLOBlength()`
- GH#7675: Debian service start on new install not getting environment
  variables from /etc/default/monetdb-sql
- GH#7680: `UNION ALL` doesn't work as expected
- GH#7681: Describe table feature not working correctly
- GH#7682: replacing a login trigger crashes server
- GH#7683: wrong  Driver= and Setup= libary paths stored in
  /etc/odbcinst.ini after installation of MonetDB ODBC driver on ubuntu
- GH#7686: DELETE FROM empty table should always be a no-op
- GH#7688: exists with nulls gives incorrect result

* Wed Aug 13 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.11-20250821
- MonetDB: It is now relatively easy to configure the location of the database farm
  (aka dbfarm) directory when using systemd.  Just create an override
  file for the monetdbd service and add an Environment entry for DBFARM
  pointing to the new directory.

* Tue Aug  5 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.11-20250821
- gdk: The SIGUSR1 output now displays counts for memory sizes in a
  human-readable format next to the original byte counts.

* Fri Aug  1 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.11-20250821
- monetdb5: The PCRE module has been ported to the PCRE2 version of the library.
  The main difference is in the regexp_replace function which now no
  longer accepts \ to introduce replacements.  Only $ is accepted (it
  was already accepted before).

* Tue Jul  8 2025 Niels Nes <niels@cwi.nl> - 11.53.11-20250821
- sql: Fixed issue #7655, now the segments keep the number of deleted
  rows. Only search for reuse when deleted rows are available.

* Fri Jul 04 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.9-20250704
- Rebuilt.
- GH#7629: monetdbd causes SELinux denial
- GH#7654: Query remote table that targets remote server table not owned
  by monetdb default user

* Mon Jun 30 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.7-20250630
- Rebuilt.

* Fri Jun 27 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.5-20250627
- Rebuilt.
- GH#7625: Missing entry in sys.table_types table for new LOCAL TEMPORARY
  VIEW
- GH#7626: crash in window function with constant aggregation
- GH#7627: Increased memory consumption, slowness and crash
- GH#7632: Unexpected Left Join Crash
- GH#7633: Unexpected Out of Memory of Inner Join
- GH#7634: Join with subquery crash
- GH#7636: Unexpected Anti Join Crash
- GH#7638: PREPARE statement increases the memory use of the session even
  when DEALLOCATEd
- GH#7644: Unexpected anti join crash
- GH#7646: Unexpected Left Join Crash

* Thu Jun 19 2025 Lucas Pereira <lucas.pereira@monetdbsolutions.com> - 11.53.5-20250627
- sql: When a prepared statement is executed, sys.queue now shows the text
  of the original PREPARE statement along with the EXEC and its arguments.

* Fri Jun 13 2025 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.53.5-20250627
- sql: Add optional parameters omit_unlogged (bool) and omit_table_ids (str) to
  sys.hot_snapshot(). If omit_unlogged is set to true, the data in UNLOGGED
  tables is omitted from the snapshot. If omit_table_ids is given, it must
  be a comma-separated list of table ids as found in sys.tables. The data in
  each of those tables will be omitted from the snapshot.
- sql: Empty BATs are omitted from the snapshot, the restored server will created
  them if necessary.

* Tue Jun  3 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.5-20250627
- clients: When connecting to a database, if there are multiple monetdbd servers
  running, mclient will try them all, and also both UNIX domain
  sockets and then TCP, in order to find a server that accepts the
  connection.  However, when a server that handles the requested
  database does exist but refuses the connection for some other
  reason, mclient would continue searching.  This has now been
  changed.  If monetdbd reports an error other than database unknown,
  mclient will now stop looking and report the error.  This is
  actually a change in the "mapi" library, so any program using the
  library gets the new behavior.
- clients: There is a new option --quiet (or just -q) in mclient.  If used, the
  welcome message that is normally printed in an interactive invocation
  is suppressed.

* Thu May 22 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.5-20250627
- merovingian: When mserver5 is started by monetdbd due to an implicit request
  (application trying to connect to a database), and mserver5 crashes
  or exits before a connection can be established, monetdbd will stop
  trying to start the server after a few attempts.  When using an explicit
  command to start the server (using monetdb start), monetdbd will always
  attempt to start the server.

* Wed May 14 2025 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.53.5-20250627
- sql: Corrected reading decimal type columns from external ODBC data sources
  via proto_loader('odbc:...'). Those columns were mapped to varchar type
  columns. Now they will be mapped to decimal type, when possible.

* Fri May  9 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.5-20250627
- clients: There is now a \dm command in the interactive mclient to show
  information about merge tables.

* Thu May  8 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.5-20250627
- MonetDB: It is now possible to specify an idle timeout using --set
  idle_timeout=<seconds> (see mserver5 manual page) which gets triggered
  if a connection to the server is idle (i.e. does not send any queries
  to the server) while there is a SQL transaction active.

* Mon Mar 24 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.3-20250324
- Rebuilt.
- GH#7622: In PREPARE queries with many parameters, information about
  parameters is truncated when sent to client.
- GH#7623: Database crashed when using UPDATE xxx SET xxx RETURNING xx

* Thu Mar 20 2025 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.53.3-20250324
- sql: Added scalar functions: dayname(d date) and monthname(d date) returns varchar(10).

* Mon Mar 17 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.1-20250317
- Rebuilt.
- GH#7101: Feature request: nextafter() in SQL
- GH#7159: CREATE LOCAL TEMPORARY VIEW
- GH#7331: Support RETURNING clause
- GH#7578: explain result in Mal is truncated in large UDFs and their
  input bats is not shown
- GH#7609: Upgrade 11.49.11 to 11.51.7 issues
- GH#7611: Not possible to create table with multiple composite UNIQUE
  NULLS NOT DISTINCT constraints
- GH#7614: Filter function creates a cartesian product when used with a
  view
- GH#7615: Filter function creates a cartesian product when used with a
  view (2)
- GH#7616: Filter function disappears
- GH#7618: Tables loose their columns
- GH#7619: Resource leak in prepared statements
- GH#7621: crash on aggregate with case statement

* Tue Mar 11 2025 Niels Nes <niels@cwi.nl> - 11.53.1-20250317
- sql: ranking window functions are now optimized into topn's
  For the grouped case we added the missing grouped/heap based topn
  implementation.

* Tue Mar 11 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.1-20250317
- MonetDB: There is a new shared library called libmutils that contains some
  utility functions that are used by several programs.

* Wed Mar  5 2025 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.53.1-20250317
- sql: Added support for reading external data in a generic way via table
  returning function: proto_loader(string uri).  The uri string value
  must start with the scheme name, ending with : character.
  Supported schemes are: monetdb: and odbc:.
  The monetdb scheme allows you to connect to a remote MonetDB server
  and retrieve the data of a specific table or view in a specific schema.
  The uri syntax: monetdb://[<host>[:<port>]]/<database>/<schema>/<table>
  Example: SELECT * FROM proto_loader('monetdb://127.0.0.1:50000/demo_db/sys/tables');
  The odbc scheme allows you to connect to any ODBC data source via
  an ODBC driver and retrieve the data of a supplied query.
  The uri syntax:
   odbc:{{DSN|FILEDSN}=<data source name>|DRIVER=<path_to_driver>};
                      [<ODBC connection parameters>;]QUERY=<SQL query>
  For ODBC you normally configure a data source first. This
  is done using the ODBC administrator (on windows: odbcad32.exe,
  on linux: odbcinst).  Once a data source for a specific ODBC
  driver has been setup using a unique name, you can reference it as:
  DSN=my_bigdata; or FILE_DSN=/home/usernm/dsns/my_bigdata.dsn;
  If you do not want to setup a data source, you can use DRIVER=...;
  to specify the ODBC driver program to use. However this also means
  you have to specify all the required connection parameters yourself,
  such as UID=...;PWD=...;DATABASE=...; etc.
  The QUERY=<SQL query> part is mandatory and must be specified at the
  end of the uri string, after the optional ODBC connection parameters.
  Examples: SELECT * FROM proto_loader(
  'odbc:DSN=Postgres;UID=claude;PWD=monet;QUERY=SELECT * FROM customers');
  SELECT * FROM proto_loader('odbc:DRIVER=/usr/lib64/libsqlite3odbc.so;
    Database=/home/martin/sqlite3/chinook.db;QUERY=SELECT * FROM customers');
  Note that the 'odbc:' scheme is experimental and not enabled by default.
  To enable it, the MonetDB server has to be started with argument:
   --loadmodule odbc_loader

* Tue Feb 18 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.1-20250317
- clients: Support for dumping databases from servers from before Jul2021 (11.41.X)
  has been removed.

* Mon Feb 10 2025 stefanos mavros <stemavros@gmail.com> - 11.53.1-20250317
- sql: Extended the constant aggregate optimizer in order to eliminate
  aggregates with constant arguments whenever possible.

* Wed Jan 29 2025 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.53.1-20250317
- sql: REMOTE TABLES and REPLICA TABLES now fully support the monetdb://
  and monetdbs:// URL's introduced in Aug2024.
  Any mapi:monetdb:// URL's are normalized to the new style.
- sql: Add function sa_msettings_create() to allocate an msettings object
  using the arena allocator.
- sql: Unused helper function mapiuri_database() has been removed from
  rel_remote.h.
- mapilib: msettings can now be allocated with a custom memory allocator using
  msettings_create_with() and msettings_clone_with().  This is used in
  the SQL module to allocate them using the arena allocator.
- mapilib: The msettings objects no longer keep track of 'ignored' settings.
  Function msetting_set_ignored has been removed.
- mapilib: Function msetting_as_string() has been changed to never return a newly
  allocated string.  To make this possible the user now has to pass in
  a small scratch buffer that will be used if the setting is a number.
  (booleans and strings can use existing strings).
- mapilib: Functions msettings_parse_url() and msettings_validate() have been
  modified to return any error message instead of setting it through a
  pointer parameter.
- mapilib: Function msettings_write_url() has been added to render an msettings
  object as a URL string.

* Mon Jan 13 2025 Sjoerd Mullender <sjoerd@acm.org> - 11.53.1-20250317
- monetdb5: Removed function bat.attach since it wasn't used.

* Fri Dec 20 2024 Niels Nes <niels@cwi.nl> - 11.53.1-20250317
- sql: Added support for aggregates which order within the group such
  as quantile and which potentially order within the group such as
  group_concat. The ordering for such operators in now handled once in
  the relational plan. For this the create function statements can now
  have an optional order specification, using the keywords 'ORDERED'
  and 'WITH ORDER'.

* Fri Dec 20 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.53.1-20250317
- sql: Added support for recursive CTE's.
- sql: The SQL parser was cleaned up.  This resulted in some keywords being
  used more strictly.  If any of these keywords are to be used as column
  names, they have to be quoted using double quotes: AS, TABLE, COLUMN,
  DISTINCT, EXEC, EXECUTE.

* Mon Dec 16 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.53.1-20250317
- geom: Removed type geometryA (geometry array).  It was deprecated in the
  Jun2023 release (11.47.X) because there was no use for the type.

* Mon Dec 16 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.53.1-20250317
- monetdb5: Removed the MAL type "identifier" and supporting functions.  There has
  never been an SQL interface to this type.
- monetdb5: Removed the MAL type "color" and supporting functions.  There has
  never been an SQL interface to this type.

* Mon Dec 16 2024 Yunus Koning <yunus.koning@monetdbsolutions.com> - 11.53.1-20250317
- sql: Introduce the RETURNING clause for INSERT, UPDATE and DELETE statements.
  Specifying a RETURNING clause causes the SQL statement to return the
  modified records which can be queried using SELECT like expressions
  in the RETURNING clause. Aggregate functions are allowed.
  This is a common non-standard SQL extension.
  Examples:
  INSERT INTO foo values (1,10), (-1,-10) RETURNING i+2*j AS bar
  ----
  21
  -21
  UPDATE foo SET i = -i WHERE i >0 RETURNING sum(j), count(j)
  ----
  -60|3

* Mon Dec 16 2024 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.53.1-20250317
- MonetDB: Hot snapshot: allow member files larger than 64 GiB. By member files we mean
  the files inside the resulting .tar file, not the tar file itself. Huge member
  files are written using a GNU tar extension to the original tar format, which
  doesn't support more than 8 GiB.

* Mon Dec 16 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.53.1-20250317
- gdk: The implementation for the imprints index on numeric columns has
  been removed.  It hasn't been used in years, and when it is enabled,
  it doesn't really make queries go faster.

* Mon Dec 16 2024 Lucas Pereira <lucas.pereira@monetdbsolutions.com> - 11.53.1-20250317
- sql: Introduce division_min_scale SQL environment variable for specifying
  minimum scale of the division result. The default value is 3.

