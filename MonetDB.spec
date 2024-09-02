# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

%global version 11.52.0

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
# If the EPEL repository is availabe, or if building for Fedora, most
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
%if %{fedpkgs} && (0%{?rhel} != 7) && (0%{?rhel} != 8)
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
Source: https://www.monetdb.org/downloads/sources/Aug2024/MonetDB-%{version}.tar.bz2

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
BuildRequires: pkgconfig(libpcre) >= 4.5
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
%{_bindir}/ODBCStmtAttr
%{_bindir}/ODBCgetInfo
%{_bindir}/ODBCmetadata
%{_bindir}/ODBCtester
%{_bindir}/arraytest
%{_bindir}/bincopydata
%{_bindir}/murltest
%{_bindir}/odbcconnect
%{_bindir}/odbcsample1
%{_bindir}/sample0
%{_bindir}/sample1
%{_bindir}/sample4
%{_bindir}/shutdowntest
%{_bindir}/smack00
%{_bindir}/smack01
%{_bindir}/streamcat
%{_bindir}/testcondvar
%{_bindir}/malsample.pl
%{_bindir}/sqlsample.php
%{_bindir}/sqlsample.pl
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
%{_bindir}/example_proxy

%package testing-python
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Requires: %{name}-client-tests = %{version}-%{release}
Requires: python3dist(pymonetdb)
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
rm -f "${RPM_BUILD_ROOT}"%{_libdir}/monetdb5*/lib_opt_sql_append.so
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
* Mon Aug 19 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.51.3-20240819
- Rebuilt.
- GH#7562: Assertion failure when comparing `INTERVAL` value
- GH#7566: After a while, all new sessions (connections) may get refused

* Mon Aug 12 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.51.1-20240812
- Rebuilt.
- GH#7045: A value filtered in a subquery finds its way to a later filter
  in the outer query
- GH#7097: Add an 'ANY' or 'ARBITRARY' aggregate function
- GH#7245: monetdbe silently importing 0 rows, at random
- GH#7265: COPY INTO Not Reading in all records of fixed width delimited
  file
- GH#7272: Missed rewrite to bulk operators in simple SQL UDF
- GH#7312: Test Button for ODBC Driver
- GH#7332: Support IS [NOT] DISTINCT FROM predicate
- GH#7353: INTERVAL SECOND columns become incorrect when an INTERVAL HOUR
  column is present
- GH#7367: Libraries linked more than once?
- GH#7370: timestamp_to_str function not aware of timezone
- GH#7374: Different date and time returns
- GH#7392: Evaluate Profile-Guided Optimization
- GH#7424: Performance issue in select-joins
- GH#7459: Crash when using `CONTAINS` in `ORDER BY` clause
- GH#7460: Crash when using `CAST` and `BETWEEN AND`
- GH#7463: Unexpected result when using `CONTAINS` and type casting
- GH#7466: Crash when `INNER JOIN` with `CONTAINS`
- GH#7467: Conversion TIMESTAMPTZ to TIME does not take session TZ into
  account
- GH#7474: MonetDB server crashes in `VLTgenerator_table_`
- GH#7475: MonetDB server crashes in `__nss_database_lookup`
- GH#7476: MonetDB server crashes in `subrel_bin`
- GH#7477: MonetDB server crashes in `atom_cmp`
- GH#7480: MonetDB server crashes in `get_rel_count`
- GH#7481: MonetDB server crashes in `mvc_row_result_wrap`
- GH#7482: MonetDB server crashes in `bin_find_smallest_column`
- GH#7483: MonetDB server crashes in `rel_get_statistics_`
- GH#7484: MonetDB server crashes in `rel_optimize_projections_`
- GH#7485: MonetDB server crashes in `exp_setalias`
- GH#7486: MonetDB server crashes in `ALGgroupby`
- GH#7488: MonetDB server crashes in `strCmp`
- GH#7497: Multi-column IN clause with value list produces syntax error
- GH#7500: request: ANALYZE statement should be allowed to be used in the
  body of a procedure or function or trigger.
- GH#7514: Nonexistent window function raises
  `ParseException:SQLparser:42000!Query too complex: running out of stack
  space`
- GH#7517: UNLOGGED tables don't get cleaned up properly when DROPped.
- GH#7521: Unexpected result when using `IS DISTINCT FROM`
- GH#7522: Crash when creating view with `HAVING`
- GH#7523: Assertion failure when using `CONTAINS`
- GH#7524: Unexpected error when using `NATURAL RIGHT JOIN`
- GH#7525: Related to bug #7422 (variadic arguments in aggregate UDFs)
- GH#7527: Unexpected result when using `IS DISTINCT FROM` with `RIGHT
  JOIN`
- GH#7528: Assertion failure when using `JAROWINKLER` in `ORDER BY` clause
- GH#7529: Finding minimum value in reverse sorted column with NULL values
  at the end gives wrong result.
- GH#7530: Assertion failure when using `JAROWINKLER` with empty string
- GH#7533: DROP of a schema with CASCADE option drops tables, but bats are
  not removed.
- GH#7534: Unexpected result when using `IS DISTINCT FROM` with `AND`
- GH#7535: Assertion failure when using `GROUP BY` when `CREATE VIEW`
- GH#7538: BUG with decimal values
- GH#7539: Crash when using `IS DISTINCT FROM` with `SIN`
- GH#7540: Assertion failure when using `STARTSWITH`
- GH#7542: Nested query triggers an assert
- GH#7543: Unexpected result when using `IS DISTINCT FROM` with constants
- GH#7544: Unexpected result when using `STARTSWITH`
- GH#7545: Crash when creating view with `GROUP BY`
- GH#7547: drop login trigger causes server crash
- GH#7550: non-admin user can no longer query sys.statistics or
  information_schema.tables
- GH#7552: Unexpected result when using `NULL` constant in comparison
- GH#7553: Assertion failure when using `INNER JOIN` on `STARTSWITH`
- GH#7554: Unexpected result when using range comparison with `NULL`
- GH#7555: Unexpected result when casting integer to boolean in comparison
- GH#7556: Assertion failure when using `STARTSWITH` with view

* Wed Aug  7 2024 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.51.1-20240812
- odbc: Extended ODBC Data Source Setup program on Windows with fields to specify
  optional Client Information such as Application Name and Client Remark.

* Tue Aug  6 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.51.1-20240812
- MonetDB: The CMake configuration files for building extensions have now been
  included in the various MonetDB development RPMs and debs.

* Thu Aug  1 2024 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.51.1-20240812
- odbc: Extended ODBC Data Source Setup program on Windows with fields to specify
  new TLS Connection settings.
- odbc: Extended ODBC Data Source Setup program on Windows with a Test-button
  to quickly test connectivity to a MonetDB server.

* Tue Jul 16 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.51.1-20240812
- sql: The "phash" column in the sys.storage() table now indicates whether a
  hash exists.  If the hash is not loaded but there is a hash available
  on disk, the phash value is "true", but the "hashes" value is 0.

* Thu Jul 11 2024 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.51.1-20240812
- sql: Added execution privilege on all sys.generate_series(first, limit)
  and sys.generate_series(first, limit, stepsize) functions to public,
  so all users can now call these table producing generator functions.

* Sat Jun 29 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.51.1-20240812
- MonetDB: Removed upgrade code for versions before Jul2021.

* Sat Jun 29 2024 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.51.1-20240812
- mapilib: Add new columns to sys.sessions. Column 'language' is usually 'sql'.
  Column 'peer' is the network address of the client (something like
  '[::1]:46558' or '<UNIX SOCKET>'). Columns 'hostname', 'application',
  'client', 'clientpid' and 'remark' can be set by the client.
  Libmapi/mclient, pymonetdb and monetdb-java have been modified to fill
  in sensible default values.

* Sat Jun 29 2024 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.51.1-20240812
- sql: Extend CREATE USER MAX_MEMORY and ALTER USER MAX_MEMORY to accept
  strings of the form '10MiB', '10G', etc.

* Sat Jun 29 2024 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.51.1-20240812
- sql: Extended view sys.sessions and function sys.sessions() with new columns:
  language, peer, hostname, application, client, clientpid and remark.
- sql: All users now have SELECT privilege on view sys.sessions, but non-admin
  users only see their own sessions.
- sql: Added procedure sys.setclientinfo(property string, value string)
  to allow the client application to set a specific client info property.
- sql: Added system table sys.clientinfo_properties that lists the supported
  client info properties and their associated column name in sys.sessions view.
  It contains property names: ClientHostname, ApplicationName,
  ClientLibrary, ClientPid and ClientRemark.

* Sat Jun 29 2024 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.51.1-20240812
- odbc: ODBC now supports TLS. It can be configured through the following
  DSN- or Connection String attributes (canonical name / user friendly name):
    TLS / Encrypt = ON/OFF
    CERT / Server Certificate = PATH
    CERTHASH / Server Certificate Hash = sha256:HEXDIGITS
    CLIENTKEY / Client Key = PATH
    CLIENTCERT / Client Certificate = PATH
- odbc: Several more connection properties have been made configurable:
    SCHEMA / Schema = NAME
    TIMEZONE / Time Zone = Minutes East Of UTC
    REPLYSIZE / Reply Size = NUMBER
    LOGFILE / Log File = PATH
    LOGINTIMEOUT / Login Timeout = MILLISECONDS
    CONNECTIONTIMEOUT / Connection Timeout = MILLISECONDS
    AUTOCOMMIT / Autocommit = ON/OFF
    SOCK / Unix Socket = PATH (unix only)
- odbc: SQLBrowseConnect() adds On/Off suggestions to boolean settings
  and prioritizes the DATABASE attribute if it notices monetdbd
  requires one. Apart from that only UID/User and PWD/Password
  are required, all others have sensible defaults.

* Sat Jun 29 2024 Niels Nes <niels@cwi.nl> - 11.51.1-20240812
- sql: Extended sys.generate_series() to generate dates. Added 2 new functions:
  sys.generate_series(first date, "limit" date, stepsize interval month) and
  sys.generate_series(first date, "limit" date, stepsize interval day).

* Sat Jun 29 2024 Niels Nes <niels@cwi.nl> - 11.51.1-20240812
- sql: Added support for select exp, count(*) group by 1 order by 1;
  ie. using numeric references in group by clause.
- sql: Added support for GROUP BY ALL. This finds all expressions from the
  selections which aren't aggregations and groups on those.
  At least one aggregation must be specified.
  The ALL keyword can also be replaced by '*', so: GROUP BY *.
- sql: Added support for ORDER BY ALL. This orders on all columns of the selection.
  The ALL keyword can also be replaced by '*', so: ORDER BY *.

* Sat Jun 29 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.51.1-20240812
- MonetDB: The shared library (.dll aka .so files) now have the version number
  as part of the name.  This should allow the building of compatibility
  versions that can be installed in parallel to the latest version.
- MonetDB: Some of the Debian/Ubuntu packages have been renamed.  The old monetdb5
  names have been changed to plain monetdb, and libmonetdb5-server-*
  packages have been renamed monetdb-*.
- MonetDB: The names of some of the provided RPM files have been changed.
  References to the old MonetDB5 name have been removed.  All packages
  are now just MonetDB.

* Sat Jun 29 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.51.1-20240812
- gdk: Various changes were made having to do with things like case-insensitive
  comparisons and converting to upper or lower case.  Case insensitive
  comparison (i.e. the ILIKE operator) uses case folding which is similar
  to converting to lower case, but changes more characters, also sometimes
  to multiple characters (e.g. German sharp s (ß) compares equal to SS).

* Sat Jun 29 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.51.1-20240812
- stream: ICONV support has been removed from the stream library.  The server
  itself only needs UTF-8 support.  The client (mclient) does have
  iconv support.

* Sat Jun 29 2024 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.51.1-20240812
- sql: Removed the obsolete ANALYZE statement syntax options: SAMPLE nn and
  MINMAX. Both options have been ignored since release Jan2022. Now they
  are no longer accepted in the ANALYZE statement.
- sql: The ANALYZE statement can now be used in procedures, functions and triggers.

* Sat Jun 29 2024 Lucas Pereira <lucas.pereira@monetdbsolutions.com> - 11.51.1-20240812
- sql: Make schema renaming more permissive. A schema can be renamed if it
  does not contain objects that are a dependency for objects outside
  the schema. If such dependencies exist, they are shown in the
  table sys.dependencies.

* Sat Jun 29 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.51.1-20240812
- gdk: Made some changes to how BAT descriptors are allocated.  They are now
  allocated in bulk, meaning fewer malloc/free calls during processing.
- gdk: Removed macro BBP_cache and its associated code.  Checking whether a
  BAT is cached (loaded in memory) can be done by checking the BBPLOADED
  bit in the BBP_status value.  Getting a pointer to the BAT descriptor
  can be done by using BBP_desc.

* Sat Jun 29 2024 Yunus Koning <yunus.koning@monetdbsolutions.com> - 11.51.1-20240812
- sql: Introduce IS [NOT] DISTINCT FROM syntax. The syntax allows two values
  to be compared. The comparison always returns boolean FALSE or TRUE
  never NULL.

* Sat Jun 29 2024 Yunus Koning <yunus.koning@monetdbsolutions.com> - 11.51.1-20240812
- sql: SQL2023 feature: Introduce UNIQUE NULLS [NOT] DISTINCT syntax which
  allows for NULLS to be treated as unique, i.e. a column with this
  constraint can have one NULL value at most.
- sql: SQL2023 feature: Allow project and ORDER BY expressions on
  UNIQUE constrained columns when the primary key column is
  used in a GROUP BY expression.

* Sat Jun 29 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.51.1-20240812
- stream: CURL support has been removed from the stream library.  If support is
  needed, look at the source code in either streamcat.c or mclient.c.

* Sat Jun 29 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.51.1-20240812
- gdk: The SQL transaction ID is no longer saved in the BBP.dir file.

* Sat Jun 29 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.51.1-20240812
- clients: Msqldump now accepts --output and --outputdir options.  When the
  --outputdir option is used, the dump is placed in the file dump.sql in
  the specified directory and all tables are dumped to separate CSV files.
  In this way it is feasible to edit the dump script by hand if needed,
  even for a large database.

* Sat Jun 29 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.51.1-20240812
- clients: The --table (-t) option of msqldump now accepts SQL-style % wildcard
  characters to dump all tables that match the pattern.  E.g. -t
  %test%.%test% dumps all tables with 'test' in both the schema and
  table name.

* Sat Jun 29 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.51.1-20240812
- clients: Implemented interrupt handling in mclient.  When using mclient
  interactively, an interrupt (usually control-C) stops whatever the
  client is doing.  When editing a line, the line is discarded; when
  editing a second or later line of a query, the whole query is discarded;
  when a query is being executed, the server is asked to stop the query
  at its earliest convenience.  Stopping a running query can only be
  done with an up-to-date server.  All of this does not work on Windows.

* Sat Jun 29 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.51.1-20240812
- gdk: Made some changes to the TIMEOUT macros.  Most importantly, they
  now get a pointer to a QryCtx structure as argument instead of the
  timeout value.

* Sat Jun 29 2024 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.51.1-20240812
- sql: Add a DECIMAL AS clause to COPY INTO that configures the decimal separator
  and thousands separator for decimals, temporal types and floats.

* Sat Jun 29 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.11-20240629
- Rebuilt.
- GH#7509: MonetDB Dec2023-SP2 crashes at `sql_init_subtype`
- GH#7511: MonetDB Dec2023-SP2 crashes with the `PASSWORD_HASH` function
- GH#7512: MonetDB Dec2023-SP2 crashes with the
  `GET_MERGE_TABLE_PARTITION_EXPRESSIONS` function
- GH#7513: MonetDB Dec2023-SP2 crashes with the `GETHOST` function
- GH#7518: mserver reports errors when starting if geom module is enabled
  but database was created without
- GH#7526: deadlock, causing new connections to hang indefinitely
- GH#7531: loading more than 2147483647 rows gives issue.
- GH#7536: Truncated file when dumping a table from mclient into a gzipped
  file
- GH#7537: MonetDB crashes with a SIGSEGV due to a null pointer
  dereference when using prepared statements
- GH#7541: Unexpected result when using `LEVENSHTEIN`
- GH#7546: monetdbd leaks file descriptors when starting mserver5.

* Sat Jun 29 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.11-20240629
- monetdb5: The mserver5 program has a new option: --without-geom.  If the server
  was compiled with geom support (or the geom module was installed in a
  binary distribution), this option allows the server to start without
  the geom module, so that it can proceed with a database that was
  created without geom.

* Wed May 15 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.11-20240629
- sql: When sys.persist_unlogged is called for a table, it may return that
  zero rows were persisted.  If this is because the call was done too
  early, i.e. the table was recently created and the write-ahead log
  where this was logged has not been processed yet, the call will
  request an immediate write-ahead log rotation.  This means that the
  WAL will be processed as soon as possible and a new call to
  sys.persist_unlogged soon after will likely return a positive result.

* Thu May 02 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.9-20240502
- Rebuilt.
- GH#7422: Aggregate functions with variadic arguments
- GH#7472: MonetDB server crashes in `tail_type`
- GH#7473: MonetDB server crashes in `SQLunionfunc`
- GH#7478: MonetDB server crashes in `exp_equal`
- GH#7496: Query on view fails to produce a resultset. Assertion triggered
  in rel2bin_select.
- GH#7499: create schema + set schema inside a transaction that is rolled
  back causes the connection to be aborted
- GH#7501: files remain in backup causing problems at restart
- GH#7503: MonetDB server crashes using `WHEN MATCHED THEN UPDATE`
- GH#7504: possible deadlock when a bat is made persistent when it is also
  getting unloaded
- GH#7506: MonetDB Dec2023-SP2 crashes at `rel_value_exp2`
- GH#7507: BBPextend: ERROR: trying to extend BAT pool beyond the limit
  (163840000)
- GH#7508: MonetDB Dec2023-SP2 crashes at `exp_ref`

* Tue Apr 09 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.7-20240409
- Rebuilt.
- GH#7469: Crash when using `CONTAINS`
- GH#7479: MonetDB server crashes in `exp_ref`
- GH#7490: commonTerms optimizer no longer works
- GH#7495: Crash when simultaneously querying and updating a string column.

* Thu Mar 28 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.7-20240409
- gdk: Threads have their own list of free bats.  The list was not returned
  to the system when a thread exited, meaning that the free bats that
  were in the list would not be reused by any thread.  This has been
  fixed.

* Tue Mar 19 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.7-20240409
- monetdb5: Fixed interaction between mserver5 and remote mserver5 when only one
  of the two has 128 bit integer support.

* Tue Mar 19 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.7-20240409
- sql: Fixed issue where equal column aliases were created. When those
  aliases were parsed on the remote side it could give crashes.

* Mon Mar 18 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.7-20240409
- gdk: Fixed a couple of deadlock situations, one actually observed, one
  never observed.

* Tue Mar 12 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.5-20240312
- Rebuilt.
- GH#7390: Some MonetDB Server crashes found
- GH#7465: Unexpected result when using `NULL` in `BETWEEN`

* Fri Mar  8 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.5-20240312
- gdk: The internal hash function for floating point types has been changed.
  It is now no longer based on the bit representation, but on the value,
  meaning that +0 and -0 (yes, they both exist in floating point) now
  hash to the same value.

* Thu Mar  7 2024 Lucas Pereira <lucas.pereira@monetdbsolutions.com> - 11.49.5-20240312
- sql: performance improvement of 'startswith' and 'endswith' filter functions
  for join operators

* Wed Mar  6 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.5-20240312
- clients: Fixed an issue where mclient wouldn't exit if the server it had
  connected to exited for whatever reason while the client was waiting
  for a query result.

* Mon Mar 04 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.3-20240304
- Rebuilt.
- GH#6800: Please add information_schema (ANSI SQL norm)
- GH#7152: Occasional dbfarm corruption upon database restart
- GH#7412: MonetDB server crashes in `vscanf`
- GH#7415: MonetDB server crashes in `HEAP_malloc`
- GH#7416: MonetDB server crashes in `atom_get_int`
- GH#7417: MonetDB server crashes in `trimchars`.
- GH#7418: MonetDB server crashes in `bind_col_exp`
- GH#7420: Performance issue with lower(string)
- GH#7425: The last statement, execution error, is a false positive?
- GH#7426: Unexpected result for INNER JOIN with IS NOT NULL
- GH#7428: Unexpected result when using BETWEEN operator
- GH#7429: Unexpected result when using `CASE WHEN`
- GH#7430: Unexpected result when using `AND` and `IS NOT NULL`
- GH#7431: [bug] Error code found, please confirm
- GH#7432: MonetDB server crashes in `dameraulevenshtein`
- GH#7433: MonetDB server crashes in `exp_atom`
- GH#7434: MonetDB server crashes in `exp_bin`
- GH#7435: MonetDB server crashes in `exp_copy`
- GH#7436: MonetDB server crashes in `exp_ref`
- GH#7437: MonetDB server crashes in `exp_values_set_supertype`
- GH#7438: MonetDB server crashes in `exps_bind_column`
- GH#7439: MonetDB server crashes in `exps_card`
- GH#7440: MonetDB server crashes in `gc_col`
- GH#7441: MonetDB server crashes in `is_column_unique`
- GH#7442: MonetDB server crashes in `mat_join2`
- GH#7443: MonetDB server crashes in `merge_table_prune_and_unionize`
- GH#7444: [bug] the table cannot be created because the reserved word is
  incorrectly set
- GH#7447: Unexpected result when using `BETWEEN` in `INNER JOIN`
- GH#7448: Unexpected result when using `AND`/`OR` chain
- GH#7450: Unexpected result when `CREATE VIEW` with `WHERE NULL`
- GH#7451: Unexpected result when using `BETWEEN` and `CAST`
- GH#7453: Cannot recover an msqldump
- GH#7455: Unexpected result when using `BETWEEN` with `BOOLEAN` values
- GH#7456: Crash when `INNER JOIN` with `VIEW`
- GH#7457: Unexpected result when using `AND` with `INTEGER`
- GH#7458: Unexpected result when using `SIGN`
- GH#7461: Crash by potentially use of bad escape characters
- GH#7462: Crash when using `BETWEEN AND`

* Fri Mar  1 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.3-20240304
- gdk: Fixed a regression where bats weren't always cleaned up when they
  weren't needed anymore.  In particular, after a DELETE FROM table query
  without a WHERE clause (which deletes all rows from the table), the
  bats for the table get replaced by new ones, and the old, now unused,
  bats weren't removed from the database.

* Mon Jan 15 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.3-20240304
- geom: We switched over to using the reentrant interface of the geos library.
  This fixed a number of bugs that would occur sporadically.

* Mon Jan 15 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.3-20240304
- sql: The function json.isvalid(json) incorrectly returned true if the
  argument was null.  It should return null.

* Thu Jan 11 2024 Sjoerd Mullender <sjoerd@acm.org> - 11.49.3-20240304
- MonetDB: The copyright for the MonetDB software has been transferred to the newly
  established MonetDB Foundation, a not-for-profit foundation with the
  express goal of furthering the MonetDB database system.  The license
  for the software does not change: MonetDB remains fully open source.

* Thu Dec 21 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.49.1-20231221
- Rebuilt.
- GH#6933: Add support for scalar function IFNULL(expr1, expr2)
- GH#7044: Improve error message regarding 3-level SQL names
- GH#7261: Misleading error message
- GH#7274: Aggregate function ST_Collect crashes mserver5
- GH#7376: Concurrency Issue: Second Python UDF Awaits Completion of First
  UDF
- GH#7391: SQL 2023 : greatest/least functions with unlimited arguments
  (not only 2)
- GH#7403: Join not recognized between two row_number() columns
- GH#7413: MonetDB server crashes in `BATcalcbetween_intern`

* Tue Dec 19 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.49.1-20231221
- monetdb5: Removed MAL functions bat.reuse and bat.reuseMap.

* Tue Dec 12 2023 Lucas Pereira <lucas.pereira@monetdbsolutions.com> - 11.49.1-20231221
- gdk: Introduced options wal_max_dropped, wal_max_file_age and
  wal_max_file_size that control the write-ahead log file rotation.

* Wed Dec  6 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.49.1-20231221
- monetdb5: The MAL functions io.import and io.export have been removed.

* Tue Dec  5 2023 Lucas Pereira <lucas.pereira@monetdbsolutions.com> - 11.49.1-20231221
- sql: Introduction of table returning function `persist_unlogged(schema
  string, table string)` that attempts to persist data in disk if
  "schema"."table" is unlogged table in insert only mode.  If persist
  attempt is successful, the count of the persisted rows is returned,
  otherwise the count is 0.

* Fri Dec  1 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.49.1-20231221
- MonetDB: All binary packages are now signed with a new key with key fingerprint
  DBCE 5625 94D7 1959 7B54  CE85 3F1A D47F 5521 A603.

* Thu Nov 30 2023 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.49.1-20231221
- odbc: Corrected the output value of column CHAR_OCTET_LENGTH of ODBC functions
  SQLColumns() and SQLProcedureColumns().

* Thu Nov 23 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.49.1-20231221
- geom: Because recent changes to the geom module require the use of geos
  3.10, the geom module is no longer available in older versions of
  Debian and Ubuntu.  Specifically, Debian 10 and 11 (buster and
  bullseye) and Ubuntu 20.04 (Focal Fossa) are affected.  There is no
  automatic upgrade available for databases that were geom enabled to
  databases that are not, so dump + restore is the only option (if no
  geom types are actually used).

* Thu Nov 23 2023 stefanos mavros <stemavros@gmail.com> - 11.49.1-20231221
- geom: Implements Rtree index in GDK layer based on librtree. The index is
  used in the implementation of the filter functions ST_Intersects and
  ST_Dwithin for geometric points.
- geom: Improves shapefile support by replacing functions SHPattach,
  SHPpartialimport, ahd SHPimport with SHPload.
- geom: Introduces functions ST_DistanceGeographic, ST_DwithinGeographic,
  ST_IntersectsGeographic, ST_CoversGeographic, ST_Collects with geodesic
  semantics. ST_Transform can be used to convert geodetic into geographic
  data using libPROJ.

* Tue Nov 21 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.49.1-20231221
- gdk: Fixed a (rare) race condition between copying a bat (COLcopy) and
  updates happening in parallel to that same bat.  This may only be
  an actual problem with string bats, and then only in very particular
  circumstances.

* Mon Nov 20 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.49.1-20231221
- gdk: Removed function BATroles to set column names on BATs.

* Mon Nov 20 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.49.1-20231221
- monetdb5: Removed MAL functions bat.getRole and bat.setColumn since the
  underlying function BATroles was removed.

* Thu Nov 16 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.49.1-20231221
- gdk: Removed the compiled-in limit on the number of threads that can be used.
  The number of threads are still limited, but the limit is dictated
  solely by the operating system and the availability of enough memory.

* Thu Nov 16 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.49.1-20231221
- MonetDB: The ranges of merge partitions are now pushed down into the low
  level GDK operations, giving them a handle to sometimes execute more
  efficiently.

* Thu Nov 16 2023 Panagiotis Koutsourakis <kutsurak@monetdbsolutions.com> - 11.49.1-20231221
- monetdb5: Change how json is stored in the database: We now normalize json
  strings after parsing, removing whitespace and eliminating duplicate
  keys in objects.
- monetdb5: The function json.filter now properly returns json scalars instead of
  wrapping them in an array.

* Thu Nov 16 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.49.1-20231221
- gdk: We now prevent accidental upgrades from a database without 128 bit
  integers to one with 128 bit integers (also known as HUGEINT) from
  happening.  Upgrades will only be done if the server is started with
  the option --set allow_hge_upgrade=yes.

* Thu Nov 16 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.49.1-20231221
- monetdb5: Removed the MAL tokenizer module.  It was never usable from SQL and
  in this form never would be.

* Thu Nov 16 2023 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.49.1-20231221
- sql: Added ISO/IEC 9075-11 SQL/Schemata (SQL:2011) with SQL system views:
   information_schema.schemata
   information_schema.tables
   information_schema.views
   information_schema.columns
   information_schema.character_sets
   information_schema.check_constraints
   information_schema.table_constraints
   information_schema.referential_constraints
   information_schema.routines
   information_schema.parameters
   information_schema.sequences
  For details see
  https://www.monetdb.org/documentation/user-guide/sql-catalog/information_schema/
  Most views have been extended (after the standard columns) with MonetDB
  specific information columns such as schema_id, table_id, column_id, etc.
  This simplifies filtering and joins with system tables/views in sys schema
  when needed.
  Note: MonetDB does NOT support catalog qualifiers in object names, so all the
  CATALOG columns in these information_schema views will always return NULL.

* Thu Nov 16 2023 Niels Nes <niels.nes@monetdbsolutions.com> - 11.49.1-20231221
- sql: Added support for generated column syntax:
   GENERATED BY DEFAULT AS IDENTITY ...
  This allows the user to override the default generated sequence value
  during inserts.

* Thu Nov 16 2023 Niels Nes <niels@cwi.nl> - 11.49.1-20231221
- MonetDB: Removed the PYTHON MAP external language option, as after a fork the
  synchronization primitives could be in any state, leading to deadlocks.
  During the upgrade function definitions will fallback to the normal
  PYTHON language option.

* Thu Nov 16 2023 Panagiotis Koutsourakis <kutsurak@monetdbsolutions.com> - 11.49.1-20231221
- MonetDB: Implemented direct masking for strimp construction. The strimps
  datastructure now keeps an array of 65K 64-bit integers that is zero
  everywhere except at the indexes that correspond to header pairs. The
  entry for the nth pair in order has the nth bit of the bitstring
  on. These can be used to quickly construct bitstrings.

* Thu Nov 16 2023 Niels Nes <niels.nes@monetdbsolutions.com> - 11.49.1-20231221
- sql: Added SQL support for: <result offset clause> and <fetch first clause>
  in  <query expression> ::=
      [ <with clause> ] <query expression body>
      [ <order by clause> ]
      [ <result offset clause> ]
      [ <fetch first clause> ]
      [ <sample clause> ]
  <result offset clause> ::=
     OFFSET <offset row count> [ {ROW|ROWS} ]
  <fetch first clause> ::=
     FETCH {FIRST|NEXT} <fetch first row count> {ROW|ROWS} ONLY

