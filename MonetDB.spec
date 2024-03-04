# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

%global name MonetDB
%global version 11.49.3
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
# By default create the MonetDB-geom-MonetDB5 package on Fedora and RHEL 7
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
Source: https://www.monetdb.org/downloads/sources/Dec2023/%{name}-%{version}.tar.bz2

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
BuildRequires: pkgconfig(odbc)
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
# BuildRequires: pkgconfig(snappy)      # -DWITH_SNAPPY=ON
# BuildRequires: pkgconfig(valgrind)    # -DWITH_VALGRIND=ON

%if (0%{?fedora} >= 22)
Recommends: %{name}-SQL-server5%{?_isa} = %{version}-%{release}
Recommends: MonetDB5-server%{?_isa} = %{version}-%{release}
Suggests: %{name}-client%{?_isa} = %{version}-%{release}
%endif

%description
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the core components of MonetDB in the form of a
single shared library.  If you want to use MonetDB, you will certainly
need this package, but you will also need at least the MonetDB5-server
package, and most likely also %{name}-SQL-server5, as well as one or
more client packages.

%ldconfig_scriptlets

%files
%license COPYING
%defattr(-,root,root)
%{_libdir}/libbat.so.*

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
%{_libdir}/libbat.so
%{_libdir}/pkgconfig/monetdb-gdk.pc

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
%{_libdir}/libstream.so.*

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
%{_libdir}/libstream.so
%{_includedir}/monetdb/stream.h
%{_includedir}/monetdb/stream_socket.h
%{_libdir}/pkgconfig/monetdb-stream.pc

%package client-lib
Summary: MonetDB - Monet Database Management System Client Programs
Group: Applications/Databases
%if (0%{?fedora} >= 22)
Recommends: %{name}-SQL-server5%{?_isa} = %{version}-%{release}
Recommends: MonetDB5-server%{?_isa} = %{version}-%{release}
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
%{_libdir}/libmapi.so.*

%package client
Summary: MonetDB - Monet Database Management System Client Programs
Group: Applications/Databases
Requires: %{name}-client-lib%{?_isa} = %{version}-%{release}
%if (0%{?fedora} >= 22)
Recommends: %{name}-SQL-server5%{?_isa} = %{version}-%{release}
Recommends: MonetDB5-server%{?_isa} = %{version}-%{release}
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
%{_bindir}/mclient
%{_bindir}/msqldump
%{_mandir}/man1/mclient.1*
%{_mandir}/man1/msqldump.1*

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
%{_libdir}/libmapi.so
%{_includedir}/monetdb/mapi*.h
%{_includedir}/monetdb/msettings.h
%{_libdir}/pkgconfig/monetdb-mapi.pc

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

%package client-tests
Summary: MonetDB Client tests package
Group: Applications/Databases
Requires: MonetDB5-server%{?_isa} = %{version}-%{release}
Requires: %{name}-client%{?_isa} = %{version}-%{release}
Requires: %{name}-client-odbc%{?_isa} = %{version}-%{release}
%if (0%{?fedora} >= 22)
Recommends: perl-DBD-monetdb >= 1.0
Recommends: php-monetdb >= 1.0
%endif
Requires: MonetDB5-server%{?_isa} = %{version}-%{release}
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

%if %{with geos}
%package geom-MonetDB5
Summary: MonetDB5 SQL GIS support module
Group: Applications/Databases
Requires: MonetDB5-server%{?_isa} = %{version}-%{release}

%description geom-MonetDB5
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the GIS (Geographic Information System)
extensions for MonetDB5-server.

%files geom-MonetDB5
%defattr(-,root,root)
%{_libdir}/monetdb5/lib_geom.so
%endif

%if %{with rintegration}
%package R
Summary: Integration of MonetDB and R, allowing use of R from within SQL
Group: Applications/Databases
Requires: MonetDB5-server%{?_isa} = %{version}-%{release}

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
%{_libdir}/monetdb5/rapi.R
%{_libdir}/monetdb5/lib_rapi.so
%endif

%if %{with py3integration}
%package python3
Summary: Integration of MonetDB and Python, allowing use of Python from within SQL
Group: Applications/Databases
Requires: MonetDB5-server%{?_isa} = %{version}-%{release}
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
%{_libdir}/monetdb5/lib_pyapi3.so
%endif

%if %{with fits}
%package cfitsio
Summary: MonetDB: Add on module that provides support for FITS files
Group: Applications/Databases
Requires: MonetDB5-server%{?_isa} = %{version}-%{release}

%description cfitsio
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains a module for accessing data in the FITS file
format.

%files cfitsio
%defattr(-,root,root)
%{_libdir}/monetdb5/lib_fits.so
%endif

%package -n MonetDB5-libs
Summary: MonetDB - Monet Database Main Libraries
Group: Applications/Databases

%description -n MonetDB5-libs
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the MonetDB server component in the form of a set
of libraries.  You need this package if you want to use the MonetDB
database system, either as independent program (MonetDB5-server) or as
embedded library (%{name}-embedded).

%ldconfig_scriptlets -n MonetDB5-libs

%files -n MonetDB5-libs
%defattr(-,root,root)
%{_libdir}/libmonetdb5.so.*
%{_libdir}/libmonetdbsql.so*
%dir %{_libdir}/monetdb5
%if %{with cintegration}
%{_libdir}/monetdb5/lib_capi.so
%endif
%{_libdir}/monetdb5/lib_csv.so
%{_libdir}/monetdb5/lib_generator.so

%package -n MonetDB5-server
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Requires(pre): shadow-utils
Requires: %{name}-client%{?_isa} = %{version}-%{release}
Requires: MonetDB5-libs%{?_isa} = %{version}-%{release}
Obsoletes: MonetDB5-server-hugeint < 11.38.0
%if %{with hugeint}
Provides: MonetDB5-server-hugeint%{?_isa} = %{version}-%{release}
%endif
%if (0%{?fedora} >= 22)
Recommends: %{name}-SQL-server5%{?_isa} = %{version}-%{release}
Suggests: %{name}-client%{?_isa} = %{version}-%{release}
%endif
Requires(pre): systemd

%description -n MonetDB5-server
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the MonetDB server component.  You need this
package if you want to use the MonetDB database system.  If you want to
use the monetdb and monetdbd programs to manage your databases
(recommended), you also need %{name}-SQL-server5.

%pre -n MonetDB5-server
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

%files -n MonetDB5-server
%defattr(-,root,root)
%{_sysusersdir}/monetdb.conf
%attr(2750,monetdb,monetdb) %dir %{_localstatedir}/lib/monetdb
%attr(2770,monetdb,monetdb) %dir %{_localstatedir}/monetdb5
%attr(2770,monetdb,monetdb) %dir %{_localstatedir}/monetdb5/dbfarm
%{_bindir}/mserver5
%{_mandir}/man1/mserver5.1*
%dir %{_datadir}/doc/MonetDB
%docdir %{_datadir}/doc/MonetDB
%{_datadir}/doc/MonetDB/*

%package -n MonetDB5-server-devel
Summary: MonetDB development files
Group: Applications/Databases
Requires: MonetDB5-libs%{?_isa} = %{version}-%{release}
Requires: %{name}-devel%{?_isa} = %{version}-%{release}

%description -n MonetDB5-server-devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains files needed to develop extensions that can be
used from the MAL level.

%files -n MonetDB5-server-devel
%defattr(-,root,root)
%{_includedir}/monetdb/mal*.h
%{_includedir}/monetdb/mel.h
%{_libdir}/libmonetdb5.so
%{_libdir}/pkgconfig/monetdb5.pc

%package SQL-server5
Summary: MonetDB5 SQL server modules
Group: Applications/Databases
Requires(pre): MonetDB5-server%{?_isa} = %{version}-%{release}
Obsoletes: %{name}-SQL-server5-hugeint < 11.38.0
%if %{with hugeint}
Provides: %{name}-SQL-server5-hugeint%{?_isa} = %{version}-%{release}
%endif
%if (0%{?fedora} >= 22)
Suggests: %{name}-client%{?_isa} = %{version}-%{release}
%endif
%{?systemd_requires}

%description SQL-server5
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the monetdb and monetdbd programs and the systemd
configuration.

%post SQL-server5
%systemd_post monetdbd.service

%preun SQL-server5
%systemd_preun monetdbd.service

%postun SQL-server5
%systemd_postun_with_restart monetdbd.service

%files SQL-server5
%defattr(-,root,root)
%{_bindir}/monetdb
%{_bindir}/monetdbd
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

%package SQL-server5-devel
Summary: MonetDB5 SQL server modules
Group: Applications/Databases
Requires: %{name}-SQL-server5%{?_isa} = %{version}-%{release}
Requires: MonetDB5-server-devel%{?_isa} = %{version}-%{release}

%description SQL-server5-devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains files needed to develop SQL extensions.

%files SQL-server5-devel
%defattr(-,root,root)
%{_includedir}/monetdb/exception_buffer.h
%{_includedir}/monetdb/opt_backend.h
%{_includedir}/monetdb/rel_*.h
%{_includedir}/monetdb/sql*.h
%{_includedir}/monetdb/store_*.h

%package embedded
Summary: MonetDB as an embedded library
Group: Applications/Databases
Requires: MonetDB5-libs%{?_isa} = %{version}-%{release}

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
%{_bindir}/Mconvert.py
%{_bindir}/Mtest.py
%{_bindir}/Mz.py
%{_bindir}/mktest.py
%{_bindir}/sqllogictest.py
%dir %{python3_sitelib}/MonetDBtesting
%{python3_sitelib}/MonetDBtesting/*

%package selinux
Summary: SELinux policy files for MonetDB
Group: Applications/Databases
%if "%{?_selinux_policy_version}" != ""
Requires:       selinux-policy >= %{?_selinux_policy_version}
%endif
Requires(post):   MonetDB5-server%{?_isa} = %{version}-%{release}
Requires(postun): MonetDB5-server%{?_isa} = %{version}-%{release}
Requires(post):   %{name}-SQL-server5%{?_isa} = %{version}-%{release}
Requires(postun): %{name}-SQL-server5%{?_isa} = %{version}-%{release}
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
# use /var/run/monetdb since that's what it says in the monetdb.fc file
# it says that because /run/monetdb for some reason doesn't work
/sbin/restorecon -R %{_localstatedir}/monetdb5 %{_localstatedir}/log/monetdb /var/run/monetdb %{_bindir}/monetdbd %{_bindir}/mserver5 %{_unitdir}/monetdbd.service &> /dev/null || :
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
  /sbin/restorecon -R %{_localstatedir}/monetdb5 %{_localstatedir}/log/monetdb %{_rundir}/monetdb %{_bindir}/monetdbd %{_bindir}/mserver5 %{_unitdir}/monetdbd.service &> /dev/null || :
  if [ $active = active ]; then
    /usr/bin/systemctl start monetdbd.service
  fi
fi

%files selinux
%defattr(-,root,root,0755)
%docdir %{_datadir}/doc/MonetDB-selinux
%{_datadir}/doc/MonetDB-selinux/*
%{_datadir}/selinux/*/monetdb.pp

%prep
%setup -q

%build
%cmake3 \
        -DCMAKE_INSTALL_RUNSTATEDIR=/run \
        -DRELEASE_VERSION=ON \
        -DASSERT=OFF \
        -DCINTEGRATION=%{?with_cintegration:ON}%{!?with_cintegration:OFF} \
        -DFITS=%{?with_fits:ON}%{!?with_fits:OFF} \
        -DGEOM=%{?with_geos:ON}%{!?with_geos:OFF} \
        -DINT128=%{?with_hugeint:ON}%{!?with_hugeint:OFF} \
        -DNETCDF=OFF \
        -DODBC=ON \
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
        -DWITH_SNAPPY=OFF \
        -DWITH_VALGRIND=OFF \
        -DWITH_XML2=ON \
        -DWITH_ZLIB=ON

%cmake3_build

%install
mkdir -p "%{buildroot}/usr"
for d in etc var; do mkdir "%{buildroot}/$d"; ln -s ../$d "%{buildroot}/usr/$d"; done
%cmake3_install
rm "%{buildroot}/usr/var" "%{buildroot}/usr/etc"

# move file to correct location
mkdir -p %{buildroot}%{_tmpfilesdir} %{buildroot}%{_sysusersdir}
mv %{buildroot}%{_sysconfdir}/tmpfiles.d/monetdbd.conf %{buildroot}%{_tmpfilesdir}
cat > %{buildroot}%{_sysusersdir}/monetdb.conf << EOF
u monetdb - "MonetDB Server" /var/lib/monetdb
EOF
rmdir %{buildroot}%{_sysconfdir}/tmpfiles.d

install -d -m 0750 %{buildroot}%{_localstatedir}/lib/monetdb
install -d -m 0770 %{buildroot}%{_localstatedir}/monetdb5/dbfarm
install -d -m 0775 %{buildroot}%{_localstatedir}/log/monetdb
install -d -m 0775 %{buildroot}%{_rundir}/monetdb

# remove unwanted stuff
# .la files
rm -f %{buildroot}%{_libdir}/*.la
rm -f %{buildroot}%{_libdir}/monetdb5/*.la
rm -f %{buildroot}%{_libdir}/monetdb5/lib_opt_sql_append.so
rm -f %{buildroot}%{_libdir}/monetdb5/lib_microbenchmark*.so
rm -f %{buildroot}%{_libdir}/monetdb5/lib_udf*.so
rm -f %{buildroot}%{_bindir}/monetdb_mtest.sh
rm -rf %{buildroot}%{_datadir}/monetdb # /cmake

if [ -x /usr/sbin/hardlink ]; then
    /usr/sbin/hardlink -cv %{buildroot}%{_datadir}/selinux
else
    # Fedora 31
    /usr/bin/hardlink -cv %{buildroot}%{_datadir}/selinux
fi

# update shebang lines for Python scripts
%if %{?py3_shebang_fix:1}%{!?py3_shebang_fix:0}
    # Fedora has py3_shebang_fix macro
    %{py3_shebang_fix} %{buildroot}%{_bindir}/*.py
%else
    # EPEL does not, but we can use the script directly
    /usr/bin/pathfix.py -pni "%{__python3} -s" %{buildroot}%{_bindir}/*.py
%endif

%changelog
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

* Thu Nov 16 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.17-20231116
- Rebuilt.

* Thu Nov 16 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.17-20231116
- gdk: Fixed a regression where after a while the write-ahead log files
  weren't being rotated, meaning from some point onwards, the newest
  file just kept on growing.

* Thu Nov 09 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.15-20231109
- Rebuilt.
- GH#7410: SIGSEGV cause database corruption

* Tue Nov  7 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.15-20231109
- gdk: When saving the SQL catalog during a low-level commit, we should
  only save the part of the catalog that corresponds to the part of the
  write-ahead log that has been processed.  What we did was save more,
  which resulted in the catalog containing references to tables and
  columns whose disk presence is otherwise only in the write-ahead log.

* Fri Nov 03 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.13-20231103
- Rebuilt.
- GH#7300: Implement missing standard SQL DATE and TIMESTAMP functions
- GH#7324: string_distance('method',str1, str2) as a generic distance
  function
- GH#7409: Numpy table returning UDFs with variadic arguments

* Thu Nov  2 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.13-20231103
- sql: Added a missing interface function sys.timestamp_to_str with
  a TIMESTAMP (as opposed to TIMESTAMP WITH TIME ZONE) argument.
  The missing interface caused error messages being produced when the
  function was called with a TIMESTAMP argument, although it did give
  the correct result.

* Tue Oct 31 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.13-20231103
- gdk: A bug was fixed where the administration of which bats were in use was
  interpreted incorrectly during startup, causing problems later.  One
  symptom that has been observed was failure to startup with a message
  that the catalog tables could not be loaded.

* Fri Sep 29 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.11-20230929
- Rebuilt.

* Fri Sep 29 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.11-20230929
- MonetDB: Fixed an installation issue on Debian and Ubuntu introduced in the
  last build.

* Wed Sep 27 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.9-20230927
- Rebuilt.
- GH#7402: Privileges on merge table not propagated to partition tables

* Mon Sep 25 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.7-20230925
- Rebuilt.
- GH#7094: Drop remote tables in transactions and rollback
- GH#7303: Improve the performance of multi-column filters
- GH#7400: VM max memory is not check correctly for cgroups v2
- GH#7401: Column aliases used incorrectly in UNION subqueries

* Fri Sep 22 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.7-20230925
- gdk: Fixed a number of data races (race conditions).

* Mon Sep 18 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.7-20230925
- gdk: Fixed a reference counting problem when a BAT could nog be loaded,
  e.g. because of resource limitations.

* Wed Aug 30 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.7-20230925
- gdk: Only check for virtual memory limits when creating or growing bats,
  not for general memory allocations.  There is (still) too much code
  that doesn't properly handle failing allocations, so we need to avoid
  those as much as possible.  This has mostly an effect if there are
  virtual memory size restrictions imposed by cgroups (memory.swap.max
  in cgroups v2, memory.memsw.limit_in_bytes in cgroups v1).
- gdk: The low-level commit turned out to always commit every persistent bat
  in the system.  There is no need for that, it should only commit bats
  that were changed.  This has now been fixed.
- gdk: Implemented timeout/exit checks in a bunch more operators.  Long(er)
  running operators occasionally check whether they're taking too long
  (past a user-specified timeout) or whether the server is exiting.
  This is now done in more places.

* Wed Aug 30 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.7-20230925
- MonetDB: Do a lot more error checking, mostly for allocation failures.  More is
  still needed, though.

* Thu Aug 10 2023 Panagiotis Koutsourakis <kutsurak@monetdbsolutions.com> - 11.47.7-20230925
- MonetDB: Improve performance of the ILIKE operator when the pattern contains only
  ASCII characters. In this case we do not need to treat any characters as
  UTF-8 and we can use much faster routines that perform byte comparisons.

* Tue Jul 18 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.5-20230718
- Rebuilt.
- GH#7388: Query results in large cross product
- GH#7394: BBPextend: ERROR: trying to extend BAT pool beyond the limit
  (163840000)

* Thu Jun 22 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.5-20230718
- sql: An upgrade that both creates a new .snapshot user and extends the
  sys.db_user_info table with (among others) a password column did
  these in such a way that the passord value for the new user was NULL.
  This is fixed by updating the password.

* Thu Jun 22 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.5-20230718
- monetdb5: There is now a new option --set tablet_threads=N to limit the number
  of threads used for a COPY INTO from CSV file query.  This option can
  also be set for a specific database using the monetdb command using
  the ncopyintothreads property.

* Thu Jun 22 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.3-20230622
- Rebuilt.
- GH#7344: Database upgrade failure due to user object dependency on
  system procedure
- GH#7378: MonetDB server crashes at sql_trans_copy_key
- GH#7379: MonetDB server 11.46.0 crashes at cs_bind_ubat
- GH#7380: MonetDB server 11.46.0 crashes at `BLOBcmp`
- GH#7381: MonetDB server 11.46.0 crashes at `log_create_delta`
- GH#7382: MonetDB server 11.46.0 crashes at `gc_col`
- GH#7383: MonetDB server 11.46.0 crashes at `list_append`
- GH#7384: MonetDB server 11.46.0 crashes at `__nss_database_lookup`
- GH#7386: MonetDB server 11.46.0 crashes in `rel_deps`
- GH#7387: MonetDB server 11.46.0 crashes in `rel_sequences`

* Tue Jun 20 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.3-20230622
- clients: The COPY INTO from file ON CLIENT was extended to also look for a
  relative path name relative to the file from which the query was read.
  This is only possible if the name of the query file is known, so when
  it is specified on the command line or read using the interactive
  \< command.

* Fri Jun 16 2023 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.47.3-20230622
- sql: Add MAX_MEMORY and MAX_WORKERS options to the ALTER USER statement

* Fri Jun 16 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.1-20230616
- Rebuilt.
- GH#7311: Missing `REGEXP_REPLACE` function.
- GH#7348: Subquery inside case always evaluated

* Tue Jun  6 2023 Lucas Pereira <lucas.pereira@monetdbsolutions.com> - 11.47.1-20230616
- sql: Function 'similarity(x string, y string)' marked as deprecated and will
  be removed in the next release.

* Tue Jun  6 2023 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.47.1-20230616
- odbc: Extended Windows MonetDB ODBC Data Source setup program with option
  to specify a logfile to enable tracing of ODBC Driver API calls.
  On other platforms users can edit the  odbc.ini  file and add a line:
  logfile=/home/username/odbctrace.log
  When a logfile is specified it will start logging the ODBC Driver API calls
  to the logfile after a new connection is made via SQLConnect() or
  SQLDriverConnect() or SQLBrowseConnect().
  Note that enabling ODBC logging will slow down the performance of ODBC
  applications, so enable it only for analysing ODBC Driver problems.

* Tue Jun  6 2023 Lucas Pereira <lucas.pereira@monetdbsolutions.com> - 11.47.1-20230616
- sql: New functionality for string matching and similarity: startswith,
  endswith, contains, Levenshtein distance and Jaro-Winkler similarity.
  The functions startswith, endswith and contains have a version where
  a case insentive flag can be used.
  Also, there are new custom join functionality for startswith, endswith,
  contains, Levenshtein distance and Jaro-Winkler similarity.
- sql: Renamed previous Levenshtein distance to Damerau-Levenshtein distance.
- sql: New string function that transform from UTF-8 encoding to Ascii called
  asciify.

* Tue Jun  6 2023 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.47.1-20230616
- odbc: Enhanced SQLTables() by adding support for table type names: 'BASE TABLE',
  'GLOBAL TEMPORARY' and 'LOCAL TEMPORARY' in parameter TableType. These are
  synonyms of: 'TABLE', 'GLOBAL TEMPORARY TABLE' and 'LOCAL TEMPORARY TABLE'.

* Tue Jun  6 2023 Lucas Pereira <lucas.pereira@monetdbsolutions.com> - 11.47.1-20230616
- sql: Session timeout feature improvement to start evaluating from the moment
  the procedure is called, instead of beginning of the session.

* Tue Jun  6 2023 Lucas Pereira <lucas.pereira@monetdbsolutions.com> - 11.47.1-20230616
- sql: Queries stopped with the stop procedure are now marked as 'aborted'
  'finished'.

* Tue Jun  6 2023 Niels Nes <niels@monetdbsolutions.com> - 11.47.1-20230616
- monetdb5: The MAL debugger code has been removed.

* Tue Jun  6 2023 Niels Nes <niels@monetdbsolutions.com> - 11.47.1-20230616
- sql: The DEBUG statement has been removed.

* Tue Jun  6 2023 Lucas Pereira <lucas.pereira@monetdbsolutions.com> - 11.47.1-20230616
- sql: SQL function sys.queue() overloaded with sys.queue(username string),
  SYSADMIN only, allowing to filter the global queue by username or
  use 'ALL' to retrieve the global queue. Calling the function without
  arguments returns the queue for the current user.
- sql: SQL procedures sys.pause(tag bigint), sys.resume(tag bigint),
  sys.stop(tag bigint) overloaded with sys.pause(tag bigint, username string),
  sys.resume(tag bigint, username string) and sys.stop(tag bigint, username
  string), SYSADMIN only, allowing to pause, resume and stop query
  executions by TAG, USERNAME. The call without arguments is a public
  procedure giving access to users to pause, resume and stop their
  own query executions.

* Tue Jun  6 2023 svetlin <svetlin.stalinov@monetdbsolutions.com> - 11.47.1-20230616
- sql: Added support of ODBC escape sequences syntax to SQL layer. Now all clients
  (including ODBC/JDBC/pymonetdb) can use them without further processing.
  For details on ODBC escape sequences syntax see:
  https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/odbc-escape-sequences

* Tue Jun  6 2023 Aris Koning <aris.koning@monetdbsolutions.com> - 11.47.1-20230616
- merovingian: The monetdb get, inherit, lock and release commands are extended with
  ‘apply-to-all’ syntax similar to related functionality:
  Usage: monetdb set property=value [database ...]
  sets property to value for the given database(s), or all
  Usage: monetdb inherit property [database ...]
  unsets property, reverting to its inherited value from the
  default configuration for the given database(s), or all
  Usage: monetdb lock [-a] database [database...]
  Puts the given database in maintenance mode.
  Options:
    -a  locks all known databases
  Usage: monetdb release [-a] database [database ...]
  Brings back a database from maintenance mode.
  Options:
    -a  releases all known databases

* Tue Jun  6 2023 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.47.1-20230616
- sql: It is no longer allowed to create a merge table or remote table or
  replica table or unlogged table in schema "tmp". The tmp schema is
  reserved for temporary objects only, such as local/global temp tables.

* Tue Jun  6 2023 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.47.1-20230616
- sql: System views sys.dependency_tables_on_functions and
  dependency_views_on_functions have been extended with column: function_id.

* Tue Jun  6 2023 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.47.1-20230616
- mapilib: Deprecate mapi_setfilecallback() in favor of mapi_setfilecallback2() which
  can handle binary downloads. For the time being, the old callback still works.

* Tue Jun  6 2023 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.47.1-20230616
- sql: When loading data using COPY BINARY INTO, apply default values instead of just
  inserting NULLs.
- sql: When loading data using COPY BINARY INTO, validate DECIMAL(prec,scale) and
  VARCHAR(n) column width.
- sql: When loading data using COPY BINARY INTO, string used to have their line
  endings converted from CR LF to LF. Do not do this, it is the responsibility
  of the client.
- sql: Implemented dumping binary data using COPY SELECT ... INTO BINARY <file(s)>.

* Tue Jun  6 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.47.1-20230616
- sql: Removed code for Workload Capture and Replace, including system schemas
  "wlc" and "wlr" and the objects in those schemas. The code was
  experimental, and it didn't work out. A different approach will be taken.

