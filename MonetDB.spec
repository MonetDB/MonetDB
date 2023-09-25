# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.

%global name MonetDB
%global version 11.47.7
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
%if %{fedpkgs}
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
Vendor: MonetDB BV <info@monetdb.org>

Group: Applications/Databases
License: MPL-2.0
URL: https://www.monetdb.org/
BugURL: https://github.com/MonetDB/MonetDB/issues
Source: https://www.monetdb.org/downloads/sources/Jun2023-SP1/%{name}-%{version}.tar.bz2

# The Fedora packaging document says we need systemd-rpm-macros for
# the _unitdir and _tmpfilesdir macros to exist; however on RHEL 7
# that doesn't exist and we need systemd, so instead we just require
# the macro file that contains the definitions.
# We need checkpolicy and selinux-policy-devel for the SELinux policy.
BuildRequires: /usr/lib/rpm/macros.d/macros.systemd
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
BuildRequires: geos-devel >= 3.4.0
%endif
BuildRequires: pkgconfig(libcurl)
BuildRequires: pkgconfig(liblzma)
BuildRequires: pkgconfig(libxml-2.0)
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
%if (0%{?fedora} == 32)
# work around a packaging bug on Fedora 32 (18 Nov 2020)
# problem is things like:
# file /etc/texlive/web2c/updmap.cfg conflicts between attempted installs of texlive-tetex-7:20190410-12.fc32.noarch and texlive-texlive-scripts-7:20200327-16.fc32.noarch
# texlive-tetex is obsoleted by texlive-obsolete
BuildRequires: texlive-obsolete
%endif
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

%package client
Summary: MonetDB - Monet Database Management System Client Programs
Group: Applications/Databases
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
%{_libdir}/libmapi.so.*
%doc %{_mandir}/man1/mclient.1.gz
%doc %{_mandir}/man1/msqldump.1.gz

%package client-devel
Summary: MonetDB - Monet Database Management System Client Programs
Group: Applications/Databases
Requires: %{name}-client%{?_isa} = %{version}-%{release}
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
%{_libdir}/pkgconfig/monetdb-mapi.pc

%package client-odbc
Summary: MonetDB ODBC driver
Group: Applications/Databases
Requires: %{name}-client%{?_isa} = %{version}-%{release}
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

%package -n MonetDB5-server
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Requires(pre): shadow-utils
Requires: %{name}-client%{?_isa} = %{version}-%{release}
Obsoletes: MonetDB5-server-hugeint < 11.38.0
%if %{with hugeint}
Provides: MonetDB5-server-hugeint%{?_isa} = %{version}-%{release}
%endif
%if (0%{?fedora} >= 22)
Recommends: %{name}-SQL-server5%{?_isa} = %{version}-%{release}
Suggests: %{name}-client%{?_isa} = %{version}-%{release}
%endif
# versions up to 1.0.5 don't accept the queryid field in the result set
Conflicts: python-pymonetdb < 1.0.6
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
%{_libdir}/libmonetdb5.so.*
%{_libdir}/libmonetdbsql.so*
%dir %{_libdir}/monetdb5
%if %{with cintegration}
%{_libdir}/monetdb5/lib_capi.so
%endif
%{_libdir}/monetdb5/lib_generator.so
%doc %{_mandir}/man1/mserver5.1.gz
%dir %{_datadir}/doc/MonetDB
%docdir %{_datadir}/doc/MonetDB
%{_datadir}/doc/MonetDB/*

%package -n MonetDB5-server-devel
Summary: MonetDB development files
Group: Applications/Databases
Requires: MonetDB5-server%{?_isa} = %{version}-%{release}
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
%doc %{_mandir}/man1/monetdb.1.gz
%doc %{_mandir}/man1/monetdbd.1.gz
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
Requires: MonetDB5-server%{?_isa} = %{version}-%{release}

%description embedded
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the library to turn MonetDB into an embeddable
library, also known as MonetDBe.  Also see %{name}-embedded-devel to
use this in a program.

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
Requires: python3dist(pymonetdb) >= 1.0.6
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
Requires(post):   /usr/sbin/semodule, /sbin/restorecon, /sbin/fixfiles
Requires(postun): /usr/sbin/semodule, /sbin/restorecon, /sbin/fixfiles
BuildArch: noarch

%global selinux_types %(%{__awk} '/^#[[:space:]]*SELINUXTYPE=/,/^[^#]/ { if ($3 == "-") printf "%s ", $2 }' /etc/selinux/config 2>/dev/null)
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

* Tue Jun 06 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.17-20230606
- Rebuilt.

* Tue Jun  6 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.17-20230606
- sql: Fixed a regression introduced in the previous build: when a table is
  dropped in the same transaction where it was created, the data wasn't
  removed from disk.

* Tue May 30 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.15-20230530
- Rebuilt.
- GH#7360: Aggregates returning string crash
- GH#7361: Table data is lost on DB restart, but only when a table has
  2147483647 or more rows.

* Tue May 16 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.15-20230530
- gdk: Warnings and informational messages are now sent to stdout instead of
  stderr, which means that monetdbd will now log them with the tag MSG
  instead of ERR.

* Tue Apr 25 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.15-20230530
- gdk: Fixed parsing of the BBP.dir file when BAT ids grow larger than 2**24
  (i.e. 100000000 in octal).

* Thu Apr 20 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.15-20230530
- gdk: Fixed yet another occurrence of a missing .tailN file.  This one could
  happen if a string bat was appended to in stages so that between appends
  the column was committed.  If an append caused both a realloc of the
  tail heap because it was getting longer and a realloc because it got
  wider, the file might get removed before the GDK level commit happened.

* Mon Apr 17 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.15-20230530
- clients: If the number of rows in mclient is set to 0 (using either --rows=0
  option or \r0 on the mclient command line), the internal pager is used
  and it then uses the height of the terminal window.

* Wed Apr  5 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.15-20230530
- sql: When creating a hot snapshot, allow other clients to proceed, even
  with updating queries.

* Fri Mar 24 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.15-20230530
- gdk: When processing the WAL, if a to-be-destroyed object cannot be found,
  don't stop, but keep processing the rest of the WAL.

* Fri Mar 24 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.15-20230530
- monetdb5: Client connections are cleaned up better so that we get fewer instances
  of clients that cannot connect.

* Fri Mar 24 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.15-20230530
- sql: Increased the size of a variable counting the number of changes made
  to the database (e.g. in case more than 2 billion rows are added to
  a table).
- sql: Improved cleanup after failures such as failed memory allocations.

* Mon Feb 20 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.15-20230530
- gdk: A race condition was fixed where certain write-ahead log messages
  could get intermingled, resulting in a corrupted WAL file.
- gdk: If opening of a file failed when it was supposed to get memory mapped,
  an incorrect value was returned to indicate the failure, causing
  crashes later on.  This has been fixed.

* Thu Feb 16 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.15-20230530
- gdk: A race condition was fixed that could result in a missing tail file
  for a string bat (i.e. a file with .tail1, .tail2, or .tail4 extension).

* Mon Feb 13 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.15-20230530
- gdk: When saving a bat failed for some reason during a low-level commit,
  this was logged in the log file, but the error was then subsequently
  ignored, possibly leading to files that are too short or even missing.
- gdk: The write-ahead log (WAL) is now rotated a bit more efficiently by
  doing multiple log files in one go (i.e. in one low-level transaction).

* Mon Feb 13 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.15-20230530
- sql: An insert into a table from which a column was dropped in a parallel
  transaction was incorrectly not flagged as a transaction conflict.

* Thu Jan 26 2023 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.45.15-20230530
- sql: Fix bug where boolean NULLs were not recognized by COPY BINARY INTO

* Tue Jan 24 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.13-20230124
- Rebuilt.
- GH#7343: GDKmmap requesting 0 virtual memory
- GH#7347: A bug where an exception occurs even though it is a query with
  normal syntax (Merge Table)

* Mon Jan 23 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.13-20230124
- sql: Fixed a regression where when there are multiple concurrent
  transactions, the dependencies weren't checked properly.

* Mon Jan 16 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.13-20230124
- gdk: Fixed a race condition that could lead to a bat being added to the SQL
  catalog but nog being made persistent, causing a subsequent restart
  of the system to fail (and crash).

* Wed Jan  4 2023 Sjoerd Mullender <sjoerd@acm.org> - 11.45.13-20230124
- odbc: A crash in the ODBC driver was fixed when certain unsupported functions
  where used in a {fn ...} escape.

* Wed Dec 21 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.13-20230124
- odbc: Prepare of a query where the sum of the number of parameters (question
  marks in the query) and the number of output columns is larger than
  100 could fail with an unexpected error.  This has been fixed.

* Fri Dec 16 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.13-20230124
- sql: Added some error checking to prevent crashes.  Errors would mainly
  occur under memory pressure.

* Wed Dec 14 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.13-20230124
- gdk: Fixed a race condition where a hash could have been created on a
  bat using the old bat count while in another thread the bat count
  got updated.  This would make the hash be based on too small a size,
  causing failures later on.

* Wed Dec 14 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.13-20230124
- sql: Fixed cleanup after a failed allocation where the data being cleaned
  up was unitialized but still used as pointers to memory that also had
  to be freed.

* Thu Dec  8 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.13-20230124
- gdk: When extending a bat failed, the capacity had been updated already and
  was therefore too large.  This could then later cause a crash.  This has
  been fixed by only updating the capacity if the extend succeeded.

* Wed Dec  7 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.13-20230124
- sql: Fixed a double cleanup after a failed allocation in COPY INTO.  The
  double cleanup could cause a crash due to a race condition it enabled.

* Mon Dec 05 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.11-20221205
- Rebuilt.
- GH#7342: column which datatype is double couldn't group or aggregation
  in version 11.45.7

* Mon Nov 28 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.9-20221128
- Rebuilt.
- GH#7330: Creating temporary table fails after reconnect
- GH#7333: DLLs fail to load on Windows with accented characters in path
- GH#7336: Selecting from a literal-value table returns wrong values
- GH#7339: MonetDB corrupted state after SIGKILL

* Wed Nov  9 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.9-20221128
- clients: Also dump the new options of CREATE USER.

* Wed Nov  9 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.9-20221128
- gdk: On Windows, use the wide-character interface of system calls when
  dealing with the environment (i.e. file names and getenv()).
- gdk: Memory leaks have been fixed.
- gdk: Improved maintenance of the estimated number of distinct values in BATs.
  The estimate helps in deciding which low-level algorithm to use.

* Wed Nov  9 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.9-20221128
- monetdb5: Fixed a crash when the server runs out of client contexts (i.e. more
  concurrent clients than the server is configured to handle).
- monetdb5: A race condition in the SHA hash code was fixed which resulted in
  occasional failed connection attempts when they occurred concurrently.

* Wed Nov  9 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.9-20221128
- sql: Improved the handling of the "idle" value in the sys.sessions function
  and view.

* Wed Oct 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.9-20221128
- monetdb5: Fix a bug where the MAL optimizer would use the starttime of the
  previous query to determine whether a query timeout occurred.

* Thu Oct 13 2022 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.45.9-20221128
- odbc: Fixed issue with generated raw strings prefix when ODBC driver is used
  against a server older than Jun2020 (11.37).

* Wed Oct 12 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.9-20221128
- merovingian: Stop logging references to monetdbd's logfile in said logfile.

* Mon Oct 10 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.9-20221128
- gdk: Offset heaps (.tailN files) were growing too fast and unnecessarily
  under certain conditions.  This has been fixed.  Also, when such too
  large files are now loaded into the system, it is recognized they are
  too large and they are truncated to a more reasonable size.

* Fri Sep 23 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.7-20220923
- Rebuilt.

* Thu Sep 22 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.5-20220922
- Rebuilt.

* Wed Sep 21 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.3-20220921
- Rebuilt.

* Wed Sep 21 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.3-20220921
- clients: Dumping of function GRANTs was improved by adding the types of the
  function (and procedure) arguments.

* Wed Sep 21 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.3-20220921
- sql: The function sys.tracelog is now executable by anyone.  The function
  (and view of the same name) returns the tracing information of a query
  that was prepended with the TRACE keyword.

* Mon Sep 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.3-20220921
- gdk: Fixed a bug in ORDER BY with both NULLS LAST and LIMIT when the ordering
  was on an interger or integer-like column and NULL values are present.

* Mon Sep 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.3-20220921
- sql: Fixed a bug in COPY BINARY INTO where the input wasn't checked
  thoroughly enough.

* Tue Sep 13 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.3-20220921
- gdk: The median_avg and quantile_avg returned bogus results in the
  non-grouped case (i.e. something like SELECT sys.median_avg(i) FROM t).

* Tue Sep 13 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.1-20220913
- Rebuilt.
- GH#6982: Wrong constraint name in error message of constraint violation
- GH#7209: Configuration option for merovingian.log
- GH#7225: Invalid memory access when extending a BAT during appends
- GH#7227: Date calculations, bug or feature
- GH#7273: Concurrent reads and writes causes "BATproject2: does not match
  always" error
- GH#7282: call sys.dump_table_data(); fails
- GH#7285: C-UDFs: aggr_group.count has wrong value (number of input rows
  instead of number of groups).
- GH#7296: Implictly cast a timestamp string to DATE when appropriate
- GH#7297: Parsing partial dates behaves unpredictable
- GH#7306: ODBC Driver Assertion `stmt->Dbc->FirstStmt' Failed
- GH#7314: ODBC Driver : please mask/hide password
- GH#7318: distinct in a subquery not working properly

* Thu Sep  1 2022 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.45.1-20220913
- odbc: Improved SQLSetPos(). It now allows RowNumber to be 0 when Operation
  is SQL_POSITION.

* Fri Aug 26 2022 Lucas Pereira <lucas.pereira@monetdbsolutions.com> - 11.45.1-20220913
- sql: Extended the built-in profiler to emit non-MAL events related to query
  compilation, optimization, transactions and client connections. To
  minimize, simplify and optimize the process of generating and processing
  profiler output, only the events marking the end of an operation are
  emitted in most cases and the emitted json messages themselves are
  trimmed down to their essential fields. Furthermore, the MAL instruction
  profiler.openstream now requires an integer as a single argument, "0" for
  default behaviour or "4" to turn on the profiler in minimal mode,
  which causes it to only emit general events and excludes individual MAL
  instruction execution events from the profiler streams.
  The MAL instruction profiler.openstream with zero arguments is deprecated.

* Thu Aug 25 2022 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.45.1-20220913
- sql: Extended system view sys.users with columns: schema_path, max_memory,
  max_workers, optimizer and default_role.
  Extended system table sys.db_user_info with columns: schema_path,
  max_memory, max_workers, optimizer, default_role and password.
  The password is encrypted. This table can only be queried when the
  user has been granted the select privilege from monetdb.

* Wed Aug 24 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.1-20220913
- gdk: The median_avg and quantile_avg returned bogus results in the
  non-grouped case (i.e. something like SELECT sys.median_avg(i) FROM t).

* Wed Aug 24 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.1-20220913
- merovingian: In certain cases (when an mserver5 process exits right after producing
  a message) the log message was logged over and over again, causing
  monetdbd to use 100% CPU.  This has been fixed.

* Fri Aug 19 2022 svetlin <svetlin.stalinov@monetdbsolutions.com> - 11.45.1-20220913
- sql: CREATE USER statement has been extended to take more optional arguments
  like MAX_MEMORY, MAX_WORKERS, OPTIMIZER, DEFAULT ROLE. ALTER USER statement
  has also been extended with DEFAULT ROLE.

* Fri Aug 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.1-20220913
- sql: A new function sys.url_extract_host(string, bool) which returns the
  host name from the given URL has been implemented.  The bool argument,
  if true, causes the www. prefix to be removed.

* Fri Aug 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.1-20220913
- gdk: The abort_on_error parameter of all GDK-level functions has been removed.
  Errors (e.g. overflow) now also results in an error.

* Fri Aug 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.1-20220913
- sql: The user authentication tables are now administered by the SQL layer
  instead of the MAL layer.  This means that changing (adding, dropping,
  altering) user and role information is now transaction-safe.

* Fri Aug 19 2022 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.45.1-20220913
- odbc: Improved ODBC Error/Warning messages. They now include the name of the
  Data Source as required by the ODBC specification:
  [MonetDB][ODBC driver VERSION][data-source-name] data-source-supplied-text

* Fri Aug 19 2022 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.45.1-20220913
- odbc: Improved MonetDB ODBC Data Source Configuration dialog on MS Windows by
  hiding the typed in password text. It now shows dots for the characters.
  This fixes request  https://github.com/MonetDB/MonetDB/issues/7314

* Fri Aug 19 2022 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.45.1-20220913
- odbc: Changed output of TABLE_CAT or PROCEDURE_CAT result columns as returned
  by ODBC functions: SQLTables(), SQLColumns(), SQLPrimaryKeys(),
  SQLForeignKeys(), SQLStatistics(), SQLTablePrivileges(),
  SQLColumnPrivileges(), SQLProcedures() and SQLProcedureColumns().
  They used to return the static database name but now they will return
  NULL as MonetDB does not support CATALOG objects or qualifiers.

* Fri Aug 19 2022 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.45.1-20220913
- odbc: Removed the possibility to retrieve or set the CURRENT_CATALOG
  via SQLGetConnectAttr(hdbc, SQL_ATTR_CURRENT_CATALOG, ...) and
  SQLSetConnectAttr(hdbc, SQL_ATTR_CURRENT_CATALOG, ...) as MonetDB does
  not support CATALOG objects (no SQL support for: CREATE CATALOG abc
  or SET CATALOG abc) and therefore there is no CURRENT_CATALOG.

* Fri Aug 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.1-20220913
- gdk: Implemented BC/AD (and BCE/CE) suffixes when parsing dates.

* Fri Aug 19 2022 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.45.1-20220913
- odbc: Corrected ODBC functions SQLTablePrivileges() and SQLColumnPrivileges()
  for local temporary tables located in schema tmp. They did not return
  any rows when the temporary table had privileges set. Now they do return
  rows as expected.

* Fri Aug 19 2022 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.45.1-20220913
- odbc: Improved SQLProcedures() and SQLProcedureColumns(). They now list
  information also for all built-in system procedures and functions, not
  only those created via SQL. Also corrected the value of ORDINAL_POSITION
  for scalar function arguments. It would start at 2 instead of 1.
- odbc: Extended output of SQLProcedures() and SQLProcedureColumns() resultsets
  with an extra column SPECIFIC_NAME. This column contains the name which
  uniquely identifies this procedure or function within its schema. As
  MonetDB supports overloaded procedures and functions, the combination of
  PROCEDURE_SCHEM and PROCEDURE_NAME is not enough to uniquely identify
  a procedure or function. This extra column allows you to correctly
  match the corresponding rows returned by SQLProcedureColumns() with the
  specific rows of SQLProcedures(). This extra column SPECIFIC_NAME is
  implemented similar to the JDBC DatabaseMetaData methods getProcedures()
  and getProcedureColumns().

* Fri Aug 19 2022 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.45.1-20220913
- odbc: For SQLForeignKeys() corrected the output of columns UPDATE_RULE and
  DELETE_RULE. These columns used to always return 3 (= SQL_NO_ACTION)
  but now they will report the action codes as specified in the FOREIGN KEY
  CONSTRAINT construction.

* Fri Aug 19 2022 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.45.1-20220913
- odbc: Fixed issue in function SQLProcedureColumns(). When the argument ColumnName
  was not empty it generated an invalid SQL query which failed with error:
   SELECT: no such column 'c.name'. This has been resolved.
- odbc: Corrected implementation of SQLGetInfo(hdbc, SQL_MAX_DRIVER_CONNECTIONS, ...).
  It used to always return 64. Now it returns the value from the MonetDB server.
- odbc: Changed the column names case of the result sets as returned by
  SQLTables(), SQLColumns(), SQLSpecialColumns(), SQLPrimaryKeys(),
  SQLForeignKeys(), SQLStatistics(), SQLTablePrivileges(),
  SQLColumnPrivileges(), SQLProcedures() and SQLProcedureColumns(). The
  column names where all in lowercase but the ODBC specification defines
  them in uppercase, so changed them to uppercase.

* Fri Aug 19 2022 Panagiotis Koutsourakis <kutsurak@monetdbsolutions.com> - 11.45.1-20220913
- gdk: The interface for using strimps has not changed (create an imprint index
  on a column of a read only table), but now construction happens at the
  beginning of the first query that uses the strimp and is performed in
  a multithreaded manner.

* Fri Aug 19 2022 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.45.1-20220913
- odbc: Corrected SQLSpecialColumns(..., SQL_BEST_ROWID, ...). Previously it only
  returned rows when the table had a primary key. Now it also returns
  rows when a table has a unique constraint.
- odbc: Corrected SQLStatistics(..., SQL_INDEX_ALL, ...). Previously it only
  returned rows when the table had a primary or unique key. Now it also
  returns rows for indices which are not based on a key.
- odbc: Corrected SQLStatistics(..., SQL_ENSURE). It now returns CARDINALITY
  information for columns based on a primary/unique key. Previously it
  always returned NULL for the CARDINALITY result column.

* Fri Aug 19 2022 Panagiotis Koutsourakis <kutsurak@monetdbsolutions.com> - 11.45.1-20220913
- gdk: Implemented the use of strimps for NOT LIKE queries. The idea is to
  run the corresponding LIKE query using strimps and take the complement
  of the result. We keep around NULL values both during strimp filtering
  and during the pcre part of the LIKE query so that they get discarded
  automatically when we take the complement.

* Fri Aug 19 2022 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.45.1-20220913
- monetdb5: Disabled volcano pipeline due to known issues.

* Fri Aug 19 2022 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.45.1-20220913
- odbc: Corrected ODBC functions SQLPrimaryKeys(), SQLSpecialColumns() and
  SQLStatistics() for local temporary tables located in schema tmp. They did
  not return any rows when the temp table had a primary or unique key or
  index. Now they do return rows as expected.

* Fri Aug 19 2022 Nuno Faria <nunofpfaria@gmail.com> - 11.45.1-20220913
- sql: Added the UNLOGGED TABLE feature.

* Fri Aug 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.1-20220913
- gdk: The function BBPkeepref now gets a BAT pointer as argument instead of
  a bat id.

* Fri Aug 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.1-20220913
- gdk: Get rid of macro Tsize, use ->twidth instead.
- gdk: Get rid of macro BUNlast, just use BATcount instead.

* Fri Aug 19 2022 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.45.1-20220913
- merovingian: Added "loglevel" property for monetdbd logging (to merovingian.log).
  The loglevel can be set to: error or warning or information or debug.
  The loglevel property can be changed dynamically via command:
   monetdbd set loglevel=warning /path/to/dbfarm
  Default the loglevel is set to: information
  When loglevel is error, only errors are logged.
  When loglevel is warning, errors and warnings are logged.
  When loglevel is information, errors and warnings and information messages
  are logged.  When loglevel is debug, all messages are logged.

* Fri Aug 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.1-20220913
- merovingian: There is now a loadmodules property that can be used to add --loadmodule
  arguments to the mserver5 command line.  See the monetdb and mserver5
  manual pages.

* Fri Aug 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.1-20220913
- sql: Removed functions sys.index and sys.strings.

* Fri Aug 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.1-20220913
- gdk: The BLOB type has been moved into the GDK layer.

* Fri Aug 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.45.1-20220913
- gdk: When adding or subtracting months from a date or timestamp value,
  clamp the result to the calculated month instead of wrapping to the
  beginning of the next month.  See bug 7227.

