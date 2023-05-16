%global name MonetDB
%global version 11.43.27
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
Summary: MonetDB - Monet Database Management System
Vendor: MonetDB BV <info@monetdb.org>

Group: Applications/Databases
License: MPLv2.0
URL: https://www.monetdb.org/
BugURL: https://bugs.monetdb.org/
Source: https://www.monetdb.org/downloads/sources/Jan2022-SP6/%{name}-%{version}.tar.bz2

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
BuildRequires: pkgconfig(uuid)
BuildRequires: pkgconfig(libxml-2.0)
%if %{with pcre}
BuildRequires: pkgconfig(libpcre) >= 4.5
%endif
BuildRequires: pkgconfig(zlib)
BuildRequires: pkgconfig(liblz4) >= 1.8
%if %{with py3integration}
BuildRequires: pkgconfig(python3) >= 3.5
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
# BuildRequires: pkgconfig(cmocka)	# -DWITH_CMOCKA=ON
# BuildRequires: pkgconfig(gdal)	# -DSHP=ON
# BuildRequires: pkgconfig(netcdf)	# -DNETCDF=ON
# BuildRequires: pkgconfig(proj)	# -DWITH_PROJ=ON
# BuildRequires: pkgconfig(snappy)	# -DWITH_SNAPPY=ON
# BuildRequires: pkgconfig(valgrind)	# -DWITH_VALGRIND=ON

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
Requires: python3-pymonetdb >= 1.0.6
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
%{_bindir}/bincopydata
%{_bindir}/odbcsample1
%{_bindir}/sample0
%{_bindir}/sample1
%{_bindir}/sample4
%{_bindir}/shutdowntest
%{_bindir}/smack00
%{_bindir}/smack01
%{_bindir}/streamcat
%{_bindir}/testgetinfo
%{_bindir}/testStmtAttr
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
	-DWITH_UUID=ON \
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
rm -f %{buildroot}%{_libdir}/monetdb5/lib_run_*.so
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
* Mon Nov 21 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.25-20221121
- Rebuilt.
- GH#7336: Selecting from a literal-value table returns wrong values

* Wed Nov  9 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.25-20221121
- gdk: Memory leaks have been fixed.

* Wed Nov  9 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.25-20221121
- monetdb5: A race condition in the SHA hash code was fixed which resulted in
  occasional failed connection attempts when they occurred concurrently.

* Wed Oct 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.25-20221121
- monetdb5: Fix a bug where the MAL optimizer would use the starttime of the
  previous query to determine whether a query timeout occurred.

* Wed Oct 12 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.25-20221121
- merovingian: Stop logging references to monetdbd's logfile in said logfile.

* Mon Oct 10 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.25-20221121
- gdk: Offset heaps (.tailN files) were growing too fast and unnecessarily
  under certain conditions.  This has been fixed.  Also, when such too
  large files are now loaded into the system, it is recognized they are
  too large and they are truncated to a more reasonable size.

* Mon Oct 03 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.23-20221003
- Rebuilt.

* Mon Sep 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.23-20221003
- gdk: Fixed a bug in ORDER BY with both NULLS LAST and LIMIT when the ordering
  was on an interger or integer-like column and NULL values are present.

* Mon Sep 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.23-20221003
- sql: Fixed a bug in COPY BINARY INTO where the input wasn't checked
  thoroughly enough.

* Wed Aug 24 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.23-20221003
- gdk: The median_avg and quantile_avg returned bogus results in the
  non-grouped case (i.e. something like SELECT sys.median_avg(i) FROM t).

* Wed Aug 24 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.23-20221003
- merovingian: In certain cases (when an mserver5 process exits right after producing
  a message) the log message was logged over and over again, causing
  monetdbd to use 100% CPU.  This has been fixed.

* Fri Aug 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.21-20220819
- Rebuilt.

* Mon Aug 15 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.19-20220815
- Rebuilt.

* Thu Aug 11 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.17-20220811
- Rebuilt.
- GH#7040: Memory leak detected for MAPI interface
- GH#7298: Irresponsive database server after reading incomplete SQL
  script.
- GH#7308: Race condition in MVCC transaction management

* Wed Aug 10 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.17-20220811
- gdk: A bug was fixed when upgrading a database from the Oct2020 releases
  (11.39.X) or older when the write-ahead log (WAL) was not empty and
  contained instructions to create new tables.

* Tue Aug  2 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.17-20220811
- gdk: When destroying a bat, make sure there are no files left over in
  the BACKUP directory since they can cause problems when the bat id
  gets reused.

* Thu Jul 28 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.17-20220811
- gdk: Fixed an off-by-one error in the logger which caused older log files
  to stick around longer in the write-ahead log than necessary.
- gdk: When an empty BAT is committed, skip writing (and synchronizing to
  disk) the heap (tail and theap) files and write 0 for their sizes to
  the BBP.dir file.  When reading the BBP.dir file, if an empty BAT is
  encountered, set the sizes of those files to 0.  This fixes potential
  issues during startup of the server (BBPcheckbats reporting errors).

* Thu Jun 23 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.17-20220811
- merovingian: When multiple identical messages are written to the log, write the
  first one, and combine subsequent ones in a single message.

* Wed Jun 22 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.17-20220811
- gdk: Make sure heap files of transient bats get deleted when the bat is
  destroyed.  If the bat was a partial view (sharing the vheap but not
  the tail), the tail file wasn't deleted.
- gdk: Various changes were made to satisfy newer compilers.
- gdk: The batDirtydesc and batDirtyflushed Boolean values have been deprecated
  and are no longer used.  They were both holdovers from long ago.
- gdk: Various race conditions (data races) have been fixed.

* Wed Jun 22 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.17-20220811
- merovingian: Fixed a leak where the log file wasn't closed when it was reopened
  after a log rotation (SIGHUP signal).
- merovingian: Try to deal more gracefully with "inherited" mserver5 processes.
  This includes not complaining about an "impossible state", and allowing
  such processes to be stopped by the monetdbd process.
- merovingian: When a transient failure occurs during processing of a new connection to
  the monetdbd server, sleep for half a second so that if the transient
  failure occurs again, the log file doesn't get swamped with error
  messages.

* Wed Jun 22 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.17-20220811
- monetdb5: Various race conditions (data races) have been fixed.

* Fri Jun 10 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.17-20220811
- clients: Implemented dump of global grants, that is to say, grants for COPY INTO
  and COPY FROM which grant permission to users to write to or read from
  files on the server (COPY INTO queries without the ON CLIENT option).

* Tue May 31 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.17-20220811
- clients: Fixed a bug where when the semicolon at the end of a COPY INTO query
  that reads from STDIN is at exactly a 10240 byte boundary in a file,
  the data isn't read as input for the COPY INTO but instead as a new
  SQL query.

* Fri May 20 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.17-20220811
- gdk: All accesses to the BACKUP directory need to be protected by the
  same lock.  The lock already existed (GDKtmLock), but wasn't used
  consistently.  This is now fixed.  Hopefully this makes the hot snapshot
  code more reliable.

* Fri May 20 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.15-20220520
- Rebuilt.
- GH#7036: Generate column names instead of labels

* Thu May 19 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.15-20220520
- gdk: All accesses to the BACKUP directory need to be protected by the
  same lock.  The lock already existed (GDKtmLock), but wasn't used
  consistently.  This is now fixed.  Hopefully this makes the hot snapshot
  code more reliable.

* Tue May 10 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.15-20220520
- gdk: When exiting, long running instructions are aborted using the same
  mechanism that is used for query timeouts.

* Mon Apr 25 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.15-20220520
- sql: GLOBAL TEMPORARY tables are now treated like LOCAL TEMPORARY tables
  as far as the table content is concerned.  The schema information
  stays global.  This fixes an issue with concurrent access and cleanup
  of stale data.

* Thu Apr 14 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.15-20220520
- sql: The NO CONSTRAINT option of the COPY INTO query has been removed.
  It didn't work and it was never a good idea anyway.

* Fri Apr 01 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.13-20220401
- Rebuilt.
- GH#7278: BUG when there is more than one field/filter in the having
  clause

* Fri Apr  1 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.13-20220401
- gdk: Improved speed of BATappend to empty varsized bat: we now just copy
  the heaps instead of inserting individual values.

* Fri Apr  1 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.13-20220401
- monetdb5: Improved parsing speed of blob values, especially on Windows.
  On Windows, using the locale-aware functions isdigit and isxdigit is
  comparatively very slow, so we avoid them.

* Tue Mar 29 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.13-20220401
- gdk: Improved speed of projection (BATproject) on varsized bats by sharing
  the data heap (vheap).

* Fri Mar 25 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.11-20220325
- Rebuilt.
- GH#7252: Segmentation fault on second run
- GH#7253: Extremely slow INSERT INTO <table> SELECT
- GH#7254: Commit with deletions is very slow
- GH#7263: PRIMARY KEY constraint is not persistent through server restarts
- GH#7267: Update after delete does not update some rows

* Fri Mar 18 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.11-20220325
- gdk: Fixed a race condition which could cause a too large size being written
  for a .theap file to the BBP.dir file after the correct size file had
  been saved to disk.
- gdk: We now ignore the size and capacity columns in the BBP.dir file.
  These values are essential during run time, but not useful in the
  on-disk image of the database.

* Wed Mar  9 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.11-20220325
- gdk: Fixed a bug in the append code for msk (bit mask) bats.
- gdk: Conversions from floating point types to integral types that involve
  multiplication now use the "long double" as intermediate type, thereby
  loosing as few significant bits as is feasible.
- gdk: Found and fixed another source for the now infamous BBPcheckbats error
  that sometimes occurs at startup of the server.

* Wed Feb 16 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.11-20220325
- clients: Improved the handling of the \r (internal pager) command in mclient.
  It now properly counts the header of table, and when a (very) long
  table is being printed and aborted part way in the built-in pager, not
  all data is transferred to the client (and then discarded).  Instead
  at most 1000 rows are transferred.

* Mon Feb 07 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.9-20220207
- Rebuilt.
- GH#7237: SELECT with concurrent writes rarely returns corrupt data
- GH#7238: query with system function: "index"(varchar, boolean) fails
  with GDK error or assertion failure.
- GH#7241: Replacing a view by a query on the view itself crashes the
  server.

* Thu Feb 03 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.7-20220203
- Rebuilt.
- GH#7228: COMMIT: transaction is aborted because of concurrency
  conflicts, will ROLLBACK instead
- GH#7230: Prepared statement of INSERT with SELECT fails when types difer
- GH#7232: False conflicts when inserting in a not null field

* Mon Jan 24 2022 svetlin <svetlin.stalinov@monetdbsolutions.com> - 11.43.7-20220203
- sql: [This feature was already released in Jan2022 (11.43), but the ChangeLog was missing]
  Added SQL procedures sys.vacuum(sname string, tname string, cname string),
  sys.vacuum(sname string, tname string, cname string, interval int),
  sys.stop_vacuum(sname string, tname string, cname string).
  These can be used to vacuum string columns.

* Tue Jan 18 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.5-20220118
- Rebuilt.

* Thu Jan 13 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.5-20220118
- NT: We now build Windows binaries using Visual Studio 2022.

* Wed Jan 12 2022 Panagiotis Koutsourakis <kutsurak@monetdbsolutions.com> - 11.43.5-20220118
- gdk: Implement string imprints (strimps for short) a pre-filter structure
  for strings in order to accelerate LIKE queries. If a strimp exists
  for a specific string column the strings are pre-filtered, rejecting
  strings that cannot possibly match, before the more expensive and
  accurate matching algorithms run.

* Wed Jan 12 2022 Panagiotis Koutsourakis <kutsurak@monetdbsolutions.com> - 11.43.5-20220118
- sql: Add string imprints to the existing imprints index creation syntax. On
  string column "col" of a table "tbl" marked read only ("ALTER TABLE tbl
  SET READ ONLY") the user can create a string imprint using the syntax:
  "CREATE IMPRINTS INDEX index_name ON tbl(col);".

* Wed Jan 12 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.5-20220118
- MonetDB: A couple of concurrency issues have been fixed.

* Tue Jan 11 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.3-20220111
- Rebuilt.
- GH#7215: ODBC Driver SQLStatistics returns duplicate rows/rows for other
  tables

* Tue Jan 11 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.3-20220111
- gdk: On Windows, files and directories we create now get the attribute
  FILE_ATTIBUTE_NOT_CONTENT_INDEXED, meaning that they should not be
  indexed by indexing or search services.

* Thu Jan  6 2022 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.43.3-20220111
- merovingian: Disabled logging into merovingian.log of next info message types:
  "proxying client <host>:<port> for database '<dbname>' to <url>" and
  "target connection is on local UNIX domain socket, passing on filedescriptor instead of proxying".
  These messages were written to the log file at each connection. In most
  cases this information is not used. The disabling reduces the log file size.

* Mon Jan 03 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.43.1-20220103
- Rebuilt.
- GH#7168: Loosing the documentation
- GH#7180: GROUP BY-subquery crashes MonetDb
- GH#7182: Queries against sys.querylog_catalog, sys.querylog_calls or
  sys.querylog_history fail after restore of a db created using call
  sys.hot_snapshot(R'\path\file.tar');
- GH#7201: Selection of a subquery with a LEFT JOIN returns the wrong
  result set
- GH#7202: DISTINCT does not work when sorting by additional columns

* Wed Dec 15 2021 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.43.1-20220103
- monetdb5: The storage cleanup in the 11.41.5 (Jul2021) release made the OLTP
  optimizer pipeline obsolete, thus it was removed.

* Wed Dec 15 2021 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.43.1-20220103
- sql: With the storage cleanup in the 11.41.5 (Jul2021) release, the ANALYZE
  statement was updated to accomodate those changes. The SAMPLE parameter
  is now ignored because ANALYZE generated statistics used by
  relational operators, are required to be precise.
- sql: In order to mitigate the I/O required to update the 'statistics' table,
  this table is no longer persisted. Alternately, it was changed into a
  computed view every time when queried. The 'stamp' and 'sample' fields
  were removed for the aforementioned reasons. The 'schema', 'table' and
  'column' fields were added for convenience.

* Mon Dec 13 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.43.1-20220103
- sql: In previous versions there was no check that the INCREMENT BY value of
  a SEQUENCE was not zero.  During the automatic upgrade of a database,
  INCREMENT BY values that are zero are set to one.

* Mon Dec 13 2021 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.43.1-20220103
- sql: The method to compute the 'side_effect' effect property was changed
  for SQL functions defined in the backend engine (eg. ``CREATE FUNCTION
  ... EXTERNAL NAME "module"."function"''). It was changed from being
  computed by the SQL layer to the backend engine itself. As a consequence,
  the computed 'side_effect' value may be different, thus bringing
  incompatibilities. After an upgrade, if a 'side_effect' incompatibility
  arises, either the 'side_effect' value in the backend should be changed or
  the function should be re-created in SQL.

* Mon Dec 13 2021 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.43.1-20220103
- sql: Removed deprecated system view sys.systemfunctions. It was marked
  as deprecated from release Apr2019 (11.33.3).  Use query:
    select id as function_id from sys.functions where system;
  to get the same data as the old view.

* Mon Dec 13 2021 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.43.1-20220103
- sql: Extended SQL system catalog with lookup table sys.fkey_actions and
  view sys.fkeys to provide user friendly querying of existing foreign
  keys and their ON UPDATE and ON DELETE referential action specifications.

* Mon Dec 13 2021 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.43.1-20220103
- sql: Many improvements were done for REMOTE table plans. As a consequence,
  master or slave servers from this feature release are not compatible
  with older releases.

* Mon Dec 13 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.43.1-20220103
- sql: The view sys.ids has been changed to give more information about the
  objects in the system.  In particular, there is an extra column
  added at the end that indicates whether the object is a system
  object.

* Mon Dec 13 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.43.1-20220103
- sql: The example modules opt_sql_append and udf are no longer loaded by
  default and no longer part of the binary release.  If installed,
  they can be loaded using the --loadmodule option.

* Mon Dec 13 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.43.1-20220103
- clients: A new output formatting mode was added to mclient.  Use -fcsv-noquote
  to produce a CSV (comma-separated values) output where the quote
  characters have not been escapes.  This can be useful when producing
  a single column string output that should be saved as is, e.g. when
  using the sys.dump_database() function.

* Mon Dec 13 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.43.1-20220103
- gdk: Many (most) low level functions that could take a long time (such as
  BATjoin) can now be aborted with a timeout.  When the function takes too
  long, the function will fail, and hence the whole SQL query will fail.
- gdk: At some point in the past, string heaps were created where the
  hash value of the string was stored in the heap before the string.
  This hasn't been used in a long time.  Now the code that could still
  read those old heaps has been removed.  Bats that used the old format
  are converted automatically.

* Mon Dec 13 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.43.1-20220103
- misc: Reliance on the OpenSSL library has been removed.  OpenSSL was used
  for the hash algorithms it contained (e.g. SHA512 and RIPEMD160) and
  for producing random numbers.  The hash functions have been replaced
  by the original published functions, and random numbers are generated
  using system-specific random sources (i.e. not simply pseudo-random
  number generators).

* Mon Dec 13 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.43.1-20220103
- sql: The built-in SQL functions to produce a dump that were added as a
  proof-of-concept in the previous release have been improved and are
  now usable.  Use the query ``SELECT stmt FROM sys.dump_database(FALSE)
  ORDER BY o'' to produce a dump.  The dump code built into mclient and
  msqldump is probably still more efficient, though.

* Mon Dec 13 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.43.1-20220103
- gdk: Some small interface changes to the atom functions: the atomPut function
  now returns (var_t) -1 on error instead of 0; the atomHeap function
  now returns success or failure as a gdk_return value.

* Mon Dec 13 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.43.1-20220103
- sql: The sys.epoch function has always been confusing.  There are two
  versions, one with an INTEGER argument, and one with a BIGINT
  argument.  The former accepted values as seconds, whereas the
  latter expected milliseconds.  Also, the construct EXTRACT(EPOCH
  FROM value) returns a BIGINT with millisecond precision.  This has
  now been overhauled.  There is no longer a function sys.epoch with
  BIGINT argument, but instead there is a new function sys.epoch with
  DECIMAL(18,3) argument.  The argument is seconds, but with 3 decimals,
  it provides millisecond accuracy. Also the EXTRACT(EPOCH FROM value)
  now returns a DECIMAL(18,3), again seconds with 3 decimals giving
  millisecond accuracy.  Note that the internal, binary representation
  of DECIMAL(18,3) interpreted as seconds with 3 decimals and BIGINT
  with millisecond precision is exactly the same.

* Mon Dec 13 2021 Panagiotis Koutsourakis <kutsurak@monetdbsolutions.com> - 11.43.1-20220103
- merovingian: Removed the deprecated monetdb commands `profilerstart` and
  `profilerstop`.

* Mon Dec 13 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.13-20211213
- Rebuilt.
- GH#7163: Multiple sql.mvc() invocations in the same query
- GH#7167: sys.shutdown() problems
- GH#7184: Insert into query blocks all other queries
- GH#7185: GROUPING SETS on groups with aliases provided in the SELECT
  returns empty result
- GH#7186: data files created with COPY SELECT .. INTO 'file.csv' fail to
  be loaded using COPY INTO .. FROM 'file.csv' when double quoted string
  data contains the field values delimiter character
- GH#7191: [MonetDBe] monetdbe_cleanup_statement() with bound NULLs on
  variable-sized types bug
- GH#7196: BATproject2: does not match always
- GH#7198: Suboptimal query plan for query containing JSON access filter
  and two negative string comparisons
- GH#7200: PRIMARY KEY unique constraint is violated with concurrent
  inserts
- GH#7206: Python UDF fails when returning an empty table as a dictionary

* Mon Dec 13 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.13-20211213
- clients: Dumping the database now also dumps the read-only and insert-only
  states of tables.

* Mon Dec 13 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.13-20211213
- gdk: Sometimes when the server was restarted, it wouldn't start anymore due
  to an error from BBPcheckbats.  We finally found and fixed a (hopefully
  "the") cause of this problem.

* Thu Oct 28 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.13-20211213
- sql: Number parsing for SQL was fixed.  If a number was immediately followed
  by letters (i.e. without a space), the number was accepted and the
  alphanumeric string starting with the letter was interpreted as an alias
  (if aliases were allowed in that position).

* Thu Sep 30 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.11-20210930
- Rebuilt.

* Tue Sep 28 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.9-20210928
- Rebuilt.

* Mon Sep 27 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.7-20210927
- Rebuilt.
- GH#7140: SQL Query Plan Non Optimal with View
- GH#7162: Extend sys.var_values table
- GH#7165: `JOINIDX: missing '.'` when running distributed join query on
  merged remote tables
- GH#7172: Unexpected query result with merge tables
- GH#7173: If truncate is in transaction then after restart of MonetDB the
  table is empty
- GH#7178: Remote Table Throws Error - createExceptionInternal: !ERROR:
  SQLException:RAstatement2:42000!The number of projections don't match
  between the generated plan and the expected one: 1 != 1200

* Wed Sep 22 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.7-20210927
- gdk: Some deadlock and race condition issues were fixed.
- gdk: Handling of the list of free bats has been improved, leading to less
  thread contention.
- gdk: A problem was fixed where the server wouldn't start with a message from
  BBPcheckbats about files being too small.  The issue was not that the
  file was too small, but that BBPcheckbats was looking at the wrong file.
- gdk: An issue was fixed where a "short read" error was produced when memory
  was getting tight.

* Wed Sep 22 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.7-20210927
- sql: If the server has been idle for a while with no active clients, the
  write-ahead log is now rotated.
- sql: A problem was fixed where files belonging to bats that had been deleted
  internally were not cleaned up, leading to a growing database (dbfarm)
  directory.
- sql: A leak was fixed where extra bats were created but never cleaned up,
  each taking up several kilobytes of memory.

* Tue Aug 17 2021 Ying Zhang <y.zhang@cwi.nl> - 11.41.7-20210927
- sql: [This feature was already released in Jul2021 (11.41.5), but the ChangeLog was missing]
  Grant indirect privileges.  With "GRANT SELECT ON <my_view> TO
  <another_user>"  and "GRANT EXECUTE ON FUNCTION <my_func> TO
  <another_user>", one can grant access to "my_view" and "my_func"
  to another user who does not have access to the underlying database
  objects (e.g. tables, views) used in "my_view" and "my_func".  The
  grantee will only be able to access data revealed by "my_view" or
  conduct operations provided by "my_func".

* Mon Aug 16 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.7-20210927
- sql: Improved error reporting in COPY INTO by giving the line number
  (starting with one) for the row in which an error was found.  In
  particular, the sys.rejects() table now lists the line number of the
  CSV file on which the record started in which an error was found.

* Wed Aug 11 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.7-20210927
- gdk: When appending to a string bat, we made an optimization where the string
  heap was sometimes copied completely to avoid having to insert strings
  individually.  This copying was still done too eagerly, so now the
  string heap is copied less frequently.  In particular, when appending
  to an empty bat, the string heap is now not always copied whole.

* Tue Aug 03 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.5-20210803
- Rebuilt.
- GH#7161: fix priority

* Tue Aug  3 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.5-20210803
- gdk: A bug in the grouping code has been fixed.

* Tue Aug  3 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.5-20210803
- sql: The system view sys.ids has been updated to include some more system
  IDs.

* Fri Jul 30 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.3-20210730
- Rebuilt.

* Fri Jul 30 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.3-20210730
- gdk: Hash indexes are no longer maintained at all cost: if the number of
  distinct values is too small compared to the total number of values,
  the index is dropped instead of being maintained during updates.

* Fri Jul 30 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.3-20210730
- sql: The sys.storage() function now only returns meta data, i.e. data that
  can be calculated without access to the column contents.

* Wed Jul 28 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.3-20210730
- sql: Since STREAM tables support is removed, left over STREAM tables are
  dropped from the catalog.

* Fri Jul 23 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.1-20210723
- Rebuilt.
- GH#2030: Temporary table is semi-persistent when transaction fails
- GH#7031: I cannot start MoentDb, because the installation path has
  Chinese.
- GH#7055: Table count returning function used inside other function gives
  wrong results.
- GH#7075: Inconsistent Results using CTEs in Large Queries
- GH#7079: WITH table AS... UPDATE ignores the WHERE conditions on table
- GH#7081: Attempt to allocate too much space in UPDATE query
- GH#7093: `current_schema` not in sys.keywords
- GH#7096: DEBUG SQL statement broken
- GH#7115: Jul2021: ParseException while upgrading Oct2020 database
- GH#7116: Jul2021: Cannot create filter functions
- GH#7125: MonetDB Round Function issues in the latest release
- GH#7126: The "lower" and "upper" functions doesn't work for Cyrillic
  alphabet
- GH#7127: Bug report: "write error on stream" that results in mclient
  crash
- GH#7128: Bug report: strange error message "Subquery result missing"
- GH#7129: Bug report: TypeException:user.main[19]:'batcalc.between'
  undefined
- GH#7130: Bug report: TypeException:user.main[396]:'algebra.join'
  undefined
- GH#7131: Bug report: TypeException:user.main[273]:'bat.append' undefined
- GH#7133: WITH <alias> ( SELECT x ) DELETE FROM ... deletes wrong tuples
- GH#7136: MERGE statement is deleting rows if the column is set as NOT
  NULL even though it should not
- GH#7137: Segmentation fault while loading data
- GH#7138: Monetdb Python UDF crashes because of null aggr_group_arr
- GH#7141: COUNT(DISTINCT col) does not calculate correctly distinct values
- GH#7142: Aggregates returning tables should not be allowed
- GH#7144: Type up-casting (INT to BIGINT) doesn't always happen
  automatically
- GH#7146: Query produces this error: !ERROR: Could not find %102.%102
- GH#7147: Internal error occurs and is not shown on the screen
- GH#7148: Select distinct is not working correctly
- GH#7151: Insertion is too slow
- GH#7153: System UDFs lose their indentation - Python functions broken
- GH#7158: Python aggregate UDF returns garbage when run on empty table

* Wed Jul 21 2021 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.41.1-20210723
- mapilib: Add optional MAPI header field which can be used to immediately
  set reply size, autocommit, time zone and some other options, see
  mapi.h.  This makes client connection setup faster.  Support has been
  added to mapilib, pymonetdb and the jdbc driver.

* Wed Jul 21 2021 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.41.1-20210723
- sql: Fix a warning emitted by some implementations of the tar(1) command
  when unpacking hot snapshot files.
- sql: support reading the concatenation of compressed files as a single
  compressed file.
- sql: COPY BINARY overhaul.  Allow control over binary endianness using
  COPY [ (BIG | LITTLE | NATIVE) ENDIAN] BINARY syntax.  Defaults to
  NATIVE.  Strings are now \0 terminated rather than \n.  Support for
  BOOL, TINYINT, SMALLINT, INT, LARGEINT, HUGEINT, with their
  respective "INTMIN" values as the NULL representation; 32 and 64 bit
  FLOAT/REAL, with NaN as the NULL representation; VARCHAR/TEXT, JSON
  and URL with \x80 as the NULL representation; UUID as fixed width 16
  byte binary values, with (by default) all zeroes as the NULL
  representation; temporal type structs as defined in copybinary.h
  with any invalid value as the NULL representation.

* Tue Jul 20 2021 Niels Nes <niels@cwi.nl> - 11.41.1-20210723
- sql: In the Jul2021 release the storage and transaction layers have
  undergone major changes.  The goal of these changes is robust
  performance under inserts/updates and deletes and lowering the
  transaction startup costs, allowing faster (small) queries.  Where
  the old transaction layer duplicated a lot of data structures during
  startup, the new layer shares the same tree.  Using object
  timestamps the isolation of object is guaranteed.  On the storage
  side the timestamps indicate whether a row is visible (deleted or
  valid), to a transaction as well.  The changes also give some slight
  changes on the perceived transactional behavior.  The new
  implementation uses shared structures among all transactions, which
  do not allow multiple changes of the same object.  And we then
  follow the principle of the first writer wins, i.e., if a
  transaction creates a table with name 'table_name', and concurrently
  one other transaction does the same the later of the two will fail
  with a concurrency conflict error message (even if the first writer
  never commits).  We expect most users not to notice this change, as
  such schema changes aren't usually done concurrently.

* Tue Jul 20 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.1-20210723
- clients: The MonetDB stethoscope has been removed.  There is now a separate
  package available with PIP (monetdb_stethoscope) or as an RPM or DEB
  package (stethoscope) from the monetdb.org repository.

* Tue Jul 20 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.1-20210723
- gdk: A new type, called msk, was introduced.  This is a bit mask type.
  In a bat with type msk, each row occupies a single bit, so 8 rows are
  stored in a single byte.  There is no NULL value for this type.
- gdk: The function of the BAT iterator (type BATiter, function bat_iterator)
  has been expanded.  The iterator now contains more information about
  the BAT, and it contains a pointer to the heaps (theap and tvheap)
  that are stable, at least in the sense that they will remain available
  even when parallel threads update the BAT and cause those heaps to grow
  (and therefore possibly move in memory).  A call to bat_iterator must
  now be accompanied by a call to bat_iterator_end.

* Mon Jun  7 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.1-20210723
- monetdb5: When using the --in-memory option, mserver5 will run completely in
  memory, i.e. not create a database on disk.  The server can still be
  connected to using the name of the in-memory database.  This name is
  "in-memory".

* Tue May 11 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.1-20210723
- sql: There is now a function sys.current_sessionid() to return the session
  ID of the current session.  This ID corresponds with the sessionid in
  the sys.queue() result.

* Mon May 10 2021 Panagiotis Koutsourakis <kutsurak@monetdbsolutions.com> - 11.41.1-20210723
- merovingian: Deprecate `profilerstart` and `profilerstop` commands. Since
  stethoscope is a separate project (https://github.com/MonetDBSolutions/monetdb-pystethoscope)
  the installation directory is not standard anymore. `profilerstart` and
  `profilerstop` commands assume that the stethoscope executable is in the
  same directory as `mserver5`. This is no longer necessarily true since
  stethoscope can now be installed in a python virtual environment. The
  commands still work if stethoscope is installed using the official
  MonetDB installers, or if a symbolic link is created in the directory
  where `mserver5` is located.

* Fri May  7 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.1-20210723
- odbc: A typo that made the SQLSpecialColumns function unusable was fixed.

* Mon May  3 2021 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.41.1-20210723
- sql: Merge statements could not produce correct results on complex join
  conditions, so a renovation was made. As a consequence, subqueries
  now have to be disabled on merge join conditions.

* Mon May  3 2021 svetlin <svetlin.stalinov@monetdbsolutions.com> - 11.41.1-20210723
- sql: preserve in-query comments

* Mon May  3 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.1-20210723
- merovingian: The exittimeout value can now be set to a negative value (e.g. -1) to
  indicate that when stopping the dbfarm (using monetdbd stop dbfarm),
  any mserver5 processes are to be sent a termination signal and then
  waited for until they terminate.  In addition, if exittimeout is greater
  than zero, the mserver5 processes are sent a SIGKILL signal after the
  specified timeout and the managing monetdbd is sent a SIGKILL signal
  after another five seconds (if it didn't terminate already).  The old
  situation was that the managing monetdbd process was sent a SIGKILL
  after 30 seconds, and the mserver5 processes that hadn't terminated
  yet would be allowed to continue their termination sequence.

* Mon May  3 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.1-20210723
- gdk: Implemented function BUNreplacemultiincr to replace multiple values
  in a BAT in one go, starting at a given position.
- gdk: Implemented new function BUNreplacemulti to replace multiple values
  in a BAT in one go, at the given positions.
- gdk: Removed function BUNinplace, just use BUNreplace, and check whether
  the BAT argument is of type TYPE_void before calling if you don't
  want to materialize.

* Mon May  3 2021 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.41.1-20210723
- sql: Use of CTEs inside UPDATE and DELETE statements are now more
  restrict. Previously they could be used without any extra specification
  in the query (eg. with "v1"("c1") as (...) delete from "t"
  where "t"."c1" = "v1"."c1"), however this was not conformant with the
  SQL standard. In order to use them, they must be specified in the FROM
  clause in UPDATE statements or inside a subquery.

* Mon May  3 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.1-20210723
- gdk: Implemented a function BUNappendmulti which appends an array of values
  to a BAT.  It is a generalization of the function BUNappend.

* Mon May  3 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.1-20210723
- gdk: Changed the interface of the atom read function.  It now requires an
  extra pointer to a size_t value that gives the current size of the
  destination buffer, and when that buffer is too small, it receives the
  size of the reallocated buffer that is large enough.  In any case,
  and as before, the return value is a pointer to the destination buffer.

* Mon May  3 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.1-20210723
- gdk: Environment variables (sys.env()) must be UTF-8, but since they can
  contain file names which may not be UTF-8, there is now a mechanism
  to store the original values outside of sys.env() and store
  %-escaped (similar to URL escaping) values in the environment.  The
  key must still be UTF-8.

* Mon May  3 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.1-20210723
- gdk: We now save the location of the min and max values when known.

* Mon May  3 2021 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.41.1-20210723
- sql: Added 'schema path' property to user, specifying a list of schemas
  to be searched on to find SQL objects such as tables and
  functions. The scoping rules have been updated to support this feature
  and it now finds SQL objects in the following order:
   1. On occasions with multiple tables (e.g. add foreign key constraint,
      add table to a merge table), the child will be searched on the
      parent's schema.
   2. For tables only, declared tables on the stack.
   3. 'tmp' schema if not listed on the 'schema path'.
   4. Session's current schema.
   5. Each schema from the 'schema path' in order.
   6. 'sys' schema if not listed on the 'schema path'.
  Whenever the full path is specified, ie "schema"."object", no search will
  be made besides on the explicit schema.
- sql: To update the schema path ALTER USER x SCHEMA PATH y; statement was added.
  [SCHEMA PATH string] syntax was added to the CREATE USER statement.
  The schema path must be a single string where each schema must be between
  double quotes and separated with a single comma, e.g. '"sch1","sch2"'
  For every created user, if the schema path is not given, '"sys"' will be
  the default schema path.
- sql: Changes in the schema path won't be reflected on currently connected users,
  therefore they have to re-connect to see the change. Non existent schemas
  on the path will be ignored.

* Mon May  3 2021 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.41.1-20210723
- sql: Leftover STREAM table definition from Datacell extension was removed
  from the parser. They had no effect anymore.

* Mon May  3 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.41.1-20210723
- monetdb5: By using the option "--dbextra=in-memory", mserver5 can be instructed
  to keep transient BATs completely in memory.

