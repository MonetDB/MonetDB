%global name MonetDB
%global version 11.40.0
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

%{!?__python3: %global __python3 /usr/bin/python3}
%{!?python3_sitelib: %global python3_sitelib %(%{__python3} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}

Name: %{name}
Version: %{version}
Release: %{release}
Summary: MonetDB - Monet Database Management System
Vendor: MonetDB BV <info@monetdb.org>

Group: Applications/Databases
License: MPLv2.0
URL: https://www.monetdb.org/
BugURL: https://bugs.monetdb.org/
Source: https://www.monetdb.org/downloads/sources/Oct2020-SP4/%{name}-%{version}.tar.bz2

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
BuildRequires: /usr/bin/python3
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
BuildRequires: pkgconfig(openssl)
%if %{with pcre}
BuildRequires: pkgconfig(libpcre) >= 4.5
%endif
BuildRequires: pkgconfig(zlib)
%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} > 7
# not on RHEL 7
BuildRequires: pkgconfig(liblz4) >= 1.8
%global LZ4 ON
%else
%global LZ4 OFF
%endif
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
Requires: openssl-devel

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
%exclude %{_bindir}/stethoscope
%{_libdir}/libmonetdb5.so.*
%{_libdir}/libmonetdbsql.so*
%dir %{_libdir}/monetdb5
%if %{with cintegration}
%{_libdir}/monetdb5/lib_capi.so
%endif
%{_libdir}/monetdb5/lib_generator.so
%{_libdir}/monetdb5/lib_udf.so
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
%{_includedir}/monetdb/sql*.h

%package embedded
Summary: MonetDB as an embedded library
Group: Applications/Databases

%description embedded
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the library to turn MonetDB into an embeddable
library.  Also see %{name}-embedded-devel to use this in a program.

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

%package testing-python
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Requires: %{name}-client-tests = %{version}-%{release}
Requires: /usr/bin/python3
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
	-DWITH_CRYPTO=ON \
	-DWITH_CURL=ON \
	-DWITH_LZ4=%{LZ4} \
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
rm -f %{buildroot}%{_libdir}/monetdb5/run_*.mal
rm -f %{buildroot}%{_libdir}/monetdb5/lib_run_*.so
rm -f %{buildroot}%{_libdir}/monetdb5/microbenchmark.mal
rm -f %{buildroot}%{_libdir}/monetdb5/lib_microbenchmark*.so
rm -f %{buildroot}%{_bindir}/monetdb_mtest.sh
rm -rf %{buildroot}%{_datadir}/monetdb # /cmake

if [ -x /usr/sbin/hardlink ]; then
    /usr/sbin/hardlink -cv %{buildroot}%{_datadir}/selinux
else
    # Fedora 31
    /usr/bin/hardlink -cv %{buildroot}%{_datadir}/selinux
fi

%changelog
* Fri Apr 02 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.39.15-20210402
- Rebuilt.
- GH#6786: function json.isvalid(js json) is not useful, could be removed
- GH#7016: Database crashes when use similarity function on a table with
  more than 200k records
- GH#7037: Clearer err msg for ALTER USER with insufficient privileges
- GH#7042: AddressSanitizer:DEADLYSIGNAL in Oct2020/gdk/gdk_tracer.c:494
- GH#7050: file descriptor leak when forward=redirect
- GH#7057: ODBC driver installer on Windows is missing some DLLs
- GH#7058: MonetDBe: COPY INTO csv file does not produce any output
- GH#7059: MonetDBe: 'reverse' C UDF crashes
- GH#7061: Have bulk load support combined gzip files
- GH#7064: Temporary hashes created in hash and unique logic should try to
  use transient data farm first
- GH#7066: percent_rank function with wrong results
- GH#7070: double free error when running MonetDBe Example
- GH#7076: mserver5 ignores memory.low from cgroups v2
- GH#7077: Oct2020: new default privileges not effectively communicated
- GH#7083: MonetDBe C++ Compiling Error
- GH#7085: Mitosis and filter functions
- GH#7087: SIGSEGV caused by error in subquery's function being ignored by
  top-level query
- GH#7089: Data consistency problem of query results in the latest release
  of Monetdb (Remote Table)

* Wed Mar 31 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.39.15-20210402
- odbc: When connecting using a DSN (Data Source Name), information about the
  data source is retrieved from the ODBC.INI file.  Now we also get the
  location of the LOGFILE from this file.  The logfile can be used to
  log all calls to the MonetDB ODBC driver to a file which can be used
  for debugging.

* Thu Mar 25 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.39.15-20210402
- odbc: The ODBC driver now only passes on information about HUGEINT columns
  as HUGEINT when the application has indicated interest by querying
  about the SQL_HUGEINT extension type using the SQLGetTypeInfo
  function or by specifying the type in a call to SQLSetDescField.
  Otherwise the driver silently translates the HUGEINT type to BIGINT.
  This means that most application will see BIGINT columns when the
  server produced a HUGEINT column and only give an error if the value
  in the HUGEINT column didn't fit into a BIGINT.

* Thu Feb 11 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.39.13-20210211
- Rebuilt.
- GH#7049: Implement DISTINCT for GROUP_CONCAT

* Mon Jan 18 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.39.11-20210118
- Rebuilt.
- GH#3772: Any user can grant a role.

* Mon Jan 11 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.39.9-20210111
- Rebuilt.
- GH#6862: mserver5: crashes under update_table() when calling lib_sql.so
  ( max_clients = 2048)
- GH#7002: monetdb stop fails
- GH#7012: mclient enters an infinite loop when a file on the command line
  does not exist
- GH#7013: Select * on grouped view: wrong error "cannot use non GROUP BY
  column 'a1' in query results without an aggregate function"
- GH#7017: mal seems to leak in functions
- GH#7020: release an older savepoint causes "BATproject2: does not match
  always"
- GH#7021: savepoints crash mserver5
- GH#7022: transaction with an unreleased savepoint not properly persisted
- GH#7023: CREATE VIEW: SELECT: cannot use non GROUP BY column '%1' in
  query results without an aggregate function
- GH#7024: DELETE FROM or TRUNCATE on freshly created table leads to
  loosing all further inserts in same transaction
- GH#7030: DROP TABLE with AUTO_INCREMENT doesn't drop sequence causing
  left-over dependency
- GH#7034: User with sysadmin role cannot create another user
- GH#7035: UPDATE and SELECT column privileges

* Thu Dec 10 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.39.9-20210111
- sql: CREATE [OR REPLACE] TRIGGER schema_name.trigger_name is now disallowed,
  because the trigger will be stored on the same schema as the table it
  refers to. Use a schema-qualified on the table reference (ie ON clause)
  when necessary.

* Wed Nov 18 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.39.7-20201118
- Rebuilt.
- BZ#6890: Add support of xz/lzma (de)compression on MS Windows
- BZ#6891: Add support of lz4 (de)compression on MS Windows
- BZ#6971: Parsing table returning function on remote server fails
- BZ#6981: Oct2020: PREPARE DDL statement silently fails
- BZ#6983: monetdb allows to use non-existing optimizer pipe
- BZ#6998: MAL profiler buffer limitations
- BZ#7001: crossproduct generated for a simple (semi-)join
- BZ#7003: Segfault on large chain of constant decimal multiplication
- BZ#7005: Dropping a STREAM TABLE does not remove the associated column
  info from sys._columns
- BZ#7010: deallocate <id> results in all prepared statements being
  deallocated (not error-related)
- BZ#7011: uuid() called only once when used in projection list

* Tue Oct 13 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.39.5-20201013
- Rebuilt.

* Mon Oct 12 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.39.5-20201013
- clients: mclient and msqldump now also look in $XDG_CONFIG_HOME for the monetdb
  configuration file.

* Fri Oct 09 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.39.3-20201009
- Rebuilt.

* Tue Oct 06 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.39.1-20201006
- Rebuilt.
- BZ#3553: All schema access to ubiquitous functions
- BZ#3815: Incorrect results when expression contains implicit
  float/integer conversions
- BZ#6415: Date arithmetic types are inconsistent
- BZ#6814: provide native implementations for scalar functions
  sys.degrees(rad) and sys.radians(deg)
- BZ#6843: function sys.getcontent(url) always returns "Feature not
  supported"
- BZ#6857: remove not implemented aggregate function json.output(js json)
- BZ#6870: Missing bulk operators
- BZ#6910: SQLancer query: 'bat.append' undefined
- BZ#6930: SQLancer crash on join with coalesce
- BZ#6931: Allow EDITOR to be used for the current command in mclient
- BZ#6935: Wrong result when dividing interval by literal float
- BZ#6937: Lost the microsecond precisions
- BZ#6938: Segmentation fault in MalOptimizer
- BZ#6939: Error in optimizer multiplex when selecting
  profiler.getlimit() or wlc.clock() or wlc.tick() or wlr.clock()
  or wlr.tick()
- BZ#6941: SELECT queries on remote table fail when using LIKE in WHERE
  conditions
- BZ#6943: JSON parser is too permissive
- BZ#6948: msqldump with Empty BLOBs cannot be imported
- BZ#6949: Loosing timing precision
- BZ#6950: redundant/replicated code line in gdk/gdk_hash.c
- BZ#6951: Use a different naming scheme for MAL blocks
- BZ#6954: FILTER functions no longer find their implementation
- BZ#6955: ROUND(DECIMAL, PRECISION) gives incorrect result with
  non-scalar precision parameter
- BZ#6960: implementation of log(arg1,arg2) function is not compliant
  with the SQL standard, arguments are switched
- BZ#6962: "SELECT * FROM ids LIMIT 1" produces: exp_bin: !ERROR: Could
  not find %173.id
- BZ#6964: Table returning function: Cannot access column descriptor
- BZ#6965: Crash when using distinct on the result of a table returning
  function
- BZ#6974: Oct2020-branch cannot attach and load FITS files
- BZ#6976: Oct2020: default dbfarm cannot be started
- BZ#6978: Oct2020: d shows empty result in schema created by include
  sql script
- BZ#6979: timestamp add integer
- BZ#6980: Oct2020: wrong mel definition for str.epilogue

* Mon Aug 31 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.39.1-20201006
- sql: Made general logarithm function log(x,base) compliant with the SQL
  standard, by swapping the input parameters.
  Instead of log(x,base), now is log(base,x).

* Thu Aug 20 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.39.1-20201006
- monetdb5: The settings for specifying how mserver5 should listen to "The
  Internet" have been overhauled.  See the manual for details.  In
  brief, mapi_autosense, mapi_ipv6 and mapi_open are gone.  If
  mapi_listenaddr equals "localhost" or "all", we listen to both IPv4
  and IPv6 (if available), if "127.0.0.1" or "0.0.0.0", we listen to
  IPv4 only, if "::1" or "::" we listen to IPv6 only.  The first of
  each pair is loopback interface only, the second is all interfaces.
  If mapi_listenaddr is "none", then no IP port is opened, you need to
  use a UNIX domain socket.  If mapi_port is 0, we let the operating
  system choose a free port (like mapi_autosense).  Default behavior
  has not changed.

* Mon Aug 10 2020 Ying Zhang <y.zhang@cwi.nl> - 11.39.1-20201006
- MonetDB: Finished a first version of the new monitoring function
  user_statistics(), which is only intended for the DBAs.
  For each database user who has logged in during the current mserver5
  session, it returns
  "username": login name of the database user,
  "querycount": the number of queries this user has executed since his/her
      first login,
  "totalticks": the total execution time (in microsecond) of the queries ran
      by this user,
  "maxquery": the query with the longest execution time (if two queries have
      the same execution time, the newer overwrites the older),
  "maxticks": the execution time of the 'maxquery' (in microsecond),
  "started": the start timestamp of the 'maxquery',
  "finished": the finish timestamp of the 'maxquery'.

* Thu Jul 23 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.39.1-20201006
- sql: Removed compatibility between interval types and other numeric types in
  favor for a more strict SQL standard compliance. This means operations
  between temporal types and other numeric types such as INT and
  DECIMAL are no longer possible, instead use interval types.
  e.g. SELECT date '2020-01-01' + 1; now gives the error. Instead do:
  SELECT date '2020-01-01' + interval '1' day; if 1 was meant to be a
  day interval.
  Setting an interval variable such as the session's current timezone
  with a number e.g. SET current_timezone = 1; is no longer possible.
  Instead do SET current_timezone = interval '1' hour;
  Casting between interval and other numeric types is no longer possible
  as well, because they are not compatible.
- sql: Because of incompatibilities this change may create, if a user intends
  to convert a numeric value to an interval, the multiplication function
  can be used in the form: <numeric value> * interval '1' <interval length>
  e.g. 10 * interval '1' second = interval '10' second.
  As for the other way around, the 'EPOCH' option was added to the extract
  syntax. This option returns the number of milliseconds since the UNIX
  epoch 1970-01-01 00:00:00 UTC for date, timestamp and time values (it
  can be negative). Meanwhile, for day and second intervals, it returns the
  total number of milliseconds in the interval. As a side note, the 'EPOCH'
  option is not available for month intervals, because this conversion is
  not transparent for this type.

* Thu Jul 23 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.39.1-20201006
- sql: Removed obsolete json.output(json) function.

* Thu Jul 23 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.39.1-20201006
- sql: Removed obsolete sys.getContent(url) function.

* Thu Jul 23 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.39.1-20201006
- MonetDB: Removed support for LiDAR data, that is the SQL procedures
  sys.lidarattach, sys.lidarload, and sys.lidarexport.

* Thu Jul 23 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.39.1-20201006
- sql: Removed '@' syntax used to refer into a variable in a query. It
  was a non-standard method, which was replaced by a schema addition to
  variables. Variables in the global scope now have schema. All default
  global variables are set under schema "sys". However variables
  inside PSM don't have a schema, because there are no transaction
  semantics inside PSM at the moment.
- sql: Removed declared variables and tables from the global scope. They were
  transaction agnostic and incompatible with the SQL standard, i.e. they
  are valid exclusively under PSM (e.g. functions, procedures and
  triggers).
- sql: Scoping semantics were added for both variables and tables. Variables
  with the same name at a query are now resolved under the following
  precedence rules:
    1. Tables, Views and CTEs at the FROM clause.
    2. Variable declared in the body of function/procedure, i.e. local
       variable.
    3. Function/procedure parameter.
    4. Variable from the global scope.
  Tables with the same name now have the following precedence rules at a
  SQL query:
    1. Table declared in the body of function/procedure, ie local table.
    2. Temporary table.
    3. Table from the current session schema.
  This means the query: SELECT * FROM "keys"; will list keys from
  temporary tables instead of persisted ones, because "keys" table
  is available for both "sys" and "tmp" schemas.
- sql: The table returning function "var" was extended with more details
  about globally declared variables, namely their schema, type and
  current value.

* Thu Jul 23 2020 Martin Kersten <mk@cwi.nl> - 11.39.1-20201006
- MonetDB: The sys.queue() has been turned into a circular buffer to allow for
  inspection of both active, paused and recently executed queries.

* Thu Jul 23 2020 Martin Kersten <mk@cwi.nl> - 11.39.1-20201006
- sql: Extended the system monitor with a table-returning function
  user_statistics() which keeps some statistics for each SQL user, e.g. the
  user's query count, total time spent, and maximal query seen.

* Thu Jul 23 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.11-20200723
- Rebuilt.
- BZ#6917: Decimal parsing fails
- BZ#6932: Syntax error while parsing JSON numbers with exponent
- BZ#6934: sys.isauuid() returns wrong answer for some invalid uuid
  strings

* Mon Jul 20 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.9-20200720
- Rebuilt.
- BZ#6844: sys.getUser('https://me:pw@www.monetdb.org/Doc') does not
  return the user: me
- BZ#6845: the url sys.get...(url) functions do not allow null as
  a parameter
- BZ#6858: json.keyarray(json '{ "":0 }') fails with error: Could not
  allocate space
- BZ#6859: only first character of the separator string in json.text(js
  json, sep string) is used
- BZ#6873: sys.hot_snapshot() creates incomplete snapshots if the
  write-ahead log is very large
- BZ#6876: tar files created by sys.hot_snapshot() produce warnings on
  some implementations of tar
- BZ#6877: MonetDB produces malformed LZ4 files
- BZ#6878: SQL Connection Error when running SELECT queries containing
  AND command
- BZ#6880: Left fuzzy queries are much slower than other fuzzy queries.
- BZ#6882: cgroups limits no longer respected?
- BZ#6883: SQLancer crash on delete query
- BZ#6884: SQLancer generates query with unclear error message
- BZ#6885: SQLancer causes assertion error on UTF8_strlen
- BZ#6886: SQLancer alter table add unique gives strange error message
- BZ#6887: SQLancer crash on complex query
- BZ#6888: SQLancer crash on cross join on view
- BZ#6889: SQLancer crash on long query
- BZ#6892: SQLancer crash on query with HAVING
- BZ#6893: SQLancer inner join reporting GDK error
- BZ#6894: SQLancer crash on rtrim function
- BZ#6895: SQLancer causing 'algebra.select' undefined error
- BZ#6896: SQLancer algebra.select' undefined 2
- BZ#6897: SQLancer distinct aggregate with error on group by constant
- BZ#6898: SQLancer crash on join query
- BZ#6899: SQLancer TLP query with wrong results
- BZ#6900: SQLancer generated SIGFPE
- BZ#6901: SQLancer TLP query with wrong results 2
- BZ#6902: SQLancer query: batcalc.between undefined
- BZ#6903: SQLancer calc.abs undefined
- BZ#6904: SQLancer aggr.subavg undefined
- BZ#6905: SQLancer TLP query with wrong results 3
- BZ#6906: SQLancer crash on complex join
- BZ#6907: SQLancer algebra.select undefined
- BZ#6908: SQLancer inputs not the same size
- BZ#6909: SQLancer query with wrong results
- BZ#6911: SQLancer query: 'calc.bit' undefined
- BZ#6918: SQLancer query compilation error
- BZ#6919: SQLancer insert function doesn't handle utf-8 strings
- BZ#6920: SQLancer project_bte: does not match always
- BZ#6922: Timestamp columns not migrated to new format
- BZ#6923: Imprints data files for timestamp BAT not migrated to the
  new format
- BZ#6924: SQLancer query copy on unique pair of columns fails and
  complex query with GDK error
- BZ#6925: Count string rows in union of string tables leaks (RSS) memory
- BZ#6926: SQLancer query with wrong results
- BZ#6927: SQLancer inputs not the same size
- BZ#6928: SQLancer crash on coalesce
- BZ#6929: SQLancer calc.date undefined

* Tue Jun  9 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.9-20200720
- gdk: Hash buckets come in variable widths.  But if a BAT grows long enough so
  that the BAT indexes that are stored in the buckets don't fit anymore,
  the buckets need to be widened.  This is now fixed.

* Fri May 29 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.7-20200529
- Rebuilt.

* Tue May 26 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.5-20200526
- Rebuilt.
- BZ#6864: (I)LIKE with multiple % doen't find matches

* Mon May 18 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.3-20200518
- Rebuilt.
- BZ#6863: thash files not released upon drop table

* Mon May 11 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- Rebuilt.
- BZ#6298: unexpectedly slow execution of SELECT length(fieldname)
  FROM tablename LIMIT 1 queries
- BZ#6401: Suspected memory leak in mserver5 when creating/dropping tables
- BZ#6687: Count distinct very slow and use too much the hard drive
- BZ#6731: Add system view to allow querying of available prepared
  statements and their parameters
- BZ#6732: Add SQL command to close a specific prepared statement
- BZ#6750: Executing a query on a non-existing column on a remote table
  crashes the remote server
- BZ#6785: function sys.isaURL(url) should have been declared as
  sys.isaURL(string)
- BZ#6808: reveal the alarm.sleep procedure in SQL
- BZ#6813: function not_uniques(bigint) returns error when called
- BZ#6818: usage of multiple column expressions in where-clause (f(a),
  f(b)) in (select a, b)  causes assertion failure on mserver5
- BZ#6821: Failed to start monetdb with embedded python
- BZ#6828: Server crashes when executing a window query with ordering
  by EXTRACT date
- BZ#6846: Global temporary table not accessible in other connections
  / sessions
- BZ#6847: A simple way of speeding up impscheck for dense canditers
- BZ#6850: Idle timestamp not set
- BZ#6851: json parser doesn't parse integers correctly

* Fri May  8 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- monetdb5: The mserver5 option --verbose (-v) was removed.  A similar effect can
  be had by issuing the query CALL logging.setcomplevel('SQL_TRANS',
  'INFO'); as the monetdb user.

* Wed May  6 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- selinux: There was a problem with the MonetDB SELinux support on Fedora 32.
  That is fixed in this release.  In order to do a proper upgrade of
  the package if you have already installed MonetDB-selinux on Fedora
  32, you may need to uninstall (dnf remove) the old package and then
  install the new.

* Tue Apr 28 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- gdk: The functions BATintersect, BATsemijoin, and BATsubcross have an
  extra argument, bool max_one, which indicates that there must be no
  more than one match in the join.

* Tue Apr 28 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- monetdb5: The functions algebra.intersect, algebra.semijoin, and
  algebra.crossproduct have an extra argument, bool max_one, which
  indicates that there must be no more than one match in the join.

* Thu Apr 23 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.37.1-20200511
- sql: Updating the value of a sequence now requires privilege on its own
  schema.

* Mon Apr 20 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- clients: The monetdb-client-tools (Debian/Ubuntu) and MonetDB-client-tools
  (Fedora/RH) containing the stethoscope, tachograph, and tomograph has
  been removed.  A completely new version of stethoscope will be released
  to replace the old version.

* Mon Apr 20 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- gdk: The "unique" property on BATs was removed.  The property indicated
  that all values in a BAT *had* to be distinct, but this was not
  actually used.
- gdk: A new type of candidate list has been introduced.  Candidate lists
  are used internally to specify which rows of a column participate
  in an operation.  Before, candidate lists always contained a list of
  candidate row IDs.  The new candidate list type specifies a list of
  row IDs that should NOT be considered (negative candidates).
- gdk: The maximum number of BATs in the system has been increased for 64
  bit architectures.
- gdk: The hash tables used internally by the system now uses a technique
  based on Linear Hashing which allows them to grow gracefully.  This
  means that hash tables aren't removed and recreated nearly as often
  anymore.  This also meant that the hash table had to be split into
  two files, which means that after an upgrade the hash tables have to
  be recreated.

* Mon Apr 20 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- merovingian: On Fedora and RHEL systems (not RHEL 6), if monetdbd runs under systemd,
  when the package is updated, monetdbd (and hence any mserver5 process
  it runs) is restarted.

* Mon Apr 20 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- monetdb5: The example module opt_sql_append is not installed in the binary
  packages anymore.

* Mon Apr 20 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- MonetDB: A new system to deal with debug output has been implemented.  There is
  now an option --dbtrace to mserver5 that takes a file argument to which
  debug output is written.  The default value is the file mdbtrace.log
  inside the database directory.  This option can also be set through
  the monetdb program.
- MonetDB: The home directory of the automatically created monetdb user was
  changed from /var/MonetDB to /var/lib/monetdb (RPM based systems
  only).  This home directory is (currently) not used for anything,
  though.
- MonetDB: Python 2 support has been removed.  There is now only support for
  using Python 3.

* Mon Apr 20 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- odbc: The NUMERIC and FLOAT types are now handled fully.  Before only DECIMAL,
  FLOAT, and DOUBLE were handled fully.
- odbc: Some bugs were fixed in the passing back and forth between application
  and server of values of type GUID (UUID).

* Thu Apr 16 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- clients: Removed the possibility of using the MD5 checksum for authentication
  purposes.  It was never actively used but was there as an option.
  Now the option has been removed.

* Thu Apr 16 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- sql: The sys.querylog_enable(threshold integer) now actually enables the
  querylog and uses a threshold in milliseconds.

* Wed Apr 15 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.37.1-20200511
- sql: Removed UNION JOIN statements. They were dropped by the SQL:2003
  standard, plus MonetDB implementation was not fully compliant.

* Wed Apr  1 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- sql: The OFFSET value in the COPY INTO query now counts uninterpreted
  newlines.  Before it counted "unquoted record separators" which meant
  that if you had a single quote on a line that you want to skip, you
  could not use the feature.

* Mon Mar 30 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- gdk: Implemented a version of BATproject, called BATproject2, with two
  "right" arguments which conceptually follow each other.

* Fri Mar 27 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.37.1-20200511
- sql: Added support for FROM RANGE MINVALUE TO RANGE MAXVALUE and FROM RANGE
  MINVALUE TO RANGE MAXVALUE WITH NULL VALUES cases in partitioned tables
  by range (before they weren't).

* Wed Mar 25 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- gdk: Removed MT_mmap and MT_munmap from the list of exported functions.
  Use GDKmmap and GDKmunmap with the same parameters instead.

* Fri Mar 20 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- gdk: Changed the interface of the atom "fix" and "unfix" functions.
  They now return a value of type gdk_return to indicate success/failure.

* Sat Feb 22 2020 Thodoris Zois <thodoris.zois@monetdbsolutions.com> - 11.37.1-20200511
- merovingian: Added dbtrace mserver5 option to the daemon in order to set
  mserver5's output directory for the produced traces.

* Sat Feb 22 2020 Thodoris Zois <thodoris.zois@monetdbsolutions.com> - 11.37.1-20200511
- monetdb5: Added mserver5 option (--dbtrace=<path>) in order to be able to
  specify the output file any produced traces.

* Sat Feb 22 2020 Panagiotis Koutsourakis <kutsurak@monetdbsolutions.com> - 11.37.1-20200511
- clients: Add port and host as fields in the .monetdb file.

* Sat Feb 22 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- MonetDB: Removed support for bam and sam files.

* Sat Feb 22 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.37.1-20200511
- sql: Implemented 'covar_pop' and 'covar_samp' aggregate functions, as well
  as their window function counterparts. Implemented 'stddev_samp',
  'stddev_pop', 'var_samp', 'var_pop', 'corr' and 'group_concat'
  window function correspondents.
- sql: Extended SQL catalog with CREATE WINDOW syntax for user-defined
  SQL:2003 window functions. At the moment, window functions must be
  defined on the backend engine, i.e. on this case MAL. In the current
  implementation, the backend code generation creates two additional
  columns of type lng with the start and end offsets for each row.

* Sat Feb 22 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- sql: Removed support for Python 2.  Python 2 itself is no longer
  supported.  Use Python 3 instead.  Functions that were declared as
  LANGUAGE PYTHON2 or LANGUAGE PYTHON2_MAP are changed to LANGUAGE
  PYTHON and LANGUAGE PYTHON_MAP respectively (without changing the
  Python code).

* Sat Feb 22 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.37.1-20200511
- sql: Added prepared_statements_args view, which details the arguments for
  the prepared statements created in the current session.

* Sat Feb 22 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.37.1-20200511
- sql: Added sys.prepared_statements view, which lists the available prepared
  statements in the current session.
- sql: Added deallocate statements with the syntax 'DEALLOCATE [PREPARE]
  { number | ALL }', to close an existing prepared statement or all,
  through the SQL layer. Previously this feature was available via MAPI
  exclusively with the "release" command.

* Sat Feb 22 2020 Panagiotis Koutsourakis <kutsurak@monetdbsolutions.com> - 11.37.1-20200511
- MonetDB: Added mserver5 option (--set raw_strings=true|false) and monetdb
  database property (raw_strings=yes|no) to control interpretation
  of strings.

* Sat Feb 22 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- gdk: Removed the tunique property.  The tunique property indicated that
  all values in the column had to be distinct.  It was removed because
  it wasn't used.

* Sat Feb 22 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- monetdb5: Removed function bat.setKey().

* Sat Feb 22 2020 Panagiotis Koutsourakis <kutsurak@monetdbsolutions.com> - 11.37.1-20200511
- sql: Added support for raw strings using the syntax r'' or R''. This means
  that C-like escapes will remain uninterpreted within those strings. For
  instance SELECT r'\"' returns a string of length two. The user needs
  to escape single quotes by doubling them: SELECT r''''.

* Sat Feb 22 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.37.1-20200511
- sql: Implemented ROLLUP, CUBE and GROUPING SETS from SQL:1999. They
  define grouping subsets used with the GROUP BY clause in order to
  compute partial groupings. Also, the GROUPING aggregate was
  added. This aggregate is a bitmask identifying the grouping columns
  not present in the generated grouping row when used with the
  operators described above.

* Sat Feb 22 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- gdk: BATrangeselect now has two extra arguments: anti and symmetric
  (both bool).

* Sat Feb 22 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- monetdb5: algebra.rangejoin now has two extra arguments: anti:bit and
  symmetric:bit.

* Sat Feb 22 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.37.1-20200511
- monetdb5: Added session identifier, number of workers and memory claim to the
  sysmon queue.
- monetdb5: The worker (number of threads), memory (in MB) and optimizer pipeline
  limits can now be set per user session basis. The query and session
  timeouts are now set in seconds.
- monetdb5: With required privileges an user can set resource limits for a session.

* Sat Feb 22 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.37.1-20200511
- sql: Updated user session procedures by adding the possibility to set properties
  based on a session identifier.
  Optimizer pipeline: sys.setoptimizer(int, string)
  Number of worker threads: sys.setworkerlimit(int, int)
  Memory limits (in MB): sys.setmemorylimit(int, int)
  Query timeout (in ms): sys.setquerytimeout(int, int)
  Session timeout (in ms): sys.setsessiontimeout(int, int)
  The first argument corresponds to the id of the session to modify, and
  these procedures are bound to the monetdb user exclusively.
  The versions of the mentioned procedures with just the second argument were
  added as well, where the changes are reflected in the current user session,
  and therefore every user can call them.
- sql: The procedures sys.settimeout(bigint), sys.settimeout(bigint,bigint)
  and sys.session(bigint) are now deprecated. Instead use sys.setquerytimeout
  and sys.setsessiontimeout mentioned above.

* Sat Feb 22 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.37.1-20200511
- monetdb5: There are now versions of group.(sub)group(done) that produce a single
  output containing just the groups.
- monetdb5: algebra.join and algebra.leftjoin now have forms which return a single
  column.  The column that is returned is the left column of the two
  column version.

* Sat Feb 22 2020 Joeri van Ruth <joeri.van.ruth@monetdbsolutions.com> - 11.37.1-20200511
- sql: Added SQL procedure sys.hot_snapshot() which can be used to write
  a snapshot of the database to a tar file. For example,
  sys.hot_snapshot('/tmp/snapshot.tar'). If compression support is
  compiled in, snapshots can also be compressed ('/tmp/snapshot.tar.gz').
  The tar file expands to a single directory with the same name as the
  database that was snapshotted. This directory can be passed directly
  as the --dbpath argument of mserver5 or it can be copied into an
  existing dbfarm and started from monetdbd.

* Sat Feb 22 2020 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.37.1-20200511
- clients: Added 'sessionid' column to system function sys.queue(), so each query
  gets tagged with the current session identifier

* Sat Feb 22 2020 Martin Kersten <mk@cwi.nl> - 11.37.1-20200511
- clients: Allow monetdb user to control session and query time out and selectively
  stopping a client sessions with a soft termination request.

* Sat Feb 22 2020 Martin Kersten <mk@cwi.nl> - 11.37.1-20200511
- monetdb5: The MAL profiler now assigns the SQL TRACE output to the client record
  thereby avoiding the interaction with other queries, but loosing
  insight of competing queries. The stethoscope should be used for that.

