%global name MonetDB
%global version 11.37.13
%{!?buildno: %global buildno %(date +%Y%m%d)}

# Use bcond_with to add a --with option; i.e., "without" is default.
# Use bcond_without to add a --without option; i.e., "with" is default.
# The --with OPTION and --without OPTION arguments can be passed on
# the commandline of both rpmbuild and mock.

# On 64 bit architectures we build "hugeint" packages.
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

%if %{?rhel:1}%{!?rhel:0} && 0%{?rhel} < 7
# RedHat Enterprise Linux < 7
# There is no macro _rundir, and no directory /run, instead use /var/run.
%global _rundir %{_localstatedir}/run
%endif

# On Fedora, the geos library is available, and so we can require it
# and build the geom modules.  On RedHat Enterprise Linux and
# derivatives (CentOS, Scientific Linux), the geos library is not
# available.  However, the geos library is available in the Extra
# Packages for Enterprise Linux (EPEL).  However, On RHEL 6, the geos
# library is too old for us, so we need an extra check for an
# up-to-date version of RHEL.
%if %{fedpkgs}
%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} >= 7
# By default create the MonetDB-geom-MonetDB5 package on Fedora and RHEL 7
%bcond_without geos
%endif
%endif

# On Fedora, the liblas library is available, and so we can require it
# and build the lidar modules.  On RedHat Enterprise Linux and
# derivatives (CentOS, Scientific Linux), the liblas library is only
# available if EPEL is enabled, and then only on version 7.
%if %{fedpkgs}
%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} == 7
# By default create the MonetDB-lidar package on Fedora and RHEL 7
%bcond_without lidar
%endif
%endif

# By default use PCRE for the implementation of the SQL LIKE and ILIKE
# operators.  Otherwise the POSIX regex functions are used.
%bcond_without pcre

%if %{fedpkgs}
# By default, create the MonetDB-R package.
%bcond_without rintegration
%endif

%if 0%{?rhel} >= 7 || 0%{?fedora} > 0
# On RHEL 6, Python 3 is too old.  On RHEL 7, Python 3 was too old
# when RHEL 7 was released, but now it is ok.
%bcond_without py3integration
%endif

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
Source: https://www.monetdb.org/downloads/sources/Jun2020-SP2/%{name}-%{version}.tar.bz2

# The Fedora packaging document says we need systemd-rpm-macros for
# the _unitdir and _tmpfilesdir macros to exist; however on RHEL 7
# that doesn't exist and we need systemd, so instead we just require
# the macro file that contains the definitions.
# We need checkpolicy and selinux-policy-devel for the SELinux policy.
%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} >= 7
# RHEL >= 7, and all current Fedora
BuildRequires: /usr/lib/rpm/macros.d/macros.systemd
BuildRequires: checkpolicy
BuildRequires: selinux-policy-devel
BuildRequires: hardlink
%endif
BuildRequires: gcc
BuildRequires: bison
BuildRequires: /usr/bin/python3
%if %{?rhel:1}%{!?rhel:0}
BuildRequires: bzip2-devel
%else
BuildRequires: pkgconfig(bzip2)
%endif
%if %{with fits}
BuildRequires: pkgconfig(cfitsio)
%endif
%if %{with geos}
BuildRequires: geos-devel >= 3.4.0
%endif
%if %{with lidar}
BuildRequires: liblas-devel >= 1.8.0
BuildRequires: pkgconfig(gdal)
%endif
BuildRequires: pkgconfig(libcurl)
BuildRequires: pkgconfig(liblzma)
# BuildRequires: libmicrohttpd-devel
BuildRequires: libuuid-devel
BuildRequires: pkgconfig(libxml-2.0)
BuildRequires: pkgconfig(openssl)
%if %{with pcre}
BuildRequires: pkgconfig(libpcre) >= 4.5
%endif
BuildRequires: readline-devel
BuildRequires: unixODBC-devel
# BuildRequires: uriparser-devel
BuildRequires: pkgconfig(zlib)
%if %{with py3integration}
BuildRequires: python3-devel >= 3.5
BuildRequires: python3-numpy
%endif
%if %{with rintegration}
BuildRequires: R-core-devel
%endif

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
Requires(post): unixODBC
Requires(postun): unixODBC

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
Requires: %{name}-SQL-server5%{?_isa} = %{version}-%{release}
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
%{_bindir}/odbcsample1
%{_bindir}/sample0
%{_bindir}/sample1
%{_bindir}/sample4
%{_bindir}/smack00
%{_bindir}/smack01
%{_bindir}/shutdowntest
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
extensions for %{name}-SQL-server5.

%files geom-MonetDB5
%defattr(-,root,root)
%{_libdir}/monetdb5/autoload/*_geom.mal
%{_libdir}/monetdb5/createdb/*_geom.sql
%{_libdir}/monetdb5/geom.mal
%{_libdir}/monetdb5/lib_geom.so
%endif

%if %{with lidar}
%package lidar
Summary: MonetDB5 SQL support for working with LiDAR data
Group: Applications/Databases
Requires: MonetDB5-server%{?_isa} = %{version}-%{release}

%description lidar
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains support for reading and writing LiDAR data.

%files lidar
%defattr(-,root,root)
%{_libdir}/monetdb5/autoload/*_lidar.mal
%{_libdir}/monetdb5/createdb/*_lidar.sql
%{_libdir}/monetdb5/lidar.mal
%{_libdir}/monetdb5/lib_lidar.so
%endif

%if %{with rintegration}
%package R
Summary: Integration of MonetDB and R, allowing use of R from within SQL
Group: Applications/Databases
Requires: MonetDB-SQL-server5%{?_isa} = %{version}-%{release}

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
%{_libdir}/monetdb5/rapi.*
%{_libdir}/monetdb5/autoload/*_rapi.mal
%{_libdir}/monetdb5/lib_rapi.so
%endif

%if %{with py3integration}
%package python3
Summary: Integration of MonetDB and Python, allowing use of Python from within SQL
Group: Applications/Databases
Requires: MonetDB-SQL-server5%{?_isa} = %{version}-%{release}

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
%{_libdir}/monetdb5/pyapi3.*
%{_libdir}/monetdb5/autoload/*_pyapi3.mal
%{_libdir}/monetdb5/lib_pyapi3.so
%endif

%if %{with fits}
%package cfitsio
Summary: MonetDB: Add on module that provides support for FITS files
Group: Applications/Databases
Requires: MonetDB-SQL-server5%{?_isa} = %{version}-%{release}

%description cfitsio
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains a module for accessing data in the FITS file
format.

%files cfitsio
%defattr(-,root,root)
%{_libdir}/monetdb5/fits.mal
%{_libdir}/monetdb5/autoload/*_fits.mal
%{_libdir}/monetdb5/createdb/*_fits.sql
%{_libdir}/monetdb5/lib_fits.so
%endif

%package -n MonetDB5-server
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Requires(pre): shadow-utils
Requires: %{name}-client%{?_isa} = %{version}-%{release}
%if (0%{?fedora} >= 22)
Recommends: %{name}-SQL-server5%{?_isa} = %{version}-%{release}
%if %{with hugeint}
Recommends: MonetDB5-server-hugeint%{?_isa} = %{version}-%{release}
%endif
Suggests: %{name}-client%{?_isa} = %{version}-%{release}
%endif
# versions up to 1.0.5 don't accept the queryid field in the result set
Conflicts: python-pymonetdb < 1.0.6
%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} >= 7
Requires(pre): systemd
%endif

%description -n MonetDB5-server
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the MonetDB server component.  You need this
package if you want to use the MonetDB database system.  If you want
to use the SQL front end, you also need %{name}-SQL-server5.

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
%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} >= 7
%{_sysusersdir}/monetdb.conf
%endif
%attr(2750,monetdb,monetdb) %dir %{_localstatedir}/lib/monetdb
%attr(2770,monetdb,monetdb) %dir %{_localstatedir}/monetdb5
%attr(2770,monetdb,monetdb) %dir %{_localstatedir}/monetdb5/dbfarm
%{_bindir}/mserver5
%exclude %{_bindir}/stethoscope
%{_libdir}/libmonetdb5.so.*
%dir %{_libdir}/monetdb5
%dir %{_libdir}/monetdb5/autoload
%if %{with fits}
%exclude %{_libdir}/monetdb5/fits.mal
%exclude %{_libdir}/monetdb5/autoload/*_fits.mal
%endif
%if %{with geos}
%exclude %{_libdir}/monetdb5/geom.mal
%endif
%if %{with lidar}
%exclude %{_libdir}/monetdb5/lidar.mal
%endif
%if %{with py3integration}
%exclude %{_libdir}/monetdb5/pyapi3.mal
%endif
%if %{with rintegration}
%exclude %{_libdir}/monetdb5/rapi.mal
%endif
%exclude %{_libdir}/monetdb5/sql*.mal
%if %{with hugeint}
%exclude %{_libdir}/monetdb5/*_hge.mal
%exclude %{_libdir}/monetdb5/autoload/*_hge.mal
%endif
%{_libdir}/monetdb5/*.mal
%if %{with geos}
%exclude %{_libdir}/monetdb5/autoload/*_geom.mal
%endif
%if %{with lidar}
%exclude %{_libdir}/monetdb5/autoload/*_lidar.mal
%endif
%if %{with py3integration}
%exclude %{_libdir}/monetdb5/autoload/*_pyapi3.mal
%endif
%if %{with rintegration}
%exclude %{_libdir}/monetdb5/autoload/*_rapi.mal
%endif
%exclude %{_libdir}/monetdb5/autoload/??_sql*.mal
%{_libdir}/monetdb5/autoload/*.mal
%{_libdir}/monetdb5/lib_capi.so
%{_libdir}/monetdb5/lib_generator.so
%{_libdir}/monetdb5/lib_udf.so
%doc %{_mandir}/man1/mserver5.1.gz
%dir %{_datadir}/doc/MonetDB
%docdir %{_datadir}/doc/MonetDB
%{_datadir}/doc/MonetDB/*

%if %{with hugeint}
%package -n MonetDB5-server-hugeint
Summary: MonetDB - 128-bit integer support for MonetDB5-server
Group: Applications/Databases
Requires: MonetDB5-server%{?_isa}

%description -n MonetDB5-server-hugeint
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package provides HUGEINT (128-bit integer) support for the
MonetDB5-server component.

%files -n MonetDB5-server-hugeint
%exclude %{_libdir}/monetdb5/sql*_hge.mal
%{_libdir}/monetdb5/*_hge.mal
%exclude %{_libdir}/monetdb5/autoload/??_sql_hge.mal
%{_libdir}/monetdb5/autoload/*_hge.mal
%endif

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
%dir %{_includedir}/monetdb
%{_includedir}/monetdb/mal*.h
%{_libdir}/libmonetdb5.so
%{_libdir}/pkgconfig/monetdb5.pc

%package SQL-server5
Summary: MonetDB5 SQL server modules
Group: Applications/Databases
Requires(pre): MonetDB5-server%{?_isa} = %{version}-%{release}
%if (0%{?fedora} >= 22)
%if %{with hugeint}
Recommends: %{name}-SQL-server5-hugeint%{?_isa} = %{version}-%{release}
%endif
Suggests: %{name}-client%{?_isa} = %{version}-%{release}
%endif
%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} >= 7
%{?systemd_requires}
%endif

%description SQL-server5
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the SQL front end for MonetDB.  If you want to
use SQL with MonetDB, you will need to install this package.

%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} >= 7
%post SQL-server5
%systemd_post monetdbd.service

%preun SQL-server5
%systemd_preun monetdbd.service

%postun SQL-server5
%systemd_postun_with_restart monetdbd.service
%endif

%files SQL-server5
%defattr(-,root,root)
%{_bindir}/monetdb
%{_bindir}/monetdbd
%dir %attr(775,monetdb,monetdb) %{_localstatedir}/log/monetdb
%dir %attr(775,monetdb,monetdb) %{_rundir}/monetdb
%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} >= 7
# RHEL >= 7, and all current Fedora
%{_tmpfilesdir}/monetdbd.conf
%{_unitdir}/monetdbd.service
%else
# RedHat Enterprise Linux < 7
%exclude %{_sysconfdir}/tmpfiles.d/monetdbd.conf
# no _unitdir macro
%exclude %{_prefix}/lib/systemd/system/monetdbd.service
%endif
%config(noreplace) %attr(664,monetdb,monetdb) %{_localstatedir}/monetdb5/dbfarm/.merovingian_properties
%verify(not mtime) %attr(664,monetdb,monetdb) %{_localstatedir}/monetdb5/dbfarm/.merovingian_lock
%config(noreplace) %attr(644,root,root) %{_sysconfdir}/logrotate.d/monetdbd
%{_libdir}/monetdb5/autoload/??_sql.mal
%{_libdir}/monetdb5/lib_sql.so
%dir %{_libdir}/monetdb5/createdb
%if %{with fits}
%exclude %{_libdir}/monetdb5/createdb/*_fits.sql
%endif
%if %{with geos}
%exclude %{_libdir}/monetdb5/createdb/*_geom.sql
%endif
%if %{with lidar}
%exclude %{_libdir}/monetdb5/createdb/*_lidar.sql
%endif
%{_libdir}/monetdb5/createdb/*.sql
%{_libdir}/monetdb5/sql*.mal
%if %{with hugeint}
%exclude %{_libdir}/monetdb5/createdb/*_hge.sql
%exclude %{_libdir}/monetdb5/sql*_hge.mal
%endif
%doc %{_mandir}/man1/monetdb.1.gz
%doc %{_mandir}/man1/monetdbd.1.gz
%dir %{_datadir}/doc/MonetDB-SQL
%docdir %{_datadir}/doc/MonetDB-SQL
%{_datadir}/doc/MonetDB-SQL/*

%if %{with hugeint}
%package SQL-server5-hugeint
Summary: MonetDB5 128 bit integer (hugeint) support for SQL
Group: Applications/Databases
Requires: MonetDB5-server-hugeint%{?_isa} = %{version}-%{release}
Requires: MonetDB-SQL-server5%{?_isa} = %{version}-%{release}

%description SQL-server5-hugeint
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package provides HUGEINT (128-bit integer) support for the SQL
front end of MonetDB.

%files SQL-server5-hugeint
%defattr(-,root,root)
%{_libdir}/monetdb5/autoload/??_sql_hge.mal
%{_libdir}/monetdb5/createdb/*_hge.sql
%{_libdir}/monetdb5/sql*_hge.mal
%endif

%package testing
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases

%description testing
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL front end.

This package contains the programs and files needed for testing the
MonetDB packages.  You probably don't need this, unless you are a
developer.  If you do want to test, install %{name}-testing-python.

%files testing
%license COPYING
%defattr(-,root,root)
%{_bindir}/Mdiff
%{_bindir}/Mlog

%package testing-python
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Requires: %{name}-testing = %{version}-%{release}
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
%{_bindir}/Mapprove.py
%{_bindir}/Mtest.py
%dir %{python3_sitelib}/MonetDBtesting
%{python3_sitelib}/MonetDBtesting/*

%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} >= 7
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
%doc buildtools/selinux/monetdb.*
%{_datadir}/selinux/*/monetdb.pp

%endif

%prep
%setup -q

%build

# There is a bug in GCC version 4.8 on AArch64 architectures
# that causes it to report an internal error when compiling
# testing/difflib.c.  The work around is to not use -fstack-protector-strong.
# The bug exhibits itself on CentOS 7 on AArch64.
# Everywhere else, add -Wno-format-truncation to the compiler options
# to reduce the number of warnings during compilation.
%ifarch aarch64
    if gcc -v 2>&1 | grep -q 'gcc version 4\.'; then
	CFLAGS="${CFLAGS:-$(echo %optflags | sed 's/-fstack-protector-strong//')}"
    else
	CFLAGS="${CFLAGS:-%optflags -Wno-format-truncation}"
    fi
%else
    CFLAGS="${CFLAGS:-%optflags -Wno-format-truncation}"
%endif
export CFLAGS
# do not use --enable-optimize or --disable-optimize: we don't want
# any changes to optimization flags
%{configure} \
 	--with-rundir=%{_rundir}/monetdb \
	--enable-assert=no \
	--enable-debug=yes \
	--enable-developer=no \
	--enable-embedded=no \
	--enable-embedded-r=no \
	--enable-fits=%{?with_fits:yes}%{!?with_fits:no} \
	--enable-geom=%{?with_geos:yes}%{!?with_geos:no} \
	--enable-int128=%{?with_hugeint:yes}%{!?with_hugeint:no} \
	--enable-lidar=%{?with_lidar:yes}%{!?with_lidar:no} \
	--enable-mapi=yes \
	--enable-netcdf=no \
	--enable-odbc=yes \
	--enable-profiler=no \
	--enable-py3integration=%{?with_py3integration:yes}%{!?with_py3integration:no} \
	--enable-rintegration=%{?with_rintegration:yes}%{!?with_rintegration:no} \
	--enable-sanitizer=no \
	--enable-shp=no \
	--enable-static-analysis=no \
	--enable-strict=no \
	--enable-testing=yes \
	--with-bz2=yes \
	--with-curl=yes \
	--with-gdal=%{?with_lidar:yes}%{!?with_lidar:no} \
	--with-geos=%{?with_geos:yes}%{!?with_geos:no} \
	--with-liblas=%{?with_lidar:yes}%{!?with_lidar:no} \
	--with-libxml2=yes \
	--with-lz4=no \
	--with-lzma=yes \
	--with-openssl=yes \
	--with-proj=no \
	--with-pthread=yes \
	--with-python3=yes \
	--with-readline=yes \
	--with-regex=%{?with_pcre:PCRE}%{!?with_pcre:POSIX} \
	--with-snappy=no \
	--with-unixodbc=yes \
	--with-uuid=yes \
	--with-valgrind=no \
	%{?comp_cc:CC="%{comp_cc}"}

%make_build

%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} >= 7
cd buildtools/selinux
%if 0%{?fedora} < 27
# no `map' policy available before Fedora 27
sed -i '/map/d' monetdb.te
%endif

for selinuxvariant in %{selinux_variants}
do
  make NAME=${selinuxvariant} -f /usr/share/selinux/devel/Makefile
  mv monetdb.pp monetdb.pp.${selinuxvariant}
  make NAME=${selinuxvariant} -f /usr/share/selinux/devel/Makefile clean
done
cd -
%endif

%install
%make_install

# move file to correct location
%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} >= 7
mkdir -p %{buildroot}%{_tmpfilesdir} %{buildroot}%{_sysusersdir}
mv %{buildroot}%{_sysconfdir}/tmpfiles.d/monetdbd.conf %{buildroot}%{_tmpfilesdir}
cat > %{buildroot}%{_sysusersdir}/monetdb.conf << EOF
u monetdb - "MonetDB Server" /var/lib/monetdb
EOF
rmdir %{buildroot}%{_sysconfdir}/tmpfiles.d
%endif

install -d -m 0750 %{buildroot}%{_localstatedir}/lib/monetdb
install -d -m 0770 %{buildroot}%{_localstatedir}/monetdb5/dbfarm
install -d -m 0775 %{buildroot}%{_localstatedir}/log/monetdb
install -d -m 0775 %{buildroot}%{_rundir}/monetdb

# remove unwanted stuff
# .la files
rm -f %{buildroot}%{_libdir}/*.la
rm -f %{buildroot}%{_libdir}/monetdb5/*.la
rm -f %{buildroot}%{_libdir}/monetdb5/opt_sql_append.mal
rm -f %{buildroot}%{_libdir}/monetdb5/lib_opt_sql_append.so
rm -f %{buildroot}%{_libdir}/monetdb5/autoload/??_opt_sql_append.mal

%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} >= 7
for selinuxvariant in %{selinux_variants}
do
  install -d %{buildroot}%{_datadir}/selinux/${selinuxvariant}
  install -p -m 644 buildtools/selinux/monetdb.pp.${selinuxvariant} \
    %{buildroot}%{_datadir}/selinux/${selinuxvariant}/monetdb.pp
done
if [ -x /usr/sbin/hardlink ]; then
    /usr/sbin/hardlink -cv %{buildroot}%{_datadir}/selinux
else
    # Fedora 31
    /usr/bin/hardlink -cv %{buildroot}%{_datadir}/selinux
fi
%endif

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%changelog
* Mon May 09 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.37.13-20220509
- Rebuilt.
- GH#6938: Segmentation fault in MalOptimizer
- GH#6939: Error in optimizer multiplex when selecting
  profiler.getlimit() or wlc.clock() or wlc.tick() or wlr.clock() or
  wlr.tick()
- GH#6950: redundant/replicated code line in gdk/gdk_hash.c
- GH#6954: FILTER functions no longer find their implementation
- GH#6955: ROUND(DECIMAL, PRECISION) gives incorrect result with
  non-scalar precision parameter
- GH#6964: Table returning function: Cannot access column descriptor
- GH#6965: Crash when using distinct on the result of a table returning
  function
- GH#7108: Monetdb crashes on query execution

* Mon May  9 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.37.13-20220509
- gdk: Improve the heuristic for growing the result bat in a rangejoin.
- gdk: Improved management of bats around commit time, both at the SQL level
  (write-ahead log) and low level (so-called subcommit).
- gdk: Improved BATproject for string bats.
- gdk: Improved low-level algorithm selection in join.

* Mon May  9 2022 Sjoerd Mullender <sjoerd@acm.org> - 11.37.13-20220509
- sql: Print current query when using CALL
  logging.setcomplevel('SQL_EXECUTION', 'INFO');
- sql: Comments inside queries are now available in the sys.queue
  function/view.

* Mon May  3 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.37.13-20220509
- gdk: A bug that would very occasionally produce an error "strPut: incorrectly
  encoded UTF-8", even when no incorrectly coded characters are used
  at all, has been fixed.  It was the result of a rare combination of
  strings having been added to the column that caused essentially an
  off-by-one type of error to occur.

* Mon May  3 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.37.13-20220509
- merovingian: When stopping monetdbd using the `monetdbd stop' command, this command
  now waits for 5 seconds longer than the exittimeout value before it
  kills the monetdbd daemon instead of only 30 seconds total (or until
  that daemon stops earlier).  This gives the daemon enough time to
  terminate the mserver5 processes that it is managing.  If exittimeout
  is negative, the daemon and the monetdbd process initiating the stop
  wait indefinitely until the mserver5 processes have stopped.

* Mon May  3 2021 Sjoerd Mullender <sjoerd@acm.org> - 11.37.13-20220509
- sql: A bug where a sequence of TRUNCATE TABLE and COPY INTO the just
  truncated table would result in success being reported to both queries,
  but the table still being empty afterwards, has been fixed.

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

* Sat Feb 22 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.35.19-20200222
- Rebuilt.
- BZ#6829: NTILE window function returns incorrect results

* Fri Feb 21 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.35.17-20200221
- Rebuilt.
- BZ#6827: CUME_DIST window function returns incorrect results

* Mon Feb 17 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.35.15-20200217
- Rebuilt.
- BZ#6817: running analyze on a schema which contains a stream table
  stops with an error
- BZ#6819: functions do not persist

* Wed Feb 12 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.35.13-20200212
- Rebuilt.

* Tue Feb 11 2020 Sjoerd Mullender <sjoerd@acm.org> - 11.35.11-20200211
- Rebuilt.
- BZ#6805: Using the cascade operator in a drop table statement ends in
  an exit from the Monetdb shell.
- BZ#6807: Median_avg and quantile_avg ignore NULL values
- BZ#6815: query with ifthenelse() crashes mserver5
- BZ#6816: Monetdb Crashes on INSERT statement after ALTER statement in
  another connection

* Wed Dec 18 2019 Sjoerd Mullender <sjoerd@acm.org> - 11.35.9-20191218
- Rebuilt.
- BZ#6804: DNS resolution of 0.0.0.0 fails on recent Ubuntus

* Tue Dec 17 2019 Sjoerd Mullender <sjoerd@acm.org> - 11.35.7-20191217
- Rebuilt.

* Thu Dec 12 2019 Sjoerd Mullender <sjoerd@acm.org> - 11.35.5-20191212
- Rebuilt.
- BZ#6723: columns aliases duplicates should not be allowed. automatic
  aliasing required.
- BZ#6724: Prepare confuses types when more than one argument is used
- BZ#6726: Python aggregation does not create aggr_group when aggregating
  over all rows
- BZ#6765: GRANT SELECT privilege on a subset of table columns results
  in access denied error when selecting the same columns from the table
- BZ#6790: Count distinct giving wrong results
- BZ#6791: str_to_time('11:40', '%H:%M') creates wrong time value
- BZ#6792: JSON path compiler accepts invalid input
- BZ#6793: cast(interval second value to int or decimal) is wrong (by
  a factor of 1000), cast(interval month value to decimal or floating
  point) fails
- BZ#6794: external name fits.listdir not bound (sys.listdir) Fatal
  error during initialization:
- BZ#6796: Incorrect crash time reported by monetdb tool after crash
  of mserver5
- BZ#6798: json.text off by one error

* Mon Nov 25 2019 Sjoerd Mullender <sjoerd@acm.org> - 11.35.3-20191125
- Rebuilt.
- BZ#3533: SQL aggregate functions avg(), sum() and median() return an
  error when used on a column with datatype interval second

* Mon Nov 18 2019 Sjoerd Mullender <sjoerd@acm.org> - 11.35.1-20191118
- Rebuilt.
- BZ#6134: Query produces error: HEAPalloc: Insufficient space for HEAP
  of 1168033427456 bytes.
- BZ#6613: LATERAL crash /.../rel_bin.c:1473: rel2bin_table: Assertion
  `0' failed.
- BZ#6683: Bug in subselect
- BZ#6686: Bug in subselect (count function)
- BZ#6688: Bug in subselect (or condition)
- BZ#6689: Trying to improve the performance of SQL queries with a large
  list of members in IN clause.
- BZ#6695: timestamp transformation
- BZ#6700: Monetdb Bugs in Subselect statements:
- BZ#6722: window functions issues
- BZ#6740: while upgrading the database from verison (MonetDB-11.27.13)
  to (MonetDB-11.33.3) we are unable to bring up the database
- BZ#6754: in mclient a strang msg is reported after issueing command:
  set schema sys;
- BZ#6755: Assertion failure in rel_bin.c
- BZ#6756: Error in optimizer garbageCollector on merge tables select
- BZ#6757: Double free or corruption (out)
- BZ#6758: SIGSEGV in __strcmp_sse2_unaligned()
- BZ#6759: Python JSON loader creates invalid data type for strings
- BZ#6761: Error: Program contains errors.:(NONE).multiplex
- BZ#6762: mserver5 crashes on (re-)start
- BZ#6764: mserver5 crashes with corruption, double free, invalid size
  or invalid pointer
- BZ#6766: Missing bulk implementation for get_value and next_value calls
- BZ#6769: ProfilerStart is not threadsafe
- BZ#6771: R-devel
- BZ#6773: json.filter returns corrupted string when selecting JSON
  null value
- BZ#6774: PROD aggregation gives wrong result
- BZ#6775: NOT IN with an AND containing an OR gives wrong result
- BZ#6776: Creating a table with a full outer join query gives type with
  wrong digits on the joined key.
- BZ#6779: Using Windows Messages translation for errno error codes.
- BZ#6780: Wrong value of the rank function
- BZ#6781: Insert after index creation crash
- BZ#6782: JDBC IsValid(int) does not reset lastquerytimeout on server
- BZ#6783: AVG changes scale of its results
- BZ#6784: function sys.isauuid(string) should return false if string
  value cannot be converted to a UUID

* Mon Nov  4 2019 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.35.1-20191118
- sql: Removed functions json.text(string) returns string and json.text(int)
  returns string. Their MAL implementation didn't exist, so they were
  meaningless.

* Thu Oct 17 2019 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.35.1-20191118
- merovingian: Added "vmmaxsize" and "memmaxsize" mserver5 options to the daemon in
  order to set mserver5's maximum virtual and committed memory
  respectively.

* Wed Sep 25 2019 Sjoerd Mullender <sjoerd@acm.org> - 11.35.1-20191118
- sql: Strings are now limited to 1GB, double-quoted tokens are limited to 2kB.
  These sizes are bytes of (UTF-8 encoded) input data.

* Mon Sep 23 2019 Sjoerd Mullender <sjoerd@acm.org> - 11.35.1-20191118
- sql: There are new aggregate functions sys.median_avg and sys.quantile_avg
  that return the interpolated value if the median/quantile doesn't fall
  exactly on a particular row.  These functions always return a value
  of type DOUBLE and only work for numeric types (various width integers
  and floating point).

* Sun Sep  8 2019 Sjoerd Mullender <sjoerd@acm.org> - 11.35.1-20191118
- gdk: BATcalcbetween and all its variants now have an extra bool parameter
  "anti" to invert the test.

* Thu Sep  5 2019 Sjoerd Mullender <sjoerd@acm.org> - 11.35.1-20191118
- monetdb5: The server "console" has been removed, as has the --daemon option.
  The server now doesn't read from standard input anymore.  The way to
  stop a server is by sending it a TERM signal (on Linux/Unix) or by
  sending it an interrupt signal (usually control-C -- on all
  systems).

* Fri Aug 30 2019 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.35.1-20191118
- sql: Added sys.deltas ("schema" string, "table" string, "column" string)
  returns table ("values" bigint) system function which returns a single
  column with 6 values: a flag indicating if the column's upper table is
  cleared or not, the count of the RDONLY, RD_INS and RD_UPD_ID deltas
  of the column itself, the number of deleted values of the column's
  table, as well as the level of the current transaction in the
  transaction level tree. It should be used for debugging purposes only.

* Fri Aug 30 2019 Sjoerd Mullender <sjoerd@acm.org> - 11.35.1-20191118
- monetdb5: Implemented a function bat.diffcand to calculate difference of two
  candidate lists.

* Fri Aug 30 2019 Sjoerd Mullender <sjoerd@acm.org> - 11.35.1-20191118
- monetdb5: The mtime module was completely rewritten, the atom types date,
  daytime, and timestamp were changed.  Upgrade code for BATs
  containing these types has been implemented.  The old daytime type
  used a 32 bit integer to record milliseconds since the start of the
  day.  The new daytime type uses a 64 bit integer to record
  microseconds since the start of the day.  The old date type recorded
  days since or before the year 1.  The new daytime type records the
  day of the month and the number of months since the year -4712
  separately in a single 32 bit integer of which only 26 bits are
  used.  Dates now use the proleptic Gregorian calendar, meaning the
  normal Gregorian callendar has been extended backward, and before
  the year 1, we have the year 0.  Both the old and new timestamp
  types are a combination of a daytime and a date value, but since
  those types have changed, the timestamp type has also changed.  The
  new date type has a smaller range than the old.  The new date range
  is from the year -4712 to the year 170049.  During conversion of
  date and timestamp columns, the dates are clamped to this range.
- monetdb5: The tzone and rule atom types have been removed.  They were not used
  by any code, and they were defined in a non-portable way.

* Fri Aug 30 2019 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.35.1-20191118
- sql: Added "VALUES row_list" statement as a top SQL projection statement.

* Fri Aug 30 2019 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.35.1-20191118
- merovingian: Added ipv6 property to monetdbd properties to force IPv6 addresses
  binding only.  By default this property is false to allow IPv4
  addresses as well.

* Fri Aug 30 2019 Pedro Ferreira <pedro.ferreira@monetdbsolutions.com> - 11.35.1-20191118
- monetdb5: Added "mapi_ipv6" property to monet_options to force ipv6 address
  binding only.  This property is inherited while forking from
  monetdbd if it is also set there.

* Fri Aug 30 2019 Sjoerd Mullender <sjoerd@acm.org> - 11.35.1-20191118
- gdk: All forms of BATcalcbetween and VARcalcbetween have extra bool arguments
  for low inclusive, high inclusive and nils false.  The latter causes
  the result to be false instead of nil if the value being checked is nil.

* Fri Aug 30 2019 Sjoerd Mullender <sjoerd@acm.org> - 11.35.1-20191118
- monetdb5: Removed (bat)calc.between_symmetric and changed (bat)calc.between
  by adding a number of extra arguments, all of type :bit: symmetric,
  low inclusive, high inclusive, nils false.

* Fri Aug 30 2019 Aris Koning <aris.koning@monetdbsolutions.com> - 11.35.1-20191118
- sql: The implementation of in-expression now follows a join-based approach
  instead of using iterative union/selects. This greatly improves
  performance for large in-value-lists. Furthermore the old approach has
  large in-value-lists generate large MAL plans. This is now no longer
  the case.

