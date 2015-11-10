%define name MonetDB
%define version 11.22.0
%{!?buildno: %define buildno %(date +%Y%m%d)}

# groups of related archs
%define all_x86 i386 i586 i686

%ifarch %{all_x86}
%define bits 32
%else
%define bits 64
%endif

# only add .oidXX suffix if oid size differs from bit size
%if %{bits} == 64 && %{?oid32:1}%{!?oid32:0}
%define oidsuf .oid32
%endif

%define release %{buildno}%{?dist}%{?oidsuf}

# On RedHat Enterprise Linux and derivatives, if the Extra Packages
# for Enterprise Linux (EPEL) repository is available, you can define
# the _with_epel macro.  When using mock to build the RPMs, this can
# be done using the --with=epel option to mock.
# If the EPEL repository is availabe, or if building for Fedora, all
# optional sub packages can be built.  We indicate that here by
# setting the macro fedpkgs to 1.  If the EPEL repository is not
# available and we are not building for Fedora, we set fedpkgs to 0.
%if %{?rhel:1}%{!?rhel:0}
# RedHat Enterprise Linux (or CentOS or Scientific Linux)
%if %{?_with_epel:1}%{!?_with_epel:0}
# EPEL is enabled through the command line
%define fedpkgs 1
%else
# EPEL is not enabled
%define fedpkgs 0
%endif
%else
# Not RHEL (so presumably Fedora)
%define fedpkgs 1
%endif

# On Fedora, the geos library is available, and so we can require it
# and build the geom modules.  On RedHat Enterprise Linux and
# derivatives (CentOS, Scientific Linux), the geos library is not
# available.  However, the geos library is available in the Extra
# Packages for Enterprise Linux (EPEL).  In other words, we can check
# the fedpkgs macro (see above).
%if %{fedpkgs}
%define with_geos 1
%endif

%if %{?rhel:0}%{!?rhel:1}
# If the _without_samtools macro is set, the MonetDB-bam-MonetDB5 RPM
# will be created.  The macro can be set when using mock by passing it
# the flag --without=samtools.
# Note that the samtools-devel RPM is not available on RedHat
# Enterprise Linux and derivatives, even with EPEL availabe.
# (Actually, at the moment of writing, samtools-devel is available in
# EPEL for RHEL 6, but not for RHEL 7.  We don't make the distinction
# here and just not build the MonetDB-bam-MonetDB5 RPM.)
%if %{?_without_samtools:0}%{!?_without_samtools:1}
%define with_samtools 1
%endif
%endif

%if %{fedpkgs}
# If the _without_rintegration macro is not set, the MonetDB-R RPM
# will be created.  The macro can be set when using mock by passing it
# the flag --without=rintegration.
%if %{?_without_rintegration:0}%{!?_without_rintegration:1}
%define with_rintegration 1
%endif
%endif

%if %{fedpkgs}
# If the _with_fits macro is set, the MonetDB-cfitsio RPM will be
# created.  The macro can be set when using mock by passing it the
# flag --with=fits.
%if %{?_with_fits:1}%{!?_with_fits:0}
%define with_fits 1
%endif
%endif

Name: %{name}
Version: %{version}
Release: %{release}
Summary: MonetDB - Monet Database Management System
Vendor: MonetDB BV <info@monetdb.org>

Group: Applications/Databases
License: MPL - http://www.monetdb.org/Legal/MonetDBLicense
URL: http://www.monetdb.org/
Source: http://dev.monetdb.org/downloads/sources/Jul2015-SP1/%{name}-%{version}.tar.bz2

BuildRequires: bison
BuildRequires: bzip2-devel
%if %{?with_fits:1}%{!?with_fits:0}
BuildRequires: cfitsio-devel
%endif
%if %{?with_geos:1}%{!?with_geos:0}
BuildRequires: geos-devel >= 3.0.0
%endif
BuildRequires: gsl-devel
BuildRequires: libatomic_ops-devel
BuildRequires: libcurl-devel
# BuildRequires: libmicrohttpd-devel
# BuildRequires: libsphinxclient-devel
BuildRequires: libuuid-devel
BuildRequires: libxml2-devel
BuildRequires: openssl-devel
BuildRequires: pcre-devel >= 4.5
BuildRequires: perl
BuildRequires: python-devel
%if %{?rhel:0}%{!?rhel:1}
BuildRequires: python3-devel
%endif
BuildRequires: readline-devel
# On RedHat Enterprise Linux and derivatives (CentOS, Scientific
# Linux), the rubygem-activerecord package is not available (also not
# in the Extra Packages for Enterprise Linux EPEL), so it makes no
# sense providing our ruby packages.
%if %{?rhel:0}%{!?rhel:1}
BuildRequires: ruby
BuildRequires: rubygems
BuildRequires: rubygems-devel
%endif
BuildRequires: unixODBC-devel
# BuildRequires: uriparser-devel
BuildRequires: zlib-devel
%if %{?with_samtools:1}%{!?with_samtools:0}
BuildRequires: samtools-devel
%endif
%if %{?with_rintegration:1}%{!?with_rintegration:0}
BuildRequires: R-core-devel
%endif

%if (0%{?fedora} >= 22)
Recommends: %{name}-SQL-server5%{?_isa} = %{version}-%{release}
Recommends: MonetDB5-server%{?_isa} = %{version}-%{release}
Suggests: %{name}-client%{?_isa} = %{version}-%{release}
%endif

# need to define python_sitelib on RHEL 5 and older
# no need to define python3_sitelib: it's defined by python3-devel
%if 0%{?rhel} && 0%{?rhel} <= 5
%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}
%endif

%description
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the core components of MonetDB in the form of a
single shared library.  If you want to use MonetDB, you will certainly
need this package, but you will also need at least the MonetDB5-server
package, and most likely also %{name}-SQL-server5, as well as one or
more client packages.

%files
%defattr(-,root,root)
%{_libdir}/libbat.so.*

%package devel
Summary: MonetDB development files
Group: Applications/Databases
Requires: %{name}%{?_isa} = %{version}-%{release}
Requires: %{name}-stream-devel%{?_isa} = %{version}-%{release}
Requires: libatomic_ops-devel

%description devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains files needed to develop extensions to the core
functionality of MonetDB.

%files devel
%defattr(-,root,root)
%dir %{_includedir}/monetdb
%{_includedir}/monetdb/gdk*.h
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
accelerators.  It also has an SQL frontend.

This package contains a shared library (libstream) which is needed by
various other components.

%files stream
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
accelerators.  It also has an SQL frontend.

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
accelerators.  It also has an SQL frontend.

This package contains mclient, the main client program to communicate
with the MonetDB database server, and msqldump, a program to dump the
SQL database so that it can be loaded back later.  If you want to use
MonetDB, you will very likely need this package.

%files client
%defattr(-,root,root)
%{_bindir}/mclient
%{_bindir}/msqldump
%{_libdir}/libmapi.so.*
%doc %{_mandir}/man1/mclient.1.gz
%doc %{_mandir}/man1/msqldump.1.gz

%package client-tools
Summary: MonetDB - Monet Database Management System Client Programs
Group: Applications/Databases
Requires: %{name}-client%{?_isa} = %{version}-%{release}

%description client-tools
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains stethoscope, tomograph, and tachograph.  These
tools can be used to monitor the MonetDB database server.

%files client-tools
%defattr(-,root,root)
%{_bindir}/stethoscope
%{_bindir}/tachograph
%{_bindir}/tomograph
%dir %{_datadir}/doc/MonetDB-client-tools
%docdir %{_datadir}/doc/MonetDB-client-tools
%{_datadir}/doc/MonetDB-client-tools/*

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
accelerators.  It also has an SQL frontend.

This package contains the files needed to develop with the
%{name}-client package.

%files client-devel
%defattr(-,root,root)
%dir %{_includedir}/monetdb
%{_libdir}/libmapi.so
%{_includedir}/monetdb/mapi.h
%{_libdir}/pkgconfig/monetdb-mapi.pc

%package client-odbc
Summary: MonetDB ODBC driver
Group: Applications/Databases
Requires: %{name}-client%{?_isa} = %{version}-%{release}
Requires(pre): unixODBC

%description client-odbc
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

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
%defattr(-,root,root)
%{_libdir}/libMonetODBC.so
%{_libdir}/libMonetODBCs.so

%package client-php
Summary: MonetDB php interface
Group: Applications/Databases
Requires: php
BuildArch: noarch

%description client-php
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the files needed to use MonetDB from a PHP
program.

%files client-php
%defattr(-,root,root)
%dir %{_datadir}/php/monetdb
%{_datadir}/php/monetdb/*

%package client-perl
Summary: MonetDB perl interface
Group: Applications/Databases
Requires: perl(:MODULE_COMPAT_%(eval "`%{__perl} -V:version`"; echo $version))
Requires: perl(DBI)
Requires: perl(Digest::SHA)
Requires: perl(Digest::MD5)
# when not using BuildArch: noarch, globally replace perl_vendorlib by
# perl_vendorarch
BuildArch: noarch
%{?perl_default_filter}
%global __requires_exclude perl\\(DBD::monetdb|perl\\(MonetDB::|perl\\(Mapi\\)

%description client-perl
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the files needed to use MonetDB from a Perl
program.

%files client-perl
%defattr(-,root,root)
%{perl_vendorlib}/*

%if %{?rhel:0}%{!?rhel:1}
%package -n rubygem-monetdb-sql
Summary: MonetDB ruby interface
Group: Applications/Databases
Requires: ruby(release)
Obsoletes: %{name}-client-ruby
BuildArch: noarch

%description -n rubygem-monetdb-sql
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the files needed to use MonetDB from a Ruby
program.

%files -n rubygem-monetdb-sql
%defattr(-,root,root)
%docdir %{gem_dir}/doc/ruby-monetdb-sql-0.2
%{gem_dir}/doc/ruby-monetdb-sql-0.2/*
%{gem_dir}/cache/ruby-monetdb-sql-0.2.gem
# %dir %{gem_dir}/gems/ruby-monetdb-sql-0.2
%{gem_dir}/gems/ruby-monetdb-sql-0.2
%{gem_dir}/specifications/ruby-monetdb-sql-0.2.gemspec
%endif

%package client-tests
Summary: MonetDB Client tests package
Group: Applications/Databases
Requires: MonetDB5-server%{?_isa} = %{version}-%{release}
Requires: %{name}-client%{?_isa} = %{version}-%{release}
Requires: %{name}-client-odbc%{?_isa} = %{version}-%{release}
Requires: %{name}-client-perl = %{version}-%{release}
Requires: %{name}-client-php = %{version}-%{release}
Requires: %{name}-SQL-server5%{?_isa} = %{version}-%{release}
Requires: python-monetdb = %{version}-%{release}

%description client-tests
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the sample MAPI programs used for testing other
MonetDB packages.  You probably don't need this, unless you are a
developer.

%files client-tests
%defattr(-,root,root)
%{_bindir}/arraytest
%{_bindir}/odbcsample1
%{_bindir}/sample0
%{_bindir}/sample1
%{_bindir}/sample2
%{_bindir}/sample3
%{_bindir}/sample4
%{_bindir}/smack00
%{_bindir}/smack01
%{_bindir}/testgetinfo
%{_bindir}/malsample.pl
%{_bindir}/sqlsample.php
%{_bindir}/sqlsample.pl

%if %{?with_geos:1}%{!?with_geos:0}
%package geom-MonetDB5
Summary: MonetDB5 SQL GIS support module
Group: Applications/Databases
Requires: MonetDB5-server%{?_isa} = %{version}-%{release}
Obsoletes: %{name}-geom
Obsoletes: %{name}-geom-devel

%description geom-MonetDB5
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the GIS (Geographic Information System)
extensions for %{name}-SQL-server5.

%files geom-MonetDB5
%defattr(-,root,root)
%{_libdir}/monetdb5/autoload/*_geom.mal
%{_libdir}/monetdb5/createdb/*_geom.sql
%{_libdir}/monetdb5/geom.mal
%{_libdir}/monetdb5/lib_geom.so
%endif

%package gsl-MonetDB5
Summary: MonetDB5 SQL interface to the gsl library
Group: Applications/Databases
Requires: MonetDB5-server%{?_isa} = %{version}-%{release}

%description gsl-MonetDB5
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the interface to the GNU Scientific Library for
numerical analysis (gsl).

%files gsl-MonetDB5
%defattr(-,root,root)
%{_libdir}/monetdb5/autoload/*_gsl.mal
%{_libdir}/monetdb5/createdb/*_gsl.sql
%{_libdir}/monetdb5/gsl.mal
%{_libdir}/monetdb5/lib_gsl.so

%if %{?with_samtools:1}%{!?with_samtools:0}
%package bam-MonetDB5
Summary: MonetDB5 SQL interface to the bam library
Group: Applications/Databases
Requires: MonetDB5-server%{?_isa} = %{version}-%{release}

%description bam-MonetDB5
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the interface to load and query BAM (binary
version of Sequence Alignment/Map) data.

%files bam-MonetDB5
%defattr(-,root,root)
%{_libdir}/monetdb5/autoload/*_bam.mal
%{_libdir}/monetdb5/createdb/*_bam.sql
%{_libdir}/monetdb5/bam.mal
%{_libdir}/monetdb5/lib_bam.so
%endif

%if %{?with_rintegration:1}%{!?with_rintegration:0}
%package R
Summary: Integration of MonetDB and R, allowing use of R from within SQL
Group: Applications/Databases
Requires: MonetDB-SQL-server5%{?_isa} = %{version}-%{release}

%description R
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

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

%if %{?with_fits:1}%{!?with_fits:0}
%package cfitsio
Summary: MonetDB: Add on module that provides support for FITS files
Group: Applications/Databases
Requires: MonetDB-SQL-server5%{?_isa} = %{version}-%{release}

%description cfitsio
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

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
Obsoletes: MonetDB5-server-rdf
%if (0%{?fedora} >= 22)
Recommends: %{name}-SQL-server5%{?_isa} = %{version}-%{release}
%if %{bits} == 64
Recommends: MonetDB5-server-hugeint%{?_isa} = %{version}-%{release}
%endif
Suggests: %{name}-client%{?_isa} = %{version}-%{release}
%endif

%description -n MonetDB5-server
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the MonetDB server component.  You need this
package if you want to use the MonetDB database system.  If you want
to use the SQL frontend, you also need %{name}-SQL-server5.

%pre -n MonetDB5-server
getent group monetdb >/dev/null || groupadd -r monetdb
getent passwd monetdb >/dev/null || \
useradd -r -g monetdb -d %{_localstatedir}/MonetDB -s /sbin/nologin \
    -c "MonetDB Server" monetdb
exit 0

%post -n MonetDB5-server
# move database from old location to new location
if [ -d %{_localstatedir}/MonetDB5/dbfarm -a ! %{_localstatedir}/MonetDB5/dbfarm -ef %{_localstatedir}/monetdb5/dbfarm ]; then
	# old database exists and is different from new
	if [ $(find %{_localstatedir}/monetdb5 -print | wc -l) -le 2 ]; then
		# new database is still empty
		rmdir %{_localstatedir}/monetdb5/dbfarm
		rmdir %{_localstatedir}/monetdb5
		mv %{_localstatedir}/MonetDB5 %{_localstatedir}/monetdb5
	fi
fi

%files -n MonetDB5-server
%defattr(-,root,root)
%attr(750,monetdb,monetdb) %dir %{_localstatedir}/MonetDB
%attr(2770,monetdb,monetdb) %dir %{_localstatedir}/monetdb5
%attr(2770,monetdb,monetdb) %dir %{_localstatedir}/monetdb5/dbfarm
%{_bindir}/mserver5
%{_libdir}/libmonetdb5.so.*
%dir %{_libdir}/monetdb5
%dir %{_libdir}/monetdb5/autoload
%if %{?with_fits:1}%{!?with_fits:0}
%exclude %{_libdir}/monetdb5/fits.mal
%exclude %{_libdir}/monetdb5/autoload/*_fits.mal
%exclude %{_libdir}/monetdb5/createdb/*_fits.sql
%exclude %{_libdir}/monetdb5/lib_fits.so
%endif
%if %{?with_geos:1}%{!?with_geos:0}
%exclude %{_libdir}/monetdb5/geom.mal
%endif
%exclude %{_libdir}/monetdb5/gsl.mal
%if %{?with_rintegration:1}%{!?with_rintegration:0}
%exclude %{_libdir}/monetdb5/rapi.mal
%endif
%exclude %{_libdir}/monetdb5/sql*.mal
%if %{bits} == 64
%exclude %{_libdir}/monetdb5/*_hge.mal
%exclude %{_libdir}/monetdb5/autoload/*_hge.mal
%endif
%{_libdir}/monetdb5/*.mal
%if %{?with_geos:1}%{!?with_geos:0}
%exclude %{_libdir}/monetdb5/autoload/*_geom.mal
%endif
%exclude %{_libdir}/monetdb5/autoload/*_gsl.mal
%if %{?with_rintegration:1}%{!?with_rintegration:0}
%exclude %{_libdir}/monetdb5/autoload/*_rapi.mal
%endif
%exclude %{_libdir}/monetdb5/autoload/??_sql*.mal
%{_libdir}/monetdb5/autoload/*.mal
%if %{?with_geos:1}%{!?with_geos:0}
%exclude %{_libdir}/monetdb5/lib_geom.so
%endif
%exclude %{_libdir}/monetdb5/lib_gsl.so
%if %{?with_rintegration:1}%{!?with_rintegration:0}
%exclude %{_libdir}/monetdb5/lib_rapi.so
%endif
%if %{?with_samtools:1}%{!?with_samtools:0}
%exclude %{_libdir}/monetdb5/bam.mal
%exclude %{_libdir}/monetdb5/autoload/*_bam.mal
%exclude %{_libdir}/monetdb5/lib_bam.so
%endif
%exclude %{_libdir}/monetdb5/lib_sql.so
%{_libdir}/monetdb5/*.so
%doc %{_mandir}/man1/mserver5.1.gz
%dir %{_datadir}/doc/MonetDB
%docdir %{_datadir}/doc/MonetDB
%{_datadir}/doc/MonetDB/*

%if %{bits} == 64
%package -n MonetDB5-server-hugeint
Summary: MonetDB - 128-bit integer support for MonetDB5-server
Group: Application/Databases
Requires: MonetDB5-server%{?_isa}

%description -n MonetDB5-server-hugeint
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

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
accelerators.  It also has an SQL frontend.

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
Requires: MonetDB5-server%{?_isa} = %{version}-%{release}
%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} >= 7
# RHEL >= 7, and all current Fedora
Requires: %{_bindir}/systemd-tmpfiles
%endif
Obsoletes: MonetDB-SQL-devel
Obsoletes: %{name}-SQL
%if (0%{?fedora} >= 22)
%if %{bits} == 64
Recommends: %{name}-SQL-server5-hugeint%{?_isa} = %{version}-%{release}
%endif
Suggests: %{name}-client%{?_isa} = %{version}-%{release}
%endif

%description SQL-server5
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the SQL frontend for MonetDB.  If you want to
use SQL with MonetDB, you will need to install this package.

%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} >= 7
%post SQL-server5
systemd-tmpfiles --create %{_sysconfdir}/tmpfiles.d/monetdbd.conf
%endif

%files SQL-server5
%defattr(-,root,root)
%{_bindir}/monetdb
%{_bindir}/monetdbd
%dir %attr(775,monetdb,monetdb) %{_localstatedir}/log/monetdb
%if %{?rhel:0}%{!?rhel:1} || 0%{?rhel} >= 7
# RHEL >= 7, and all current Fedora
%{_sysconfdir}/tmpfiles.d/monetdbd.conf
%else
# RedHat Enterprise Linux < 7
%dir %attr(775,monetdb,monetdb) %{_localstatedir}/run/monetdb
%exclude %{_sysconfdir}/tmpfiles.d/monetdbd.conf
%endif
%config(noreplace) %{_localstatedir}/monetdb5/dbfarm/.merovingian_properties
%{_libdir}/monetdb5/autoload/??_sql.mal
%{_libdir}/monetdb5/lib_sql.so
%{_libdir}/monetdb5/*.sql
%dir %{_libdir}/monetdb5/createdb
%if %{?with_geos:1}%{!?with_geos:0}
%exclude %{_libdir}/monetdb5/createdb/*_geom.sql
%endif
%exclude %{_libdir}/monetdb5/createdb/*_gsl.sql
%if %{?with_samtools:1}%{!?with_samtools:0}
%exclude %{_libdir}/monetdb5/createdb/*_bam.sql
%endif
%{_libdir}/monetdb5/createdb/*.sql
%{_libdir}/monetdb5/sql*.mal
%if %{bits} == 64
%exclude %{_libdir}/monetdb5/createdb/*_hge.sql
%exclude %{_libdir}/monetdb5/sql*_hge.mal
%endif
%doc %{_mandir}/man1/monetdb.1.gz
%doc %{_mandir}/man1/monetdbd.1.gz
%dir %{_datadir}/doc/MonetDB-SQL
%docdir %{_datadir}/doc/MonetDB-SQL
%{_datadir}/doc/MonetDB-SQL/*

%if %{bits} == 64
%package SQL-server5-hugeint
Summary: MonetDB5 128 bit integer (hugeint) support for SQL
Group: Applications/Databases
Requires: MonetDB5-server-hugeint%{?_isa} = %{version}-%{release}
Requires: MonetDB-SQL-server5%{?_isa} = %{version}-%{release}

%description SQL-server5-hugeint
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package provides HUGEINT (128-bit integer) support for the SQL
frontend of MonetDB.

%files SQL-server5-hugeint
%defattr(-,root,root)
%{_libdir}/monetdb5/autoload/??_sql_hge.mal
%{_libdir}/monetdb5/createdb/*_hge.sql
%{_libdir}/monetdb5/sql*_hge.mal
%endif

%package -n python-monetdb
Summary: Native MonetDB client Python API
Group: Applications/Databases
Requires: python
BuildArch: noarch
Obsoletes: MonetDB-client-python

%description -n python-monetdb
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the files needed to use MonetDB from a Python
program.  This package is for Python version 2.  If you want to use
Python version 3, you need %{name}-python3-monetdb.

%files -n python-monetdb
%defattr(-,root,root)
%dir %{python_sitelib}/monetdb
%{python_sitelib}/monetdb/*
%{python_sitelib}/python_monetdb-*.egg-info
%doc clients/python2/README.rst

%if %{?rhel:0}%{!?rhel:1}
%package -n python3-monetdb
Summary: Native MonetDB client Python3 API
Group: Applications/Databases
Requires: python3
BuildArch: noarch

%description -n python3-monetdb
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the files needed to use MonetDB from a Python3
program.  This package is for Python version 3.  If you want to use
Python version 2, you need %{name}-python-monetdb.

%files -n python3-monetdb
%defattr(-,root,root)
%dir %{python3_sitelib}/monetdb
%{python3_sitelib}/monetdb/*
%{python3_sitelib}/python_monetdb-*.egg-info
%doc clients/python3/README.rst
%endif

%package testing
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Obsoletes: MonetDB-python

%description testing
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the programs and files needed for testing the
MonetDB packages.  You probably don't need this, unless you are a
developer.  If you do want to test, install %{name}-testing-python.

%files testing
%defattr(-,root,root)
%{_bindir}/Mdiff
%{_bindir}/MkillUsers
%{_bindir}/Mlog
%{_bindir}/Mtimeout

%package testing-python
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Requires: %{name}-testing = %{version}-%{release}
Requires: %{name}-client-tests = %{version}-%{release}
Requires: python
BuildArch: noarch

%description testing-python
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the Python programs and files needed for testing
the MonetDB packages.  You probably don't need this, unless you are a
developer, but if you do want to test, this is the package you need.

%files testing-python
%defattr(-,root,root)
# at least F12 doesn't produce these
# %exclude %{_bindir}/*.pyc
# %exclude %{_bindir}/*.pyo
%{_bindir}/Mapprove.py
%{_bindir}/Mtest.py
%dir %{python_sitelib}/MonetDBtesting
%{python_sitelib}/MonetDBtesting/*

%prep
%setup -q

%build

%{configure} \
	--enable-assert=no \
	--enable-console=yes \
	--enable-debug=no \
	--enable-developer=no \
	--enable-fits=%{?with_fits:yes}%{!?with_fits:no} \
	--enable-gdk=yes \
	--enable-geom=%{?with_geos:yes}%{!?with_geos:no} \
	--enable-gsl=yes \
	--enable-instrument=no \
	--enable-jdbc=no \
	--enable-merocontrol=no \
	--enable-microhttpd=no \
	--enable-monetdb5=yes \
	--enable-odbc=yes \
	--enable-oid32=%{?oid32:yes}%{!?oid32:no} \
	--enable-optimize=yes \
	--enable-profile=no \
	--enable-rintegration=%{?with_rintegration:yes}%{!?with_rintegration:no} \
	--enable-sql=yes \
	--enable-strict=no \
	--enable-testing=yes \
	--with-ant=no \
	--with-bz2=yes \
	--with-geos=%{?with_geos:yes}%{!?with_geos:no} \
	--with-java=no \
	--with-perl=yes \
	--with-perl-libdir=lib/perl5 \
	--with-pthread=yes \
	--with-python2=yes \
	--with-python3=%{?rhel:no}%{!?rhel:yes} \
	--with-readline=yes \
	--with-rubygem=%{?rhel:no}%{!?rhel:yes} \
	--with-rubygem-dir=%{?rhel:no}%{!?rhel:"%{gem_dir}"} \
	--with-samtools=%{?with_samtools:yes}%{!?with_samtools:no} \
	--with-sphinxclient=no \
	--with-unixodbc=yes \
	--with-valgrind=no \
	%{?comp_cc:CC="%{comp_cc}"}

make %{?_smp_mflags}

%install
%make_install

mkdir -p %{buildroot}%{_localstatedir}/MonetDB
mkdir -p %{buildroot}%{_localstatedir}/monetdb5/dbfarm
mkdir -p %{buildroot}%{_localstatedir}/log/monetdb
mkdir -p %{buildroot}%{_localstatedir}/run/monetdb
mkdir -p %{buildroot}%{perl_vendorlib}
if [ ! %{buildroot}%{_prefix}/lib/perl5 -ef %{buildroot}%{perl_vendorlib} ]; then
    mv %{buildroot}%{_prefix}/lib/perl5/* %{buildroot}%{perl_vendorlib}
fi

# remove unwanted stuff
# .la files
rm -f %{buildroot}%{_libdir}/*.la
rm -f %{buildroot}%{_libdir}/monetdb5/*.la
# internal development stuff
rm -f %{buildroot}%{_bindir}/Maddlog

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%changelog
* Fri Oct 30 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.11-20151030
- Rebuilt.
- BZ#3828: Schema corruption after several ALTER TABLE statements and
  server restart
- BZ#3839: msqldump generates incorrect syntax ON UPDATE (null)

* Mon Oct 26 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.9-20151026
- Rebuilt.
- BZ#3816: Server crashes when trying to convert timestamp to str with
  incorrect format
- BZ#3823: JDBC Connection to a schema - setSchema() error
- BZ#3827: Certains comparisons between UUID produce a MAL error
- BZ#3829: Certains simple WHERE clause cause MonetDB to segfault
  without explanation
- BZ#3830: Coalesce typing inconsistencies
- BZ#3833: NULL literals refused at many places
- BZ#3834: Date comparison returns incorrect results

* Tue Oct 20 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.7-20151020
- Rebuilt.
- BZ#3789: Query on large string table fails on HEAPextend
- BZ#3794: table function sys.rejects() and view sys.rejects() are listed
  are metadata objects but give an (incorrect) error when they are queried
- BZ#3797: COPY INTO with incorrect number columns
- BZ#3798: SELECT query with INTERSECT causes assertion failure
- BZ#3800: LIKE is broken for many patterns
- BZ#3802: Disk space never freed: a logical ref is keeped on a deleted
  BATs
- BZ#3803: SQL query parser fails to parse valid SELECT query with a
  CASE .. END in it. It fails with parser error: identifier 'x' ambiguous
- BZ#3804: `monetdb status` command crashes under certain conditions
- BZ#3809: Inefficient plan is generated for queries with many (>= 24)
  joined tables which take a long time or an HEAPalloc error. I get Error:
  GDK reported error. HEAPalloc: Insufficient space for HEAP of 400000000
  bytes.
- BZ#3810: Fix statistics gathering
- BZ#3811: NOT LIKE not working if the operand doesn't contains wildcards.
- BZ#3813: COPY INTO fails on perfectly clean CSV file
- BZ#3814: Server crash when using bitwise NOT operation in SQL query
- BZ#3818: Crash when performing UNION/GROUP BY over tables with
  different columns
- BZ#3819: order of tables in FROM-clause has negative impact on generated
  plan (using crossproducts instead of joins)
- BZ#3820: mclient accepts table with repeated constraint which causes
  crash on insert
- BZ#3821: Unexpected error when using a number instead of a boolean
- BZ#3822: Yet another LIKE operator issue
- BZ#3825: MonetDB not cleaning intermediate results which leads to
  filling up disk space and ultimately server crash

* Sun Aug 30 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.7-20151020
- clients: In the SQL formatter of mclient (the default) we now properly align
  East Asian wide characters.

* Mon Aug 24 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.5-20150824
- Rebuilt.
- BZ#3730: SAMPLE function not sampling randomly

* Tue Aug 18 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.3-20150818
- Rebuilt.
- BZ#3361: constants as MAL function parameters prevent intermediate reuse
- BZ#3440: Sequence type errors
- BZ#3449: mserver crash on start - Freebsd 10 amd64
- BZ#3496: autocompletion table names does not work correctly
- BZ#3758: "COPY INTO ..." doesn't work, if executing from 2 processes
  concurrently.
- BZ#3763: JDBC PreparedStatement for a table with 14 Foreign Keys
  crashing the Database
- BZ#3783: Behavioural change in Jul2015 for 'timestamp minus timestamp'
- BZ#3784: Assertion failed: (bn->batCapacity >= cnt), function
  BAT_scanselect, file gdk_select.c, line 1008.
- BZ#3785: sum(interval) causes overflow in conversion to bte
- BZ#3786: ResultSet.close() never sends Xclose to free resources
- BZ#3787: "b and g must be aligned" from complex group/union query
- BZ#3791: HEAPextend: failed to extend to 2420077101056

* Tue Aug 18 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.3-20150818
- sql: Differences between time, timestamp, and date values now return properly
  typed interval types (second or month intervals) instead of integers.

* Fri Aug 07 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.1-20150807
- Rebuilt.
- BZ#3364: Cannot set role back to a user's default role
- BZ#3365: Unable to grant object privileges while having a non-default
  current_role
- BZ#3476: Cannot revoke object access
- BZ#3556: when 2 multiplexed functions in MAL plan, only one is mapped
  correctly to bat<mod>.function primitive
- BZ#3564: Request: add support for postgresql specific scalar function:
  split_part(string text, delimiter text, field int)
- BZ#3625: SIGSEGV because mat array can overrun in opt_mergetable.c
- BZ#3627: SQRT in CASE does not work as of Oct2014
- BZ#3654: configure --enable-fits requires extra commands after creating
  a database instance
- BZ#3673: mclient 'expanded' row formatter
- BZ#3674: Obfuscate event tracing
- BZ#3679: No error is given when incorrect timezone value is specified
  for a timetz column
- BZ#3686: Wrong associativity of multiply/divide
- BZ#3702: Filter function not found if created in a user schema
- BZ#3708: wrong scoping for cross-schema view references
- BZ#3716: alter table my_merge_table drop table t1; crashes mserver5
  with Segmentation fault
- BZ#3724: Wrong size calculation in BATsubjoin
- BZ#3732: memory leak (of InstrRecord) in opt_mergetable
- BZ#3733: "(TRUE OR <Exp>) AND <Exp>" is evaluated incorrectly
- BZ#3735: python connection with unix_socket
- BZ#3736: crash if mclient disconnects abruptly during a query
- BZ#3738: Database inconsistency when using savepoint
- BZ#3739: CASE statements do not handle NULLs in the IN () operator
  properly
- BZ#3740: select epoch(now()); types timestamptz(7,0) and bigint(64,0)
  are not equal
- BZ#3742: Division By Zero
- BZ#3744: cast to int gives different results for decimal than double
- BZ#3747: joins fail in the presence of nulls
- BZ#3748: Missing META-INF/services/java.sql.Driver in JDBC package
- BZ#3753: Hang on json field parsing
- BZ#3754: select from a REMOTE TABLE referring local table crashes
  mserver5
- BZ#3756: column type conversion sticks to subsequent queries
- BZ#3759: select data from "sys"."rejects" returns unexpected error and
  when next select data from "sys"."sessions" causes an assertion failure
  in mal_interpreter.c:646.
- BZ#3760: SQL parser has problem with (position of) a scalar subquery
  in a SELECT-list
- BZ#3761: SQL executor has problem with (position of) a subquery in a
  SELECT-list. Inconsistent behavior.
- BZ#3764: DROPping multiple users causes a crash
- BZ#3765: Re-granting a revoked privilege does not work
- BZ#3766: VIEW not visible if created under a different schema
- BZ#3767: CREATE TEMP TABLE using "LIKE" incorrectly handled
- BZ#3769: SIGSEGV when combining a cast/column alias with a UNION
  ALL view
- BZ#3770: combined conditions on declared table in User Defined Function
  definition crashes monetdb
- BZ#3771: Owner of the schema loses rights if assumes the monetdb role.
- BZ#3772: Any user can grant a role.
- BZ#3773: quantile(col, 0) and quantile(col, 1) fail
- BZ#3774: mclient is unaware of merge tables and remote tables
- BZ#3775: COPY INTO: Backslash preceding field separator kills import
- BZ#3778: Crash on remote table schema mismatch
- BZ#3779: server crashes on MAX() on SELECT DISTINCT something combo

* Thu Aug  6 2015 Martin van Dinther <martin.van.dinther@monetdbsolutions.com> - 11.21.1-20150807
- java: Improved JDBC driver to not throw NullPointerException anymore
  when calling isNullable() or getPrecision() or getScale() or
  getColumnDisplaySize() or getSchemaName() or getTableName() or
  getColumnClassName() on a ResultSetMetaData object.

* Tue Jul 28 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.1-20150807
- sql: Added support for 128-bit integers (called HUGEINT) on platforms that
  support this.

* Thu Jul 16 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.1-20150807
- java: We now compile the Java classes using the latest Java 1.8 version, and
  we tell it to compile for Java 1.7.

* Wed Jun  3 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.1-20150807
- buildtools: Upgraded the license to the Mozilla Public License Version 2.0.

* Wed Jun  3 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.1-20150807
- clients: Added a new output format to mclient: --format=expanded (or -fx).
  In this format, column values are shown in full and below each other.

* Wed Jun  3 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.1-20150807
- sql: Removed support for the mseed library.

* Wed Jun  3 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.1-20150807
- sql: Removed support for RDF.
- sql: Removed DataCell.  It was experimental code that was never enabled.

* Wed Jun  3 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.1-20150807
- gdk: BUNtvar and BUNhvar macros no longer work for TYPE_void columns.

* Wed Jun  3 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.1-20150807
- gdk: Changed interfaces of a lot of GDK-level functions.  When they modify a
  BAT, don't return the same BAT or NULL, but instead return GDK_SUCCEED
  or GDK_FAIL.

* Wed Jun  3 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.1-20150807
- monetdb5: Implemented batcalc.min and batcalc.max.  Made calc.min and calc.max
  generic so that no other implementations are needed.

* Wed Jun  3 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.1-20150807
- monetdb5: Removed function batcalc.ifthen.

* Wed Jun  3 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.21.1-20150807
- gdk: Changed a bunch of hash-related functions to work on the tail column.
  The functions that have been changed to work on the tail column are:
  BAThash, BATprepareHash, HASHgonebad, HASHins, and HASHremove.

* Wed Jun 03 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.19.15-20150603
- Rebuilt.
- BZ#3707: var() possibly not working in debug builds
- BZ#3720: Incorrect results on joining with same table
- BZ#3725: LEFT JOIN bug with CONST value
- BZ#3731: left shift for IP addresses not available to non-system users

* Tue May 19 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.19.13-20150519
- Rebuilt.
- BZ#3712: Concurrency issue on querying the SQL catalog
- BZ#3713: Long startup cost for simple session
- BZ#3715: Crash with two ALTER TABLE statements in a transaction
- BZ#3718: Adding and dropping a non existing tablename to/from a merge
  table is accepted without an error
- BZ#3719: Assertion failure in /MonetDB-11.19.11/gdk/gdk_bat.c:2841:
  BATassertHeadProps: Assertion `!b->H->key || cmp != 0' failed.
- BZ#3723: Assertion failure in rel_bin.c:2548: rel2bin_groupby: Assertion
  `0' failed.

* Thu Apr 23 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.19.11-20150423
- Rebuilt.
- BZ#3466: UPDATE statements fails with "GDKerror: MT_mremap() failed"
- BZ#3602: Surprising overload resolution of generate_series
- BZ#3613: SQL data dictionary contains columns names which are also
  special keywords. This causes unexpected/unneeded SQL query errors
- BZ#3645: Network address operators such as << and <<= do not work
- BZ#3647: missing BAT for a column leads to crash in gtr_update_delta
- BZ#3648: memory corruption on unclean connection shutdown with local
  temporary tables
- BZ#3650: Naming of persistent BATs is fragile
- BZ#3653: PREPARE crashes mserver if unbound variable is function
  parameter
- BZ#3655: SQL WHERE -1 in (-1) issue?
- BZ#3656: error message after calling fitsload()
- BZ#3660: Incorrect Results for Comparison Operators on inet Datatype
- BZ#3661: Ship debug symbols for pre-built binaries
- BZ#3662: UPDATE row with row value constructor crashes monetdb server
- BZ#3663: Incorrect result ROW_NUMBER in subquery
- BZ#3664: SQLstatementIntern missing parameter when using jsonstore
- BZ#3665: inter-session starvation issue, particularly affects sys.queue
- BZ#3666: casting text column to inet truncating text column and
  resulting inet for first occurrence only
- BZ#3667: insert of negative value for oid column aborts mserver5
  process with assertion failure
- BZ#3669: ALTER TABLE <tbl_nm> ADD CONSTRAINT <tbl_uc1> UNIQUE (col1,
  col2, col3) causes Assertion failure and abort
- BZ#3671: ODBC-Access on Windows 2012 does not work - E_FAIL
- BZ#3672: libbat_la-gdk_utils.o: relocation R_X86_64_PC32 against
  `MT_global_exit' can not be used when making a shared object
- BZ#3676: merovingian hangs trying to exit
- BZ#3677: Crash in BATgroup_internal (caused by 87379087770d?)
- BZ#3678: Ruby driver installation ignores prefix
- BZ#3680: Prepared statements fail on execution with message 'Symbol
  type not found'
- BZ#3684: Wrong query result set WHERE "IS NULL" or "NOT IN" clauses
  uses in combination with ORDER, LIMIT and OFFSET
- BZ#3687: 'bat.insert' undefined
- BZ#3688: Crash at exit (overrun THRerrorcount?)
- BZ#3689: No more connections accepted if a single client misbehaves
- BZ#3690: find_fk: Assertion `t && i' failed.
- BZ#3691: conversion of whitespaces string to double or float is accepted
  without an error during insert
- BZ#3693: algebra.join undefined (caused by non-existing variables in
  the plan)
- BZ#3696: Inconsistent behavior between dbl (SQL double) and flt (SQL
  real) data types and across platforms
- BZ#3697: mserver5[26946]: segfault at 0 ip 00007f3d0e1ab808 sp
  00007f3cefbfcad0 error 4 in lib_sql.so[7f3d0e180000+16c000]
- BZ#3699: segfault again! (during last week I found 3 segfault bugs
  already)
- BZ#3703: INSERT INTO a MERGE TABLE crashes mserver5
- BZ#3704: Unknown identifier from subquery
- BZ#3705: Assertion failure in rel_bin.c:2274: rel2bin_project: Assertion
  `0' failed.
- BZ#3706: Assertion failure in gdk_bat.c: BATassertHeadProps: Assertion
  `!b->H->sorted || cmp <= 0' failed.
- BZ#3709: "BATproject: does not match always" on abusive use of ALTER
  TABLE SET READ ONLY

* Tue Feb  3 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.19.11-20150423
- buildtools: We now also create debug packages for Debian and Ubuntu.

* Tue Jan 27 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.19.11-20150423
- gdk: Replaced the rangejoin implementation with one that uses imprints if
  it can.

* Fri Jan 23 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.19.9-20150123
- Rebuilt.
- BZ#3467: Field aliases with '#' character excise field names in
  result set.
- BZ#3605: relational query without result
- BZ#3619: Missing dll on MonetDB Start
- BZ#3622: Type resolution error
- BZ#3624: insert of incomplete or invalid ip address values in
  inet column is silently accepted but the values are not stored (they
  become/show nil)
- BZ#3626: casting a type without alias results in program contains errors
- BZ#3628: mclient and ODBC driver report 'type mismatch' when stddev_pop
  used in a select which returns 0 rows
- BZ#3629: IF THEN ELSEIF always evaluates the first test as true
- BZ#3630: segv on rel_order_by_column_exp
- BZ#3632: running make clean twice gives an error in clients/ruby/adapter
- BZ#3633: Wrong result for HAVING with floating-point constant
- BZ#3640: Missing implementation of scalar function: sql_sub(<date>,
  <month interval>)
- BZ#3641: SQL lexer fails to detect string end if it the last character
  is U+FEFF ZERO WIDTH NO-BREAK SPACE
- BZ#3642: Combined WHERE conditions less-than plus equals-to produce
  incorrect results
- BZ#3643: Missing implementations of scalar function: sql_sub(<timetz>,
  arg2)
- BZ#3644: COPY INTO fails to import "inet" data type when value has
  prefix length in CIDR notation
- BZ#3646: ORDER BY clause does not produce proper results on 'inet'
  datatype
- BZ#3649: recycler crashes with concurrent transactions

* Mon Jan 19 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.19.9-20150123
- sql: Fixed a typo in a column name of the sys.tablestoragemodel view
  (auxillary changed to auxiliary).

* Tue Jan 13 2015 Sjoerd Mullender <sjoerd@acm.org> - 11.19.9-20150123
- clients: Changes to the Perl interface, thanks to Stefan O'Rear:
  1. removes "use sigtrap", because this has global effects and should
  not be used by modules, only by the application.
  2. allows Perl 5.8.1+ Unicode strings to be passed to quote() and
  included in statements (UTF-8 encoded, as expected by Monet's str
  module)
  3. quote and unquote now use the same quoting rules as the MonetDB
  server, allowing for all characters except NUL to be round-tripped
  4. several character loops have been reimplemented in regex for much
  greater performance
  5. micro-optimizations to the result fetch loop
  6. block boundaries are preserved in piggyback data so that Xclose is
  not appended or prepended to a SQL command
  7. diagnostic messages #foo before a result header are ignored, this
  is necessary to use recycler_pipe
  8. fail quickly and loudly if we receive a continuation prompt (or any
  other response that starts with a non-ASCII character)
  9. header lines must start with %, not merely contain %, fixing a bug
  when querying a table where string values contain %
  10. after closing a large resultset, account for the fact that a reply
  will come and do not lose sync
  11. allow a MAPI_TRACE environment variable to dump wire protocol
  frames to standard output
  12. fixes maximum MAPI block size to match the server limit of 16k.
  previously would crash on blocks larger than 16k

* Fri Nov 21 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.7-20141121
- Rebuilt.

* Thu Nov 20 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.5-20141120
- Rebuilt.
- BZ#3580: cosmetic change (append newline)
- BZ#3609: Incorrect use of generate_series
- BZ#3611: quantile() and median() commands crash when used 2x on the
  same variable on a null table
- BZ#3612: assertion failure when deleting rows from table to which a
  FK constraint is defined
- BZ#3620: ORDER BY broken when using UNION ALL
- BZ#3621: Hexadecimal literal vs decimal literal

* Thu Nov 20 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.5-20141120
- gdk: Implemented a change to the way in which string bats are appended.
  We now try harder to limit the growth of the string heap.

* Thu Nov 20 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.5-20141120
- monetdb5: Fixed adding of 0 intervals to dates.

* Thu Nov 20 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.5-20141120
- sql: Fixed sys.queue() implementation to report on other queries being
  executed.

* Fri Nov 14 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.5-20141120
- sql: A number of bugs were fixed in the code to upgrade a database from
  previous releases.  This version should fix the upgrade of a database
  that had been upgraded to the Oct2014 release, but also properly
  upgrade directly from Jan2014 and Feb2013 releases.

* Fri Nov  7 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.5-20141120
- buildtools: The libraries included in the Windows installers have been upgraded.
  We now use libxml2-2.9.2, openssl-1.0.1j, pcre-8.36, and zlib-1.2.8.

* Wed Nov  5 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.5-20141120
- gdk: Fixed some problems with BATsample.  It was possible for BATsample to
  return a value that was just beyond the end of the sampled BAT.  Also,
  on some systems the range of the rand() function is rather limited
  (0..32767) and trying to get a sample larger than this range would
  result in an infinite loop.

* Tue Oct 28 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.3-20141028
- Rebuilt.

* Fri Oct 24 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.1-20141024
- Rebuilt.
- BZ#2618: Implement master slave scheme
- BZ#2945: evaluation of SQL "between SYMMETRIC" requires MAL iterator
  because there is no (bulk) MIN/MAX(a,b)
- BZ#3204: monetdb create: allow setting admin password during creation
- BZ#3390: Missing definition for pushSht in monetdb5/mal/mal_builder.h
- BZ#3402: We should have a C implementation of mal.multiplex.
- BZ#3422: Segmentation fault after large table insert
- BZ#3459: incomplete implementation of JDBC driver getNumericFunctions(),
  getStringFunctions(), getSystemFunctions(), getTimeDateFunctions(),
  getSQLKeywords() methods in MonetDatabaseMetaData.java
- BZ#3471: JDBC driver: incorrect output result of SQL query: SELECT 1 ;
- BZ#3474: bulk and scalar versions of mkey.rotate_xor_hash differ
- BZ#3484: COPY INTO on a file works fine on Linux/OSX, but not on Windows
- BZ#3488: Slow SQL execution for correlated scalar subquery
- BZ#3489: SQL query with ORDER BY does not order its result as requested
- BZ#3490: SQL query kills the mserver5 (Segmentation fault)
- BZ#3491: SQL query kills the mserver5 (Segmentation fault)
- BZ#3493: Test monetdb5/modules/mal/Tests/pqueue.mal fails since
  recent checkins
- BZ#3494: Tests monetdb5/modules/mal/Tests/pqueue[23].mal lack
  correct/expected/intended output
- BZ#3495: Test sql/test/centipede/Tests/olap.sql lacks
  correct/expected/intended output
- BZ#3497: monetdb start reports crash in merovingian.log
- BZ#3498: SQL throws TypeException if aggregations and limit statements
  are both present
- BZ#3502: Database was killed by signal SIGBUS
- BZ#3504: COPY INTO does not allow OFFSET without specifying amount
  of records
- BZ#3505: expression with <boolean> = NOT <boolean> returns a syntax
  error but NOT <boolean> = <boolean> not
- BZ#3506: conversion to varchar terminates mserver
- BZ#3508: conversion of string '0   ' to type smallint or integer fails
- BZ#3510: timestamp + month interval generates bogus MAL?
- BZ#3511: When having multiple selections combined with aliases not
  all of them seem to be evalauted.
- BZ#3512: auto-conversion of string to `sht` type no longer works
- BZ#3513: COPY BINARY INTO fails on 6gb file; works fine on 3gb
- BZ#3516: inserting '0' into a column of datatype numeric fails
- BZ#3518: UNION with subqueries
- BZ#3521: large results of function exp() are not automatically returned
  as double
- BZ#3522: SQL catalog table sys.columns lists columns for table ids
  which do not exist in sys.tables
- BZ#3523: Window function over union gives no result
- BZ#3524: wrong error on missing aggregation column
- BZ#3527: select distinct - order by - limit 2 results in one single
  result
- BZ#3528: segfault at mal_session.c:521
- BZ#3532: several geom tests crash after manifold changes
- BZ#3534: missing table name with invalid column in join using (and
  problems after resolving it)
- BZ#3536: program contains error with join using integer and smallint
- BZ#3542: gdk/gdk_bat.c:2904: BATassertHeadProps: Assertion
  `!b->H->revsorted || cmp >= 0' failed.
- BZ#3543: invalid behavior and incorrect data results for SQL data
  type: numeric(4,4)
- BZ#3544: sys.reuse() corrupts data
- BZ#3546: Division by zero in CASE statement that should avoid it
- BZ#3547: Empty query when selecting a field from a view made of
  UNION ALL
- BZ#3551: Wrong ticks in TRACE
- BZ#3552: incorrect data results for "WHERE int_col <> 0"
- BZ#3554: Issue with subselect and ORDER BY
- BZ#3555: Order of evaluation inside CASE WHEN
- BZ#3558: numeric values (as strings) are incorrectly parsed/converted
  and invalid strings are accepted without error
- BZ#3560: Error "BATproject: does not match always" with
  subselect/groupby/having
- BZ#3562: mserver5: gdk_bat.c:2855: BATassertHeadProps: Assertion
  `!b->H->revsorted || cmp >= 0' failed.
- BZ#3563: incorrect results for scalar function locate(in_str,
  search_str, occurrence)
- BZ#3565: Wrong/confusing error message when trying to add a FK to a
  TEMP TABLE
- BZ#3572: Table names with escaped double quotes are rejected
- BZ#3573: alter table alter_not_null_test alter test set NOT NULL;
  is accepted when test contains null. This used to be restricted but
  isn't anymore
- BZ#3575: segmentation fault in mserver5 process
- BZ#3576: Dropping default value definitions from a table does not work
  as expected
- BZ#3577: SIGSEGV in BATins_kdiff
- BZ#3579: segmentation fault in mserver5 process
- BZ#3581: mserver5: rel_bin.c:2504: rel2bin_groupby: Assertion `0'
  failed.
- BZ#3582: mserver5: sql_mem.c:48: sql_ref_dec: Assertion `r->refcnt >
  0' failed.
- BZ#3583: Possible buffer overflow in max(varchar)
- BZ#3585: Incorrect query terminates connection
- BZ#3586: mserver5: sql/storage/store.c:3610: sys_drop_func: Assertion
  `rid_func != oid_nil' failed.
- BZ#3592: SIGSEGV in MANIFOLDjob
- BZ#3593: delta_append_val: Assertion `!c || ((c)->S->count) ==
  bat->ibase' failed.
- BZ#3594: gdk/gdk_bat.c:2855: BATassertHeadProps: Assertion
  `!b->H->revsorted || cmp >= 0' failed.
- BZ#3595: Race/heap corruption on thread exit
- BZ#3596: gdk_bat.c:2861: BATassertHeadProps: Assertion `!b->H->nonil ||
  cmp != 0' failed.
- BZ#3597: SQL to MAL listing looses types
- BZ#3598: SQL bulk load should ignore leading/trailing spaces also with
  type decimal (as with integers & real/double)
- BZ#3599: Double-free of imprints
- BZ#3601: Trivial typo in debian/monetdb5-sql.init.d
- BZ#3603: "monetdb create -p" hangs monetdbd
- BZ#3604: Sys.queue ignored upon errors

* Mon Sep 15 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.1-20141024
- monetdb5: Removed algebra.materialize.

* Fri Aug 29 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.1-20141024
- monetdb5: Removed algebra.kunique and algebra.tunique.  They were subsumed by
  algebra.subunique.

* Tue Aug 26 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.1-20141024
- monetdb5: Remove algebra.antiuselect and algebra.thetauselect.  They were subsumed
  by algebra.subselect.

* Mon Aug 25 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.1-20141024
- monetdb5: Removed algebra.topN and its imlementation BATtopN.  The function was
  not used.
- monetdb5: Removed aggr.histogram and its implementation BAThistogram.  The
  function was not used, and did not produce output in the "headless"
  (i.e. dense-headed) format.  Histograms can be created as a by-product
  of group.subgroup.

* Fri Jul 25 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.1-20141024
- gdk: Added "multifarm" capability.  It is now possible to separate persistent
  and transient BATs into different directories (presumably on different
  disks).  This can be done by using the --dbextra option of mserver5
  (see the man page).

* Fri Jul 25 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.1-20141024
- buildtools: Jacqueline, the MonetDB/JAQL frontend, has been removed.  The frontend
  never grew beyond being experimental, and there is no interest anymore
  to maintain the code.

* Fri Jul 25 2014 Jennie Zhang <y.zhang@cwi.nl> - 11.19.1-20141024
- sql: Added PostgreSQL compatible string TRIM, LTRIM, RTRIM, LPAD and RPAD
  functions

* Fri Jul 25 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.1-20141024
- mapilib: Changed mapi_timeout argument from seconds to milliseconds.

* Fri Jul 25 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.1-20141024
- stream: Changed mnstr_settimeout function so that the specified timeout is now
  in milliseconds (used to be seconds), and that it also needs an extra
  argument specifying a callback function (no arguments, int result)
  that should return TRUE if the timeout should cause the function to
  abort or continue what it was doing.

* Fri Jul 25 2014 Fabian Groffen <fabian@monetdb.org> - 11.19.1-20141024
- merovingian: monetdb create: add -p flag to set monetdb user password on creation,
  and therefore allow creating the database in unlocked state

* Fri Jul 25 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.19.1-20141024
- sql: Stop support for upgrading directly from a database created with a
  server from the Oct2012 release or older.  You can upgrade via the
  Feb2013 or Jan2014 release.

* Fri Jul 25 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.21-20140725
- Rebuilt.
- BZ#3519: Uppercase TRUE/FALSE strings cannot be converted to boolean
  values

* Tue Jul 22 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.19-20140722
- Rebuilt.
- BZ#3487: dead link to "Professional services"
- BZ#3500: MonetDB driver wants an empty string for SQLTables and
  SQLColumns API calls, where other drivers expect NULL
- BZ#3514: mserver5 crash due (assertion failure in gdk_select.c)
- BZ#3515: mserver5 crash due (assertion failure in gdk_bat.c)

* Tue Jun  3 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.19-20140722
- buildtools: Fix configure to continue without Python if the python binary is
  too old.  This instead of always aborting configure if python happens
  to be too old.

* Wed May 14 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.17-20140514
- Rebuilt.
- BZ#3482: Crossproduct error

* Thu May 08 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.15-20140508
- Rebuilt.
- BZ#3424: numeric values at the front of strings determines whether
  CAST works successfully
- BZ#3439: Python driver drops milliseconds from timestamps
- BZ#3446: SET READ ONLY forgets previous changes
- BZ#3455: String columns unusable from 64-bit .NET via ODBC
- BZ#3456: Insert fails
- BZ#3457: When kernel of remote client crashes, the connection remains
  established on server side
- BZ#3458: mserver5 crash on SQL: SELECT COUNT(*) FROM SYS.TABLES HAVING
  COUNT(*) > 0
- BZ#3461: mserver5 crash on SQL: SELECT * FROM SYS.ARGS WHERE FUNC_ID
  NOT IN (SELECT ID FROM SYS.FUNCTIONS) OR FUNC_ID NOT IN (SELECT *
  FROM SYS.FUNCTIONS)
- BZ#3462: Invalid SQL (IN with subquery which returns multiple columns)
  is accepted
- BZ#3463: Crash on SELECT with SERIAL aggregation and GROUP BY column
  alias's
- BZ#3468: Local temporary table persists across sessions
- BZ#3469: Absolute network paths considered invalid for COPY INTO
  ... FROM statement.
- BZ#3473: Various memory leaks in SQL compilation
- BZ#3477: ODBC driver raises "unexpected end of input" for prepared
  string parameter from .NET application
- BZ#3481: Cannot run multiple COPY INTO statements in one 's'-command

* Wed Apr 30 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.15-20140508
- buildtools: Lots of minor fixes were made for potential defects found by Coverity
  Scan.

* Tue Apr  1 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.15-20140508
- clients: ODBC: Implemented {call procedure-name(...)} escape.  The version
  {?=call ...} is not implemented.

* Mon Mar 24 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.15-20140508
- buildtools: On Windows we now build the geom module against version 3.4.2 of the
  geos library.

* Thu Mar 06 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.13-20140306
- Rebuilt.
- BZ#3452: ODBC driver build fails on Mac OS X due to a conflicting
  types for the SQLColAttribute with the unixODBC library

* Mon Mar 03 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.11-20140303
- Rebuilt.
- BZ#3442: COPY INTO ... LOCKED reports incorrect count
- BZ#3443: DROP INDEX crashes server with BATsubselect: invalid argument:
  b must have a dense head
- BZ#3444: AND after ON () of LEFT OUTER JOIN with certain expressions
  will cause crash

* Fri Feb 28 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.11-20140303
- buildtools: Configure now enables the SQL front end by default.

* Sun Feb 16 2014 Fabian Groffen <fabian@monetdb.org> - 11.17.11-20140303
- merovingian: monetdb destroy -f now also works on running databases

* Thu Feb 13 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.9-20140213
- Rebuilt.
- BZ#3435: INDEX prevents JOIN from discovering matches

* Fri Feb 07 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.7-20140207
- Rebuilt.
- BZ#3436: COPY INTO from file containing leading Byte Order Mark (BOM)
  causes corruption

* Thu Feb 06 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.5-20140206
- Rebuilt.
- BZ#3420: Database does not start after upgrade
- BZ#3425: Temporal extraction glitches
- BZ#3427: Consistent use of current_timestamp and now()
- BZ#3428: Aggregation over two columns is broken
- BZ#3429: SAMPLE on JOIN result crashes server
- BZ#3430: Wrong temporary handling
- BZ#3431: SQLGetInfo returns incorrect value for SQL_FN_NUM_TRUNCATE
- BZ#3432: MonetDB SQL syntax incompatible with SQL-92 <delimited
  identifier> syntax

* Sat Jan 25 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.3-20140125
- Rebuilt.
- BZ#3418: Segmentation fault on a query from table expression
- BZ#3419: Database does not start after upgrade
- BZ#3423: Group by alias with distinct count doesn't work

* Tue Jan 14 2014 Sjoerd Mullender <sjoerd@acm.org> - 11.17.1-20140114
- Rebuilt.
- BZ#3040: Wrong NULL behavior in EXCEPT and INTERSECT
- BZ#3092: ODBC client doesn't support scalar function escape
- BZ#3198: SIGSEGV insert_string_bat (b=0x7fffe419d0a0, n=0x7fffc4006010,
  append=0) at gdk_batop.c:196
- BZ#3210: Unexpected concurrency conflict when inserting to 2 tables
  simultaneously and querying one of them
- BZ#3273: Add support to Python DBAPI package for timetz, inet and
  url types
- BZ#3285: no such table 'queryHistory'
- BZ#3298: GDKmmap messages and monetdb start db takes very long
- BZ#3354: Introduce query time-out
- BZ#3371: (i)like generates batloop instead of algebra.likesubselect
- BZ#3372: Large group by queries never complete - server at 100%
  cpu(all cores) until MonetDB stopped
- BZ#3383: Bad performance with DISTINCT GROUP BY
- BZ#3391: Bad performance with GROUP BY and FK with out aggregate
  function
- BZ#3393: "COPY .. INTO ..." - escape of string quotes
- BZ#3399: server crashed on simple (malformed) query
- BZ#3401: inconsistent/strange handling of invalid dates
  (e.g. 2013-02-29) in where clause
- BZ#3403: NOT NULL constraint can't be applied after deleting rows with
  null values
- BZ#3404: Assertion `h->storage == STORE_MMAP' failed.
- BZ#3408: nested concat query crashed server
- BZ#3411: (disguised) BETWEEN clause not recognised. Hence no rangejoin.
- BZ#3412: Boolean expressions in WHERE clause, result in incorrect
  resulsts
- BZ#3417: Nested Common Table Expressions Crash

* Tue Dec 10 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.17.1-20140114
- buildtools: Created packages for RPM based systems and Debian/Ubunty containing
  the MonetDB interface to the GNU Scientific Library (gsl).

* Wed Nov 20 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.17.1-20140114
- gdk: Removed some unused fields in the atomDesc structure.  This change
  requires a complete recompilation of the whole suite.

* Wed Nov 20 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.17.1-20140114
- clients: ODBC: Implemented {fn scalar()} and {interval ...} escapes.

* Wed Nov 20 2013 Gijs Molenaar <g.j.molenaar@uva.nl> - 11.17.1-20140114
- python2: Changed defaults for connecting (defaults to unix socket now).
- python2: Unix sockets partially working for control protocol.
- python2: Add support for unix socket.

* Wed Nov 20 2013 Gijs Molenaar <g.j.molenaar@uva.nl> - 11.17.1-20140114
- python3: Changed defaults for connecting (defaults to unix socket now).
- python3: Unix sockets partially working for control protocol.
- python3: Add support for unix socket.

* Wed Nov 20 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.17.1-20140114
- buildtools: We no longer install the .la files in our Fedora/Debian/Ubuntu packages.

* Wed Nov 20 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.17.1-20140114
- gdk: Replaced the mutex implementation for both GNU C and Visual Studio with
  a home-grown implementation that uses atomic instructions (__sync_*()
  in gcc, _Interlocked*() in VS).

* Wed Nov 20 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.19-20131120
- Rebuilt.
- BZ#3243: Segmentation fault (possible data corruption) after clean
  shutdown
- BZ#3258: Scheduling issues
- BZ#3368: BAT sortedness info ignored on ORDER BY and TOPN
- BZ#3374: UNIQUE constraint does not set tkey property on the
  corresponding BAT
- BZ#3382: Response to PREPARE emtpy if query contains a LIMIT
- BZ#3385: Simple query fails with 'identifier not found'
- BZ#3387: mclient does not properly double quote schema names when
  using autofill tab
- BZ#3388: case statement in "order by" clause doesn't work when used
  together with "group by"
- BZ#3389: median function with "group by"  - SIGSEGV
- BZ#3392: ODBC datatype conversion for INTEGER not working properly
- BZ#3394: "Cannot find column type" error in temporary tables in
  functions
- BZ#3395: error occurred during a query: "'CASE WHEN" sentence
- BZ#3396: Improper UDF expansion
- BZ#3397: Error in ODBC-Driver when using Prepared Statements
- BZ#3398: Cannot stop monetdbd after erroneously starting an
  uninitialized dbfarm

* Tue Nov 19 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.19-20131120
- clients: mclient: Fixed a bug where the -H option only worked if the readline
  history file already existed.  Now we properly create and use the
  history file.

* Tue Nov 19 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.19-20131120
- gdk: Stopped using the deprecated sbrk() system call.
- gdk: Fixed a problem when reverse sorting a sorted column.
- gdk: Fixed bugs that deal with problems that could possibly occur when
  transactions are aborted and the server is restarted.  See bug #3243.
- gdk: A bug was fixed in the handling of grouped aggregates when all values
  in a group are null.  See bug #3388.

* Tue Nov 19 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.19-20131120
- monetdb5: Fixed a possible buffer overflow in the COPY INTO code.
- monetdb5: Fixed a problem that when the server is using all available threads
  for a query, it was not possible to attach another client and have
  it execute even the smallest query.  This is fixed by creating extra
  threads for each client entering the fray at the cost of having more
  threads that execute queries.  But at least there is guaranteed progress
  for all clients (modulo the operating system scheduler).  See bug #3258.

* Tue Nov 19 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.19-20131120
- sql: Fixed a bug where the server at some point stopped saving compiled
  queries in the SQL query cache.

* Fri Nov 15 2013 Fabian Groffen <fabian@monetdb.org> - 11.15.19-20131120
- merovingian: monetdbd(1) now refuses to startup if it cannot read the properties
  from the dbfarm, bug #3398

* Wed Nov  6 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.19-20131120
- clients: ODBC: Fixed interpretation SQL_C_SLONG/SQL_C_ULONG/SQL_C_LONG to
  refer to a 32 bit integer always (i.e. "int" on 64 bit architectures
  despite the name and the Microsoft documentation).  This seems to be
  the consensus.
- clients: ODBC: Fixed transaction level: MonetDB only supports the highest level
  (SQL_TXN_SERIALIZABLE), so setting the transaction level can be accepted
  and ignored.

* Tue Oct 08 2013 Hannes Muehleisen <hannes@cwi.nl> - 11.15.17-20131008
- Rebuilt.
- BZ#3323: Heapcache bugs/performance issues
- BZ#3331: SAMPLE will return same result every time.
- BZ#3356: DatabaseMetaData.getColumns() doesn't work correctly when
  using index-based getters
- BZ#3367: Fully qualified order by column gives "relational query
  without result"
- BZ#3368: BAT sortedness info ignored on ORDER BY and TOPN
- BZ#3370: SQL environment settings are updated even when the set
  statement fails
- BZ#3373: Setting table to read-only (Bug 3362) fails on big-endian
- BZ#3375: LIKE join: BATfetchjoin does not hit always
- BZ#3376: COPY INTO fails with HEAPextend: failed to extend: MT_mremap()
  failed
- BZ#3377: Query interfering with next query in same transaction,
  after SP4
- BZ#3380: Python DBAPI driver throws exception when fetching EXPLAIN
  results
- BZ#3381: Windows ODBC driver hangs or crashes on simple queries

* Mon Oct  7 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.17-20131008
- java: Rearranged order of returned columns of certain metadata functions to
  comply with the JDBC documentation.  See bug 3356.

* Fri Oct  4 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.17-20131008
- clients: ODBC: Implemented retrieving variable-length data in parts with
  SQLGetData.  See bug 3381.

* Mon Sep 30 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.17-20131008
- gdk: Removed the heap cache.  Since the fix for bug 3323 which made that
  the cache was actually getting used, bug 3376 made clear that it didn't
  work very well.  In addition, on Linux at least, the heap cache slows
  things down.

* Wed Sep 25 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.15-20130925
- Rebuilt.

* Fri Sep 20 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.13-20130920
- Rebuilt.

* Tue Jul 30 2013 Fabian Groffen <fabian@monetdb.org> - 11.15.13-20130920
- merovingian: monetdb now no longer compresses output when not connected to a terminal,
  bug #3292

* Wed Jun 19 2013 Hannes Muehleisen <hannes@cwi.nl> - 11.15.11-20130619
- Rebuilt.

* Wed Jun 19 2013 Hannes Muehleisen <hannes@cwi.nl> - 11.15.9-20130619
- Rebuilt.

* Sun Jun  9 2013 Fabian Groffen <fabian@monetdb.org> - 11.15.9-20130619
- java: Further improved setBigDecimal() method, based on patch by Ben Reilly
  in bug #3290

* Thu May 23 2013 Fabian Groffen <fabian@monetdb.org> - 11.15.9-20130619
- java: Fixed bug where PreparedStatement.setBigDecimal() wouldn't format its
  input well enough for the server causing odd errors.
- java: Allow PreparedStatement.setXXX() methods to be called with null
  arguments, bug #3288

* Tue May  7 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.9-20130619
- gdk: System calls to flush files to disks were added.  This may cause
  some slowdown, but it should provide better durability, especially
  in the face of power failures.

* Fri Apr 26 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.7-20130426
- Rebuilt.

* Sat Apr 13 2013 Niels Nes <niels@cwi.nl> - 11.15.7-20130426
- sql: Added TEMPORARY to the non-reserved keywords, ie no need for double
  quotes when used as identifier.

* Fri Apr 12 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.5-20130412
- Rebuilt.

* Thu Apr 11 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.5-20130412
- java: The pre-compiled .jar files are now created using Java 7 instead of
  Java 6.

* Mon Apr  8 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.5-20130412
- gdk: Fixed a bug in case the candidate list is dense and completely
  outside the range of the bat being worked upon.

* Tue Mar 12 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.5-20130412
- monetdb5: Fixed argument parsing of mapi.reconnect() with 5 arguments.

* Wed Feb 27 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.3-20130227
- Rebuilt.

* Tue Feb 12 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.1-20130212
- Rebuilt.

* Thu Jan 17 2013 Stefan Manegold <Stefan.Manegold@cwi.nl> - 11.15.1-20130212
- testing: enabled "top-level" Mtest.py
  So far, while Mtest.py could be called in any subdirectory of the MonetDB
  source tree (and could then run all tests in the entire sub-tree),
  it was not possible to call Mtest.py in the top-level MonetDB source
  directory to run all tests.  Instead, to run all tests, Mtest.py had to
  be called at least 4 times, once in each of these directories: "clients",
  "monetdb5", "sql", "geom".
  Now, it is possible to call Mtest.py once in the top-level MonetDB source
  directory to run all tests in one go.
  The behaviour of calling Mtest.py in any subdirectory, including the
  four mentioned above, did not changed, other than that now obsolete
  command line options "-p / --package <package>" and "-5 / --monetdb5"
  have been removed.

* Tue Jan 15 2013 Fabian Groffen <fabian@monetdb.org> - 11.15.1-20130212
- clients: Mapi protocol v8 support was removed from all client drivers.  Protocol
  v8 has not been used by the servers any more since Apr2012 release
- clients: The tool mnc was removed from installations

* Tue Jan 15 2013 Fabian Groffen <fabian@monetdb.org> - 11.15.1-20130212
- java: merocontrol was changed to return server URIs, and lastStop time.
  Connections and dbpath were removed.
- java: Mapi protocol v8 support was removed from MapiSocket.  Protocol
  v8 has not been used by the servers any more since Apr2012 release

* Tue Jan 15 2013 Fabian Groffen <fabian@monetdb.org> - 11.15.1-20130212
- merovingian: Upgrade support for dbfarms from Mar2011 and Aug2011 was dropped

* Tue Jan 15 2013 Fabian Groffen <fabian@monetdb.org> - 11.15.1-20130212
- merovingian: monetdb status now uses a more condensed output, to cater for the uris
  being shown, and prints how long a database is stopped, or how long
  ago it crashed

* Tue Jan 15 2013 Fabian Groffen <fabian@monetdb.org> - 11.15.1-20130212
- merovingian: monetdb status now prints the connection uri for each database,
  when available.  The connections and database path properties have
  been dropped.

* Tue Jan 15 2013 Fabian Groffen <fabian@cwi.nl> - 11.15.1-20130212
- merovingian: monetdb status now prints last crash date only if the database has
  not been started since.

* Tue Jan 15 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.1-20130212
- monetdb5: mserver5: The --dbname and --dbfarm options have been replaced by the
  single --dbpath option.

* Tue Jan 15 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.15.1-20130212
- clients: msqldump: Implmented an option (--table/-t) to dump a single table.
- clients: Changed msqdump's trace option to be in line with mclient.  In both
  cases, the long option is --Xdebug and the short option is -X.

* Tue Jan 15 2013 Martin Kersten <mk@cwi.nl> - 11.15.1-20130212
- monetdb5: The scheduler of mserver5 was changed to use a fixed set of workers to
  perform the work for all connected clients.  Previously, each client
  connection had its own set of workers, easily causing resource problems
  upon multiple connections to the server.

* Tue Jan 15 2013 Sjoerd Mullender <sjoerd@acm.org> - 11.13.9-20130115
- Rebuilt.

* Wed Dec 12 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.13.7-20121212
- Rebuilt.

* Fri Nov 23 2012 Fabian Groffen <fabian@monetdb.org> - 11.13.7-20121212
- java: Implemented type map support of Connection to allow custom mapping
  of UDTs to Java classes.  By default the INET and URL UDTs are
  now mapped to nl.cwi.monetdb.jdbc.types.{INET,URL}.  Most notably,
  ResultSet.getObject() and PreparedStatement.setObject() deal with the
  type map.

* Thu Nov 22 2012 Fabian Groffen <fabian@monetdb.org> - 11.13.7-20121212
- java: Fixed a problem in PreparedStatement where the prepared statement's
  ResultSetMetaData (on its columns to be produced) incorrectly threw
  exceptions about non existing columns.  Bug #3192

* Wed Nov 21 2012 Fabian Groffen <fabian@monetdb.org> - 11.13.7-20121212
- sql: Fixed crash when performing an INSERT on a table with string-like column
  defaulting to NULL and omitting that column from VALUES, bug #3168

* Fri Nov 16 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.13.5-20121116
- Rebuilt.

* Tue Oct 16 2012 Fabian Groffen <fabian@monetdb.org> - 11.13.3-20121016
- Rebuilt.

* Wed Oct 10 2012 Fabian Groffen <fabian@cwi.nl> - 11.13.3-20121016
- java: Fixed problem with PreparedStatements and setXXX() methods using column
  numbers instead of names, bug #3158

* Wed Oct 10 2012 Fabian Groffen <fabian@monetdb.org> - 11.13.1-20121010
- Rebuilt.

* Tue Oct  9 2012 Fabian Groffen <fabian@cwi.nl> - 11.13.1-20121010
- merovingian: Fixed problem where monetdbd would refuse to startup when discovery
  was set to false, bug #3155

* Tue Sep 25 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.13.1-20121010
- monetdb5: Removed module attach since it wasn't used or even tested.

* Mon Sep 17 2012 Fabian Groffen <fabian@cwi.nl> - 11.13.1-20121010
- clients: mclient now accepts URIs as database to connect to.

* Mon Sep 17 2012 Fabian Groffen <fabian@cwi.nl> - 11.13.1-20121010
- monetdb5: The MAL-to-C Compiler (mcc) was removed.  The code wasn't tested and
  most likely non-functional.

* Mon Sep 17 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.13.1-20121010
- gdk: Removed the gdk_embedded (and embedded) option.  The code wasn't tested
  and most likely non-functional.

* Mon Sep 17 2012 Gijs Molenaar <g.j.molenaar@uva.nl> - 11.13.1-20121010
- clients: all strings returned by python2 are unicode, removed use_unicode option
- clients: python2 and 3 type convertion speed improvements
- clients: python2 uses new styl objects now (bug #3104)
- clients: split python2 and python3

* Mon Sep 17 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.13.1-20121010
- gdk: BAT-of-BATs is no longer allowed.  It was already not allowed to
  make these types of BATs persistent, but now they can't be created at
  all anymore.

* Mon Sep 17 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.13.1-20121010
- buildtools: Removed --enable-noexpand configure option.

* Mon Sep 17 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.11.11-20120917
- Rebuilt.

* Tue Sep 11 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.11.9-20120911
- Rebuilt.

* Fri Sep  7 2012 Fabian Groffen <fabian@cwi.nl> - 11.11.9-20120911
- monetdb5: Changed the way nclients maximum was calculated to avoid 'out of client
  slots' errors way before the maximum was reached.

* Fri Aug 31 2012 Fabian Groffen <fabian@cwi.nl> - 11.11.9-20120911
- merovingian: Resolved a problem where monetdb could fail to start a database with
  the message "database 'X' started up, but failed to open up a
  communication channel".  Bug #3134, comment #7.

* Fri Aug 31 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.11.9-20120911
- gdk: Fixed a bug in BATantijoin when either side is a singleton BAT.
  This fixes bug 3139.

* Tue Aug 14 2012 Fabian Groffen <fabian@cwi.nl> - 11.11.9-20120911
- java: Fixed a bug where DatabaseMetaData.getURL() did return null:0 for
  hostname:port.

* Mon Aug 13 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.11.7-20120813
- Rebuilt.

* Thu Aug  2 2012 Fabian Groffen <fabian@cwi.nl> - 11.11.7-20120813
- merovingian: Starting a server now waits for as long as the server needs to possibly
  recover, bug #3134.  In case of a long wait, the monetdbd logfile
  gives extra information on what the server is doing to recover.
- merovingian: Fixed a crash of monetdbd when local databases were unshared, bug #3135

* Thu Aug  2 2012 Fabian Groffen <fabian@cwi.nl> - 11.11.7-20120813
- monetdb5: The server now distinguishes between starting and started states,
  such that monetdbd can wait for it to finish starting.

* Fri Jul 20 2012 Fabian Groffen <fabian@cwi.nl> - 11.11.7-20120813
- java: Fixed adaptive cache size used when retrieving results, not to cause
  divide by zero errors when memory gets short, bug #3119.

* Wed Jul 18 2012 Fabian Groffen <fabian@cwi.nl> - 11.11.7-20120813
- merovingian: Resolved a problem where automatic starting of a database initiated by
  multiple clients at the same time could cause failed starts.  Bug #3107

* Tue Jul 17 2012 Fabian Groffen <fabian@cwi.nl> - 11.11.7-20120813
- clients: mclient no longer prints the SQLSTATE at the start of each error
  returned by the SQL-server.

* Tue Jul 10 2012 Fabian Groffen <fabian@monetdb.org> - 11.11.5-20120710
- Rebuilt.

* Mon Jul  9 2012 Niels Nes <niels@cwi.nl> - 11.11.5-20120710
- gdk: Fixed intermittent problem that joins and selects return incorrect
  results and possibly inconsistent databases. The problems only occurred
  after a series of queries and updates, therefore it was hard to reproduce.

* Mon Jul 09 2012 Fabian Groffen <fabian@monetdb.org> - 11.11.3-20120709
- Rebuilt.

* Sat Jul  7 2012 Fabian Groffen <fabian@cwi.nl> - 11.11.3-20120709
- merovingian: Fixed misc memory leaks, which caused monetdbd to grow in memory size
  over time.

* Fri Jul 06 2012 Fabian Groffen <fabian@monetdb.org> - 11.11.1-20120706
- Rebuilt.

* Mon Jul  2 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.11.1-20120706
- buildtools: Created seperate RPM and DEB packages for MonetDB/JAQL.

* Fri Jun 29 2012 Fabian Groffen <fabian@cwi.nl> - 11.11.1-20120706
- sql: COPY INTO now accepts optional parenthesis for file argument.
  Binary COPY INTO now requires 'COPY BINARY INTO'.

* Fri Jun 29 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.11.1-20120706
- clients: ODBC: Fixed a bug where SQLNativeSql expected a statment handle instead
  of a connection handle.

* Thu Jun 14 2012 Fabian Groffen <fabian@cwi.nl> - 11.11.1-20120706
- monetdb5: Crackers code has been removed.  Development continues in the holindex
  branch.

* Wed Jun 13 2012 Fabian Groffen <fabian@cwi.nl> - 11.11.1-20120706
- merovingian: Removed erroneously (re-)added master and slave properties, this
  functionality is currently not working.

* Thu Jun  7 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.11.1-20120706
- buildtools: Removed --enable-bits option from configure.

* Thu Jun  7 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.11.1-20120706
- buildtools: Split the MonetDB-client-ruby RPM package into two and named them in
  accordance with the Fedora packaging guidelines as rubygem-<gem-name>.

* Thu Jun  7 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.11.1-20120706
- gdk: The sorted property, which was used to maintain whether a column in
  a BAT was sorted or reverse sorted, has been replaced by a pair of
  properties, sorted and revsorted.  These new properties can be set
  independently (unlike the old sorted property), and so if both are set,
  the column must be constant.  In addition, internal property checking
  has been overhauled.  Now, when a property is set incorrectly, and
  when assertions are enabled, an assertion will go off.  There is also
  a function which can derive properties.

* Thu Jun  7 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.11.1-20120706
- gdk: Implemented proper overflow checking on all arithmetic operations.

* Thu Jun 07 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.9.7-20120607
- Rebuilt.

* Wed May 23 2012 Fabian Groffen <fabian@cwi.nl> - 11.9.7-20120607
- clients: Resolved a cosmetical error where tab-characters would cause
  misalignments in tabular result views.  For the time being, tabs are
  now represented as a single space in tabular view.

* Thu May 17 2012 Fabian Groffen <fabian@cwi.nl> - 11.9.7-20120607
- gdk: Limit number of detected CPU cores to 16 on 32-bits systems to avoid
  running quickly out of addressable resources followed by a kill from
  the OS.

* Wed May 16 2012 Fabian Groffen <fabian@monetdb.org> - 11.9.5-20120516
- Rebuilt.

* Tue May 15 2012 Fabian Groffen <fabian@cwi.nl> - 11.9.5-20120516
- merovingian: Fixed a bug where connecting to a stopped multiplex-funnel would result
  in a 'there are no available connections' error.

* Tue May 15 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.9.5-20120516
- sql: Databases that were upgraded from the Aug2011 release have an error
  in the catalog for SQL procedures.  This is now fixed.

* Mon May 14 2012 Fabian Groffen <fabian@monetdb.org> - 11.9.3-20120514
- Rebuilt.

* Wed May  2 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.9.3-20120514
- buildtools: Windows: we now use OpenSSL 1.0.1b.

* Fri Apr 27 2012 Fabian Groffen <fabian@cwi.nl> - 11.9.3-20120514
- gdk: Implemented MT_getrss for Mac OS X systems, this allows the server to
  know about how much memory is currently in use.

* Wed Apr 18 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.9.1-20120418
- Rebuilt.

* Mon Mar 12 2012 Fabian Groffen <fabian@cwi.nl> - 11.9.1-20120418
- merovingian: The logfile and pidfile monetdbd properties are now displayed with
  dbfarm path when relative

* Mon Mar 12 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.9.1-20120418
- clients: ODBC: Implemented the SQL_ATTR_CONNECTION_TIMEOUT attribute.

* Mon Mar 12 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.9.1-20120418
- clients: mclient now has a -a (--autocommit) option to turn off autocommit mode.

* Mon Mar 12 2012 Wouter Alink <wouter@spinque.com> - 11.9.1-20120418
- java: Password reading by JdbcClient no longer results in strange artifacts
- java: JdbcClient now returns exit code 1 in case of failures

* Mon Mar 12 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.9.1-20120418
- gdk: The type "chr" has been removed.
  chr has long been superseded by bte for 1 byte arithmetic plus it is
  pretty useless to hold single characters since we use Unicode and
  thus only a tiny subset of the supported character set would fit.

* Mon Mar 12 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.9.1-20120418
- monetdb5: The type "chr" has been removed.
  chr has long been superseded by bte for 1 byte arithmetic plus it is
  pretty useless to hold single characters since we use Unicode and
  thus only a tiny subset of the supported character set would fit.

* Mon Mar 12 2012 Fabian Groffen <fabian@monetdb.org> - 11.7.9-20120312
- Rebuilt.

* Tue Feb 28 2012 Fabian Groffen <fabian@cwi.nl> - 11.7.9-20120312
- java: Implemented missing Number types support in
  PreparedStatement.setObject()

* Fri Feb 24 2012 Fabian Groffen <fabian@monetdb.org> - 11.7.7-20120224
- Rebuilt.

* Wed Feb 22 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.7.7-20120224
- buildtools: Fixed some of the package names for Debian/Ubuntu.  Packages for
  libraries should contain the major number of the library version number.
  This was not always the case.

* Mon Feb 20 2012 Fabian Groffen <fabian@cwi.nl> - 11.7.7-20120224
- java: Fixed bug in DatabaseMetaData.getSchemas() method that caused an SQL
  error when called with catalog and schema argument.

* Fri Feb 17 2012 Fabian Groffen <fabian@cwi.nl> - 11.7.7-20120224
- merovingian: Fixed a bug in the multiplex-funnel where certain clients would abort
  on responses for update queries.

* Fri Feb 17 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.7.7-20120224
- sql: Fixed a crash that happened at the end of a database upgrade to the
  Dec2011 database scheme.  The crash happened during cleanup after the
  database was upgraded, so it was merely inconvenient.

* Wed Feb 15 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.7.7-20120224
- sql: Stripped off implementation-specific parts from error messages before
  they get presented to the user.

* Tue Feb 14 2012 Fabian Groffen <fabian@cwi.nl> - 11.7.7-20120224
- java: Resolved a bug where JDBC and Control connections could terminate
  abruptly with 'Connection closed' messages

* Thu Feb  9 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.7.7-20120224
- monetdb5: Fixed potential crash by dealing better with non-standard types.

* Tue Feb  7 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.7.7-20120224
- buildtools: On Debian and Ubuntu, install Python modules in dist-packages instead
  of site-packages.  This fixed bug 2997.

* Mon Jan 30 2012 Fabian Groffen <fabian@cwi.nl> - 11.7.7-20120224
- merovingian: Fixed problem where version and mserver properties for monetdbd were
  not always successfully retrieved.  Bug #2982.
- merovingian: Fixed problem where shutdown of monetdbd would lead to shutting down
  database 'control' which does not exist.  Bug #2983.
- merovingian: Fixed issue causing (harmless) 'error reading from control channel'
  messages.  Bug #2984.
- merovingian: Resolved problem where remote start/stop/etc. commands with monetdb
  would report error 'OK'.  Bug #2984.

* Fri Jan 20 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.7.5-20120120
- Rebuilt.

* Tue Jan 17 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.7.3-20120117
- Rebuilt.

* Mon Jan 16 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.7.3-20120117
- monetdb5: A couple of memory leaks have been plugged.

* Wed Jan 11 2012 Sjoerd Mullender <sjoerd@acm.org> - 11.7.1-20120111
- Rebuilt.

* Mon Jan  2 2012 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- java: Implemented getMetaData() method of PreparedStatement.

* Tue Dec 27 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- java: Fixed an AssertionError for special results from e.g. EXPLAIN queries.

* Tue Dec 27 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- merovingian: Fixed crash in monetdb when an invalid property was retrieved using
  the get command, bug #2953.

* Wed Dec 21 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- java: Fixed overflow error when batching large statements, bug #2952

* Tue Dec 20 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- java: Resolved a concurrency problem where ResultSet's date-related getters
  could cause odd stack traces when used by multiple threads at the
  same time, bug #2950.

* Mon Dec 19 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- java: JDBC now implements JDBCv4.1 which makes it possible to be built with
  Java 7 and up.  JDBCv4.1 is a maintenance release of JDBCv4, and hence
  can be compiled with Java 6, although the added methods obviously are
  not part of the java.sql interfaces.

* Sun Dec 11 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- java: SQLExceptions thrown now carry a SQLSTATE.  Until the server starts
  sending correct SQLSTATEs for all errors, server originated errors
  without SQLSTATE are considered generic data exceptions (22000).

* Sat Dec 10 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- java: Fixed a bug where closing ResultSets and PreparedStatements could lead
  to errors on concurrent running queries using the same Connection due
  to a race condition.

* Thu Dec  8 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- java: Changed version scheme of JDBC driver and MCL jar to be more standard,
  from monetdb-X.Y-<thing>.jar to monetdb-<thing>-X.Y.jar, bug #2943

* Wed Dec  7 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- clients: Fix making connections with stethoscope to hosts without monetdbd.
  Bug #2944.

* Tue Dec  6 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- clients: Fixed some bugs in the PHP interface affecting the mapi_ping(),
  monetdb_insert_id() and auto_commit() functions.  Bugs #2936, #2937,
  #2938.

* Tue Dec  6 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- gdk: Fixed memory detection on 32-bits Solaris systems with more memory
  than can be addressed in 32-bits mode, bug #2935

* Tue Nov 15 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.7.1-20120111
- clients: ODBC: Fixed SQLNumResultCols and SQLDescribeCol to return useful
  information after a call to SQLPrepare and before any SQLExecute.

* Tue Nov 15 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.7.1-20120111
- clients: mclient: The exact interpretation of the -i (--interactive) option
  and the - filename argument have changed.  The - filename argument
  means read from standard input and no longer implies that no \
  interpretation is done on the input.  Instead, \ interpretation is done
  if either standard input is a terminal, or if the -i option is given.
  The -i option no longer causes mclient to read from standard input.
  It only means to do \ interpretation when reading from standard input.
  Use the - filename argument to read from standard input.  Note that
  if no -s option is specified and no filename arguments are present,
  mclient still reads from standard input.

* Tue Nov 15 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.7.1-20120111
- sql: mclient: The csv output format can now also be of the form csv=c and
  csv+c where c is the column separator.  The form with csv+ produces
  a single header line with column names.

* Tue Nov 15 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.7.1-20120111
- clients: ODBC: Improved internal query for SQLSpecialColumns.  Before, the query
  returned all columns taking part in a PRIMARY KEY *and* all columns
  taking part in a UNIQUE constraint.  Now it returns only one or the
  other set.

* Tue Nov 15 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.7.1-20120111
- sql: Changed a bug where the sign() function returned the same type as its
  argument instead of always an INTEGER.

* Tue Nov 15 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.7.1-20120111
- clients: ODBC: The database name is now used as the catalog name throughout.
  Functions that return a catalog name return the database name, and
  functions that match on catalog name match it with the database name.

* Tue Nov 15 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.7.1-20120111
- clients: ODBC: Implemented an easier way to create a log file of interactions
  with the ODBC driver.  You can now add a connection attribute
  "LOGFILE=filename" to the connection string parameter of
  SQLBrowseConnect and SQLDriverConnect, and to the relevant part of
  the Windows registry or odbc.ini file.  This value is only used if
  there is no environment variable ODBCDEBUG.

* Tue Nov 15 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- java: The embedded monet instance capability of MonetConnection was removed.
- java: Bump JDBC version to 2.0 (Liberica).  JDBC now implements JDBCv4 which
  makes it possible to be built with Java 6 and up.  Java 5 and before
  are no longer supported, and can use the 1.x releases of the driver.

* Tue Nov 15 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- clients: Quoting of object names for mclient's \d command is now more flexible
  and consistent with standard SQL quoting rules, bug #2846.

* Tue Nov 15 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- merovingian: monetdb get output is now grouped by database instead of by property

* Tue Nov 15 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- merovingian: Unlike in previous releases, 'monetdbd get mserver' now returns the path
  to the mserver5 binary only for dbfarms that have a running monetdbd,
  instead of only for those that are not served by a monetdbd.

* Tue Nov 15 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- merovingian: Add nclients property that controls the maximum number of concurrent
  clients allowed to access the database

* Tue Nov 15 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- monetdb5: Introduced new variable max_clients that allows to define how many
  concurrent connections are allowed to be made against the database.

* Tue Nov 15 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.7.1-20120111
- clients: ODBC: Implemented SQLColumnPrivileges function.

* Tue Nov 15 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- java: INTERVAL columns are now treated as decimals, since they can have
  sub-second precision.

* Tue Nov 15 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- merovingian: Starting monetdbd without any arguments or without dbfarm is no longer
  supported.  A dbfarm now must be provided for each command.
- merovingian: The control passphrase has been turned into a hash of the password, for
  a more stronger authorisation model as used by mclients.  On upgrade,
  any existing passphrase is converted to the hashed version of the
  passphrase.
- merovingian: The monetdbd controlport option has been removed in favour of a
  boolean control option.  On upgrade, when controlport was set to
  a non-zero value and a passphrase was set, control is set to true,
  or false otherwise.

* Tue Nov 15 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- clients: Install new program, mnc, which provides netcat functionality, but
  based on MonetDB's communication libraries

* Tue Nov 15 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- clients: Report full URI to database instead of just database when available
  in mclient.

* Tue Nov 15 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- merovingian: The monetdbd discoveryport option has been removed in favour of a
  boolean discovery option.  On upgrade, when discoveryport was set to
  a non-zero value, discovery is set to true, or false otherwise.

* Tue Nov 15 2011 Fabian Groffen <fabian@cwi.nl> - 11.7.1-20120111
- clients: The time format of the timer output can now be controlled with an
  optional argument to the -i option.  ms, s and m force the time to be
  formatted as milliseconds, seconds or minutes + seconds respectively.

* Tue Nov 15 2011 Martin Kersten <mk@cwi.nl> - 11.7.1-20120111
- monetdb5: bpm and partitions have been moved to the attic.  It is replaced by
  the partition optimizer, still under construction.

* Tue Nov 15 2011 Martin Kersten <mk@cwi.nl> - 11.7.1-20120111
- monetdb5: mal_interpreter.mx Protect against concurrent exceptions If multiple
  parallel blocks access the exception variables and perform GDKfree
  on old messages, then we may enter a case that one thread attempts a
  second free.  Simple lock-based protection is the first line of defense.

* Tue Nov 15 2011 Martin Kersten <mk@cwi.nl> - 11.7.1-20120111
- monetdb5: The dataflow optimizer uses a less strict side-effect test for BAT
  new operations and better recognizes trivial plans.

* Tue Nov 15 2011 Martin Kersten <mk@cwi.nl> - 11.7.1-20120111
- monetdb5: The MAL debugger next/step operations semantics have been fixed.
  The profiler also now clearly shows entrance/exit of a MAL function.

* Tue Nov 15 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.9-20111115
- Rebuilt.

* Sun Nov  6 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.9-20111115
- merovingian: Fixed a bug where monetdbd's socket files from /tmp were removed when
  a second monetdbd was attempted to be started using the same port.

* Wed Oct 26 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.9-20111115
- sql: Added a fix for bug #2834, which caused weird (failing) behaviour
  with PreparedStatements.

* Fri Oct 21 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.7-20111021
- Rebuilt.

* Thu Oct 20 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.7-20111021
- clients: ODBC: Implemented a workaround in SQLTables for bug 2908.

* Tue Oct 18 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.5-20111018
- Rebuilt.

* Mon Oct 17 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.5-20111018
- clients: Small improvement to mclient's table rendering for tables without
  any rows.  Previously, the column names in the header could be
  squeezed to very small widths, degrading readability.

* Wed Oct 12 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.5-20111018
- clients: Python DB API connect() function now supports PEP 249-style arguments
  user and host, bug #2901

* Wed Oct 12 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.5-20111018
- clients: mclient now checks the result of encoding conversions using the iconv
  library.

* Mon Oct 10 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.5-20111018
- clients: Fixed a source of crashes in mclient when a query on the command line
  using the -s option is combined with input on standard input (e.g. in
  the construct mclient -s 'COPY INTO t FROM STDIN ...' < file.csv).

* Sun Oct  9 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.5-20111018
- merovingian: Resolved problem where monetdbd would terminate abnormally when
  databases named 'control', 'discovery' or 'merovingian' were stopped.

* Fri Oct  7 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.5-20111018
- merovingian: monetdbd get status now also reports the version of the running monetdbd

* Fri Oct  7 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.5-20111018
- clients: Fixed bug 2897 where slow (network) reads could cause blocks to not
  be fully read in one go, causing errors in the subsequent use of
  those blocks.  With thanks to Rémy Chibois.

* Thu Oct  6 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.5-20111018
- merovingian: Improved response time of 'monetdb start' when the database fails
  to start.

* Wed Oct  5 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.5-20111018
- merovingian: Fixed a bug in monetdbd where starting a failing database could
  incorrectly be reported as a 'running but dead' database.

* Fri Sep 30 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.5-20111018
- merovingian: To avoid confusion, all occurrences of merovingian were changed into
  monetdbd for error messages sent to a client.

* Tue Sep 27 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.5-20111018
- clients: Fixed a bug in mclient where processing queries from files could result
  in ghost empty results to be reported in the output

* Sun Sep 25 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.3-20110925
- Rebuilt.

* Fri Sep 23 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.3-20110925
- clients: Fixed Perl DBD rowcount for larger results, bug #2889

* Wed Sep 21 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.3-20110925
- monetdb5: Fixed a problem where MAL variables weren't properly cleared before
  reuse of the data strucutre.  This problem could cause the data flow
  scheduler to generate dependencies between instructions that didn't
  actually exist, which in turn could cause circular dependencies among
  instructions with deadlock as a result.  Bugs 2865 and 2888.

* Wed Sep 21 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.3-20110925
- sql: Fixed a bug when using default values for interval columns.  Bug 2877.

* Mon Sep 19 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.3-20110925
- clients: Perl: We now distinguish properly between TABLE and GLOBAL TEMPORARY
  (the latter are recognized by being in the "tmp" schema).
- clients: Perl: fixed a bunch of syntax errors.  This fixes bug 2884.  With thanks
  to Rémy Chibois.
- clients: Perl: Fixed DBD::monetdb table_info and tabletype_info.  This fixes
  bug 2885.  With thanks to Rémy Chibois.

* Fri Sep 16 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.3-20110925
- sql: A bug was fixed where deleted rows weren't properly accounted for in
  all operations.  This was bug 2882.
- sql: A bug was fixed which caused an update to an internal table to
  happen too soon.  The bug could be observed on a multicore system
  with a query INSERT INTO t (SELECT * FROM t) when the table t is
  "large enough".  This was bug 2883.

* Tue Sep 13 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.3-20110925
- clients: mclient: fix display of varchar columns with only NULL values.
- clients: Fixed a bug in mclient/msqldump where an internal error occurred during
  dump when there are GLOBAL TEMPORARY tables.

* Wed Sep 07 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.1-20110907
- Rebuilt.

* Wed Aug 31 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.1-20110907
- clients: msqldump now also accepts the database name as last argument on the
  command line (i.e. without -d).

* Fri Aug 26 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.1-20110907
- clients: Made error messages from the server in mclient go to stderr, instead
  of stdout.

* Thu Aug 25 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.1-20110907
- gdk: Removed conversion code for databases that still used the (more than
  two year) old format of "string heaps".

* Tue Aug 23 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.1-20110907
- merovingian: Fixed confusing 'Success' error message for monetdb commands where an
  invalid hostname was given

* Fri Aug 19 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.1-20110907
- merovingian: The path to the mserver5 binary is no longer returned for the mserver
  property with monetdbd get for a dbfarm which is currently served by
  a monetdbd.  Since the called monetdbd needs not to be the same as
  the running monetdbd, the reported mserver5 binary may be incorrect,
  and obviously lead to confusing situations.  Refer to the running
  monetdbd's logfile to determine the mserver5 binary location instead.

* Thu Aug 18 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.1-20110907
- clients: Implemented SQL_ATTR_METADATA_ID attribute.  The attribute is used
  in SQLColumns, SQLProcedures, and SQLTablePrivileges.
- clients: Implemented SQLTablePrivileges in the ODBC driver.

* Wed Aug 17 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.1-20110907
- merovingian: Added -n option to monetdbd start command, which prevents monetdbd
  from forking into the background.

* Wed Aug 10 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.1-20110907
- gdk: On Windows and Linux/Unix we can now read databases built on the other
  O/S, as long as the hardware-related architecture (bit size, floating
  point format, etc.) is identical.

* Sat Aug  6 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.1-20110907
- merovingian: Fix incorrect (misleading) path for pidfile in pidfile error message,
  bug #2851

* Sat Aug  6 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.1-20110907
- buildtools: Fixed Fedora 15 (and presumably later) configuration that uses a tmpfs
  file system for /var/run.  This fixes bug 2850.

* Fri Aug  5 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.1-20110907
- clients: mclient now automatically sets the SQL `TIME ZONE' variable to its
  (the client's) time zone.

* Fri Jul 29 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.1-20110907
- geom: Implemented NULL checks in the geom module.  Now when given NULL
  as input, the module functions return NULL instead of an exception.
  This fixes bug 2814.

* Tue Jul 26 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.1-20110907
- clients: Removed perl/Cimpl, MonetDB-CLI-MapiLib and MonetDB-CLI-MapiXS
- clients: Switched implementation of MonetDB::CLI::MapiPP to Mapi.pm, and made
  it the default MonetDB::CLI provider.

* Tue Jul 26 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.1-20110907
- buildtools: The default OID size for 64-bit Windows is now 64 bits.  Databases with
  32 bit OIDs are converted automatically.

* Tue Jul 26 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.1-20110907
- clients: Made Mapi.pm usable with current versions of MonetDB again

* Tue Jul 26 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.1-20110907
- monetdb5: Make crackers optional and disable by default, since it wasn't used
  normally

* Tue Jul 26 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.1-20110907
- java: Add so_timeout Driver property to specify a SO_TIMEOUT value for the
  socket in use to the database.  Setting this property to a value in
  milliseconds defines the timeout for read calls, which may 'unlock'
  the driver if the server hangs, bug #2828

* Tue Jul 26 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.1-20110907
- java: Added a naive implementation for PreparedStatement.setCharacterStream

* Tue Jul 26 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.5.1-20110907
- gdk: Implemented automatic conversion of a 64-bit database with 32-bit OIDs
  to one with 64-bit OIDs.

* Tue Jul 26 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.1-20110907
- merovingian: added status property to get command

* Tue Jul 26 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.1-20110907
- monetdb5: Authorisation no longer takes scenarios into account.  Access for only
  sql or mal is no longer possible.  Any credentials now mean access to
  all scenarios that the server has available.

* Tue Jul 26 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.1-20110907
- clients: When the first non-option argument of mclient does not refer to an
  exising file, it now is taken as database name.  This allows to simply
  do `mclient mydb`.

* Tue Jul 26 2011 Fabian Groffen <fabian@cwi.nl> - 11.5.1-20110907
- java: The obsolete Java-based implementation for PreparedStatements (formerly
  activated using the java_prepared_statements property) has been dropped

* Tue Jul 26 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.7-20110726
- Rebuilt.

* Wed Jul 20 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.5-20110720
- Rebuilt.

* Tue Jul 19 2011 Fabian Groffen <fabian@cwi.nl> - 11.3.5-20110720
- sql: Fixed regression where the superuser password could no longer be
  changed, bug #2844

* Wed Jul 13 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.5-20110720
- buildtools: We can now build RPMs on CentOS 6.0.  Since there is no geos library
  on CentOS, we do not support the geom modules there.

* Sat Jul  9 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.5-20110720
- gdk: Fixed a problem where appending string BATs could cause enormous growth
  of the string heap.  This fixes bug 2820.

* Fri Jul  8 2011 Fabian Groffen <fabian@cwi.nl> - 11.3.5-20110720
- java: Return false from Statement.getMoreResults() instead of a
  NullPointerException when no query has been performed on the Statement
  yet, bug #2833

* Fri Jul  1 2011 Fabian Groffen <fabian@cwi.nl> - 11.3.5-20110720
- clients: Fix stethoscope's mod.fcn filter when using multiple targets, bug #2827

* Wed Jun 29 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.5-20110720
- buildtools: We can now also build on Fedora 15.  This required some very minor
  changes.
- buildtools: Changed configure check for OpenSSL so that we can also build on CentOS
  5.6.  We now no longer demand that OpenSSL is at least version 0.9.8f,
  but instead we require that the hash functions we need are supported.

* Wed Jun 29 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.5-20110720
- clients: The separate Python distribution now uses the same version number as
  the main package.

* Wed Jun 29 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.5-20110720
- gdk: Fixes to memory detection on FreeBSD.

* Wed Jun 29 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.5-20110720
- sql: Fixed incorrect insert counts.
- sql: Fixed bug 2823: MAL exeption on SQL query with subquery in the where
  part.
- sql: Redirect error from create scripts back to the first client.  This
  fixes bug 2813.
- sql: Added joinidx based semijoin; push join through union (using
  joinidx).
- sql: Fixed pushing select down.

* Mon Jun  6 2011 Fabian Groffen <fabian@cwi.nl> - 11.3.5-20110720
- java: Fixed read-only interpretation.  Connection.isReadOnly now always
  returns false, setReadOnly now generates a warning when called with
  true.  Partly from bug #2818
- java: Allow readonly to be set when autocommit is disabled as well.  Bug #2818

* Tue May 17 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.3-20110517
- Rebuilt.

* Fri May 13 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.3-20110517
- gdk: Fixed a bug where large files (> 2GB) didn't always get deleted on
Windows.

* Wed May 11 2011 Fabian Groffen <fabian@cwi.nl> - 11.3.3-20110517
- java: Insertion via PreparedStatement and retrieval via ResultSet of timestamp
and time fields with and without timezones was improved to better
respect timezones, as partly indicated in bug #2781.

* Wed May 11 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.3-20110517
- monetdb5: Fixed a bug in conversion from string to the URL type.  The bug was
an incorrect call to free().

* Wed Apr 27 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.3-20110517
- geom: Fixed various problems so that now all our tests work correctly on
all our testing platforms.

* Thu Apr 21 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.1-20110421
- Rebuilt.

* Mon Apr 18 2011 Fabian Groffen <fabian@cwi.nl> - 11.3.1-20110421
- merovingian: Fix monetdb return code upon failure to start/stop a database

* Thu Apr 14 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.1-20110414
- Rebuilt.

* Thu Apr 14 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.1-20110414
- gdk: Fixed bugs in antiselect which gave the incorrect result when upper
  and lower bound were equal.  This bug could be triggered by the SQL
  query SELECT * FROM t WHERE x NOT BETWEEN y AND y.

* Thu Apr 14 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.3.1-20110414
- sql: Some names in the SQL catalog were changed.  This means that the
  database in the Apr2011 release is not compatible with pre-Apr2011
  databases.  The database is converted automatically when opened the
  first time.  This database can then no longer be read by an older
  release.

* Tue Apr  5 2011 Fabian Groffen <fabian@cwi.nl> - 11.3.1-20110414
- clients: Plugged a small memory leak occurring upon redirects by the server
  (e.g. via monetdbd)

* Tue Apr  5 2011 Fabian Groffen <fabian@cwi.nl> - 11.3.1-20110414
- java: clarify exception messages for unsupported methods

* Thu Mar 24 2011 Fabian Groffen <fabian@cwi.nl> - 11.3.1-20110414
- merovingian: The forward property for databases has been removed.  Instead, only
  a global proxy or redirect mode can be set using monetdbd.

* Thu Mar 24 2011 Fabian Groffen <fabian@cwi.nl> - 11.3.1-20110414
- merovingian: monetdbd can no longer log error and normal messages to separate
  logfiles, logging to stdout and stderr is no longer possible either.
- merovingian: The .merovingian_pass file is no longer in use, and replaced by the
  .merovingian_properties file.  Use monetdbd (get|set) passphrase to
  view/edit the control passphrase.  Existing .merovingian_pass files
  will automatically be migrated upon startup of monetdbd.
- merovingian: monetdbd now understands commands that allow to create, start, stop,
  get and set properties on a given dbfarm.  This behaviour is intended
  as primary way to start a MonetDB Database Server, on a given location
  of choice.  monetdbd get and set are the replacement of editing the
  monetdb5.conf file (which is no longer in use as of the Apr2011
  release).  See monetdbd(1).

* Thu Mar 24 2011 Fabian Groffen <fabian@cwi.nl> - 11.3.1-20110414
- clients: Remove XQuery related code from Ruby adapter, PHP driver and Perl Mapi
  library

* Thu Mar 24 2011 Fabian Groffen <fabian@cwi.nl> - 11.3.1-20110414
- java: Removed XQuery related XRPC wrapper and XML:DB code, removed support
  for language=xquery and language=mil from JDBC.

* Thu Mar 24 2011 Fabian Groffen <fabian@cwi.nl> - 11.3.1-20110414
- clients: Make SQL the default language for mclient, e.g. to use when --language=
  or -l is omitted

* Thu Mar 24 2011 Fabian Groffen <fabian@cwi.nl> - 11.3.1-20110414
- monetdb5: mserver5 no longer reads monetdb5.conf upon startup by default.
  Use --config=file to have mserver5 read a configuration on startup

* Thu Mar 24 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.1.1-20110324
- Rebuilt.

* Tue Mar 22 2011 Fabian Groffen <fabian@cwi.nl> - 11.1.1-20110324
- gdk: Fixed memory detection on Darwin (Mac OS X) systems not to return
  bogus values

* Thu Mar 17 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.1.1-20110317
- Rebuilt.

* Tue Mar 15 2011 Fabian Groffen <fabian@cwi.nl> - 11.1.1-20110317
- geom: Set endianness for wkb en/decoding.

* Sat Mar 05 2011 Stefan de Konink <stefan@konink.de> - 11.1.1-20110317
- monetdb5: sphinx module: update, adding limit/max_results support

* Mon Feb 14 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.1.1-20110317
- clients: Fixed bug 2677: SQL_DESC_OCTET_LENGTH should give the size in bytes
  required to copy the data.

* Mon Jan 24 2011 Fabian Groffen <fabian@cwi.nl> - 11.1.1-20110317
- clients: Disable timer functionality for non-XQuery languages since it is
  incorrect, bug #2705

* Mon Jan 24 2011 Fabian Groffen <fabian@cwi.nl> - 11.1.1-20110317
- sql: Fix bug #2648, do not allow restarting a sequence with NULL via the
  result of a sub-query.

* Fri Jan 14 2011 Fabian Groffen <fabian@cwi.nl> - 11.1.1-20110317
- gdk: MonetDB/src/gdk was moved to gdk

* Tue Jan  4 2011 Fabian Groffen <fabian@cwi.nl> - 11.1.1-20110317
- clients: Added mapi_get_uri function to retrieve mapi URI for the connection

* Tue Jan  4 2011 Fabian Groffen <fabian@cwi.nl> - 11.1.1-20110317
- merovingian: Allow use of globs with all commands that accept database names as
  their parameters

* Tue Jan  4 2011 Fabian Groffen <fabian@cwi.nl> - 11.1.1-20110317
- java: PreparedStatements now free the server-side resources attached to them
  when closed.  This implements bug #2720

* Tue Jan  4 2011 Niels Nes <niels@cwi.nl> - 11.1.1-20110317
- sql: Allow clients to release prepared handles using Xrelease commands

* Tue Jan  4 2011 Fabian Groffen <fabian@cwi.nl> - 11.1.1-20110317
- clients: Allow to dump table data using INSERT INTO statements, rather than COPY
  INTO + CSV data using the -N/--inserts flag of mclient and msqldump.
  Bug #2727

* Tue Jan  4 2011 Fabian Groffen <fabian@cwi.nl> - 11.1.1-20110317
- clients: Added support for \dn to list schemas or describe a specific one

* Tue Jan  4 2011 Fabian Groffen <fabian@cwi.nl> - 11.1.1-20110317
- clients: Added support for \df to list functions or describe a specific one
- clients: Added support for \ds to list sequences or describe a specific one

* Tue Jan  4 2011 Fabian Groffen <fabian@cwi.nl> - 11.1.1-20110317
- clients: Added support for wildcards * and ? in object names given to \d
  commands, such that pattern matching is possible, e.g. \d my*
- clients: Added support for \dS that lists also system tables

* Tue Jan  4 2011 Fabian Groffen <fabian@cwi.nl> - 11.1.1-20110317
- clients: object names given to \d are now lowercased, unless quoted by either
  single or double quotes
- clients: Strip any trailing whitespace with the \d command

* Tue Jan  4 2011 Fabian Groffen <fabian@cwi.nl> - 11.1.1-20110317
- merovingian: merovingian has been renamed into monetdbd.  Internally, monetdbd keeps
  referring to merovingian for e.g. settings and logfiles.  Merovingian
  has been renamed to make the process more recognisable as part of the
  MonetDB suite.

* Tue Jan  4 2011 Fabian Groffen <fabian@cwi.nl> - 11.1.1-20110317
- monetdb5: Improve the performance of remote.put for BAT arguments.  The put
  speed is now roughly equal to the speed of get on a BAT.

* Tue Jan  4 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.0.0-0
- Created top-level bootstrap/configure/make with new version numbers.

