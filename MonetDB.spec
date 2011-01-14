%define name MonetDB
%define version 11.0.0
%{!?buildno: %define buildno %(date +%Y%m%d)}
%define release %{buildno}%{?dist}%{?oid32:.oid32}%{!?oid32:.oid%{bits}}

# groups of related archs
%define all_x86 i386 i586 i686

%ifarch %{all_x86}
%define bits 32
%else
%define bits 64
%endif

# by default we do not build the netcdf package
%{!?_with_netcdf: %{!?_without_netcdf: %define _without_netcdf --without-netcdf}}

Name: %{name}
Version: %{version}
Release: %{release}
Summary: MonetDB - Monet Database Management System
Vendor: MonetDB BV <info@monetdb.org>

Group: Applications/Databases
License: MPL - http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
URL: http://monetdb.cwi.nl/
Source: http://dev.monetdb.org/downloads/sources/Oct2010/%{name}-%{version}.tar.gz

BuildRequires: bzip2-devel
BuildRequires: geos-devel >= 2.2.0
BuildRequires: libxml2-devel
BuildRequires: openssl-devel >= 0.9.8f
BuildRequires: pcre-devel >= 4.5
BuildRequires: perl
BuildRequires: perl-devel
BuildRequires: python
BuildRequires: raptor-devel >= 1.4.16
BuildRequires: readline-devel
BuildRequires: ruby
BuildRequires: rubygems
BuildRequires: swig
BuildRequires: unixODBC-devel
BuildRequires: zlib-devel

%define perl_libdir %(perl -MConfig -e '$x=$Config{installvendorarch}; $x =~ s|$Config{vendorprefix}/||; print $x;')
%if ! (0%{?fedora} > 12 || 0%{?rhel} > 5)
%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}
%endif
%{!?ruby_sitelib: %global ruby_sitelib %(ruby -rrbconfig -e 'puts Config::CONFIG["sitelibdir"] ')}
%define gemdir %(ruby -rubygems -e 'puts Gem::dir' 2>/dev/null)

%description
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the core components of MonetDB.  If you want to
use MonetDB, you will certainly need this package.

%package devel
Summary: MonetDB development package
Group: Applications/Databases
Obsoletes: MonetDB-client-devel
Obsoletes: MonetDB-geom-devel
Obsoletes: MonetDB4-server-devel
Obsoletes: MonetDB5-server-devel
Obsoletes: MonetDB4-XQuery-devel
Obsoletes: MonetDB-SQL-devel
Requires: %{name} = %{version}-%{release}
Requires: %{name}-clients = %{version}-%{release}
Requires: %{name}-clients-odbc = %{version}-%{release}
Requires: %{name}-geom-MonetDB4 = %{version}-%{release}
Requires: %{name}-geom-MonetDB5 = %{version}-%{release}
Requires: MonetDB4-server = %{version}-%{release}
%if %{?_with_netcdf:1}%{!?_with_netcdf:0}
Requires: MonetDB4-server-netcdf = %{version}-%{release}
%endif
Requires: MonetDB5-server = %{version}-%{release}
Requires: MonetDB5-server-rdf = %{version}-%{release}
Requires: %{name}-SQL-server5 = %{version}-%{release}
Requires: MonetDB4-XQuery = %{version}-%{release}
Requires: MonetDB4-XQuery-ferry = %{version}-%{release}

Requires: bzip2-devel
Requires: geos-devel >= 2.2.0
Requires: libxml2-devel
%if %{?_with_netcdf:1}%{!?_with_netcdf:0}
Requires: netcdf-devel
%endif
Requires: openssl-devel >= 0.9.8f
Requires: pcre-devel >= 4.5
Requires: perl
Requires: perl-devel
Requires: python
Requires: raptor-devel >= 1.4.16
Requires: readline-devel
Requires: ruby
Requires: rubygems
Requires: swig
Requires: unixODBC-devel
Requires: zlib-devel

%description devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the files needed to develop with MonetDB.

%package client
Summary: MonetDB - Monet Database Management System Client Programs
Group: Applications/Databases
Requires: %{name} = %{version}-%{release}

%description client
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains mclient, the main client program to communicate
with the database server, and msqldump, a program to dump the SQL
database so that it can be loaded back later.  If you want to use
MonetDB, you will very likely need this package.

%package client-odbc
Summary: MonetDB SQL odbc
Group: Applications/Databases
Requires: %{name}-client = %{version}-%{release}

%description client-odbc
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the MonetDB ODBC driver.

%package client-php
Summary: MonetDB php interface
Group: Applications/Databases
Requires: php
BuildArch: noarch

%description client-php
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the files needed to use MonetDB from a PHP
program.

%package client-perl
Summary: MonetDB perl interface
Group: Applications/Databases
Requires: %{name}-client = %{version}-%{release}
Requires: perl

%description client-perl
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the files needed to use MonetDB from a Perl
program.

%package client-ruby
Summary: MonetDB ruby interface
Group: Applications/Databases
Requires: ruby(abi) = 1.8
BuildArch: noarch

%description client-ruby
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the files needed to use MonetDB from a Ruby
program.

%package client-tests
Summary: MonetDB Client tests package
Group: Applications/Databases
Requires: %{name}-client = %{version}-%{release}
Requires: %{name}-client-odbc = %{version}-%{release}

%description client-tests
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the sample MAPI programs used for testing other
MonetDB packages.  You probably don't need this, unless you are a
developer.

%package geom
Summary: MonetDB Geom - Monet Database Management System
Group: Applications/Databases

%description geom
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the common parts of the GIS (Geographic
Information System) extensions for MonetDB-SQL.

%package geom-MonetDB4
Summary: MonetDB4 SQL GIS modules
Group: Applications/Databases
Requires: MonetDB4-server = %{version}-%{release}

%description geom-MonetDB4
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the GIS (Geographic Information System)
extensions for MonetDB4-server.

%package geom-MonetDB5
Summary: MonetDB5 SQL GIS modules
Group: Applications/Databases
Requires: MonetDB5-server = %{version}-%{release}

%description geom-MonetDB5
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the GIS (Geographic Information System)
extensions for MonetDB-SQL-server5.

%package -n MonetDB4-server
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Requires(pre): shadow-utils
Requires: %{name}-client = %{version}-%{release}

%description -n MonetDB4-server
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the MonetDB4 server component.  You need this
package if you want to work using the MIL language, or if you want to
use the XQuery frontend (in which case you need MonetDB-XQuery as
well).

%if %{?_with_netcdf:1}%{!?_with_netcdf:0}
%package -n MonetDB4-server-netcdf
Summary: MonetDB4 module for using NetCDF
Group: Applications/Databases
Requires: MonetDB4-server = %{version}-%{release}
BuildRequires: netcdf-devel

%description -n MonetDB4-server-netcdf
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains a module for use with the MonetDB4 server
component to interface to the NetCDF (network Common Data Form)
libraries.
%endif

%package -n MonetDB5-server
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Requires(pre): shadow-utils
Requires: %{name}-client = %{version}-%{release}

%description -n MonetDB5-server
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the MonetDB5 server component.  You need this
package if you want to work using the MAL language, or if you want to
use the SQL frontend (in which case you need MonetDB-SQL-server5 as
well).

%package -n MonetDB5-server-rdf
Summary: MonetDB RDF interface
Group: Applications/Databases
Requires: MonetDB5-server = %{version}-%{release}

%description -n MonetDB5-server-rdf
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the MonetDB5 RDF module.

%package SQL
Summary: MonetDB SQL - Monet Database Management System
Group: Applications/Databases

%description SQL
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains some common files for the MonetDB-SQL-server5
package.  You really need to install MonetDB-SQL-server5.

%package SQL-server5
Summary: MonetDB5 SQL server modules
Group: Applications/Databases
Requires: MonetDB5-server = %{version}-%{release}

%description SQL-server5
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the SQL frontend for MonetDB5.  If you want to
use SQL with MonetDB, you will need to install this package.

%package -n MonetDB4-XQuery
Summary: MonetDB XQuery - Monet Database Management System
Group: Applications/Databases
Requires: MonetDB4-server = %{version}-%{release}

%description -n MonetDB4-XQuery
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XQuery- frontends.

This package contains the XQuery frontend.  If you want to store XML
documents in MonetDB and use XQuery to query those documents, you will
need this package.

%package -n MonetDB4-XQuery-ferry
Summary: MonetDB XQuery Ferry library
Group: Applications/Databases
Requires: MonetDB4-XQuery = %{version}-%{release}

%description -n MonetDB4-XQuery-ferry
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XQuery- frontends.

This package contains the pf_ferry library.

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
accelerators, SQL- and XQuery- frontends.

This package contains the files needed to use MonetDB from a Python
program.

%package testing
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Requires: %{name}-devel = %{version}-%{release}
Requires: %{name}-testing-python = %{version}-%{release}
Obsoletes: MonetDB-python

%description testing
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the programs and files needed for testing the
MonetDB packages.  You probably don't need this, unless you are a
developer.

%package testing-python
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Requires: %{name}-devel = %{version}-%{release}
Requires: %{name}-client-tests = %{version}-%{release}
Requires: python
BuildArch: noarch

%description testing-python
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the Python programs and files needed for testing
the MonetDB packages.  You probably don't need this, unless you are a
developer.

%prep
%setup -q

%build

%{configure} \
        --enable-strict=no \
        --enable-assert=no \
        --enable-debug=no \
        --enable-optimize=yes \
        --enable-bits=%{bits} \
	--enable-java=no \
        %{?oid32:--enable-oid32} \
        %{?comp_cc:CC="%{comp_cc}"} \
	%{?_with_netcdf} %{?_without_netcdf}

make

%install
rm -rf $RPM_BUILD_ROOT

make install DESTDIR=$RPM_BUILD_ROOT

mkdir -p $RPM_BUILD_ROOT/%{_localstatedir}/MonetDB
mkdir -p $RPM_BUILD_ROOT/%{_localstatedir}/MonetDB4/dbfarm

# remove unwanted stuff
# .la files
rm -f $RPM_BUILD_ROOT%{_libdir}/MonetDB5/lib/*.la
# DB2 specific stuff:
rm -f $RPM_BUILD_ROOT%{_bindir}/pfserialize.pl
rm -f $RPM_BUILD_ROOT%{_bindir}/pfloadtables.sh
rm -f $RPM_BUILD_ROOT%{_bindir}/pfcreatetables.sh

%pre -n MonetDB4-server
getent group monetdb >/dev/null || groupadd -r monetdb
getent passwd monetdb >/dev/null || \
useradd -r -g monetdb -d %{_localstatedir}/MonetDB -s /sbin/nologin \
    -c "MonetDB Server" monetdb
exit 0

%pre -n MonetDB5-server
getent group monetdb >/dev/null || groupadd -r monetdb
getent passwd monetdb >/dev/null || \
useradd -r -g monetdb -d %{_localstatedir}/MonetDB -s /sbin/nologin \
    -c "MonetDB Server" monetdb
exit 0

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%clean
rm -fr $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%{_libdir}/libbat.so.*
%{_libdir}/libstream.so.*

%files devel
%defattr(-,root,root)
%{_bindir}/calibrator
%dir %{_includedir}/MonetDB
%dir %{_includedir}/MonetDB/common
%dir %{_includedir}/MonetDB/gdk
%dir %{_includedir}/MonetDB/mapilib
%dir %{_includedir}/MonetDB4
%dir %{_includedir}/MonetDB5
%dir %{_includedir}/MonetDB5/atoms
%dir %{_includedir}/MonetDB5/compiler
%dir %{_includedir}/MonetDB5/crackers
%dir %{_includedir}/MonetDB5/kernel
%dir %{_includedir}/MonetDB5/mal
%dir %{_includedir}/MonetDB5/optimizer
%dir %{_includedir}/MonetDB5/rdf
%dir %{_includedir}/MonetDB5/scheduler
%dir %{_includedir}/MonetDB5/sql
%dir %{_includedir}/MonetDB5/tools
%if %{?_with_netcdf:1}%{!?_with_netcdf:0}
%exclude %{_includedir}/MonetDB4/mnetcdf/*.[hcm]
%endif
%{_includedir}/MonetDB4/*/*.[hcm]
%{_includedir}/MonetDB5/*/*.[hcm]
%{_includedir}/MonetDB/*/*.[hcm]
%{_includedir}/pf_ferry.h
%{_libdir}/MonetDB5/lib/lib_*.so
%{_libdir}/pkgconfig/*.pc
%{_libdir}/*.la
%{_libdir}/*.so

%files client
%defattr(-,root,root)
%{_bindir}/mclient
%{_bindir}/msqldump
%{_bindir}/stethoscope
%{_libdir}/libMapi.so.*
%{_mandir}/man1/mclient.1.gz
%{_mandir}/man1/msqldump.1.gz

%files client-odbc
%defattr(-,root,root)
%{_libdir}/libMonetODBC.*
%{_libdir}/libMonetODBCs.*

%files client-php
%defattr(-,root,root)
%dir %{_datadir}/php/monetdb
%{_datadir}/php/monetdb/*

%files client-perl
%defattr(-,root,root)
%{_prefix}/%{perl_libdir}/*
%dir %{_datadir}/MonetDB/perl
%{_datadir}/MonetDB/perl/*

%files client-ruby
%defattr(-,root,root)
%doc %{gemdir}/doc/activerecord-monetdb-adapter-0.1
%doc %{gemdir}/doc/ruby-monetdb-sql-0.1
%{gemdir}/cache/activerecord-monetdb-adapter-0.1.gem
%{gemdir}/cache/ruby-monetdb-sql-0.1.gem
%{gemdir}/gems/activerecord-monetdb-adapter-0.1/
%{gemdir}/gems/ruby-monetdb-sql-0.1/
%{gemdir}/specifications/activerecord-monetdb-adapter-0.1.gemspec
%{gemdir}/specifications/ruby-monetdb-sql-0.1.gemspec

%files client-tests
%defattr(-,root,root)
%dir %{_libdir}/MonetDB/Tests
%{_libdir}/MonetDB/Tests/*

%files geom
%defattr(-,root,root)
%{_datadir}/MonetDB/sql/geom.sql

%files geom-MonetDB4
%defattr(-,root,root)
%{_libdir}/MonetDB4/geom.mil
%{_libdir}/MonetDB4/lib/lib_geom.so*

%files geom-MonetDB5
%defattr(-,root,root)
%{_libdir}/MonetDB5/autoload/*_geom.mal
%{_libdir}/MonetDB5/geom.mal
%{_libdir}/MonetDB5/lib/lib_geom.so*

%files -n MonetDB4-server
%defattr(-,root,root)
%attr(750,monetdb,monetdb) %dir %{_localstatedir}/MonetDB
%attr(2770,monetdb,monetdb) %dir %{_localstatedir}/MonetDB4
%attr(2770,monetdb,monetdb) %dir %{_localstatedir}/MonetDB4/dbfarm
%{_bindir}/Mbeddedmil
%{_bindir}/Mserver
%config(noreplace) %{_sysconfdir}/MonetDB.conf
%dir %{_libdir}/MonetDB4
%dir %{_libdir}/MonetDB4/lib
%if %{?_with_netcdf:1}%{!?_with_netcdf:0}
%exclude %{_libdir}/MonetDB4/lib/lib_mnetcdf.so*
%exclude %{_libdir}/MonetDB4/mnetcdf.mil
%endif
%{_libdir}/libembeddedmil.so.*
%{_libdir}/libmonet.so.*
%{_libdir}/MonetDB4/lib/*.so*
%{_libdir}/MonetDB4/*.mil

%if %{?_with_netcdf:1}%{!?_with_netcdf:0}
%files -n MonetDB4-server-netcdf
%defattr(-,root,root)
%{_includedir}/MonetDB4/mnetcdf/*.[hcm]
%{_libdir}/MonetDB4/lib/lib_mnetcdf.so*
%{_libdir}/MonetDB4/mnetcdf.mil
%endif

%files -n MonetDB5-server
%defattr(-,root,root)
%attr(750,monetdb,monetdb) %dir %{_localstatedir}/MonetDB
%attr(2770,monetdb,monetdb) %dir %{_localstatedir}/MonetDB5
%attr(2770,monetdb,monetdb) %dir %{_localstatedir}/MonetDB5/dbfarm
%{_bindir}/Mbeddedmal
%{_bindir}/mserver5
%config(noreplace) %{_sysconfdir}/monetdb5.conf
%dir %{_libdir}/MonetDB5
%dir %{_libdir}/MonetDB5/autoload
%dir %{_libdir}/MonetDB5/lib
%{_libdir}/*.so.*
%{_mandir}/man5/monetdb5.conf.5.gz

%files -n MonetDB5-server-rdf
%defattr(-,root,root)
%{_libdir}/MonetDB5/autoload/*_rdf.mal
%{_libdir}/MonetDB5/lib/lib_rdf.so*
%{_libdir}/MonetDB5/rdf.mal

%files SQL
%defattr(-,root,root)
%docdir %{_datadir}/doc/MonetDB-SQL-%{version}
%{_datadir}/doc/MonetDB-SQL-%{version}/*

%files SQL-server5
%defattr(-,root,root)
%{_bindir}/Mbeddedsql5
%{_bindir}/monetdb
%{_bindir}/monetdbd
%dir %attr(775,monetdb,monetdb) %{_localstatedir}/log/MonetDB
%dir %attr(775,monetdb,monetdb) %{_localstatedir}/run/MonetDB
%{_libdir}/libembeddedsql5.so.*
%{_libdir}/MonetDB5/autoload/*.mal
%{_libdir}/MonetDB5/lib/lib_*.so.*
%{_libdir}/MonetDB5/*.mal
%{_libdir}/MonetDB5/*.sql
%{_mandir}/man1/monetdb.1.gz
%{_mandir}/man1/monetdbd.1.gz

%files -n MonetDB4-XQuery
%defattr(-,root,root)
%{_bindir}/Mbeddedxq
%{_bindir}/pf
%{_bindir}/pfopt
%{_bindir}/pfshred
%{_bindir}/pfsql
%dir %{_datadir}/MonetDB/xrpc
%dir %{_datadir}/MonetDB/xrpc/admin
%dir %{_datadir}/MonetDB/xrpc/demo
%dir %{_datadir}/MonetDB/xrpc/export
%{_datadir}/MonetDB/xrpc/admin/*
%{_datadir}/MonetDB/xrpc/demo/*
%{_datadir}/MonetDB/xrpc/export/*
%{_libdir}/libembeddedxq.so.*
%{_libdir}/MonetDB4/lib/lib_*
%{_libdir}/MonetDB4/*.mil

%files -n MonetDB4-XQuery-ferry
%defattr(-,root,root)
%{_libdir}/libpf_ferry.so.*

%files -n python-monetdb
%defattr(-,root,root)
%dir %{python_sitelib}/monetdb
%dir %{python_sitelib}/monetdb/sql
%{python_sitelib}/monetdb/*
%{python_sitelib}/python_monetdb-*.egg-info
%doc python/README.txt

%files testing
%defattr(-,root,root)
%{_bindir}/Mdiff
%{_bindir}/MkillUsers
%{_bindir}/Mlog
%{_bindir}/Mtimeout

%files testing-python
%defattr(-,root,root)
# at least F12 doesn't produce these
# %exclude %{_bindir}/*.pyc
# %exclude %{_bindir}/*.pyo
%{_bindir}/Mapprove.py
%{_bindir}/Mfilter.py
%{_bindir}/Mtest.py
%dir %{python_sitelib}/MonetDBtesting
%{python_sitelib}/MonetDBtesting/*

%changelog
* Tue Jan  4 2011 Sjoerd Mullender <sjoerd@acm.org> - 11.0.0-0
- Created top-level bootstrap/configure/make with new version numbers.

