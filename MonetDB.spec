%define name MonetDB
%define version 11.17.0
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

Name: %{name}
Version: %{version}
Release: %{release}
Summary: MonetDB - Monet Database Management System
Vendor: MonetDB BV <info@monetdb.org>

Group: Applications/Databases
License: MPL - http://www.monetdb.org/Legal/MonetDBLicense
URL: http://www.monetdb.org/
Source: http://dev.monetdb.org/downloads/sources/Feb2013-SP6/%{name}-%{version}.tar.bz2

BuildRequires: bison
BuildRequires: bzip2-devel
# BuildRequires: cfitsio-devel
BuildRequires: flex
%if %{?rhel:0}%{!?rhel:1}
# no geos library on RedHat Enterprise Linux and derivatives
BuildRequires: geos-devel >= 2.2.0
%endif
BuildRequires: libcurl-devel
BuildRequires: libuuid-devel
BuildRequires: libxml2-devel
BuildRequires: openssl-devel
BuildRequires: pcre-devel >= 4.5
BuildRequires: perl
BuildRequires: python-devel
%if %{?rhel:0}%{!?rhel:1}
BuildRequires: python3-devel
%endif
# BuildRequires: raptor-devel >= 1.4.16
BuildRequires: readline-devel
BuildRequires: ruby
BuildRequires: rubygems
%if %{?rhel:0}%{!?rhel:1}
BuildRequires: rubygems-devel
%endif
BuildRequires: unixODBC-devel
BuildRequires: zlib-devel

%define perl_libdir %(perl -MConfig -e '$x=$Config{installvendorarch}; $x =~ s|$Config{vendorprefix}/||; print $x;')
# need to define python_sitelib on RHEL 5 and older
# no need to define python3_sitelib: it's defined by python3-devel
%if 0%{?rhel} && 0%{?rhel} <= 5
%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}
%endif
%{!?gem_dir: %global gem_dir %(ruby -rubygems -e 'puts Gem::dir' 2>/dev/null)}

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
Requires: %{name} = %{version}-%{release}

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
Requires: %{name}-stream = %{version}-%{release}
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
Requires: %{name}-client = %{version}-%{release}

%description client-tools
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains stethoscope and tomograph.  These tools can be
used to monitor the MonetDB database server.

%files client-tools
%defattr(-,root,root)
%{_bindir}/stethoscope
%{_bindir}/tomograph

%package client-devel
Summary: MonetDB - Monet Database Management System Client Programs
Group: Applications/Databases
Requires: %{name}-client = %{version}-%{release}
Requires: %{name}-stream-devel = %{version}-%{release}
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
Requires: %{name}-client = %{version}-%{release}
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
Requires: %{name}-client = %{version}-%{release}
Requires: perl
Requires: perl(DBI)
Requires: perl(Digest::SHA)
Requires: perl(Digest::MD5)

%description client-perl
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the files needed to use MonetDB from a Perl
program.

%files client-perl
%defattr(-,root,root)
%{_prefix}/%{perl_libdir}/*

%package -n rubygem-monetdb-sql
Summary: MonetDB ruby interface
Group: Applications/Databases
Requires: ruby
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
%docdir %{gem_dir}/doc/ruby-monetdb-sql-0.1
%{gem_dir}/doc/ruby-monetdb-sql-0.1/*
%{gem_dir}/cache/ruby-monetdb-sql-0.1.gem
# %dir %{gem_dir}/gems/ruby-monetdb-sql-0.1
%{gem_dir}/gems/ruby-monetdb-sql-0.1
%{gem_dir}/specifications/ruby-monetdb-sql-0.1.gemspec

%package -n rubygem-activerecord-monetdb-adapter
Summary: MonetDB ruby interface
Group: Applications/Databases
Requires: ruby
Requires: rubygem-activerecord
Requires: rubygem-monetdb-sql
BuildArch: noarch

%description -n rubygem-activerecord-monetdb-adapter
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the activerecord adapter for MonetDB.

%files -n rubygem-activerecord-monetdb-adapter
%defattr(-,root,root)
%docdir %{gem_dir}/doc/activerecord-monetdb-adapter-0.1
%{gem_dir}/doc/activerecord-monetdb-adapter-0.1/*
%{gem_dir}/cache/activerecord-monetdb-adapter-0.1.gem
# %dir %{gem_dir}/gems/activerecord-monetdb-adapter-0.1
%{gem_dir}/gems/activerecord-monetdb-adapter-0.1
%{gem_dir}/specifications/activerecord-monetdb-adapter-0.1.gemspec

%package client-tests
Summary: MonetDB Client tests package
Group: Applications/Databases
Requires: MonetDB5-server = %{version}-%{release}
Requires: %{name}-client = %{version}-%{release}
Requires: %{name}-client-odbc = %{version}-%{release}
Requires: %{name}-client-perl = %{version}-%{release}
Requires: %{name}-client-php = %{version}-%{release}
Requires: %{name}-SQL-server5 = %{version}-%{release}
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

%if %{?rhel:0}%{!?rhel:1}
%package geom-MonetDB5
Summary: MonetDB5 SQL GIS support module
Group: Applications/Databases
Requires: MonetDB5-server = %{version}-%{release}
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

%package jaql
Summary: MonetDB5 JAQL
Group: Applications/Databases
Requires: MonetDB5-server = %{version}-%{release}

%description jaql
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the JAQL extension for MonetDB.  JAQL is a
querly language for JavaScript Object Notation (JSON).

%files jaql
%defattr(-,root,root)
%{_libdir}/monetdb5/autoload/*_jaql.mal
%{_libdir}/monetdb5/jaql*.mal
%{_libdir}/monetdb5/json*.mal
%{_libdir}/monetdb5/lib_jaql.so
%{_libdir}/monetdb5/lib_json.so

%package -n MonetDB5-server
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Requires(pre): shadow-utils
Requires: %{name}-client = %{version}-%{release}
Obsoletes: MonetDB5-server-rdf

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
%if %{?rhel:0}%{!?rhel:1}
%exclude %{_libdir}/monetdb5/geom.mal
%endif
# %exclude %{_libdir}/monetdb5/rdf.mal
%exclude %{_libdir}/monetdb5/sql.mal
%exclude %{_libdir}/monetdb5/jaql*.mal
%exclude %{_libdir}/monetdb5/json*.mal
%{_libdir}/monetdb5/*.mal
# %{_libdir}/monetdb5/autoload/*_fits.mal
%{_libdir}/monetdb5/autoload/*_lsst.mal
%{_libdir}/monetdb5/autoload/*_opt_sql_append.mal
%{_libdir}/monetdb5/autoload/*_udf.mal
%{_libdir}/monetdb5/autoload/*_vault.mal
%if %{?rhel:0}%{!?rhel:1}
%exclude %{_libdir}/monetdb5/lib_geom.so
%endif
# %exclude %{_libdir}/monetdb5/lib_rdf.so
%exclude %{_libdir}/monetdb5/lib_sql.so
%exclude %{_libdir}/monetdb5/lib_jaql.so
%exclude %{_libdir}/monetdb5/lib_json.so
%{_libdir}/monetdb5/*.so
%doc %{_mandir}/man1/mserver5.1.gz

%package -n MonetDB5-server-devel
Summary: MonetDB development files
Group: Applications/Databases
Requires: MonetDB5-server = %{version}-%{release}

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

# %package -n MonetDB5-server-rdf
# Summary: MonetDB RDF interface
# Group: Applications/Databases
# Requires: MonetDB5-server = %{version}-%{release}

# %description -n MonetDB5-server-rdf
# MonetDB is a database management system that is developed from a
# main-memory perspective with use of a fully decomposed storage model,
# automatic index management, extensibility of data types and search
# accelerators.  It also has an SQL frontend.

# This package contains the MonetDB5 RDF module.

# %files -n MonetDB5-server-rdf
# %defattr(-,root,root)
# %{_libdir}/monetdb5/autoload/*_rdf.mal
# %{_libdir}/monetdb5/lib_rdf.so
# %{_libdir}/monetdb5/rdf.mal
# %{_libdir}/monetdb5/createdb/*_rdf.sql

%package SQL-server5
Summary: MonetDB5 SQL server modules
Group: Applications/Databases
Requires: MonetDB5-server = %{version}-%{release}
%if %{?rhel:0}%{!?rhel:1}
# for systemd-tmpfiles
Requires: systemd-units
%endif
Obsoletes: MonetDB-SQL-devel
Obsoletes: %{name}-SQL

%description SQL-server5
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators.  It also has an SQL frontend.

This package contains the SQL frontend for MonetDB.  If you want to
use SQL with MonetDB, you will need to install this package.

%if %{?rhel:0}%{!?rhel:1}
%post SQL-server5
systemd-tmpfiles --create %{_sysconfdir}/tmpfiles.d/monetdbd.conf
%endif

%files SQL-server5
%defattr(-,root,root)
%{_bindir}/monetdb
%{_bindir}/monetdbd
%dir %attr(775,monetdb,monetdb) %{_localstatedir}/log/monetdb
%if %{?rhel:0}%{!?rhel:1}
# Fedora 15 and newer
%{_sysconfdir}/tmpfiles.d/monetdbd.conf
%else
# RedHat Enterprise Linux
%dir %attr(775,monetdb,monetdb) %{_localstatedir}/run/monetdb
%exclude %{_sysconfdir}/tmpfiles.d/monetdbd.conf
%endif
%config(noreplace) %{_localstatedir}/monetdb5/dbfarm/.merovingian_properties
%{_libdir}/monetdb5/autoload/*_sql.mal
%{_libdir}/monetdb5/lib_sql.so
%{_libdir}/monetdb5/*.sql
%dir %{_libdir}/monetdb5/createdb
%if %{?rhel:0}%{!?rhel:1}
%exclude %{_libdir}/monetdb5/createdb/*_geom.sql
%endif
# %exclude %{_libdir}/monetdb5/createdb/*_rdf.sql
%{_libdir}/monetdb5/createdb/*
%{_libdir}/monetdb5/sql*.mal
%doc %{_mandir}/man1/monetdb.1.gz
%doc %{_mandir}/man1/monetdbd.1.gz
%if (0%{?fedora} >= 20)
%docdir %{_datadir}/doc/MonetDB-SQL
%{_datadir}/doc/MonetDB-SQL/*
%else
%docdir %{_datadir}/doc/MonetDB-SQL-%{version}
%{_datadir}/doc/MonetDB-SQL-%{version}/*
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
%{_bindir}/Mfilter.py
%{_bindir}/Mtest.py
%dir %{python_sitelib}/MonetDBtesting
%{python_sitelib}/MonetDBtesting/*

%prep
%setup -q

%build

%{configure} \
	--enable-assert=no \
	--enable-bits=%{bits} \
	--enable-console=yes \
	--enable-datacell=no \
	--enable-debug=no \
	--enable-developer=no \
	--enable-fits=no \
	--enable-gdk=yes \
	--enable-geom=%{?rhel:no}%{!?rhel:yes} \
	--enable-instrument=no \
	--enable-jaql=yes \
	--enable-jdbc=no \
	--enable-merocontrol=no \
	--enable-monetdb5=yes \
	--enable-odbc=yes \
	--enable-oid32=%{?oid32:yes}%{!?oid32:no} \
	--enable-optimize=yes \
	--enable-profile=no \
	--enable-rdf=no \
	--enable-sql=yes \
	--enable-strict=no \
	--enable-testing=yes \
	--with-ant=no \
	--with-bz2=yes \
	--with-geos=%{?rhel:no}%{!?rhel:yes} \
	--with-hwcounters=no \
	--with-java=no \
	--with-mseed=no \
	--with-perl=yes \
	--with-pthread=yes \
	--with-python2=yes \
	--with-python3=%{?rhel:no}%{!?rhel:yes} \
	--with-readline=yes \
	--with-rubygem=yes \
	--with-rubygem-dir="%{gem_dir}" \
	--with-sphinxclient=no \
	--with-unixodbc=yes \
	--with-valgrind=no \
	%{?comp_cc:CC="%{comp_cc}"}

make

%install
rm -rf $RPM_BUILD_ROOT

%makeinstall

mkdir -p $RPM_BUILD_ROOT%{_localstatedir}/MonetDB
mkdir -p $RPM_BUILD_ROOT%{_localstatedir}/monetdb5/dbfarm
mkdir -p $RPM_BUILD_ROOT%{_localstatedir}/log/monetdb
mkdir -p $RPM_BUILD_ROOT%{_localstatedir}/run/monetdb

# remove unwanted stuff
# .la files
rm -f $RPM_BUILD_ROOT%{_libdir}/*.la
rm -f $RPM_BUILD_ROOT%{_libdir}/monetdb5/*.la
# internal development stuff
rm -f $RPM_BUILD_ROOT%{_bindir}/Maddlog

%if 0%{?fedora} >= 20
mv $RPM_BUILD_ROOT%{_datadir}/doc/MonetDB-SQL-%{version} $RPM_BUILD_ROOT%{_datadir}/doc/MonetDB-SQL
%endif

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%clean
rm -fr $RPM_BUILD_ROOT

%changelog
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

