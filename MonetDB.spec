%define name MonetDB
%define version 11.4.0
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

%define release %{buildno}%{?dist}%{?oidsuf:.oidsuf}

Name: %{name}
Version: %{version}
Release: %{release}
Summary: MonetDB - Monet Database Management System
Vendor: MonetDB BV <info@monetdb.org>

Group: Applications/Databases
License: MPL - http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
URL: http://monetdb.cwi.nl/
Source: http://dev.monetdb.org/downloads/sources/Apr2011-SP1/%{name}-%{version}.tar.bz2

BuildRequires: bison
BuildRequires: bzip2-devel
# BuildRequires: cfitsio-devel
BuildRequires: flex
BuildRequires: geos-devel >= 2.2.0
BuildRequires: libcurl-devel
BuildRequires: libuuid-devel
BuildRequires: libxml2-devel
BuildRequires: openssl-devel >= 0.9.8f
BuildRequires: pcre-devel >= 4.5
BuildRequires: perl
BuildRequires: perl-devel
BuildRequires: python
# BuildRequires: raptor-devel >= 1.4.16
BuildRequires: readline-devel
BuildRequires: ruby
BuildRequires: rubygems
BuildRequires: swig
BuildRequires: unixODBC-devel
BuildRequires: zlib-devel

Obsoletes: %{name}-devel

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

This package contains the core components of MonetDB in the form of a
single shared library.  If you want to use MonetDB, you will certainly
need this package, but you will also need one of the server packages.

%files
%defattr(-,root,root)
%{_libdir}/libbat.so.*

%package stream
Summary: MonetDB stream library
Group: Applications/Databases

%description stream
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

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
accelerators, SQL- and XML- frontends.

This package contains the files to develop with the %{name}-stream
library.

%files stream-devel
%defattr(-,root,root)
%dir %{_includedir}/monetdb
%{_libdir}/libstream.so
%{_libdir}/libstream.la
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
accelerators, SQL- and XML- frontends.

This package contains mclient, the main client program to communicate
with the database server, and msqldump, a program to dump the SQL
database so that it can be loaded back later.  If you want to use
MonetDB, you will very likely need this package.

%files client
%defattr(-,root,root)
%{_bindir}/mclient
%{_bindir}/msqldump
%{_bindir}/stethoscope
%{_libdir}/libmapi.so.*
%doc %{_mandir}/man1/mclient.1.gz
%doc %{_mandir}/man1/msqldump.1.gz

%package client-devel
Summary: MonetDB - Monet Database Management System Client Programs
Group: Applications/Databases
Requires: %{name}-client = %{version}-%{release}
Requires: %{name}-stream-devel = %{version}-%{release}
Requires: openssl-devel >= 0.9.8f

%description client-devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the files needed to develop with the
%{name}-client package.

%files client-devel
%defattr(-,root,root)
%dir %{_includedir}/monetdb
%{_libdir}/libmapi.so
%{_libdir}/libmapi.la
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
accelerators, SQL- and XML- frontends.

This package contains the MonetDB ODBC driver.

%post client-odbc
# install driver if first install of package or if driver not installed yet
if [ "$1" -eq 1 ] || ! grep -q MonetDB /etc/odbcinst.ini; then
odbcinst -i -d -r <<EOF
[MonetDB]
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
accelerators, SQL- and XML- frontends.

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

%description client-perl
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the files needed to use MonetDB from a Perl
program.

%files client-perl
%defattr(-,root,root)
%{_prefix}/%{perl_libdir}/*
%dir %{_datadir}/monetdb/perl
%{_datadir}/monetdb/perl/*

%package client-ruby
Summary: MonetDB ruby interface
Group: Applications/Databases
Requires: ruby
BuildArch: noarch

%description client-ruby
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the files needed to use MonetDB from a Ruby
program.

%files client-ruby
%defattr(-,root,root)
%docdir %{gemdir}/doc/activerecord-monetdb-adapter-0.1
%docdir %{gemdir}/doc/ruby-monetdb-sql-0.1
%{gemdir}/doc/activerecord-monetdb-adapter-0.1/*
%{gemdir}/doc/ruby-monetdb-sql-0.1/*
%{gemdir}/cache/*.gem
%dir %{gemdir}/gems/activerecord-monetdb-adapter-0.1
%dir %{gemdir}/gems/ruby-monetdb-sql-0.1
%{gemdir}/gems/activerecord-monetdb-adapter-0.1
%{gemdir}/gems/ruby-monetdb-sql-0.1
%{gemdir}/specifications/*.gemspec

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
accelerators, SQL- and XML- frontends.

This package contains the sample MAPI programs used for testing other
MonetDB packages.  You probably don't need this, unless you are a
developer.

%files client-tests
%defattr(-,root,root)
# %{_bindir}/odbcsample1
%{_bindir}/sample0
%{_bindir}/sample1
%{_bindir}/sample2
%{_bindir}/sample3
%{_bindir}/sample4
%{_bindir}/smack00
%{_bindir}/smack01
# %{_bindir}/testgetinfo
%{_bindir}/malsample.pl
%{_bindir}/sqlsample.php
%{_bindir}/sqlsample.pl
%{_bindir}/sqlsample.py

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
accelerators, SQL- and XML- frontends.

This package contains the GIS (Geographic Information System)
extensions for MonetDB-SQL-server5.

%files geom-MonetDB5
%defattr(-,root,root)
%{_libdir}/monetdb5/autoload/*_geom.mal
%{_libdir}/monetdb5/createdb/*_geom.sql
%{_libdir}/monetdb5/geom.mal
%{_libdir}/monetdb5/lib_geom.so

%package -n MonetDB5-server
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Requires(pre): shadow-utils
Requires: %{name}-client = %{version}-%{release}
Obsoletes: MonetDB5-server-devel
Obsoletes: MonetDB5-server-rdf

%description -n MonetDB5-server
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the MonetDB5 server component.  You need this
package if you want to work using the MAL language, or if you want to
use the SQL frontend (in which case you need MonetDB-SQL-server5 as
well).

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
%exclude %{_libdir}/monetdb5/geom.mal
# %exclude %{_libdir}/monetdb5/rdf.mal
%exclude %{_libdir}/monetdb5/sql.mal
%exclude %{_libdir}/monetdb5/sql_bpm.mal
%{_libdir}/monetdb5/*.mal
# %{_libdir}/monetdb5/autoload/*_fits.mal
%{_libdir}/monetdb5/autoload/*_vault.mal
%{_libdir}/monetdb5/autoload/*_lsst.mal
%{_libdir}/monetdb5/autoload/*_udf.mal
%exclude %{_libdir}/monetdb5/lib_geom.so
# %exclude %{_libdir}/monetdb5/lib_rdf.so
%exclude %{_libdir}/monetdb5/lib_sql.so
%{_libdir}/monetdb5/*.so
%doc %{_mandir}/man1/mserver5.1.gz

# %package -n MonetDB5-server-rdf
# Summary: MonetDB RDF interface
# Group: Applications/Databases
# Requires: MonetDB5-server = %{version}-%{release}

# %description -n MonetDB5-server-rdf
# MonetDB is a database management system that is developed from a
# main-memory perspective with use of a fully decomposed storage model,
# automatic index management, extensibility of data types and search
# accelerators, SQL- and XML- frontends.

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
Obsoletes: MonetDB-SQL-devel
Obsoletes: %{name}-SQL

%description SQL-server5
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the SQL frontend for MonetDB5.  If you want to
use SQL with MonetDB, you will need to install this package.

%files SQL-server5
%defattr(-,root,root)
%{_bindir}/monetdb
%{_bindir}/monetdbd
%dir %attr(775,monetdb,monetdb) %{_localstatedir}/log/monetdb
%dir %attr(775,monetdb,monetdb) %{_localstatedir}/run/monetdb
%config(noreplace) %{_localstatedir}/monetdb5/dbfarm/.merovingian_properties
%{_libdir}/monetdb5/autoload/*_sql.mal
%{_libdir}/monetdb5/lib_sql.so
%{_libdir}/monetdb5/*.sql
%dir %{_libdir}/monetdb5/createdb
%exclude %{_libdir}/monetdb5/createdb/*_geom.sql
# %exclude %{_libdir}/monetdb5/createdb/*_rdf.sql
%{_libdir}/monetdb5/createdb/*
%{_libdir}/monetdb5/sql*.mal
%doc %{_mandir}/man1/monetdb.1.gz
%doc %{_mandir}/man1/monetdbd.1.gz
%docdir %{_datadir}/doc/MonetDB-SQL-%{version}
%{_datadir}/doc/MonetDB-SQL-%{version}/*

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

%files -n python-monetdb
%defattr(-,root,root)
%dir %{python_sitelib}/monetdb
%{python_sitelib}/monetdb/*
%{python_sitelib}/python_monetdb-*.egg-info
%doc clients/python/README.rst

%package testing
Summary: MonetDB - Monet Database Management System
Group: Applications/Databases
Obsoletes: MonetDB-python

%description testing
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

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
accelerators, SQL- and XML- frontends.

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
	--enable-strict=no \
	--enable-assert=no \
	--enable-debug=no \
	--enable-optimize=yes \
	--enable-bits=%{bits} \
	--enable-jdbc=no \
	--enable-merocontrol=no \
	--enable-rdf=no \
	--enable-fits=no \
	--with-valgrind=no \
	--with-mseed=no \
	%{?oid32:--enable-oid32} \
	%{?comp_cc:CC="%{comp_cc}"} \
	%{?_with_netcdf} %{?_without_netcdf}

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
rm -f $RPM_BUILD_ROOT%{_libdir}/monetdb5/*.la
# internal development stuff
rm -f $RPM_BUILD_ROOT%{_bindir}/calibrator
rm -f $RPM_BUILD_ROOT%{_bindir}/Maddlog
rm -f $RPM_BUILD_ROOT%{_libdir}/libbat.la
rm -f $RPM_BUILD_ROOT%{_libdir}/libbat.so
rm -f $RPM_BUILD_ROOT%{_libdir}/libMonetODBC*.la
rm -f $RPM_BUILD_ROOT%{_libdir}/libmonet.la
rm -f $RPM_BUILD_ROOT%{_libdir}/libmonet.so
rm -f $RPM_BUILD_ROOT%{_libdir}/libmonetdb5.la
rm -f $RPM_BUILD_ROOT%{_libdir}/libmonetdb5.so

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%clean
rm -fr $RPM_BUILD_ROOT

%changelog
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

