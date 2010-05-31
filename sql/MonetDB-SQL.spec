%define name MonetDB-SQL
%define version 2.39.0
%{!?buildno: %define buildno %(date +%Y%m%d)}
%define release %{buildno}%{?dist}%{?oid32:.oid32}%{!?oid32:.oid%{bits}}

# groups of related archs
%define all_x86 i386 i586 i686

%ifarch %{all_x86}
%define bits 32
%else
%define bits 64
%endif

# buildsystem is set to 1 when building an rpm from within the build
# directory; it should be set to 0 (or not set) when building a proper
# rpm
%{!?buildsystem: %define buildsystem 0}

Name: %{name}
Version: %{version}
Release: %{release}
Summary: MonetDB SQL - Monet Database Management System
Vendor: MonetDB BV <info@monetdb.org>

Group: Applications/Databases
License:   MPL - http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
URL: http://monetdb.cwi.nl/
Source: http://downloads.sourceforge.net/monetdb/MonetDB-SQL-%{version}.tar.gz
BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

BuildRequires: e2fsprogs-devel

%if !%{?buildsystem}
BuildRequires: MonetDB-devel >= 1.38
#                               ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
BuildRequires: MonetDB-client-devel >= 1.38
#                                      ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
%endif

%description
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains some common files for the %{name}-server5 package.
You really need to install %{name}-server5.

%package server5
Summary: MonetDB5 SQL server modules
Group: Applications/Databases
Requires: MonetDB5-server >= 5.20
#                            ^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
%if !%{?buildsystem}
BuildRequires: MonetDB5-server-devel >= 5.20
#                                       ^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
%endif

%description server5
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the SQL frontend for MonetDB5.


%package devel
Summary: MonetDB SQL development package
Group: Applications/Databases
Requires: %{name}-server5
Requires: MonetDB5-server-devel
Requires: MonetDB-devel
Requires: MonetDB-client-devel

%description devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the files needed to develop with %{name}.

%prep
rm -rf $RPM_BUILD_ROOT

%setup -q -n MonetDB-SQL-%{version}

%build

%configure \
	--enable-strict=no \
	--enable-assert=no \
	--enable-debug=no \
	--enable-optimize=yes \
	--enable-bits=%{bits} \
	%{?oid32:--enable-oid32} \
	%{?comp_cc:CC="%{comp_cc}"} \
	--with-monetdb=%{_prefix} \
	--with-clients=%{_prefix}

make

%install
rm -rf $RPM_BUILD_ROOT

make install DESTDIR=$RPM_BUILD_ROOT

# cleanup stuff we don't want to install
#find $RPM_BUILD_ROOT -name .incs.in | xargs rm
find $RPM_BUILD_ROOT -name \*.la | xargs rm -f
rm -rf $RPM_BUILD_ROOT%{_prefix}/lib*/python*/site-packages
mkdir -p $RPM_BUILD_ROOT/%{_localstatedir}/log/MonetDB $RPM_BUILD_ROOT/%{_localstatedir}/run/MonetDB

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%dir %{_datadir}/doc/%{name}-%{version}
%{_datadir}/doc/%{name}-%{version}/*

%files server5
%defattr(-,root,root)
%{_bindir}/Mbeddedsql5
%{_libdir}/libembeddedsql5.so.*
%{_libdir}/MonetDB5/lib/lib_sql*
%{_libdir}/MonetDB5/*.mal
%{_libdir}/MonetDB5/*.sql
%{_libdir}/MonetDB5/autoload/*_sql.mal
%dir %{_includedir}/MonetDB5/sql
%{_includedir}/MonetDB5/sql/embeddedclient.h
%{_bindir}/merovingian
%{_bindir}/monetdb
%{_mandir}/man1/monetdb.1.gz
%{_mandir}/man1/merovingian.1.gz
%dir %attr(775,monetdb,monetdb) %{_localstatedir}/log/MonetDB
%dir %attr(775,monetdb,monetdb) %{_localstatedir}/run/MonetDB

%files devel
%defattr(-,root,root)
%{_libdir}/pkgconfig/MonetDB-SQL.pc
%{_bindir}/monetdb-sql-config
%{_libdir}/libembeddedsql5.so

%changelog
* Tue Apr 20 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.36.5-20100420
- Rebuilt.

* Thu Apr  8 2010 Stefan Manegold <manegold@cwi.nl> - 2.36.5-20100420
- fixed bug #2983773 "SQL: minimal optimizer pipe unstable"
  https://sourceforge.net/tracker/index.php?func=detail&aid=2983773&group_id=56967&atid=482468

* Mon Mar 22 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.36.3-20100322
- Rebuilt.

* Wed Feb 24 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.36.1-20100224
- Rebuilt.

