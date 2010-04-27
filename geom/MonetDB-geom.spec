%define name MonetDB-geom
%define version 0.16.6
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
Summary: MonetDB Geom - Monet Database Management System
Vendor: MonetDB BV <monet@cwi.nl>

Group: Applications/Databases
License: MPL - http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
URL: http://monetdb.cwi.nl/
Source: http://downloads.sourceforge.net/monetdb/MonetDB-geom-%{version}.tar.gz
BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

%{!?_with_monetdb4: %{!?_without_monetdb4: %define _without_monetdb4 --without-monetdb4}}
%{!?_with_monetdb5: %{!?_without_monetdb5: %define _with_monetdb5 --with-monetdb5}}

%if !%{?buildsystem}
BuildRequires: MonetDB-devel >= 1.36
#                               ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
BuildRequires: geos-devel
%endif

%description
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the common parts of the GIS (Geographic
Information System) extensions for MonetDB-SQL.

%if %{?_with_monetdb4:1}%{!?_with_monetdb4:0}
%package MonetDB4
Summary: MonetDB4 SQL GIS modules
Group: Applications/Databases
Requires: MonetDB4-server >= 4.36
#                            ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
%if !%{?buildsystem}
BuildRequires: MonetDB4-server-devel >= 4.36
#                                       ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
%endif

%description MonetDB4
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automaticautomatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the GIS (Geographic Information System)
extensions for MonetDB-SQL-server4.

%endif

%if %{?_with_monetdb5:1}%{!?_with_monetdb5:0}
%package MonetDB5
Summary: MonetDB5 SQL GIS modules
Group: Applications/Databases
Requires: MonetDB5-server >= 5.18
#                            ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
%if !%{?buildsystem}
BuildRequires: MonetDB5-server-devel >= 5.18
#                                       ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
%endif

%description MonetDB5
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automaticautomatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the GIS (Geographic Information System)
extensions for MonetDB-SQL-server5.

%endif

%package devel
Summary: MonetDB SQL GIS development package
Group: Applications/Databases
%if %{?_with_monetdb5:1}%{!?_with_monetdb5:0}
Requires: %{name}-MonetDB5
Requires: MonetDB5-server-devel
%endif
Requires: MonetDB-devel
Requires: geos-devel

%description devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the files needed to develop with %{name}.

%prep
rm -rf $RPM_BUILD_ROOT

%setup -q -n MonetDB-geom-%{version}

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
	%{?_with_monetdb4} %{?_without_monetdb4} \
	%{?_with_monetdb5} %{?_without_monetdb5}

make

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT

# cleanup stuff we don't want to install
#find $RPM_BUILD_ROOT -name .incs.in | xargs rm
find $RPM_BUILD_ROOT -name \*.la | xargs rm -f

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%{_datadir}/MonetDB/sql/geom.sql
%if %{?_with_monetdb4:1}%{!?_with_monetdb4:0}
%files MonetDB4
%{_libdir}/MonetDB4/lib/lib_geom.so*
%{_libdir}/MonetDB4/geom.mil
%endif
%if %{?_with_monetdb5:1}%{!?_with_monetdb5:0}
%files MonetDB5
%{_libdir}/MonetDB5/lib/lib_geom.so*
%{_libdir}/MonetDB5/geom.mal
%endif

%files devel
%{_bindir}/monetdb-geom-config
%if %{?_with_monetdb5:1}%{!?_with_monetdb5:0}
%endif

%changelog
* Tue Apr 20 2010 Sjoerd Mullender <sjoerd@acm.org> - 0.16.5-20100420
- Rebuilt.

* Mon Mar 22 2010 Sjoerd Mullender <sjoerd@acm.org> - 0.16.3-20100322
- Rebuilt.

* Wed Feb 24 2010 Sjoerd Mullender <sjoerd@acm.org> - 0.16.1-20100224
- Rebuilt.

