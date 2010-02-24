%define name MonetDB4-server
%define version 4.37.0
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
Summary: MonetDB - Monet Database Management System
Vendor: MonetDB BV <monet@cwi.nl>

Group: Applications/Databases
License:   MPL - http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
URL: http://monetdb.cwi.nl/
Source: http://downloads.sourceforge.net/monetdb/MonetDB4-server-%{version}.tar.gz
BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

Requires(pre): shadow-utils
BuildRequires: pcre-devel

# when we want MonetDB to run as system daemon, we need this
# also see the scriptlets below
# the init script should implement start, stop, restart, condrestart, status
# Requires(post): /sbin/chkconfig
# Requires(preun): /sbin/chkconfig
# Requires(preun): /sbin/service
# Requires(postun): /sbin/service

# by default we do not build the netcdf package
%{!?_with_netcdf: %{!?_without_netcdf: %define _without_netcdf --without-netcdf}}

%define builddoc 0

Requires: MonetDB-client >= 1.36
#                           ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
%if !%{?buildsystem}
BuildRequires: MonetDB-devel >= 1.36
#                               ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
BuildRequires: MonetDB-client-devel >= 1.36
#                                      ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
%endif

%if %{?_with_netcdf:1}%{!?_with_netcdf:0}
%package netcdf
Summary: MonetDB4 module for using NetCDF
Group: Applications/Databases
Requires: %{name} = %{version}-%{release}
Requires: netcdf
BuildRequires: netcdf-devel

%description netcdf
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains a module for use with the MonetDB4 server
component to interface to the NetCDF (network Common Data Form)
libraries.
%endif

%package devel
Summary: MonetDB development package
Group: Applications/Databases
Requires: %{name} = %{version}-%{release}
Requires: MonetDB-devel >= 1.36
#                          ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
Requires: MonetDB-client-devel >= 1.36
#                                 ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.

%description
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the MonetDB4 server component.  You need this
package if you want to work using the MIL language, or if you want to
use the XQuery frontend (in which case you need MonetDB4-XQuery as
well).

%description devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the files needed to develop with MonetDB4.

%prep
rm -rf $RPM_BUILD_ROOT

%setup -q -n MonetDB4-server-%{version}

%build

%configure \
	--enable-strict=no \
	--enable-assert=no \
	--enable-debug=no \
	--enable-optimize=yes \
	--enable-bits=%{bits} \
	%{?oid32:--enable-oid32} \
	%{?comp_cc:CC="%{comp_cc}"} \
	%{?_with_netcdf} %{?_without_netcdf}

make

%install
rm -rf $RPM_BUILD_ROOT

make install DESTDIR=$RPM_BUILD_ROOT

mkdir -p $RPM_BUILD_ROOT/%{_localstatedir}/MonetDB
mkdir -p $RPM_BUILD_ROOT/%{_localstatedir}/MonetDB4
# insert example db here!

# cleanup stuff we don't want to install
find $RPM_BUILD_ROOT -name .incs.in -print -o -name \*.la -print | xargs rm -f
rm -rf $RPM_BUILD_ROOT%{_libdir}/MonetDB4/Tests/*

%pre
getent group monetdb >/dev/null || groupadd -r monetdb
getent passwd monetdb >/dev/null || \
useradd -r -g monetdb -d %{_localstatedir}/MonetDB -s /sbin/nologin \
    -c "MonetDB Server" monetdb
exit 0

%post
/sbin/ldconfig

# when we want MonetDB to run as system daemon, we need this
# # This adds the proper /etc/rc*.d links for the script
# /sbin/chkconfig --add monetdb4

# %preun
# if [ $1 = 0 ]; then
# 	/sbin/service monetdb4 stop >/dev/null 2>&1
# 	/sbin/chkconfig --del monetdb4
# fi

%postun
/sbin/ldconfig

# when we want MonetDB to run as system daemon, we need this
# if [ "$1" -ge "1" ]; then
# 	/sbin/service monetdb4 condrestart >/dev/null 2>&1 || :
# fi

%clean
rm -fr $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%{_bindir}/Mserver
%{_bindir}/Mbeddedmil

%{_libdir}/libmonet.so.*
%{_libdir}/libembeddedmil.so.*
%dir %{_libdir}/MonetDB4
%dir %{_libdir}/MonetDB4/lib
%if %{?_with_netcdf:1}%{!?_with_netcdf:0}
%exclude %{_libdir}/MonetDB4/lib/lib_mnetcdf.so*
%exclude %{_libdir}/MonetDB4/mnetcdf.mil
%endif
%{_libdir}/MonetDB4/lib/*.so*
%{_libdir}/MonetDB4/*.mil

%attr(770,monetdb,monetdb) %dir %{_localstatedir}/MonetDB
%attr(770,monetdb,monetdb) %dir %{_localstatedir}/MonetDB4

%config(noreplace) %{_sysconfdir}/MonetDB.conf

%if %{?_with_netcdf:1}%{!?_with_netcdf:0}
%files netcdf
%{_libdir}/MonetDB4/lib/lib_mnetcdf.so*
%{_libdir}/MonetDB4/mnetcdf.mil
%{_includedir}/MonetDB4/mnetcdf/*.[hcm]
%endif

%files devel
%defattr(-,root,root)
%{_bindir}/monetdb4-config
%{_bindir}/calibrator
%{_libdir}/pkgconfig/MonetDB.pc
%dir %{_includedir}/MonetDB4
%{_includedir}/MonetDB4/*/*.[hcm]
%if %{?_with_netcdf:1}%{!?_with_netcdf:0}
%exclude %{_includedir}/MonetDB4/mnetcdf/*.[hcm]
%endif
%{_libdir}/libmonet.so
%{_libdir}/libembeddedmil.so

%changelog
* Wed Feb 24 2010 Sjoerd Mullender <sjoerd@acm.org> - 4.36.1-20100224
- Rebuilt.

