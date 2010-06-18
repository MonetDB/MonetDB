%define name MonetDB-testing
%define version 1.38.1
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
Vendor: MonetDB BV <info@monetdb.org>

Group: Applications/Databases
License: MPL - http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
URL: http://monetdb.cwi.nl/
Source: http://downloads.sourceforge.net/monetdb/MonetDB-testing-%{version}.tar.gz
BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

Obsoletes: MonetDB-python
Requires: MonetDB-devel, MonetDB-client-devel, MonetDB4-server-devel, MonetDB5-server-devel, MonetDB-SQL-devel, MonetDB4-XQuery-devel, MonetDB-geom-devel

BuildRequires: python

%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}

%description
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the programs and files needed for testing the
MonetDB packages.  You probably don't need this, unless you are a
developer.

%prep
rm -rf $RPM_BUILD_ROOT

%setup -q -n %{name}-%{version}

%build

%{configure} \
	--enable-strict=no \
	--enable-assert=no \
	--enable-debug=no \
	--enable-optimize=yes \
	--enable-bits=%{bits} \
	%{?oid32:--enable-oid32} \
	%{?comp_cc:CC="%{comp_cc}"} \
	%{?_with_php} %{?_without_php} \
	%{?_with_python} %{?_without_python} \
	%{?_with_perl} %{?_without_perl} \
	%{?_with_java} %{?_without_java}

make

%install
rm -rf $RPM_BUILD_ROOT

make install DESTDIR=$RPM_BUILD_ROOT

# cleanup stuff we don't want to have installed
find $RPM_BUILD_ROOT -name .incs.in -delete -o -name \*.la -delete

# Make sure that these .pyc and .pyo files exist so we can exclude
# them.  Not all versions of Fedora generate these files during the
# build process, and so the exclude would otherwise generate an error
# which we now avoid (note, the generation of .pyc and .pyo files
# happens in a separate pass after the install).
for i in $RPM_BUILD_ROOT%{_bindir}/*.py; do touch ${i}c ${i}o; done

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%clean
rm -fr $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%exclude %{_bindir}/*.pyc
%exclude %{_bindir}/*.pyo
%{_bindir}/monetdb-testing-config
%{_bindir}/Mtest.py
%{_bindir}/Mapprove.py
%{_bindir}/Mdiff
%{_bindir}/Mfilter.py
%{_bindir}/MkillUsers
%{_bindir}/Mlog
%{_bindir}/Mtimeout
%dir %{python_sitelib}/MonetDBtesting
%exclude %{python_sitelib}/MonetDBtesting/*.pyc
%exclude %{python_sitelib}/MonetDBtesting/*.pyo
%{python_sitelib}/MonetDBtesting/__init__.py
%{python_sitelib}/MonetDBtesting/monet_options.py
%{python_sitelib}/MonetDBtesting/process.py
%{python_sitelib}/MonetDBtesting/trace.py
%{python_sitelib}/MonetDBtesting/subprocess26.py

%changelog
* Fri Jun 18 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.1-20100618
- Rebuilt.

* Mon May 31 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.1-20100618
- Updated Vendor information.

* Tue Apr 20 2010 Stefan Manegold <manegold@cwi.nl> - 1.38.1-20100618
- Made compilation of "testing" (and "java") independent of MonetDB.
  This is mainy for Windows, but also on other systems, "testing" can now be
  built independently of (and hence before) "MonetDB".
  Files that mimic configure functionality on Windows were moved from
  "MonetDB" to "buildtools"; hence, this affects all packages on Windows,
  requiring a complete rebuild from scratch on Windows.
  getopt() support in testing has changed; hence, (most probably) requiring a
  rebuild from scratch of testing on other systems.

* Tue Apr 20 2010 Stefan Manegold <manegold@cwi.nl> - 1.38.1-20100618
- Implemented build directory support for Windows,
  i.e., like on Unix/Linux also on Windows we can now build in a separate
  build directory as alternative to ...<package>NT, and thus keep the
  latter clean from files generated during the build.
  On Windows, the build directory must be a sibling of ...<package>NT .

* Tue Apr 20 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.5-20100420
- Rebuilt.

* Mon Mar 22 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.3-20100322
- Rebuilt.

* Wed Feb 24 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.1-20100224
- Rebuilt.

