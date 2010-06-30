%define name MonetDB
%define version 1.39.0
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
Source: http://dev.monetdb.org/downloads/sources/Jun2010/%{name}-%{version}.tar.gz
BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

BuildRequires: zlib-devel, bzip2-devel, openssl-devel

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
Requires: %{name} = %{version}-%{release}
Requires: zlib-devel, bzip2-devel, openssl-devel

%description devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the files needed to develop with MonetDB.

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
	%{?comp_cc:CC="%{comp_cc}"}

make

%install
rm -rf $RPM_BUILD_ROOT

make install DESTDIR=$RPM_BUILD_ROOT

# cleanup stuff we don't want to have installed
find $RPM_BUILD_ROOT -name .incs.in -delete -o -name \*.la -delete

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%clean
rm -fr $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%{_libdir}/libmutils.so.*
%{_libdir}/libstream.so.*
%{_libdir}/libbat.so.*

%files devel
%defattr(-,root,root)
%{_bindir}/monetdb-config

%dir %{_includedir}/%{name}
%dir %{_includedir}/%{name}/*
%{_includedir}/%{name}/*.h
%{_includedir}/%{name}/*/*.[hcm]
%{_libdir}/libmutils.so
%{_libdir}/libstream.so
%{_libdir}/libbat.so

%changelog
* Wed Jun 30 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.1-20100630
- Rebuilt.

* Fri Jun 25 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.1-20100625
- Rebuilt.

* Tue Jun 22 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.1-20100622
- Rebuilt.

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

* Thu Apr 15 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.5-20100420
- Fixed a bug that could cause a crash when string BATs are combined.
  (SF bug 2947763.)

* Sun Apr 11 2010 Niels Nes <niels@cwi.nl> - 1.36.5-20100420
- Fixed a potential file leak: under certain conditions, files in the
  database might not get deleted when they should (they would be
  deleted when the server restarts).

* Mon Mar 29 2010 Fabian Groffen <fabian@cwi.nl> - 1.36.5-20100420
- Fix regression introduced in Feb2010-SP1 causing UDP connections to malfunction, in particular affecting the stethoscope tool.

* Mon Mar 22 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.3-20100322
- Rebuilt.

* Mon Mar 01 2010 Fabian Groffen <fabian@cwi.nl> - 1.36.3-20100322
- Fixed bug in UDP stream creation causing UDP connections to already
  bound ports to be reported as successful.
* Wed Feb 24 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.1-20100224
- Rebuilt.

* Mon Feb 22 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.1-20100223
- Various concurrency bugs were fixed.
- Various changes were made to run better on systems that don't have enough
  memory to keep everything in core that was touched during query processing.
  This is done by having the higher layers giving hints to the database
  kernel about future use, and the database kernel giving hings to the
  operating system kernel about how (virtual) memory is going to be used.

* Thu Feb 18 2010 Stefan Manegold <Stefan.Manegold@cwi.nl> - 1.36.1-20100223
- Fixed bug in mergejoin implementation.
  This fixes bug  #2952191.

* Tue Feb  2 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.1-20100223
- Added support for compiling on Windows using the Cygwin-provided
  version of flex.

* Thu Jan 21 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.1-20100223
- Fix compilation issue when configured with --with-curl.
  This fixes bug #2924999.

* Thu Jan 21 2010 Fabian Groffen <fabian@cwi.nl> - 1.36.1-20100223
- Added implementation of MT_getrss() for Solaris.  This yields in the
  kernel knowing about its (approximate) memory usage to try and help
  the operating system to free that memory that is best to free, instead
  of a random page, e.g. the work of the vmtrim thread.

* Wed Jan 20 2010 Sjoerd Mullender <sjoerd@cwi.nl> - 1.36.1-20100223
- Implemented a "fast" string BAT append:
  Under certain conditions, instead of inserting values one-by-one,
  we now concatenate the string heap wholesale and just manipulate
  the offsets.
  This works both for BATins and BATappend.

* Wed Jan  6 2010 Sjoerd Mullender <sjoerd@cwi.nl> - 1.36.1-20100223
- Changed the string heap implementation to also contain the hashes of
  strings.
- Changed the implementation of the string offset columns to be
  dynamically sized.

