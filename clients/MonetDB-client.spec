%define name MonetDB-client
%define version 1.40.3
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
Source: http://dev.monetdb.org/downloads/sources/Oct2010-SP1/MonetDB-client-%{version}.tar.gz
BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

Requires: MonetDB >= 1.40
#                    ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
BuildRequires: readline-devel, openssl-devel

%{!?_with_php: %{!?_without_php: %define _with_php --with-php}}
%{!?_with_perl: %{!?_without_perl: %define _with_perl --with-perl}}
%{!?_with_rubygem: %{!?_without_rubygem: %define _with_rubygem --with-rubygem}}
%{!?_with_unixODBC: %{!?_without_unixODBC: %define _with_unixODBC --with-unixodbc}}

%if %{?_with_rubygem:1}%{!?_with_rubygem:0}
%{!?ruby_sitelib: %global ruby_sitelib %(ruby -rrbconfig -e 'puts Config::CONFIG["sitelibdir"] ')}
%define gemdir %(ruby -rubygems -e 'puts Gem::dir' 2>/dev/null)
%endif

%if !%{?buildsystem}
BuildRequires: MonetDB-devel >= 1.40
#                               ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
%endif

%package devel
Summary: MonetDB Client development package
Group: Applications/Databases
Requires: %{name} = %{version}-%{release}
Requires: MonetDB-devel >= 1.40
#                          ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
Requires: readline-devel, openssl-devel
%if %{?_with_perl:1}%{!?_with_perl:0}
Requires: perl, swig
%if 0%{?fedora} >= 7
# in Fedora Core 6 there is no perl-devel package: everything is in perl;
# in Fedora 7 there is a perl-devel package which we need.
Requires: perl-devel
%endif
%endif
%if %{?_with_rubygem:1}%{!?_with_rubygem:0}
Requires: rubygems, ruby
%endif
%if %{?_with_unixODBC:1}%{!?_with_unixODBC:0}
Requires: unixODBC, unixODBC-devel
%endif

%package odbc
Summary: MonetDB SQL odbc
Group: Applications/Databases
Requires: %{name} = %{version}-%{release}
%if %{?_with_unixODBC:1}%{!?_with_unixODBC:0}
BuildRequires: unixODBC-devel
%endif

%if %{?_with_php:1}%{!?_with_php:0}
%package php
Summary: MonetDB php interface
Group: Applications/Databases
Requires: %{name} = %{version}-%{release}
Requires: php
%endif

%if %{?_with_perl:1}%{!?_with_perl:0}
%package perl
Summary: MonetDB perl interface
Group: Applications/Databases
Requires: %{name} = %{version}-%{release}
Requires: perl
%define perl_libdir %(perl -MConfig -e '$x=$Config{installvendorarch}; $x =~ s|$Config{vendorprefix}/||; print $x;')
%if !%{?buildsystem}
BuildRequires: perl, swig
%if 0%{?fedora} >= 7
# in Fedora Core 6 there is no perl-devel package: everything is in perl;
# in Fedora 7 there is a perl-devel package which we need.
BuildRequires: perl-devel
%endif
%endif
%endif

%if %{?_with_rubygem:1}%{!?_with_rubygem:0}
%package ruby
Summary: MonetDB ruby interface
Group: Applications/Databases
Requires: %{name} = %{version}-%{release}
Requires: ruby(abi) = 1.8
%if !%{?buildsystem}
BuildRequires: ruby
BuildRequires: rubygems
%endif
%if 0%{?fedora} >= 10
BuildArch: noarch
%endif
Provides: rubygem(ruby-monetdb-sql) = 0.1
Provides: rubygem(activerecord-monetdb-adapter) = 0.1
%endif

%package tests
Summary: MonetDB Client tests package
Group: Applications/Databases
Requires: %{name} = %{version}-%{release}
Requires: %{name}-odbc = %{version}-%{release}

%description
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains mclient, the main client program to communicate
with the database server, and msqldump, a program to dump the SQL
database so that it can be loaded back later.

%description devel
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the files needed to develop with
MonetDB-client.

%description odbc
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the ODBC driver.

%if %{?_with_php:1}%{!?_with_php:0}
%description php
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the files needed to use MonetDB from a PHP
program.
%endif

%if %{?_with_perl:1}%{!?_with_perl:0}
%description perl
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the files needed to use MonetDB from a Perl
program.
%endif

%if %{?_with_rubygem:1}%{!?_with_rubygem:0}
%description ruby
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the files needed to use MonetDB from a Ruby
program.
%endif

%description tests
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package contains the sample MAPI programs used for testing other
MonetDB packages.  You probably don't need this, unless you are a
developer.


%prep
rm -rf $RPM_BUILD_ROOT

%setup -q -n MonetDB-client-%{version}

%build

%configure \
	--enable-strict=no \
	--enable-assert=no \
	--enable-debug=no \
	--enable-optimize=yes \
	--enable-bits=%{bits} \
	%{?oid32:--enable-oid32} \
	%{?comp_cc:CC="%{comp_cc}"} \
	%{?_with_php} %{?_without_php} \
	%{?_with_perl} %{?_without_perl} \
	%{?_with_rubygem} %{?_without_rubygem} \
	%{?_with_unixODBC} %{?_without_unixODBC}

make

%if %{?_with_rubygem:1}%{!?_with_rubygem:0}
cd src/ruby
gem build ruby-monetdb-sql-0.1.gemspec
cd adapter
gem build activerecord-monetdb-adapter-0.1.gemspec
cd ../../..
%endif

%install
rm -rf $RPM_BUILD_ROOT

make install DESTDIR=$RPM_BUILD_ROOT

# cleanup stuff we don't want to install
find $RPM_BUILD_ROOT -name .incs.in -print -o -name \*.la -print | xargs rm -f

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%clean
rm -fr $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%{_bindir}/mclient
%{_bindir}/msqldump
%{_bindir}/stethoscope
%{_libdir}/libMapi.so.*
%{_mandir}/man1/mclient.1.gz
%{_mandir}/man1/msqldump.1.gz

%files devel
%defattr(-,root,root)
%{_bindir}/monetdb-clients-config
%{_libdir}/libMapi.so
%dir %{_includedir}/MonetDB/mapilib
%{_includedir}/MonetDB/mapilib/Mapi.h
%{_libdir}/pkgconfig/monetdb-mapi.pc

# todo odbc-devel package ?
%files odbc
%defattr(-,root,root)
%{_libdir}/libMonetODBC.*
%{_libdir}/libMonetODBCs.*

%if %{?_with_php:1}%{!?_with_php:0}
%files php
%defattr(-,root,root)
%dir %{_datadir}/php/monetdb
%{_datadir}/php/monetdb/*
%endif

%if %{?_with_perl:1}%{!?_with_perl:0}
%files perl
%defattr(-,root,root)
%{_prefix}/%{perl_libdir}/*
%dir %{_datadir}/MonetDB/perl
%{_datadir}/MonetDB/perl/*
%endif

%if %{?_with_rubygem:1}%{!?_with_rubygem:0}
%files ruby
%defattr(-,root,root)
%{gemdir}/gems/ruby-monetdb-sql-0.1/
%doc %{gemdir}/doc/ruby-monetdb-sql-0.1
%{gemdir}/cache/ruby-monetdb-sql-0.1.gem
%{gemdir}/specifications/ruby-monetdb-sql-0.1.gemspec
%{gemdir}/gems/activerecord-monetdb-adapter-0.1/
%doc %{gemdir}/doc/activerecord-monetdb-adapter-0.1
%{gemdir}/cache/activerecord-monetdb-adapter-0.1.gem
%{gemdir}/specifications/activerecord-monetdb-adapter-0.1.gemspec
%endif

%files tests
%{_libdir}/MonetDB/Tests/*

%changelog
* Wed Dec 15 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.40.3-20101215
- Rebuilt.

* Fri Nov 19 2010 Fabian Groffen <fabian@cwi.nl> - 1.40.3-20101215
- Workaround usage of strncat to solve mclient/mapilib aborts from
  bug #2725

* Fri Nov 12 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.40.3-20101215
- Fixed a corner case in the table display code.

* Wed Nov 10 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.40.1-20101110
- Rebuilt.

* Tue Nov 09 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.40.1-20101109
- Rebuilt.

* Fri Nov 05 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.40.1-20101105
- Rebuilt.

* Fri Oct 29 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.40.1-20101029
- Rebuilt.

* Thu Oct 28 2010 Fabian Groffen <fabian@cwi.nl> - 1.40.1-20101029
- Fix crash when the server disconnects during \d query

* Wed Oct 27 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.40.1-20101029
- A manual page for the msqldump program was added.
- Mclient now recognizes the file name "-" to refer to its standard
  input.

* Fri Oct  1 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.40.1-20101029
- Save readline history as we go along.  This fixes bug 2632.

* Tue Sep 14 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.40.1-20101029
- The \d and \D commands now require a space if they are followed by a
  table name.  This is to accomodate future expansion where \d and \D
  could be immediately followed by another letter to indicate the type
  of object of interest.
- Implemented dumping of "external" functions.  This fixes bug 2546.

* Fri Sep 10 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.40.1-20101029
- A bug was fixed (bug 2639) where using completion failed with an error
  when using another schema than "sys".

* Thu Aug 26 2010 Fabian Groffen <fabian@cwi.nl> - 1.40.1-20101029
- Empty resultsets now show a header in mclient using SQL mode.

* Tue Aug 24 2010 Fabian Groffen <fabian@cwi.nl> - 1.40.1-20101029
- Allow users to specify their own default formatter (using format=), or
  their default formatting width (using width=) in their .monetdb file.
  Setting the latter option is mostly useful when using the sql
  formatter, such that truncation can be disabled by setting width=-1.
- Rendering of mclient's tabular mode (when using SQL) has been changed
  considerably to get improved output.  Most importantly it now omits
  parts of field values or even full columns when the used terminal width
  is insufficient to display the result.  To disable this behaviour,
  simply set width to -1 (via -w option or \w in mclient), or chose
  another rendering mode.

* Tue Aug 24 2010 Fabian Groffen <fabian@cwi.nl> - 1.40.1-20101029
- In SQL mode, always use the table formatter, as there is no sense to
  use raw (protocol debug) mode when reading from pipe or file really.
  People likely prefer tab (-ftab) formatting in such case for easy
  scripting.

* Tue Aug 24 2010 Fabian Groffen <fabian@cwi.nl> - 1.40.1-20101029
- In SQL (rendering) mode, mclient now returns timings for SELECT and
  UPDATE/INSERT/DELETE queries next to the number of tuples returned or
  rows affected.

* Tue Aug 24 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.5-20100824
- Rebuilt.

* Mon Aug 23 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.5-20100823
- Rebuilt.

* Thu Aug 19 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.5-20100823
- mclient now complains about NULL bytes in the input when in interactive
  mode.

* Fri Aug 13 2010 Fabian Groffen <fabian@cwi.nl> - 1.38.5-20100823
- Slight rendering improvements to mclient's tabular output when
  rendering results larger than the available screen width, headers
  were previously unnecessarily squeezed.
- Fix bug #2650, a too small buffer caused the active database as
  reported by mclient's welcome message to be truncated

* Wed Jul 21 2010 Fabian Groffen <fabian@cwi.nl> - 1.38.5-20100823
- Add --version option to mclient.

* Tue Jul 20 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.5-20100823
- In case of an incomplete line from the server, add a newline.
  This fixes bug 2619.

* Mon Jul 19 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.5-20100823
- Avoid using SQLROWSETSIZE and SQLROWOFFSET.
  This fixes bug 2558.

* Tue Jul 13 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.3-20100713
- Rebuilt.

* Mon Jul 12 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.3-20100712
- Rebuilt.

* Fri Jul 09 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.3-20100709
- Rebuilt.

* Mon Jul  5 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.3-20100706
- Implemented dumping of GRANT statements.
  This fixes bug 2574.

* Thu Jul  1 2010 Fabian Groffen <fabian@cwi.nl> - 1.38.3-20100706
- Fix implementation of mapi_mapiuri to deal with UNIX socket urls
  properly.  This fixes one part of Bug #2567.

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

* Tue Apr 20 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.1-20100618
- The MonetDB ODBC driver now no longer depends on its own copy of the
  standard ODBC include files but instead depends on system include
  files (unixODBC on Linux and Microsoft SDK on Windows).

* Tue Apr 20 2010 Stefan Manegold <manegold@cwi.nl> - 1.38.1-20100618
- Made compilation of "testing" (and "java") independent of MonetDB.
  This is mainly for Windows, but also on other systems, "testing" can
  now be built independently of (and hence before) "MonetDB".  Files
  that mimic configure functionality on Windows were moved from
  "MonetDB" to "buildtools"; hence, this affects all packages on
  Windows, requiring a complete rebuild from scratch on Windows.
  getopt() support in testing has changed; hence, (most probably)
  requiring a rebuild from scratch of testing on other systems.

* Tue Apr 20 2010 Stefan Manegold <manegold@cwi.nl> - 1.38.1-20100618
- Implemented build directory support for Windows, i.e., like on
  Unix/Linux also on Windows we can now build in a separate build
  directory as alternative to ...\<package>\NT, and thus keep the latter
  clean from files generated during the build.  On Windows, the build
  directory must be a sibling of ...\<package>\NT .

* Tue Apr 20 2010 Fabian Groffen <fabian@cwi.nl> - 1.38.1-20100618
- Changed \d output of mclient no longer to list (internal use only)
  system tables.  Administrator users who like to inspect these tables
  can use SELECT * FROM tables; instead

* Tue Apr 20 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.38.1-20100618
- Implemented the changes required for properly running ODBC on a
  64-bit platform.

* Tue Apr 20 2010 Fabian Groffen <fabian@cwi.nl> - 1.38.1-20100618
- Added stethoscope, a utility to profile MonetDB5 instances,
  originally from the MonetDB5 repository.

* Tue Apr 20 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.5-20100420
- Rebuilt.

* Thu Apr 15 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.5-20100420
- Documented the database option in the DSN in the Perl interface.
  Also incremented the version number of the Perl interface.
- Fixed a problem in the PHP interface where under certain
  circumstances not all data from the server was read, leading to a
  limited number of rows being returned and other weird behavior.
  This fixes SF bug 2975433.
- If mclient is called with argument -? or --help, exit with exit code
  0.  This fixes SF bug 2956574.

* Mon Mar 22 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.3-20100322
- Rebuilt.

* Wed Feb 24 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.1-20100224
- Rebuilt.

* Wed Feb 17 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.1-20100223
- Fixed a bug in the Mapi library when sending very large queries to
  the server.
- Implemented BLOB and CLOB support in ODBC driver.

* Tue Feb 02 2010 Fabian Groffen <fabian@cwi.nl> - 1.36.1-20100223
- Fixed crash upon connect to a server that is under maintenance but
  has the requested language (scenario) not loaded.

* Wed Jan 27 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.1-20100223
- Fixed bug in the ODBC driver where a non-ASCII character at the end
  of a string caused an error when the string was converted to wide
  characters.

* Wed Jan 20 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.1-20100223
- mclient timer information (-t option) is now written to standard
  error instead of standard output.  This makes getting timer
  information easier when there is a large amount of regular output
  (which can be redirected to a file or /dev/null).

* Tue Jan 19 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.36.1-20100223
- Implemented new function mapi_fetch_field_len() which returns the
  length (excluding trailing NULL byte) of the field returned by
  mapi_fetch_field().

