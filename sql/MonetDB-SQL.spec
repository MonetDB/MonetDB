%define name MonetDB-SQL
%define version 2.41.0
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
Source: http://dev.monetdb.org/downloads/sources/Oct2010/MonetDB-SQL-%{version}.tar.gz
BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

BuildRequires: e2fsprogs-devel
BuildRequires: pcre-devel

%if !%{?buildsystem}
BuildRequires: MonetDB-devel >= 1.40
#                               ^^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
BuildRequires: MonetDB-client-devel >= 1.40
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
Requires: MonetDB5-server >= 5.22
#                            ^^^
# Maintained via vertoo. Please don't modify by hand!
# Contact MonetDB-developers@lists.sourceforge.net for details and/or assistance.
%if !%{?buildsystem}
BuildRequires: MonetDB5-server-devel >= 5.22
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
%{_libdir}/MonetDB5/lib/lib_*.so.*
%{_libdir}/MonetDB5/*.mal
%{_libdir}/MonetDB5/*.sql
%{_libdir}/MonetDB5/autoload/*.mal
%{_bindir}/monetdbd
%{_bindir}/monetdb
%{_mandir}/man1/monetdb.1.gz
%{_mandir}/man1/monetdbd.1.gz
%dir %attr(775,monetdb,monetdb) %{_localstatedir}/log/MonetDB
%dir %attr(775,monetdb,monetdb) %{_localstatedir}/run/MonetDB

%files devel
%defattr(-,root,root)
%{_bindir}/monetdb-sql-config
%{_libdir}/libembeddedsql5.so
%{_libdir}/MonetDB5/lib/lib_*.so
%{_libdir}/pkgconfig/monetdb-embeddedsql.pc
%dir %{_includedir}/MonetDB5/sql
%{_includedir}/MonetDB5/sql/*.h

%changelog
* Tue Nov 09 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.40.1-20101109
- Rebuilt.

* Fri Nov 05 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.40.1-20101105
- Rebuilt.

* Fri Oct 29 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.40.0-20101029
- Rebuilt.

* Thu Oct 21 2010 Fabian Groffen <fabian@cwi.nl> - 2.40.0-20101029
- Report a stopped and locked database as under maintenance, instead of
  without connections, bug #2685

* Mon Oct 18 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.40.0-20101029
- Fixed bug 2695: crash when starting mserver in readonly mode on a
  new database.

* Wed Sep 22 2010 Fabian Groffen <fabian@cwi.nl> - 2.40.0-20101029
- Improved uuid detection, to solve problems like bug #2675

* Fri Sep 17 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.40.0-20101029
- Fixed a bug where the server silently ignored the last record in a
  COPY INTO if it was incomplete (e.g. missing a quote).

* Mon Aug 30 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.40.0-20101029
- Fixed a crash of the server when an extremely complex query is
  attempted.  This is the latest incarnation of bug 104.

* Tue Aug 24 2010 Arjen de Rijke <arjen.de.rijke@cwi.nl> - 2.40.0-20101029
- Add readonly property for databases to monetdb and merovingian.

* Tue Aug 24 2010 Niels Nes <niels@cwi.nl> - 2.40.0-20101029
- make it possible to use '?' in offset and limit

* Tue Aug 24 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.38.5-20100824
- Rebuilt.

* Mon Aug 23 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.38.5-20100823
- Rebuilt.

* Fri Aug 20 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.38.5-20100823
- Fixed a case where the optimizer incorrectly removed an expression.
  This fixes bu 2602.
- Fix a crash in prepared statements when a parameter is on the left-hand
  side of a binary operator.  This fixes bug 2599.
- Fixed reporting of a violated foreign key constraint.  This fixes
  bug 2598.
- Certain schema altering queries didn't report success, even though
  they did succeed.  This fixes bug 2589.
- Fixed a crash when a non-existing table was used in an IN clause.
  Fixes bug 2604.
- Fixed bug 2633.  Adding a LIMIT clause could, in certain conditions,
  cause a crash.

* Thu Aug 19 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.38.5-20100823
- A bug was fixed where updates were missing in large transaction.
  This fixes bug 2543.
- A memory leak was fixed which caused the server to grow when loading
  and emptying a table.  This was bug 2539.

* Fri Aug 13 2010 Niels Nes <niels@cwi.nl> - 2.38.5-20100823
- Fixed bug 2643 added more defensive code, when an aggregation function
  doesn't exist
- Fixed bug 2651 properly handle dead code elimination with
  op_semi/op_anti and references

* Thu Aug 12 2010 Niels Nes <niels@cwi.nl> - 2.38.5-20100823
- Fixed bug 2652. Correctly list all columns of a 'IN' query with 'EXCEPT'
- Fixed bug 2353. The relational optimizer didn't handle range join
  expressions properly.
- fixed bug 2354. Improved function resolution.

* Mon Aug  9 2010 Jennie Zhang <y.zhang@cwi.nl> - 2.38.5-20100823
- Fixed bug 2645: mat.pack+algebra.slice should be replaced by mat.slice
  for 'limit 1' (when the default_pipe is used)

* Fri Aug  6 2010 Fabian Groffen <fabian@cwi.nl> - 2.38.5-20100823
- Fixed bug #2641,  The SQL server now handles Unicode BOM sequences
  occurring in any place.  Previously an "unexpected character (U+FEFF)"
  error would be returned.

* Fri Jul 30 2010 Niels Nes <niels@cwi.nl> - 2.38.5-20100823
- Fix Bug 2611. Fixed check for multiple functions without parameters.
- Fixed bug 2569 (except/union/intersect right after
  insert/delete/update. We now correctly fallback to the more general
  subquery case (not only simple selects (SQL_SELECT)).

* Thu Jul 29 2010 Niels Nes <niels@cwi.nl> - 2.38.5-20100823
- Fixed bug in handling 'WITH' and row_number()  (Bug 2631).
  The cardinality of the row_number expression was incorrect.

* Thu Jul 29 2010 Niels Nes <niels@cwi.nl> - 2.38.5-20100823
- Fixed ORDER BY over UNION etc. (bug 2606) by
  automatically adding select * around select x union select y order by z.

* Mon Jul 26 2010 Fabian Groffen <fabian@cwi.nl> - 2.38.5-20100823
- On installs with (very) long prefixes, the UNIX domain sockets could get
  truncated, causing merovingian to become unavailable to monetdb and mclient.
  Similarly, a fall back to a regular TCP socket for mapi connections is
  used for forked mservers in this case.

* Wed Jul 21 2010 Fabian Groffen <fabian@cwi.nl> - 2.38.5-20100823
- Really shutdown when an argument to merovingian was given, instead of
  ending up in some inconsistent state.  This solves all weird behaviour
  observed in bug #2628.
- Report MonetDB release on --version flags and in the logs.

* Tue Jul 20 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.38.5-20100823
- Fixed bug 2624: function returning decimal returned result that was
  scaled incorrectly.

* Sun Jul 18 2010 Stefan Manegold <Stefan.Manegold@cwi.nl> - 2.38.5-20100823
- fixed bug 2622 "LIMIT & OFFSET ignored on 64-bit big-endian when
  combined with GROUP BY"

* Fri Jul 16 2010 Fabian Groffen <fabian@cwi.nl> - 2.38.5-20100823
- Remove references to master/slave settings, since replication isn't yet
  fully implemented in Jun2010 branch.

* Tue Jul 13 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.38.3-20100713
- Rebuilt.

* Mon Jul 12 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.38.3-20100712
- Rebuilt.

* Mon Jul 12 2010 Stefan Manegold <Stefan.Manegold@cwi.nl> - 2.38.3-20100712
- Make queries like (SELECT ...) UNION ALL (SELECT ...) ORDER BY ...;
  work, again, that were broken since Jun 22 2010 triggering errors
  like "ORDER BY: missing select operator"

* Fri Jul  9 2010 Fabian Groffen <fabian@cwi.nl> - 2.38.3-20100712
- Removed false connection warning about missing SQL script ("could
  not read createdb.sql") received by the client upon first connect on
  a newly created database.  Bug #2591

* Fri Jul 09 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.38.3-20100709
- Rebuilt.

* Thu Jul  8 2010 Fabian Groffen <fabian@cwi.nl> - 2.38.3-20100709
- Improved error message for certain type-related problems by
  including the affected column name.

* Wed Jul  7 2010 Fabian Groffen <fabian@cwi.nl> - 2.38.3-20100709
- Make TEXT a separate keyword, separating it from CLOB, such that we
  can sloppily allow TEXT to appear as a column name, since it seems
  not to be in the standard as reserved keyword.

* Wed Jul  7 2010 Niels Nes <niels@cwi.nl> - 2.38.3-20100709
- Fixed bug 2581. Completed the implementation of handling Boolean
  types in prepare statements.
- Fix bug 2582. Statements with 'constant in ( )' are now handled
  properly.
- Fixed bug 2583 + added test. The assert was incorrect.

* Mon Jul  5 2010 Niels Nes <niels@cwi.nl> - 2.38.3-20100706
- Fixed bug in zero_or_one
- Fixed bug in dead code elimination for projections with distinct
- Fixed bug handling join with constant values on both sides (like
  group results and constants)
- fixed bug in UPDATE TABLE when updating multiple rows

* Wed Jun 30 2010 Stefan Manegold <Stefan.Manegold@cwi.nl> - 2.38.3-20100706
- fixed bug 2564: in case group by column is not found as alias in
  projection list, fall back to check plain input columns in order to
  find the underlying BAT and check its sortedness

* Wed Jun 30 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.38.1-20100630
- Rebuilt.

* Fri Jun 25 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.38.1-20100625
- Rebuilt.

* Thu Jun 24 2010 Niels Nes <niels@cwi.nl> - 2.38.1-20100625
- make it possible to use '?' in offset and limit

* Tue Jun 22 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.38.1-20100622
- Rebuilt.

* Fri Jun 18 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.38.1-20100622
- Added include and .lib files to Windows installers that are needed
  to compile client programs.

* Fri Jun 18 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.38.1-20100618
- Rebuilt.

* Mon May 31 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.38.1-20100618
- Updated Vendor information.

* Tue Apr 20 2010 Stefan Manegold <manegold@cwi.nl> - 2.38.1-20100618
- Made compilation of "testing" (and "java") independent of MonetDB.
  This is mainly for Windows, but also on other systems, "testing" can
  now be built independently of (and hence before) "MonetDB".  Files
  that mimic configure functionality on Windows were moved from
  "MonetDB" to "buildtools"; hence, this affects all packages on
  Windows, requiring a complete rebuild from scratch on Windows.
  getopt() support in testing has changed; hence, (most probably)
  requiring a rebuild from scratch of testing on other systems.

* Tue Apr 20 2010 Stefan Manegold <manegold@cwi.nl> - 2.38.1-20100618
- Implemented build directory support for Windows, i.e., like on
  Unix/Linux also on Windows we can now build in a separate build
  directory as alternative to ...\<package>\NT, and thus keep the latter
  clean from files generated during the build.  On Windows, the build
  directory must be a sibling of ...\<package>\NT .

* Tue Apr 20 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.36.5-20100420
- Rebuilt.

* Thu Apr  8 2010 Stefan Manegold <manegold@cwi.nl> - 2.36.5-20100420
- fixed bug #2983773 "SQL: minimal optimizer pipe unstable"

* Mon Mar 22 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.36.3-20100322
- Rebuilt.

* Wed Feb 24 2010 Sjoerd Mullender <sjoerd@acm.org> - 2.36.1-20100224
- Rebuilt.

