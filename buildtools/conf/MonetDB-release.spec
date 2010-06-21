Name:		MonetDB-release
Version:	1.1
Release:	1%{?dist}
Summary:	MonetDB YUM Repository

Group:		Applications/Databases
License:	MPL - http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
URL:		http://dev.monetdb.org/downloads/
Source0:	http://dev.monetdb.org/downloads/sources/%{name}-%{version}.tar.gz
BuildArch:	noarch

Vendor:		MonetDB BV <info@monetdb.org>

Requires:	fedora-release

%description
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package provides the necessary files to use the MonetDB YUM
repository.

%prep
%setup -q


%build


%install
%{__install} -D -p -m 0644 MonetDB-GPG-KEY \
    ${RPM_BUILD_ROOT}%{_sysconfdir}/pki/rpm-gpg/RPM-GPG-KEY-MonetDB
%{__install} -D -p -m 0644 monetdb.repo \
    ${RPM_BUILD_ROOT}%{_sysconfdir}/yum.repos.d/monetdb.repo


%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(-,root,root,-)
%doc
%{_sysconfdir}/yum.repos.d/monetdb.repo
%{_sysconfdir}/pki/rpm-gpg/RPM-GPG-KEY-MonetDB



%changelog
* Mon Jun 21 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.1-1
- Moved the repository to dev.monetdb.org.

* Mon May 31 2010 Sjoerd Mullender <sjoerd@acm.org> - 1.0-4
- Updated Vendor information.

* Fri Dec 11 2009 Sjoerd Mullender <sjoerd@acm.org> - 1.0-3
- Added a testing repository (for release candidates).

* Fri Nov  6 2009 Sjoerd Mullender <sjoerd@acm.org> - 1.0-2
- Don't import key when installing MonetDB-release RPM, but when the
  first package asks for it.

* Tue Nov 18 2008 Sjoerd Mullender <sjoerd@acm.org> - 1.0-1
- Initial version.

