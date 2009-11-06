Name:		MonetDB-release
Version:	1.0
Release:	1%{?dist}
Summary:	MonetDB YUM Repository

Group:		Applications/Databases
License:	MPL - http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
URL:		http://monetdb.cwi.nl/
Source0:	http://monetdb.cwi.nl/downloads/sources/%{name}-%{version}.tar.gz
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:	noarch

Vendor:		MonetDB B.V.

Requires:	fedora-release

%description
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, SQL- and XML- frontends.

This package provides the necessary files to use the MonetDB repository.

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


%post
# Import homerepo.net gpg key if needed
rpm -q gpg-pubkey-0583366f-491d42fe >/dev/null 2>&1 || \
    rpm --import %{_sysconfdir}/pki/rpm-gpg/RPM-GPG-KEY-MonetDB
# We don't want a possible error to leave the previous package installed
exit 0

%files
%defattr(-,root,root,-)
%doc
%{_sysconfdir}/yum.repos.d/monetdb.repo
%{_sysconfdir}/pki/rpm-gpg/RPM-GPG-KEY-MonetDB



%changelog
* Tue Nov 18 2008 Sjoerd Mullender <sjoerd@acm.org> - 1.0-1
- Initial version.

