%define name MonetDB
%define major_version 4
%define minor_version 3
%define sublevel 4
%define release 1
%define version %{major_version}.%{minor_version}.%{sublevel}

Name: %{name}
Version: %{version}
Release: %{release}
Summary: MonetDB sql database system
Group: System
Source: %{name}-%{version}.tar.gz
BuildRoot: /var/tmp/%{name}-root
Packager: Niels Nes <Niels.Nes@cwi.nl>
Copyright:   MPL

%package client
Summary: MonetDB sql client 
Group: System

%package server
Summary: MonetDB sql server 
Group: System 

%package devel
Summary: MonetDB  server 
Group: System 

%description
Add the MonetDB description here

%description client
Add the MonetDB client description here

%description server
Add the MonetDB server description here

%description devel
Add the MonetDB devel description here


%prep
rm -rf $RPM_BUILD_ROOT

%setup -q -n %{name}-%{version}

%build
./configure --prefix=/usr 
make

%install

%files server
%defattr(-,monet,monet)

%pre server
adduser monet
addgroup monet
