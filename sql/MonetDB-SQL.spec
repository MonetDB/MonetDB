%define name MonetDB-SQL
%define major_version 2
%define minor_version 0
%define sublevel 0
%define release 1beta
%define version %{major_version}.%{minor_version}.%{sublevel}
%define prefix /usr

Name: %{name}
Version: %{version}
Release: %{release}
Summary: MonetDB SQL - Monet Database Management System
Group: System
Source: sql-%{major_version}.%{minor_version}.tar.gz
BuildRoot: /var/tmp/%{name}-root
Packager: Niels Nes <Niels.Nes@cwi.nl>
Copyright:   MPL - http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
BuildRequires: MonetDB-server

%package client
Summary: MonetDB SQL clients
Group: System

%package server
Summary: MonetDB SQL server modules
Group: System 

%package devel
Summary: MonetDB SQL development package 
Group: System 

%description
MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accellerators, SQL- and XML- frontends.

%description client
Add the MonetDB SQL client description here
Requires: MonetDB-client

%description server
Add the MonetDB SQL server module description here
Requires: %{name}-client
Requires: MonetDB-server

%description devel
Add the MonetDB devel description here
Requires: %{name}-server
Requires: MonetDB-devel


%prep
rm -rf $RPM_BUILD_ROOT

%setup -q -n sql-%{major_version}.%{minor_version}

%build

./configure --prefix=%{prefix} --with-monet=%{prefix}

make

%install
rm -rf $RPM_BUILD_ROOT

make install \
	DESTDIR=$RPM_BUILD_ROOT 

#find $RPM_BUILD_ROOT -name .incs.in | xargs rm

# Fixes monet config script
#perl -p -i -e "s|$RPM_BUILD_ROOT||" $RPM_BUILD_ROOT%{prefix}/bin/monet_config

%clean
rm -fr $RPM_BUILD_ROOT

%files client
%defattr(-,monetdb,monetdb) 
%{prefix}/bin/sql_dump 
%{prefix}/bin/sql_client 
%{prefix}/bin/Msql 
%{prefix}/lib/libsql.*
%{prefix}/lib/libgdk_wrap.* 
%{prefix}/lib/libMonetODBC*

%files server
%defattr(-,monetdb,monetdb) 
%{prefix}/lib/MonetDB/lib_sqlserver*
%{prefix}/share/MonetDB/*

%files devel
%defattr(-,monetdb,monetdb) 
%{prefix}/include/*.h
%{prefix}/include/*/*.[hcm]
