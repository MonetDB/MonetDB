%define name MonetDB
%define major_version 4
%define minor_version 3
%define sublevel 5
%define release 1
%define version %{major_version}.%{minor_version}.%{sublevel}
%define prefix /usr

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
Requires: %{name}-client

%description devel
Add the MonetDB devel description here
Requires: %{name}-server
Requires: epsffit


%prep
rm -rf $RPM_BUILD_ROOT

%setup -q -n %{name}-%{version}

%build
./configure --prefix=$RPM_BUILD_ROOT%{prefix} 
make

%install
make install

# Fixes monet config script
perl -p -i -e "s|$RPM_BUILD_ROOT||" $RPM_BUILD_ROOT%{prefix}/bin/monet_config


%files client
%defattr(-,monet,monet) 
%{prefix}/bin/MapiClient 
%{prefix}/lib/libMapi.so* 
%{prefix}/lib/libstream.so*

%{prefix}/share/MonetDB/site_perl/* 
%{prefix}/share/MonetDB/python/* 

%files server
%defattr(-,monet,monet) 
%{prefix}/bin/Mserver 
%{prefix}/bin/Mshutdown
%{prefix}/bin/monet-config

%{prefix}/lib/libbat.so*
%{prefix}/lib/libmonet.so*
%{prefix}/lib/MonetDB/*.so* 

%{prefix}/share/MonetDB/mapi.mil 
%{prefix}/share/MonetDB/tools/* 

%{prefix}/etc/monet.conf 


%files devel
%defattr(-,monet,monet) 
%{prefix}/share/MonetDB/conf/monet.m4 

%{prefix}/include/*.h
%{prefix}/include/*/*.[hcm]

%{prefix}/bin/mel
%{prefix}/bin/calibrator
%{prefix}/bin/Mx 
%{prefix}/bin/prefixMxFile 
%{prefix}/bin/idxmx 

%{prefix}/share/MonetDB/monet.Mprofile.conf 
%{prefix}/share/MonetDB/Mprofile-commands.lst 
%{prefix}/share/MonetDB/monet.Mtest.conf 
%{prefix}/share/MonetDB/quit.mil 

%{prefix}/bin/prof.py
%{prefix}/bin/Mtest.py
%{prefix}/bin/Mapprove.py
%{prefix}/bin/Mfilter.py
%{prefix}/bin/Mprofile.py
%{prefix}/bin/Mlog
%{prefix}/bin/Mdiff
%{prefix}/bin/Mtimeout
%{prefix}/bin/MkillUsers

%{prefix}/lib/autogen/* 

%pre server
adduser monet
addgroup monet
