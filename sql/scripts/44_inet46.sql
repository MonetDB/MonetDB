-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2024, 2025 MonetDB Foundation;
-- Copyright August 2008 - 2023 MonetDB B.V.;
-- Copyright 1997 - July 2008 CWI.

create type inet4 external name inet4;

create type inet6 external name inet6;

create function contains(ip1 inet4, ip2 inet4, netmask tinyint) returns bool external name "inet46"."inet4containsinet4";
create function contains(ip1 inet4, netmask1 tinyint, ip2 inet4, netmask2 tinyint) returns bool external name "inet46"."inet4containsinet4";
create function containsorequal(ip1 inet4, ip2 inet4, netmask tinyint) returns bool external name "inet46"."inet4containsorequalinet4";
create function containsorequal(ip1 inet4, netmask1 tinyint, ip2 inet4, netmask2 tinyint) returns bool external name "inet46"."inet4containsorequalinet4";
create function containssymmetric(ip1 inet4, netmask1 tinyint, ip2 inet4, netmask2 tinyint) returns bool external name "inet46"."inet4containssymmetricinet4";


create function contains(ip1 inet6, ip2 inet6, netmask tinyint) returns bool external name "inet46"."inet6containsinet6";
create function contains(ip1 inet6, netmask1 tinyint, ip2 inet6, netmask2 tinyint) returns bool external name "inet46"."inet6containsinet6";
create function containsorequal(ip1 inet6, ip2 inet6, netmask tinyint) returns bool external name "inet46"."inet6containsorequalinet6";
create function containsorequal(ip1 inet6, netmask1 tinyint, ip2 inet6, netmask2 tinyint) returns bool external name "inet46"."inet6containsorequalinet6";
create function containssymmetric(ip1 inet6, netmask1 tinyint, ip2 inet6, netmask2 tinyint) returns bool external name "inet46"."inet6containssymmetricinet6";

grant execute on function contains(inet4, inet4, tinyint) to public;
grant execute on function contains(inet4, tinyint, inet4, tinyint) to public;
grant execute on function containsorequal(inet4, inet4, tinyint) to public;
grant execute on function containsorequal(inet4, tinyint, inet4, tinyint) to public;
grant execute on function containssymmetric(inet4, tinyint, inet4, tinyint) to public;

grant execute on function contains(inet6, inet6, tinyint) to public;
grant execute on function contains(inet6, tinyint, inet6, tinyint) to public;
grant execute on function containsorequal(inet6, inet6, tinyint) to public;
grant execute on function containsorequal(inet6, tinyint, inet6, tinyint) to public;
grant execute on function containssymmetric(inet6, tinyint, inet6, tinyint) to public;
