-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- For copyright information, see the file debian/copyright.

create type inet4 external name inet4;

create type inet6 external name inet6;

create function contains(ip1 inet4, ip2 inet4, netmask tinyint)
	returns bool external name "inet46"."inet4containsinet4";
create function contains(ip1 inet4, netmask1 tinyint, ip2 inet4, netmask2 tinyint)
	returns bool external name "inet46"."inet4containsinet4";
create function containsorequal(ip1 inet4, ip2 inet4, netmask tinyint)
	returns bool external name "inet46"."inet4containsorequalinet4";
create function containsorequal(ip1 inet4, netmask1 tinyint, ip2 inet4, netmask2 tinyint)
	returns bool external name "inet46"."inet4containsorequalinet4";
create function containssymmetric(ip1 inet4, netmask1 tinyint, ip2 inet4, netmask2 tinyint)
	returns bool external name "inet46"."inet4containssymmetricinet4";

create function contains(ip1 inet6, ip2 inet6, netmask smallint)
	returns bool external name "inet46"."inet6containsinet6";
create function contains(ip1 inet6, netmask1 smallint, ip2 inet6, netmask2 smallint)
	returns bool external name "inet46"."inet6containsinet6";
create function containsorequal(ip1 inet6, ip2 inet6, netmask smallint)
	returns bool external name "inet46"."inet6containsorequalinet6";
create function containsorequal(ip1 inet6, netmask1 smallint, ip2 inet6, netmask2 smallint)
	returns bool external name "inet46"."inet6containsorequalinet6";
create function containssymmetric(ip1 inet6, netmask1 smallint, ip2 inet6, netmask2 smallint)
	returns bool external name "inet46"."inet6containssymmetricinet6";

create function bit_not(ip1 inet4)
	returns inet4 external name "calc"."not";
create function bit_and(ip1 inet4, ip2 inet4)
	returns inet4 external name "calc"."and";
create function bit_or(ip1 inet4, ip2 inet4)
	returns inet4 external name "calc"."or";
create function bit_xor(ip1 inet4, ip2 inet4)
	returns inet4 external name "calc"."xor";
create function bit_not(ip1 inet6)
	returns inet6 external name "calc"."not";
create function bit_and(ip1 inet6, ip2 inet6)
	returns inet6 external name "calc"."and";
create function bit_or(ip1 inet6, ip2 inet6)
	returns inet6 external name "calc"."or";
create function bit_xor(ip1 inet6, ip2 inet6)
	returns inet6 external name "calc"."xor";

grant execute on function contains(inet4, inet4, tinyint) to public;
grant execute on function contains(inet4, tinyint, inet4, tinyint) to public;
grant execute on function containsorequal(inet4, inet4, tinyint) to public;
grant execute on function containsorequal(inet4, tinyint, inet4, tinyint) to public;
grant execute on function containssymmetric(inet4, tinyint, inet4, tinyint) to public;

grant execute on function contains(inet6, inet6, smallint) to public;
grant execute on function contains(inet6, smallint, inet6, smallint) to public;
grant execute on function containsorequal(inet6, inet6, smallint) to public;
grant execute on function containsorequal(inet6, smallint, inet6, smallint) to public;
grant execute on function containssymmetric(inet6, smallint, inet6, smallint) to public;

grant execute on function bit_not(inet4) to public;
grant execute on function bit_and(inet4, inet4) to public;
grant execute on function bit_or(inet4, inet4) to public;
grant execute on function bit_xor(inet4, inet4) to public;
grant execute on function bit_not(inet6) to public;
grant execute on function bit_and(inet6, inet6) to public;
grant execute on function bit_or(inet6, inet6) to public;
grant execute on function bit_xor(inet6, inet6) to public;
