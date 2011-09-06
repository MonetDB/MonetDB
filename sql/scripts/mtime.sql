-- The contents of this file are subject to the MonetDB Public License
-- Version 1.1 (the "License"); you may not use this file except in
-- compliance with the License. You may obtain a copy of the License at
-- http://www.monetdb.org/Legal/MonetDBLicense
--
-- Software distributed under the License is distributed on an "AS IS"
-- basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
-- License for the specific language governing rights and limitations
-- under the License.
--
-- The Original Code is the MonetDB Database System.
--
-- The Initial Developer of the Original Code is CWI.
-- Copyright August 2008-2011 MonetDB B.V.
-- All Rights Reserved.

create function curdate( ) returns date
	external name mtime."current_date";

create function "current_date"( ) returns date
	external name mtime."current_date";

create function curtime( ) returns TIMETZ
	external name mtime."current_time";
create function "current_time"( ) returns TIMETZ
	external name mtime."current_time";

create function "current_timestamp"( ) returns TIMESTAMPTZ
	external name mtime."current_timestamp";

create function "localtime"( ) returns TIME
	external name mtime."current_time";
create function "localtimestamp"( ) returns TIMESTAMP
	external name mtime."current_timestamp";

create type second_interval as interval day to sec;

create function local_timezone( ) returns second_interval
	external name mtime.local_timezone;


create function sql_sub( date, second_interval ) returns date
	external name mtime.date_sub_sec_interval;
create function sql_sub( date, month_interval ) returns date
	external name mtime.date_sub_month_interval;
create function sql_sub( timestamp, second_interval ) returns timestamp
	external name mtime.timestamp_sub_sec_interval;
create function sql_sub( timestamp, month_interval ) returns timestamp
	external name mtime.timestamp_sub_month_interval;
create function sql_sub( time, second_interval ) returns time
	external name mtime.time_sub_sec_interval;
create function sql_sub( date, date ) returns integer
	external name mtime.diff;
create function sql_sub( timestamp, timestamp ) returns bigint
	external name mtime.diff;

create function sql_add( date, second_interval ) returns date
	external name mtime.date_add_sec_interval;
create function sql_add( date, month_interval ) returns date
	external name mtime.addmonths;
create function sql_add( timestamp, second_interval ) returns timestamp
	external name mtime.timestamp_add_sec_interval;
create function sql_add( timestamp, month_interval ) returns timestamp
	external name mtime.addmonths;
create function sql_add( time, second_interval ) returns time
	external name mtime.time_add_sec_interval;

create function year( date ) returns integer
	external name mtime.year;
create function month( date ) returns integer
	external name mtime.month;
create function day( date ) returns integer
	external name mtime.day;
create function hour( time ) returns integer
	external name mtime.hours;
create function minute( time ) returns integer
	external name mtime.minutes;
create function second( time ) returns integer
	external name mtime.seconds;

create function year( timestamp ) returns integer
	external name mtime.year;
create function month( timestamp ) returns integer
	external name mtime.month;
create function day( timestamp ) returns integer
	external name mtime.day;
create function hour( timestamp ) returns integer
	external name mtime.hours;
create function minute( timestamp ) returns integer
	external name mtime.minutes;
create function second( timestamp ) returns integer
	external name mtime.seconds;

create function year( month_interval ) returns integer
	external name mtime.year;
create function month( month_interval ) returns integer
	external name mtime.month;
create function day( second_interval ) returns integer
	external name mtime.day;
create function hour( second_interval ) returns integer
	external name mtime.hours;
create function minute( second_interval ) returns integer
	external name mtime.minutes;
create function second( second_interval ) returns integer
	external name mtime.seconds;

create function dayofyear( date ) returns integer
	external name mtime.dayofyear;
create function weekofyear( date ) returns integer
	external name mtime.weekofyear;
create function dayofweek( date ) returns integer
	external name mtime.dayofweek;
create function dayofmonth( date ) returns integer
	external name mtime.dayofmonth;
create function week( date ) returns integer
	external name mtime.weekofyear;
