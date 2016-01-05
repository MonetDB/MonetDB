# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

package DBD::monetdb::TypeInfo;

use DBI qw(:sql_types);

my %index =
(
  TYPE_NAME          =>  0
, DATA_TYPE          =>  1
, COLUMN_SIZE        =>  2
, LITERAL_PREFIX     =>  3
, LITERAL_SUFFIX     =>  4
, CREATE_PARAMS      =>  5
, NULLABLE           =>  6
, CASE_SENSITIVE     =>  7
, SEARCHABLE         =>  8
, UNSIGNED_ATTRIBUTE =>  9
, FIXED_PREC_SCALE   => 10
, AUTO_UNIQUE_VALUE  => 11
, LOCAL_TYPE_NAME    => 12
, MINIMUM_SCALE      => 13
, MAXIMUM_SCALE      => 14
, SQL_DATA_TYPE      => 15
, SQL_DATETIME_SUB   => 16
, NUM_PREC_RADIX     => 17
, INTERVAL_PRECISION => 18
);

my @data =
(
#   NAME                        TYPE                              SIZE PREFIX SUFFIX  PARAMS            N  C  S      U  F      A  NAME     MIN    MAX  TYPE            SUB  RADIX   IV_P
  ['char'                     , SQL_CHAR                     , 1000000, "'"  , "'"  ,'length'         , 1, 1, 3, undef, 0,     0, undef, undef, undef, SQL_CHAR    , undef, undef, undef ]
, ['character'                , SQL_CHAR                     , 1000000, "'"  , "'"  ,'length'         , 1, 1, 3, undef, 0,     0, undef, undef, undef, SQL_CHAR    , undef, undef, undef ]
, ['decimal'                  , SQL_DECIMAL                  ,      19, undef, undef,'precision,scale', 1, 0, 2,     0, 0,     0, undef,     0,    19, SQL_DECIMAL , undef,    10, undef ]
, ['dec'                      , SQL_DECIMAL                  ,      19, undef, undef,'precision,scale', 1, 0, 2,     0, 0,     0, undef,     0,    19, SQL_DECIMAL , undef,    10, undef ]
, ['numeric'                  , SQL_DECIMAL                  ,      19, undef, undef,'precision,scale', 1, 0, 2,     0, 0,     0, undef,     0,    19, SQL_DECIMAL , undef,    10, undef ]
, ['int'                      , SQL_INTEGER                  ,       9, undef, undef, undef           , 1, 0, 2,     0, 0,     0, undef,     0,     0, SQL_INTEGER , undef,    10, undef ]
, ['integer'                  , SQL_INTEGER                  ,       9, undef, undef, undef           , 1, 0, 2,     0, 0,     0, undef,     0,     0, SQL_INTEGER , undef,    10, undef ]
, ['mediumint'                , SQL_INTEGER                  ,       9, undef, undef, undef           , 1, 0, 2,     0, 0,     0, undef,     0,     0, SQL_INTEGER , undef,    10, undef ]
, ['smallint'                 , SQL_SMALLINT                 ,       4, undef, undef, undef           , 1, 0, 2,     0, 0,     0, undef,     0,     0, SQL_SMALLINT, undef,    10, undef ]
, ['tinyint'                  , SQL_SMALLINT                 ,       4, undef, undef, undef           , 1, 0, 2,     0, 0,     0, undef,     0,     0, SQL_SMALLINT, undef,    10, undef ]
, ['float'                    , SQL_FLOAT                    ,      24, undef, undef,'precision,scale', 1, 0, 2,     0, 0,     0, undef,     0,     0, SQL_FLOAT   , undef,     2, undef ]
, ['real'                     , SQL_REAL                     ,      24, undef, undef, undef           , 1, 0, 2,     0, 0,     0, undef,     0,     0, SQL_REAL    , undef,     2, undef ]
, ['double'                   , SQL_DOUBLE                   ,      53, undef, undef, undef           , 1, 0, 2,     0, 0,     0, undef,     0,     0, SQL_DOUBLE  , undef,     2, undef ]
, ['double precision'         , SQL_DOUBLE                   ,      53, undef, undef, undef           , 1, 0, 2,     0, 0,     0, undef,     0,     0, SQL_DOUBLE  , undef,     2, undef ]
, ['varchar'                  , SQL_VARCHAR                  , 1000000, "'"  , "'"  ,'length'         , 1, 1, 3, undef, 0, undef, undef, undef, undef, SQL_VARCHAR , undef, undef, undef ]
, ['character varying'        , SQL_VARCHAR                  , 1000000, "'"  , "'"  ,'length'         , 1, 1, 3, undef, 0, undef, undef, undef, undef, SQL_VARCHAR , undef, undef, undef ]
, ['char varying'             , SQL_VARCHAR                  , 1000000, "'"  , "'"  ,'length'         , 1, 1, 3, undef, 0, undef, undef, undef, undef, SQL_VARCHAR , undef, undef, undef ]
, ['boolean'                  , SQL_BOOLEAN                  ,       1, undef, undef, undef           , 1, 0, 2,     0, 1,     0, undef, undef, undef, SQL_BOOLEAN , undef, undef, undef ]
, ['bool'                     , SQL_BOOLEAN                  ,       1, undef, undef, undef           , 1, 0, 2,     0, 1,     0, undef, undef, undef, SQL_BOOLEAN , undef, undef, undef ]
, ['bigint'                   , 25                           ,      19, undef, undef, undef           , 1, 0, 2,     0, 0,     0, undef,     0,     0, 25          , undef,    10, undef ]
, ['blob'                     , SQL_BLOB                     , 1000000, "'"  , "'"  ,'length'         , 1, 1, 3, undef, 0, undef, undef, undef, undef, SQL_BLOB    , undef, undef, undef ]
, ['binary large object'      , SQL_BLOB                     , 1000000, "'"  , "'"  ,'length'         , 1, 1, 3, undef, 0, undef, undef, undef, undef, SQL_BLOB    , undef, undef, undef ]
, ['clob'                     , SQL_CLOB                     , 1000000, "'"  , "'"  ,'length'         , 1, 1, 3, undef, 0, undef, undef, undef, undef, SQL_CLOB    , undef, undef, undef ]
, ['character large object'   , SQL_CLOB                     , 1000000, "'"  , "'"  ,'length'         , 1, 1, 3, undef, 0, undef, undef, undef, undef, SQL_CLOB    , undef, undef, undef ]
, ['char large object'        , SQL_CLOB                     , 1000000, "'"  , "'"  ,'length'         , 1, 1, 3, undef, 0, undef, undef, undef, undef, SQL_CLOB    , undef, undef, undef ]
, ['string'                   , SQL_CLOB                     , 1000000, "'"  , "'"  ,'length'         , 1, 1, 3, undef, 0, undef, undef, undef, undef, SQL_CLOB    , undef, undef, undef ]
, ['text'                     , SQL_CLOB                     , 1000000, "'"  , "'"  ,'length'         , 1, 1, 3, undef, 0, undef, undef, undef, undef, SQL_CLOB    , undef, undef, undef ]
, ['tinytext'                 , SQL_CLOB                     , 1000000, "'"  , "'"  ,'length'         , 1, 1, 3, undef, 0, undef, undef, undef, undef, SQL_CLOB    , undef, undef, undef ]
, ['date'                     , SQL_TYPE_DATE                   ,      10,      "date '", "'"                 , undef           , 1, 0, 2, undef, 0, undef, undef, undef, undef, SQL_DATE    ,     1, undef, undef ]
, ['time'                     , SQL_TYPE_TIME                   ,      12,      "time '", "'"                 ,'precision'      , 1, 0, 2, undef, 0, undef, undef, undef, undef, SQL_DATE    ,     2, undef, undef ]
, ['timestamp'                , SQL_TYPE_TIMESTAMP              ,      23, "timestamp '", "'"                 ,'precision'      , 1, 0, 2, undef, 0, undef, undef, undef, undef, SQL_DATE    ,     3, undef, undef ]
, ['timetz'                   , SQL_TYPE_TIME_WITH_TIMEZONE     ,      18,      "time '", "'"                 ,'precision'      , 1, 0, 2, undef, 0, undef, undef, undef, undef, SQL_DATE    ,     4, undef, undef ]
, ['time with time zone'      , SQL_TYPE_TIME_WITH_TIMEZONE     ,      18,      "time '", "'"                 ,'precision'      , 1, 0, 2, undef, 0, undef, undef, undef, undef, SQL_DATE    ,     4, undef, undef ]
, ['timestamptz'              , SQL_TYPE_TIMESTAMP_WITH_TIMEZONE,      29, "timestamp '", "'"                 ,'precision'      , 1, 0, 2, undef, 0, undef, undef, undef, undef, SQL_DATE    ,     5, undef, undef ]
, ['timestamp with time zone' , SQL_TYPE_TIMESTAMP_WITH_TIMEZONE,      29, "timestamp '", "'"                 ,'precision'      , 1, 0, 2, undef, 0, undef, undef, undef, undef, SQL_DATE    ,     5, undef, undef ]
, ['interval year'            , SQL_INTERVAL_YEAR               ,       9,  "interval '", "' year"            ,'precision'      , 1, 0, 2, undef, 0, undef, undef,     0,     0, SQL_TIME    ,     1, undef,     9 ]
, ['interval month'           , SQL_INTERVAL_MONTH              ,      10,  "interval '", "' month"           ,'precision'      , 1, 0, 2, undef, 0, undef, undef,     0,     0, SQL_TIME    ,     2, undef,    10 ]
, ['month_interval'           , SQL_INTERVAL_MONTH              ,      10,  "interval '", "' month"           ,'precision'      , 1, 0, 2, undef, 0, undef, undef,     0,     0, SQL_TIME    ,     2, undef,    10 ]
, ['interval day'             , SQL_INTERVAL_DAY                ,       5,  "interval '", "' day"             ,'precision'      , 1, 0, 2, undef, 0, undef, undef,     0,     0, SQL_TIME    ,     3, undef,     5 ]
, ['interval hour'            , SQL_INTERVAL_HOUR               ,       6,  "interval '", "' hour"            ,'precision'      , 1, 0, 2, undef, 0, undef, undef,     0,     0, SQL_TIME    ,     4, undef,     6 ]
, ['interval minute'          , SQL_INTERVAL_MINUTE             ,       8,  "interval '", "' minute"          ,'precision'      , 1, 0, 2, undef, 0, undef, undef,     0,     0, SQL_TIME    ,     5, undef,     8 ]
, ['interval second'          , SQL_INTERVAL_SECOND             ,      10,  "interval '", "' second"          ,'precision'      , 1, 0, 2, undef, 0, undef, undef,     0,     0, SQL_TIME    ,     6, undef,    10 ]
, ['sec_interval'             , SQL_INTERVAL_SECOND             ,      10,  "interval '", "' second"          ,'precision'      , 1, 0, 2, undef, 0, undef, undef,     0,     0, SQL_TIME    ,     6, undef,    10 ]
, ['interval year to month'   , SQL_INTERVAL_YEAR_TO_MONTH      ,      12,  "interval '", "' year to month"   , undef           , 1, 0, 2, undef, 0, undef, undef,     0,     0, SQL_TIME    ,     7, undef,     9 ]
, ['interval day to hour'     , SQL_INTERVAL_DAY_TO_HOUR        ,       8,  "interval '", "' day to hour"     , undef           , 1, 0, 2, undef, 0, undef, undef,     0,     0, SQL_TIME    ,     8, undef,     5 ]
, ['interval day to minute'   , SQL_INTERVAL_DAY_TO_MINUTE      ,      11,  "interval '", "' day to minute"   , undef           , 1, 0, 2, undef, 0, undef, undef,     0,     0, SQL_TIME    ,     9, undef,     5 ]
, ['interval day to second'   , SQL_INTERVAL_DAY_TO_SECOND      ,      14,  "interval '", "' day to second"   ,'precision'      , 1, 0, 2, undef, 0, undef, undef,     0,     0, SQL_TIME    ,    10, undef,     5 ]
, ['interval hour to minute'  , SQL_INTERVAL_HOUR_TO_MINUTE     ,       9,  "interval '", "' hour to minute"  , undef           , 1, 0, 2, undef, 0, undef, undef,     0,     0, SQL_TIME    ,    11, undef,     6 ]
, ['interval hour to second'  , SQL_INTERVAL_HOUR_TO_SECOND     ,      12,  "interval '", "' hour to second"  ,'precision'      , 1, 0, 2, undef, 0, undef, undef,     0,     0, SQL_TIME    ,    12, undef,     6 ]
, ['interval minute to second', SQL_INTERVAL_MINUTE_TO_SECOND   ,      13,  "interval '", "' minute to second",'precision'      , 1, 0, 2, undef, 0, undef, undef,     0,     0, SQL_TIME    ,    13, undef,    10 ]
);

sub type_info_all { [ \%index, @data ] }

%typeinfo = ();
%prefixes = ();
%suffixes = ();

for ( @data ) {
  $typeinfo{$_->[0]} = $_;
  $prefixes{$_->[1]} = $_->[3];
  $suffixes{$_->[1]} = $_->[4];
}

1;
