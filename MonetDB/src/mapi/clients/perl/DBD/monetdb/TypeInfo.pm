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
        [ "boolean",                  SQL_BIT,                      1,      undef,undef,undef,            1,0,2,0,    1,0,    undef,           undef,undef,SQL_BIT,     undef,undef,undef, ],
        [ "tinyint",                  SQL_TINYINT,                  2,      undef,undef,"precision",      1,0,2,0,    0,0,    undef,           0,    0,    SQL_TINYINT, undef,10,   undef, ],
        [ "bigint",                   -5,                           19,     undef,undef,"precision",      1,0,2,0,    0,0,    undef,           0,    0,    -5,          undef,10,   undef, ],
        [ "character",                SQL_CHAR,                     1000000,"'",  "'",  "length",         1,1,3,undef,0,0,    undef,           undef,undef,SQL_CHAR,    undef,undef,undef, ],
        [ "char",                     SQL_CHAR,                     1000000,"'",  "'",  "length",         1,1,3,undef,0,0,    undef,           undef,undef,SQL_CHAR,    undef,undef,undef, ],
        [ "numeric",                  SQL_NUMERIC,                  19,     undef,undef,"precision,scale",1,0,2,0,    0,0,    undef,           0,    19,   SQL_NUMERIC, undef,10,   undef, ],
        [ "decimal",                  SQL_DECIMAL,                  19,     undef,undef,"precision,scale",1,0,2,0,    0,0,    undef,           0,    19,   SQL_DECIMAL, undef,10,   undef, ],
        [ "integer",                  SQL_INTEGER,                  9,      undef,undef,"precision",      1,0,2,0,    0,0,    undef,           0,    0,    SQL_INTEGER, undef,10,   undef, ],
        [ "mediumint",                SQL_INTEGER,                  9,      undef,undef,"precision",      1,0,2,0,    0,0,    undef,           0,    0,    SQL_INTEGER, undef,10,   undef, ],
        [ "smallint",                 SQL_SMALLINT,                 4,      undef,undef,"precision",      1,0,2,0,    0,0,    undef,           0,    0,    SQL_SMALLINT,undef,10,   undef, ],
        [ "float",                    SQL_FLOAT,                    24,     undef,undef,undef,            1,0,2,0,    0,0,    undef,           0,    0,    SQL_FLOAT,   undef,2,    undef, ],
        [ "real",                     SQL_REAL,                     24,     undef,undef,undef,            1,0,2,0,    0,0,    undef,           0,    0,    SQL_REAL,    undef,2,    undef, ],
        [ "double",                   SQL_DOUBLE,                   53,     undef,undef,undef,            1,0,2,0,    0,0,    undef,           0,    0,    SQL_DOUBLE,  undef,2,    undef, ],
        [ "varchar",                  SQL_VARCHAR,                  1000000,"'",  "'",  "length",         1,1,3,undef,0,undef,undef,           undef,undef,SQL_VARCHAR, undef,undef,undef, ],
        [ "date",                     SQL_TYPE_DATE,                10,     "'",  "'",  undef,            1,0,2,undef,0,undef,undef,           undef,undef,SQL_DATE,    1,    undef,undef, ],
        [ "time",                     SQL_TYPE_TIME,                12,     "'",  "'",  undef,            1,0,2,undef,0,undef,undef,           undef,undef,SQL_DATE,    2,    undef,undef, ],
        [ "timestamp",                SQL_TYPE_TIMESTAMP,           23,     "'",  "'",  "precision",      1,0,2,undef,0,undef,undef,           undef,undef,SQL_DATE,    3,    undef,undef, ],
        [ "interval year",            SQL_INTERVAL_YEAR,            9,      "'",  "'",  "precision",      1,0,2,undef,0,undef,undef,           0,    0,    SQL_TIME,    1,    undef,9,     ],
        [ "interval month",           SQL_INTERVAL_MONTH,           10,     "'",  "'",  "precision",      1,0,2,undef,0,undef,"month_interval",0,    0,    SQL_TIME,    2,    undef,10,    ],
        [ "interval day",             SQL_INTERVAL_DAY,             5,      "'",  "'",  "precision",      1,0,2,undef,0,undef,undef,           0,    0,    SQL_TIME,    3,    undef,5,     ],
        [ "interval hour",            SQL_INTERVAL_HOUR,            6,      "'",  "'",  "precision",      1,0,2,undef,0,undef,undef,           0,    0,    SQL_TIME,    4,    undef,6,     ],
        [ "interval minute",          SQL_INTERVAL_MINUTE,          8,      "'",  "'",  "precision",      1,0,2,undef,0,undef,undef,           0,    0,    SQL_TIME,    5,    undef,8,     ],
        [ "interval second",          SQL_INTERVAL_SECOND,          10,     "'",  "'",  "precision",      1,0,2,undef,0,undef,"sec_interval",  0,    0,    SQL_TIME,    6,    undef,10,    ],
        [ "interval year to month",   SQL_INTERVAL_YEAR_TO_MONTH,   12,     "'",  "'",  "precision",      1,0,2,undef,0,undef,undef,           0,    0,    SQL_TIME,    7,    undef,9,     ],
        [ "interval day to hour",     SQL_INTERVAL_DAY_TO_HOUR,     8,      "'",  "'",  "precision",      1,0,2,undef,0,undef,undef,           0,    0,    SQL_TIME,    8,    undef,5,     ],
        [ "interval day to minute",   SQL_INTERVAL_DAY_TO_MINUTE,   11,     "'",  "'",  "precision",      1,0,2,undef,0,undef,undef,           0,    0,    SQL_TIME,    9,    undef,5,     ],
        [ "interval day to second",   SQL_INTERVAL_DAY_TO_SECOND,   14,     "'",  "'",  "precision",      1,0,2,undef,0,undef,undef,           0,    0,    SQL_TIME,    10,   undef,5,     ],
        [ "interval hour to minute",  SQL_INTERVAL_HOUR_TO_MINUTE,  9,      "'",  "'",  "precision",      1,0,2,undef,0,undef,undef,           0,    0,    SQL_TIME,    11,   undef,6,     ],
        [ "interval hour to second",  SQL_INTERVAL_HOUR_TO_SECOND,  12,     "'",  "'",  "precision",      1,0,2,undef,0,undef,undef,           0,    0,    SQL_TIME,    12,   undef,6,     ],
        [ "interval minute to second",SQL_INTERVAL_MINUTE_TO_SECOND,13,     "'",  "'",  "precision",      1,0,2,undef,0,undef,undef,           0,    0,    SQL_TIME,    13,   undef,10,    ],
);

$type_info_all = [ \%index, @data ];
sub type_info_all { [ \%index, @data ] }

1;
