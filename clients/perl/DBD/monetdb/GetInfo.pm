# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

package DBD::monetdb::GetInfo;

use strict;
use DBD::monetdb();

my $sql_driver = 'monetdb';
my $sql_ver_fmt = '%02d.%02d.%04d';   # ODBC version string: ##.##.#####
my $sql_driver_ver = sprintf $sql_ver_fmt, split(/\./, $DBD::monetdb::VERSION), 0;

my @Keywords = qw(
BOOLEAN
COLUMNS
FLOOR
IMPORT
REAL
);

sub sql_keywords {
  return join ',', @Keywords;
}

sub sql_dbms_version {
  my $dbh = shift;
  return sprintf $sql_ver_fmt, 4, 6, 2;  # TODO: mapi_...
}

sub sql_data_source_name {
  my $dbh = shift;
  return "dbi:$sql_driver:" . $dbh->{Name};
}

sub sql_user_name {
  my $dbh = shift;
  return $dbh->{Username};
}

my %info = (
     20 => 'Y',                           # SQL_ACCESSIBLE_PROCEDURES
     19 => 'N',                           # SQL_ACCESSIBLE_TABLES
      0 => 0,                             # SQL_ACTIVE_CONNECTIONS
    116 => 0,                             # SQL_ACTIVE_ENVIRONMENTS
      1 => 0,                             # SQL_ACTIVE_STATEMENTS
    169 => 127,                           # SQL_AGGREGATE_FUNCTIONS
    117 => 0,                             # SQL_ALTER_DOMAIN
     86 => 55656,                         # SQL_ALTER_TABLE
  10021 => 0,                             # SQL_ASYNC_MODE
    120 => 2,                             # SQL_BATCH_ROW_COUNT
    121 => 3,                             # SQL_BATCH_SUPPORT
     82 => 0,                             # SQL_BOOKMARK_PERSISTENCE
    114 => 0,                             # SQL_CATALOG_LOCATION
  10003 => 'N',                           # SQL_CATALOG_NAME
     41 => '',                            # SQL_CATALOG_NAME_SEPARATOR
     42 => '',                            # SQL_CATALOG_TERM
     92 => 0,                             # SQL_CATALOG_USAGE
  10004 => 'UTF-8',                       # SQL_COLLATING_SEQUENCE
  10004 => 'UTF-8',                       # SQL_COLLATION_SEQ
     87 => 'Y',                           # SQL_COLUMN_ALIAS
     22 => 0,                             # SQL_CONCAT_NULL_BEHAVIOR
     53 => 2097151,                       # SQL_CONVERT_BIGINT
     54 => 2097151,                       # SQL_CONVERT_BINARY
     55 => 2097151,                       # SQL_CONVERT_BIT
     56 => 2097151,                       # SQL_CONVERT_CHAR
     57 => 2097151,                       # SQL_CONVERT_DATE
     58 => 2097151,                       # SQL_CONVERT_DECIMAL
     59 => 2097151,                       # SQL_CONVERT_DOUBLE
     60 => 2097151,                       # SQL_CONVERT_FLOAT
     48 => 3,                             # SQL_CONVERT_FUNCTIONS
#   173 => undef,                         # SQL_CONVERT_GUID
     61 => 2097151,                       # SQL_CONVERT_INTEGER
    123 => 2097151,                       # SQL_CONVERT_INTERVAL_DAY_TIME
    124 => 2097151,                       # SQL_CONVERT_INTERVAL_YEAR_MONTH
     71 => 2097151,                       # SQL_CONVERT_LONGVARBINARY
     62 => 2097151,                       # SQL_CONVERT_LONGVARCHAR
     63 => 2097151,                       # SQL_CONVERT_NUMERIC
     64 => 2097151,                       # SQL_CONVERT_REAL
     65 => 2097151,                       # SQL_CONVERT_SMALLINT
     66 => 2097151,                       # SQL_CONVERT_TIME
     67 => 2097151,                       # SQL_CONVERT_TIMESTAMP
     68 => 2097151,                       # SQL_CONVERT_TINYINT
     69 => 2097151,                       # SQL_CONVERT_VARBINARY
     70 => 2097151,                       # SQL_CONVERT_VARCHAR
#   122 => undef,                         # SQL_CONVERT_WCHAR
#   125 => undef,                         # SQL_CONVERT_WLONGVARCHAR
#   126 => undef,                         # SQL_CONVERT_WVARCHAR
     74 => 2,                             # SQL_CORRELATION_NAME
    127 => 0,                             # SQL_CREATE_ASSERTION
    128 => 0,                             # SQL_CREATE_CHARACTER_SET
    129 => 0,                             # SQL_CREATE_COLLATION
    130 => 0,                             # SQL_CREATE_DOMAIN
    131 => 3,                             # SQL_CREATE_SCHEMA
    132 => 13851,                         # SQL_CREATE_TABLE
    133 => 0,                             # SQL_CREATE_TRANSLATION
    134 => 3,                             # SQL_CREATE_VIEW
     23 => 0,                             # SQL_CURSOR_COMMIT_BEHAVIOR
     24 => 0,                             # SQL_CURSOR_ROLLBACK_BEHAVIOR
  10001 => 1,                             # SQL_CURSOR_SENSITIVITY
      2 => \&sql_data_source_name,        # SQL_DATA_SOURCE_NAME
     25 => 'N',                           # SQL_DATA_SOURCE_READ_ONLY
    119 => 0,                             # SQL_DATETIME_LITERALS
     17 => 'MonetDB',                     # SQL_DBMS_NAME
     18 => \&sql_dbms_version,            # SQL_DBMS_VERSION
    170 => 0,                             # SQL_DDL_INDEX
     26 => 2,                             # SQL_DEFAULT_TRANSACTION_ISOLATION
     26 => 2,                             # SQL_DEFAULT_TXN_ISOLATION
  10002 => 'N',                           # SQL_DESCRIBE_PARAMETER
#-  171 => '03.52.6019.0000',             # SQL_DM_VER
#-    3 => 28510912,                      # SQL_DRIVER_HDBC
#   135 => undef,                         # SQL_DRIVER_HDESC
#-    4 => 28510880,                      # SQL_DRIVER_HENV
#    76 => undef,                         # SQL_DRIVER_HLIB
#     5 => undef,                         # SQL_DRIVER_HSTMT
      6 => $INC{'DBD/monetdb.pm'},        # SQL_DRIVER_NAME
#-   77 => '03.52',                       # SQL_DRIVER_ODBC_VER
      7 => $sql_driver_ver,               # SQL_DRIVER_VER
    136 => 0,                             # SQL_DROP_ASSERTION
    137 => 0,                             # SQL_DROP_CHARACTER_SET
    138 => 0,                             # SQL_DROP_COLLATION
    139 => 0,                             # SQL_DROP_DOMAIN
    140 => 0,                             # SQL_DROP_SCHEMA
    141 => 0,                             # SQL_DROP_TABLE
    142 => 0,                             # SQL_DROP_TRANSLATION
    143 => 0,                             # SQL_DROP_VIEW
    144 => 0,                             # SQL_DYNAMIC_CURSOR_ATTRIBUTES1
    145 => 0,                             # SQL_DYNAMIC_CURSOR_ATTRIBUTES2
     27 => 'Y',                           # SQL_EXPRESSIONS_IN_ORDERBY
      8 => 1,                             # SQL_FETCH_DIRECTION
     84 => 0,                             # SQL_FILE_USAGE
    146 => 0,                             # SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1
    147 => 0,                             # SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2
     81 => 15,                            # SQL_GETDATA_EXTENSIONS
     88 => 3,                             # SQL_GROUP_BY
     28 => 2,                             # SQL_IDENTIFIER_CASE
     29 => '"',                           # SQL_IDENTIFIER_QUOTE_CHAR
#   148 => undef,                         # SQL_INDEX_KEYWORDS
    149 => 0,                             # SQL_INFO_SCHEMA_VIEWS
    172 => 0,                             # SQL_INSERT_STATEMENT
     73 => 'N',                           # SQL_INTEGRITY
    150 => 0,                             # SQL_KEYSET_CURSOR_ATTRIBUTES1
    151 => 0,                             # SQL_KEYSET_CURSOR_ATTRIBUTES2
     89 => \&sql_keywords,                # SQL_KEYWORDS
    113 => 'Y',                           # SQL_LIKE_ESCAPE_CLAUSE
     78 => 0,                             # SQL_LOCK_TYPES
     34 => 255,                           # SQL_MAXIMUM_CATALOG_NAME_LENGTH
     97 => 0,                             # SQL_MAXIMUM_COLUMNS_IN_GROUP_BY
     98 => 0,                             # SQL_MAXIMUM_COLUMNS_IN_INDEX
     99 => 0,                             # SQL_MAXIMUM_COLUMNS_IN_ORDER_BY
    100 => 0,                             # SQL_MAXIMUM_COLUMNS_IN_SELECT
    101 => 0,                             # SQL_MAXIMUM_COLUMNS_IN_TABLE
     30 => 255,                           # SQL_MAXIMUM_COLUMN_NAME_LENGTH
      1 => 0,                             # SQL_MAXIMUM_CONCURRENT_ACTIVITIES
     31 => 0,                             # SQL_MAXIMUM_CURSOR_NAME_LENGTH
      0 => 0,                             # SQL_MAXIMUM_DRIVER_CONNECTIONS
  10005 => 0,                             # SQL_MAXIMUM_IDENTIFIER_LENGTH
    102 => 0,                             # SQL_MAXIMUM_INDEX_SIZE
    104 => 0,                             # SQL_MAXIMUM_ROW_SIZE
     32 => 255,                           # SQL_MAXIMUM_SCHEMA_NAME_LENGTH
    105 => 0,                             # SQL_MAXIMUM_STATEMENT_LENGTH
# 20000 => undef,                         # SQL_MAXIMUM_STMT_OCTETS
# 20001 => undef,                         # SQL_MAXIMUM_STMT_OCTETS_DATA
# 20002 => undef,                         # SQL_MAXIMUM_STMT_OCTETS_SCHEMA
    106 => 0,                             # SQL_MAXIMUM_TABLES_IN_SELECT
     35 => 255,                           # SQL_MAXIMUM_TABLE_NAME_LENGTH
    107 => 0,                             # SQL_MAXIMUM_USER_NAME_LENGTH
  10022 => 0,                             # SQL_MAX_ASYNC_CONCURRENT_STATEMENTS
    112 => 0,                             # SQL_MAX_BINARY_LITERAL_LEN
     34 => 255,                           # SQL_MAX_CATALOG_NAME_LEN
    108 => 1048576,                       # SQL_MAX_CHAR_LITERAL_LEN
     97 => 0,                             # SQL_MAX_COLUMNS_IN_GROUP_BY
     98 => 0,                             # SQL_MAX_COLUMNS_IN_INDEX
     99 => 0,                             # SQL_MAX_COLUMNS_IN_ORDER_BY
    100 => 0,                             # SQL_MAX_COLUMNS_IN_SELECT
    101 => 0,                             # SQL_MAX_COLUMNS_IN_TABLE
     30 => 255,                           # SQL_MAX_COLUMN_NAME_LEN
      1 => 0,                             # SQL_MAX_CONCURRENT_ACTIVITIES
     31 => 0,                             # SQL_MAX_CURSOR_NAME_LEN
      0 => 0,                             # SQL_MAX_DRIVER_CONNECTIONS
  10005 => 0,                             # SQL_MAX_IDENTIFIER_LEN
    102 => 0,                             # SQL_MAX_INDEX_SIZE
     32 => 255,                           # SQL_MAX_OWNER_NAME_LEN
     33 => 0,                             # SQL_MAX_PROCEDURE_NAME_LEN
     34 => 255,                           # SQL_MAX_QUALIFIER_NAME_LEN
    104 => 0,                             # SQL_MAX_ROW_SIZE
    103 => 'N',                           # SQL_MAX_ROW_SIZE_INCLUDES_LONG
     32 => 255,                           # SQL_MAX_SCHEMA_NAME_LEN
    105 => 0,                             # SQL_MAX_STATEMENT_LEN
    106 => 0,                             # SQL_MAX_TABLES_IN_SELECT
     35 => 255,                           # SQL_MAX_TABLE_NAME_LEN
    107 => 0,                             # SQL_MAX_USER_NAME_LEN
     37 => 'Y',                           # SQL_MULTIPLE_ACTIVE_TXN
     36 => 'N',                           # SQL_MULT_RESULT_SETS
    111 => 'Y',                           # SQL_NEED_LONG_DATA_LEN
     75 => 1,                             # SQL_NON_NULLABLE_COLUMNS
     85 => 1,                             # SQL_NULL_COLLATION
     49 => 16777215,                      # SQL_NUMERIC_FUNCTIONS
      9 => 1,                             # SQL_ODBC_API_CONFORMANCE
    152 => 1,                             # SQL_ODBC_INTERFACE_CONFORMANCE
     12 => 1,                             # SQL_ODBC_SAG_CLI_CONFORMANCE
     15 => 1,                             # SQL_ODBC_SQL_CONFORMANCE
     73 => 'N',                           # SQL_ODBC_SQL_OPT_IEF
#-   10 => '03.52.0000',                  # SQL_ODBC_VER
    115 => 0,                             # SQL_OJ_CAPABILITIES
     90 => 'N',                           # SQL_ORDER_BY_COLUMNS_IN_SELECT
     38 => 'Y',                           # SQL_OUTER_JOINS
    115 => 0,                             # SQL_OUTER_JOIN_CAPABILITIES
     39 => '',                            # SQL_OWNER_TERM
     91 => 0,                             # SQL_OWNER_USAGE
    153 => 0,                             # SQL_PARAM_ARRAY_ROW_COUNTS
    154 => 0,                             # SQL_PARAM_ARRAY_SELECTS
     80 => 4,                             # SQL_POSITIONED_STATEMENTS
     79 => 0,                             # SQL_POS_OPERATIONS
     21 => 'N',                           # SQL_PROCEDURES
     40 => '',                            # SQL_PROCEDURE_TERM
    114 => 0,                             # SQL_QUALIFIER_LOCATION
     41 => '',                            # SQL_QUALIFIER_NAME_SEPARATOR
     42 => '',                            # SQL_QUALIFIER_TERM
     92 => 0,                             # SQL_QUALIFIER_USAGE
     93 => 3,                             # SQL_QUOTED_IDENTIFIER_CASE
     11 => 'N',                           # SQL_ROW_UPDATES
     39 => '',                            # SQL_SCHEMA_TERM
     91 => 0,                             # SQL_SCHEMA_USAGE
     43 => 1,                             # SQL_SCROLL_CONCURRENCY
     44 => 16,                            # SQL_SCROLL_OPTIONS
     14 => '',                            # SQL_SEARCH_PATTERN_ESCAPE
     13 => 'MonetDB',                     # SQL_SERVER_NAME
     94 => '`!#$;:\'<>',                  # SQL_SPECIAL_CHARACTERS
#   155 => undef,                         # SQL_SQL92_DATETIME_FUNCTIONS
#   156 => undef,                         # SQL_SQL92_FOREIGN_KEY_DELETE_RULE
#   157 => undef,                         # SQL_SQL92_FOREIGN_KEY_UPDATE_RULE
#   158 => undef,                         # SQL_SQL92_GRANT
#   159 => undef,                         # SQL_SQL92_NUMERIC_VALUE_FUNCTIONS
#   160 => undef,                         # SQL_SQL92_PREDICATES
#   161 => undef,                         # SQL_SQL92_RELATIONAL_JOIN_OPERATORS
#   162 => undef,                         # SQL_SQL92_REVOKE
#   163 => undef,                         # SQL_SQL92_ROW_VALUE_CONSTRUCTOR
#   164 => undef,                         # SQL_SQL92_STRING_FUNCTIONS
#   165 => undef,                         # SQL_SQL92_VALUE_EXPRESSIONS
    118 => 8,                             # SQL_SQL_CONFORMANCE
#   166 => undef,                         # SQL_STANDARD_CLI_CONFORMANCE
    167 => 583,                           # SQL_STATIC_CURSOR_ATTRIBUTES1
    168 => 4096,                          # SQL_STATIC_CURSOR_ATTRIBUTES2
     83 => 0,                             # SQL_STATIC_SENSITIVITY
     50 => 458751,                        # SQL_STRING_FUNCTIONS
     95 => 23,                            # SQL_SUBQUERIES
     51 => 7,                             # SQL_SYSTEM_FUNCTIONS
     45 => '',                            # SQL_TABLE_TERM
    109 => 0,                             # SQL_TIMEDATE_ADD_INTERVALS
    110 => 0,                             # SQL_TIMEDATE_DIFF_INTERVALS
     52 => 131071,                        # SQL_TIMEDATE_FUNCTIONS
     46 => 2,                             # SQL_TRANSACTION_CAPABLE
     72 => 4,                             # SQL_TRANSACTION_ISOLATION_OPTION
     46 => 2,                             # SQL_TXN_CAPABLE
     72 => 4,                             # SQL_TXN_ISOLATION_OPTION
     96 => 1,                             # SQL_UNION
     96 => 1,                             # SQL_UNION_STATEMENT
     47 => \&sql_user_name,               # SQL_USER_NAME
  10000 => '',                            # SQL_XOPEN_CLI_YEAR
);

sub get_info {
  my ($dbh, $info_type) = @_;
  my $value = $info{int($info_type)};
  $value = $value->($dbh) if ref $value eq 'CODE';
  return $value;
}

1;
