# definition of types, for more info:
# http://monetdb.cwi.nl/projects/monetdb/SQL/Documentation/Data-Types.html
#
# TODO: check if this list is complete

CHAR = 0                        # (L) character string with length L
VARCHAR = 'varchar'                     # (L) string with atmost length L
CLOB = 2
BLOB = 'blob'
DECIMAL = 'decimal'                     # (P,S)
SMALLINT = 'smallint'                    # 16 bit integer
INT = 'int'                         # 32 bit integer
BIGINT = 'bigint'                      # 64 bit integer
serial = 8                      # special 64 bit integer (sequence generator)
REAL = 'real'                        # 32 bit floating point
DOUBLE = 'double'                     # 64 bit floating point
BOOLEAN = 'boolean'
DATE = 'date'
TIME = 'time'                       # (T) time of day
TIMESTAMP = 'timestamp'                  # (T) date concatenated with unique time
INTERVAL = 15                   # (Q) a temporal interval

# Not on the website:
TINYINT = 'tinyint'
SHORTINT = 'shortint'
MEDIUMINT = 'mediumint'
LONGINT = 'longint'
FLOAT = 'float'


# full names and aliases, spaces are replaced with underscores
CHARACTER = CHAR
CHARACTER_VARYING = VARCHAR
CHARACHTER_LARGE_OBJECT = CLOB
BINARY_LARGE_OBJECT = BLOB
NUMERIC = DECIMAL
DOUBLE_PRECISION = DOUBLE
