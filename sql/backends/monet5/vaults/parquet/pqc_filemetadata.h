/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _PQC_FILEMETADATA_H_
#define _PQC_FILEMETADATA_H_

#include <stdbool.h>
#include <sys/types.h>

/* get windows defa */
#define pqc_export extern

#define P_CRITICAL 1
#define P_ERROR 2
#define P_WARNING 3
#define P_INFO 4
#define P_DEBUG 5

typedef enum FieldRepetitionType {
	FRT_UNKNOWN = -1,
	FRT_REQUIRED = 0,
	FRT_OPTIONAL = 1,
	FRT_REPEATED = 2
} FieldRepetitionType;

typedef enum PhysicalType {
	PT_UNKNOWN = -1,
	PT_BOOLEAN = 0,
	PT_INT32 = 1,
	PT_INT64 = 2,
	PT_INT96 = 3,  // deprecated, only used by legacy implementations.
	PT_FLOAT = 4,
	PT_DOUBLE = 5,
	PT_BYTE_ARRAY = 6,
	PT_FIXED_LEN_BYTE_ARRAY = 7
} PhysicalType;

typedef enum convertedtype {
	CT_UNKNOWN = -1,
	CT_UTF8 = 0, 		// a BYTE_ARRAY actually contains UTF8 encoded chars
	CT_MAP = 1, 		// a map is converted as an optional field containing a repeated key/value pair
	CT_MAP_KEY_VALUE = 2, 	// a key/value pair is converted into a group of two fields
	CT_LIST = 3, 		// a list is converted into an optional field containing a repeated field for its values
	CT_ENUM = 4, 		// an enum is converted into a binary field

	// A decimal value.
	//
	// This may be used to annotate binary or fixed primitive types. The
	// underlying byte array stores the unscaled value encoded as two's
	// complement using big-endian byte order (the most significant byte is the
	// zeroth element). The value of the decimal is the value * 10^{-scale}.
	//
	// This must be accompanied by a (maximum) precision and a scale in the
	// SchemaElement. The precision specifies the number of digits in the decimal
	// and the scale stores the location of the decimal point. For example 1.23
	// would have precision 3 (3 total digits) and scale 2 (the decimal point is
	// 2 digits over).
	CT_DECIMAL = 5,
	CT_DATE = 6, 		// Stored as days since Unix epoch, encoded as the INT32 physical type.
	CT_TIME_MILLIS = 7, 	// The total number of milliseconds since midnight. The value is stored as an INT32 physical type.
	CT_TIME_MICROS = 8, 	// The total number of microseconds since midnight. The value is stored as an INT64 physical type.
	CT_TIMESTAMP_MILLIS = 9, // Date and time recorded as milliseconds since the Unix epoch. Recorded as a physical type of INT64.
	CT_TIMESTAMP_MICROS = 10, // Date and time recorded as microseconds since the Unix epoch. The value is stored as an INT64 physical type.

	// unsigned int
	CT_UINT_8 = 11,
	CT_UINT_16 = 12,
	CT_UINT_32 = 13,
	CT_UINT_64 = 14,

	// signed int
	CT_INT_8 = 15,
	CT_INT_16 = 16,
	CT_INT_32 = 17,
	CT_INT_64 = 18,

	CT_JSON = 19, // A JSON document embedded within a single UTF8 column.
	CT_BSON = 20, // A BSON document embedded within a single BINARY column.

	// An interval of time
	//
	// This type annotates data stored as a FIXED_LEN_BYTE_ARRAY of length 12
	// This data is composed of three separate little endian unsigned
	// integers.  Each stores a component of a duration of time.  The first
	// integer identifies the number of months associated with the duration,
	// the second identifies the number of days associated with the duration
	// and the third identifies the number of milliseconds associated with
	// the provided duration.  This duration of time is independent of any
	// particular timezone or date.
	CT_INTERVAL = 21
} convertedtype;

typedef enum logicaltype {
	LT_UNKNOWN = -1,
	stringtype = 1, 		// use ConvertedType UTF
	maptype = 2, 			// use ConvertedType MAP
	listtype = 3, 			// use ConvertedType LIST
  	enumtype = 4, 			// use ConvertedType ENUM
  	decimaltype = 5, 		// use ConvertedType DECIMAL + SchemaElement.{scale, precision}
  	datetype = 6,			// use ConvertedType DATE
  	// use ConvertedType TIME_MICROS for TIME(isAdjustedToUTC = *, unit = MICROS)
  	// use ConvertedType TIME_MILLIS for TIME(isAdjustedToUTC = *, unit = MILLIS)
  	timetype = 7,
  	// use ConvertedType TIMESTAMP_MICROS for TIMESTAMP(isAdjustedToUTC = *, unit = MICROS)
  	// use ConvertedType TIMESTAMP_MILLIS for TIMESTAMP(isAdjustedToUTC = *, unit = MILLIS)
  	timestamptype = 8,
  	intervaltype = 9,
  	inttype = 10,         		// use ConvertedType INT_* or UINT_*
  	nulltype = 11, 			// no compatible ConvertedType
  	jsontype = 12, 			// use ConvertedType JSON
  	bsontype = 13, 			// use ConvertedType BSON
  	uuidtype = 14, 			// no compatible ConvertedType
	float16type = 15,
	varianttype = 16,
  	floattype = 25 			// no compatible ConvertedType
} logicaltype;

typedef enum {
    LOGICAL_TYPE_STRING = 1,      // Field ID for STRING
    LOGICAL_TYPE_MAP = 2,         // Field ID for MAP
    LOGICAL_TYPE_LIST = 3,        // Field ID for LIST
    LOGICAL_TYPE_ENUM = 4,        // Field ID for ENUM
    LOGICAL_TYPE_DECIMAL = 5,     // Field ID for DECIMAL
    LOGICAL_TYPE_DATE = 6,        // Field ID for DATE
    LOGICAL_TYPE_TIME = 7,        // Field ID for TIME
    LOGICAL_TYPE_TIMESTAMP = 8,   // Field ID for TIMESTAMP
    // Field ID 9 is reserved for INTERVAL
    LOGICAL_TYPE_INTEGER = 10,    // Field ID for INTEGER
    LOGICAL_TYPE_UNKNOWN = 11,    // Field ID for UNKNOWN
    LOGICAL_TYPE_JSON = 12,       // Field ID for JSON
    LOGICAL_TYPE_BSON = 13,       // Field ID for BSON
    LOGICAL_TYPE_UUID = 14,       // Field ID for UUID
    LOGICAL_TYPE_FLOAT16 = 15     // Field ID for FLOAT16
} LogicalTypeFieldID;

typedef enum {
    SCHEMA_ELEMENT_TYPE = 1,            // Field ID for 'type'
    SCHEMA_ELEMENT_TYPE_LENGTH = 2,     // Field ID for 'type_length'
    SCHEMA_ELEMENT_REPETITION_TYPE = 3, // Field ID for 'repetition_type'
    SCHEMA_ELEMENT_NAME = 4,            // Field ID for 'name'
    SCHEMA_ELEMENT_NUM_CHILDREN = 5,    // Field ID for 'num_children'
    SCHEMA_ELEMENT_CONVERTED_TYPE = 6,  // Field ID for 'converted_type'
    SCHEMA_ELEMENT_SCALE = 7,           // Field ID for 'scale'
    SCHEMA_ELEMENT_PRECISION = 8,       // Field ID for 'precision'
    SCHEMA_ELEMENT_FIELD_ID = 9,        // Field ID for 'field_id'
    SCHEMA_ELEMENT_LOGICAL_TYPE = 10    // Field ID for 'logicalType'
} SchemaElementFieldID;

typedef enum {
    FILE_METADATA_VERSION = 1,                 // Field ID for 'version'
    FILE_METADATA_SCHEMA = 2,                  // Field ID for 'schema'
    FILE_METADATA_NUM_ROWS = 3,                // Field ID for 'num_rows'
    FILE_METADATA_ROW_GROUPS = 4,              // Field ID for 'row_groups'
    FILE_METADATA_KEY_VALUE_METADATA = 5,      // Field ID for 'key_value_metadata'
    FILE_METADATA_CREATED_BY = 6,              // Field ID for 'created_by'
    FILE_METADATA_COLUMN_ORDERS = 7,           // Field ID for 'column_orders'
    FILE_METADATA_ENCRYPTION_ALGORITHM = 8,    // Field ID for 'encryption_algorithm'
    FILE_METADATA_FOOTER_SIGNING_KEY_METADATA = 9 // Field ID for 'footer_signing_key_metadata'
} FileMetaDataFieldID;

typedef enum {
    STATISTICS_MAX = 1,               // optional binary max; DEPRECATED: Use max_value.
    STATISTICS_MIN = 2,               // optional binary min; DEPRECATED: Use min_value.
    STATISTICS_NULL_COUNT = 3,        // optional i64 null_count;
    STATISTICS_DISTINCT_COUNT = 4,    // optional i64 distinct_count;
    STATISTICS_MAX_VALUE = 5,         // optional binary max_value;
    STATISTICS_MIN_VALUE = 6,         // optional binary min_value;
    STATISTICS_IS_MAX_VALUE_EXACT = 7, // optional bool is_max_value_exact;
    STATISTICS_IS_MIN_VALUE_EXACT = 8  // optional bool is_min_value_exact;
} StatisticsFields;

typedef enum {
    PAGE_ENCODING_STATS_PAGE_TYPE = 1, // required PageType page_type;
    PAGE_ENCODING_STATS_ENCODING = 2,  // required Encoding encoding;
    PAGE_ENCODING_STATS_COUNT = 3      // required i32 count;
} PageEncodingStatsFields;

typedef enum {
    COLUMN_META_DATA_TYPE = 1,                   // required Type type
    COLUMN_META_DATA_ENCODINGS = 2,              // required list<Encoding> encodings
    COLUMN_META_DATA_PATH_IN_SCHEMA = 3,         // required list<string> path_in_schema
    COLUMN_META_DATA_CODEC = 4,                  // required CompressionCodec codec
    COLUMN_META_DATA_NUM_VALUES = 5,             // required i64 num_values
    COLUMN_META_DATA_TOTAL_UNCOMPRESSED_SIZE = 6,// required i64 total_uncompressed_size
    COLUMN_META_DATA_TOTAL_COMPRESSED_SIZE = 7,  // required i64 total_compressed_size
    COLUMN_META_DATA_KEY_VALUE_METADATA = 8,     // optional list<KeyValue> key_value_metadata
    COLUMN_META_DATA_DATA_PAGE_OFFSET = 9,       // required i64 data_page_offset
    COLUMN_META_DATA_INDEX_PAGE_OFFSET = 10,     // optional i64 index_page_offset
    COLUMN_META_DATA_DICTIONARY_PAGE_OFFSET = 11,// optional i64 dictionary_page_offset
    COLUMN_META_DATA_STATISTICS = 12,            // optional Statistics statistics
    COLUMN_META_DATA_ENCODING_STATS = 13,        // optional list<PageEncodingStats> encoding_stats
    COLUMN_META_DATA_BLOOM_FILTER_OFFSET = 14,   // optional i64 bloom_filter_offset
    COLUMN_META_DATA_BLOOM_FILTER_LENGTH = 15,   // optional i32 bloom_filter_length
    COLUMN_META_DATA_SIZE_STATISTICS = 16        // optional SizeStatistics size_statistics
} ColumnMetaDataFields;

typedef enum {
    COLUMN_CHUNK_FILE_PATH = 1,                  // optional string file_path
    COLUMN_CHUNK_FILE_OFFSET = 2,                // required i64 file_offset = 0
    COLUMN_CHUNK_META_DATA = 3,                  // optional ColumnMetaData meta_data
    COLUMN_CHUNK_OFFSET_INDEX_OFFSET = 4,        // optional i64 offset_index_offset
    COLUMN_CHUNK_OFFSET_INDEX_LENGTH = 5,        // optional i32 offset_index_length
    COLUMN_CHUNK_COLUMN_INDEX_OFFSET = 6,        // optional i64 column_index_offset
    COLUMN_CHUNK_COLUMN_INDEX_LENGTH = 7,        // optional i32 column_index_length
    COLUMN_CHUNK_CRYPTO_METADATA = 8,            // optional ColumnCryptoMetaData crypto_metadata
    COLUMN_CHUNK_ENCRYPTED_COLUMN_METADATA = 9   // optional binary encrypted_column_metadata
} ColumnChunkFields;

typedef enum {
    SORTING_COLUMN_COLUMN_IDX = 1,    // required i32 column_idx
    SORTING_COLUMN_DESCENDING = 2,    // required bool descending
    SORTING_COLUMN_NULLS_FIRST = 3    // required bool nulls_first
} SortingColumnFields;

typedef enum {
    ROW_GROUP_COLUMNS = 1,                   // required list<ColumnChunk> columns
    ROW_GROUP_TOTAL_BYTE_SIZE = 2,           // required i64 total_byte_size
    ROW_GROUP_NUM_ROWS = 3,                  // required i64 num_rows
    ROW_GROUP_SORTING_COLUMNS = 4,           // optional list<SortingColumn> sorting_columns
    ROW_GROUP_FILE_OFFSET = 5,               // optional i64 file_offset
    ROW_GROUP_TOTAL_COMPRESSED_SIZE = 6,     // optional i64 total_compressed_size
    ROW_GROUP_ORDINAL = 7                    // optional i16 ordinal
} RowGroupFields;

typedef enum {
    TIME_UNIT_MILLIS = 1,  // MilliSeconds
    TIME_UNIT_MICROS = 2,  // MicroSeconds
    TIME_UNIT_NANOS = 3    // NanoSeconds
} TimeUnitFields;

typedef enum {
    TIME_TYPE_IS_ADJUSTED_TO_UTC = 1,  // required bool isAdjustedToUTC
    TIME_TYPE_UNIT = 2                 // required TimeUnit unit
} TimeTypeFields;

typedef enum {
    TIMESTAMP_TYPE_IS_ADJUSTED_TO_UTC = 1,  // required bool isAdjustedToUTC
    TIMESTAMP_TYPE_UNIT = 2                 // required TimeUnit unit
} TimestampTypeFields;

typedef enum {
    DECIMAL_TYPE_SCALE = 1,      // required i32 scale
    DECIMAL_TYPE_PRECISION = 2   // required i32 precision
} DecimalTypeFields;

typedef enum {
    INT_TYPE_BIT_WIDTH = 1,   // required i8 bitWidth
    INT_TYPE_IS_SIGNED = 2    // required bool isSigned
} IntTypeFields;

typedef enum {
    KEY_VALUE_KEY = 1,         // required string key
    KEY_VALUE_VALUE = 2        // optional string value
} KeyValueFields;

typedef enum {
    PAGE_HEADER_TYPE = 1,                  // PageType type
    PAGE_HEADER_UNCOMPRESSED_PAGE_SIZE = 2, // i32 uncompressed_page_size
    PAGE_HEADER_COMPRESSED_PAGE_SIZE = 3,   // i32 compressed_page_size
    PAGE_HEADER_CRC = 4,                    // i32 crc (optional)
    PAGE_HEADER_DATA_PAGE_HEADER = 5,       // DataPageHeader data_page_header (optional)
    PAGE_HEADER_INDEX_PAGE_HEADER = 6,      // IndexPageHeader index_page_header (optional)
    PAGE_HEADER_DICTIONARY_PAGE_HEADER = 7, // DictionaryPageHeader dictionary_page_header (optional)
    PAGE_HEADER_DATA_PAGE_HEADER_V2 = 8     // DataPageHeaderV2 data_page_header_v2 (optional)
} PageHeaderFields;

typedef enum {
    DATA_PAGE_HEADER_NUM_VALUES = 1,                 // i32 num_values
    DATA_PAGE_HEADER_ENCODING = 2,                   // Encoding encoding
    DATA_PAGE_HEADER_DEFINITION_LEVEL_ENCODING = 3,  // Encoding definition_level_encoding
    DATA_PAGE_HEADER_REPETITION_LEVEL_ENCODING = 4,  // Encoding repetition_level_encoding
    DATA_PAGE_HEADER_STATISTICS = 5                  // Statistics statistics (optional)
} DataPageHeaderFields;

typedef enum {
    DATA_PAGE_HEADER_V2_NUM_VALUES = 1,                       // i32 num_values
    DATA_PAGE_HEADER_V2_NUM_NULLS = 2,                        // i32 num_nulls
    DATA_PAGE_HEADER_V2_NUM_ROWS = 3,                         // i32 num_rows
    DATA_PAGE_HEADER_V2_ENCODING = 4,                         // Encoding encoding
    DATA_PAGE_HEADER_V2_DEFINITION_LEVELS_BYTE_LENGTH = 5,    // i32 definition_levels_byte_length
    DATA_PAGE_HEADER_V2_REPETITION_LEVELS_BYTE_LENGTH = 6,    // i32 repetition_levels_byte_length
    DATA_PAGE_HEADER_V2_IS_COMPRESSED = 7,                    // bool is_compressed (optional)
    DATA_PAGE_HEADER_V2_STATISTICS = 8                        // Statistics statistics (optional)
} DataPageHeaderV2Fields;

typedef enum {
    DICTIONARY_PAGE_HEADER_NUM_VALUES = 1,  // i32 num_values
    DICTIONARY_PAGE_HEADER_ENCODING = 2,    // Encoding encoding
    DICTIONARY_PAGE_HEADER_IS_SORTED = 3    // bool is_sorted (optional)
} DictionaryPageHeaderFields;

typedef enum PageType {
  DATA_PAGE = 0,
  INDEX_PAGE = 1,
  DICTIONARY_PAGE = 2,
  DATA_PAGE_V2 = 3,
} PageType;

typedef enum CompressionCodec {
  CC_UNCOMPRESSED = 0,
  CC_SNAPPY = 1,
  CC_GZIP = 2,
  CC_LZO = 3,
  CC_BROTLI = 4,  // Added in 2.4
  CC_LZ4 = 5,     // DEPRECATED (Added in 2.4)
  CC_ZSTD = 6,    // Added in 2.4
  CC_LZ4_RAW = 7, // Added in 2.9
} CompressionCodec;

typedef enum Encoding {
  /** Default encoding.
   * BOOLEAN - 1 bit per value. 0 is false; 1 is true.
   * INT32 - 4 bytes per value.  Stored as little-endian.
   * INT64 - 8 bytes per value.  Stored as little-endian.
   * FLOAT - 4 bytes per value.  IEEE. Stored as little-endian.
   * DOUBLE - 8 bytes per value.  IEEE. Stored as little-endian.
   * BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
   * FIXED_LEN_BYTE_ARRAY - Just the bytes.
   */
  PLAIN = 0,

  /** Group VarInt encoding for INT32/INT64.
   * This encoding is deprecated. It was never used
   */
  //  GROUP_VAR_INT = 1;

  /**
   * Deprecated: Dictionary encoding. The values in the dictionary are encoded in the
   * plain type.
   * in a data page use RLE_DICTIONARY instead.
   * in a Dictionary page use PLAIN instead
   */
  PLAIN_DICTIONARY = 2,

  /** Group packed run length encoding. Usable for definition/repetition levels
   * encoding and Booleans (on one bit: 0 is false; 1 is true.)
   */
  RLE = 3,

  /** Bit packed encoding.  This can only be used if the data has a known max
   * width.  Usable for definition/repetition levels encoding.
   */
  BIT_PACKED = 4,

  /** Delta encoding for integers. This can be used for int columns and works best
   * on sorted data
   */
  DELTA_BINARY_PACKED = 5,

  /** Encoding for byte arrays to separate the length values and the data. The lengths
   * are encoded using DELTA_BINARY_PACKED
   */
  DELTA_LENGTH_BYTE_ARRAY = 6,

  /** Incremental-encoded byte array. Prefix lengths are encoded using DELTA_BINARY_PACKED.
   * Suffixes are stored as delta length byte arrays.
   */
  DELTA_BYTE_ARRAY = 7,

  /** Dictionary encoding: the ids are encoded using the RLE encoding
   */
  RLE_DICTIONARY = 8,

  /** Encoding for fixed-width data (FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY).
      K byte-streams are created where K is the size in bytes of the data type.
      The individual bytes of a value are scattered to the corresponding stream and
      the streams are concatenated.
      This itself does not reduce the size of the data but can lead to better compression
      afterwards.

      Added in 2.8 for FLOAT and DOUBLE.
      Support for INT32, INT64 and FIXED_LEN_BYTE_ARRAY added in 2.11.
   */
  BYTE_STREAM_SPLIT = 9
} Encoding;

typedef struct pqc_schema_element {
	PhysicalType physical_type;
	uint32_t type_length;
	convertedtype converted_type;
	logicaltype type;	/* generalized types, ie type (logicaltype nr), precision, scale combinations */
	int ccnr;  // column chunk number?
	int scale;
	int precision;
	int size;		/* type size, int-8/16/32 are stored in a 32 bit, etc */
	bool binary;
	bool isSigned;
	bool isAdjustedToUTC;

	FieldRepetitionType repetition; /* required (NOT NULL), optional (NULL), repeated */
	char *name;
	int nchildren;
	int curchild; /* only needed during meta data parsing */
	struct pqc_schema_element **elements;
	struct pqc_schema_element *parent;
	/* optional logical type */
} pqc_schema_element;

typedef struct pqc_keyvalue {
	char *key;
	char *value_string;
	uint64_t value;
} pqc_keyvalue;

typedef struct pqc_stat {
	char *max_string;
	char *max_value;
	uint32_t max;
	char *min_string;
	char *min_value;
	uint32_t min;
	uint64_t null_count;
	uint64_t distinct_count;
} pqc_stat;

typedef struct pqc_pageencodings {
	uint32_t page_type;
	uint32_t page_encoding;
	uint32_t page_count;
} pqc_pageencodings;

typedef struct pqc_page {
	uint32_t num_values;
	uint32_t num_nulls;
	uint32_t num_rows;
	pqc_pageencodings pageencodings[3];
	uint32_t definition_levels_byte_length; /* v2 only, v1 had it in the rle block */
	uint32_t repetition_levels_byte_length; /* v2 only, v1 had it in the rle block */
	bool is_compressed;
	pqc_stat stat;
	uint32_t num_read;
} pqc_page;

typedef struct pqc_columnchunk {
	char *file_path;
	uint64_t file_offset;
	/* meta data*/
	uint64_t offset_index_offset;
	uint32_t offset_index_length;
	uint64_t column_index_offset;
	uint32_t column_index_length;

	uint32_t type;
	char *path_in_schema;
	uint32_t num_encodings;
	Encoding *encodings;
	CompressionCodec codec;
	uint64_t nrows;
	uint64_t num_values;
	pqc_page cur_page;
	uint64_t total_uncompressed_size;
	uint64_t total_compressed_size;
	uint64_t data_page_offset;
	uint64_t index_page_offset;
	uint64_t dictionary_page_offset;

	pqc_stat stat;
	pqc_pageencodings pageencodings[3]; // page encodings stats

	int nkeyvalues;
	pqc_keyvalue *keyvalues; /* optional key values */
} pqc_columnchunk;

typedef struct pqc_sortingcolumn {
	uint32_t column_idx;
	bool descending;
	bool nulls_first;
} pqc_sortingcolumn;

typedef struct pqc_row_group {
	/* columns */
	int ncolumnchunks;
	pqc_columnchunk *columnchunks;
	int nsortingcolumns;
	pqc_sortingcolumn *sortingcolumns;
	uint64_t total_byte_size;
	uint64_t num_rows;
	uint64_t file_offset;
	uint64_t total_compressed_size;
	uint32_t ordinal;
} pqc_row_group;

typedef struct pqc_filemetadata {
	char *created_by;
	uint32_t version;

	/* internal data */
	int64_t nrows;
	int nrowgroups;
	pqc_row_group *rowgroups;

	/* list column order */
	int nelements;
	pqc_schema_element *elements;

	int nkeyvalues;
	pqc_keyvalue *keyvalues; /* optional key values */
	char *encryption_algorithm;
	char *footer_signing_key_metadata;
} pqc_filemetadata;

typedef struct pqc_file pqc_file;

pqc_export int pqc_open( pqc_file **pq, char *fn);
pqc_export pqc_file *pqc_dup( pqc_file *pq);
pqc_export pqc_file *pqc_copy( pqc_file *pq);
pqc_export void pqc_close( pqc_file *pq);
pqc_export int pqc_read_schema( pqc_file *pq);
pqc_export const pqc_schema_element *pqc_get_schema_elements( pqc_file *pq, int *nr); /* returns (after call to pqc_read_filemetadata) arraywith schema_elements and number of elements */

pqc_export int pqc_read_filemetadata( pqc_file *pq);
pqc_export pqc_filemetadata *pqc_get_filemetadata( pqc_file *pq );

pqc_export int64_t pqc_read( pqc_file *pq, int64_t offset, char *buffer, int nrbytes);

#endif /*_PQC_FILEMETADATA_H_*/
