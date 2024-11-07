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

typedef enum logicaltype {
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




typedef struct pqc_schema_element {
	logicaltype type;	/* generalized types, ie type (logicaltype nr), precision, scale combinations */
	int ccnr;
	int scale;
	int precision;
	int size;		/* type size, int-8/16/32 are stored in a 32 bit, etc */
	bool binary;
	bool isSigned;
	bool isAdjustedToUTC;

	int repetition; /* required (NOT NULL), optional (NULL), repeated */
	char *name;
	int nchildren;
	/* optional logical type */
} pqc_schema_element;

typedef struct pqc_keyvalue {
	char *key;
	char *value_string;
	u_int64_t value;
} pqc_keyvalue;

typedef struct pqc_stat {
	char *max_string;
	char *max_value;
	u_int32_t max;
	char *min_string;
	char *min_value;
	u_int32_t min;
	u_int64_t null_count;
	u_int64_t distinct_count;
} pqc_stat;

typedef struct pqc_pageencodings {
	u_int32_t page_type;
	u_int32_t page_encoding;
	u_int32_t page_count;
} pqc_pageencodings;

typedef struct pqc_page {
	u_int32_t num_values;
	u_int32_t num_nulls;
	u_int32_t num_rows;
	pqc_pageencodings pageencodings[3];
	u_int32_t definition_levels_byte_length; /* v2 only, v1 had it in the rle block */
	u_int32_t repetition_levels_byte_length; /* v2 only, v1 had it in the rle block */
	bool is_compressed;
	pqc_stat stat;
	u_int32_t num_read;
} pqc_page;

typedef struct pqc_columnchunk {
	char *file_path;
	u_int64_t file_offset;
	/* meta data*/
	u_int64_t offset_index_offset;
	u_int32_t offset_index_length;
	u_int64_t column_index_offset;
	u_int32_t column_index_length;

	u_int32_t type;
	char *path_in_schema;
	u_int32_t encodings[3];
	u_int32_t codec;
	u_int64_t nrows;
	//u_int32_t num_values;
	pqc_page cur_page;
	u_int64_t total_uncompressed_size;
	u_int64_t total_compressed_size;
	u_int64_t data_page_offset;
	u_int64_t index_page_offset;
	u_int64_t dictionary_page_offset;

	pqc_stat stat;
	pqc_pageencodings pageencodings[3]; // page encodings stats

	int nkeyvalues;
	pqc_keyvalue *keyvalues; /* optional key values */
} pqc_columnchunk;

typedef struct pqc_sortingcolumn {
	u_int32_t column_idx;
	bool descending;
	bool nulls_first;
} pqc_sortingcolumn;

typedef struct pqc_row_group {
	/* columns */
	int ncolumnchunks;
	pqc_columnchunk *columnchunks;
	int nsortingcolumns;
	pqc_sortingcolumn *sortingcolumns;
	u_int64_t total_byte_size;
	u_int64_t num_rows;
	u_int64_t file_offset;
	u_int64_t total_compressed_size;
	u_int32_t ordinal;
} pqc_row_group;

typedef struct pqc_filemetadata {
	int64_t nrows;
	int nelements;
	pqc_schema_element *elements;

	/* key values */
	char *created_by;

	/* internal data */
	int nrowgroups;
	pqc_row_group *rowgroups;

	/* list column order */

	int nkeyvalues;
	pqc_keyvalue *keyvalues; /* optional key values */
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
