

#ifndef POSTGRES_DEFINES
#define POSTGRES_DEFINES


#include "yyjson.h"
#include "monetdb_config.h"
#include "gdk.h"
// c.h
#define FLEXIBLE_ARRAY_MEMBER	/* empty */


typedef uint32_t uint32;
typedef int int32;
typedef sht int16;
typedef uint16_t uint16;
typedef lng int64;


// jsonb.h
enum jbvType
{
	/* Scalar types */
	jbvNull = 0x0,
	jbvString,
	jbvNumeric,
	jbvBool,
	/* Composite types */
	jbvArray = 0x10,
	jbvObject,
	/* Binary (i.e. struct Jsonb) jbvArray/jbvObject */
	jbvBinary,

	/*
	 * Virtual types.
	 *
	 * These types are used only for in-memory JSON processing and serialized
	 * into JSON strings when outputted to json/jsonb.
	 */
	jbvDatetime = 0x20,
};

// postgres.h
typedef uintptr_t Datum;

// postgres_ext.h
typedef unsigned int Oid;

#if 0
// numeric.c
typedef int16 NumericDigit;
struct NumericShort
{
	uint16		n_header;		/* Sign + display scale + weight */
	NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

struct NumericLong
{
	uint16		n_sign_dscale;	/* Sign + display scale */
	int16		n_weight;		/* Weight of 1st digit	*/
	NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

union NumericChoice
{
	uint16		n_header;		/* Header word */
	struct NumericLong n_long;	/* Long form (4-byte header) */
	struct NumericShort n_short;	/* Short form (2-byte header) */
};

struct NumericData
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	union NumericChoice choice; /* choice of format */
};
struct NumericData;
typedef struct NumericData *Numeric;
#endif

typedef lng Numeric;

typedef yyjson_val JsonbValue;


// primnodes.h
typedef enum JsonWrapper
{
	JSW_UNSPEC,
	JSW_NONE,
	JSW_CONDITIONAL,
	JSW_UNCONDITIONAL,
} JsonWrapper;

#define DatumGetJsonbP(jb) ((JsonbValue*) jb)

typedef yyjson_val Jsonb;

typedef struct list List;

#endif // POSTGRES_DEFINES
