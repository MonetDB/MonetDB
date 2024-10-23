

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

	/*
	 * Virtual types.
	 *
	 * These types are used only for in-memory JSON processing and serialized
	 * into JSON strings when outputted to json/jsonb.
	 */
	jbvDatetime = 0x20,
};

// postgres.h
typedef uintptr_t Datum; // TODO: remove this type

// postgres_ext.h
typedef unsigned int Oid; // TODO: remove this type

typedef struct {
	union {
		lng lnum;
		dbl dnum;
	};
	int type;
} Numeric;

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
