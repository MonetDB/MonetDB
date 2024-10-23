

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

// postgres_ext.h
typedef unsigned int Oid;

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

typedef yyjson_val Jsonb;

typedef struct list List;

#endif // POSTGRES_DEFINES
