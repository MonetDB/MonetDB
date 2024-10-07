

#ifndef POSTGRES_DEFINES
#define POSTGRES_DEFINES



// c.h
#define FLEXIBLE_ARRAY_MEMBER	/* empty */

#include "monetdb_config.h"
#include "sql_list.h"

typedef uint32_t uint32;
typedef list List;
typedef int int32;


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

// stringinfo.h
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;

typedef StringInfoData *StringInfo;

// postgres_ext.h
typedef unsigned int Oid;


struct Node {};

/* Often-useful macro for checking if a soft error was reported */
#define SOFT_ERROR_OCCURRED(escontext) (false)


#define TODO_ERROR 0
#define ereturn(context, dummy_value, ...)	return TODO_ERROR;

#define errcode(X)	/* TODO */
#define errmsg(X)	/* TODO */

#endif
