#ifndef POSTGRES_DEFINES_INTERNAL
#define POSTGRES_DEFINES_INTERNAL

#include "monetdb_config.h"
#include "sql_list.h"

typedef node ListCell;

// stringinfo.h
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;

typedef StringInfoData *StringInfo;



typedef struct Node
{
	allocator *sa;
} Node;

#define SOFT_ERROR_OCCURRED(escontext) (false)


#define TODO_ERROR 0
#define ereturn(context, dummy_value, ...)	{(void) context; return TODO_ERROR;}

#define errcode(X)	/* TODO */
#define errmsg(X)	/* TODO */

#define palloc(X)	GDKzalloc(X)
#define pfree(X)	GDKfree(X)
#define repalloc(M,NSIZE) GDKrealloc(M, NSIZE)

#define Max(A,B) MAX(A, B)
#define Min(A,B) MIN(A, B)

#define list_make1(X) list_add(NULL, X)
#define list_make2(X, Y) list_append(list_add(NULL, X), Y)
#define lappend(X, Y) list_append(X, Y)

#define linitial(L) (L->h ? L->h->data : NULL)
#define lsecond(L) list_fetch(L, 1)
#define list_second_cell(L) ((L)->h->next)
#define lnext(L, c) ((c)->next)

static inline int32
pg_strtoint32(const char *s)
{
	const char *src = s;
	size_t len;
	int *dst;
	bool external = true;
	intFromStr(src, &len, &dst, external);

	int res = *dst;

	return res;
}

#define check_stack_depth()		/* TODO */
#define CHECK_FOR_INTERRUPTS()	/* TODO */

#define DatumGetNumeric(X) (X)
#define numeric_in atoi
#define DirectFunctionCall3(func, A, B, C) 	func(A)
#define CStringGetDatum(X)	(X)
#define ObjectIdGetDatum(X)	(X)
#define Int32GetDatum(X)	(X)
#define DirectFunctionCall1(func, A)	(func A)
#define NumericGetDatum(X) (X)


#define for_each_from(cell, list, N) \
cell = list->h; \
for (int i = 0; i < N; i++) {cell = cell->next;} \
for (;cell;cell = cell->next)

#define foreach(cell, list) for_each_from(cell, list, 0)

#define lfirst(cell) (cell->data)

#define Assert(x) assert(x)
#define NIL NULL
#define PG_UINT32_MAX ((uint32) UINT32_MAX)

#include "mal_exception.h"
#define ereport(TYPE, X) createException(MAL, "pg_jsonpath", "TODO_ERROR")
#define ERROR "TODO"
#define errmsg_internal(frmt, msg) "TODO"
#define elog(TYPE, X, ...) (void ) "TODO";

#define errsave(a,b) (void) result; (void) escontext; (void) message;

// c.h
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))

#define pg_strncasecmp(s1, s2, l2) GDKstrncasecmp(s1, s2, strlen(s1), l2)

// pg_wchar.h
#define MAX_UNICODE_EQUIVALENT_STRING	16
#define MAX_MULTIBYTE_CHAR_LEN	4

// pg_wchar.h
typedef unsigned int pg_wchar;

// pg_wchar.h
static inline bool
is_valid_unicode_codepoint(pg_wchar c)
{
	return (c > 0 && c <= 0x10FFFF);
}

// pg_wchar.h
static inline bool
is_utf16_surrogate_first(pg_wchar c)
{
	return (c >= 0xD800 && c <= 0xDBFF);
}

// pg_wchar.h
static inline bool
is_utf16_surrogate_second(pg_wchar c)
{
	return (c >= 0xDC00 && c <= 0xDFFF);
}

// pg_wchar.h
static inline pg_wchar
surrogate_pair_to_codepoint(pg_wchar first, pg_wchar second)
{
	return ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF);
}

// pg_wchar.h
static inline
int
pg_utf_mblen(const unsigned char *s)
{
	int			len;

	if ((*s & 0x80) == 0)
		len = 1;
	else if ((*s & 0xe0) == 0xc0)
		len = 2;
	else if ((*s & 0xf0) == 0xe0)
		len = 3;
	else if ((*s & 0xf8) == 0xf0)
		len = 4;
#ifdef NOT_USED
	else if ((*s & 0xfc) == 0xf8)
		len = 5;
	else if ((*s & 0xfe) == 0xfc)
		len = 6;
#endif
	else
		len = 1;
	return len;
}

// pg_wchar.h
static inline unsigned char *
unicode_to_utf8(pg_wchar c, unsigned char *utf8string)
{
	if (c <= 0x7F)
	{
		utf8string[0] = c;
	}
	else if (c <= 0x7FF)
	{
		utf8string[0] = 0xC0 | ((c >> 6) & 0x1F);
		utf8string[1] = 0x80 | (c & 0x3F);
	}
	else if (c <= 0xFFFF)
	{
		utf8string[0] = 0xE0 | ((c >> 12) & 0x0F);
		utf8string[1] = 0x80 | ((c >> 6) & 0x3F);
		utf8string[2] = 0x80 | (c & 0x3F);
	}
	else
	{
		utf8string[0] = 0xF0 | ((c >> 18) & 0x07);
		utf8string[1] = 0x80 | ((c >> 12) & 0x3F);
		utf8string[2] = 0x80 | ((c >> 6) & 0x3F);
		utf8string[3] = 0x80 | (c & 0x3F);
	}

	return utf8string;
}

// mbutils.h
static inline
bool
pg_unicode_to_server_noerror(pg_wchar c, unsigned char *s)
{
	/*
	 * Complain if invalid Unicode code point.  The choice of errcode here is
	 * debatable, but really our caller should have checked this anyway.
	 */
	if (!is_valid_unicode_codepoint(c))
		return false;

	/* Otherwise, if it's in ASCII range, conversion is trivial */
	if (c <= 0x7F)
	{
		s[0] = (unsigned char) c;
		s[1] = '\0';
		return true;
	}

	/* If the server encoding is UTF-8, we just need to reformat the code */
	unicode_to_utf8(c, s);
	s[pg_utf_mblen(s)] = '\0';
	return true;
}

#define PG_USED_FOR_ASSERTS_ONLY

// postgres error codes
#define ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM "ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM"
#define ERRCODE_SQL_JSON_SCALAR_REQUIRED "ERRCODE_SQL_JSON_SCALAR_REQUIRED"
#define ERRCODE_NON_NUMERIC_SQL_JSON_ITEM "ERRCODE_NON_NUMERIC_SQL_JSON_ITEM"

// jsonb.h
/* convenience macros for accessing a JsonbContainer struct */
#define JsonContainerSize(jc)		((jc)->header & JB_CMASK)
#define JsonContainerIsScalar(jc)	(((jc)->header & JB_FSCALAR) != 0)
#define JsonContainerIsObject(jc)	(((jc)->header & JB_FOBJECT) != 0)
#define JsonContainerIsArray(jc)	(((jc)->header & JB_FARRAY) != 0)

bool yyjson_is_null(yyjson_val *val);  // null
bool yyjson_is_true(yyjson_val *val);  // true
bool yyjson_is_false(yyjson_val *val); // false
bool yyjson_is_bool(yyjson_val *val);  // true/false
bool yyjson_is_uint(yyjson_val *val);  // uint64_t
bool yyjson_is_sint(yyjson_val *val);  // int64_t
bool yyjson_is_int(yyjson_val *val);   // uint64_t/int64_t
bool yyjson_is_real(yyjson_val *val);  // double
bool yyjson_is_num(yyjson_val *val);   // uint64_t/int64_t/double
bool yyjson_is_str(yyjson_val *val);   // string
bool yyjson_is_arr(yyjson_val *val);   // array
bool yyjson_is_obj(yyjson_val *val);   // object
bool yyjson_is_ctn(yyjson_val *val);   // array/object
bool yyjson_is_raw(yyjson_val *val);   // raw string

#define IsAJsonbScalar(jsonbval)	(yyjson_is_null(jsonbval) || yyjson_is_num(jsonbval) || yyjson_is_str(jsonbval) /* there is no datetime in yyjson */)

// jsonb.h
typedef uint32 JEntry;
typedef struct JsonbContainer
{
	uint32		header;			/* number of elements or key/value pairs, and
								 * flags */
	JEntry		children[FLEXIBLE_ARRAY_MEMBER];

	/* the data for each child node follows. */
} JsonbContainer;

/* flags for the header-field in JsonbContainer */
#define JB_CMASK				0x0FFFFFFF	/* mask for count field */
#define JB_FSCALAR				0x10000000	/* flag bits */
#define JB_FOBJECT				0x20000000
#define JB_FARRAY				0x40000000

// c.h
struct varlena
{
	char		vl_len_[4];		/* Do not touch this field directly! */
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];	/* Data content is here */
};

#define VARHDRSZ		((int32) sizeof(int32))

/*
 * These widely-used datatypes are just a varlena header and the data bytes.
 * There is no terminating null or anything like that --- the data length is
 * always VARSIZE_ANY_EXHDR(ptr).
 */
typedef struct varlena text;

static inline Numeric
numeric_add_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	(void) have_error;
	return num1 + num2;
}

static inline Numeric
numeric_sub_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	(void) have_error;
	return num1 - num2;
}

static inline Numeric
numeric_mul_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	(void) have_error;
	return num1 * num2;
}

static inline Numeric
numeric_div_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	(void) have_error;
	return num1 / num2;
}

static inline Numeric
numeric_mod_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	(void) have_error;
	return num1 % num2;
}

static inline Numeric numeric_uminus(Numeric num)
{
	return - num;
}

#define forboth(lc1, list1, lc2, list2) for (lc1 = list1->h, lc2 = list2->h; lc1 != NULL && lc2 != NULL; lc1 = lc1->next, lc2 = lc2->next )

typedef str String;

static inline Numeric
numeric_abs(Numeric num)
{
	return (num > 0) ? num : - num;
}

static inline Datum
Int64GetDatum(int64 X)
{
	return (Datum) X;
}

#endif
