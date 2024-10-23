#ifndef POSTGRES_DEFINES_INTERNAL
#define POSTGRES_DEFINES_INTERNAL

#include "monetdb_config.h"
#include "sql_list.h"

typedef node ListCell;



typedef struct Node
{
	allocator *sa;
	char* _errmsg;
} Node;

#define SOFT_ERROR_OCCURRED(escontext) (false)

#define palloc(X)	GDKzalloc(X)
#define pfree(X)	GDKfree(X)
#define repalloc(M,NSIZE) GDKrealloc(M, NSIZE)

#define Max(A,B) MAX(A, B)
#define Min(A,B) MIN(A, B)

#define list_make1(X) list_add(sa_list(escontext->sa), X)
#define list_make2(X, Y) list_append(list_add(sa_list(escontext->sa), X), Y)
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

#define check_stack_depth(result) if (THRhighwater()) {snprintf(cxt->_errmsg, 1024, "stack overflow"); return (res = result);}
#define CHECK_FOR_INTERRUPTS()	/* TODO */


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


#define errdetail(X) (_errdetail = X)
#define ereturn(context, dummy_value, X)	do {\
		char* _errmsg = context->_errmsg; \
		(void) context; char* _errdetail = NULL; (void) _errdetail; char* _errcode; \
		(void) _errcode; char* _errhint; (void) _errhint; \
		(X);\
		if (_errdetail) {\
			char __errmsg[1024] = {0};\
			snprintf(__errmsg, sizeof(__errmsg), "%s detail: %s", _errmsg, _errdetail); \
			strcpy(_errmsg, __errmsg);\
		}\
		return dummy_value;} while(0)

#define errcode(X)	(_errcode = #X)
#define errhint(X)	(_errhint = X)
#define errmsg(...)	snprintf(_errmsg, 1024, __VA_ARGS__)
#define ereport(TYPE, X) do {char* _errmsg = cxt->_errmsg; char* _errcode; (void) _errcode; char* _errhint; (void) _errhint; (X);} while(0)
#define elog(TYPE, ...) do {char* _errmsg = cxt->_errmsg; errmsg(__VA_ARGS__); } while(0);

#define errsave(context, X) \
		do {\
			(void) result;\
		 	char* _errcode; (void) _errcode;\
			char* _errmsg = context->_errmsg;\
			(X);\
		} while(0)

// c.h
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#define gettext(x) (x)
#define _(x) gettext(x)

#define pg_strncasecmp(s1, s2, l2) GDKstrncasecmp(s1, s2, strlen(s1), l2)

// pg_wchar.h
#define MAX_UNICODE_EQUIVALENT_STRING	16

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



#define IsAJsonbScalar(jsonbval)	(yyjson_is_null(jsonbval) || yyjson_is_num(jsonbval) || yyjson_is_str(jsonbval) /* there is no datetime in yyjson */)




/*
 * These widely-used datatypes are just a varlena header and the data bytes.
 * There is no terminating null or anything like that --- the data length is
 * always VARSIZE_ANY_EXHDR(ptr).
 */
typedef struct varlena text;


static inline
dbl Numeric_get_as_dbl(Numeric* num) {
	if (num->type == YYJSON_SUBTYPE_REAL)
		return num->dnum;
	else {
		return (dbl) num->lnum;
	}
}

static inline
lng Numeric_get_as_lng(Numeric* num) {
	if (num->type == YYJSON_SUBTYPE_REAL) {
		return (lng) num->dnum;
	}
	else {
		return num->lnum;
	}
}

#define body_binary(op) \
	(void) have_error;\
	Numeric res = {0};\
	if (num1.type == YYJSON_SUBTYPE_REAL || num2.type == YYJSON_SUBTYPE_REAL) {\
		res.type = YYJSON_SUBTYPE_REAL;\
		res.dnum = Numeric_get_as_dbl(&num1) op Numeric_get_as_dbl(&num2);\
		return res;\
	}\
	res.type = YYJSON_SUBTYPE_SINT; /*cast all other cases as lng for now*/\
	res.lnum = Numeric_get_as_lng(&num1) op Numeric_get_as_lng(&num2);\
	return res;

static inline Numeric
numeric_add_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	body_binary(+)
}

static inline Numeric
numeric_sub_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	body_binary(-)
}

static inline Numeric
numeric_mul_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	body_binary(*)
}

static inline Numeric
numeric_div_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	body_binary(/)
}

static inline Numeric
numeric_mod_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	Numeric res = {0};
	if (num1.type == YYJSON_SUBTYPE_REAL || num2.type == YYJSON_SUBTYPE_REAL) {
		if (have_error)
			*have_error = true;
		return res;
	}
	res.type = YYJSON_SUBTYPE_SINT; /*cast all other cases as lng for now*/
	res.dnum = Numeric_get_as_lng(&num1) % Numeric_get_as_lng(&num2);
	return res;
}

static inline Numeric numeric_uminus(Numeric num)
{
	if (num.type == YYJSON_SUBTYPE_REAL)
		num.dnum = - num.dnum;
	else
		num.lnum = -num.lnum;

	return num;
}

#define forboth(lc1, list1, lc2, list2) for (lc1 = list1->h, lc2 = list2->h; lc1 != NULL && lc2 != NULL; lc1 = lc1->next, lc2 = lc2->next )

typedef str String;

static inline Numeric
numeric_abs(Numeric num)
{
	if (num.type == YYJSON_SUBTYPE_REAL)
		num.dnum = (num.dnum > 0) ? num.dnum : - num.dnum;
	else if (num.type == YYJSON_SUBTYPE_SINT)
		num.lnum = (num.lnum > 0) ? num.lnum : - num.lnum;

	return num;
}

#endif
