

#ifndef POSTGRES_DEFINES_INTERNAL
#define POSTGRES_DEFINES_INTERNAL




#include "monetdb_config.h"
#include "sql_list.h"

typedef list List;
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

#define palloc(X)	GDKmalloc(X)
#define pfree(X)	GDKfree(X)
#define repalloc(M,NSIZE) GDKrealloc(M, NSIZE)

#define Max(A,B) MAX(A, B)

#define SA	(escontext->sa)
#define list_make1(X) sa_list_append(SA, NULL, X)
#define list_make2(X, Y) list_append(sa_list_append(SA, NULL, X), Y)
#define lappend(X, Y) list_append(X, Y)

#define linitial(L) (L->h ? L->h->data : NULL)
#define lsecond(L) list_fetch(L, 1)

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

#define CHECK_FOR_INTERRUPTS() /* EMPTY */

#define DatumGetNumeric(X) (NULL) // TODO
#define DirectFunctionCall3 	/* TODO */
#define CStringGetDatum 	/* TODO */
#define ObjectIdGetDatum // TODO
#define Int32GetDatum // TODO
#define DirectFunctionCall1 // TODO
#define NumericGetDatum // TODO

#define for_each_from(cell, list, N) \
cell = list->h; \
{int i = 0; while (i < N && cell->next) {cell = cell->next;}} \
for (;cell;cell = cell->next)

#define foreach(cell, list) for_each_from(cell, list, 0)

#define lfirst(cell) (cell->data)

#define Assert(x) assert(x)
#define NIL NULL
#define PG_UINT32_MAX ((uint32) UINT32_MAX)

#define ereport(c, m)  (void) fmt; (void) msg;
#define ERROR "TODO"
#define errmsg_internal(frmt, msg) "TODO"

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

#endif
