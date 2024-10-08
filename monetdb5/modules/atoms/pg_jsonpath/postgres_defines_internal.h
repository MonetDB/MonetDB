

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


struct Node {
	allocator *sa;
};

#define SOFT_ERROR_OCCURRED(escontext) (false)


#define TODO_ERROR 0
#define ereturn(context, dummy_value, ...)	return TODO_ERROR;

#define errcode(X)	/* TODO */
#define errmsg(X)	/* TODO */

#define palloc(X) GDKmalloc(X)

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

#endif
