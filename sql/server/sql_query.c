
#include "monetdb_config.h"
#include "sql_query.h"

sql_query *
query_create( mvc *sql)
{
	sql_query *q = SA_NEW(sql->sa, sql_query);

	q->sql = sql;
	q->outer = sql_stack_new(sql->sa, 32);
	return q;
}

void 
query_push_outer(sql_query *q, sql_rel *r)
{
	assert(r);
	sql_stack_push(q->outer, r);
}

sql_rel *
query_pop_outer(sql_query *q)
{
	sql_rel *r = sql_stack_pop(q->outer);
	return r;
}

sql_rel *
query_fetch_outer(sql_query *q, int i)
{
	return sql_stack_peek(q->outer, i);
}

int 
query_has_outer(sql_query *q)
{
	return !sql_stack_empty(q->outer);
}
