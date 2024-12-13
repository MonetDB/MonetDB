
#include "monetdb_config.h"
#include "stream.h"
#include "sql_mvc.h"
#include "sql_parser.tab.h"
#include "rel_sequence.h"
#include "sql_semantic.h"
#include <stdio.h>


extern void bat_storage_init(void *b);
extern void bat_table_init(void *b);
extern void bat_logger_init(void *b);
extern void bat_utils_init(void *b);

void bat_storage_init(void *b) { (void)b; }
void bat_table_init(void *b) { (void)b; }
void bat_logger_init(void *b) { (void)b; }
void bat_utils_init(void *b) { (void)b; }

void
sql_add_param(mvc *sql, const char *name, sql_subtype *st)
{
	(void)sql;
	(void)name;
	(void)st;
}

int sql_bind_param(mvc *sql, const char *name)
{	/* -1 error, 0 nr-1, param */
	(void)sql;
	(void)name;
	return 0;
}

char*
sql_next_seq_name(mvc *m)
{
	(void)m;
	return NULL;
}

sql_type *
mvc_bind_type(mvc *m, const char *name)
{
	(void)m;
	(void)name;
	return NULL;
}


/* only need the session->status */
static sql_session *
session_new(allocator *sa)
{
	sql_session *s = SA_ZNEW(sa, sql_session);
	if (!s)
		return NULL;
	s->sa = sa;
	assert(sa);
	s->tr = NULL;
	return s;
}

static mvc *
mvc_new( bstream *rs, stream *ws) {
	mvc *m;

	allocator *pa = sa_create(NULL);
 	m = SA_ZNEW(pa, mvc);
	if (!m)
		return NULL;

	TRC_DEBUG(SQL_TRANS, "MVC create\n");

	m->errstr[0] = '\0';
	/* if an error exceeds the buffer we don't want garbage at the end */
	m->errstr[ERRSIZE-1] = '\0';

	m->qc = NULL;
	m->pa = pa;
	m->sa = sa_create(m->pa);
	m->ta = sa_create(m->pa);
#ifdef __has_builtin
#if __has_builtin(__builtin_frame_address)
	m->sp = (uintptr_t) __builtin_frame_address(0);
#define BUILTIN_USED
#endif
#endif
#ifndef BUILTIN_USED
	m->sp = (uintptr_t)(&m);
#endif
#undef BUILTIN_USED

	m->use_views = false;
	m->sym = NULL;

	m->role_id = m->user_id = -1;
	m->timezone = 0;
	m->sql_optimizer = INT_MAX;
	m->clientid = 0;
	m->div_min_scale = 3;

	m->emode = m_normal;
	m->emod = mod_none;
	m->reply_size = 100;
	m->debug = 0;

	m->label = 0;
	m->nid = 1;
	m->cascade_action = NULL;
	m->runs = NULL;

	m->schema_path_has_sys = true;
	m->schema_path_has_tmp = false;
	m->no_int128 = false;
	m->store = NULL;

	m->session = session_new(pa);
	if (!m->session)
		return NULL;

	m->type = Q_PARSE;
	scanner_init(&m->scanner, rs, ws);
	//m->scanner.started = 1;
	m->scanner.mode = LINE_1;
	return m;
}

int
main(int argc, char *argv[])
{
	if (argc > 2) {
		printf("usage: %s file.sql\n", argv[0]);
		return -1;
	}
	mnstr_init();

	stream *f = open_rstream(argv[1]);
	if (!f) {
		printf("ERROR: Failed to open file '%s'\n", argv[1]);
		return -2;
	}
	bstream *rs = bstream_create(f, 8192);
	mvc *m = mvc_new( rs, stdout_wastream());
	keyword_init();
    if (scanner_init_keywords() != 0) {
		return -3;
	}
	types_init(m->pa);
	/* parse file */
	int err = 0;
	/* read some data */
	if (bstream_next(rs) < 0)
		return -4;
	while ((err = sqlparse(m)) == 0 && m->scanner.rs->pos < m->scanner.rs->len) {
		if (m->sym) {
			printf("parsed\n");
	 	}
		scanner_query_processed(&m->scanner);
		m->sym = NULL;
	}
	return 0;
}
