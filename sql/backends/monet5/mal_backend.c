
#include "monetdb_config.h"
#include "sql.h"
#include "mal_backend.h"

backend *
backend_reset(backend *b)
{
	b->out = b->client->fdout;
	b->language = 0;

	b->vtop = 0;
	b->q = NULL;
	b->mvc_var = 0;
	b->output_format = OFMT_CSV;
	return b;
}

backend *
backend_create(mvc *m, Client c)
{
	backend *b = NEW(backend);

	b->console = isAdministrator(c);
	b->mvc = m;
	b->client = c;
	b->mvc_var = 0;
	b->output_format = OFMT_CSV;
	return backend_reset(b);
}

void
backend_destroy(backend *b)
{
	_DELETE(b);
}

