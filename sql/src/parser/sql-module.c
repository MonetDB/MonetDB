#include <gdk.h>

#include "symbol.h"
#include <catalog.h>
#include "sqlparser.tab.h"
#include "sqlexecute.h"
#include "context.h"
#include <mem.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <stream.h>
#include <statement.h>

extern catalog *catalog_create( context *lc);

int sql( str file, str output, int debug ){
	int res;
	stream *in;
	stream *out;
	context lc;

	in = open_rastream( file );
	out = open_wastream( output );
	if (in->errnr || out->errnr){
		return GDK_FAIL;
	}

	sql_init_context( &lc, out, debug, default_catalog_create() );
	catalog_create( &lc );
	lc.cat->cc_getschema( lc.cat, "default-schema", "default-user");
	res = sql_execute( &lc, in ); 

	stream_close_stream(in);
	sql_exit_context( &lc );
	if (res != 0)
		return GDK_FAIL;
	else
		return GDK_SUCCEED;
}
