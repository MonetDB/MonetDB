#include <gdk.h>
#include "sqlexecute.h"

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
	res = sql_execute( &lc, in ); 

	stream_close_stream(in);
	sql_exit_context( &lc );
	if (res != 0)
		return GDK_FAIL;
	else
		return GDK_SUCCEED;
}
