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

int sql_create_context( context **res, int *debug, int *opt, stream **ws ){
	context *lc = NEW(context);

	sql_init_context( lc, *ws, *debug, default_catalog_create() );
	lc->optimize = *opt;
	catalog_create( lc );
	lc->cat->cc_getschema( lc->cat, "default-schema", "default-user");
	*res = lc;
	return GDK_SUCCEED;
}

int sql_destroy_context( context **lc ){
	sql_exit_context( *lc );
	return GDK_SUCCEED;
}

int sqlline( context **Lc, char *line ){
	context *lc = *Lc;
	statement *res = NULL;

	res =  sqlexecute( lc, line );
	if (res){
	    int nr = 1;
	    statement_dump( res, &nr, lc );

	    lc->out->flush( lc->out );
	}
	return GDK_SUCCEED;
}
