
.MODULE sql;
	
    .USE streams;

    .ATOM context = ptr;
    .END;

    .COMMAND create_context( int debug, int opt, stream o ) : context = 
	sql_create_context; "create a context for the sql interpreter"

    .COMMAND destroy_context( context c ) = 
	sql_destroy_context; "destroy the sql interpreter context"

    .COMMAND sql( context c, str cmd ) = 
	sqlline; "interpret the sql command"

.END sql;
