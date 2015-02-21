#include <R.h>
#include <Rdefines.h>

int profiler_start();

SEXP profiler_start_listen() {
	SEXP port;
	port = NEW_INTEGER(1);
 	INTEGER_POINTER(port)[0] = profiler_start();
	return port;
}
