#include <R.h>
#include <Rdefines.h>

#include "profiler.h"

SEXP profiler_start_listen() {
	SEXP port;
	int pret;
	port = NEW_INTEGER(1);
	pret = profiler_start();
	if (pret < 0) {
		error("Failed to listen for UDP messages");
	}
 	INTEGER_POINTER(port)[0] = pret;
	return port;
}
