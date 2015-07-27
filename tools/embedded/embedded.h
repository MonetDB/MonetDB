
#include <Rembedded.h>
#include <Rdefines.h>
#include <Rinternals.h>
#include <R_ext/Parse.h>


int monetdb_startup(char* dir);
int monetdb_shutdown(void);
void* monetdb_query(char* query);
void monetdb_cleanup_result(void* output);
SEXP monetdb_query_R(SEXP query);
SEXP monetdb_startup_R(SEXP dir);
