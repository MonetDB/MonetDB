#include <stdio.h>
#include "embedded.h"

int monetdb_startup(char* dir) {
	dir=(char*) dir;
	fprintf(stderr, "Hello, World!\n");
	return 42;
}
int monetdb_shutdown(monetdb_instance instance){
	instance=(monetdb_instance) instance;
	return 42;
}
int monetdb_query(monetdb_instance instance, char* query) {
	instance= (monetdb_instance) instance;
	query =(char*) query;
	return 42;
}