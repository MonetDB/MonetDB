typedef int monetdb_instance;
int monetdb_startup(char* dir);
int monetdb_shutdown(monetdb_instance instance);
int monetdb_query(monetdb_instance instance, char* query);