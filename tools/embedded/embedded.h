int monetdb_startup(char* dir);
int monetdb_shutdown(void);
void* monetdb_query(char* query);
void monetdb_cleanup_result(void* output);
void* monetdb_query_R(char* query);
