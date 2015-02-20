typedef enum {
	ASSIGNMENT, FUNCTION, PARAM, QUOTED, ESCAPEDP
} mal_statement_state;

typedef struct {
	char* assignment;
	char* function;
	unsigned short nparams;
	char** params;
} mal_statement;

void mal_statement_split(char* stmt, mal_statement *out, size_t maxparams);
void profiler_clearbar(void);
void profiler_renderbar_dl(int* state, int* total);
void profiler_arm(void);
int profiler_start(void);
void profiler_renderbar(size_t state, size_t total, char *symbol);
