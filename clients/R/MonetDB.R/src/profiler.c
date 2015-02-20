#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>

#include <stdio.h>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/time.h>

#include <R.h>
#include <Rdefines.h>

#ifdef __WIN32__
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#endif

// trace output format and columns
#define TRACE_NCOLS 14
#define TRACE_COL_QUERYID 2
#define TRACE_COL_STATEFL 4
#define TRACE_COL_MALSTMT 13
#define TRACE_MAL_MAXPARAMS 3 // we don't need more than 3 params here

// size of the progress bar in characters
#define PROFILER_BARSYMB 60

static int profiler_socket;
static pthread_t profiler_pthread;
static int profiler_needcleanup = 0;
static int profiler_armed = 0;

static char* profiler_symb_query = "X";
static char* profiler_symb_trans = "V";
static char* profiler_symb_bfree = "_";
static char* profiler_symb_bfull = "#";

int strupp(char *s) {
    int i;
    for (i = 0; i < strlen(s); i++)
        s[i] = toupper(s[i]);
    return i;
}

/* standalone MAL function call parser */
typedef enum {
	ASSIGNMENT, FUNCTION, PARAM, QUOTED, ESCAPED
} mal_statement_state;

typedef struct {
	char* assignment;
	char* function;
	unsigned short nparams;
	char** params;
} mal_statement;

void mal_statement_split(char* stmt, mal_statement *out, size_t maxparams) {
	#define TRIM(str) \
	while (str[0] == ' ' || str[0] == '"') str++; endPos = curPos - 1; \
	while (stmt[endPos] == ' ' || stmt[endPos] == '"') { stmt[endPos] = '\0'; endPos--; }

	unsigned int curPos, endPos, paramStart = 0, stmtLen;
	mal_statement_state state = ASSIGNMENT;

	out->assignment = stmt;
	out->function = stmt;
	out->nparams = 0;

	stmtLen = strlen(stmt);
	for (curPos = 0; curPos < stmtLen; curPos++) {
		char chr = stmt[curPos];
		switch (state) {
		case ASSIGNMENT:
			if (chr == ':' && stmt[curPos+1] == '=') {
				stmt[curPos] = '\0';
				TRIM(out->assignment)
				state = FUNCTION;
				out->function = &stmt[curPos + 2];
			}
			break;
		
		case FUNCTION:
			if (chr == '(' || chr == ';') {
				stmt[curPos] = '\0';
				TRIM(out->function)
				state = PARAM;
				paramStart = curPos+1;
			}
			break;

		case PARAM:
			if (chr == '"') {
				state = QUOTED;
			}
			if (chr == ',' || chr == ')') {
				stmt[curPos] = '\0';
				out->params[out->nparams] = &stmt[paramStart];
				TRIM(out->params[out->nparams])
				out->nparams++;
				if (out->nparams >= maxparams) {
					return;
				}
				paramStart = curPos+1;
			}
			break;

		case QUOTED:
			if (chr == '"') {
				state = PARAM;
				break;
			}
			if (chr == '\\') {
				state = ESCAPED;
				break;
			}
			break;

		case ESCAPED:
			state = QUOTED;
			break;
		}
	}
}

// from mapisplit.c, the trace tuple format is similar(*) to the mapi tuple format
void mapi_line_split(char* line, char** out, size_t ncols);
void mapi_unescape(char* in, char* out);

unsigned long profiler_tsms() {
	unsigned long ret = 0;
	struct timeval tv;
	gettimeofday(&tv, NULL);
	ret += tv.tv_sec * 1000;
	ret += tv.tv_usec / 1000;
	return ret;
}

// clear line and overwrite with spaces
void profiler_clearbar() {
	if (!profiler_needcleanup) return;
	for (int bs=0; bs < PROFILER_BARSYMB + 3 + 6; bs++) printf("\b \b"); 
	profiler_needcleanup = 0;
}

void profiler_renderbar(size_t state, size_t total, char *symbol) {
	int bs;
	unsigned short percentage, symbols;
	percentage = (unsigned short) round((1.0 * 
		state / total) * 100);
	symbols = PROFILER_BARSYMB*(percentage/100.0);

	profiler_clearbar();
	profiler_needcleanup = 1;
	printf("%s ", symbol);
	for (bs=0; bs < symbols; bs++) printf("%s", profiler_symb_bfull);
	for (bs=0; bs < PROFILER_BARSYMB-symbols; bs++) printf("%s", profiler_symb_bfree); 

	printf(" %3u%% ", percentage);
	fflush(stdout);
}

void *profiler_thread() {
	char buf[BUFSIZ];
	char* elems[TRACE_NCOLS];
	// query ids are unlikely to be longer than BUFSIZ
	char* thisqueryid = malloc(BUFSIZ); 
	char* queryid     = malloc(BUFSIZ);

	ssize_t recvd = 0;
	size_t profiler_msgs_expect = 0;
	size_t profiler_msgs_done = 0;

	unsigned long profiler_querystart;
	char* stmtbuf = malloc(65507); // maximum size of an IPv6 UDP packet

	mal_statement *stmt = malloc(sizeof(mal_statement));
	stmt->params = malloc(TRACE_MAL_MAXPARAMS * sizeof(char*));

	for(;;) {
		recvd = read(profiler_socket, buf, sizeof(buf));
		if (recvd > 0) {
			buf[recvd] = 0;
			mapi_line_split(buf, elems, TRACE_NCOLS);
			if (strncmp(elems[TRACE_COL_STATEFL], "done", 4) != 0) {
				continue;
			}
			// cleanup overloaded query identifier
			size_t i = 0, j = 0;
			char ib = 0;
			for (i = 0; i < strlen(elems[TRACE_COL_QUERYID]); i++) {
				if (elems[TRACE_COL_QUERYID][i] == '[') {ib = 1; thisqueryid[j++] = '*'; }
				if (elems[TRACE_COL_QUERYID][i] == ']') {ib = 0; continue;}
				if (!ib) thisqueryid[j++] = elems[TRACE_COL_QUERYID][i];
			}
			thisqueryid[j] = '\0';

			mapi_unescape(elems[TRACE_COL_MALSTMT], stmtbuf);
			mal_statement_split(stmtbuf, stmt, TRACE_MAL_MAXPARAMS);

			if (profiler_armed && strcmp(stmt->function, "querylog.define") == 0) {
				// the third parameter to querylog.define contains the MAL plan size
				profiler_msgs_expect = atol(stmt->params[2])- 5; 
				strcpy(queryid, thisqueryid);
				profiler_querystart = profiler_tsms();
				profiler_msgs_done = 0;
				profiler_needcleanup = 0;
				profiler_armed = 0;
				continue;
			}

			if (strcmp(queryid, thisqueryid) != 0) { // not my department
				continue;
			}

			profiler_msgs_done++;
	        if (profiler_msgs_expect > 0 && (profiler_tsms() - profiler_querystart) > 200) {
	        	profiler_renderbar(profiler_msgs_done, profiler_msgs_expect, profiler_symb_query);
        	}
        	if (profiler_msgs_done >= profiler_msgs_expect) {
        		profiler_clearbar();
        		profiler_msgs_expect = 0;
        	}
		}
	}
	return NULL;
}

void profiler_renderbar_dl(int* state, int* total) {
	profiler_renderbar(*state, *total, profiler_symb_trans);
}

void profiler_arm() {
	profiler_armed = 1;
}

SEXP profiler_start_listen() {
	SEXP port;

	profiler_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if(profiler_socket < 0) {
	    error("socket error\n");
	    return R_NilValue;
	}

	struct sockaddr_in serv_addr;
	socklen_t len = sizeof(serv_addr);

	memset((char *) &serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	serv_addr.sin_port = 0; // automatically find free port

	if (bind(profiler_socket, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0 || 
		getsockname(profiler_socket, (struct sockaddr *)&serv_addr, &len) < 0) {
       error("could not bind to process (%d) %s\n", errno, strerror(errno));
       return R_NilValue;
	}
	// start backgroud listening thread
	pthread_create(&profiler_pthread, NULL, profiler_thread, NULL);

	port = NEW_INTEGER(1);
 	INTEGER_POINTER(port)[0] = ntohs(serv_addr.sin_port);

 	// some nicer characters for UTF-enabled terminals
 	char* ctype = getenv("LC_CTYPE");
 	strupp(ctype);
 	if (strstr(ctype, "UTF-8") != NULL) {
 		profiler_symb_query = "\u27F2";
		profiler_symb_trans = "\u2193";
		profiler_symb_bfree = "\u2591";
		profiler_symb_bfull = "\u2588";
 	}
	return port;
}
