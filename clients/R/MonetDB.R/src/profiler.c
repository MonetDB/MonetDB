#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>

#include <stdio.h>
#ifndef _MSC_VER
#include <pthread.h>
#endif
#include <signal.h>
#include <unistd.h>
#include <math.h>
#include <stdlib.h>

#include <sys/types.h>
#ifdef _MSC_VER
#include <sys/timeb.h>
#else
#include <sys/fcntl.h>
#include <sys/time.h>
#endif

#ifdef _MSC_VER
#include <winsock2.h>
#include <ws2tcpip.h>
typedef int ssize_t;
#else
#include <sys/socket.h>
#include <netinet/in.h>
#define HAVE_NL_LANGINFO	/* not on Windows, probably everywhere else */
typedef int SOCKET;
#define INVALID_SOCKET (-1)
#define SOCKET_ERROR (-1)
#endif

#ifdef HAVE_NL_LANGINFO
#include <langinfo.h>
#endif

#include "mapisplit.h"
#include "profiler.h"

// trace output format and columns
#define TRACE_NCOLS 14
#define TRACE_COL_QUERYID 2
#define TRACE_COL_STATEFL 4
#define TRACE_COL_MALSTMT 13
#define TRACE_MAL_MAXPARAMS 3 // we don't need more than 3 params here

// size of the progress bar in characters
#define PROFILER_BARSYMB 60

static SOCKET profiler_socket;
#ifdef _MSC_VER
static HANDLE profiler_pthread;
#else
static pthread_t profiler_pthread;
#endif
static int profiler_needcleanup = 0;
static int profiler_armed = 0;

static char* profiler_symb_query = "X";
static char* profiler_symb_trans = "V";
static char* profiler_symb_bfree = "_";
static char* profiler_symb_bfull = "#";

/* standalone MAL function call parser */
void mal_statement_split(char* stmt, mal_statement *out, size_t maxparams) {
	#define TRIM(str) \
	while (str[0] == ' ' || str[0] == '"') str++; endPos = curPos - 1; \
	while (stmt[endPos] == ' ' || stmt[endPos] == '"') { stmt[endPos] = '\0'; endPos--; }

	size_t curPos, endPos, paramStart = 0, stmtLen;
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
				state = ESCAPEDP;
				break;
			}
			break;

		case ESCAPEDP:
			state = QUOTED;
			break;
		}
	}
}

static unsigned long profiler_tsms(void) {
#ifdef _MSC_VER
	struct _timeb tb;
	_ftime_s(&tb);
	return (unsigned long) tb.time * 1000 + (unsigned long) tb.millitm;
#else
	unsigned long ret = 0;
	struct timeval tv;
	gettimeofday(&tv, NULL);
	ret += tv.tv_sec * 1000;
	ret += tv.tv_usec / 1000;
	return ret;
#endif
}

// clear line and overwrite with spaces
void profiler_clearbar(void) {
	int bs;
	if (!profiler_needcleanup) return;
	for (bs=0; bs < PROFILER_BARSYMB + 3 + 6; bs++) printf("\b \b"); 
	profiler_needcleanup = 0;
}

void profiler_renderbar(size_t state, size_t total, char *symbol) {
	int bs;
	unsigned short percentage, symbols;

	profiler_clearbar();
	profiler_needcleanup = 1;

	percentage = (unsigned short) ceil((1.0 * 
		state / total) * 100);
	symbols = (unsigned short) (PROFILER_BARSYMB*(percentage/100.0));
	
	printf("%s ", symbol);
	for (bs=0; bs < symbols; bs++) printf("%s", profiler_symb_bfull);
	for (bs=0; bs < PROFILER_BARSYMB-symbols; bs++) printf("%s", profiler_symb_bfree); 
	printf(" %3u%% ", percentage);
	fflush(NULL);
}

#ifdef _MSC_VER
static DWORD WINAPI profiler_thread(LPVOID params)
#else
static void* profiler_thread(void* params)
#endif
{
	char buf[BUFSIZ];
	char* elems[TRACE_NCOLS];
	// query ids are unlikely to be longer than BUFSIZ
	char* thisqueryid = malloc(BUFSIZ); 
	char* queryid     = malloc(BUFSIZ);

	ssize_t recvd = 0;
	size_t profiler_msgs_expect = 0;
	size_t profiler_msgs_done = 0;

	unsigned long profiler_querystart = 0;
	char* stmtbuf = malloc(65507); // maximum size of an IPv4 UDP packet

	mal_statement *stmt = malloc(sizeof(mal_statement));
	stmt->params = malloc(TRACE_MAL_MAXPARAMS * sizeof(char*));

	(void) params;
	for(;;) {
		recvd = recv(profiler_socket, buf, sizeof(buf), 0);
		if (recvd == SOCKET_ERROR)
			return 0;
		if (recvd > 0) {
			size_t i = 0, j = 0;
			char ib = 0;
			buf[recvd] = 0;
			if (buf[0]== '#') {
				continue;
			}
			mapi_line_split(buf, elems, TRACE_NCOLS);
			if (strncmp(elems[TRACE_COL_STATEFL], "done", 4) != 0) {
				continue;
			}
			// cleanup overloaded query identifier
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
				profiler_msgs_expect = atol(stmt->params[2]) - 2; 
#ifdef _MSC_VER
				strcpy_s(queryid, BUFSIZ, thisqueryid);
#else
				strcpy(queryid, thisqueryid);
#endif
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

			if (profiler_msgs_expect > 0 && (profiler_tsms() - profiler_querystart) > 500) {
				profiler_renderbar(profiler_msgs_done, profiler_msgs_expect, profiler_symb_query);
			}
			if (profiler_msgs_done >= profiler_msgs_expect) {
				profiler_clearbar();
				profiler_msgs_expect = 0;
			}
		}
	}
}

void profiler_renderbar_dl(int* state, int* total) {
	profiler_renderbar(*state, *total, profiler_symb_trans);
}

void profiler_arm(void) {
	profiler_armed = 1;
}

int profiler_start(void) {
	struct sockaddr_in serv_addr;
	socklen_t len = sizeof(serv_addr);

	profiler_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if(profiler_socket == INVALID_SOCKET) {
		return -1;
	}

	memset((char *) &serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	serv_addr.sin_port = 0; // automatically find free port

	if (bind(profiler_socket, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) == SOCKET_ERROR || 
	    getsockname(profiler_socket, (struct sockaddr *)&serv_addr, &len) == SOCKET_ERROR) {
		return -1;
	}

#ifdef HAVE_NL_LANGINFO
	if (strcasecmp(nl_langinfo(CODESET), "utf-8") == 0) {
		profiler_symb_query = "\342\237\262";	/* U+27F2 */
		profiler_symb_trans = "\342\206\223";	/* U+2193 */
		profiler_symb_bfree = "\342\226\221";	/* U+2591 */
		profiler_symb_bfull = "\342\226\210";	/* U+2588 */
	}
#endif

	// start backgroud listening thread
#ifdef _MSC_VER
	profiler_pthread = CreateThread(NULL, 1024*1024, profiler_thread, NULL, 0, NULL);
#else
	pthread_create(&profiler_pthread, NULL, &profiler_thread, NULL);
#endif
	return ntohs(serv_addr.sin_port);
}
