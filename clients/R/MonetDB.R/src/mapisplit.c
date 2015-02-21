#include <assert.h>
#include <string.h>
#include <errno.h>
#include "mapisplit.h"

typedef enum {
	INQUOTES, ESCAPED, INTOKEN, INCRAP
} mapi_line_chrstate;

void mapi_unescape(char* in, char* out) {
	char escaped = 0;
	size_t i, o = 0;

	for (i=0; i < strlen(in); i++) {
		if (!escaped && in[i] == '\\') {
			escaped = 1;
			continue;
		}
		out[o++] = in[i];
		escaped = 0;
	}
	out[o] = '\0';
}

void mapi_line_split(char* line, char** out, size_t ncols) {
	size_t cCol = 0;
	int tokenStart = 2;
	int endQuote = 0;
	int curPos;

	int linelen = strlen(line);
	mapi_line_chrstate state = INCRAP;

	for (curPos = 2; curPos < linelen - 1; curPos++) {
		char chr = line[curPos];

		switch (state) {
		case INCRAP:
			if (chr != '\t' && chr != ',' && chr != ' ') {
				tokenStart = curPos;
				if (chr == '"') {
					state = INQUOTES;
					tokenStart++;
				} else {
					state = INTOKEN;
				}
			}
			break;

		case INTOKEN:
			if (chr == ',' || curPos == linelen - 2) {
				int tokenLen = curPos - tokenStart - endQuote;
				line[tokenStart + tokenLen] = '\0';
				out[cCol] = &line[tokenStart];
				assert(cCol < ncols);
				cCol++;
				endQuote = 0;
				state = INCRAP;
			}
			break;

		case ESCAPED:
			state = INQUOTES;
			break;

		case INQUOTES:
			if (chr == '"') {
				state = INTOKEN;
				endQuote++;
				break;
			}
			if (chr == '\\') {
				state = ESCAPED;
				break;
			}
			break;
		}
	}
}
