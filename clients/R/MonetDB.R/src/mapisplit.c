#include <assert.h>
#include <string.h>
#include <errno.h>

#include <R.h>
#include <Rdefines.h>

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
	int cCol = 0;
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

char nullstr[] = "NULL";

SEXP mapi_split(SEXP mapiLinesVector, SEXP numCols) {
	assert(TYPEOF(mapiLinesVector) == CHARSXP);

	int cols = INTEGER_POINTER(AS_INTEGER(numCols))[0];
	int rows = LENGTH(mapiLinesVector);

	assert(rows > 0);
	assert(cols > 0);

	SEXP colVec;
	PROTECT(colVec = NEW_LIST(cols));

	int col;
	for (col = 0; col < cols; col++) {
		SEXP colV = PROTECT(NEW_STRING(rows));
		assert(TYPEOF(colV) == STRSXP);
		SET_ELEMENT(colVec, col, colV);
		UNPROTECT(1);
	}

	int cRow;
	int cCol;
	char* elems[cols];

	for (cRow = 0; cRow < rows; cRow++) {
		const char *rval = CHAR(STRING_ELT(mapiLinesVector, cRow));
		char *val = strdup(rval);
		cCol = 0;
		mapi_line_split(val, elems, cols);

		for (cCol = 0; cCol < cols; cCol++) {
			SEXP colV = VECTOR_ELT(colVec, cCol);
			size_t tokenLen = strlen(elems[cCol]);
			if (tokenLen < 1 || strcmp(elems[cCol], nullstr) == 0) {
				SET_STRING_ELT(colV, cRow, NA_STRING);
			}
			else {
				SET_STRING_ELT(colV, cRow, mkCharLen(elems[cCol], tokenLen));
			}
		}
		free(val);
	}

	UNPROTECT(1);
	return colVec;
}
