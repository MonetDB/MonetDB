#include <R.h>
#include <Rdefines.h>
#include <assert.h>
#include <string.h>
#include <errno.h>

typedef enum {
	INQUOTES, ESCAPED, INTOKEN, INCRAP
} chrstate;

char nullstr[] = "NULL";

SEXP mapiSplit(SEXP mapiLinesVector, SEXP numCols) {
	PROTECT(mapiLinesVector = AS_CHARACTER(mapiLinesVector));

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
	int tokenStart;
	int curPos;
	int endQuote;

	int bsize = 1024;
	char *valPtr = (char*) malloc(bsize * sizeof(char));
	if (valPtr == NULL) {
		error("malloc() failed. Are you running out of memory? [%s]\n",
				strerror(errno));
		return colVec;
	}

	for (cRow = 0; cRow < rows; cRow++) {
		const char *val = CHAR(STRING_ELT(mapiLinesVector, cRow));
		int linelen = LENGTH(STRING_ELT(mapiLinesVector, cRow));

		cCol = 0;
		tokenStart = 2;
		curPos = 0;
		endQuote = 0;

		chrstate state = INCRAP;

		for (curPos = 2; curPos < linelen - 1; curPos++) {
			char chr = val[curPos];

			switch (state) {
			case INCRAP:
				if (chr != '\t' && chr != ',') {
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
					// we thing we are at the end of a token, so copy the token from the line and add to result

					// copy from line, extend buffer if required
					int tokenLen = curPos - tokenStart - endQuote;
					// check if token fits in buffer, if not, realloc
					while (tokenLen >= bsize) {
						bsize *= 2;
						valPtr = realloc(valPtr, bsize * sizeof(char*));
						if (valPtr == NULL) {
							error(
									"realloc() failed. Are you running out of memory? [%s]\n",
									strerror(errno));
							return colVec;
						}
					}
					strncpy(valPtr, val + tokenStart, tokenLen);
					valPtr[tokenLen] = '\0';

					// get correct column vector from result set and set element
					if (cCol >= cols) {
						warning(
								"Unreadable line from server response (#%d/%d).",
								cRow, rows);
						curPos = linelen;
						continue;
					}
					assert(cCol < cols);
					SEXP colV = VECTOR_ELT(colVec, cCol);
					if (tokenLen < 1 || strcmp(valPtr, nullstr) == 0) {
						SET_STRING_ELT(colV, cRow, NA_STRING);

					} else {
						SET_STRING_ELT(colV, cRow, mkCharLen(valPtr, tokenLen));
					}

					cCol++;
					// reset
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
		assert(cCol == cols - 1);
	}
	free(valPtr);

	UNPROTECT(2);
	return colVec;
}
