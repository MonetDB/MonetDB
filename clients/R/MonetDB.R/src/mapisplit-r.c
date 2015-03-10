#include <assert.h>

#include <R.h>
#include <Rdefines.h>

void mapi_line_split(char* line, char** out, size_t ncols);

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
