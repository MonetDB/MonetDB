/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 * 
 * The Original Code is the Monet Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.  
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

/**********************************************************************
 * SQLExecute()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

static int next_result(stream *rs,  ODBCStmt *	hstmt, int *type ){
	int flag, status;
	if (!stream_readInt(rs, &flag) || flag == COMM_DONE) {
		/* 08S01 = Communication link failure */
		addStmtError(hstmt, "08S01", NULL, 0);
		return SQL_ERROR;
	}

	stream_readInt(rs, type);	/* read result type */
	stream_readInt(rs, &status);	/* read result size (is < 0 on error) */
	if (status < 0) {
		/* output error */
		char buf[BLOCK+1];
		int last = 0;
		int nr = bs_read_next(rs, buf, &last);
		/* read result string (not used) */
		while (!last) {
			nr = bs_read_next(rs, buf, &last);
		}
		/* HY000 = General Error */
		addStmtError(hstmt, "HY000", "No result available (status < 0)", 0);
		return SQL_ERROR;
	}
	return status;
}

SQLRETURN Execute(SQLHSTMT hStmt)
{
	ODBCStmt *	hstmt = (ODBCStmt *) hStmt;
	ODBCDbc *	dbc = NULL;
	context *	sqlContext = NULL;
	stmt *		res = NULL;
	RETCODE		rc = SQL_SUCCESS;


	if (! isValidStmt(hstmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors(hstmt);

	/* check statement cursor state, query should be prepared */
	if (hstmt->State != PREPARED) {
		/* 24000 = Invalid cursor state */
		addStmtError(hstmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	/* internal state correctness checks */
	assert(hstmt->Query != NULL);
	assert(hstmt->ResultCols == NULL);
	assert(hstmt->ResultRows == NULL);

	dbc = hstmt->Dbc;
	assert(dbc);
	assert(dbc->Mrs);
	assert(dbc->Mws);

	/* Send the Query to the server for execution */
	dbc->Mws->write( dbc->Mws, hstmt->Query, strlen(hstmt->Query), 1 );
	dbc->Mws->write( dbc->Mws, ";\n", 2, 1 );
	dbc->Mws->flush( dbc->Mws );

	/* now get the result data and store it to our internal data structure */
	{ /* start of "get result data" code block */
	int	nCol = 0;
	int	nRow = 0;
	int	nCols = 0;
	int	nRows = 0;
	int	type = 0;
	int	status = 0;
	int	flag = 0;
	stream *	rs;
	ColumnHeader *	pColumnHeader;

	/* initialize the Result meta data values */
	hstmt->nrCols = 0;
	hstmt->nrRows = 0;
	hstmt->currentRow = 0;

	rs = dbc->Mrs;
	status = next_result(rs, hstmt, &type);
	if (status == SQL_ERROR)
		return status;

	if (type == QHEADER && status > 0) { /* header info */
		char *sc, *ec;
		bstream *bs = bstream_create(rs, BLOCK);
		int eof = 0;
		int cur = 1;
		ColumnHeader *pCol = NULL;

		fprintf(stderr, "QHEADER %d\n", status);

		nCols = status;

		hstmt->nrCols = nCols;
		hstmt->ResultCols = NEW_ARRAY(ColumnHeader,(nCols+1));
		memset( hstmt->ResultCols, 0, (nCols+1)*sizeof(ColumnHeader));

		eof = (bstream_read(bs, bs->size - (bs->len - bs->pos)) == 0);
		sc = bs->buf + bs->pos;
		ec = bs->buf + bs->len;
		while(sc < ec){
			char *s, *name = NULL, *type = NULL;

			s = sc;
			while(sc<ec && *sc != ',') sc++;
			if (sc>=ec && !eof){
				bs->pos = s - bs->buf;
				eof = (bstream_read(bs, bs->size - (bs->len - bs->pos)) == 0);
				sc = bs->buf + bs->pos; 
				ec = bs->buf + bs->len; 
				continue;
			} else if (eof){
				/* TODO: set some error message */
				break;
			}

			*sc = 0;
			name = strdup(s);
			sc++;
			s = sc;
			while(sc<ec && *sc != '\n') sc++;
			if (sc>=ec && !eof){
				bs->pos = s - bs->buf;
				eof = (bstream_read(bs, bs->size - (bs->len - bs->pos)) == 0);
				sc = bs->buf + bs->pos; 
				ec = bs->buf + bs->len; 
				while(sc<ec && *sc != '\n') sc++;
				if (sc>=ec){
					/* TODO: set some error message */
					break;
				}
			} else if (eof){
				/* TODO: set some error message */
				break;
			} 
			*sc = 0;
			type = strdup(s);
			sc++;

			pCol = hstmt->ResultCols + cur;

			pCol->pszSQL_DESC_BASE_COLUMN_NAME = name;
			pCol->pszSQL_DESC_BASE_TABLE_NAME = strdup("tablename");
			pCol->pszSQL_DESC_TYPE_NAME = type;
			pCol->pszSQL_DESC_LOCAL_TYPE_NAME = strdup("Mtype");
			pCol->pszSQL_DESC_LABEL = strdup(name);
			pCol->pszSQL_DESC_CATALOG_NAME = strdup("catalog");
			pCol->pszSQL_DESC_LITERAL_PREFIX = strdup("pre");
			pCol->pszSQL_DESC_LITERAL_SUFFIX = strdup("suf");
			pCol->pszSQL_DESC_NAME = strdup(name);
			pCol->pszSQL_DESC_SCHEMA_NAME = strdup("schema");
			pCol->pszSQL_DESC_TABLE_NAME = strdup("table");
			pCol->nSQL_DESC_DISPLAY_SIZE = strlen(name) + 2;
			cur++;
		}
		bstream_destroy(bs);

		status = next_result(rs, hstmt, &type);
		if (status == SQL_ERROR)
			return status;
	}
	if (type == QTABLE && status > 0) {
		char *sc, *ec;
		bstream *bs = bstream_create(rs, BLOCK);
		int eof = 0;

		fprintf(stderr, "QTABLE %d\n", status);

		nRows = status;

		hstmt->nrRows = nRows;
		hstmt->ResultRows = NEW_ARRAY(char*,(nCols+1)*(nRows+1));
		memset(hstmt->ResultRows, 0, (nCols+1)*(nRows+1));
		assert(hstmt->ResultRows != NULL);

		/* Next copy data from all columns for all rows */
		eof = (bstream_read(bs, bs->size - (bs->len - bs->pos)) == 0);
		sc = bs->buf + bs->pos;
		ec = bs->buf + bs->len;
		for (nRow = 1; nRow <= nRows && !eof; nRow++) {
			for (nCol = 1; nCol <= nCols && !eof; nCol++) {
				char *s = sc;
				while (sc < ec && *sc != '\t' && *sc != '\n') 
					sc++;
				if (sc >= ec && !eof) {
					bs->pos = s - bs->buf;
					eof = (bstream_read(bs, bs->size - (bs->len - bs->pos)) == 0);
					sc = bs->buf + bs->pos; 
					ec = bs->buf + bs->len; 
					while (sc < ec && *sc != '\t' && *sc != '\n') 
						sc++;
					if (sc >= ec){
						bstream_destroy(bs);
						return SQL_ERROR;
					}
				}
				*sc = '\0';
				if (*s == '\"' && *(sc-1) == '\"'){
					s++;
					*(sc-1) = '\0';
				}
				if (*s == '\'' && *(sc-1) == '\''){
					s++;
					*(sc-1) = '\0';
				}
				hstmt->ResultRows[nRow*nCols+nCol] = strdup(s);
				sc++;
			}
		}

		bstream_destroy(bs);
	} else if (QUPDATE) {  
		hstmt->nrRows = nRows;
		hstmt->ResultRows = NULL;
	}

	} /* end of "get result data" code block */

	hstmt->State = EXECUTED;
	return SQL_SUCCESS;
}

SQLRETURN SQLExecute(SQLHSTMT hStmt)
{
	return Execute( hStmt );
}
