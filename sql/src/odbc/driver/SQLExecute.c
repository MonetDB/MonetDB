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

static char *convert(char *str ){
	char *res = NULL;
	int i, len;

	for(len = 1, i=0; str[i]; i++, len++){
		if (str[i] == '\'')
			len++;
	}
	res = malloc(len);
	for(len = 0, i=0; str[i]; i++, len++){
		if (str[i] == '\''){
			res[len] = '\\';
			len ++;
		}
		res[len] = str[i];
	}
	res[len] = '\0';
	return res;
}

static int next_result(stream *rs,  ODBCStmt *	hstmt, int *type ){
	int status;
	if (!stream_readInt(rs, type) || *type == Q_END) {
		/* 08S01 = Communication link failure */
		addStmtError(hstmt, "08S01", NULL, 0);
		return SQL_ERROR;
	}

	stream_readInt(rs, &status);	/* read result size (is < 0 on error) */
	if (*type < 0 || status < 0) {
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

SQLRETURN SQLExecute(SQLHSTMT hStmt)
{
	ODBCStmt *	hstmt = (ODBCStmt *) hStmt;
	ODBCDbc *	dbc = NULL;
	char* 		query = NULL;

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

	query = hstmt->Query;
	/* Send the Query to the server for execution */
	if (hstmt->bindParams.size){
		char	*Query = 0;
		int	i = 0, params = 1;
		int	queryLen = strlen(hstmt->Query) + 1;
		char    *oldquery = strdup(hstmt->Query);
		char 	**strings = (char**)malloc(sizeof(char*)*hstmt->bindParams.size );

		memset(strings,0,sizeof(char*)*hstmt->bindParams.size);
		for(i=1; i <= hstmt->bindParams.size; i++){
			if (!hstmt->bindParams.array[i])
				break;

			strings[i] = convert(hstmt->bindParams.array[i]->ParameterValuePtr);
			queryLen += 2 + strlen(strings[i]);
		}
		Query = malloc(queryLen);
		Query[0] = '\0';
		i = 0;
		query = oldquery;
		while(query && *query){
			/* problem with strings with ?s */
			char *old = query;
			if ((query = strchr(query, '?')) != NULL){
				*query = '\0';
				if (!hstmt->bindParams.array[params])
					break;
				i += snprintf(Query+i, queryLen-i, "%s'%s'", old, strings[params]);
				query++;
				old = query;
				params++;
		        }
			if (old && *old != '\0') 
				i += snprintf(Query+i, queryLen-i, "%s", old);
			Query[i] = '\0';
		}
		for(i=0;i<hstmt->bindParams.size; i++){
			if(strings[i])
				free(strings[i]);
		}
		free(strings);
		free(oldquery);
		query = Query;
	}

	dbc->Mws->write( dbc->Mws, query, strlen(query), 1 );
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
	stream *	rs;

	/* initialize the Result meta data values */
	hstmt->nrCols = 0;
	hstmt->nrRows = 0;
	hstmt->currentRow = 0;

	rs = dbc->Mrs;
	status = next_result(rs, hstmt, &type);
	if (status == SQL_ERROR)
		return status;

	if (type == Q_RESULT && status > 0) { /* header info */
		char *sc, *ec;
		bstream *bs = bstream_create(rs, BLOCK);
		int eof = 0;
		int cur = 1;
		int id = 0;
		ColumnHeader *pCol = NULL;

		stream_readInt(rs, &id);
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

		{ char buf[1024]; int i;
		i = snprintf(buf, BLOCK, "mvc_export_table( myc, Output, %d, 0, -1, \"\\t\", \"\\n\");\n", id);
		dbc->Mws->write(dbc->Mws, buf, i, 1);
		dbc->Mws->flush(dbc->Mws);
		}
		status = next_result(rs, hstmt, &type);
		if (status == SQL_ERROR)
			return status;
	}
	if (type == Q_TABLE && status > 0) {
		char *sc, *ec;
		bstream *bs = bstream_create(rs, BLOCK);
		int eof = 0;

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
	} else if (Q_UPDATE) {  
		hstmt->nrRows = nRows;
		hstmt->ResultRows = NULL;
	}

	} /* end of "get result data" code block */

	hstmt->State = EXECUTED;
	return SQL_SUCCESS;
}
