/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at
 * http://monetdb.cwi.nl/Legal/MonetDBPL-1.0.html
 *
 * Software distributed under the License is distributed on an
 * "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
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
 * 		Martin Kersten  <Martin.Kersten@cwi.nl>
 * 		Peter Boncz  <Peter.Boncz@cwi.nl>
 * 		Niels Nes  <Niels.Nes@cwi.nl>
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

#include <sqlexecute.h>	/* for sqlexecute(), see src/common/sqlexecute.c */
#include "ODBCGlobal.h"
#include "ODBCStmt.h"


/* does ?? retuns ?? */
/* TODO: move this utility function to src/common/sqlUtil.c */
static char * receive(stream * rs)
{
	int size = BLOCK+1, last = 0;
	char *buf = malloc(size);
	int nr = bs_read_next(rs,buf,&last);

	while (!last) {
		buf = realloc(buf,size+BLOCK);
		nr = bs_read_next(rs,buf,&last);
	}
	return buf;
}


SQLRETURN SQLExecute(SQLHSTMT hStmt)
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
	assert(hstmt->Result == NULL);

	dbc = hstmt->Dbc;
	assert(dbc);
	sqlContext = &dbc->Mlc;
	assert(sqlContext);

	/* Send the Query to the server for execution */
	res = sqlexecute(sqlContext, hstmt->Query);
	if (res == NULL) {
		/* Failed to execute the query */
		if (sqlContext->errstr != NULL) {
			addStmtError(hstmt, "HY000", sqlContext->errstr, 0);
		} else {
			addStmtError(hstmt, "HY000", "Error: Could not execute SQL statement", 0);
		}
		return SQL_ERROR;
	}

	/* now get the result data and store it to our internal data structure */
	assert(res != NULL);
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

	/* ?? */
	int nr = 1;
	stmt_dump(res, &nr, sqlContext);

	/* ?? */
	sqlContext->out->flush(sqlContext->out);

	/* initialize the Result meta data values */
	hstmt->nrCols = 0;
	hstmt->nrRows = 0;
	hstmt->currentRow = 0;

	rs = dbc->Mrs;
	if (stream_readInt(rs, &flag) && flag == COMM_DONE) {
		/* 08S01 = Communication link failure */
		addStmtError(hstmt, "08S01", NULL, 0);
		return SQL_ERROR;
	}

	stream_readInt(rs, &type);		/* read result type */
	stream_readInt(rs, &status);	/* read result size (is < 0 on error) */
	if (status < 0) {
		/* output error */
		char buf[BLOCK];
		int last = 0;
		int nr = bs_read_next(rs, buf, &last);
		/* ?? */
		while (!last) {
			nr = bs_read_next(rs, buf, &last);
		}
		/* HY000 = General Error */
		addStmtError(hstmt, "HY000", "No result available (status < 0)", 0);
		return SQL_ERROR;
	}
	nRows = status;

	if (type == QTABLE) {
		list *l;
		node *n;
		char *buf = (nRows > 0) ? receive(rs) : NULL;
		char *start = buf;
		char *m = buf;

		if (res->op1.stval->type == st_order) {
			l = res->op2.stval->op1.lval;
		} else {
			l = res->op1.stval->op1.lval;
		}

		n = l->h;

		hstmt->nrRows = nRows;
		nCols = list_length(l);
		hstmt->nrCols = nCols;
		/* allocate memory for columns headers and result data
		 * row 0 is column header while col 0 is reserved for bookmarks
		 */
		hstmt->Result = NEW_ARRAY(char*,(nCols+1)*(nRows+1));
		assert(hstmt->Result != NULL);

		/* First fill the header info */
		for (nCol = 1; nCol <= nCols; nCol++) {
			stmt *cs = tail_column(n->data);
			ColumnHeader * cHdr = NEW(ColumnHeader);
			memset(cHdr,0,sizeof(cHdr));
			(hstmt->Result[nCol]) = (char*)cHdr;
			if (cs) {
				column *col = cs->op1.cval;
				cHdr->pszSQL_DESC_BASE_COLUMN_NAME = strdup(col->name);
				cHdr->pszSQL_DESC_BASE_TABLE_NAME = strdup(col->table->name);
				cHdr->pszSQL_DESC_TYPE_NAME = strdup(col->tpe->type->sqlname);
				cHdr->pszSQL_DESC_LOCAL_TYPE_NAME = strdup(col->tpe->type->name);
			}
			cHdr->pszSQL_DESC_LABEL = strdup(column_name(n->data));
			cHdr->pszSQL_DESC_CATALOG_NAME = strdup("catalog");
			cHdr->pszSQL_DESC_LITERAL_PREFIX = strdup("pre");
			cHdr->pszSQL_DESC_LITERAL_SUFFIX = strdup("suf");
			cHdr->pszSQL_DESC_NAME = strdup("name");
			cHdr->pszSQL_DESC_SCHEMA_NAME = strdup("schema");
			cHdr->pszSQL_DESC_TABLE_NAME = strdup("table");
			n = n->next;
		}

		/* Next copy data from all columns for all rows */
		/* TODO: this should be altered because on large result sets */
		for (nRow = 1; nRow <= nRows; nRow++) {
			for (nCol = 1; nCol <= nCols; nCol++) {
				start = m;
				while (*m) {
					if (*m == '\t' || *m == '\n' ) {
						break;
					}
					m++;
				}
				*m = '\0';
				hstmt->Result[nRow*nCols+nCol] = strdup(start);
				m++;
			}
		}
		_DELETE(buf);
	} else {
		/* HY000 = General Error */
		addStmtError(hstmt, "HY000", "Result type was not QTABLE", 0);
		return SQL_ERROR;
	}

	stmt_destroy(res);
	} /* end of "get result data" code block */

	hstmt->State = EXECUTED;
	return SQL_SUCCESS;
}
