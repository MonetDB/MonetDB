/**********************************************************************
 * SQLExecute
 *
 **********************************************************************
 *
 * This code was created by Peter Harvey (mostly during Christmas 98/99).
 * This code is LGPL. Please ensure that this message remains in future
 * distributions and uses of this code (thats about all I get out of it).
 * - Peter Harvey pharvey@codebydesign.com
 *
 **********************************************************************/

#include "driver.h"

extern statement *sqlexecute( context *, char *);

#define CHUNK (64*1024)

static char *readblock( stream *s ){
	int len = 0;
	int size = CHUNK + 1;
	char *buf = NEW_ARRAY(char, size ), *start = buf;

	while ((len = s->read(s, start, 1, CHUNK)) == CHUNK){
		size += CHUNK;
		buf = RENEW_ARRAY(char, buf, size); 
		start+= CHUNK;
		*start = '\0';
	}
	start += len;
	*start = '\0';
	return buf;
}

SQLRETURN SQLExecute( SQLHSTMT  hDrvStmt )
{
    HDRVSTMT 	hStmt	= (HDRVSTMT)hDrvStmt;
	int			nColumn = 0;
	int			nRow = 0;
	int			nCols = 0;
	int			nRows = 0;
	COLUMNHDR	*pColumnHeader;			
	statement *res = NULL;
	context *sql;
	stream *rs;

	/* SANITY CHECKS */
    if( NULL == hStmt )
        return SQL_INVALID_HANDLE;

	sprintf( (char*)hStmt->szSqlMsg, "hStmt = $%08lX", hStmt );
    logPushMsg( hStmt->hLog, __FILE__, __FILE__, __LINE__, LOG_WARNING, LOG_WARNING, hStmt->szSqlMsg );

    if( hStmt->pszQuery == NULL )
    {
		logPushMsg( hStmt->hLog, __FILE__, __FILE__, __LINE__, LOG_WARNING, LOG_WARNING, "SQL_ERROR No prepared statement" );
        return SQL_ERROR;
    }

    sql = &((DRVDBC*)hStmt->hDbc)->hDbcExtras->lc;
    rs = ((DRVDBC*)hStmt->hDbc)->hDbcExtras->rs;

    /**************************
	 * Free any current results
     **************************/
	if ( hStmt->hStmtExtras->aResults )
		_FreeResults( hStmt->hStmtExtras );

    /**************************
	 * send prepared query to server
     **************************/

	printf("before sqlexecute\n");
	res =  sqlexecute( 
			&((DRVDBC*)hStmt->hDbc)->hDbcExtras->lc,
			(char*)hStmt->pszQuery
		); 
	printf("after sqlexecute\n");

	if (res){
	    int nr = 1;
	    statement_dump( res, &nr, sql );

	    sql->out->flush( sql->out );
	}
	printf("done writing\n");

    /**************************
	 * allocate memory for columns headers and result data (row 0 is column header while col 0 is reserved for bookmarks)
     **************************/
	/* optain row count from server */
	hStmt->hStmtExtras->nRows = 0;
	if (res && res->type == st_list){
		nCols = list_length(res->op1.lval);
		hStmt->hStmtExtras->nCols = nCols;
	}
	if (res && res->type == st_output){
		list *l = res->op1.stval->op1.lval;
		node *n = l->h;
		char *buf = readblock( rs ), *start = buf, *m = buf;

		nRows = strtol(m,&m,10);
		m++;

		hStmt->hStmtExtras->nRows = nRows; 

		nCols = list_length(l);
		hStmt->hStmtExtras->aResults = NEW_ARRAY(char*,(nCols+1)*(nRows+1));
		hStmt->hStmtExtras->nCols = nCols;
		for( nColumn = 1; nColumn <= nCols; nColumn++){
			column *col = basecolumn(n->data.stval);
			COLUMNHDR* cHdr = NEW(COLUMNHDR);
			(hStmt->hStmtExtras->aResults[nColumn]) = (char*)cHdr;
			cHdr->pszSQL_DESC_BASE_COLUMN_NAME = strdup(col->name);
			cHdr->pszSQL_DESC_BASE_TABLE_NAME = strdup(col->table->name);
			cHdr->pszSQL_DESC_CATALOG_NAME = strdup("catalog");
			cHdr->pszSQL_DESC_LABEL = strdup(column_name(n->data.stval));
			cHdr->pszSQL_DESC_LITERAL_PREFIX = strdup("pre");
			cHdr->pszSQL_DESC_LITERAL_SUFFIX = strdup("suf");
			cHdr->pszSQL_DESC_LOCAL_TYPE_NAME = strdup(col->tpe->name);
			cHdr->pszSQL_DESC_NAME = strdup("name");
			cHdr->pszSQL_DESC_SCHEMA_NAME = strdup("schema");
			cHdr->pszSQL_DESC_TABLE_NAME = strdup("table");
			cHdr->pszSQL_DESC_TYPE_NAME = strdup(col->tpe->sqlname);
			n = n->next;
		}

		start = buf;
	        for( nRow = 1; nRow <= nRows; nRow++){
	 	 for( nColumn = 1; nColumn <= nCols; nColumn++){
		  start = m;
		  while(*m){
			if (*m == '\t' || *m == '\n' ){
				break;
			}
			m++;
		  }
		  *m = '\0';
		  hStmt->hStmtExtras->aResults[nRow*nCols+nColumn] = 
			  strdup(start);
		  m++; 
		 }
		}
		_DELETE(buf);
	}
	if (res) statement_destroy(res);

    /**************************
	 * gather column header information (save col 0 for bookmarks)
     **************************/

	/************************
	 * gather data (save col 0 for bookmarks)
	 ************************/

    /**************************
	 * free the snapshot
     **************************/

    logPushMsg( hStmt->hLog, __FILE__, __FILE__, __LINE__, LOG_INFO, LOG_INFO, "SQL_SUCCESS" );
    return SQL_SUCCESS;
}




