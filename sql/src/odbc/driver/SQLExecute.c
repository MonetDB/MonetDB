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

extern stmt *sqlexecute( context *, char *);

char *receive( stream *rs ){
	char *buf = NULL;
	int flag = 0;
	if (rs->readInt(rs, &flag) && flag != COMM_DONE){
		int size = BLOCK+1, nr, last = 0;

		buf = malloc(size);
	        nr = bs_read_next(rs,buf,&last);
		while(!last){
			buf = realloc(buf,size+BLOCK);
			nr = bs_read_next(rs,buf,&last);
		}
	} else if (flag != COMM_DONE){
		printf("flag %d\n", flag);
	}
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
	stmt *res = NULL;
	context *sql;
	stream *rs;

	/* SANITY CHECKS */
    if( NULL == hStmt )
        return SQL_INVALID_HANDLE;

	sprintf( (char*)hStmt->szSqlMsg, "hStmt = $%08lX", (long)hStmt );
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

	res =  sqlexecute( sql, (char*)hStmt->pszQuery); 

	if (res){
	    int nr = 1;
	    stmt_dump( res, &nr, sql );

	    sql->out->flush( sql->out );
	}

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
		list *l;
		node *n;
		char *buf = receive( rs ), *start = buf, *m = buf;

		if (res->op1.stval->type == st_order){
			l = res->op2.stval->op1.lval;
		} else {
			l = res->op1.stval->op1.lval;
		}

		nRows = strtol(m,&m,10);
		n = l->h;
		m++;
		hStmt->hStmtExtras->nRows = nRows;
		nCols = list_length(l); 
		printf("nRows %d, nCols %d\n", nRows, nCols );
		hStmt->hStmtExtras->aResults = NEW_ARRAY(char*,(nCols+1)*(nRows+1));
		hStmt->hStmtExtras->nCols = nCols;
		for( nColumn = 1; nColumn <= nCols; nColumn++){
			stmt *cs = tail_column(n->data);
			COLUMNHDR* cHdr = NEW(COLUMNHDR);
			memset(cHdr,0,sizeof(cHdr));
			(hStmt->hStmtExtras->aResults[nColumn]) = (char*)cHdr;
			if (cs){
				column *col = cs->op1.cval;
				cHdr->pszSQL_DESC_BASE_COLUMN_NAME = strdup(col->name);
				cHdr->pszSQL_DESC_BASE_TABLE_NAME = strdup(col->table->name);
				cHdr->pszSQL_DESC_TYPE_NAME = strdup(col->tpe->sqlname);
				cHdr->pszSQL_DESC_LOCAL_TYPE_NAME = strdup(col->tpe->name);
				printf("column %s %s\n", cHdr->pszSQL_DESC_BASE_COLUMN_NAME, cHdr->pszSQL_DESC_TYPE_NAME);
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
	if (res) stmt_destroy(res);

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




