/**********************************************************************
 * SQLTables
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

/****************************
 * STANDARD COLUMNS RETURNED BY SQLTables
 ***************************/
enum nSQLTables
{
  TABLE_CAT = 1,
  TABLE_SCHEM,
  TABLE_NAME,
  TABLE_TYPE,
  REMARKS,
  COL_MAX
};

/****************************
 * COLUMN HEADERS (1st row of result set)
 ***************************/
char *aSQLTables[] =
{
  "",
  "TABLE_CAT",
  "TABLE_SCHEM",
  "TABLE_NAME",
  "TABLE_TYPE",
  "REMARKS"
};


SQLRETURN SQLTables(SQLHSTMT    hDrvStmt,
					SQLCHAR     *szCatalogName,
					SQLSMALLINT nCatalogNameLength,
					SQLCHAR     *szSchemaName,
					SQLSMALLINT nSchemaNameLength,
					SQLCHAR     *szTableName,
					SQLSMALLINT nTableNameLength,
					SQLCHAR     *szTableType,
					SQLSMALLINT nTableTypeLength)
{
  HDRVSTMT hStmt = (HDRVSTMT)hDrvStmt;
  COLUMNHDR *pColumnHeader;			
  long nResultMemory;
  int nColumn;
  context *sql;
  catalog *cat;
  list *schemas;
  list *tables;
  node *n;
  table *t;
  int nRow;
  int nCols;
  int nRows;


  /* SANITY CHECKS */
  if( hStmt == SQL_NULL_HSTMT )
    return SQL_INVALID_HANDLE;

  sprintf( hStmt->szSqlMsg, "hStmt = $%08lX", hStmt );
  logPushMsg( hStmt->hLog, __FILE__, __FILE__, __LINE__, LOG_WARNING, LOG_WARNING, hStmt->szSqlMsg );

  /**************************
   * close any existing result
   **************************/
  if ( hStmt->hStmtExtras->aResults )
    _FreeResults( hStmt->hStmtExtras );

  if ( hStmt->pszQuery != NULL )
    free( hStmt->pszQuery );

  hStmt->pszQuery = NULL;
	
  /************************
   * generate a result set listing tables
   ************************/

  sql = &((DRVDBC*)hStmt->hDbc)->hDbcExtras->lc;
  schemas = sql->cat->schemas;
  tables = ((struct schema*) schemas->h->data)->tables;
  
  /*************************
   * allocate memory for columns headers and result data (row 0 is column header while col 0 is reserved for bookmarks)
   *************************/

  hStmt->hStmtExtras->nCols = nCols = COL_MAX-1;
  hStmt->hStmtExtras->nRows = nRows = tables->cnt;
  hStmt->hStmtExtras->nRow = 0;
  hStmt->hStmtExtras->aResults = NEW_ARRAY(char*, (nCols+1)*(nRows+1));


  /**************************
   * gather column header information (save col 0 for bookmarks)
   **************************/

  for ( nColumn = 1; nColumn < COL_MAX; nColumn++ ) {
    COLUMNHDR* cHdr = NEW(COLUMNHDR);
    (hStmt->hStmtExtras->aResults[nColumn]) = (char*)cHdr;
    pColumnHeader = (COLUMNHDR*)(hStmt->hStmtExtras->aResults)[nColumn];
    memset( pColumnHeader, 0, sizeof(COLUMNHDR) );
    _NativeToSQLColumnHeader( pColumnHeader, &(aSQLTables[nColumn]) );
  }


  /************************
   * gather data (save col 0 for bookmarks)
   ************************/

  hStmt->hStmtExtras->nRow = nRow = 0;
  n=tables->h;

  while(n != NULL) {
    t=((struct table*) n->data);
    printf("for-loop, index is %d\n",nRow);

    hStmt->hStmtExtras->nRow = nRow += 1;
    for ( nColumn = 1; nColumn<=nCols; nColumn++ ) {
      printf("inner for-loop, index is %d\n",nColumn);

      switch ( nColumn ) {
      case TABLE_NAME:
		printf("Table name is %s\n",t->name);
		(hStmt->hStmtExtras->aResults)[hStmt->hStmtExtras->nRow*hStmt->hStmtExtras->nCols+nColumn] = strdup(t->name);
		break;
      default:
		(hStmt->hStmtExtras->aResults)[hStmt->hStmtExtras->nRow*hStmt->hStmtExtras->nCols+nColumn] = NULL;
      }
    }
    n = n->next;
  }
  hStmt->hStmtExtras->nRow = 0;


  /**************************
   * free the snapshot
   **************************/

  logPushMsg( hStmt->hLog, __FILE__, __FILE__, __LINE__, LOG_INFO, LOG_INFO, "SQL_SUCCESS" );
  return SQL_SUCCESS;
}



