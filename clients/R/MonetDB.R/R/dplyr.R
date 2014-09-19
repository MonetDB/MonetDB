require(dplyr)

src_monetdb <- function(dbname, host = "localhost", port = 50000L, user = "monetdb",
  password = "monetdb", ...) {
  con <- dbConnect(MonetDB.R(), dbname = dbname , host = host, port = port,
    user = user, password = password, ...)
  src_sql("monetdb", con, info = dbGetInfo(con))
}

translate_env.src_monetdb <- function(x) {
  sql_variant(
    base_scalar,
    sql_translator(.parent = base_agg,
      n = function() sql("COUNT(*)"),
      sd =  sql_prefix("STDDEV_SAMP"),
      var = sql_prefix("VAR_SAMP"),
      median = sql_prefix("MEDIAN")
    )
  )
}

brief_desc.src_monetdb <- function(x) {
  paste0("MonetDB ",x$info$monet_version, " (",x$info$monet_release, ") [", x$info$merovingian_uri,"]")
}

tbl.src_monetdb <- function(src, from, ...) {
  monetdb_check_subquery(from)
  tbl_sql("mownetdb", src = src, from = from, ...)
}

# sql_create_index.src_monetdb

db_query_fields.MonetDBConnection <- function(con, sql, ...) {
  # prepare gives us column info without actually running a query
  dbGetQuery(con,build_sql("PREPARE SELECT * FROM ", ident(sql)))$column
}

db_query_rows.MonetDBConnection <- function(con, sql, ...) {
  monetdb_queryinfo(con,sql)$rows
}

db_insert_into.MonetDBConnection <- function(con, table, values, ...) {
  dbWriteTable(con,table,values,append=T,transaction=F,csvdump=T)
}

db_save_query.MonetDBConnection <- function(con, sql, name, temporary = TRUE,
                                            ...) {
  tt_sql <- build_sql("CREATE TEMPORARY TABLE ", ident(name), " AS ",
    sql, " WITH DATA", con = con)
  dbGetQuery(con, tt_sql)
  name
}

db_begin.MonetDBConnection <- function(con, ...) {
  dbBegin(con)
}

db_create_index.MonetDBConnection <- function(con, table, columns, name = NULL,
                                           ...) {
  TRUE
}

db_analyze.MonetDBConnection <- function(con, table, ...) {
  TRUE
}

# this should be the default in dplyr anyways...
db_begin.MonetDBConnection <- function(con, ...) dbBegin(con)

sql_subquery.MonetDBConnection <- function(con, sql, name = unique_name(), ...) {
  if (is.ident(sql)) return(sql)
  monetdb_check_subquery(sql)
  build_sql("(", sql, ") AS ", ident(name), con = con)
}


# sql_analyze.src_monetdb

monetdb_check_subquery <- function(sql) {
  if (grepl("ORDER BY|LIMIT|OFFSET", as.character(sql), ignore.case=TRUE)) {
    stop(sql," contains ORDER BY, LIMIT or OFFSET keywords, which are not supported.")
  }
}

monetdb_queryinfo <- function(conn, query) {
  info <- emptyenv()
  tryCatch({
    .mapiRequest(conn, "Xreply_size 1")
    res <- dbSendQuery(conn, query)
    info <- res@env$info
    dbClearResult(res);
  }, error = function(e) {
    print(e)
    warning("Failed to calculate result set size for ", query)
  }, finally = {
    .mapiRequest(conn, paste0("Xreply_size ", REPLY_SIZE))
  })
  info
}