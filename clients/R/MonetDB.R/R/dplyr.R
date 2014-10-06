src_monetdb <- function(dbname, host = "localhost", port = 50000L, user = "monetdb",
  password = "monetdb", ...) {
  requireNamespace("dplyr")
  con <- dbConnect(MonetDB.R(), dbname = dbname , host = host, port = port,
    user = user, password = password, ...)
  dplyr::src_sql("monetdb", con, info = dbGetInfo(con))
}

src_translate_env.src_monetdb <- function(x) {
  dplyr::sql_variant(
    dplyr::base_scalar,
    dplyr::sql_translator(.parent = dplyr::base_agg,
      n = function() dplyr::sql("COUNT(*)"),
      sd =  dplyr::sql_prefix("STDDEV_SAMP"),
      var = dplyr::sql_prefix("VAR_SAMP"),
      median = dplyr::sql_prefix("MEDIAN")
    )
  )
}

src_desc.src_monetdb <- function(x) {
  paste0("MonetDB ",x$info$monet_version, " (",x$info$monet_release, ") [", x$info$merovingian_uri,"]")
}

tbl.src_monetdb <- function(src, from, ...) {
  monetdb_check_subquery(from)
  dplyr::tbl_sql("monetdb", src = src, from = from, ...)
}

db_query_fields.MonetDBConnection <- function(con, sql, ...) {
  # prepare gives us column info without actually running a query. Nice.
  dbGetQuery(con, dplyr::build_sql("PREPARE SELECT * FROM ", sql))$column
}

db_query_rows.MonetDBConnection <- function(con, sql, ...) {
  monetdb_queryinfo(con,sql)$rows
}

db_insert_into.MonetDBConnection <- function(con, table, values, ...) {
  dbWriteTable(con,dbQuoteIdentifier(con,table),values,
    append=T,transaction=F,csvdump=T)
}

db_save_query.MonetDBConnection <- function(con, sql, name, temporary = TRUE,
                                            ...) {
  tt_sql <- dplyr::build_sql("CREATE TEMPORARY TABLE ", dplyr::ident(name), " AS ",
    sql, " WITH DATA", con = con)
  dbGetQuery(con, tt_sql)
  name
}

db_create_index.MonetDBConnection <- function(con, table, columns, name = NULL,
                                           ...) {
  TRUE
}

db_analyze.MonetDBConnection <- function(con, table, ...) {
  TRUE
}

sql_subquery.MonetDBConnection <- function(con, sql, name = unique_name(), ...) {
  if (dplyr::is.ident(sql)) return(sql)
  monetdb_check_subquery(sql)
  dplyr::build_sql("(", sql, ") AS ", dplyr::ident(name), con = con)
}

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

# copied from dplyr's utils.r, sql_subquery needs it
unique_name <- local({
  i <- 0

  function() {
    i <<- i + 1
    paste0("_W", i)
  }
})