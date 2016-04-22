src_monetdb <- function(dbname="demo", host = "localhost", port = 50000L, user = "monetdb",
  password = "monetdb", con = FALSE, ...) {
  if (!inherits(con, "MonetDBConnection") || !DBI::dbIsValid(con)) {
    con <- DBI::dbConnect(MonetDB.R(), dbname = dbname , host = host, port = port,
      user = user, password = password, ...)
  }
  s <- dplyr::src_sql("monetdb", con, info = DBI::dbGetInfo(con))

  # this is a (dirty) hack so we don't need to depend on dplyr
  dplyrMt <- getNamespace("dplyr")$.__S3MethodsTable__.

  dplyrMt[["src_translate_env.src_monetdb"]]           <- src_translate_env.src_monetdb
  dplyrMt[["src_desc.src_monetdb"]]                    <- src_desc.src_monetdb
  dplyrMt[["tbl.src_monetdb"]]                         <- tbl.src_monetdb
  dplyrMt[["db_query_fields.MonetDBConnection"]]       <- db_query_fields.MonetDBEmbeddedConnection
  dplyrMt[["db_query_rows.MonetDBConnection"]]         <- db_query_rows.MonetDBConnection
  dplyrMt[["db_query_rows.MonetDBEmbeddedConnection"]] <- db_query_rows.MonetDBEmbeddedConnection
  dplyrMt[["db_save_query.MonetDBConnection"]]         <- db_save_query.MonetDBConnection
  dplyrMt[["db_insert_into.MonetDBConnection"]]        <- db_insert_into.MonetDBConnection
  dplyrMt[["db_create_index.MonetDBConnection"]]       <- db_create_index.MonetDBConnection
  dplyrMt[["db_analyze.MonetDBConnection"]]            <- db_analyze.MonetDBConnection
  dplyrMt[["sql_subquery.MonetDBConnection"]]          <- sql_subquery.MonetDBConnection
  dplyrMt[["sample_n.tbl_monetdb"]]                    <- sample_n.tbl_monetdb
  dplyrMt[["sample_frac.tbl_monetdb"]]                 <- sample_frac.tbl_monetdb

  s
}

src_translate_env.src_monetdb <- function(x) {
  dplyr::sql_variant(
    dplyr::sql_translator(.parent = dplyr::base_scalar,
    `!=` = dplyr::sql_infix("<>")
    ),
    dplyr::sql_translator(.parent = dplyr::base_agg,
      n = function() dplyr::sql("COUNT(*)"),
      sd =  dplyr::sql_prefix("STDDEV_SAMP"),
      var = dplyr::sql_prefix("VAR_SAMP"),
      median = dplyr::sql_prefix("MEDIAN"),
      n_distinct = function(x) {dplyr::build_sql(dplyr::sql("count(distinct "), 
        x, dplyr::sql(")"))}
    )
  )
}

src_desc <- function(x) UseMethod("src_desc")

src_desc.src_monetdb <- function(x) {
  paste0("MonetDB ",x$info$monet_version, " (",x$info$monet_release, ")")
}

tbl.src_monetdb <- function(src, from, ...) {
  monetdb_check_subquery(from)
  dplyr::tbl_sql("monetdb", src = src, from = from, ...)
}

sample_n.tbl_monetdb <- function(x, size, replace = FALSE, weight = NULL) {
  if (replace || !is.null(weight)) {
    stop("Sorry, replace and weight are not supported for MonetDB tables. \
      Consider collect()'ing first.")
  }
  DBI::dbGetQuery(x$src$con, dplyr::build_sql(x$query$sql, " SAMPLE ", as.integer(size)))
}

sample_frac.tbl_monetdb <- function(tbl, frac=1, replace = FALSE, weight = NULL) {
  if (frac < 0 || frac > 1) {
    stop("frac must be in [0,1]")
  }
  n <- as.integer(round(dim(tbl)[[1]] * frac))
  if (n < 1) {
    stop("not sampling 0 rows...")
  }
  dplyr::sample_n(tbl, n, replace, weight)
}

db_query_fields.MonetDBConnection <- function(con, sql, ...) {
  # prepare gives us column info without actually running a query. Nice.
  DBI::dbGetQuery(con, dplyr::build_sql("PREPARE SELECT * FROM ", sql))$column
}

db_query_fields.MonetDBEmbeddedConnection <- function(con, sql, ...) {
  names(DBI::dbGetQuery(con, dplyr::build_sql("SELECT * FROM ", sql), execute = F))
}

db_query_rows.MonetDBConnection <- function(con, sql, ...) {
  monetdb_queryinfo(con,sql)$rows
}

db_query_rows.MonetDBEmbeddedConnection <- function(con, sql, ...) {
  attr(DBI::dbGetQuery(con, sql, resultconvert = F), "__rows")
}

db_insert_into.MonetDBConnection <- function(con, table, values, ...) {
  DBI::dbWriteTable(con, DBI::dbQuoteIdentifier(con, table), values,
    append=T, transaction=F, csvdump=T)
}

db_save_query.MonetDBConnection <- function(con, sql, name, temporary = TRUE,
                                            ...) {
  tt_sql <- dplyr::build_sql("CREATE TEMPORARY TABLE ", dplyr::ident(name), " AS ",
    sql, " WITH DATA ON COMMIT PRESERVE ROWS", con = con)
  DBI::dbGetQuery(con, tt_sql)
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
