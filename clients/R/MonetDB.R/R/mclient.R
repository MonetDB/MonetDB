mclient <- function(conn, n=20) {
	res <- NA
	cat(paste0("Tables: ", paste0(sort(dbListTables(conn)), collapse=", "), "\n"))
	cat("Enter SQL queries or table names below. 'Q' to exit.\n")
	repeat {
		tryCatch({
			repeat {
				query <- readline("sql> ")
				if (tolower(query) == "q") {
					return(invisible(TRUE))
				}
				if (tolower(query) %in% tolower(dbListTables(conn))) {
					cat(paste0("Fields in table ", query, ": ", paste0(dbListFields(conn, query), collapse=", "), "\n"))
					next
				}
				if (nchar(query) < 5) {
					next
				}
				res <- dbGetQuery(conn, query)
				print(head(res, n))
				break
			}}
		, error = function(e) {
			message(e, "\n")
		})
	}
}
