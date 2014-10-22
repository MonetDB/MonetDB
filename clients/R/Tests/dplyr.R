ll <- NULL
if (Sys.getenv("TSTTRGDIR") != "") {
	ll <- paste0(Sys.getenv("TSTTRGDIR"),"/rlibdir")
}
ff <- textConnection("asdf", open="w")
# hide output from connect and attach since it would blow up the test output
# dangerous since it might hide useful warnings
# so if things go south it might be a good idea to uncomment the cat(dd) below
dd <- capture.output( suppressMessages ( {

library(MonetDB.R,quietly=T,lib.loc=ll)
library(dplyr,quietly=T)
library(Lahman,quietly=T)

args <- commandArgs(trailingOnly = TRUE)
dbport <- 50000
dbname <- "mTests_clients_R"
if (length(args) > 0) 
	dbport <- args[[1]]
if (length(args) > 1) 
	dbname <- args[[2]]


# old way
if (exists("lahman_monetdb")) {
	# overwrite all args because lahman_monetdb sets a default arg in the first pos.
	dps <- lahman_monetdb(host="localhost", dbname=dbname, port=dbport ,
		user="monetdb",password="monetdb",timeout=100,wait=T,language="sql")
# new way
} else {
	dps <- src_monetdb(dbname=dbname, port=dbport)
	copy_lahman(dps)
}
}))

#cat(dd)

# the remainder is pretty much the example from the manpage.

# Methods -------------------------------------------------------------------
batting <- tbl(dps, "Batting")

length(dim(batting)) == 2

length(colnames(batting)) > 1
nrow(head(batting, n=10L))

# co* verbs
cc <- collapse(batting)
cc <- collect(batting)
# cc <- compute(batting)
# head(cc)


# Data manipulation verbs ---------------------------------------------------
nrow(head(filter(batting, yearID > 2005, G > 130), n=10L))
nrow(head(select(batting, playerID:lgID), n=10L))
nrow(head(arrange(batting, playerID, desc(yearID)), n=10L))
length(summarise(batting, G = mean(G), n = n())) > 1
nrow(head(mutate(batting, rbi2 = if(!is.null(AB) & AB > 0) 1.0 * R / AB else 0), n=10L))

# note that all operations are lazy: they don't do anything until you
# request the data, either by `print()`ing it (which shows the first ten
# rows), by looking at the `head()`, or `collect()` the results locally.
nrow(head(collect(filter(batting, yearID > 2010)), n=10L))

# Group by operations -------------------------------------------------------
# To perform operations by group, create a grouped object with group_by
players <- group_by(batting, playerID)
length(group_size(players)) > 1
nrow(head(summarise(players, mean_g = mean(G), best_ab = max(AB)), n=10L))

# When you group by multiple level, each summarise peels off one level
per_year <- group_by(batting, playerID, yearID)
stints <- summarise(per_year, stints = max(stint))
nrow(head(filter(stints, stints > 3), n=10L))
nrow(head(summarise(stints, max(stints)), n=10L))

# Joins ---------------------------------------------------------------------
player_info <- select(tbl(dps, "Master"), playerID,
  birthYear)
hof <- select(filter(tbl(dps, "HallOfFame"), inducted == "Y"),
 playerID, votedBy, category)

invisible(suppressMessages( {

# Match players and their hall of fame data
print(nrow(head(inner_join(player_info, hof), n=10L)))
# Keep all players, match hof data where available
print(nrow(head(left_join(player_info, hof), n=10L)))
# Find only players in hof
print(nrow(head(semi_join(player_info, hof), n=10L)))
# Find players not in hof
print(nrow(head(anti_join(player_info, hof), n=10L)))

}))
# TODO: set ops

# Arbitrary SQL -------------------------------------------------------------
# You can also provide sql as is, using the sql function:
batting2008 <- tbl(dps,
  sql('SELECT * FROM "Batting" WHERE "yearID" = 2008'))
nrow(head(batting2008, n=10L))

print("SUCCESS")
