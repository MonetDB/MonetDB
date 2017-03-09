ff <- textConnection("asdf", open="w")
# hide output from connect and attach since it would blow up the test output
# dangerous since it might hide useful warnings
# so if things go south it might be a good idea to uncomment the cat(dd) below
dd <- capture.output( suppressMessages ( {

library(dplyr, quietly = T)

args <- commandArgs(trailingOnly = TRUE)
dbport <- 50000
dbname <- "mTests_clients_R"
if (length(args) > 0) 
	dbport <- args[[1]]
if (length(args) > 1) 
	dbname <- args[[2]]

dps <- MonetDBLite::src_monetdb(dbname=dbname, port=dbport)
if (!DBI::dbExistsTable(con_acquire(dps), "AllstarFull")) copy_lahman(dps)

}))

printn <- 10
printsth <- function(s) {
	print(nrow(collect(head(s, printn))))
	printn <<- printn+1
}

# the remainder is pretty much the example from the manpage.

# Methods -------------------------------------------------------------------
batting <- tbl(dps, "Batting")

length(dim(batting)) == 2
length(colnames(batting)) > 1

printsth(batting)

# co* verbs
c1 <- collapse(batting)
c2 <- collect(batting)
c3 <- compute(batting)

printsth(c1)
printsth(c2)
printsth(c3)


# Data manipulation verbs ---------------------------------------------------
printsth(filter(batting, yearID > 2005, G > 130))
printsth(select(batting, playerID:lgID))
#printsth(arrange(batting, playerID, desc(yearID)))
length(summarise(batting, G = mean(G), n = n())) > 1
printsth(mutate(batting, rbi2 = if(!is.null(AB) & AB > 0) 1.0 * R / AB else 0))

# note that all operations are lazy: they don't do anything until you
# request the data, either by `print()`ing it (which shows the first ten
# rows), by looking at the `head()`, or `collect()` the results locally.
printsth(filter(batting, yearID > 2010))

# Group by operations -------------------------------------------------------
# To perform operations by group, create a grouped object with group_by
players <- group_by(batting, playerID)
length(group_size(players)) > 1
nrow(head(summarise(players, mean_g = mean(G), best_ab = max(AB)), n=16L))

# When you group by multiple level, each summarise peels off one level
per_year <- group_by(batting, playerID, yearID)
stints <- summarise(per_year, stints = max(stint))
nrow(head(filter(stints, stints > 3), n=17L))
nrow(head(summarise(stints, max(stints)), n=18L))

# Joins ---------------------------------------------------------------------
player_info <- select(tbl(dps, "Master"), playerID,
  birthYear)
hof <- select(filter(tbl(dps, "HallOfFame"), inducted == "Y"),
 playerID, votedBy, category)

invisible(suppressMessages( {

# Match players and their hall of fame data
printsth(inner_join(player_info, hof))
# Keep all players, match hof data where available
printsth(left_join(player_info, hof))
# Find only players in hof
printsth(semi_join(player_info, hof))
# Find players not in hof
printsth(anti_join(player_info, hof))

}))
# TODO: set ops

# Arbitrary SQL -------------------------------------------------------------
# You can also provide sql as is, using the sql function:
batting2008 <- tbl(dps,
  sql('SELECT * FROM "Batting" WHERE "yearID" = 2008'))
printsth(batting2008)

# sample functions
printsth(sample_n(player_info, 24L))
printsth(sample_frac(player_info, .5))


DBI::dbWriteTable(con_acquire(dps), "mtcars", mtcars, overwrite=T)
my_tbl <- tbl(dps, "mtcars") 

# https://github.com/hadley/dplyr/issues/1165
aa <- my_tbl %>% 
    group_by( cyl , gear ) %>% 
    summarise( n = n() ) %>% collect()
print(nrow(aa))

# this works fine
aa <- my_tbl %>% 
    group_by( cyl , gear ) %>% 
    summarise( n = n() ) %>% collect()
print(nrow(aa))


DBI::dbRemoveTable(con_acquire(dps), "mtcars")


print("SUCCESS")
