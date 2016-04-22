library(testthat)
library(MonetDB.R)
library(dplyr)
library(DBI)

library(Lahman)
data(Batting)

test_that("source creation / import", {
	dps <<- src_monetdb(dbname="", embedded=tempdir())
	copy_lahman(dps)
})

test_that("basics", {
	batting <<- tbl(dps, "Batting")
	expect_equal(dim(batting), dim(Batting))
	expect_equal(colnames(batting), names(Batting))
	expect_equal(nrow(batting), nrow(Batting))
	expect_equal(dim(collect(batting)), dim(Batting))
	expect_equal(dim(filter(batting, yearID > 2005, G > 130)), c(1126, 24))
	expect_equal(dim(select(batting, playerID:lgID)), c(97889, 5))
	
	print(dim(arrange(batting, playerID, desc(yearID))))
	print(dim(summarise(batting, G = mean(G), n = n())))
	print(dim(mutate(batting, rbi2 = if(!is.null(AB) & AB > 0) 1.0 * R / AB else 0)))
})


stop()


# co* verbs

# cc <- compute(batting)
# head(cc)


# note that all operations are lazy: they don't do anything until you
# request the data, either by `print()`ing it (which shows the first ten
# rows), by looking at the `head()`, or `collect()` the results locally.
nrow(head(collect(filter(batting, yearID > 2010)), n=15L))

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
print(nrow(head(inner_join(player_info, hof), n=20L)))
# Keep all players, match hof data where available
print(nrow(head(left_join(player_info, hof), n=21L)))
# Find only players in hof
print(nrow(head(semi_join(player_info, hof), n=22L)))
# Find players not in hof
print(nrow(head(anti_join(player_info, hof), n=23L)))

}))
# TODO: set ops

# Arbitrary SQL -------------------------------------------------------------
# You can also provide sql as is, using the sql function:
batting2008 <- tbl(dps,
  sql('SELECT * FROM "Batting" WHERE "yearID" = 2008'))
nrow(head(batting2008, n=26L))

# sample functions
print(nrow(sample_n(player_info, 24L)))
print(nrow(head(sample_frac(player_info, .5), n=25L)))


dbWriteTable(dps$con, "mtcars", mtcars)
my_tbl <- tbl(dps, "mtcars") 

# https://github.com/hadley/dplyr/issues/1165
aa <- my_tbl %>% 
    group_by( cyl , gear ) %>% 
    summarise( n = n() ) %>% collect()
print(nrow(aa))

# this works fin
aa <- my_tbl %>% 
    group_by( cyl , gear ) %>% 
    summarise( n = n() ) %>% collect()
print(nrow(aa))

# this breaks
# aa <- my_tbl %>% 
#     group_by( cyl , gear ) %>% 
#     summarise( n = n() ) %>% 
#     mutate( pct = 100 * n / sum( n ) ) %>% collect()

# aa <- my_tbl %>%
#     group_by( cyl , gear ) %>%
#     tally %>%
#     group_by( cyl ) %>%
#     mutate( pct = ( 100 * n ) / sum( n ) )  %>% collect()
# print(nrow(aa))




dbRemoveTable(dps$con, "mtcars")


