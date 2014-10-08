ll <- NULL
if (Sys.getenv("TSTTRGDIR") != "") {
	ll <- paste0(Sys.getenv("TSTTRGDIR"),"/rlibdir")
}
library(MonetDB.R,quietly=T,lib.loc=ll)
library(dplyr,quietly=T)
library(Lahman,quietly=T)

args <- commandArgs(trailingOnly = TRUE)
dbport <- 50000
dbname <- "mTests_clients_R"
if (length(args) > 0) 
	dbport <- args[[1]]

# old way
if (exists("lahman_monetdb")) {
	# overwrite all args because lahman_monetdb sets a default arg in the first pos.
	dps <- lahman_monetdb(host="localhost", dbname=dbname, port=dbport ,
		user="monetdb",password="monetdb",timeout=100,wait=T,language="sql")
# new way
} else {
	dps <-  src_monetdb(dbname=dbname, port=dbport)
	copy_lahman(dps)
}

# the remainder is pretty much the example from the manpage.


# Methods -------------------------------------------------------------------
batting <- tbl(dps, "Batting")
dim(batting)
colnames(batting)
head(batting)

# co* verbs
cc <- collapse(batting)
cc <- collect(batting)
# cc <- compute(batting)
# head(cc)


# Data manipulation verbs ---------------------------------------------------
filter(batting, yearID > 2005, G > 130)
select(batting, playerID:lgID)
arrange(batting, playerID, desc(yearID))
summarise(batting, G = mean(G), n = n())
mutate(batting, rbi2 = if(!is.null(AB) & AB > 0) 1.0 * R / AB else 0)

# note that all operations are lazy: they don't do anything until you
# request the data, either by `print()`ing it (which shows the first ten
# rows), by looking at the `head()`, or `collect()` the results locally.

cat("#~BeginVariableOutput~#\n")
system.time(recent <- filter(batting, yearID > 2010))
system.time(collect(recent))
cat("#~EndVariableOutput~#\n")

# Group by operations -------------------------------------------------------
# To perform operations by group, create a grouped object with group_by
players <- group_by(batting, playerID)
group_size(players)
summarise(players, mean_g = mean(G), best_ab = max(AB))

# When you group by multiple level, each summarise peels off one level
per_year <- group_by(batting, playerID, yearID)
stints <- summarise(per_year, stints = max(stint))
filter(stints, stints > 3)
summarise(stints, max(stints))

# Joins ---------------------------------------------------------------------
player_info <- select(tbl(dps, "Master"), playerID,
  birthYear)
hof <- select(filter(tbl(dps, "HallOfFame"), inducted == "Y"),
 playerID, votedBy, category)

# Match players and their hall of fame data
inner_join(player_info, hof)
# Keep all players, match hof data where available
left_join(player_info, hof)
# Find only players in hof
semi_join(player_info, hof)
# Find players not in hof
anti_join(player_info, hof)

# TODO: set ops

# Arbitrary SQL -------------------------------------------------------------
# You can also provide sql as is, using the sql function:
batting2008 <- tbl(dps,
  sql('SELECT * FROM "Batting" WHERE "yearID" = 2008'))
batting2008

print("SUCCESS")
