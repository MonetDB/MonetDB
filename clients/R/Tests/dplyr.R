library(dplyr,quietly=T)
library(MonetDB.R,quietly=T)
library(Lahman,quietly=T)
options(monetdb.debug.query=T)

args <- commandArgs(trailingOnly = TRUE)
dbport <- 50000
dbname <- "mTests_clients_R"
if (length(args) > 0) 
	dbport <- args[[1]]

# the remainder is pretty much the example from the manpage.

# overwrite all args because lahman_monetdb sets a default arg in the first pos.
# srct <- function() lahman_monetdb(host="localhost", ,
# 	user="monetdb",password="monetdb",timeout=100,wait=T,language="sql")

srct <- function() src_monetdb(dbname=dbname, port=dbport)
copy_lahman(srct())

# Methods -------------------------------------------------------------------
batting <- tbl(srct(), "Batting")
dim(batting)
colnames(batting)
head(batting)

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
player_info <- select(tbl(srct(), "Master"), playerID, hofID,
  birthYear)
hof <- select(filter(tbl(srct(), "HallOfFame"), inducted == "Y"),
 hofID, votedBy, category)

# Match players and their hall of fame data
inner_join(player_info, hof)
# Keep all players, match hof data where available
left_join(player_info, hof)
# Find only players in hof
semi_join(player_info, hof)
# Find players not in hof
anti_join(player_info, hof)

# Arbitrary SQL -------------------------------------------------------------
# You can also provide sql as is, using the sql function:
batting2008 <- tbl(srct(),
  sql('SELECT * FROM "Batting" WHERE "yearID" = 2008'))
batting2008

print("SUCCESS")
