ff <- textConnection("asdf", open="w")
# hide output from connect and attach since it would blow up the test output
# dangerous since it might hide useful warnings
# so if things go south it might be a good idea to uncomment the cat(dd) below
dd <- capture.output( suppressMessages ( {

library(dplyr, quietly=T)

args <- commandArgs(trailingOnly = TRUE)
dbport <- 50000
dbname <- "mTests_clients_R"
if (length(args) > 0) 
  dbport <- args[[1]]
if (length(args) > 1) 
  dbname <- args[[2]]
}))

so <- function(x) {
  print(dim(collect(head(x, 42))))
}

my_db <- MonetDBLite::src_monetdb(dbname=dbname, port=dbport, wait=T)
if (!DBI::dbExistsTable(my_db$con  , 'flights')) DBI::dbWriteTable(my_db$con , 'flights' , nycflights13::flights , csvdump=T, overwrite=T)
flights <- tbl( my_db , 'flights')

dim(flights)
so(flights)
so(filter(flights, month == 1, day == 1))
so(filter(flights, month == 1 | month == 2))

# MonetDBLite has ORDER BY in subqueries, but standalone MonetDB does not.
# so(arrange(flights, year, month, day))
# so(arrange(flights, desc(arr_delay)))
so(select(flights, year, month, day))
so(select(flights, year:day))
so(select(flights, -(year:day)))
so(select(flights, tail_num = tailnum))
so(rename(flights, tail_num = tailnum))
so(distinct(select(flights, tailnum)))

so(distinct(select(flights, origin, dest)))
so(mutate(flights,
  gain = arr_delay - dep_delay,
  speed = distance / air_time * 60))


so(mutate(flights,
  gain = arr_delay - dep_delay,
  gain_per_hour = (arr_delay - dep_delay) / (air_time / 60)
))

so(transmute(flights,
  gain = arr_delay - dep_delay,
  gain_per_hour = (arr_delay - dep_delay) / (air_time / 60)
))

so(summarise(flights,
  delay = mean(dep_delay)))

so(sample_n(flights, 10))
so(sample_frac(flights, 0.01))

# slice is not supported for reldbs as per dplyr doc
# slice(flights, 1:10)
# filter(flights, between(row_number(), 1, 10))

destinations <- group_by(flights, dest)

so(summarise(destinations,
  planes = n_distinct(tailnum),
  flights = n()
))

by_tailnum <- group_by(flights, tailnum)
delay <- summarise(by_tailnum,
  count = n(),
  dist = mean(distance),
  delay = mean(arr_delay))

delay <- collect(filter(delay, count > 20, dist < 2000))
so(delay)

daily <- group_by(flights, year, month, day)
so(per_day   <- summarise(daily, flights = n()))
so(per_month <- summarise(per_day, flights = sum(flights)))
so(per_yr  <- summarise(per_month, flights = sum(flights)))

a1 <- group_by(flights, year, month, day)
a2 <- select(a1, arr_delay, dep_delay)


a3 <- summarise(a2,
  arr = mean(arr_delay),
  dep = mean(dep_delay))


a4 <- filter(a3, arr > 30 | dep > 30)
so(a4)

so(filter(
  summarise(
    select(
      group_by(flights, year, month, day),
      arr_delay, dep_delay
    ),
    arr = mean(arr_delay),
    dep = mean(dep_delay)
  ),
  arr > 30 | dep > 30
))

so(flights %>%
  group_by(year, month, day) %>%
  select(arr_delay, dep_delay) %>%
  summarise(
    arr = mean(arr_delay),
    dep = mean(dep_delay)
  ) %>%
  filter(arr > 30 | dep > 30))


print("SUCCESS")
