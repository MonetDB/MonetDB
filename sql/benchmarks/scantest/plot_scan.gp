
#set term postscript eps

set xlabel 'Number of rows summed'
set ylabel 'Time'

set title 'Summing all columns, warm start, each table has 100000 rows'
set key 30000,2.0
plot 'time_sum.dat' using 1:2 title '1 column', \
	 'time_sum.dat' using 1:3 title '5 columns', \
	 'time_sum.dat' using 1:4 title '10 columns', \
	 'time_sum.dat' using 1:5 title '25 columns'

pause -1 "Hit return"

set title 'Summing all columns, warm start, each table has 100000 rows'
set key 30000,20.0
plot 'time_sum.dat' using 1:6 title '50 columns', \
	 'time_sum.dat' using 1:7 title '100 columns', \
	 'time_sum.dat' using 1:8 title '200 columns'

pause -1 "Hit return"

set title 'Summing some columns, warm start, each table has 100000 rows'
set key 30000,2.0
plot 'time_sum.dat' using 1:2 title '1 from 1', \
	 'time_sum.dat' using 1:3 title '5 from 5', \
	 'time_sum.dat' using 1:4 title '10 from 10', \
	 'time_sum.dat' using 1:5 title '25 from 25', \
	 'time_sum.dat' using 1:9 title '1 from 25', \
	 'time_sum.dat' using 1:10 title '5 from 25', \
	 'time_sum.dat' using 1:11 title '10 from 25'

pause -1 "Hit return"

set title 'Summing and other function, warm start, each table has 100000 rows'
set key 30000,35.0
plot 'time_sum.dat' using 1:5 title 'sum 25', \
	 'time_sum.dat' using 1:12 title 'sum 25 + 1 trunc', \
	 'time_sum.dat' using 1:13 title 'sum 25 + 1 sin'

pause -1 "Hit return"

set title 'Summing where v1 in range, warm start, each table has 100000 rows'
set key 30000,5.5
plot 'time_sum.dat' using 1:4 title '10 where rownum', \
	 'time_sum.dat' using 1:5 title '25 where rownum', \
	 'time_sum.dat' using 1:6 title '50 where rownum', \
	 'time_sum.dat' using 1:14 title '10 where v1', \
	 'time_sum.dat' using 1:15 title '25 where v1', \
	 'time_sum.dat' using 1:16 title '50 where v1'
