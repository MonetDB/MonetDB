#! /bin/sh

# fetches the source of the first given argument

if [ ! $1 ]; then
	echo "$0 expects an argument, ie. $0 http://monetdb.cwi.nl/" > /dev/stderr
	exit -1
fi

if [ `which lynx >& /dev/null; echo $?` -eq 0 ]; then
	lynx -source $1
elif [ `which links >& /dev/null; echo $?` -eq 0 ]; then
	links -source $1
elif [ `which curl >& /dev/null; echo $?` -eq 0 ]; then
	# most sun/apple systems are shipped with this
	curl -s $1
elif [ `which wget >& /dev/null; echo $?` -eq 0 ]; then
	# wget is a more sophisticated and friendly version of curl
	wget -q -O - $1
else
	echo "No available method for fetching HTTP data found" > /dev/stderr
	exit -1
fi
