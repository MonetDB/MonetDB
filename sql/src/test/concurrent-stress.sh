#!/usr/bin/env bash
# licence

# Run mclient with a variable number of concurrent connections for
# simple SELECT 1; queries.  Disconnect after each query.  Continue as
# long as the server can cope.
# This script intends to simulate a scenario as reported in bug #2700

do_usage() {
	echo "usage: concurrent_stress.sh <concurrency> [database [host [port]]]" 
	exit 1
}

[[ -z $1 ]] && do_usage

CONCURRENCY=$1
shift
for v in MDATABASE:d MHOST:h MPORT:p ; do
	[[ -z $1 ]] && break
	eval ${v%:*}="-${v#*:}$1"
	shift
done

if ! type -P mclient > /dev/null ; then
	echo "cannot find mclient in PATH!" > /dev/stderr
	exit 1
fi

echo "invoking $CONCURRENCY runners using the command:"
echo "  mclient $MDATABASE $MHOST $MPORT -ftab -s \"SELECT 1;\""

concurrent_runner() {
	local num=$1
	local cnt=1
	while mclient $MDATABASE $MHOST $MPORT -ftab -s "SELECT 1;" > /dev/null ; do
		: $((cnt++))
	done
	echo "mclient $num terminated in query $cnt"
}

for nr in $(seq 1 $CONCURRENCY) ; do
	concurrent_runner $nr &
done
wait
