#!/usr/bin/env bash

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

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
	local now=$SECONDS
	local lcnt=0
	local elapse=
	local t=
	while mclient $MDATABASE $MHOST $MPORT -ftab -s "SELECT 1;" > /dev/null ; do
		: $((cnt++))
		elapse=$((SECONDS - now))
		if [[ ${elapse} -ge 3 ]] ; then
			t=$((cnt - lcnt))
			t=$((t * 100))
			t=$((t / elapse))
			echo "mclient $num executed query $cnt, current speed: ${t%??}.${t#${t%??}}q/s"
			lcnt=${cnt}
			now=$SECONDS
		fi
	done
	echo "mclient $num terminated in query $cnt"
}

FORKS=
for nr in $(seq 1 $CONCURRENCY) ; do
	concurrent_runner $nr &
	FORKS+=" $!"
done

cleanup() {
	kill $FORKS
}
trap cleanup TERM INT QUIT

# wait for all children to end
wait $FORKS
