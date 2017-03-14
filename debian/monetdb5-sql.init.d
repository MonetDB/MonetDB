#!/bin/bash

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

### BEGIN INIT INFO
# Provides:          mserver5-sql
# Required-Start:    $remote_fs $syslog
# Required-Stop:     $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: MonetDB SQL service
# Description:       MonetDB SQL service "monetdbd".
### END INIT INFO

PATH=/sbin:/usr/sbin:/bin:/usr/bin
DAEMON=/usr/bin/monetdbd
NAME=monetdbd
DESC="MonetDB SQL server"

test -x $DAEMON || exit 0

umask 022

# Include monetdb5-sql defaults if available
if [ -f /etc/default/monetdb5-sql ] ; then
    . /etc/default/monetdb5-sql
fi

PIDFILE=${DBFARM}/merovingian.pid

set -e

init() {
    if [ ! -d ${DBFARM} ]; then
        mkdir ${DBFARM}
	echo "${DBFARM} doesn't exist, creating..."
    fi

    chown -R monetdb.monetdb ${DBFARM}
    chmod 770 ${DBFARM}

    if [ ! -f ${DBFARM}/.merovingian_properties ]; then
	echo "${DBFARM} not initialized, initializing..."
	sudo -u monetdb ${DAEMON} create ${DBFARM} || exit 1
    fi
}

running_pid() {
    # Check if a given process pid's cmdline matches a given name
    pid=$1
    name=$2
    [ -z "$pid" ] && return 1
    [ ! -d /proc/$pid ] &&  return 1
    cmd=`cat /proc/$pid/cmdline | tr "\000" "\n"|head -n 1 |cut -d : -f 1`
    # Is this the expected child?
    [ "$cmd" != "$name" ] &&  return 1
    return 0
}

running() {
    [ ! -f "$PIDFILE" ] && return 1
    # Obtain the pid and check it against the binary name
    pid=`cat $PIDFILE`
    running_pid $pid $DAEMON || return 1
    return 0
}

case "$1" in
start)
    if [ "$STARTUP" != "yes" ]; then
        echo "can't start, should be enabled first by changing STARTUP to yes in /etc/default/monetdb5-sql"
        exit 0
    fi

    if running; then
        echo "$NAME is already running"
        exit 0
    fi

    init

    echo -n "Starting $DESC: "
    start-stop-daemon --start --exec $DAEMON -c monetdb:monetdb -- start $DAEMON_OPTS $DBFARM
    if running ; then
        echo "$NAME."
    else
        echo " ERROR, $NAME didn't start"
    fi
    ;;
stop)
    if running ;  then
        echo -n "Stopping $DESC: "
        start-stop-daemon --stop --pidfile $PIDFILE --exec $DAEMON --retry 60 --signal TERM
        echo "$NAME."
    else
	echo "$NAME not running."
    fi
    ;;
restart)
    $0 stop
    $0 start
    ;;
status)
    echo -n "$NAME is "
    if running ;  then
        echo "running"
    else
        echo "not running."
        exit 1
    fi
    ;;
*)
    echo "Usage: $0 {start|stop|restart|status}" >&2
    exit 1
    ;;
esac

exit 0
