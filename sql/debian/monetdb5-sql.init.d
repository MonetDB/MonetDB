#! /bin/sh

PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
DAEMON=/usr/bin/merovingian
NAME=merovingian
DESC="MonetDB SQL server"

test -x $DAEMON || exit 0

LOGDIR=/var/log/MonetDB
PIDFILE=/var/run/MonetDB/$NAME.pid

# Include monetdb5-sql defaults if available
if [ -f /etc/default/monetdb5-sql ] ; then
    . /etc/default/monetdb5-sql
fi

set -e

init() {
    if [ ! -d /var/run/MonetDB ]; then
        mkdir /var/run/MonetDB
    fi
    chown -R monetdb.monetdb /var/run/MonetDB
    chmod 775 /var/run/MonetDB
    rm -f /var/run/MonetDB/*
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
            echo "can't start, should be enabled first by change STARTUP to yes in /etc/default/monetdb5-sql"
            exit 1
        fi

        if running; then
            echo "$NAME is already running"
            exit 1
        fi

        init

        echo -n "Starting $DESC: "
        start-stop-daemon --start --exec $DAEMON -- $DAEMON_OPTS
        if running ; then
            echo "$NAME."
        else
            echo " ERROR, $NAME didn't start"
        fi
        ;;
  stop)
        echo -n "Stopping $DESC: "
        start-stop-daemon --stop --pidfile $PIDFILE --exec $DAEMON
        echo "$NAME."
        ;;
  restart)
        $0 stop
        sleep 5
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
