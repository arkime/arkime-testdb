#!/bin/bash
# usage: runtimeout.sh <seconds> <cmd...>
T=$1; shift
"$@" &
PID=$!
( sleep $T && kill -TERM $PID 2>/dev/null && sleep 2 && kill -KILL $PID 2>/dev/null ) &
WATCHER=$!
wait $PID 2>/dev/null
RC=$?
kill $WATCHER 2>/dev/null
exit $RC
