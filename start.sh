#!/bin/sh
# Start both brain and gateway in one container
python server.py &
BRAIN_PID=$!
sleep 2
python gateway.py &
GW_PID=$!

trap "kill $BRAIN_PID $GW_PID 2>/dev/null; exit 1" TERM INT
wait -n
kill $BRAIN_PID $GW_PID 2>/dev/null
exit 1
