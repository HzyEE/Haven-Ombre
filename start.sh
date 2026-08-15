#!/bin/sh
# Start both brain and gateway in one container
python server.py &
BRAIN_PID=$!
sleep 2
python gateway.py &
GW_PID=$!

trap "kill $BRAIN_PID $GW_PID 2>/dev/null; exit 1" TERM INT

# Poll until either process exits
while kill -0 $BRAIN_PID 2>/dev/null && kill -0 $GW_PID 2>/dev/null; do
  sleep 5
done

kill $BRAIN_PID $GW_PID 2>/dev/null
exit 1
