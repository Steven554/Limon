#!/bin/bash

#execute test files, write logs
sleep 15;

exec python3 -m pytest db_utils.py | tee -a /tmp/tests.log && echo "DB_UTILS.PY done" | tee -a /tmp/tests.log;
exec python3 -m pytest test_sys_poll.py | tee -a /tmp/tests.log && echo "SYS_POLL.PY done" | tee -a /tmp/tests.log;

sleep 5;

#read logs
#cat /tmp/tests.log;
echo "tests done - bash exit";
