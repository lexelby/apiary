#!/bin/sh

COMMON_OPTS='--mysql-host=db209.phx --mysql-user=foo --mysql-passwd=bar --mysql-db=indra --timeout 10'
CENTRAL_OPTS='--central -v ../logs/query-test-100k*'
WORKER_OPTS='--amqp-host=chastity --asap --f-schema --fork 2 --worker 50 '

WORKER_MACHINES='-m faith -m hope'

HIVE=hive_indra.py
# to run everything through rabbitmq, but not the database, use hive_indra_null.py
#HIVE=hive_indra_null.py

dsh -c ${WORKER_MACHINES} -- "cd /local/hive/hive && python ${HIVE} ${COMMON_OPTS} ${WORKER_OPTS} " & python ${HIVE} ${COMMON_OPTS} ${CENTRAL_OPTS}


