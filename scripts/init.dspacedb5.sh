#!/bin/bash
echo "Starting postgres"
/usr/local/bin/docker-entrypoint.sh postgres &> ./__postgres.log &
PID=$!
sleep 3

createuser --username=postgres dspace

echo "Importing clarin-dspace"
createdb --username=postgres --owner=dspace --encoding=UNICODE clarin-dspace
psql -U postgres clarin-dspace < ../dump/clarin-dspace-8.8.23.sql &> /dev/null
psql -U postgres clarin-dspace < ../dump/clarin-dspace-8.8.23.sql &> ./__clarin-dspace.log

echo "Importing clarin-utilities"
createdb --username=postgres --encoding=UNICODE clarin-utilities
psql -U postgres clarin-utilities < ../dump/clarin-utilities-8.8.23.sql &> /dev/null
psql -U postgres clarin-utilities < ../dump/clarin-utilities-8.8.23.sql &> ./__clarin-utilities.log

echo "Done, starting psql"

# psql -U postgres
echo "Waiting for PID:$PID /usr/local/bin/docker-entrypoint.sh"
wait $PID