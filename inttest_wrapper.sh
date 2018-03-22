#!/bin/bash
#To run integration tests for jupyter notebook you need to have docker and docker-compose installed

#set -e

docker-compose -p alti build 
docker-compose -p alti up -d
echo -n "Sleeping for 10 seconds waiting for the tests to complete"
sleep 10s

docker logs --tail 25 alti_jupyter_1 2>&1 | tee test-scripts/alti_jupyter_1_inttest.log
grep -F "OK" test-scripts/alti_jupyter_1_inttest.log || { echo 'JUPYTER UNIT TESTS FAILED' ; return 1;}

docker logs --tail 5 alti_test_1 2>&1 | tee test-scripts/alti_test_1_inttest.log
grep -F "OK" test-scripts/alti_test_1_inttest.log || { 'JUPYTER INTEGRATION TESTS FAILED' ; return 1;}

#docker container stop alti_jupyter_1
#docker container rm alti_jupyter_1
