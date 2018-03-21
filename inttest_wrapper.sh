#To run integration tests for jupyter notebook you need to have docker and docker-compose installed
docker-compose -p alti build 
docker-compose -p alti up -d
docker logs alti_jupyter_1
docker logs alti_test_1
